#!/usr/bin/env python3
"""
hailrunner — Dataproc lifecycle management for Hail jobs.

CLI:
    hailrunner run   --script S [--project P] [--workers N] [-- script args...]
    hailrunner create  [--project P] [--workers N]
    hailrunner submit  --cluster C --script S [--project P] [-- script args...]
    hailrunner destroy --cluster C [--project P]

Library:
    from hailrunner import HailCluster, ClusterConfig, SparkConfig

    cfg = ClusterConfig(project="my-proj", workers=16)
    with HailCluster(cfg) as cluster:
        cluster.submit("my_script.py", ["--chrom", "chr21"])
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Optional

__version__ = "0.1.0"

log = logging.getLogger("hailrunner")


# ---------------------------------------------------------------------------
# configuration
# ---------------------------------------------------------------------------

@dataclass
class ClusterConfig:
    project: str
    staging_bucket: str
    region: str = "us-central1"
    subnet: str = "subnetwork"
    workers: int = 16
    preemptibles: int = 0
    worker_type: str = "n1-highmem-8"
    driver_type: str = "n1-highmem-32"
    worker_disk_gb: int = 300
    driver_disk_gb: int = 500
    disk_type: str = "pd-standard"
    max_idle_minutes: int = 60
    max_age_minutes: int = 1440
    cluster_name: Optional[str] = None
    service_account: Optional[str] = None

    @property
    def subnet_uri(self) -> Optional[str]:
        if self.subnet:
            return f"projects/{self.project}/regions/{self.region}/subnetworks/{self.subnet}"
        return None


@dataclass
class SparkConfig:
    executor_cores: int = 4
    executor_memory: str = "26g"
    driver_cores: int = 4
    driver_memory: str = "26g"


@dataclass
class OutputSpec:
    src: str
    dst: str


class ClusterState(Enum):
    UNBORN = "unborn"
    CREATING = "creating"
    RUNNING = "running"
    SUBMITTING = "submitting"
    DESTROYING = "destroying"
    DESTROYED = "destroyed"
    FAILED = "failed"


# ---------------------------------------------------------------------------
# cost estimation
# ---------------------------------------------------------------------------

# GCP us-central1 on-demand pricing (as of 2025)
_VCPU_HOUR = 0.0475          # n1 on-demand per vCPU
_RAM_GB_HOUR = 0.006391      # n1 on-demand per GB RAM
_PREEMPT_VCPU_HOUR = 0.01    # n1 preemptible per vCPU
_PREEMPT_RAM_GB_HOUR = 0.00135  # n1 preemptible per GB RAM
_DATAPROC_VCPU_HOUR = 0.01   # Dataproc surcharge per vCPU (all types)

# Persistent disk pricing per GB/month (us-central1)
_PD_GB_MONTH = {
    "pd-standard": 0.04,
    "pd-balanced": 0.10,
    "pd-ssd": 0.17,
}

# RAM multiplier per vCPU by n1 family
_N1_RAM_PER_VCPU = {
    "standard": 3.75,
    "highmem": 6.5,
    "highcpu": 0.9,
}


def _parse_machine_type(machine_type: str) -> tuple[int, float]:
    """Parse n1-{family}-{N} into (vcpus, ram_gb). Raises ValueError if unrecognized."""
    parts = machine_type.split("-")
    if len(parts) != 3 or parts[0] != "n1":
        raise ValueError(f"Unsupported machine type: {machine_type} (only n1 family supported)")
    family = parts[1]
    if family not in _N1_RAM_PER_VCPU:
        raise ValueError(f"Unknown n1 family '{family}' in {machine_type}")
    vcpus = int(parts[2])
    ram_gb = vcpus * _N1_RAM_PER_VCPU[family]
    return vcpus, ram_gb


def estimate_cluster_cost(config: ClusterConfig, runtime_seconds: float) -> dict:
    """Estimate cluster cost from config and runtime. Returns cost breakdown dict."""
    hours = runtime_seconds / 3600

    drv_vcpus, drv_ram = _parse_machine_type(config.driver_type)
    wrk_vcpus, wrk_ram = _parse_machine_type(config.worker_type)

    driver_cost = hours * (drv_vcpus * _VCPU_HOUR + drv_ram * _RAM_GB_HOUR)
    worker_cost = hours * config.workers * (wrk_vcpus * _VCPU_HOUR + wrk_ram * _RAM_GB_HOUR)

    if config.preemptibles > 0:
        preemptible_cost = hours * config.preemptibles * (
            wrk_vcpus * _PREEMPT_VCPU_HOUR + wrk_ram * _PREEMPT_RAM_GB_HOUR
        )
    else:
        preemptible_cost = 0.0

    total_disk_gb = config.driver_disk_gb + (config.workers + config.preemptibles) * config.worker_disk_gb
    pd_gb_hour = _PD_GB_MONTH.get(config.disk_type, _PD_GB_MONTH["pd-standard"]) / (30 * 24)
    disk_cost = hours * total_disk_gb * pd_gb_hour

    total_vcpus = drv_vcpus + config.workers * wrk_vcpus + config.preemptibles * wrk_vcpus
    dataproc_surcharge = hours * total_vcpus * _DATAPROC_VCPU_HOUR

    return {
        "runtime_hours": hours,
        "driver_cost": driver_cost,
        "worker_cost": worker_cost,
        "preemptible_cost": preemptible_cost,
        "disk_cost": disk_cost,
        "dataproc_surcharge": dataproc_surcharge,
        "total_disk_gb": total_disk_gb,
        "total_estimated_cost": driver_cost + worker_cost + preemptible_cost + disk_cost + dataproc_surcharge,
    }


def _log_cost_estimate(config: ClusterConfig, runtime_seconds: float) -> None:
    """Log a formatted cost estimate table."""
    try:
        est = estimate_cluster_cost(config, runtime_seconds)
    except ValueError as e:
        log.warning("Could not estimate cost: %s", e)
        return

    lines = [
        "Cost estimate (us-central1 on-demand rates):",
        f"  Runtime:                              {est['runtime_hours']:.2f} hrs",
        f"  Driver ({config.driver_type} x 1):    ${est['driver_cost']:.2f}",
        f"  Workers ({config.worker_type} x {config.workers}):   ${est['worker_cost']:.2f}",
    ]
    if config.preemptibles > 0:
        lines.append(
            f"  Preemptibles ({config.worker_type} x {config.preemptibles}): ${est['preemptible_cost']:.2f}"
        )
    lines.extend([
        f"  Disk ({est['total_disk_gb']:,} GB {config.disk_type}):  ${est['disk_cost']:.2f}",
        f"  Dataproc surcharge:                   ${est['dataproc_surcharge']:.2f}",
        f"  {'─' * 40}",
        f"  Estimated total:                      ${est['total_estimated_cost']:.2f}",
    ])
    for line in lines:
        log.info(line)


# ---------------------------------------------------------------------------
# subprocess helpers
# ---------------------------------------------------------------------------

def _run(cmd: list[str], label: str, timeout: Optional[int] = None) -> str:
    """Run a short-lived subprocess. Captures output. Raises on failure."""
    flat = " ".join(cmd)
    log.info("[%s] %s", label, flat)
    t0 = time.time()
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        log.error("[%s] TIMEOUT after %.0fs: %s", label, time.time() - t0, flat)
        raise
    elapsed = time.time() - t0
    if result.stdout.strip():
        for line in result.stdout.strip().splitlines():
            log.info("[%s] stdout: %s", label, line)
    if result.stderr.strip():
        level = logging.WARNING if result.returncode != 0 else logging.DEBUG
        for line in result.stderr.strip().splitlines():
            log.log(level, "[%s] stderr: %s", label, line)
    if result.returncode != 0:
        log.error("[%s] FAILED (exit %d, %.0fs)", label, result.returncode, elapsed)
        raise subprocess.CalledProcessError(
            result.returncode, cmd, output=result.stdout, stderr=result.stderr,
        )
    log.info("[%s] OK (%.0fs)", label, elapsed)
    return result.stdout


def _run_streaming(cmd: list[str], label: str) -> None:
    """
    Run a long-lived subprocess with live output streaming to the logger.
    Use for job submission where we need real-time feedback.
    """
    flat = " ".join(cmd)
    log.info("[%s] %s", label, flat)
    t0 = time.time()
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )

    def _stream():
        for line in proc.stdout:
            line = line.rstrip()
            if line:
                log.info("[%s] %s", label, line)

    thread = threading.Thread(target=_stream, daemon=True)
    thread.start()
    proc.wait()
    thread.join(timeout=10)
    elapsed = time.time() - t0

    if proc.returncode != 0:
        log.error("[%s] FAILED (exit %d, %.0fs)", label, proc.returncode, elapsed)
        raise subprocess.CalledProcessError(proc.returncode, cmd)
    log.info("[%s] OK (%.0fs)", label, elapsed)


# ---------------------------------------------------------------------------
# utility functions
# ---------------------------------------------------------------------------

def _detect_account() -> str:
    out = _run(["gcloud", "config", "get-value", "account"], "gcloud-account")
    account = out.strip()
    if not account or account == "(unset)":
        raise RuntimeError("No active gcloud account.")
    log.info("Using account: %s", account)
    return account


def _generate_cluster_name(prefix: str = "hailrun") -> str:
    prefix = re.sub(r"[^a-z0-9-]", "-", prefix.lower())[:10].rstrip("-")
    if not prefix or not prefix[0].isalpha():
        prefix = "hr"
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


HAIL_VERSION = "0.2.132"
IMAGE_VERSION = "2.2.5-debian12"
INIT_SCRIPT = "/usr/local/share/hailrunner/init_hail.sh"
WHEELS_DIR = "/usr/local/share/hailrunner/wheels"


def _stage_files(bucket: str) -> str:
    """Upload init_hail.sh and wheels to the staging bucket, return init script GCS URI."""
    prefix = f"{bucket}/hailrunner"
    _run(["gsutil", "cp", INIT_SCRIPT, f"{prefix}/init_hail.sh"], "stage-init")
    _run(["gsutil", "-m", "cp", f"{WHEELS_DIR}/*", f"{prefix}/wheels/"], "stage-wheels")
    return f"{prefix}/init_hail.sh"


class ScriptSource(Enum):
    GCS = "gcs"
    URL = "url"
    LOCAL = "local"


def _detect_script_source(path: str) -> ScriptSource:
    if path.startswith("gs://"):
        return ScriptSource.GCS
    if path.startswith("http://") or path.startswith("https://"):
        return ScriptSource.URL
    return ScriptSource.LOCAL


def _resolve_script(path: str, workdir: str) -> str:
    source = _detect_script_source(path)
    log.info("Script source: %s (%s)", source.value, path)

    if source == ScriptSource.LOCAL:
        resolved = Path(path).resolve()
        if not resolved.is_file():
            raise FileNotFoundError(f"Script not found: {resolved}")
        return str(resolved)

    local_path = os.path.join(workdir, "hailrunner_script.py")
    if source == ScriptSource.GCS:
        _run(["gsutil", "cp", path, local_path], "fetch-script")
    elif source == ScriptSource.URL:
        _run(["curl", "-fsSL", "-o", local_path, path], "fetch-script")

    if not os.path.isfile(local_path):
        raise FileNotFoundError(f"Failed to fetch script from {path}")
    return local_path


def _parse_output_spec(raw: str) -> OutputSpec:
    """Parse 'gs://bucket/path/file.ext:./local_name' into an OutputSpec."""
    # find the last colon that isn't in gs://
    if not raw.startswith("gs://"):
        raise ValueError(f"Output src must start with gs://, got: {raw}")
    rest = raw[5:]
    idx = rest.rfind(":")
    if idx == -1:
        raise ValueError(f"Output spec must be 'gs://src:dst', got: {raw}")
    return OutputSpec(src="gs://" + rest[:idx], dst=rest[idx + 1:])


def _copy_outputs(specs: list[OutputSpec]) -> None:
    for spec in specs:
        log.info("Copying output: %s -> %s", spec.src, spec.dst)
        dst_dir = os.path.dirname(spec.dst)
        if dst_dir:
            os.makedirs(dst_dir, exist_ok=True)
        _run(["gsutil", "-m", "cp", spec.src, spec.dst], "copy-output")


# ---------------------------------------------------------------------------
# cluster
# ---------------------------------------------------------------------------

class HailCluster:
    """
    Manages the lifecycle of an ephemeral Dataproc cluster.
    Use as a context manager for guaranteed cleanup.
    """

    def __init__(
        self,
        config: ClusterConfig,
        spark: Optional[SparkConfig] = None,
    ):
        self.config = config
        self.spark = spark or SparkConfig()
        self.name = config.cluster_name or _generate_cluster_name()
        self.state = ClusterState.UNBORN
        self._account: Optional[str] = None
        self._start_time: Optional[float] = None

    def __enter__(self) -> HailCluster:
        self._start_time = time.time()
        self.create()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = time.time() - (self._start_time or time.time())
        self.destroy()
        if exc_type:
            log.error("Session failed after %.0fs: %s: %s", elapsed, exc_type.__name__, exc_val)
        else:
            log.info("Session completed in %.0fs", elapsed)
        _log_cost_estimate(self.config, elapsed)
        return False

    @property
    def account(self) -> str:
        if self._account is None:
            self._account = self.config.service_account or _detect_account()
        return self._account

    def _elapsed(self) -> float:
        return time.time() - (self._start_time or time.time())

    def _log(self, msg: str, *args, level: int = logging.INFO) -> None:
        prefix = f"[{self.name}] [{self.state.value}] [{self._elapsed():.0f}s]"
        log.log(level, f"{prefix} {msg}", *args)

    def create(self) -> None:
        if self.state != ClusterState.UNBORN:
            raise RuntimeError(f"Cannot create cluster in state {self.state.value}")

        self.state = ClusterState.CREATING
        self._log("Creating Dataproc cluster...")
        self._log(
            "  workers=%d preemptibles=%d type=%s driver=%s disk=%dGB",
            self.config.workers, self.config.preemptibles,
            self.config.worker_type, self.config.driver_type,
            self.config.worker_disk_gb,
        )

        init_gcs = _stage_files(self.config.staging_bucket)
        wheels_gcs = f"{self.config.staging_bucket}/hailrunner/wheels"

        properties = {
            "spark:spark.task.maxFailures": "20",
            "spark:spark.driver.extraJavaOptions": "-Xss4M",
            "spark:spark.executor.extraJavaOptions": "-Xss4M",
            "spark:spark.speculation": "true",
            "hdfs:dfs.replication": "1",
            "dataproc:dataproc.logging.stackdriver.enable": "false",
            "dataproc:dataproc.monitoring.stackdriver.enable": "false",
        }
        prop_str = ",".join(f"{k}={v}" for k, v in properties.items())

        cmd = [
            "gcloud", "dataproc", "clusters", "create", self.name,
            f"--image-version={IMAGE_VERSION}",
            f"--properties={prop_str}",
            f"--metadata=WHEELS={wheels_gcs}",
            f"--initialization-actions={init_gcs}",
            "--initialization-action-timeout=20m",
            f"--region={self.config.region}",
            f"--project={self.config.project}",
            f"--num-workers={self.config.workers}",
            f"--worker-machine-type={self.config.worker_type}",
            f"--master-machine-type={self.config.driver_type}",
            f"--master-boot-disk-size={self.config.driver_disk_gb}",
            f"--master-boot-disk-type={self.config.disk_type}",
            f"--worker-boot-disk-size={self.config.worker_disk_gb}",
            f"--worker-boot-disk-type={self.config.disk_type}",
            f"--max-idle={self.config.max_idle_minutes}m",
            f"--max-age={self.config.max_age_minutes}m",
            f"--service-account={self.account}",
        ]
        if self.config.preemptibles > 0:
            cmd.append(f"--num-secondary-workers={self.config.preemptibles}")
            cmd.append(f"--secondary-worker-boot-disk-type={self.config.disk_type}")
        if self.config.subnet_uri:
            cmd.append(f"--subnet={self.config.subnet_uri}")

        try:
            _run(cmd, "cluster-create")
        except Exception:
            self.state = ClusterState.FAILED
            self._log("Cluster creation FAILED", level=logging.ERROR)
            raise

        self.state = ClusterState.RUNNING
        self._log("Cluster is running.")

    def submit(self, script: str, script_args: Optional[list[str]] = None) -> None:
        if self.state != ClusterState.RUNNING:
            raise RuntimeError(f"Cannot submit in state {self.state.value}")

        self.state = ClusterState.SUBMITTING

        with tempfile.TemporaryDirectory() as workdir:
            local_script = _resolve_script(script, workdir)
            self._log("Submitting: %s", local_script)

            cmd = [
                "gcloud", "dataproc", "jobs", "submit", "pyspark",
                local_script,
                f"--cluster={self.name}",
                "--project", self.config.project,
                f"--region={self.config.region}",
                "--account", self.account,
                "--driver-log-levels", "root=WARN",
                "--properties",
                (
                    f"spark.executor.cores={self.spark.executor_cores},"
                    f"spark.executor.memory={self.spark.executor_memory},"
                    f"spark.driver.cores={self.spark.driver_cores},"
                    f"spark.driver.memory={self.spark.driver_memory}"
                ),
            ]
            if script_args:
                cmd.append("--")
                cmd.extend(script_args)

            try:
                _run_streaming(cmd, "job-submit")
            except Exception:
                self.state = ClusterState.RUNNING
                self._log("Job FAILED", level=logging.ERROR)
                raise

        self.state = ClusterState.RUNNING
        self._log("Job completed.")

    def destroy(self) -> None:
        if self.state in (ClusterState.DESTROYED, ClusterState.UNBORN):
            return

        prev = self.state
        self.state = ClusterState.DESTROYING
        self._log("Destroying cluster...")

        try:
            _run(
                [
                    "gcloud", "dataproc", "clusters", "delete", "--quiet",
                    "--project", self.config.project,
                    "--region", self.config.region,
                    "--account", self.account,
                    self.name,
                ],
                "cluster-destroy",
                timeout=600,
            )
        except Exception as e:
            log.warning("Cluster destroy failed (was %s): %s", prev.value, e)
            self.state = ClusterState.FAILED
            return

        self.state = ClusterState.DESTROYED
        self._log("Cluster destroyed.")


# ---------------------------------------------------------------------------
# top-level run
# ---------------------------------------------------------------------------

def run(
    config: ClusterConfig,
    spark: Optional[SparkConfig] = None,
    script: str = "",
    script_args: Optional[list[str]] = None,
    outputs: Optional[list[OutputSpec]] = None,
) -> None:
    with HailCluster(config, spark) as cluster:
        cluster.submit(script, script_args)
    if outputs:
        _copy_outputs(outputs)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _add_cluster_args(parser: argparse.ArgumentParser) -> None:
    g = parser.add_argument_group("cluster")
    g.add_argument("--project", default=os.environ.get("GOOGLE_PROJECT"))
    g.add_argument("--staging-bucket", required=True, help="GCS bucket for staging (e.g. gs://fc-secure-...)")
    g.add_argument("--region", default="us-central1")
    g.add_argument("--subnet", default="subnetwork")
    g.add_argument("--workers", type=int, default=16)
    g.add_argument("--preemptibles", type=int, default=0)
    g.add_argument("--worker-type", default="n1-highmem-8")
    g.add_argument("--driver-type", default="n1-highmem-32")
    g.add_argument("--worker-disk-gb", type=int, default=300)
    g.add_argument("--driver-disk-gb", type=int, default=500)
    g.add_argument("--disk-type", default="pd-standard",
                   choices=["pd-standard", "pd-ssd", "pd-balanced"],
                   help="Boot disk type for all nodes (default: pd-standard)")
    g.add_argument("--max-idle", type=int, default=60, help="Minutes")
    g.add_argument("--max-age", type=int, default=1440, help="Minutes")
    g.add_argument("--cluster-name", default=None)
    g.add_argument("--service-account", default=None)


def _add_spark_args(parser: argparse.ArgumentParser) -> None:
    g = parser.add_argument_group("spark")
    g.add_argument("--executor-cores", type=int, default=4)
    g.add_argument("--executor-memory", default="26g")
    g.add_argument("--driver-cores", type=int, default=4)
    g.add_argument("--driver-memory", default="26g")


def _detect_project() -> Optional[str]:
    """Auto-detect GCP project from metadata server or gcloud config."""
    # GCE metadata server — works inside any GCE VM / Cromwell container
    try:
        import urllib.request
        req = urllib.request.Request(
            "http://metadata.google.internal/computeMetadata/v1/project/project-id",
            headers={"Metadata-Flavor": "Google"},
        )
        resp = urllib.request.urlopen(req, timeout=2)
        val = resp.read().decode().strip()
        if val:
            log.info("Detected project from GCE metadata: %s", val)
            return val
    except Exception:
        pass
    # gcloud config as last resort
    try:
        out = subprocess.run(
            ["gcloud", "config", "get-value", "project"],
            capture_output=True, text=True, timeout=10,
        )
        val = out.stdout.strip()
        if val and val != "(unset)":
            return val
    except Exception:
        pass
    return None


def _config_from_args(args: argparse.Namespace) -> ClusterConfig:
    project = args.project or _detect_project()
    if not project:
        raise SystemExit("Error: --project is required (or set GOOGLE_PROJECT)")
    return ClusterConfig(
        project=project,
        staging_bucket=args.staging_bucket,
        region=args.region,
        subnet=args.subnet,
        workers=args.workers,
        preemptibles=args.preemptibles,
        worker_type=args.worker_type,
        driver_type=args.driver_type,
        worker_disk_gb=args.worker_disk_gb,
        driver_disk_gb=args.driver_disk_gb,
        disk_type=args.disk_type,
        max_idle_minutes=args.max_idle,
        max_age_minutes=args.max_age,
        cluster_name=args.cluster_name,
        service_account=args.service_account,
    )


def _spark_from_args(args: argparse.Namespace) -> SparkConfig:
    return SparkConfig(
        executor_cores=args.executor_cores,
        executor_memory=args.executor_memory,
        driver_cores=args.driver_cores,
        driver_memory=args.driver_memory,
    )


def _cmd_run(args: argparse.Namespace, script_args: list[str]) -> None:
    outputs = [_parse_output_spec(o) for o in (args.output or [])]
    run(
        config=_config_from_args(args),
        spark=_spark_from_args(args),
        script=args.script,
        script_args=script_args or None,
        outputs=outputs,
    )


def _cmd_create(args: argparse.Namespace, _: list[str]) -> None:
    cluster = HailCluster(_config_from_args(args))
    cluster.create()
    print(cluster.name)


def _cmd_submit(args: argparse.Namespace, script_args: list[str]) -> None:
    cluster = HailCluster(_config_from_args(args), _spark_from_args(args))
    cluster.name = args.cluster
    cluster.state = ClusterState.RUNNING
    cluster.submit(args.script, script_args or None)
    if args.output:
        _copy_outputs([_parse_output_spec(o) for o in args.output])


def _cmd_destroy(args: argparse.Namespace, _: list[str]) -> None:
    cluster = HailCluster(_config_from_args(args))
    cluster.name = args.cluster
    cluster.state = ClusterState.RUNNING
    cluster.destroy()


def main(argv: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        prog="hailrunner",
        description="Dataproc lifecycle management for Hail jobs.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )

    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="Create cluster, submit job, destroy cluster.")
    _add_cluster_args(p_run)
    _add_spark_args(p_run)
    p_run.add_argument("--script", required=True, help="GCS path, URL, or local file.")
    p_run.add_argument("--output", action="append", help="gs://src:local_dst (repeatable)")


    p_create = sub.add_parser("create", help="Create cluster only. Prints name.")
    _add_cluster_args(p_create)

    p_submit = sub.add_parser("submit", help="Submit job to existing cluster.")
    _add_cluster_args(p_submit)
    _add_spark_args(p_submit)
    p_submit.add_argument("--cluster", required=True)
    p_submit.add_argument("--script", required=True)
    p_submit.add_argument("--output", action="append", help="gs://src:local_dst (repeatable)")

    p_destroy = sub.add_parser("destroy", help="Destroy existing cluster.")
    _add_cluster_args(p_destroy)
    p_destroy.add_argument("--cluster", required=True)

    raw = argv if argv is not None else sys.argv[1:]
    if "--" in raw:
        sep = raw.index("--")
        our_args, script_args = raw[:sep], raw[sep + 1:]
    else:
        our_args, script_args = raw, []

    args = parser.parse_args(our_args)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
    )

    dispatch = {
        "run": _cmd_run,
        "create": _cmd_create,
        "submit": _cmd_submit,
        "destroy": _cmd_destroy,
    }
    try:
        dispatch[args.command](args, script_args)
    except subprocess.CalledProcessError as e:
        log.error("Command failed (exit %d): %s", e.returncode, " ".join(e.cmd))
        if e.stderr:
            for line in e.stderr.strip().splitlines()[-20:]:
                log.error("  %s", line)
        sys.exit(e.returncode)
    except KeyboardInterrupt:
        log.warning("Interrupted.")
        sys.exit(130)
    except Exception as e:
        log.exception("Fatal error: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()