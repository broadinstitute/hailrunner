import argparse
import json
import sys
import time

import hail as hl


def _mt_complete(path: str) -> bool:
    """Check if a completed MatrixTable exists at path."""
    return hl.hadoop_exists(path + "/_SUCCESS")


def _remove_mt(path: str) -> None:
    """Remove a (possibly partial) MatrixTable directory."""
    import subprocess
    if hl.hadoop_exists(path):
        print(f"CLEAN: removing {path}", file=sys.stderr)
        subprocess.run(["gsutil", "-m", "-q", "rm", "-r", path], check=False)


def _read_manifest(path: str) -> list[tuple[str, str]]:
    """Read a TSV manifest of (vcf_path, mt_path) pairs."""
    pairs = []
    with hl.hadoop_open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            fields = line.split("\t")
            if len(fields) < 2:
                raise ValueError(f"Manifest line needs 2 tab-separated columns, got: {line!r}")
            pairs.append((fields[0].strip(), fields[1].strip()))
    assert pairs, f"No entries found in manifest {path}"
    return pairs


def vcf_to_mt_batch(
    manifest_path: str,
    genome: str = "GRCh38",
    overwrite: bool = False,
) -> dict:
    t0 = time.time()
    pairs = _read_manifest(manifest_path)
    results = []

    for vcf_path, mt_path in pairs:
        if not overwrite and _mt_complete(mt_path):
            print(f"SKIP (exists): {mt_path}", file=sys.stderr)
            results.append({"vcf": vcf_path, "mt": mt_path, "status": "skipped"})
            continue

        # Clean up incomplete MT from a previous failed run
        if not overwrite and hl.hadoop_exists(mt_path):
            _remove_mt(mt_path)

        print(f"START: {vcf_path} -> {mt_path}", file=sys.stderr)
        t1 = time.time()
        try:
            mt = hl.import_vcf(vcf_path, reference_genome=genome, force_bgz=True)
            mt.write(mt_path, overwrite=overwrite)

            mt2 = hl.read_matrix_table(mt_path)
            n_samples = mt2.count_cols()
            n_variants = mt2.count_rows()
            elapsed = time.time() - t1

            print(f"DONE:  {mt_path} ({n_samples} samples, {n_variants} variants, {elapsed:.0f}s)", file=sys.stderr)
            results.append({
                "vcf": vcf_path,
                "mt": mt_path,
                "status": "completed",
                "n_samples": n_samples,
                "n_variants": n_variants,
                "seconds": elapsed,
            })
        except Exception as e:
            elapsed = time.time() - t1
            print(f"FAIL:  {mt_path} ({elapsed:.0f}s): {e}", file=sys.stderr)
            _remove_mt(mt_path)
            results.append({
                "vcf": vcf_path,
                "mt": mt_path,
                "status": "failed",
                "error": str(e),
                "seconds": elapsed,
            })

    completed = sum(1 for r in results if r["status"] == "completed")
    skipped = sum(1 for r in results if r["status"] == "skipped")
    failed = sum(1 for r in results if r["status"] == "failed")

    return {
        "total": len(results),
        "completed": completed,
        "skipped": skipped,
        "failed": failed,
        "results": results,
        "total_s": time.time() - t0,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch convert VCFs to Hail MatrixTables.")
    parser.add_argument("--manifest", required=True, help="GCS or local path to TSV: vcf_path<tab>mt_output_path")
    parser.add_argument("--genome", default="GRCh38", help="Reference genome (default: GRCh38).")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing MatrixTables (default: skip).")
    args = parser.parse_args()

    hl.init()
    result = vcf_to_mt_batch(args.manifest, args.genome, args.overwrite)
    print(json.dumps(result, indent=2))

    if result["failed"] > 0:
        sys.exit(1)
