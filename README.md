# hailrunner

WDL workflow for running Hail jobs on ephemeral Google Cloud Dataproc clusters.

## What it does

The `hailrunner run` workflow creates an ephemeral Dataproc cluster, submits a PySpark/Hail script, collects outputs from GCS, and destroys the cluster -- all in a single WDL task. The cluster lifecycle is fully managed: if the job fails, the cluster is still torn down.

## Docker image

The image at [`hailrunner/docker/Dockerfile`](hailrunner/docker/Dockerfile) bundles:
- Python 3.11
- Google Cloud SDK (gcloud, gsutil)
- Hail 0.2.132 (includes hailctl)
- hailrunner CLI

Pre-built image:

```
us-docker.pkg.dev/broad-dsde-methods/hailrunner/hailrunner:0.1.0
```

## WDL workflow

| Workflow | Description |
|----------|-------------|
| [`hailrunner_run.wdl`](hailrunner/wdl/hailrunner_run.wdl) | Full lifecycle: create cluster, submit Hail script, collect outputs, destroy cluster |

### Required inputs

| Input | Type | Description |
|-------|------|-------------|
| `project` | String | GCP project ID |
| `script` | String | Hail/PySpark script — GCS path, HTTPS URL, or local path |

### Key optional inputs

| Input | Type | Default | Description |
|-------|------|---------|-------------|
| `workers` | Int | 16 | Number of Dataproc workers |
| `preemptibles` | Int | 0 | Number of preemptible/spot workers |
| `worker_type` | String | n1-highmem-8 | Worker machine type |
| `driver_type` | String | n1-highmem-32 | Driver (master) machine type |
| `output_specs` | Array[String] | [] | Output copy specs: `gs://src:local_dst` |
| `hardstop` | Int? | - | Kill everything after N minutes |
| `script_args` | Array[String] | [] | Arguments passed to the Hail script |

See the [WDL file](hailrunner/wdl/hailrunner_run.wdl) for the full list of inputs.

### Authentication

On Terra/Cromwell, the task VM's service account (from the GCE metadata server) is used for gcloud operations. Ensure it has permissions to:
- Create/delete Dataproc clusters in the target project (`roles/dataproc.admin`)
- Submit Dataproc jobs
- Act as the Dataproc cluster's service account (`roles/iam.serviceAccountUser`)
- Read/write to the relevant GCS buckets

### Outputs

Output files are specified via `output_specs`. Each entry is a string of the form `gs://bucket/path/file.ext:local_name`. After the Hail job completes, hailrunner copies each GCS source to the local destination, and the WDL task collects them as `Array[File] output_files`.

### Orphaned clusters

The task VM is an orchestrator, not the compute node. If the Cromwell task is preempted or killed, the Dataproc cluster could be orphaned. The `max_age` setting (default 1440 minutes / 24 hours) ensures Dataproc auto-deletes the cluster even if the orchestrator dies.

## Building the image

```bash
cd hailrunner/docker && bash push.sh
```

## Usage example

The `script` input accepts a GCS path, an HTTPS URL (e.g. a raw GitHub link), or a local file. This makes it easy to iterate: push a script to GitHub and point the WDL at the raw URL.

### VCF → MatrixTable ([`examples/vcf_to_mt.py`](examples/vcf_to_mt.py))

```json
{
  "hailrunner_run.project": "broad-dsde-methods",
  "hailrunner_run.script": "https://raw.githubusercontent.com/broadinstitute/hailrunner/main/examples/vcf_to_mt.py",
  "hailrunner_run.script_args": [
    "--vcf", "gs://prod-drc-broad/aou_phasing/v9/chr20.aou.v9.phased.vcf.gz",
    "--mt-output", "gs://your-bucket/aou_phasing/v9/chr20.aou.v9.phased.mt"
  ],
  "hailrunner_run.workers": 16
}
```
