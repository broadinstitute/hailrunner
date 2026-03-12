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

## Cost estimation

After every job, hailrunner logs an estimated cluster cost breakdown to stderr based on us-central1 on-demand GCP rates. This covers the driver, workers, preemptible workers, disk, and Dataproc surcharge. Example output:

```
Cost estimate (us-central1 on-demand rates):
  Runtime:                              0.83 hrs
  Driver (n1-standard-16 x 1):    $0.95
  Workers (n1-highmem-8 x 16):   $9.49
  Preemptibles (n1-highmem-8 x 32): $3.48
  Disk (25,024 GB):                    $1.37
  Dataproc surcharge:                   $3.98
  ────────────────────────────────────────
  Estimated total:                      $19.27
```

The estimate is also available programmatically via `estimate_cluster_cost(config, runtime_seconds)`.

## Usage examples

The `script` input accepts a GCS path, an HTTPS URL (e.g. a raw GitHub link), or a local file. This makes it easy to iterate: push a script to GitHub and point the WDL at the raw URL.

### VCF → MatrixTable ([`examples/vcf_to_mt.py`](examples/vcf_to_mt.py))

Convert a single VCF to a Hail MatrixTable.

```json
{
  "hailrunner_run.project": "my-gcp-project",
  "hailrunner_run.script": "https://raw.githubusercontent.com/broadinstitute/hailrunner/main/examples/vcf_to_mt.py",
  "hailrunner_run.script_args": [
    "--vcf", "gs://your-bucket/vcfs/chr20.vcf.gz",
    "--mt-output", "gs://your-bucket/output/chr20.mt"
  ],
  "hailrunner_run.workers": 16
}
```

### Batch VCF → MatrixTable ([`examples/vcf_to_mt_batch.py`](examples/vcf_to_mt_batch.py))

Convert many VCFs to MatrixTables in a single cluster session. Takes a TSV manifest with columns `vcf_path<tab>mt_output_path`. Existing MatrixTables are skipped by default (checks for `_SUCCESS` marker), so failed runs can be restarted without repeating completed work.

```json
{
  "hailrunner_run.script": "https://raw.githubusercontent.com/broadinstitute/hailrunner/main/examples/vcf_to_mt_batch.py",
  "hailrunner_run.script_args": [
    "--manifest", "gs://your-bucket/manifests/chr_all.tsv"
  ],
  "hailrunner_run.workers": 16,
  "hailrunner_run.preemptibles": 32
}
```

Manifest format (TSV, `#` comments allowed):

```
gs://your-bucket/vcfs/chr1.vcf.gz	gs://your-bucket/output/chr1.mt
gs://your-bucket/vcfs/chr2.vcf.gz	gs://your-bucket/output/chr2.mt
```

Pass `--overwrite` to force re-conversion of existing MatrixTables.

### Subset MatrixTable → VCF ([`examples/subset_mt.py`](examples/subset_mt.py))

Subset a MatrixTable by samples and/or genomic regions, then export as a bgzipped, tabix-indexed VCF.

```json
{
  "hailrunner_run.script": "https://raw.githubusercontent.com/broadinstitute/hailrunner/main/examples/subset_mt.py",
  "hailrunner_run.script_args": [
    "--mt", "gs://your-bucket/output/chr20.mt",
    "--vcf-output", "gs://your-bucket/subsets/chr20_subset.vcf.bgz",
    "--samples", "gs://your-bucket/samples/sample_ids.tsv"
  ],
  "hailrunner_run.workers": 16,
  "hailrunner_run.preemptibles": 32
}
```

The `--samples` file is a headerless TSV where the first column contains sample IDs. The optional `--regions` flag accepts comma-separated locus intervals (e.g. `chr1:1-1000000,chr2`).
