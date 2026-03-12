import argparse
import json
import sys
import time

import hail as hl


def _vcf_exists(path: str) -> bool:
    """Check if a VCF output already exists at path."""
    return hl.hadoop_exists(path)


def _read_manifest(path: str) -> list[tuple[str, str]]:
    """Read a TSV manifest of (mt_path, vcf_output_path) pairs."""
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


def _load_samples(samples_path: str) -> list[str]:
    """Load sample IDs from a headerless TSV (first column)."""
    samples = []
    with hl.hadoop_open(samples_path) as f:
        for line in f:
            field = line.strip().split("\t")[0].strip()
            if field:
                samples.append(field)
    assert samples, f"No samples found in {samples_path}"
    return samples


def subset_mt_batch(
    manifest_path: str,
    samples_path: str | None = None,
    regions: str | None = None,
    genome: str = "GRCh38",
    overwrite: bool = False,
) -> dict:
    t0 = time.time()
    pairs = _read_manifest(manifest_path)

    sample_set = None
    if samples_path is not None:
        samples = _load_samples(samples_path)
        sample_set = hl.literal(set(samples))
        print(f"Loaded {len(samples)} samples from {samples_path}", file=sys.stderr)

    intervals = None
    if regions is not None:
        intervals = [
            hl.parse_locus_interval(r, reference_genome=genome)
            for r in regions.split(",")
        ]
        print(f"Parsed {len(intervals)} region(s)", file=sys.stderr)

    results = []

    for mt_path, vcf_output in pairs:
        if not overwrite and _vcf_exists(vcf_output):
            print(f"SKIP (exists): {vcf_output}", file=sys.stderr)
            results.append({"mt": mt_path, "vcf": vcf_output, "status": "skipped"})
            continue

        print(f"START: {mt_path} -> {vcf_output}", file=sys.stderr)
        t1 = time.time()
        try:
            mt = hl.read_matrix_table(mt_path)

            if sample_set is not None:
                mt = mt.filter_cols(sample_set.contains(mt.s))

            if intervals is not None:
                mt = hl.filter_intervals(mt, intervals)

            hl.export_vcf(mt, vcf_output, tabix=True)
            n_samples = mt.count_cols()
            elapsed = time.time() - t1

            print(f"DONE:  {vcf_output} ({n_samples} samples, {elapsed:.0f}s)", file=sys.stderr)
            results.append({
                "mt": mt_path,
                "vcf": vcf_output,
                "status": "completed",
                "n_samples": n_samples,
                "seconds": elapsed,
            })
        except Exception as e:
            elapsed = time.time() - t1
            print(f"FAIL:  {vcf_output} ({elapsed:.0f}s): {e}", file=sys.stderr)
            results.append({
                "mt": mt_path,
                "vcf": vcf_output,
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
    parser = argparse.ArgumentParser(description="Batch subset MatrixTables and export as VCFs.")
    parser.add_argument("--manifest", required=True, help="GCS or local path to TSV: mt_path<tab>vcf_output_path")
    parser.add_argument("--samples", default=None, help="GCS path to headerless TSV; first column = sample IDs.")
    parser.add_argument("--regions", default=None, help="Comma-separated locus intervals (e.g. chr1:1-1000000,chr2).")
    parser.add_argument("--genome", default="GRCh38", help="Reference genome (default: GRCh38).")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing VCFs (default: skip).")
    args = parser.parse_args()

    hl.init()
    result = subset_mt_batch(args.manifest, args.samples, args.regions, args.genome, args.overwrite)
    print(json.dumps(result, indent=2))

    if result["failed"] > 0:
        sys.exit(1)
