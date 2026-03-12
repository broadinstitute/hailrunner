import argparse
import json
import time

import hail as hl


def subset_mt(
    mt_path: str,
    output_path: str,
    samples_path: str | None = None,
    regions: str | None = None,
    genome: str = "GRCh38",
) -> dict:
    t0 = time.time()
    mt = hl.read_matrix_table(mt_path)

    if samples_path is not None:
        t1 = time.time()
        samples = []
        with hl.hadoop_open(samples_path) as f:
            for line in f:
                field = line.strip().split("\t")[0].strip()
                if field:
                    samples.append(field)
        assert samples, f"No samples found in {samples_path}"
        sample_set = hl.literal(set(samples))
        mt = mt.filter_cols(sample_set.contains(mt.s))
        t_filter_samples = time.time() - t1
    else:
        t_filter_samples = 0.0

    if regions is not None:
        t2 = time.time()
        intervals = [
            hl.parse_locus_interval(r, reference_genome=genome)
            for r in regions.split(",")
        ]
        mt = hl.filter_intervals(mt, intervals)
        t_filter_regions = time.time() - t2
    else:
        t_filter_regions = 0.0

    t3 = time.time()
    hl.export_vcf(mt, output_path, tabix=True)
    t_export = time.time() - t3

    n_samples = mt.count_cols()

    return {
        "n_samples": n_samples,
        "filter_samples_s": t_filter_samples,
        "filter_regions_s": t_filter_regions,
        "export_s": t_export,
        "total_s": time.time() - t0,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Subset a MatrixTable and export as VCF.")
    parser.add_argument("--mt", required=True, help="GCS path to input MatrixTable.")
    parser.add_argument("--vcf-output", required=True, help="GCS path for output VCF (bgzipped).")
    parser.add_argument("--samples", default=None, help="GCS path to headerless TSV; first column = sample IDs.")
    parser.add_argument("--regions", default=None, help="Comma-separated locus intervals (e.g. chr1:1-1000000,chr2).")
    parser.add_argument("--genome", default="GRCh38", help="Reference genome (default: GRCh38).")
    args = parser.parse_args()

    hl.init()
    result = subset_mt(args.mt, args.vcf_output, args.samples, args.regions, args.genome)
    print(json.dumps(result, indent=2))
