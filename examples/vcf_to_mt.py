import argparse
import json
import time

import hail as hl

def vcf_to_mt(vcf_path: str, mt_path: str, genome: str = "GRCh38") -> dict:
    t0 = time.time()
    mt = hl.import_vcf(vcf_path, reference_genome=genome, force_bgz=True)
    t_import = time.time() - t0

    t1 = time.time()
    mt.write(mt_path, overwrite=True)
    t_write = time.time() - t1

    mt2 = hl.read_matrix_table(mt_path)
    n_samples = mt2.count_cols()
    n_variants = mt2.count_rows()

    return {
        "n_samples": n_samples,
        "n_variants": n_variants,
        "import_s": t_import,
        "write_s": t_write,
        "total_s": time.time() - t0,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert a VCF to a Hail MatrixTable.")
    parser.add_argument("--vcf", required=True, help="GCS path to input VCF (bgzipped).")
    parser.add_argument("--mt-output", required=True, help="GCS path for output MatrixTable.")
    parser.add_argument("--genome", default="GRCh38", help="Reference genome (default: GRCh38).")
    args = parser.parse_args()

    hl.init()
    result = vcf_to_mt(args.vcf, args.mt_output, args.genome)
    print(json.dumps(result, indent=2))
