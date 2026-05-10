import argparse
import csv
from pathlib import Path
from datetime import datetime


def log(m):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {m}", flush=True)


def parse_args():
    parser = argparse.ArgumentParser(description="Merge aggregate report CSVs into one analysis dataset")
    parser.add_argument("--input-dir", required=True, help="Path to directory containing aggregate report CSVs")
    parser.add_argument("--output-dir", required=True, help="Path to directory where merged dataset will be written")
    parser.add_argument("--limit", required=True, help="Dataset limit/name suffix used in output filename")
    return parser.parse_args()


def merge_csv_files(input_dir, output_file):
    csv_files = sorted(input_dir.glob("*.csv"))

    if not csv_files:
        log(f"No CSV files found in {input_dir}")
        return False

    all_fieldnames = []
    rows = []

    for csv_file in csv_files:
        log(f"Reading {csv_file.name} ...")

        with open(csv_file, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames

            if not fieldnames:
                log(f"Skipping {csv_file.name}: empty or invalid CSV")
                continue

            for fieldname in fieldnames:
                if fieldname not in all_fieldnames:
                    all_fieldnames.append(fieldname)

            for row in reader:
                rows.append(row)

    if not rows:
        log("No rows found to merge.")
        return False

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_fieldnames)
        writer.writeheader()

        for row in rows:
            writer.writerow(row)

    log(f"Merged {len(csv_files)} CSV files into {output_file}")
    log(f"Total rows written: {len(rows)}")
    return True


def main():
    args = parse_args()

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    limit = Path(args.limit)

    if not input_dir.is_dir():
        log(f"Input directory {input_dir} does not exist or is not a directory.")
        return

    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"dataset_{limit}.csv"

    merge_csv_files(input_dir, output_file)


if __name__ == "__main__":
    main()
