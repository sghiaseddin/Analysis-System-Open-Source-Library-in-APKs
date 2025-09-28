import argparse
import csv
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

def parse_args():
    parser = argparse.ArgumentParser(description="Check license citation in decoded APKs")
    parser.add_argument("--input-dir", required=True, help="Path to directory containing summary CSVs")
    parser.add_argument("--input-dir2", required=True, help="Path to decoded apk")
    parser.add_argument("--workers", type=int, default=6, help="Number of worker threads (default: 6)")
    parser.add_argument("--log-every", type=int, default=10, help="Log progress every N repos (default: 10)")
    return parser.parse_args()

def repo_cited_in_decoded_apk(sha256, repo_url, decoded_dir):
    base_path = Path(decoded_dir) / sha256
    if not base_path.exists():
        return False
    # Walk through all files and directories inside base_path
    for root, dirs, files in os.walk(base_path):
        # Check directory names
        for d in dirs:
            if repo_url in d:
                return True
        # Check file names
        for f in files:
            if repo_url in f:
                return True
            # Also check file content if file is readable text
            file_path = Path(root) / f
            try:
                with open(file_path, "r", encoding="utf-8", errors="ignore") as file:
                    for line in file:
                        if repo_url in line:
                            return True
            except Exception:
                # Ignore files that cannot be read as text
                continue
    return False

def process_csv(csv_path, workers, log_every, decoded_dir):
    csv_path = Path(csv_path)
    rows = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        if "repo_url" not in fieldnames:
            print(f"Skipping {csv_path}: no 'repo_url' column found.")
            return
        for row in reader:
            rows.append(row)

    # Extract unique repo_urls
    repo_urls = list({row["repo_url"] for row in rows})

    # Map repo_url to cited status
    cited_map = {}

    def check_repo(repo_url):
        # The sha256 is assumed to be the file name without extension
        sha256 = csv_path.stem
        return repo_url, int(repo_cited_in_decoded_apk(sha256, repo_url, decoded_dir))

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(check_repo, repo_url): repo_url for repo_url in repo_urls}
        for i, future in enumerate(as_completed(futures), 1):
            repo_url, cited = future.result()
            cited_map[repo_url] = cited
            if i % log_every == 0:
                print(f"[{csv_path.name}] Processed {i}/{len(repo_urls)} repo_urls")

    # Add cited column next to repo_url
    new_fieldnames = []
    for fn in fieldnames:
        new_fieldnames.append(fn)
        if fn == "repo_url":
            new_fieldnames.append("cited")

    # Update rows with cited info
    for row in rows:
        row["cited"] = str(cited_map.get(row["repo_url"], 0))

    # Write back to CSV
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=new_fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

def main():
    args = parse_args()
    input_dir = Path(args.input_dir)
    if not input_dir.is_dir():
        print(f"Input directory {input_dir} does not exist or is not a directory.")
        return

    csv_files = list(input_dir.glob("*.csv"))
    if not csv_files:
        print(f"No CSV files found in {input_dir}")
        return

    decoded_dir = Path(args.input_dir2)
    if not decoded_dir.is_dir():
        print(f"Input directory {decoded_dir} does not exist or is not a directory.")
        return

    for csv_file in csv_files:
        print(f"Processing {csv_file.name} ...")
        process_csv(csv_file, args.workers, args.log_every, decoded_dir)

if __name__ == "__main__":
    main()