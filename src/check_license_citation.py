import argparse
import csv
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
from datetime import datetime

def log(m): print(f"[{datetime.now().strftime('%H:%M:%S')}] {m}", flush=True)

def parse_args():
    parser = argparse.ArgumentParser(description="Check license citation in decoded APKs")
    parser.add_argument("--input-dir", required=True, help="Path to directory containing summary CSVs")
    parser.add_argument("--input-dir2", required=True, help="Path to decoded apk")
    parser.add_argument("--workers", type=int, default=6, help="Number of worker threads (default: 6)")
    parser.add_argument("--log-every", type=int, default=10, help="Log progress every N repos (default: 10)")
    return parser.parse_args()

def extract_all_urls(decoded_path):
    url_pattern = re.compile(
        r"(?:(?:https?|ftp):\/\/)?(?:[\w.-]+(?:\.[\w\.-]+)+)(?:\/[\w\-\._~:/?#[\]@!$&'()*+,;%=]*)?",
        re.IGNORECASE
    )
    all_urls = set()
    for root, dirs, files in os.walk(decoded_path):
        for file in files:
            try:
                with open(Path(root) / file, "r", encoding="utf-8", errors="ignore") as f:
                    for line in f:
                        matches = url_pattern.findall(line)
                        all_urls.update(matches)
            except Exception:
                continue
    return all_urls

def is_url_cited(repo_url, all_urls):
    stripped_url = re.sub(r"^https?://", "", repo_url)
    for u in all_urls:
        if stripped_url in u or u in repo_url:
            return True
    return False

def repo_cited_in_decoded_apk(pkg_name, repo_url, decoded_dir, url_pattern):
    # This function is no longer used as per the new logic
    return False

def process_csv(csv_path, workers, log_every, decoded_dir):
    csv_path = Path(csv_path)
    rows = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        if "repo_url" not in fieldnames:
            return
        for row in reader:
            rows.append(row)

    url_pattern = re.compile(
        r"(?:(?:https?|ftp):\/\/)?(?:[\w.-]+(?:\.[\w\.-]+)+)(?:\/[\w\-\._~:/?#[\]@!$&'()*+,;%=]*)?",
        re.IGNORECASE
    )

    # Extract unique repo_urls that are not already cited
    repo_urls = list({row["repo_url"] for row in rows if not row.get("cited") or row.get("cited") == "0"})

    # Map repo_url to cited status
    cited_map = {}

    # Pre-extract all URLs for each decoded apk (pkg_name)
    pkg_name = csv_path.stem
    decoded_path = Path(decoded_dir) / pkg_name
    all_urls = extract_all_urls(decoded_path)

    def check_repo(repo_url):
        return repo_url, int(is_url_cited(repo_url, all_urls))

    for i, repo_url in enumerate(repo_urls, 1):
        cited = int(is_url_cited(repo_url, all_urls))
        cited_map[repo_url] = cited
        if i % log_every == 0:
            pass
            # log(f"[{csv_path.name}] Processed {i}/{len(repo_urls)} repo_urls")

    # Ensure we don't duplicate an existing 'cited' column: drop it if present
    fieldnames = [fn for fn in fieldnames if fn != "cited"]

    # Add cited column next to repo_url
    new_fieldnames = []
    for fn in fieldnames:
        new_fieldnames.append(fn)
        if fn == "repo_url":
            new_fieldnames.append("cited")

    # Update rows with cited info
    for row in rows:
        if row.get("cited") and row.get("cited") == "1":
            continue
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
        log(f"Input directory {input_dir} does not exist or is not a directory.")
        return

    csv_files = list(input_dir.glob("*.csv"))
    if not csv_files:
        log(f"No CSV files found in {input_dir}")
        return

    decoded_dir = Path(args.input_dir2)
    if not decoded_dir.is_dir():
        log(f"Input directory {decoded_dir} does not exist or is not a directory.")
        return

    for csv_file in csv_files:
        log(f"Processing {csv_file.name} ...")
        process_csv(csv_file, args.workers, args.log_every, decoded_dir)

if __name__ == "__main__":
    main()