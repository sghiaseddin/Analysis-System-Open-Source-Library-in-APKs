import argparse
import csv
from pathlib import Path
from datetime import datetime

def log(m): print(f"[{datetime.now().strftime('%H:%M:%S')}] {m}", flush=True)

def aggregate_one(summary_path: Path, license_lists_path, output_dir: Path, force=False):
    pkg_name = summary_path.stem
    license_path = license_lists_path / summary_path.name
    output_path = output_dir / summary_path.name

    if output_path.exists() and not force:
        return pkg_name, -1

    summary_rows = []
    summary_urls = set()

    if summary_path.exists():
        with open(summary_path, newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                row["origin"] = "fingerprints"
                summary_urls.add(row["repo_url"].strip())
                summary_rows.append(row)

    if license_path.exists():
        with open(license_path, newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = row.get("repo_url") or row.get("license_url") or ""
                url = url.strip()
                if not url:
                    continue

                if url in summary_urls:
                    # Update existing summary_rows entries for this repo_url to mark license exposure
                    for s in summary_rows:
                        try:
                            if s.get("repo_url", "").strip() == url:
                                orig = s.get("origin", "") or ""
                                parts = set([p.strip() for p in orig.split(",") if p.strip()])
                                parts.add("license_exposure")
                                parts.add("fingerprints")
                                s["origin"] = ",".join(sorted(parts))
                                s["cited"] = "1"
                        except Exception:
                            # defensive: skip malformed rows
                            continue
                    # already merged into summary_rows, do not append a separate license-only row
                    continue
                summary_rows.append({
                    "pkg_name": pkg_name,
                    "repo_host": "",  # Can be parsed if needed
                    "repo_path": "",
                    "repo_url": url,
                    "cited": "1",
                    "origin": "license_exposure",
                    "libarary_key": row.get("libarary_key", ""),
                    "library_name": row.get("libarary_key", ""),
                    "smali_prefix": "",
                    "fingerprint_types": row.get("found_by", ""),
                    "classes_matched": "",
                    "sample_class": "",
                    "sample_class_file": row.get("file_path", "")
                })

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            "pkg_name", "repo_host", "repo_path", "repo_url", "cited", "origin",
            "libarary_key", "library_name", "smali_prefix", "fingerprint_types",
            "classes_matched", "sample_class", "sample_class_file"
        ])
        writer.writeheader()
        writer.writerows(summary_rows)

    return pkg_name, len(summary_rows)


def main():
    ap = argparse.ArgumentParser(description="Aggregate license lists with summary report for each app.")
    ap.add_argument("--input-dir", default="summary-reports", help="Input directory with per-app summary reports")
    ap.add_argument("--input-dir2", default="license_lists", help="Input directory with per-app license lists")
    ap.add_argument("--output-dir", default="aggregate-reports", help="Where to write the reports")
    ap.add_argument("--limit", type=int, default=0, help="Max apps to process (0 = all)")
    ap.add_argument("--workers", type=int, default=4, help="Parallel workers")
    ap.add_argument("--force", action="store_true", help="Overwrite existing summaries")
    ap.add_argument("--log-every", type=int, default=20, help="Progress logging frequency")
    args = ap.parse_args()

    input_dir = Path(args.input_dir)
    license_lists_path = Path(args.input_dir2)
    output_dir = Path(args.output_dir)

    report_files = sorted(input_dir.glob("*.csv"))
    if args.limit:
        report_files = report_files[:args.limit]

    log(f"Aggregating {len(report_files)} report(s) -> {output_dir}")

    processed = 0
    written = 0
    skipped = 0

    def work(p: Path):
        return aggregate_one(p, license_lists_path, output_dir, force=args.force)

    if args.workers > 1:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futs = [ex.submit(work, p) for p in report_files]
            for i, fut in enumerate(as_completed(futs), 1):
                pkg_name, n = fut.result()
                processed += 1
                if n == -1:
                    skipped += 1
                else:
                    written += 1
                if processed % args.log_every == 0:
                    log(f"Processed {processed}/{len(report_files)}  aggregates written={written}  skipped={skipped}")
    else:
        for i, p in enumerate(report_files, 1):
            pkg_name, n = work(p)
            processed += 1
            if n == -1:
                skipped += 1
            else:
                written += 1
            if processed % args.log_every == 0:
                log(f"Processed {processed}/{len(report_files)}  aggregates written={written}  skipped={skipped}")

    log(f"Done. Aggregate written={written}  skipped={skipped}  outdir={output_dir}")

if __name__ == "__main__":
    main()