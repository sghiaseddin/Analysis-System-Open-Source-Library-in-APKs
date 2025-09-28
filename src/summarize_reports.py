#!/usr/bin/env python3
import argparse
import csv
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

def log(m): print(f"[{datetime.now().strftime('%H:%M:%S')}] {m}", flush=True)

# Columns in reports/<sha>.csv written by match_fingerprints_in_apks.py:
# app_sha256, repo_host, repo_path, repo_url, libarary_key, library_name,
# smali_prefix, fingerprint_type, class, class_file

SUMMARY_HEADERS = [
    "app_sha256",
    "repo_host",
    "repo_path",
    "repo_url",
    "libarary_key",
    "library_name",
    "smali_prefix",
    "fingerprint_types",   # unique types seen, joined by |
    "classes_matched",     # number of class hits (deduped)
    "sample_class",        # one example class
    "sample_class_file"    # its path in decoded apk
]

def summarize_one(report_csv: Path, out_dir: Path, force: bool = False) -> tuple[str, int]:
    sha = report_csv.stem
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / f"{sha}.csv"
    if out_csv.exists() and not force:
        return sha, -1  # skipped

    # aggregate by library identity
    # key = (repo_host, repo_path, libarary_key, library_name, smali_prefix)
    buckets = {}
    repo_url_by_key = {}

    with report_csv.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            key = (
                row.get("repo_host",""),
                row.get("repo_path",""),
                row.get("libarary_key",""),
                row.get("library_name",""),
                row.get("smali_prefix",""),
            )
            repo_url_by_key[key] = row.get("repo_url","")
            b = buckets.setdefault(key, {"types": set(), "classes": set(), "sample": None})
            b["types"].add(row.get("fingerprint_type",""))
            clazz = row.get("class","")
            cfile = row.get("class_file","")
            if clazz:
                b["classes"].add((clazz, cfile))
                if b["sample"] is None:
                    b["sample"] = (clazz, cfile)

    # write summary
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(SUMMARY_HEADERS)
        for (repo_host, repo_path, libkey, libname, smali_prefix), agg in buckets.items():
            types_join = "|".join(sorted(t for t in agg["types"] if t))
            classes_matched = len(agg["classes"])
            sample_class, sample_file = ("","")
            if agg["sample"]:
                sample_class, sample_file = agg["sample"]
            w.writerow([
                sha,
                repo_host,
                repo_path,
                repo_url_by_key.get((repo_host, repo_path, libkey, libname, smali_prefix), ""),
                libkey,
                libname,
                smali_prefix,
                types_join,
                classes_matched,
                sample_class,
                sample_file
            ])

    return sha, len(buckets)

def main():
    ap = argparse.ArgumentParser(description="Summarize per-app fingerprint matches to one row per library.")
    ap.add_argument("--input-dir", default="reports", help="Input directory with per-app detailed reports")
    ap.add_argument("--output-dir", default="summary-reports", help="Where to write per-app summaries")
    ap.add_argument("--limit-apps", type=int, default=0, help="Max apps to process (0 = all)")
    ap.add_argument("--workers", type=int, default=4, help="Parallel workers")
    ap.add_argument("--force", action="store_true", help="Overwrite existing summaries")
    ap.add_argument("--log-every", type=int, default=20, help="Progress logging frequency")
    args = ap.parse_args()

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    report_files = sorted(input_dir.glob("*.csv"))
    if args.limit_apps:
        report_files = report_files[:args.limit_apps]

    log(f"Summarizing {len(report_files)} report(s) -> {output_dir}")

    processed = 0
    written = 0
    skipped = 0

    def work(p: Path):
        return summarize_one(p, output_dir, force=args.force)

    if args.workers > 1:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futs = [ex.submit(work, p) for p in report_files]
            for i, fut in enumerate(as_completed(futs), 1):
                sha, n = fut.result()
                processed += 1
                if n == -1:
                    skipped += 1
                else:
                    written += 1
                if processed % args.log_every == 0:
                    log(f"Processed {processed}/{len(report_files)}  summaries written={written}  skipped={skipped}")
    else:
        for i, p in enumerate(report_files, 1):
            sha, n = work(p)
            processed += 1
            if n == -1:
                skipped += 1
            else:
                written += 1
            if processed % args.log_every == 0:
                log(f"Processed {processed}/{len(report_files)}  summaries written={written}  skipped={skipped}")

    log(f"Done. Summaries written={written}  skipped={skipped}  outdir={output_dir}")

if __name__ == "__main__":
    main()