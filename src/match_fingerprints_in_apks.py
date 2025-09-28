#!/usr/bin/env python3
import argparse
import csv
import os
import re
import sys
import json
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

CLASS_RE = re.compile(r'^\s*\.class\s+[^\s]+\s+([^\s]+)')  # captures e.g. Lcom/foo/Bar;

def log(msg: str, *, flush=True):
    print(f"[{datetime.now().strftime("%H:%M:%S")}] {msg}", flush=flush)

# ---------- Fingerprints ----------
def load_fingerprints(fp_csv: Path):
    """
    Returns:
      prefix_map: dict[smali_prefix] -> list[fprow]
      fprow fields: (repo_host, repo_path, repo_url, libarary_key, library_name, smali_prefix, fingerprint_type, repo_file_path)
    """
    prefix_map = {}
    with fp_csv.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            pref = (row.get("smali_prefix") or "").strip()
            if not pref or not pref.startswith("L") or not pref.endswith("/"):
                continue  # only accept normalized smali-style prefixes
            key = pref
            entry = (
                row.get("repo_host",""),
                row.get("repo_path",""),
                row.get("repo_url",""),
                row.get("libarary_key",""),
                row.get("library_name",""),
                pref,
                row.get("fingerprint_type",""),
                row.get("repo_file_path",""),
            )
            prefix_map.setdefault(key, []).append(entry)
    return prefix_map

# ---------- Class indexing ----------
def index_classes_for_app(app_dir: Path):
    """
    Walk smali* folders and extract .class declarations (first 20 lines).
    Returns list of dicts: {"class": "Lcom/foo/Bar;", "path": ".../smali*/com/foo/Bar.smali"}
    """
    results = []
    for smali_root in app_dir.rglob("smali*"):
        if not smali_root.is_dir():
            continue
        for smali in smali_root.rglob("*.smali"):
            try:
                with smali.open("r", encoding="utf-8", errors="ignore") as f:
                    for _ in range(20):
                        line = f.readline()
                        if not line:
                            break
                        m = CLASS_RE.match(line)
                        if m:
                            results.append({"class": m.group(1), "path": str(smali)})
                            break
            except Exception:
                pass
    return results

def load_or_build_class_index(app_dir: Path, index_dir: Path, force: bool = False):
    """
    Uses classes_index/<sha256>.json if present (and not --force). Otherwise builds and writes it.
    """
    sha = app_dir.name
    index_dir.mkdir(parents=True, exist_ok=True)
    out_json = index_dir / f"{sha}.json"
    if out_json.exists() and not force:
        try:
            return json.loads(out_json.read_text(encoding="utf-8"))
        except Exception:
            pass
    classes = index_classes_for_app(app_dir)
    out_json.write_text(json.dumps(classes, ensure_ascii=False), encoding="utf-8")
    return classes

# ---------- Matching ----------
def all_prefixes_for_class(class_desc: str):
    """
    For Lcom/foo/bar/Baz; yield:
      Lcom/
      Lcom/foo/
      Lcom/foo/bar/
    """
    if not (class_desc.startswith("L") and class_desc.endswith(";")):
        return []
    # strip class name to package path
    # Lcom/foo/bar/Baz; -> Lcom/foo/bar/
    body = class_desc[1:-1]
    if "/" not in body:
        return []  # default package — ignore
    parts = body.split("/")
    acc = ["L"]
    prefixes = []
    for i, part in enumerate(parts[:-1]):  # exclude class tail
        if i == 0:  # first is 'com'
            acc[0] = "L" + part + "/"
            prefixes.append(acc[0])
        else:
            acc[0] = acc[0] + part + "/"
            prefixes.append(acc[0])
    return prefixes

def match_app(prefix_map, app_dir: Path, classes_index_dir: Path, out_dir: Path, force: bool = False):
    sha = app_dir.name
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / f"{sha}.csv"
    if out_csv.exists() and not force:
        return "exists", 0

    classes = load_or_build_class_index(app_dir, classes_index_dir, force=False)
    if not classes:
        out_csv.write_text("")  # empty marker
        return "no_classes", 0

    # Prepare write
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "app_sha256", "repo_host", "repo_path", "repo_url",
            "libarary_key", "library_name",
            "smali_prefix", "fingerprint_type",
            "class", "class_file"
        ])

        emitted = set()
        count = 0
        for c in classes:
            clazz = c["class"]  # e.g., Lcom/foo/bar/Baz;
            cpath = c["path"]
            for pref in all_prefixes_for_class(clazz):
                if pref in prefix_map:
                    # for each fingerprint that matches this prefix, emit
                    for (repo_host, repo_path, repo_url, libkey, libname, smali_prefix, fptype, repo_fp_path) in prefix_map[pref]:
                        key = (clazz, smali_prefix, repo_host, repo_path)
                        if key in emitted:
                            continue
                        w.writerow([sha, repo_host, repo_path, repo_url, libkey, libname, smali_prefix, fptype, clazz, cpath])
                        emitted.add(key)
                        count += 1

    return "ok", count

# ---------- Main ----------
def main():
    ap = argparse.ArgumentParser(description="Match fingerprints.csv against decoded APKs and write per-app reports.")
    ap.add_argument("--input-dir", default="decoded", help="decoded/<sha256> directories")
    ap.add_argument("--input-data", default="fingerprints.csv", help="fingerprints.csv generated from repos")
    ap.add_argument("--output-dir", default="classes_index", help="where to cache/read per-app class indices")
    ap.add_argument("--output-dir2", default="reports", help="output folder for per-app report CSVs")
    ap.add_argument("--limit-apps", type=int, default=0, help="limit number of apps processed")
    ap.add_argument("--workers", type=int, default=4, help="parallel workers across apps")
    ap.add_argument("--force", action="store_true", help="overwrite existing per-app report if present")
    ap.add_argument("--log-every", type=int, default=20, help="progress logging frequency")
    args = ap.parse_args()

    input_dir = Path(args.input_dir)
    if not input_dir.exists():
        log(f"ERROR: decoded dir not found: {input_dir}")
        sys.exit(1)

    input_data = Path(args.input_data)
    if not input_data.exists():
        log(f"ERROR: fingerprints.csv not found: {input_data}")
        sys.exit(1)

    prefix_map = load_fingerprints(input_data)
    if not prefix_map:
        log("ERROR: No usable fingerprints loaded (check smali_prefix format).")
        sys.exit(1)

    apps = [p for p in sorted(input_dir.iterdir()) if p.is_dir()]
    if args.limit_apps:
        apps = apps[:args.limit_apps]

    class_dir = Path(args.output_dir)
    report_dir = Path(args.output_dir2)

    log(f"Apps: {len(apps)} | Fingerprint prefixes: {len(prefix_map)} | Output: {report_dir}")

    processed = 0
    matched_rows = 0

    def work(app_dir):
        status, n = match_app(prefix_map, app_dir, class_dir, report_dir, force=args.force)
        return (app_dir.name, status, n)

    results = []
    if args.workers > 1:
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futs = [ex.submit(work, a) for a in apps]
            for i, fut in enumerate(as_completed(futs), 1):
                sha, status, n = fut.result()
                results.append((sha, status, n))
                processed += 1
                matched_rows += n
                if processed % args.log_every == 0:
                    log(f"Processed {processed}/{len(apps)} | matches so far: {matched_rows}")
    else:
        for i, a in enumerate(apps, 1):
            sha, status, n = work(a)
            results.append((sha, status, n))
            processed += 1
            matched_rows += n
            if processed % args.log_every == 0:
                log(f"Processed {processed}/{len(apps)} | matches so far: {matched_rows}")

    # Summary
    ok = sum(1 for _, s, _ in results if s == "ok")
    exists = sum(1 for _, s, _ in results if s == "exists")
    empty = sum(1 for _, s, _ in results if s in ("no_classes",))
    log(f"Done. Reports: ok={ok}, exists={exists}, empty={empty}. Total matches written: {matched_rows}. Output dir: {report_dir}")

if __name__ == "__main__":
    main()