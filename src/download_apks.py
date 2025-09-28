#!/usr/bin/env python3
import argparse
import csv
import hashlib
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

import pandas as pd
import requests

DEFAULT_LOG_EVERY = 20
DEFAULT_THRESHOLD = 0.90
RESEARCH_CATEGORIES = []  # filled from CLI; lowercased tokens used for substring matching
CHUNK_SIZE = 1024 * 1024  # 1MB

def log(msg: str, *, flush=True):
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {msg}", flush=flush)

def parse_categories(cell: Any) -> Optional[Dict[str, float]]:
    if cell is None or (isinstance(cell, float) and pd.isna(cell)):
        return None
    try:
        data = json.loads(cell)
        # Coerce to float
        return {k: float(v) for k, v in data.items()}
    except Exception:
        return None

def row_is_eligible(row: pd.Series, threshold: float) -> bool:
    """Return True if row should be downloaded.
    If RESEARCH_CATEGORIES is non-empty, require that the app's assigned category
    matches any of the research tokens (case-insensitive substring match).
    Otherwise, fall back to old threshold logic (use parse_categories to get score dict).
    """
    # If research categories specified, prefer exact/category-string matching
    if RESEARCH_CATEGORIES:
        cell = row.get("categories")
        if cell is None:
            return False
        # If it's a plain string (e.g., 'FINANCE'), use it directly
        if isinstance(cell, str):
            cat_str = cell.strip().lower()
        else:
            # Try to parse JSON-like cell (old format)
            parsed = parse_categories(cell)
            # parsed could be a dict of scores or a dict with 'category' key
            if isinstance(parsed, dict):
                # If it contains a single string value under 'category', use it
                cat_val = parsed.get("category") or parsed.get("Category")
                if isinstance(cat_val, str):
                    cat_str = cat_val.strip().lower()
                else:
                    # Not a string category; fail-safe -> do not select
                    return False
            else:
                return False
        # Check any research token is substring of cat_str
        for token in RESEARCH_CATEGORIES:
            if token in cat_str:
                return True
        return False

    # Fallback: old behavior — JSON scores and threshold
    cats = parse_categories(row.get("categories"))
    if not cats:
        return False
    try:
        return max(cats.values()) >= threshold
    except Exception:
        return False

def safe_format(template: str, **kwargs) -> str:
    # Missing keys -> empty string
    class SafeDict(dict):
        def __missing__(self, key):
            return ""
    return template.format_map(SafeDict(**kwargs))

def download_apk(sha256: str, apikey: str, out_path: Path,
                 retries: int = 5, backoff_base: float = 0.5, verify_sha256: bool = False) -> bool:
    """Download a single APK by sha256 to out_path. Returns True if file is present/valid."""
    # Skip if exists
    if out_path.exists():
        return True

    url = "https://androzoo.uni.lu/api/download"
    params = {"apikey": apikey, "sha256": sha256}
    tmp_path = out_path.with_suffix(out_path.suffix + ".part")

    for attempt in range(retries):
        try:
            with requests.get(url, params=params, stream=True, timeout=60) as r:
                if r.status_code == 200:
                    total = int(r.headers.get("Content-Length", 0)) if r.headers.get("Content-Length") else None
                    downloaded = 0
                    h = hashlib.sha256() if verify_sha256 else None

                    with open(tmp_path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                            if not chunk:
                                continue
                            f.write(chunk)
                            downloaded += len(chunk)
                            if h:
                                h.update(chunk)

                    # Optional integrity check
                    if verify_sha256:
                        digest = h.hexdigest().lower()
                        if digest != sha256.lower():
                            tmp_path.unlink(missing_ok=True)
                            log(f"SHA256 mismatch for {sha256}: got {digest[:12]}..., expected {sha256[:12]}...")
                            return False

                    tmp_path.rename(out_path)
                    return True

                elif r.status_code in (429, 500, 502, 503, 504):
                    sleep_s = backoff_base * (2 ** attempt)
                    log(f"HTTP {r.status_code} for {sha256}, retry in {sleep_s:.1f}s...")
                    time.sleep(sleep_s)
                else:
                    log(f"HTTP {r.status_code} for {sha256}: {r.text[:200]}")
                    return False
        except Exception as e:
            sleep_s = backoff_base * (2 ** attempt)
            log(f"Request error for {sha256}: {e} — retry in {sleep_s:.1f}s")
            time.sleep(sleep_s)

    log(f"Failed to download {sha256} after retries.")
    return False

def main():
    ap = argparse.ArgumentParser(description="Download APKs from AndroZoo by sha256 based on tagged_apps.csv.")
    ap.add_argument("--input-data", required=True, help="CSV with categories JSON column (e.g., tagged_apps.csv)")
    ap.add_argument("--output-dir", default="apks", help="Directory to store APKs (default: ./apks)")
    ap.add_argument("--apikey", default=os.getenv("ANDROZOO_APIKEY") or os.getenv("APIKEY"),
                    help="AndroZoo API key (or set ANDROZOO_APIKEY/APIKEY env var)")
    ap.add_argument("--limit", type=int, default=100, help="Max rows/APKs to process (default 100)")
    ap.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD,
                    help=f"Min category score to qualify (default {DEFAULT_THRESHOLD})")
    ap.add_argument("--filename-template", default="{sha256}.apk",
                    help="Output filename template. Fields: {sha256},{pkg_name},{vercode} (default: {sha256}.apk)")
    ap.add_argument("--force", action="store_true", help="Overwrite existing files")
    ap.add_argument("--verify-sha256", action="store_true", help="Hash downloaded file and verify sha256")
    ap.add_argument("--log-every", type=int, default=DEFAULT_LOG_EVERY, help="Log every N downloads (default 20)")
    ap.add_argument("--research-categories", default=None, help="Comma-separated list of research category tokens to restrict downloads (case-insensitive substrings)")
    args = ap.parse_args()

    if args.research_categories:
        RESEARCH_CATEGORIES[:] = [s.strip().lower() for s in args.research_categories.split(",") if s.strip()]

    if not args.apikey:
        print("ERROR: Provide AndroZoo API key via --apikey or ANDROZOO_APIKEY/APIKEY env var.", file=sys.stderr)
        sys.exit(1)

    outdir = Path(args.output_dir)
    outdir.mkdir(parents=True, exist_ok=True)

    # stream CSV
    total_seen = 0
    attempted = 0
    done = 0
    skipped = 0
    exists = 0

    # We need at least sha256 + categories; pkg_name/vercode optional for filename template
    reader = pd.read_csv(args.input_data, chunksize=100_000, low_memory=True)

    try:
        for chunk in reader:
            if chunk.empty:
                continue

            for _, row in chunk.iterrows():
                # Limit
                if args.limit and attempted >= args.limit:
                    raise StopIteration

                total_seen += 1

                # Category eligibility check
                if not row_is_eligible(row, args.threshold):
                    skipped += 1
                    continue

                sha256 = str(row.get("sha256") or row.get("SHA256") or "").strip()
                if not sha256:
                    skipped += 1
                    continue

                pkg_name = str(row.get("pkg_name", "") or "")
                vercode = str(row.get("vercode", "") or "")

                filename = safe_format(args.filename_template,
                                       sha256=sha256, pkg_name=pkg_name, vercode=vercode)
                if not filename:
                    filename = f"{sha256}.apk"

                out_path = outdir / filename

                if out_path.exists() and not args.force:
                    exists += 1
                    attempted += 1
                    if attempted % args.log_every == 0:
                        log(f"Attempted={attempted}  Downloaded={done}  Exists={exists}  Skipped={skipped}")
                    continue
                elif out_path.exists() and args.force:
                    try:
                        out_path.unlink()
                    except Exception:
                        pass  # we’ll overwrite via .part anyway

                ok = download_apk(sha256, args.apikey, out_path, verify_sha256=args.verify_sha256)
                attempted += 1
                if ok:
                    done += 1

                if attempted % args.log_every == 0:
                    log(f"Attempted={attempted}  Downloaded={done}  Exists={exists}  Skipped={skipped}")

    except StopIteration:
        pass

    log(f"Finished. Seen={total_seen}  Attempted={attempted}  Downloaded={done}  Exists={exists}  Skipped={skipped}")
    log(f"APK directory: {outdir.resolve()}")

if __name__ == "__main__":
    main()