#!/usr/bin/env python3
import argparse
import pandas as pd
from datetime import datetime
import sys
import os

def parse_args():
    p = argparse.ArgumentParser(
        description="Extract latest APK row per pkg_name from a huge CSV, restricted to play.google.com"
    )
    p.add_argument("--input-data", required=True, help="Path to the input CSV (e.g., latest_with-added-date.csv)")
    p.add_argument("--output-data", required=True, help="Path to the output CSV")
    p.add_argument("--chunksize", type=int, default=300_000, help="Rows per chunk (default: 300k)")
    p.add_argument("--log-every", type=int, default=1_000_000, help="Log progress every N input rows")
    return p.parse_args()

def main():
    args = parse_args()

    if not os.path.exists(args.input_data):
        print(f"ERROR: Input not found: {args.input_data}", file=sys.stderr)
        sys.exit(1)

    # Dictionary holding the best (latest 'added') row per pkg_name.
    # Map: pkg_name -> (added_ts, row_as_dict)
    best_rows = {}

    total_rows = 0
    kept_rows = 0

    # If you want to limit columns for memory savings, define them here; otherwise None reads all.
    # Example (uncomment to use): 
    # usecols = ["sha256","sha1","md5","dex_date","apk_size","pkg_name","vercode",
    #            "vt_detection","vt_scan_date","dex_size","added","markets"]
    usecols = None

    # We’ll parse 'added' per chunk (faster/safer for messy data).
    reader = pd.read_csv(
        args.input_data,
        chunksize=args.chunksize,
        usecols=usecols,
        low_memory=True
    )

    start_time = datetime.now()
    print(f"[{start_time:%Y-%m-%d %H:%M:%S}] Start processing...", flush=True)

    for i, chunk in enumerate(reader, start=1):
        rows_in_chunk = len(chunk)
        total_rows += rows_in_chunk

        # Filter to play.google.com first to reduce work
        # markets could be a list or a string; we use 'contains'
        play_mask = chunk["markets"].astype(str).str.contains("play.google.com", na=False)
        filtered = chunk.loc[play_mask].copy()

        if filtered.empty:
            if total_rows % args.log_every < rows_in_chunk:
                now = datetime.now()
                print(f"[{now:%H:%M:%S}] Processed {total_rows:,} rows | kept {kept_rows:,} so far | unique packages {len(best_rows):,}", flush=True)
            continue

        # Parse 'added' into datetime (coerce invalid to NaT and drop them)
        filtered["added"] = pd.to_datetime(filtered["added"], errors="coerce")
        filtered = filtered.dropna(subset=["added"])

        if filtered.empty:
            if total_rows % args.log_every < rows_in_chunk:
                now = datetime.now()
                print(f"[{now:%H:%M:%S}] Processed {total_rows:,} rows | kept {kept_rows:,} so far | unique packages {len(best_rows):,}", flush=True)
            continue

        # Within this chunk, pick the latest 'added' per pkg_name
        # Use idxmax on 'added' to find the row index per group
        idx = filtered.groupby("pkg_name", sort=False)["added"].idxmax()
        winners = filtered.loc[idx]

        # Merge with global best_rows
        for _, row in winners.iterrows():
            pkg = row["pkg_name"]
            added_ts = row["added"].to_datetime64()

            prev = best_rows.get(pkg)
            if prev is None or added_ts > prev[0]:
                # Store as dict to avoid holding Pandas objects
                best_rows[pkg] = (added_ts, row.to_dict())
                kept_rows += 1

        # Periodic logging
        if total_rows % args.log_every < rows_in_chunk:
            now = datetime.now()
            print(f"[{now:%H:%M:%S}] Processed {total_rows:,} rows | kept {kept_rows:,} (updates) | unique packages {len(best_rows):,}", flush=True)

    # Build final DataFrame from dict values and write once
    if best_rows:
        out_df = pd.DataFrame([v[1] for v in best_rows.values()])

        # Ensure 'added' is ISO-like string (not numpy datetime64) for CSV
        if "added" in out_df.columns:
            out_df["added"] = pd.to_datetime(out_df["added"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S.%f")

        # Optional: sort by added desc or pkg_name asc for readability
        sort_cols = [c for c in ("added", "pkg_name") if c in out_df.columns]
        if sort_cols:
            # If 'added' exists, sort by it desc, then pkg_name asc
            if "added" in sort_cols:
                out_df = out_df.sort_values(by=["added", "pkg_name"] if "pkg_name" in sort_cols else ["added"],
                                            ascending=[False, True] if "pkg_name" in sort_cols else [False])

        out_df.to_csv(args.output_data, index=False)
        end_time = datetime.now()
        print(f"[{end_time:%Y-%m-%d %H:%M:%S}] Done. Processed {total_rows:,} rows total.", flush=True)
        print(f"Unique packages: {len(best_rows):,}")
        print(f"Wrote: {args.output_data} ({len(out_df):,} rows)")
    else:
        print("No rows matched play.google.com after processing.", flush=True)

if __name__ == "__main__":
    main()