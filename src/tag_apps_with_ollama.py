#!/usr/bin/env python3
import argparse
import csv
import json
import os
import re
import sys
import time
from datetime import datetime
from html import unescape
from pathlib import Path
from typing import Dict, Any, Optional

import pandas as pd
import requests
import subprocess

# -------- Configurable defaults --------
DEFAULT_CHUNK = 200_000
DEFAULT_LOG_EVERY = 100
DEFAULT_OLLAMA_MODEL = "llama3.1"   # or "mistral", "qwen2.5", etc.
ANDROZOO_URL_TMPL = "https://androzoo.uni.lu/api/get_gp_metadata/{pkg_name}"

ALL_CATEGORIES = [
    "ART_AND_DESIGN", "AUTO_AND_VEHICLES", "ANDROID_WEAR", "BEAUTY",
    "BOOKS_AND_REFERENCE", "BUSINESS", "COMICS", "COMMUNICATION",
    "DATING", "EDUCATION", "ENTERTAINMENT", "EVENTS", "FINANCE",
    "FOOD_AND_DRINK", "HEALTH_AND_FITNESS", "HOUSE_AND_HOME",
    "LIBRARIES_AND_DEMO", "LIFESTYLE", "MAPS_AND_NAVIGATION",
    "MEDICAL", "MUSIC_AND_AUDIO", "NEWS_AND_MAGAZINES", "PARENTING",
    "PERSONALIZATION", "PHOTOGRAPHY", "PRODUCTIVITY", "SHOPPING",
    "SOCIAL", "SPORTS", "TOOLS", "TRAVEL_AND_LOCAL", "VIDEO_PLAYERS",
    "WATCH_FACE", "WEATHER", "GAME", "GAME_ACTION", "GAME_ADVENTURE",
    "GAME_ARCADE", "GAME_BOARD", "GAME_CARD", "GAME_CASINO",
    "GAME_CASUAL", "GAME_EDUCATIONAL", "GAME_MUSIC", "GAME_PUZZLE",
    "GAME_RACING", "GAME_ROLE_PLAYING", "GAME_SIMULATION", "GAME_SPORTS",
    "GAME_STRATEGY", "GAME_TRIVIA", "GAME_WORD", "FAMILY"
]

CATEGORIES = []

# -------- Utilities --------

def log(msg: str, *, flush=True):
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {msg}", flush=flush)

def strip_html(html_text: str) -> str:
    # very lightweight HTML -> text
    txt = re.sub(r"<br\s*/?>", "\n", html_text, flags=re.I)
    txt = re.sub(r"<[^>]+>", " ", txt)      # strip tags
    txt = unescape(txt)
    # normalize whitespace
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt

def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)

def ollama_classify(desc: str, model: str = DEFAULT_OLLAMA_MODEL,
                    use_http: bool = True, host: str = "http://localhost:11434") -> Optional[Dict[str, float]]:
    """
    Calls Ollama locally and returns a dict of category->probability.
    Tries HTTP API first (if use_http), else falls back to `ollama run`.
    """
    cat_list = ALL_CATEGORIES
    categories_str = "\n".join(f"- {c}" for c in cat_list)
    prompt_template = (
        "You are a classifier. You get an Android app description.\n"
        "Pick the single most appropriate category from the following list (based on Google Play taxonomy):\n\n"
        f"{categories_str}\n\n"
        "Rules:\n"
        "- Only return a JSON string with a single key: 'category'.\n"
        "- The value must be one of the above categories.\n"
        "- No explanations or extra content.\n"
        'Description:\n"""{desc}"""'
    )

    prompt = prompt_template.format(desc=desc[:7000])  # keep prompt reasonable

    # Try HTTP API (preferred)
    if use_http:
        try:
            r = requests.post(
                f"{host}/api/generate",
                json={"model": model, "prompt": prompt, "stream": False},
                timeout=60,
            )
            r.raise_for_status()
            out = r.json().get("response", "")
            return parse_categories_json(out)
        except Exception as e:
            log(f"Ollama HTTP failed ({e}); falling back to CLI...")

    # Fallback to `ollama run`
    try:
        proc = subprocess.run(
            ["ollama", "run", model],
            input=prompt.encode("utf-8"),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=90,
        )
        out = proc.stdout.decode("utf-8", errors="ignore")
        return parse_categories_json(out)
    except Exception as e:
        log(f"Ollama CLI failed: {e}")
        return None

def parse_categories_json(txt: str) -> Optional[Dict[str, float]]:
    # Extract the first JSON object in the output containing the research_fit dictionary
    m = re.search(r"\{.*?\}", txt.strip(), flags=re.S)
    if not m:
        return None
    try:
        data = json.loads(m.group(0))
        cat = data.get("category", "").strip().upper()
        return {"category": cat} if cat in ALL_CATEGORIES else None
    except Exception:
        return None

def fetch_gp_metadata(pkg_name: str, apikey: str, cache_dir: Path, force: bool = False,
                      backoff_base: float = 0.5, max_retries: int = 5) -> Optional[Any]:
    """
    Fetches AndroZoo GP metadata for a package and caches it as JSON.
    Returns the parsed JSON (list of attempts) or None.
    """
    ensure_dir(cache_dir)
    cache_path = cache_dir / f"{pkg_name}.json"
    if cache_path.exists() and not force:
        try:
            return json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception:
            # corrupt? refetch
            pass

    url = ANDROZOO_URL_TMPL.format(pkg_name=pkg_name)
    params = {"apikey": apikey}
    for attempt in range(max_retries):
        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code == 200:
                # Some endpoints return text "None" for missing
                if r.text.strip().lower() == "none":
                    cache_path.write_text("null", encoding="utf-8")
                    return None
                data = r.json()
                cache_path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
                return data
            elif r.status_code in (429, 500, 502, 503, 504):
                sleep_s = backoff_base * (2 ** attempt)
                log(f"HTTP {r.status_code} for {pkg_name}, retrying in {sleep_s:.1f}s...")
                time.sleep(sleep_s)
            else:
                log(f"HTTP {r.status_code} for {pkg_name}: {r.text[:200]}")
                cache_path.write_text("null", encoding="utf-8")
                return None
        except Exception as e:
            sleep_s = backoff_base * (2 ** attempt)
            log(f"Request error for {pkg_name}: {e} — retry in {sleep_s:.1f}s")
            time.sleep(sleep_s)

    log(f"Failed to fetch metadata for {pkg_name} after retries.")
    return None

def latest_description_from_attempts(attempts_json: Any) -> Optional[str]:
    """
    attempts_json is a list of objects; we choose the LAST item and read 'descriptionHtml'.
    """
    if not attempts_json or not isinstance(attempts_json, list):
        return None
    last = attempts_json[-1]
    desc_html = last.get("descriptionHtml") if isinstance(last, dict) else None
    if not desc_html:
        return None
    return strip_html(desc_html)

def row_passes_threshold(categories: Dict[str, float], threshold: float = 0.90) -> bool:
    return max(categories.values()) >= threshold

# -------- Main processing --------

def main():
    ap = argparse.ArgumentParser(description="Tag APKs with categories using AndroZoo metadata + Ollama (streaming, resumable).")
    ap.add_argument("--input-data", required=True, help="Input CSV (e.g., latest_playstore_per_pkg.csv)")
    ap.add_argument("--output-data", required=True, help="Output CSV path")
    ap.add_argument("--limit", type=int, default=100, help="Max rows to process (default 100)")
    ap.add_argument("--chunksize", type=int, default=DEFAULT_CHUNK, help="Pandas chunk size (default 200k)")
    ap.add_argument("--log-every", type=int, default=DEFAULT_LOG_EVERY, help="Log progress every N processed rows (default 1000)")
    ap.add_argument("--apikey", default=os.getenv("ANDROZOO_APIKEY") or os.getenv("APIKEY"),
                    help="AndroZoo API key (or set ANDROZOO_APIKEY/APIKEY env var)")
    ap.add_argument("--output-dir", default="gp_meta", help="Directory to save GP metadata JSON files (default ./gp_meta)")
    ap.add_argument("--ollama-model", default=DEFAULT_OLLAMA_MODEL, help="Ollama model name (default: llama3.1)")
    ap.add_argument("--ollama-endpoint", default="http://localhost:11434", help="Base URL for Ollama HTTP API")
    ap.add_argument("--no-http", action="store_true", help="Disable Ollama HTTP API and use CLI fallback only")
    ap.add_argument("--force-refresh", action="store_true", help="Re-download GP metadata even if cached JSON exists")
    ap.add_argument("--require-play", action="store_true", help="Skip rows where 'markets' does NOT contain play.google.com")
    ap.add_argument("--threshold", type=float, default=0.90, help="Keep rows whose max category ≥ threshold (default 0.90)")
    ap.add_argument("--research-categories", default=None, help="Comma-separated list of research categories to classify")
    args = ap.parse_args()

    global CATEGORIES
    if args.research_categories:
        CATEGORIES = [x.strip() for x in args.research_categories.split(",") if x.strip()]

    if not args.apikey:
        print("ERROR: Provide AndroZoo API key via --apikey or ANDROZOO_APIKEY/APIKEY env var.", file=sys.stderr)
        sys.exit(1)

    cache_dir = Path(args.output_dir)
    ensure_dir(cache_dir)

    # Prepare output
    out_path = Path(args.output_data)
    out_tmp = out_path.with_suffix(out_path.suffix + ".tmp")

    total_input = 0
    processed = 0
    written = 0
    unique_pkgs = set()

    # open output in streaming mode; write header after we see the first chunk (preserve input columns)
    out_file = open(out_tmp, "w", newline="", encoding="utf-8")
    out_writer = None
    header_written = False

    log(f"Starting. Input={args.input_data}  Output={args.output_data}  Limit={args.limit}  Threshold={args.threshold}")

    # Only load needed columns to reduce IO (we still need everything to copy through).
    # We'll read all columns but only keep first `args.limit` rows overall.
    reader = pd.read_csv(args.input_data, chunksize=args.chunksize, low_memory=True)

    try:
        for chunk_idx, chunk in enumerate(reader, start=1):
            # Optionally filter to Google Play rows only
            if args.require_play and "markets" in chunk.columns:
                mask = chunk["markets"].astype(str).str.contains("play.google.com", na=False)
                chunk = chunk.loc[mask]

            if chunk.empty:
                continue

            # Initialize CSV writer with input columns + categories
            if not header_written:
                cols = list(chunk.columns)
                if "categories" not in cols:
                    cols.append("categories")
                out_writer = csv.DictWriter(out_file, fieldnames=cols)
                out_writer.writeheader()
                header_written = True

            # Iterate rows
            for _, row in chunk.iterrows():
                if args.limit and processed >= args.limit:
                    raise StopIteration

                total_input += 1

                pkg_name = str(row.get("pkg_name", "")).strip()
                if not pkg_name:
                    continue
                if pkg_name in unique_pkgs:
                    continue
                unique_pkgs.add(pkg_name)

                # Fetch (or load cached) metadata
                attempts = fetch_gp_metadata(pkg_name, args.apikey, cache_dir, force=args.force_refresh)
                desc = latest_description_from_attempts(attempts) if attempts else None
                if not desc:
                    # No description -> can't classify; skip
                    processed += 1
                    if processed % args.log_every == 0:
                        log(f"Processed={processed}  Written={written}")
                    continue

                # Classify with Ollama
                cats = ollama_classify(desc, model=args.ollama_model, use_http=not args.no_http, host=args.ollama_endpoint)
                if not cats:
                    processed += 1
                    if processed % args.log_every == 0:
                        log(f"Processed={processed}  Written={written}")
                    continue

                # Threshold filter removed (no-op)

                # Write output row: original columns + categories string
                out_row = {c: row.get(c, "") for c in out_writer.fieldnames if c != "categories"}
                out_row["categories"] = cats.get("category", "")
                out_writer.writerow(out_row)
                written += 1
                processed += 1

                if processed % args.log_every == 0:
                    log(f"Processed={processed}  Written={written}")

    except StopIteration:
        pass
    finally:
        out_file.close()

    # Atomic rename
    out_tmp.replace(out_path)

    log(f"Done. TotalInputRowsSeen={total_input}  Processed={processed}  Written={written}  UniquePkgs={len(unique_pkgs)}")
    log(f"Output written to: {args.output_data}")

if __name__ == "__main__":
    main()