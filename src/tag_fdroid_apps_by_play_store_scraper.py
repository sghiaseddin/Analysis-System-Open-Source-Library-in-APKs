#!/usr/bin/env python3
import argparse
import csv
import time
import os
import sys
import requests
from bs4 import BeautifulSoup
from pathlib import Path

def log(msg: str):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

def extract_play_store_data(pkg_name):
    status = "OK"
    url = f"https://play.google.com/store/apps/details?id={pkg_name}&hl=en&gl=us"
    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    try:
        res = requests.get(url, headers=headers, timeout=10)
        if res.status_code != 200:
            status = f"HTTP_{res.status_code}"
            log(f"Failed to fetch {pkg_name}: HTTP {res.status_code}")

        soup = BeautifulSoup(res.text, "html.parser")

        app_name = soup.select_one("h1 > span")
        app_name = app_name.text.strip() if app_name else ""

        author_tag = soup.find("a", href=lambda x: x and x.startswith("/store/apps/dev"))
        author_name = author_tag.text.strip() if author_tag else ""
        author_url = f"https://play.google.com{author_tag['href']}" if author_tag else ""

        downloads = ""
        try:
            # 1) look for any element whose text contains the word 'Downloads' (case-sensitive as page uses 'Downloads')
            label = None
            for s in soup.find_all(string=True):
                if s and 'Downloads' in s:
                    label = s
                    break

            if label:
                # label.parent is usually a <div> that has the label; try to get the count from previous sibling
                lbl_parent = label.parent
                prev = lbl_parent.find_previous_sibling()
                if prev and prev.get_text(strip=True):
                    downloads = prev.get_text(strip=True)
                else:
                    # sometimes the structure is reversed: the count is the previous element inside the same parent
                    for sibling in lbl_parent.parent.find_all(recursive=False):
                        text = sibling.get_text(strip=True)
                        if text and 'Downloads' not in text:
                            downloads = text
                            break

        except Exception:
            downloads = ""

        # Find category tag by searching for the first <a> with href containing "/store/apps/category/" that appears after <h1>
        category = ""
        try:
            h1 = soup.find("h1")
            if h1:
                current = h1.find_next("a", href=lambda x: x and "/store/apps/category/" in x)
                if current:
                    category = current["href"].split("/")[-1]
        except Exception:
            category = ""

        if status != "OK":
            return {
                "pkg_name": pkg_name,
                "play_store_status": status,
                "app_name": "",
                "author_name": "",
                "author_url": "",
                "downloads": "",
                "category": ""
            }

        return {
            "pkg_name": pkg_name,
            "play_store_status": status,
            "app_name": app_name,
            "author_name": author_name,
            "author_url": author_url,
            "downloads": downloads,
            "category": category
        }

    except Exception as e:
        log(f"Error fetching {pkg_name}: {e}")
        return {
            "pkg_name": pkg_name,
            "play_store_status": "ERROR",
            "app_name": "",
            "author_name": "",
            "author_url": "",
            "downloads": "",
            "category": ""
        }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-data", required=True, help="Main APK dataset CSV; must contain pkg_name column")
    ap.add_argument("--fdroid-data", required=True, help="F-Droid dataset CSV; must contain package and source columns")
    ap.add_argument("--output-data", required=True)
    ap.add_argument("--log-every", type=int, default=10)
    ap.add_argument("--excluded-apps", required=False, help="Comma-separated list of apps")
    ap.add_argument("--research-categories", required=True, help="Comma-separated list of categories")
    ap.add_argument("--limit", type=int, default=100)
    args = ap.parse_args()

    input_file = Path(args.input_data)
    output_file = Path(args.output_data)
    fdroid_file = Path(args.fdroid_data)
    desired_categories = set(c.strip().upper() for c in args.research_categories.split(","))
    excluded_apps = set(
        app.strip()
        for app in (args.excluded_apps or "").split(",")
        if app.strip()
    )

    if not input_file.exists():
        log(f"Input file not found: {input_file}")
        sys.exit(1)

    if not fdroid_file.exists():
        log(f"F-Droid dataset not found: {fdroid_file}")
        sys.exit(1)

    # Load pkg_names from the main input dataset
    input_pkg_names = set()
    with input_file.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pkg = row.get("pkg_name", "").strip()
            if pkg:
                input_pkg_names.add(pkg)

    # Load already processed pkg_names from output file
    existing_rows = []
    existing_pkgs = set()
    fieldnames = []
    if output_file.exists():
        with output_file.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames or []
            for row in reader:
                pkg = row.get("pkg_name", "").strip()
                if pkg:
                    existing_pkgs.add(pkg)
                existing_rows.append(row)

    # Filter the F-Droid dataset first
    fdroid_candidates = []
    with fdroid_file.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pkg = row.get("package", "").strip()
            source = row.get("source", "").strip()

            if not pkg:
                continue
            if pkg not in input_pkg_names:
                continue
            if pkg in excluded_apps:
                log(f"Skipping excluded F-Droid app: {pkg}")
                continue
            if pkg in existing_pkgs:
                continue
            if "github.com" not in source.lower():
                continue

            fdroid_candidates.append({
                "pkg_name": pkg,
                "fdroid_source": source
            })

    log(f"F-Droid candidates after filtering: {len(fdroid_candidates)}")

    # Scrape filtered F-Droid candidates and keep only research-category apps
    fdroid_rows = []
    scraped = 0
    for candidate in fdroid_candidates:
        pkg = candidate["pkg_name"]
        result = extract_play_store_data(pkg)
        time.sleep(1)
        scraped += 1

        if scraped % args.log_every == 0:
            log(f"Scraped {scraped} F-Droid candidates...")

        if result["category"].upper() not in desired_categories:
            log(f"Skipping F-Droid app {pkg}: category {result['category'].upper()} is not in research categories")
            continue

        result["origin"] = "F-Droid"
        result["fdroid_source"] = candidate["fdroid_source"]
        fdroid_rows.append(result)
        existing_pkgs.add(pkg)

        log(f"F-Droid app {pkg} in category {result['category'].upper()} is processed")

        if args.limit and len(fdroid_rows) >= args.limit:
            break

    if fdroid_rows:
        if not fieldnames:
            fieldnames = list(fdroid_rows[0].keys())

        for field in fdroid_rows[0].keys():
            if field not in fieldnames:
                fieldnames.append(field)

        all_rows = existing_rows + fdroid_rows
        with output_file.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_rows)

        log(f"Added {len(fdroid_rows)} F-Droid open-source apps to {output_file}")
    else:
        log("No matching F-Droid apps found to add")

    log(f"Done. F-Droid apps added: {len(fdroid_rows)}")

if __name__ == "__main__":
    main()
