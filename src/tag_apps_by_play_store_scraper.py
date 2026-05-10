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
    ap.add_argument("--input-data", required=True)
    ap.add_argument("--output-data", required=True)
    ap.add_argument("--log-every", type=int, default=10)
    ap.add_argument("--excluded-apps", required=False, help="Comma-separated list of apps")
    ap.add_argument("--research-categories", required=True, help="Comma-separated list of categories")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--input-dir", required=True, help="Directory to scan for additional custom APKs")
    args = ap.parse_args()

    input_file = Path(args.input_data)
    output_file = Path(args.output_data)
    desired_categories = set(c.strip().upper() for c in args.research_categories.split(","))
    excluded_apps = set(
        app.strip()
        for app in (args.excluded_apps or "").split(",")
        if app.strip()
    )

    if not input_file.exists():
        log(f"Input file not found: {input_file}")
        sys.exit(1)

    # Load already processed pkg_names
    already_processed = dict()
    output_rows = []
    if output_file.exists():
        with output_file.open("r", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                already_processed[row["pkg_name"]] = row.get("category", "").strip().upper() #todo
                row["origin"] = "AndroZoo"
                output_rows.append(row)

    # Start processing new pkg_names
    new_rows = []
    with input_file.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        count = 0
        processed = 0
        for row in reader:
            pkg = row.get("pkg_name")
            if not pkg:
                continue
            if pkg in excluded_apps:
                log(f"Skipping excluded app: {pkg}")
                continue
            if pkg in already_processed:
                if already_processed[pkg] in desired_categories:
                    count += 1
                continue

            result = extract_play_store_data(pkg)
            time.sleep(1)  # Be polite to Google

            if not result:
                new_row = row.copy()
                new_row.update({"pkg_name": pkg, "play_store_status":"ERROR","app_name":"","author_name":"","author_url":"","downloads":"","category":""})
                new_row["origin"] = "AndroZoo"
                new_rows.append(new_row)
                continue

            processed += 1
            if processed % args.log_every == 0:
                log(f"Processed {processed} apps...")

            # Check if category is one of research categories
            if result["category"].upper() in desired_categories:
                count += 1

            new_row = row.copy()
            new_row.update(result)
            new_row["origin"] = "AndroZoo"
            new_rows.append(new_row)

            log(f"App {pkg} in category {result['category'].upper()} is processed")

            if args.limit and count >= args.limit:
                break

    # Write combined output
    all_rows = output_rows + new_rows
    fieldnames = list(all_rows[0].keys())
    if "origin" not in fieldnames:
        fieldnames.append("origin")
    with output_file.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    # Scan for custom APKs
    existing_pkgs = set(row["pkg_name"] for row in all_rows)
    apk_dir = Path(args.input_dir)
    if apk_dir.exists():
        additional_rows = []
        for apk_file in apk_dir.glob("*.apk"):
            pkg = apk_file.stem
            if pkg in excluded_apps:
                log(f"Skipping excluded custom app: {pkg}")
                continue
            if pkg not in existing_pkgs:
                result = extract_play_store_data(pkg)
                time.sleep(1)
                if not result:
                    result = {
                        "pkg_name": pkg,
                        "play_store_status":"ERROR",
                        "app_name":"",
                        "author_name":"",
                        "author_url":"",
                        "downloads":"",
                        "category":""
                    }
                result["origin"] = "Custom"
                additional_rows.append(result)

        if additional_rows:
            with output_file.open("a", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                for row in additional_rows:
                    writer.writerow(row)
            log(f"Added {len(additional_rows)} custom APKs from {apk_dir}")

    log(f"Done. Tagged apps written: {len(new_rows)} new, {len(output_rows)} existing, total: {len(all_rows)}")

if __name__ == "__main__":
    main()
