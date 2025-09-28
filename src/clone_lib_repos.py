#!/usr/bin/env python3
import argparse
import csv
import os
import re
import sys
import subprocess
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse

SUPPORTED_HOSTS = {"github.com", "gitlab.com", "bitbucket.org"}

def log(msg: str, *, flush=True):
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {msg}", flush=flush)

def normalize_repo_url(url: str) -> str:
    """Strip query/fragment, trailing .git and slashes; force https scheme if missing."""
    if not url:
        return ""
    url = url.strip()
    if not re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", url):
        url = "https://" + url
    parts = urlparse(url)
    scheme = "https"  # prefer https
    netloc = parts.netloc.lower()
    path = re.sub(r"\.git$", "", parts.path.rstrip("/"))
    return f"{scheme}://{netloc}{path}"

def extract_host_repo_path(url: str):
    """
    Returns (host, repo_path) where repo_path excludes leading slash and .git,
    e.g., 'github.com', 'owner/repo'  (for GitLab, repo_path can include subgroups)
    """
    if not url:
        return None, None
    norm = normalize_repo_url(url)
    parts = urlparse(norm)
    host = parts.netloc.lower()
    if host not in SUPPORTED_HOSTS:
        return None, None
    path = parts.path.strip("/")

    # Minimal sanity: need at least owner/repo
    segs = [s for s in path.split("/") if s]
    if len(segs) < 2:
        return None, None

    # On GitLab, allow nested groups before repo (keep entire path),
    # On GitHub/Bitbucket, usually owner/repo; if extra segments present, keep first two.
    if host in {"github.com", "bitbucket.org"}:
        repo_path = "/".join(segs[:2])
    else:
        repo_path = "/".join(segs)

    # Remove trailing .git if any leaked in
    repo_path = re.sub(r"\.git$", "", repo_path)
    return host, repo_path

def repo_local_path(outdir: Path, host: str, repo_path: str) -> Path:
    return outdir / host / repo_path

def git_clone(url: str, dest: Path, force: bool = False, depth: int = 1) -> tuple[bool, str]:
    if dest.exists():
        if force:
            # safe remove existing folder
            try:
                if dest.is_dir():
                    for root, dirs, files in os.walk(dest, topdown=False):
                        for name in files:
                            try: (Path(root) / name).unlink()
                            except Exception: pass
                        for name in dirs:
                            try: (Path(root) / name).rmdir()
                            except Exception: pass
                    dest.rmdir()
                else:
                    dest.unlink()
            except Exception as e:
                return False, f"failed to remove existing: {e}"
        else:
            return True, "exists"

    dest.parent.mkdir(parents=True, exist_ok=True)
    cmd = ["git", "clone", "--depth", str(depth), url, str(dest)]
    try:
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=600)
        if proc.returncode == 0:
            return True, "cloned"
        return False, (proc.stderr or proc.stdout).strip().splitlines()[-1][:400]
    except Exception as e:
        return False, str(e)

def enumerate_repo_urls(library_lists_dir: Path, max_files: int | None = None):
    """
    Yield repo_url strings from all CSVs under library_lists_dir.
    Falls back to 'homepage' when 'repo_url' missing.
    """
    count = 0
    for csv_path in sorted(library_lists_dir.glob("*.csv")):
        if max_files and count >= max_files:
            break
        count += 1
        try:
            with csv_path.open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    url = (row.get("repo_url") or "").strip()
                    if not url:
                        url = (row.get("homepage") or "").strip()
                    if not url:
                        continue
                    yield url
        except Exception:
            continue

def main():
    ap = argparse.ArgumentParser(description="Clone unique GitHub/GitLab/Bitbucket repos referenced by library_lists CSVs.")
    ap.add_argument("--input-dir", default="library_lists", help="Directory with per-app library CSVs")
    ap.add_argument("--output-dir", default="repos", help="Where to clone repositories")
    ap.add_argument("--output-data", default="cloned_repos.csv", help="Path to manifest CSV file")
    ap.add_argument("--limit", type=int, default=0, help="Max repositories to clone (0 = no limit)")
    ap.add_argument("--file-limit", type=int, default=0, help="Max library_list CSV files to scan (0 = all)")
    ap.add_argument("--force", action="store_true", help="Re-clone even if repo directory exists")
    ap.add_argument("--workers", type=int, default=1, help="Parallel clones (1 = sequential)")
    ap.add_argument("--dry-run", action="store_true", help="List what would be cloned, do not execute git clone")
    ap.add_argument("--log-every", type=int, default=20, help="Progress log frequency")
    ap.add_argument("--path-ssh-key", default=None, help="Path to SSH private key to use for cloning")
    args = ap.parse_args()

    if args.path_ssh_key:
        os.environ["GIT_SSH_COMMAND"] = f"ssh -i {args.path_ssh_key} -o IdentitiesOnly=yes -o StrictHostKeyChecking=no"

    library_lists_dir = Path(args.input_dir)
    outdir = Path(args.output_dir)
    outdir.mkdir(parents=True, exist_ok=True)

    # Collect and normalize repo URLs → canonical keys (host + repo_path)
    seen = {}
    total_seen = 0
    for url in enumerate_repo_urls(library_lists_dir, max_files=(args.file_limit or None)):
        host, repo_path = extract_host_repo_path(url)
        if not host:
            continue
        key = (host, repo_path)
        if key not in seen:
            seen[key] = normalize_repo_url(url)
        total_seen += 1

    repos = list(seen.items())
    if args.limit and len(repos) > args.limit:
        repos = repos[:args.limit]

    log(f"Found {len(repos)} unique public repos ({total_seen} urls scanned).")
    manifest_path = Path(args.output_data)
    if not manifest_path.exists():
        with manifest_path.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["host","repo_path","url","local_path","status","message"])

    # Clone (optionally parallel)
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def work(item):
        (host, repo_path), url = item
        dest = repo_local_path(outdir, host, repo_path)
        if args.dry_run:
            return host, repo_path, url, str(dest), "dry_run", ""
        ok, msg = git_clone(url, dest, force=args.force, depth=1)
        return host, repo_path, url, str(dest), ("ok" if ok else "error"), msg

    results = []
    if args.workers > 1:
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futs = {ex.submit(work, item): item for item in repos}
            for i, fut in enumerate(as_completed(futs), 1):
                results.append(fut.result())
                if i % args.log_every == 0:
                    log(f"Cloned {i}/{len(repos)}")
    else:
        for i, item in enumerate(repos, 1):
            results.append(work(item))
            if i % args.log_every == 0:
                log(f"Cloned {i}/{len(repos)}")

    # Append to manifest
    with manifest_path.open("a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        for host, repo_path, url, dest, status, msg in results:
            w.writerow([host, repo_path, url, dest, status, msg])

    # Summary
    ok_count = sum(1 for r in results if r[4] in ("ok","exists","dry_run"))
    err_count = sum(1 for r in results if r[4] == "error")
    log(f"Done. OK={ok_count}  Errors={err_count}  Manifest={manifest_path}")

if __name__ == "__main__":
    main()