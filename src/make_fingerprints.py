#!/usr/bin/env python3
import argparse
import csv
import os
import re
import sys
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# -------- Logging --------
def log(msg: str, *, flush=True):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=flush)

# -------- Regexes --------
JAVA_PKG_RE = re.compile(r'^\s*package\s+([a-zA-Z_][\w.]*?)\s*;\s*$')
KT_PKG_RE   = re.compile(r'^\s*package\s+([a-zA-Z_][\w.]*?)\s*$')
MANIFEST_PKG_RE = re.compile(r'package\s*=\s*"([^"]+)"')
# maven pom.xml tags (very lightweight)
XML_TAG_RE = re.compile(r"<([a-zA-Z0-9_.:-]+)>(.*?)</\1>", re.S)
# gradle group definitions (kts and groovy)
GRADLE_GROUP_ASSIGN = re.compile(r'^\s*group\s*=\s*["\']([^"\']+)["\']')
GRADLE_GROUP_STR    = re.compile(r'^\s*group\s+["\']([^"\']+)["\']')

# -------- Helpers --------
def to_smali_prefix(java_pkg: str) -> str:
    """com.example.lib -> Lcom/example/lib/"""
    java_pkg = java_pkg.strip().strip(".")
    if not java_pkg:
        return ""
    return "L" + java_pkg.replace(".", "/") + "/"

def read_text(path: Path, max_bytes: int = 2_000_000) -> str | None:
    try:
        if path.stat().st_size > max_bytes:
            return None
        return path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return None

def first_lines(path: Path, n: int = 80) -> list[str]:
    out = []
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            for _ in range(n):
                ln = f.readline()
                if not ln: break
                out.append(ln.rstrip("\n"))
    except Exception:
        pass
    return out

def detect_java_kotlin_packages(repo_root: Path, max_files: int) -> list[tuple[str,str,str]]:
    """
    Returns list of (smali_prefix, type, file_path)
    type: java_package | kotlin_package
    """
    results = []
    count = 0
    for ext, tname in ((".java","java_package"), (".kt","kotlin_package")):
        for src in repo_root.rglob(f"*{ext}"):
            if count >= max_files:
                return results
            lines = first_lines(src, 50)
            for ln in lines:
                m = (JAVA_PKG_RE if ext==".java" else KT_PKG_RE).match(ln)
                if m:
                    pref = to_smali_prefix(m.group(1))
                    if pref:
                        results.append((pref, tname, str(src)))
                    break
            count += 1
    return results

def detect_manifest_package(repo_root: Path) -> list[tuple[str,str,str]]:
    out = []
    for man in repo_root.rglob("AndroidManifest.xml"):
        txt = read_text(man, max_bytes=1_000_000)
        if not txt: 
            continue
        m = MANIFEST_PKG_RE.search(txt)
        if m:
            pref = to_smali_prefix(m.group(1))
            if pref:
                out.append((pref, "manifest_package", str(man)))
    return out

def detect_maven_group_artifact(repo_root: Path) -> list[tuple[str,str,str]]:
    """
    Read any pom.xml; yield groupId as smali prefix (maven_group_prefix)
    and artifactId-root as smali prefix (artifact_root) heuristically.
    """
    out = []
    for pom in repo_root.rglob("pom.xml"):
        txt = read_text(pom, max_bytes=1_000_000)
        if not txt:
            continue
        groupId = artifactId = ""
        for tag, val in XML_TAG_RE.findall(txt):
            t = tag.lower()
            v = val.strip()
            if t.endswith("groupid") and not groupId:
                groupId = v
            elif t.endswith("artifactid") and not artifactId:
                artifactId = v
        if groupId:
            gp = to_smali_prefix(groupId)
            if gp:
                out.append((gp, "maven_group_prefix", str(pom)))
        # artifact root (e.g., okhttp -> Lokhttp/)
        if artifactId and re.match(r"^[a-zA-Z0-9_.-]+$", artifactId):
            art = artifactId.replace(".", "/")
            out.append((f"L{art}/", "maven_artifact_root", str(pom)))
    return out

def detect_gradle_group(repo_root: Path) -> list[tuple[str,str,str]]:
    out = []
    for gradle in list(repo_root.rglob("build.gradle")) + list(repo_root.rglob("build.gradle.kts")):
        lines = first_lines(gradle, 200)
        for ln in lines:
            m = GRADLE_GROUP_ASSIGN.match(ln) or GRADLE_GROUP_STR.match(ln)
            if m:
                gp = to_smali_prefix(m.group(1))
                if gp:
                    out.append((gp, "gradle_group_prefix", str(gradle)))
                break
    return out

def canonical_repo_key(host: str, repo_path: str) -> str:
    # owner/repo for GitHub/Bitbucket; full path for GitLab (subgroups)
    return repo_path

def load_manifest(manifest_csv: Path) -> list[dict]:
    rows = []
    with manifest_csv.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            # Only consider status ok/existing/dry_run
            status = (row.get("status") or "").lower()
            if status in ("ok","exists","dry_run"):
                rows.append(row)
    return rows

def dedup(seq: list[tuple[str,str,str]]) -> list[tuple[str,str,str]]:
    seen = set()
    out = []
    for smali, typ, path in seq:
        key = (smali, typ)
        if key in seen: 
            continue
        seen.add(key)
        out.append((smali, typ, path))
    return out

def process_one_repo(row: dict, out_dir: Path, max_files: int) -> list[list[str]]:
    host = (row.get("host") or "").strip().lower()
    repo_path = (row.get("repo_path") or "").strip()
    url = (row.get("url") or "").strip()
    local_path = Path(row.get("local_path") or "")
    if not local_path.exists():
        return []

    # Collect fingerprints
    fps = []
    fps += detect_java_kotlin_packages(local_path, max_files=max_files)
    fps += detect_manifest_package(local_path)
    fps += detect_maven_group_artifact(local_path)
    fps += detect_gradle_group(local_path)

    fps = dedup(fps)

    # Convert to rows for CSV
    libarary_key = canonical_repo_key(host, repo_path)  # keep your header style
    library_name = repo_path.split("/")[-1] if repo_path else ""
    rows = []
    for smali, typ, fpath in fps:
        rows.append([
            host, repo_path, url, libarary_key, library_name,
            smali, typ, fpath, ""  # evidence_excerpt left blank (optional)
        ])
    return rows

# -------- Main --------
def main():
    ap = argparse.ArgumentParser(description="Build fingerprints.csv by scanning cloned repos for package declarations.")
    ap.add_argument("--input-data", default="repos/cloned_repos.csv", help="Manifest created by clone_oss_repos.py")
    ap.add_argument("--input-dir", default="repos", help="Root directory where repos are cloned")
    ap.add_argument("--output-data", default="fingerprints.csv", help="Output CSV path")
    ap.add_argument("--limit", type=int, default=0, help="Max repos to process (0=all)")
    ap.add_argument("--max-files", type=int, default=5000, help="Max source files to inspect per repo for pkg declarations (default 5000)")
    ap.add_argument("--workers", type=int, default=4, help="Parallel workers")
    ap.add_argument("--force", action="store_true", help="Overwrite output file if exists")
    ap.add_argument("--log-every", type=int, default=20, help="Progress logging frequency")
    args = ap.parse_args()

    manifest_csv = Path(args.input_data)
    if not manifest_csv.exists():
        log(f"ERROR: manifest not found: {manifest_csv}")
        sys.exit(1)

    # Make local paths in manifest absolute if needed
    # (We don't rewrite the manifest; we only resolve when used)
    rows = load_manifest(manifest_csv)
    if args.limit:
        rows = rows[:args.limit]

    out_path = Path(args.output_data)
    if out_path.exists() and not args.force:
        log(f"ERROR: output exists: {out_path} (use --force to overwrite)")
        sys.exit(1)

    # Prepare header
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "repo_host", "repo_path", "repo_url",
            "libarary_key", "library_name",
            "smali_prefix", "fingerprint_type",
            "repo_file_path", "evidence_excerpt"
        ])

    # Process in parallel
    results = []
    def work(row):
        # Ensure local_path points inside repos-dir if manifest used a different base
        local_path = Path(row.get("local_path") or "")
        if not local_path.is_absolute():
            local_path = Path(args.input_dir) / Path(row.get("host") or "") / Path(row.get("repo_path") or "")
            row = dict(row)
            row["local_path"] = str(local_path)
        return process_one_repo(row, out_path.parent, args.max_files)

    i = 0
    written = 0
    if args.workers > 1:
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futs = [ex.submit(work, r) for r in rows]
            for fut in as_completed(futs):
                rows_out = fut.result()
                if rows_out:
                    with out_path.open("a", encoding="utf-8", newline="") as f:
                        w = csv.writer(f)
                        for r in rows_out:
                            w.writerow(r)
                            written += 1
                i += 1
                if i % args.log_every == 0:
                    log(f"Processed repos: {i}/{len(rows)}  fingerprints so far: {written}")
    else:
        for r in rows:
            rows_out = work(r)
            if rows_out:
                with out_path.open("a", encoding="utf-8", newline="") as f:
                    w = csv.writer(f)
                    for r2 in rows_out:
                        w.writerow(r2)
                        written += 1
            i += 1
            if i % args.log_every == 0:
                log(f"Processed repos: {i}/{len(rows)}  fingerprints so far: {written}")

    log(f"Done. Repos processed: {i}. Fingerprints written: {written}. Output: {out_path}")

if __name__ == "__main__":
    main()