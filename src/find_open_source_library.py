# ---------- JS/TS License Header Parser ----------
from typing import Dict, List

#!/usr/bin/env python3
import argparse
import csv
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from fnmatch import fnmatch

# ---------- Config ----------
DEFAULT_INPUT_DIR = "decoded"
DEFAULT_OUTPUT_DIR = "library_lists"
DEFAULT_LOG_EVERY = 10
DEFAULT_LIMIT = 100

# File name patterns to look for (case-insensitive)
LICENSE_GLOBS = [
    "LICENSE", "LICENSE.*", "COPYING", "COPYING.*", "NOTICE", "NOTICE.*",
    "THIRD-PARTY*", "THIRD_PARTY*", "3RD-PARTY*", "OSS*", "OpenSource*", "open_source*",
    "third_party*", "thirdparty*", "open-source*", "open source*",
]

# Common locations inside Android decoded trees
LIKELY_DIRS = [
    "res", "res/raw", "res/values", "assets", "META-INF", "META-INF/licenses", "META-INF/NOTICE", "META-INF/DEPENDENCIES"
]

# Build / dependencies files
DEP_FILES = [
    "build.gradle", "build.gradle.kts", "gradle.lockfile",
    "pom.xml", "package.json", "requirements.txt"
]

# Minimal SPDX/License name detection
SPDX_RE = re.compile(
    r"\b(Apache(?:-2\.0)?|MIT|BSD(?:-[23]-Clause)?|GPL(?:-[23](?:\.\d+)?)?|LGPL(?:-[23](?:\.\d+)?)?|MPL(?:-2\.0)?|EPL(?:-2\.0)?|CDDL|ISC|Unlicense|CC-BY(?:-[\d.]+)?|CC0)\b",
    re.I
)

URL_RE = re.compile(r"(https?://[^\s)\"\'<>]+)", re.I)

# ---------- Text file heuristics ----------
# Common text file extensions for text scan
TEXT_EXTS = {
    "txt", "md", "xml", "json", "html", "htm", "properties", "yml", "yaml", "cfg", "conf", "ini", "gradle", "kts", "csv", "mf", "sf", "pro", "java", "kt", "prefs", "config", "license", "notice"
}

# Directory patterns to exclude by default from deep scan (smali*, etc)
EXCLUDE_DIR_PATTERNS_DEFAULT = ["smali*", "original", "unknown", "build*", "out"]

# Git repo URL regex (github/gitlab/bitbucket)
GIT_URL_RE = re.compile(r"(https?://(?:github\.com|gitlab\.com|bitbucket\.org)/[\w.-]+/[\w.-]+)", re.I)

# Gradle notations: group:artifact:version within quotes
GRADLE_COORD_RE = re.compile(r"""['"]\s*([^:'"]+):([^:'"]+):([^:'"]+)\s*['"]""")

# Maven group/artifact/version and license tags
XML_TAG_RE = re.compile(r"<([a-zA-Z0-9_.:-]+)>(.*?)</\1>", re.S)

# NPM package.json keys of interest
NPM_KEYS = ["name", "version", "license", "author", "repository", "homepage"]

# CSV columns
CSV_HEADERS = [
    # Note: The column name 'libarary_key' intentionally follows the user's provided header (even though it looks like a misspelling).
    "pkg_name", "libarary_key", "library_name", "version", "license_id", "license_name", "license_url", "author", "homepage",
    "repo_url", "found_by", "file_path", "evidence_excerpt"
]

# ---------- Utils ----------
def log(msg: str, *, flush=True):
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {msg}", flush=flush)

def read_text_safely(p: Path, max_bytes: int = 2_000_000) -> Optional[str]:
    """Read small/medium text files safely; skip very large binaries."""
    try:
        if p.stat().st_size > max_bytes:
            return None
        text = p.read_text(encoding="utf-8", errors="ignore")
        return text
    except Exception:
        return None

def detect_license(text: str) -> Optional[str]:
    if not text:
        return None
    m = SPDX_RE.search(text)
    if m:
        return m.group(0).strip()
    # Small fallbacks
    if "apache license" in text.lower():
        return "Apache"
    if "mozilla public license" in text.lower():
        return "MPL"
    if "eclipse public license" in text.lower():
        return "EPL"
    return None

def first_url(text: str) -> Optional[str]:
    if not text:
        return None
    m = URL_RE.search(text)
    url = m.group(1) if m else None
    if url:
        # Skip common useless or invalid URLs
        bad_prefixes = ("http://www.w3.org", "data:")
        if any(url.startswith(p) for p in bad_prefixes):
            return None
        # Basic domain-style validation (e.g. http(s)://domain.tld/...)
        if not re.match(r"https?://[a-zA-Z0-9\-_.]+\.[a-zA-Z]{2,6}(/|$)", url):
            return None
        # Remove fragments, queries, trailing paths (e.g. wiki pages, raw files)
        if "github.com/" in url:
            parts = url.split("github.com/")[-1].split("/")
            if len(parts) >= 2:
                url = f"https://github.com/{parts[0]}/{parts[1]}"
        elif "raw.githubusercontent.com/" in url:
            # Convert raw GitHub URLs to repo base
            parts = url.split("raw.githubusercontent.com/")[-1].split("/")
            if len(parts) >= 3:
                url = f"https://github.com/{parts[0]}/{parts[1]}"
    return url

# Heuristic: is this file probably text (by extension or content sample)?
def is_probably_text(p: Path, sample_bytes: int = 4096) -> bool:
    ext = p.suffix.lower().lstrip(".")
    if ext in TEXT_EXTS:
        return True
    try:
        with p.open("rb") as f:
            sample = f.read(sample_bytes)
        if not sample:
            return False
        # Count printable (ASCII) bytes
        import string
        printable = set(bytes(string.printable, "ascii"))
        printable_count = sum(b in printable for b in sample)
        ratio = printable_count / len(sample)
        return ratio > 0.85
    except Exception:
        return False

# Helper: exclude paths based on directory patterns (e.g., smali*)
def is_excluded_path(root: Path, base: Path, patterns: List[str]) -> bool:
    rel = str(base.relative_to(root))
    parts = rel.split(os.sep)
    for part in parts[:-1]:  # check directories only
        for pat in patterns:
            if fnmatch(part, pat):
                return True
    return False
# ---------- Parsers ----------
def parse_generic_text(text: str, app_sha: str, file_path: str) -> List[Dict[str, str]]:
    out = []
    if not text:
        return out
    # 1) Git hosting URLs -> infer library name from owner/repo
    m = GIT_URL_RE.search(text)
    if m:
        url = m.group(1)
        parts = url.rstrip("/").split("/")
        if len(parts) >= 2:
            repo_name = "/".join(parts[-2:])
            lic = detect_license(text) or ""
            row = {
                "pkg_name": app_sha,
                "libarary_key": repo_name,
                "library_name": repo_name,
                "version": "",
                "license_id": lic,
                "license_name": lic,
                "license_url": "",
                "author": "",
                "homepage": "",
                "repo_url": url,
                "found_by": "generic_text_repo_url",
                "file_path": file_path,
                "evidence_excerpt": repo_name
            }
            out.append(row)
    # 2) Plain license indication — only if file path looks like a license/notice file to avoid noise
    lic = detect_license(text)
    if lic:
        row = {
            "pkg_name": app_sha,
            "libarary_key": "",
            "library_name": "",
            "version": "",
            "license_id": lic,
            "license_name": lic,
            "license_url": "",
            "author": "",
            "homepage": "",
            "repo_url": "",
            "found_by": "generic_text_license",
            "file_path": file_path,
            "evidence_excerpt": (text[:240].replace("\n"," ") if text else "")
        }
        out.append(row)
    return out

def safe_add(row: Dict[str, str], results: List[Dict[str, str]]):
    # De-dup heuristic: (library_name, version, license_id/license_name) tuple
    key = (
        (row.get("library_name") or "").lower(),
        (row.get("version") or "").lower(),
        ((row.get("license_id") or row.get("license_name") or "")).lower()
    )
    if not any((
        (r.get("library_name") or "").lower(),
        (r.get("version") or "").lower(),
        ((r.get("license_id") or r.get("license_name") or "")).lower()
    ) == key for r in results):
        results.append(row)

# ---------- Parsers ----------
def parse_gradle(text: str, app_sha: str, file_path: str) -> List[Dict[str, str]]:
    out = []
    if not text:
        return out
    for g, a, v in GRADLE_COORD_RE.findall(text):
        lib_name = f"{g}:{a}"
        lic = detect_license(text) or ""
        row = {
            "pkg_name": app_sha,
            "libarary_key": f"{g}:{a}",
            "library_name": lib_name,
            "version": v,
            "license_id": lic,
            "license_name": lic,
            "license_url": "",
            "author": "",
            "homepage": first_url(text) or "",
            "repo_url": "",
            "found_by": "gradle",
            "file_path": file_path,
            "evidence_excerpt": f"{lib_name}:{v}"
        }
        safe_add(row, out)
    return out

def parse_maven_pom(text: str, app_sha: str, file_path: str) -> List[Dict[str, str]]:
    out = []
    if not text:
        return out
    # very lightweight extraction; finds first of each
    groupId = artifactId = version = name = author = homepage = license_name = repo = ""
    for tag, val in XML_TAG_RE.findall(text):
        t = tag.lower()
        v = val.strip()
        if t.endswith("groupid"): groupId = v if not groupId else groupId
        elif t.endswith("artifactid"): artifactId = v if not artifactId else artifactId
        elif t.endswith("version"): 
            if not version and not t.endswith("modelversion"):
                version = v
        elif t.endswith("name"): name = v if not name else name
        elif t.endswith("url"): 
            if not homepage:
                homepage = v
        elif t.endswith("licenses") or t.endswith("license"):
            lic = detect_license(v)
            if lic: license_name = lic
        elif t.endswith("organization") or t.endswith("developers"):
            if not author:
                author = v.split("\n")[0][:200].strip()

    lib_display = name or (f"{groupId}:{artifactId}" if groupId and artifactId else artifactId or groupId)
    if lib_display:
        row = {
            "pkg_name": app_sha,
            "libarary_key": (f"{groupId}:{artifactId}" if groupId and artifactId else lib_display),
            "library_name": lib_display,
            "version": version,
            "license_id": license_name,
            "license_name": license_name,
            "license_url": "",
            "author": author,
            "homepage": homepage,
            "repo_url": repo,
            "found_by": "maven_pom",
            "file_path": file_path,
            "evidence_excerpt": (name or f"{groupId}:{artifactId}")[:240]
        }
        out.append(row)
    return out

def parse_license_like(text: str, app_sha: str, file_path: str) -> List[Dict[str, str]]:
    out = []
    if not text:
        return out

    # Heuristic pulls: name (line starting with 'Project:' or 'Name:'), author, URL, license
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    license_name = detect_license(text) or ""

    # pull some likely labels
    candidate_name = ""
    candidate_author = ""
    candidate_url = first_url(text) or ""

    for ln in lines[:50]:  # only scan first chunk for labels
        low = ln.lower()
        if not candidate_name and (low.startswith("project:") or low.startswith("name:")):
            candidate_name = ln.split(":",1)[-1].strip()
        if not candidate_author and (low.startswith("author:") or low.startswith("maintainer:") or low.startswith("organization:")):
            candidate_author = ln.split(":",1)[-1].strip()
        if not candidate_url:
            url_m = URL_RE.search(ln)
            if url_m:
                candidate_url = url_m.group(1)

    if candidate_name or license_name or candidate_url:
        row = {
            "pkg_name": app_sha,
            "libarary_key": (candidate_name or "").lower(),
            "library_name": candidate_name,
            "version": "",
            "license_id": license_name,
            "license_name": license_name,
            "license_url": "",
            "author": candidate_author,
            "homepage": candidate_url,
            "repo_url": "",
            "found_by": "license_file",
            "file_path": file_path,
            "evidence_excerpt": " / ".join(lines[:3])[:240]
        }
        out.append(row)
    return out

def parse_package_json(text: str, app_sha: str, file_path: str) -> List[Dict[str, str]]:
    out = []
    try:
        data = json.loads(text)
    except Exception:
        return out
    name = str(data.get("name",""))
    version = str(data.get("version",""))
    license_name = str(data.get("license","") or "")
    author = data.get("author","")
    if isinstance(author, dict):
        author = author.get("name","")
    author = str(author or "")
    homepage = str(data.get("homepage","") or "")
    repo = data.get("repository","")
    if isinstance(repo, dict):
        repo = repo.get("url","") or repo.get("repourl","") or repo.get("repo_url","") or ""
    repo = str(repo)

    # Add the top-level package.json (itself OSS?) — usually internal, so we *don't* include it by itself.
    # But we DO include declared dependencies as third-party signals.
    deps_keys = ["dependencies", "devDependencies", "optionalDependencies", "peerDependencies"]
    for k in deps_keys:
        deps = data.get(k, {})
        if isinstance(deps, dict):
            for dep_name, dep_ver in deps.items():
                row = {
                    "pkg_name": app_sha,
                    "libarary_key": dep_name,
                    "library_name": dep_name,
                    "version": str(dep_ver),
                    "license_id": "",
                    "license_name": "",
                    "license_url": "",
                    "author": "",
                    "homepage": "",
                    "repo_url": "",
                    "found_by": "package_json",
                    "file_path": f"{file_path}#{k}",
                    "evidence_excerpt": f"{dep_name}@{dep_ver}"
                }
                out.append(row)
    return out

def parse_requirements(text: str, app_sha: str, file_path: str) -> List[Dict[str, str]]:
    out = []
    for ln in (text or "").splitlines():
        ln = ln.strip()
        if not ln or ln.startswith("#"): 
            continue
        # pkg==ver or pkg>=ver or pkg<=ver
        m = re.match(r"([A-Za-z0-9_.-]+)\s*([=<>!~]=)\s*([A-Za-z0-9_.-]+)", ln)
        if m:
            name, _, ver = m.groups()
            row = {
                "pkg_name": app_sha,
                "libarary_key": name,
                "library_name": name,
                "version": ver,
                "license_id": "",
                "license_name": "",
                "license_url": "",
                "author": "",
                "homepage": "",
                "repo_url": "",
                "found_by": "requirements_txt",
                "file_path": file_path,
                "evidence_excerpt": ln[:240]
            }
            out.append(row)
        else:
            # plain pkg name
            if re.match(r"^[A-Za-z0-9_.-]+$", ln):
                row = {
                    "pkg_name": app_sha,
                    "libarary_key": ln,
                    "library_name": ln,
                    "version": "",
                    "license_id": "",
                    "license_name": "",
                    "license_url": "",
                    "author": "",
                    "homepage": "",
                    "repo_url": "",
                    "found_by": "requirements_txt",
                    "file_path": file_path,
                    "evidence_excerpt": ln[:240]
                }
                out.append(row)
    return out


# ---------- 3rdpartylicenses.txt parser ----------
def parse_3rdpartylicenses(text: str, app_sha: str, file_path: str) -> List[Dict[str, str]]:
    """Parse files like '3rdpartylicenses.txt' used by some bundlers."""
    out = []
    if not text:
        return out

    # Split by long separator lines (e.g., '--------') or multiple newlines
    blocks = re.split(r"[-=]{10,}|(?:\n\s*){2,}", text)

    for block in blocks:
        if not block.strip():
            continue

        lines = block.strip().splitlines()
        license_name = detect_license(block) or ""
        url = first_url(block) or ""
        pkg_name = ""
        author = ""

        for line in lines:
            low = line.lower()
            if low.startswith("package:") or low.startswith("library:"):
                pkg_name = line.split(":", 1)[-1].strip()
            elif low.startswith("license:"):
                val = line.split(":", 1)[-1].strip().strip('"\'')
                license_name = val if val else license_name
            elif "copyright" in low:
                # try to capture after © or (c) or "by"
                match = re.search(r"(?:©|\(c\)|copyright\s*\(c\)?|\bcopyright\b)[^\n:]*[:)]?\s*(.*)", line, re.I)
                if match:
                    author = match.group(1).strip()

        if pkg_name and license_name:
            row = {
                "pkg_name": app_sha,
                "libarary_key": pkg_name,
                "library_name": pkg_name,
                "version": "",
                "license_id": license_name,
                "license_name": license_name,
                "license_url": "",
                "author": author,
                "homepage": url,
                "repo_url": url,
                "found_by": "3rdpartylicenses",
                "file_path": file_path,
                "evidence_excerpt": block[:240].replace("\n", " ")
            }
            out.append(row)

    return out

def parse_google_oss_bundles(dir_path: Path, app_sha: str) -> List[Dict[str, str]]:
    """
    Handle common Google OSS Licenses plugin outputs:
    - res/raw/third_party_licenses (concatenated texts)
    - res/raw/third_party_license_metadata (index)
    These files come in a few formats; we best-effort extract library names from metadata
    and record evidence paths. We don't split the big license blob; we at least list names.
    """
    out = []
    meta_candidates = list(dir_path.glob("res/raw/third_party_license_metadata*"))
    if not meta_candidates:
        return out
    # Try each metadata file as JSON or plaintext lines with names
    for meta in meta_candidates:
        text = read_text_safely(meta)
        if not text:
            continue
        added_any = False
        # Try JSON array of objects with "name" fields
        try:
            data = json.loads(text)
            if isinstance(data, list):
                for obj in data:
                    name = str((obj.get("name") if isinstance(obj, dict) else "") or "").strip()
                    if name:
                        row = {
                            "pkg_name": app_sha,
                            "libarary_key": name,
                            "library_name": name,
                            "version": "",
                            "license_id": "",
                            "license_name": "",
                            "license_url": "",
                            "author": "",
                            "homepage": "",
                            "repo_url": "",
                            "found_by": "google_oss_metadata",
                            "file_path": str(meta),
                            "evidence_excerpt": name[:240]
                        }
                        safe_add(row, out)
                        added_any = True
        except Exception:
            pass
        # Fallback: plaintext lines; many builds store "name: offset length"
        if not added_any:
            for ln in text.splitlines():
                ln = ln.strip()
                if not ln:
                    continue
                # Heuristic: name then colon or tab separated
                name = ln.split(":", 1)[0].strip()
                if name and len(name) > 1 and len(name) < 200:
                    row = {
                        "pkg_name": app_sha,
                        "libarary_key": name,
                        "library_name": name,
                        "version": "",
                        "license_id": "",
                        "license_name": "",
                        "license_url": "",
                        "author": "",
                        "homepage": "",
                        "repo_url": "",
                        "found_by": "google_oss_metadata",
                        "file_path": str(meta),
                        "evidence_excerpt": ln[:240]
                    }
                    safe_add(row, out)
    return out

# ---------- New parser: all JSON licenses ----------
def parse_all_json_licenses(app_dir: Path, pkg_name: str) -> List[Dict[str, str]]:
    """
    Recursively scan all .json files for license-like keys and extract associated info.
    """
    results = []
    license_keys = {"license", "licensename", "licenseName", "license_name", "lisence", "lisense"}

    def recursive_search(obj, parent_keys=None):
        if parent_keys is None:
            parent_keys = []
        matches = []
        if isinstance(obj, dict):
            for k, v in obj.items():
                lower_k = k.lower()
                if lower_k in license_keys:
                    sibling_data = {**obj}
                    matches.append((k, v, sibling_data))
                elif isinstance(v, (dict, list)):
                    matches.extend(recursive_search(v, parent_keys + [k]))
        elif isinstance(obj, list):
            for idx, item in enumerate(obj):
                matches.extend(recursive_search(item, parent_keys + [str(idx)]))
        return matches

    for json_file in app_dir.rglob("*.json"):
        if not json_file.is_file():
            continue
        try:
            with json_file.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue
        try:
            matches = recursive_search(data)
            for key, license_val, context in matches:
                lib_name = context.get("name", "")
                version = context.get("version", "")
                license_url = context.get("licenseUrl", "") or context.get("license_url", "") or ""
                url = context.get("url", "") or context.get("repoUrl", "") or context.get("repo_url", "") or context.get("repository", "") or ""
                author = context.get("author", "") or context.get("copyrightHolder", "") or ""
                license_id = detect_license(str(license_val)) or detect_license(str(context.get("license", ""))) or ""
                row = {
                    "pkg_name": pkg_name,
                    "libarary_key": lib_name or url,
                    "library_name": lib_name,
                    "version": version,
                    "license_id": license_id,
                    "license_name": str(license_val),
                    "license_url": license_url,
                    "author": author,
                    "homepage": url,
                    "repo_url": url,
                    "found_by": "json_license_heuristic",
                    "file_path": str(json_file),
                    "evidence_excerpt": str(context)[:240]
                }
                results.append(row)
        except Exception as e:
            log(f"Failed to process {json_file}: {e}")
    return results

def parse_js_license_comment(text: str, pkg_name: str, file_path: str) -> List[Dict[str, str]]:
    """
    Extract license metadata from JS/TS file headers.

    Heuristics applied:
    - Inspect first N lines (header comment block).
    - Detect SPDX-like license names and license URLs.
    - Look for copyright/author lines and common labels (Author:, Copyright, ©, By ).
    - Detect module/plugin namespaces from patterns like `cordova.define("ns.Name"`) or `define("...")` and infer a library key.
    - Fallbacks: use @overview, @version, or first URL in header as hints.
    """
    results: List[Dict[str, str]] = []
    if not text:
        return results

    header_lines = text.splitlines()[:120]
    # Skip minified JS/TS with too few lines (likely no license header)
    if len(header_lines) < 10:
        return results
    header = "\n".join(header_lines)

    # license detection
    license_id = detect_license(header)
    license_url = first_url(header)

    # author detection heuristics
    author = ""
    for ln in header_lines:
        l = ln.strip()
        low = l.lower()
        # explicit 'Author: Name' label
        if low.startswith("author:"):
            author = l.split(":", 1)[1].strip()
            break
        # 'maintainer:' or similar
        if low.startswith("maintainer:") or low.startswith("copyright holder:"):
            author = l.split(":", 1)[1].strip()
            break
        # common copyright patterns -> try to capture name after the year or ©
        if "copyright" in low or "©" in l or "licensed to" in low:
            # Remove 'copyright', '(c)', years, and URLs to isolate name
            cleaned = re.sub(r"(?i)copyright|\\(c\\)|©|[0-9]{2,4}|http[s]?://\\S+", "", l)
            cleaned = cleaned.strip(" -*")
            if cleaned:
                author = cleaned.strip()
                break
            # 'by Name' patterns
            m2 = re.search(r"by\\s+([A-Za-z0-9 .,_&\\-]{2,80})", l, re.I)
            if m2:
                author = m2.group(1).strip()
                break

    # namespace / project detection heuristics
    project_name = ""
    m = re.search(r'\.define\(["\']([A-Za-z0-9_\-\.]+)\.', header)
    if m:
        project_name = m.group(1)
    else:
        # generic define('pkg.name' ...) or define("pkg.name"
        m2 = re.search(r'define\(["\']([A-Za-z0-9_\-\.]+)["\']', header)
        if m2:
            project_name = m2.group(1)
        else:
            # check for common tags like @overview or @name
            m3 = re.search(r'@overview\\s+([A-Za-z0-9_\\-\\. ]{2,120})', header, re.I)
            if m3:
                project_name = m3.group(1).strip()
            else:
                m4 = re.search(r'@name\\s+([A-Za-z0-9_\\-\\. ]{2,120})', header, re.I)
                if m4:
                    project_name = m4.group(1).strip()

    # best-effort library key and display name
    if license_url and "github.com/" in license_url:
        lib_key = license_url.split("github.com/")[-1].strip("/")
    else:
        lib_key = project_name or Path(file_path).stem

    lib_display = project_name or Path(file_path).stem

    # Only add a result if we found a license signal, url, author or project name
    if license_id or license_url or author or project_name:
        row = {
            "pkg_name": pkg_name,
            "libarary_key": lib_key,
            "library_name": lib_display,
            "version": "",
            "license_id": license_id or "",
            "license_name": license_id or "",
            "license_url": license_url or "",
            "author": author or "",
            "homepage": "",
            "repo_url": license_url if license_url and "github.com" in license_url else "",
            "found_by": "js_license_header",
            "file_path": file_path,
            "evidence_excerpt": header[:400],
        }
        results.append(row)

    return results

# ---------- Main scan ----------
def deep_scan_all_text(app_dir: Path, pkg_name: str, max_files: int, exclude_patterns: List[str]) -> List[Dict[str, str]]:
    results: List[Dict[str, str]] = []
    scanned = 0
    for f in app_dir.rglob("*"):
        if scanned >= max_files:
            break
        if is_excluded_path(app_dir, f, exclude_patterns):
            continue
        if not f.is_file():
            continue
        # size guard (reuse read_text_safely limit)
        try:
            if f.stat().st_size > 2_000_000:
                continue
        except Exception:
            continue
        if not is_probably_text(f):
            continue
        text = read_text_safely(f)
        if not text:
            continue
        # Try specific parsers based on filename
        name = f.name.lower()
        if name.startswith("build.gradle"):
            for row in parse_gradle(text, pkg_name, str(f)):
                safe_add(row, results)
        elif name == "pom.xml":
            for row in parse_maven_pom(text, pkg_name, str(f)):
                safe_add(row, results)
        elif name == "package.json":
            for row in parse_package_json(text, pkg_name, str(f)):
                safe_add(row, results)
        elif name == "requirements.txt":
            for row in parse_requirements(text, pkg_name, str(f)):
                safe_add(row, results)
        elif f.suffix.lower() in [".txt", "md"]:
            for row in parse_3rdpartylicenses(text, pkg_name, str(f)):
                safe_add(row, results)
        elif f.suffix.lower() in [".js", ".ts"]:
            for row in parse_js_license_comment(text, pkg_name, str(f)):
                safe_add(row, results)
        else:
            # generic content scan (SPDX, git URLs, gradle coords inside any text)
            for row in parse_generic_text(text, pkg_name, str(f)):
                safe_add(row, results)
        scanned += 1
    return results

def scan_one_app(app_dir: Path, pkg_name: str, max_files: int, exclude_patterns: List[str]) -> List[Dict[str, str]]:
    results: List[Dict[str, str]] = []

    # 1) all JSON licenses (recursively)
    results.extend(parse_all_json_licenses(app_dir, pkg_name))

    # 2) Google OSS bundles (fast)
    results.extend(parse_google_oss_bundles(app_dir, pkg_name))

    # 3) Traverse likely dirs first
    visited = set()
    for sub in LIKELY_DIRS:
        p = (app_dir / sub).resolve()
        if p.exists() and p.is_dir() and str(p) not in visited:
            visited.add(str(p))
            # License-like files
            for pattern in LICENSE_GLOBS:
                for f in p.glob(pattern):
                    if not f.is_file():
                        continue
                    text = read_text_safely(f)
                    if not text:
                        continue
                    # Parse generic license-ish file
                    for row in parse_license_like(text, pkg_name, str(f)):
                        safe_add(row, results)

            # Dependency files
            for df in DEP_FILES:
                f = p / df
                if f.exists() and f.is_file():
                    text = read_text_safely(f)
                    if not text:
                        continue
                    if f.name.startswith("build.gradle"):
                        pass
                        for row in parse_gradle(text, pkg_name, str(f)):
                            safe_add(row, results)
                    elif f.name == "pom.xml":
                        for row in parse_maven_pom(text, pkg_name, str(f)):
                            safe_add(row, results)
                    elif f.name == "package.json":
                        for row in parse_package_json(text, pkg_name, str(f)):
                            safe_add(row, results)
                    elif f.name == "requirements.txt":
                        for row in parse_requirements(text, pkg_name, str(f)):
                            safe_add(row, results)

    # 4) Shallow project root scan for leftover license-ish names
    for f in app_dir.iterdir():
        if f.is_file():
            name = f.name.lower()
            if any(k in name for k in ["license", "notice", "third", "oss", "open_source"]):
                text = read_text_safely(f)
                if text:
                    for row in parse_license_like(text, pkg_name, str(f)):
                        safe_add(row, results)

    # 5) Deep fallback: scan text-like files across entire tree for embedded coordinates, URLs, or licenses
    results.extend(deep_scan_all_text(app_dir, pkg_name, max_files=max_files, exclude_patterns=exclude_patterns))

    return results

def write_app_csv(out_dir: Path, pkg_name: str, rows: List[Dict[str, str]]):
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{pkg_name}.csv"
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        w.writeheader()
        for r in rows:
            # ensure all fields exist
            for k in CSV_HEADERS:
                r.setdefault(k, "")
            w.writerow(r)

def already_done(out_dir: Path, pkg_name: str) -> bool:
    return (out_dir / f"{pkg_name}.csv").exists()

def main():
    ap = argparse.ArgumentParser(description="Extract third-party OSS inventory from decoded APKs into per-app CSVs.")
    ap.add_argument("--input-dir", default=DEFAULT_INPUT_DIR, help="Folder with decoded apps (default: decoded)")
    ap.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="Output folder for CSVs (default: library_lists)")
    ap.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Max apps to process (default: 100)")
    ap.add_argument("--force", action="store_true", help="Overwrite existing per-app CSVs")
    ap.add_argument("--log-every", type=int, default=DEFAULT_LOG_EVERY, help="Log progress every N apps")
    ap.add_argument("--max-files", type=int, default=4000, help="Max files to inspect per app in deep scan (default 4000)")
    ap.add_argument("--include-smali", action="store_true", help="Also scan smali* directories (excluded by default)")
    args = ap.parse_args()

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)

    if not input_dir.exists():
        print(f"ERROR: input directory not found: {input_dir}", file=sys.stderr)
        sys.exit(1)

    # Gather app dirs: input_dir/<pkg_name>
    app_dirs = [p for p in sorted(input_dir.iterdir()) if p.is_dir()]
    if args.limit and len(app_dirs) > args.limit:
        app_dirs = app_dirs[:args.limit]

    log(f"Scanning {len(app_dirs)} app(s) from {input_dir} -> {output_dir}")

    # Handle exclusion patterns and max_files from CLI
    exclude_patterns = [] if args.include_smali else EXCLUDE_DIR_PATTERNS_DEFAULT
    max_files = args.max_files

    processed = 0
    written = 0
    skipped = 0

    for app_dir in app_dirs:
        pkg_name = app_dir.name
        processed += 1

        if already_done(output_dir, pkg_name) and not args.force:
            skipped += 1
            if processed % args.log_every == 0:
                log(f"Processed={processed}  Written={written}  Skipped={skipped}")
            continue

        rows = scan_one_app(app_dir, pkg_name, max_files=max_files, exclude_patterns=exclude_patterns)
        write_app_csv(output_dir, pkg_name, rows)
        written += 1

        if processed % args.log_every == 0:
            log(f"Processed={processed}  Written={written}  Skipped={skipped}")

    log(f"Done. Processed={processed}  Written={written}  Skipped={skipped}")
    log(f"Per-app CSVs are in: {output_dir.resolve()}")

if __name__ == "__main__":
    main()