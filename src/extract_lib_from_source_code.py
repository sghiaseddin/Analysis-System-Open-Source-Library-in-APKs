import argparse
import csv
import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlparse


def log(message):
    print(message, flush=True)


def normalize_github_url(source):
    if not source:
        return None

    source = source.strip()
    match = re.search(r"https?://github\.com/[^\s,;]+", source, re.IGNORECASE)
    if match:
        source = match.group(0)
    elif source.lower().startswith("github.com/"):
        source = "https://" + source
    else:
        return None

    source = source.split("#")[0].split("?")[0].rstrip("/")
    parsed = urlparse(source)

    parts = [p for p in parsed.path.split("/") if p]
    if len(parts) < 2:
        return None

    owner = parts[0]
    repo = parts[1]
    if repo.endswith(".git"):
        repo = repo[:-4]

    return f"https://github.com/{owner}/{repo}.git"


def normalize_url(raw_url):
    if not raw_url:
        return ""

    url = raw_url.strip().rstrip(".,);]")
    if url.lower().startswith("github.com/"):
        url = "https://" + url
    if url.endswith(".git"):
        url = url[:-4]
    return url.rstrip("/")


def extract_first_url(text):
    if not text:
        return ""

    match = re.search(r"https?://[^\s)\],;]+|github\.com/[^\s)\],;]+", text, re.IGNORECASE)
    if not match:
        return ""

    return normalize_url(match.group(0))


def parse_url_identity(url):
    url = normalize_url(url)
    if not url:
        return "", "", ""

    parsed = urlparse(url)
    repo_host = parsed.netloc.lower()
    parts = [p for p in parsed.path.split("/") if p]

    if repo_host == "github.com" and len(parts) >= 2:
        repo_path = f"{parts[0]}/{parts[1]}"
        repo_url = f"https://github.com/{repo_path}"
        return repo_host, repo_path, repo_url

    return repo_host, "/".join(parts), url


def parse_gradle_coordinate(coordinate):
    if not coordinate:
        return "", "", ""

    parts = coordinate.split(":")
    if len(parts) < 2:
        return "", coordinate, ""

    group_id = parts[0]
    artifact_id = parts[1]
    version = parts[2] if len(parts) >= 3 else ""
    return group_id, artifact_id, version


def normalize_library_key(library_name, repo_url=""):
    repo_host, repo_path, _ = parse_url_identity(repo_url)
    if repo_host == "github.com" and repo_path:
        return repo_path

    group_id, artifact_id, _ = parse_gradle_coordinate(library_name)
    if group_id and artifact_id:
        return f"{group_id}:{artifact_id}"

    return library_name.strip()


def repo_dir_name(pkg_name, github_url):
    parsed = urlparse(github_url)
    parts = [p for p in parsed.path.split("/") if p]
    owner = parts[0] if len(parts) > 0 else "unknown"
    repo = parts[1] if len(parts) > 1 else pkg_name
    repo = repo.removesuffix(".git")
    safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", f"{pkg_name}__{owner}__{repo}")
    return safe_name


def clone_repo(pkg_name, github_url, source_root):
    repo_path = source_root / repo_dir_name(pkg_name, github_url)

    if repo_path.exists():
        log(f"Repo already exists, skipping clone: {repo_path}")
        return repo_path

    log(f"Cloning {github_url} into {repo_path}")
    result = subprocess.run(
        ["git", "clone", "--depth", "1", github_url, str(repo_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    if result.returncode != 0:
        log(f"Failed to clone {github_url}: {result.stderr.strip()}")
        return None

    return repo_path


def read_text(path):
    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""


def add_finding(findings, pkg_name, app_repo_url, method, file_path, library_name, declaration, thirdparty_url=""):
    library_name = library_name.strip() if library_name else ""
    declaration = declaration.strip() if declaration else ""
    thirdparty_url = normalize_url(thirdparty_url or extract_first_url(declaration))

    thirdparty_repo_host, thirdparty_repo_path, thirdparty_repo_url = parse_url_identity(thirdparty_url)
    group_id, artifact_id, version = parse_gradle_coordinate(library_name)
    library_key = normalize_library_key(library_name, thirdparty_repo_url)

    findings.append({
        "pkg_name": pkg_name,
        "app_repo_url": app_repo_url,
        "method": method,
        "file_path": str(file_path),
        "library_key": library_key,
        "library_name": artifact_id or library_name,
        "declaration": declaration,
        "dependency_group": group_id,
        "dependency_artifact": artifact_id,
        "dependency_version": version,
        "thirdparty_url": thirdparty_url,
        "repo_host": thirdparty_repo_host,
        "repo_path": thirdparty_repo_path,
        "repo_url": thirdparty_repo_url,
    })


def detect_gradle_dependencies(pkg_name, repo_url, repo_path):
    findings = []
    gradle_files = list(repo_path.rglob("*.gradle")) + list(repo_path.rglob("*.gradle.kts"))

    dependency_patterns = [
        re.compile(r"(?:implementation|api|compileOnly|runtimeOnly|testImplementation|androidTestImplementation|kapt|annotationProcessor)\s*\(?\s*[\"']([^\"']+:[^\"']+:[^\"']+)[\"']", re.IGNORECASE),
        re.compile(r"(?:implementation|api|compileOnly|runtimeOnly|testImplementation|androidTestImplementation|kapt|annotationProcessor)\s*\(?\s*group\s*=\s*[\"']([^\"']+)[\"']\s*,\s*name\s*=\s*[\"']([^\"']+)[\"']\s*,\s*version\s*=\s*[\"']([^\"']+)[\"']", re.IGNORECASE),
    ]

    for gradle_file in gradle_files:
        text = read_text(gradle_file)
        rel_path = gradle_file.relative_to(repo_path)

        for line in text.splitlines():
            clean_line = line.strip()
            if not clean_line or clean_line.startswith("//"):
                continue

            first_match = dependency_patterns[0].search(clean_line)
            if first_match:
                declaration = first_match.group(1)
                add_finding(findings, pkg_name, repo_url, "gradle_dependency", rel_path, declaration, clean_line)
                continue

            second_match = dependency_patterns[1].search(clean_line)
            if second_match:
                declaration = ":".join(second_match.groups())
                add_finding(findings, pkg_name, repo_url, "gradle_dependency", rel_path, declaration, clean_line)

    return findings


def detect_version_catalog(pkg_name, repo_url, repo_path):
    findings = []
    for toml_file in repo_path.rglob("libs.versions.toml"):
        text = read_text(toml_file)
        rel_path = toml_file.relative_to(repo_path)
        in_libraries_section = False

        for line in text.splitlines():
            clean_line = line.strip()
            if clean_line == "[libraries]":
                in_libraries_section = True
                continue
            if clean_line.startswith("[") and clean_line.endswith("]") and clean_line != "[libraries]":
                in_libraries_section = False
                continue
            if not in_libraries_section or not clean_line or clean_line.startswith("#"):
                continue

            if "=" in clean_line:
                library_name = clean_line.split("=", 1)[0].strip()
                add_finding(findings, pkg_name, repo_url, "libs_versions_toml", rel_path, library_name, clean_line)

    return findings


def detect_thirdparty_readme(pkg_name, repo_url, repo_path):
    findings = []
    candidate_files = []
    for readme_name in ["README.md", "README.txt", "README"]:
        candidate_files.extend(repo_path.rglob(f"thirdparty/{readme_name}"))
        candidate_files.extend(repo_path.rglob(f"third_party/{readme_name}"))
        candidate_files.extend(repo_path.rglob(f"vendor/{readme_name}"))

    bullet_pattern = re.compile(r"^\s*[-*]\s+(.+)")
    markdown_link_pattern = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")

    for readme_file in candidate_files:
        text = read_text(readme_file)
        rel_path = readme_file.relative_to(repo_path)

        for line in text.splitlines():
            clean_line = line.strip()
            if not clean_line:
                continue

            bullet_match = bullet_pattern.match(clean_line)
            link_match = markdown_link_pattern.search(clean_line)
            if bullet_match:
                library_name = bullet_match.group(1)
                thirdparty_url = extract_first_url(clean_line)
                add_finding(findings, pkg_name, repo_url, "thirdparty_readme", rel_path, library_name, clean_line, thirdparty_url)
            elif link_match:
                library_name = link_match.group(1)
                thirdparty_url = link_match.group(2)
                add_finding(findings, pkg_name, repo_url, "thirdparty_readme", rel_path, library_name, clean_line, thirdparty_url)

    return findings


def detect_vendor_directories(pkg_name, repo_url, repo_path):
    findings = []
    vendor_dir_names = {"thirdparty", "third_party", "vendor", "external", "libs"}
    ignored_names = {".git", "build", "node_modules", ".gradle"}

    for path in repo_path.rglob("*"):
        if not path.is_dir():
            continue
        if path.name in ignored_names:
            continue
        if path.name.lower() not in vendor_dir_names:
            continue

        for child in path.iterdir():
            if child.is_dir() and child.name not in ignored_names:
                rel_path = child.relative_to(repo_path)
                add_finding(findings, pkg_name, repo_url, "vendor_directory", rel_path, child.name, str(rel_path))

    return findings

def detect_any_github_url(pkg_name, repo_url, repo_path):
    findings = []
    _, app_repo_path, _ = parse_url_identity(repo_url)
    seen_urls = set()

    ignored_dirs = {
        ".git",
        ".gradle",
        ".idea",
        ".vscode",
        "build",
        "dist",
        "node_modules",
        "target",
        "__pycache__",
    }
    ignored_extensions = {
        ".apk",
        ".aab",
        ".aar",
        ".jar",
        ".class",
        ".dex",
        ".so",
        ".dll",
        ".dylib",
        ".exe",
        ".png",
        ".jpg",
        ".jpeg",
        ".gif",
        ".webp",
        ".ico",
        ".pdf",
        ".zip",
        ".gz",
        ".7z",
        ".tar",
        ".xz",
        ".mp3",
        ".mp4",
        ".mov",
        ".ttf",
        ".otf",
    }
    github_repo_pattern = re.compile(
        r"(?:https?://)?github\.com/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)",
        re.IGNORECASE,
    )

    for file_path in repo_path.rglob("*"):
        if not file_path.is_file():
            continue
        if any(part in ignored_dirs for part in file_path.parts):
            continue
        if file_path.suffix.lower() in ignored_extensions:
            continue

        text = read_text(file_path)
        if not text:
            continue

        rel_path = file_path.relative_to(repo_path)
        for match in github_repo_pattern.finditer(text):
            owner = match.group(1)
            repo = match.group(2).removesuffix(".git")
            github_url = f"https://github.com/{owner}/{repo}"
            repo_host, github_repo_path, normalized_repo_url = parse_url_identity(github_url)

            if not github_repo_path:
                continue
            if app_repo_path and github_repo_path.lower() == app_repo_path.lower():
                continue
            if normalized_repo_url.lower() in seen_urls:
                continue

            seen_urls.add(normalized_repo_url.lower())
            add_finding(
                findings,
                pkg_name,
                repo_url,
                "any_github_url",
                rel_path,
                github_repo_path,
                match.group(0),
                normalized_repo_url,
            )

    return findings

DETECTORS = [
    detect_gradle_dependencies,
    detect_version_catalog,
    detect_thirdparty_readme,
    detect_vendor_directories,
    detect_any_github_url,
]


def extract_libraries(pkg_name, repo_url, repo_path):
    findings = []
    for detector in DETECTORS:
        try:
            findings.extend(detector(pkg_name, repo_url, repo_path))
        except Exception as exc:
            log(f"Detector failed for {pkg_name} using {detector.__name__}: {exc}")
    return findings


def load_fdroid_apps(tagged_apps_path):
    apps = []
    with tagged_apps_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pkg_name = row.get("pkg_name", "").strip()
            fdroid_source = row.get("fdroid_source", "").strip()
            if not pkg_name or not fdroid_source:
                continue

            github_url = normalize_github_url(fdroid_source)
            if not github_url:
                continue

            apps.append({
                "pkg_name": pkg_name,
                "github_url": github_url,
            })
    return apps


def write_findings(output_path, findings):
    fieldnames = [
        "pkg_name",
        "app_repo_url",
        "method",
        "file_path",
        "library_key",
        "library_name",
        "declaration",
        "dependency_group",
        "dependency_artifact",
        "dependency_version",
        "thirdparty_url",
        "repo_host",
        "repo_path",
        "repo_url",
    ]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(findings)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tagged-apps", default="../database/tagged_apps.csv", help="CSV containing fdroid_source column")
    ap.add_argument("--source-dir", default="../data/app_source_code", help="Directory where GitHub repositories will be cloned")
    ap.add_argument("--output-data", default="../database/source_code_libraries.csv", help="Output CSV for detected source-code library declarations")
    ap.add_argument("--limit", type=int, default=0, help="Optional maximum number of F-Droid apps to process")
    args = ap.parse_args()

    tagged_apps_path = Path(args.tagged_apps)
    source_root = Path(args.source_dir)
    output_path = Path(args.output_data)

    if not tagged_apps_path.exists():
        log(f"Tagged apps file not found: {tagged_apps_path}")
        sys.exit(1)

    source_root.mkdir(parents=True, exist_ok=True)

    apps = load_fdroid_apps(tagged_apps_path)
    if args.limit:
        apps = apps[:args.limit]

    log(f"F-Droid GitHub apps to process: {len(apps)}")

    all_findings = []
    for index, app in enumerate(apps, start=1):
        pkg_name = app["pkg_name"]
        github_url = app["github_url"]

        log(f"[{index}/{len(apps)}] Processing {pkg_name}")
        repo_path = clone_repo(pkg_name, github_url, source_root)
        if not repo_path:
            continue

        findings = extract_libraries(pkg_name, github_url, repo_path)
        all_findings.extend(findings)
        log(f"Detected {len(findings)} source-code library declarations for {pkg_name}")

    url_findings = sum(1 for finding in all_findings if finding.get("thirdparty_url"))
    write_findings(output_path, all_findings)
    log(f"Done. Source-code library declarations written: {len(all_findings)} to {output_path}")
    log(f"Findings with third-party URLs: {url_findings}")


if __name__ == "__main__":
    main()