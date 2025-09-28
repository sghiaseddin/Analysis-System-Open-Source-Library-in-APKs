#!/usr/bin/env python3
import argparse
import os
import re
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from subprocess import run, PIPE

APK_SHA256_RE = re.compile(r"([A-Fa-f0-9]{64})")

def log(msg: str, *, flush=True):
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {msg}", flush=flush)

def check_apktool() -> bool:
    try:
        proc = run(["apktool"], stdout=PIPE, stderr=PIPE, text=True)
        # Any return proves it exists; version flag not required
        return True
    except FileNotFoundError:
        return False

def extract_sha256_from_name(p: Path) -> str | None:
    m = APK_SHA256_RE.search(p.name)
    return m.group(1).lower() if m else None

def is_already_decoded(outdir: Path) -> bool:
    # Heuristic: main folder exists and has expected files/folders
    if not outdir.exists():
        return False
    # AndroidManifest.xml is a good signal
    if (outdir / "AndroidManifest.xml").exists():
        return True
    # Some versions create smali*/res folders before manifest is written;
    # treat non-empty as decoded to avoid rework unless --force
    try:
        return any(outdir.iterdir())
    except Exception:
        return False

def decode_one(apk_path: Path, decoded_root: Path, force: bool, quiet: bool) -> tuple[str, bool, str]:
    """Return (sha256, success, message)."""
    sha = extract_sha256_from_name(apk_path)
    if not sha:
        return ("", False, f"Skip {apk_path.name}: cannot find 64-hex sha256 in filename")

    outdir = decoded_root / sha
    if is_already_decoded(outdir) and not force:
        return (sha, True, "already decoded (skipped)")

    if outdir.exists() and force:
        try:
            shutil.rmtree(outdir)
        except Exception as e:
            return (sha, False, f"failed to remove existing dir: {e}")

    # apktool decode
    # -f: force overwrite inside output dir (we already removed if force)
    # -o: output dir
    cmd = ["apktool", "d", "-o", str(outdir), str(apk_path)]
    if quiet:
        cmd.insert(2, "-q")

    try:
        proc = run(cmd, stdout=PIPE, stderr=PIPE, text=True, timeout=1800)  # 30 min per APK guard
        if proc.returncode != 0:
            # Clean up partial dirs on failure
            try:
                if outdir.exists() and not is_already_decoded(outdir):
                    shutil.rmtree(outdir)
            except Exception:
                pass
            return (sha, False, f"apktool error: {proc.stderr.strip()[:500]}")
    except Exception as e:
        try:
            if outdir.exists() and not is_already_decoded(outdir):
                shutil.rmtree(outdir)
        except Exception:
            pass
        return (sha, False, f"exception: {e}")

    return (sha, True, "decoded")

def main():
    ap = argparse.ArgumentParser(description="Decode APKs with apktool into ./decoded/{sha256}")
    ap.add_argument("--input-dir", default="apks", help="Directory containing APK files (default: ./apks)")
    ap.add_argument("--output-dir", default="decoded", help="Output root for decoded folders (default: ./decoded)")
    ap.add_argument("--limit", type=int, default=100, help="Max APKs to process (default 100)")
    ap.add_argument("--force", action="store_true", help="Re-decode even if output exists")
    ap.add_argument("--log-every", type=int, default=20, help="Log every N processed (default 20)")
    ap.add_argument("--workers", type=int, default=1, help="Parallel workers (default 1; increase carefully)")
    ap.add_argument("--quiet", action="store_true", help="Pass -q to apktool to reduce noise")
    args = ap.parse_args()

    if not check_apktool():
        print("ERROR: apktool not found in PATH. Install it and try again.", file=sys.stderr)
        sys.exit(1)

    apkdir = Path(args.input_dir)
    decoded_root = Path(args.output_dir)
    decoded_root.mkdir(parents=True, exist_ok=True)

    if not apkdir.exists():
        print(f"ERROR: APK dir not found: {apkdir}", file=sys.stderr)
        sys.exit(1)

    # Gather APKs (any file ending in .apk or containing a 64-hex sha)
    apks = []
    for p in sorted(apkdir.iterdir()):
        if not p.is_file():
            continue
        if p.suffix.lower() == ".apk" or APK_SHA256_RE.search(p.name):
            apks.append(p)

    if not apks:
        log(f"No APKs found in {apkdir}")
        return

    # Apply limit
    if args.limit and len(apks) > args.limit:
        apks = apks[:args.limit]

    log(f"Start decoding: {len(apks)} APK(s). Output root: {decoded_root.resolve()}  Workers={args.workers}")

    processed = 0
    ok = 0
    skipped = 0
    failed = 0

    if args.workers > 1:
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futs = {ex.submit(decode_one, p, decoded_root, args.force, args.quiet): p for p in apks}
            for fut in as_completed(futs):
                sha, success, msg = fut.result()
                processed += 1
                if "already decoded" in msg:
                    skipped += 1
                elif success:
                    ok += 1
                else:
                    failed += 1
                if processed % args.log_every == 0:
                    log(f"Processed={processed}  OK={ok}  Skipped={skipped}  Failed={failed}")
                if not success:
                    log(f"  {sha or futs[fut].name}: {msg}")
    else:
        for p in apks:
            sha, success, msg = decode_one(p, decoded_root, args.force, args.quiet)
            processed += 1
            if "already decoded" in msg:
                skipped += 1
            elif success:
                ok += 1
            else:
                failed += 1
                log(f"  {sha or p.name}: {msg}")
            if processed % args.log_every == 0:
                log(f"Processed={processed}  OK={ok}  Skipped={skipped}  Failed={failed}")

    log(f"Done. Total={processed}  OK={ok}  Skipped={skipped}  Failed={failed}")
    log(f"Decoded path: {decoded_root.resolve()}")

if __name__ == "__main__":
    main()