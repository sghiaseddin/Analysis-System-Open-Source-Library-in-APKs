#!/usr/bin/env bash

set -Eeuo pipefail

# Create logs directory if it doesn't exist
mkdir -p ./log

# Generate a timestamped log file name
LOG_FILE="./log/workflow_auto_$(date +%Y-%m-%d_%H-%M-%S).log"

# Redirect stdout and stderr to both terminal and log file
exec > >(tee -a "$LOG_FILE") 2>&1
echo "Logging to $LOG_FILE"

# Load environment variables from .env file
if [ -f .env ]; then
    set -a
    # shellcheck disable=SC1091
    source .env
    set +a
else
    echo ".env file not found. Exiting."
    exit 1
fi

# LIMIT_ARRAY must be defined in .env, for example:
# LIMIT_ARRAY="100,150,200,300,500,700,1000"
if [ -z "${LIMIT_ARRAY:-}" ]; then
    echo "LIMIT_ARRAY is not defined in .env. Example: LIMIT_ARRAY=\"100,150,200,300,500,700,1000\""
    exit 1
fi

IFS=',' read -r -a LIMITS <<< "$LIMIT_ARRAY"

run_step() {
    local description="$1"
    shift

    echo ""
    echo ">> $description"
    "$@"
}

prepare_empty_output_folders() {
    rm -rf ./aggregate-reports ./summary-reports ./reports
    mkdir -p ./aggregate-reports ./summary-reports ./reports
}

archive_iteration_outputs() {
    local limit="$1"
    local archive_dir="./aggregate-reports_${limit}"

    echo ""
    echo ">> Archiving aggregate reports for limit ${limit}"

    rm -rf "$archive_dir"

    if [ -d ./aggregate-reports ]; then
        cp -a ./aggregate-reports "$archive_dir"
    else
        echo "Warning: ./aggregate-reports was not found. Creating empty archive directory: $archive_dir"
        mkdir -p "$archive_dir"
    fi

    echo ">> Resetting aggregate-reports, summary-reports, and reports folders"
    prepare_empty_output_folders

    echo ">> Deleting ./database/fingerprints.csv"
    rm -f ./database/fingerprints.csv
}

# Ensure the report folders exist before the first iteration.
mkdir -p ./aggregate-reports ./summary-reports ./reports

for raw_limit in "${LIMITS[@]}"; do
    # Trim spaces around each value from LIMIT_ARRAY.
    limit="$(echo "$raw_limit" | xargs)"

    if [ -z "$limit" ]; then
        echo "Skipping empty LIMIT_ARRAY item."
        continue
    fi

    LIMIT_ARGS=(--limit "$limit")

    echo ""
    echo "============================================================"
    echo "Starting automated workflow iteration with limit: $limit"
    echo "============================================================"

    # Step 1: Filter the list of all available APK files by latest version and marketplace.
    # input: latest_with-added-date.csv from https://androzoo.uni.lu/api_doc
    # output: latest_playstore_per_pkg.csv
    # run_step "Step 1: Filter the list of APKs" \
    #     python ./src/extract_latest_playstore.py \
    #         --input-data ./database/latest_with-added-date.csv \
    #         --output-data ./database/latest_playstore_per_pkg.csv \
    #         --chunksize 300000 \
    #         --log-every 100000

    # Step 2: Tag APKs using scraper requesting Google Play Store.
    # Also scans existing APK files and includes custom APK files added to ./data/apks.
    # input: latest_playstore_per_pkg.csv
    # input: ./data/apks
    # output: tagged_apps.csv
    run_step "Step 2: Tag APKs using scraper requesting Google Play Store" \
        python ./src/tag_apps_by_play_store_scraper.py \
            --input-data ./database/latest_playstore_per_pkg.csv \
            --input-dir ./data/apks \
            --output-data ./database/tagged_apps.csv \
            --excluded-apps "${EXCLUDED_APS:-}" \
            --log-every 10 \
            --research-categories "${RESEARCH_CATEGORY:-}" \
            "${LIMIT_ARGS[@]}"

    # Step 3: Download APK files from https://androzoo.uni.lu/api_doc.
    # input: tagged_apps.csv
    # output: apks/[sha256].apk
    run_step "Step 3: Download APKs" \
        python ./src/download_apks.py \
            --apps-data ./database/tagged_apps.csv \
            --output-dir ./data/apks \
            --log-every 10 \
            --research-categories "${RESEARCH_CATEGORY:-}" \
            --apikey "${ANDROZOO_API_KEY:-}" \
            "${LIMIT_ARGS[@]}"

    # Step 4: Decode APK files using https://apktool.org/.
    # input: apks/[pkg_name].apk
    # output: decoded/[pkg_name] directory
    run_step "Step 4: Decode APKs" \
        python ./src/decode_apks.py \
            --input-dir ./data/apks \
            --output-dir ./data/decoded \
            --log-every 10 \
            "${LIMIT_ARGS[@]}"

    # Step 5: Find every mentioned license in decoded files.
    # input: decoded/[pkg_name]
    # output: license_lists/[pkg_name].csv
    run_step "Step 5: Find licenses" \
        python ./src/find_open_source_library.py \
            --input-dir ./data/decoded \
            --output-dir ./database/license_lists \
            --log-every 10 \
            "${LIMIT_ARGS[@]}" \
            --force

    # Step 6: Clone libraries using GitHub credentials.
    # input: license_lists/[pkg_name].csv
    # output: repos_manifest.csv
    # output: repos directory
    run_step "Step 6: Clone Git Repos" \
        python ./src/clone_lib_repos.py \
            --input-dir ./database/license_lists \
            --output-data ./database/repos_manifest.csv \
            --output-dir ./data/repos \
            --path-ssh-key "${GITHUB_SSH_KEY_PATH:-}" \
            --workers 6 \
            --log-every 10

    # Step 7: Make fingerprints by looking inside repos and finding package declaration.
    # input: repos_manifest.csv
    # input: repos
    # output: fingerprints.csv
    run_step "Step 7: Make library fingerprints" \
        python ./src/make_fingerprints.py \
            --input-data ./database/repos_manifest.csv \
            --input-dir ./data/repos \
            --output-data ./database/fingerprints.csv \
            --workers 6 \
            --max-files 8000 \
            --log-every 10 \
            --force

    # Step 8: Match fingerprints in decoded APK files and report if any are found.
    # input: fingerprints.csv
    # input: decoded
    # output: classes_index
    # output: reports/[pkg_name].csv
    run_step "Step 8: Match fingerprints in decoded APKs" \
        python ./src/match_fingerprints_in_apks.py \
            --input-dir ./data/decoded \
            --input-data ./database/fingerprints.csv \
            --output-dir ./data/classes_index \
            --output-dir2 ./reports \
            --workers 6 \
            --log-every 1

    # Step 9: Aggregate reports and make a summary report for each app.
    # input: reports directory
    # output: summary-reports/[pkg_name].csv
    run_step "Step 9: Summarize report results" \
        python ./src/summarize_reports.py \
            --input-dir ./reports \
            --output-dir ./summary-reports \
            --workers 6 \
            --log-every 10

    # Step 10: Check and search decoded APK for any cited license URL.
    # input: summary-reports directory
    # output: summary-reports/[pkg_name].csv
    run_step "Step 10: Check license citation" \
        python ./src/check_license_citation.py \
            --input-dir ./summary-reports \
            --input-dir2 ./data/decoded \
            --excluded-apps "${EXCLUDED_APS:-}" \
            --workers 6 \
            --log-every 10

    # Step 11: Aggregate summary reports and license lists.
    # input: summary-reports directory
    # input: license_lists directory
    # output: aggregate-reports/[pkg_name].csv
    run_step "Step 11: Aggregate summary reports and license lists" \
        python ./src/aggregate_reports.py \
            --input-dir ./summary-reports \
            --input-dir2 ./database/license_lists \
            --output-dir ./aggregate-reports \
            --workers 6 \
            --log-every 10

    # Step 12: Merge aggregate reports into one analysis dataset.
    # input: aggregate-reports directory
    # output: analysis_datasets dataset for this limit
    run_step "Step 12: Create analysis dataset" \
        python ./src/create_analysis_dataset.py \
            --input-dir ./aggregate-reports \
            --output-dir ./analysis_datasets \
            "${LIMIT_ARGS[@]}"

    archive_iteration_outputs "$limit"

    echo ""
    echo "Completed automated workflow iteration with limit: $limit"
done

echo ""
echo "All automated workflow iterations completed successfully."
