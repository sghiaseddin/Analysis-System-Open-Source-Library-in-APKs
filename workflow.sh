#!/bin/bash

# Create logs directory if it doesn't exist
mkdir -p ./log

# Generate a timestamped log file name
LOG_FILE="./log/workflow_$(date +%Y-%m-%d_%H-%M-%S).log"

# Redirect stdout and stderr to both terminal and log file
exec > >(tee -a "$LOG_FILE") 2>&1
echo "Logging to $LOG_FILE"

# Load environment variables from .env file
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
else
    echo ".env file not found. Exiting."
    exit 1
fi

# Ask for global limit
read -p "Do you want to set a global --limit for all steps? (y/n): " set_limit
LIMIT_ARG=""
if [[ "$set_limit" == "y" ]]; then
    read -p "Enter the limit value: " limit_value
    LIMIT_ARG="--limit $limit_value"
fi

function step() {
    echo ""
    echo ">> $1"
    while true; do
        read -p "Proceed? (y/n/q): " confirm
        # normalize to lowercase
        case "${confirm,,}" in
            y|yes)
                return 0
                ;;
            n|no)
                return 1
                ;;
            q|quit)
                echo "Quitting."
                exit 0
                ;;
            *)
                echo "Please answer y, n, or q."
                ;;
        esac
    done
}

# Filter the list of all available APK files by latest version and marketplace (only Google Play)
# input: latest_with-added-date.csv from https://androzoo.uni.lu/api_doc
# output: latest_playstore_per_pkg.csv
if step "Step 1: Filter the list of APKs"; then
    python ./src/extract_latest_playstore.py --input-data ./database/latest_with-added-date.csv --output-data ./database/latest_playstore_per_pkg.csv   --chunksize 300000 --log-every 100000  || exit 1
else
    echo "Skipping: Step 1"
fi

# Fetch metadata from https://androzoo.uni.lu/gp-metadata
# Prompt Ollama with app description and tag the apps in desired categories
# input: latest_playstore_per_pkg.csv
# output: metadata/[sha256].json --> app's metadata
# output: tagged_apps.csv
#todo --> get androzoo apikey from .env file
if step "Step 2: Tag APKs with Ollama AI model"; then
    python ./src/tag_apps_with_ollama.py --input-data ./database/latest_playstore_per_pkg.csv --output-data ./database/tagged_apps.csv --ollama-endpoint $OLLAMA_ENDPOINT --ollama-model $OLLAMA_MODEL --log-every 10 --output-dir ./data/metadata --apikey $ANDROZOO_API_KEY --research-categories $RESEARCH_CATEGORY $LIMIT_ARG || exit 1
else
    echo "Skipping: Step 2"
fi

# Download apk files from https://androzoo.uni.lu/api_doc
# input: tagged_apps.csv
# output: apks/[sha256].apk
if step "Step 3: Download APKs"; then
    python ./src/download_apks.py --input-data ./database/tagged_apps.csv --output-dir ./data/apks --threshold 0.90 --log-every 10 --research-categories $RESEARCH_CATEGORY --apikey $ANDROZOO_API_KEY $LIMIT_ARG || exit 1
else
    echo "Skipping: Step 3"
fi

# Decode apk files using https://apktool.org/
# input: apks/[sha256].apk
# output: decoded/[sha256] --> directory
if step "Step 4: Decode APKs"; then
    python ./src/decode_apks.py --input-dir ./data/apks --output-dir ./data/decoded --log-every 10 $LIMIT_ARG || exit 1
else
    echo "Skipping: Step 4"
fi

# Find every mentioned license in decoded files
# input: decoded/[sha256]
# output: license_lists/[sha256].csv --> lists of licenses in each app
if step "Step 5: Find licenses"; then
    python ./src/find_open_source_library.py --input-dir ./data/decoded --output-dir ./database/license_lists --log-every 10 $LIMIT_ARG || exit 1
else
    echo "Skipping: Step 5"
fi

# Clone libraries using GitHub credentials #todo avaialable in the .env file
# input: license_lists/[sha256].csv
# output: repos_manifest.csv
# output: repos --> directory
if step "Step 6: Clone Git Repos"; then
    python ./src/clone_lib_repos.py --input-dir ./database/license_lists --output-data ./database/repos_manifest.csv --output-dir ./data/repos --path-ssh-key $GITHUB_SSH_KEY_PATH --workers 6 --log-every 10 || exit 1
else
    echo "Skipping: Step 6"
fi

# Make fingerprints by looking inside the repos and finding package declaraion
# input: repos_manifest.csv
# input: repos
# output: fingerprints.csv
if step "Step 7: Make library fingerprints"; then
    python ./src/make_fingerprints.py --input-data ./database/repos_manifest.csv --input-dir ./data/repos --output-data ./database/fingerprints.csv --workers 6 --max-files 8000 --force || exit 1
else
    echo "Skipping: Step 7"
fi

# Match fingerprints in the decoded apk files and report if find any
# input: fingerprints.csv
# input: decoded
# output: classes_evidence_list
# output: reports/[sha256].csv
if step "Step 8: Match fingerprints in decoded APKs"; then
    python ./src/match_fingerprints_in_apks.py --input-dir ./data/decoded --input-data ./database/fingerprints.csv  --output-dir ./data/classes_evidence_list --output-dir2 ./reports --workers 6 --log-every 10 || exit 1
else
    echo "Skipping: Step 8"
fi

# Aggregate reports and make a summary report for each app
# input: reports --> directory
# output: summary-reports/[sha256].csv
if step "Step 9: Summarize report results"; then
    python ./src/summarize_reports.py --input-dir ./reports --output-dir ./summary-reports --workers 6 --log-every 10 || exit 1
else
    echo "Skipping: Step 9"
fi

# Check and search decoded apk for any cited license url
# input: summary-reports --> directory
# output: summary-reports/[sha256].csv
if step "Step 10: Check license citation"; then
    python ./src/check_license_citation.py --input-dir ./summary-reports --input-dir2 ./data/decoded --workers 6 --log-every 10 || exit 1
else
    echo "Skipping: Step 10"
fi
