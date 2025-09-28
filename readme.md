# Analysis System: Open Source Library in APKs

This is a pipeline to research open source libraries used within a corpus of Android APKs. The system is designed for large-scale analysis using [AndroZoo](https://androzoo.uni.lu/) data and aims to detect library usage through static analysis and fingerprint matching.

Version: 0.1.1

---

## Research Goal

The primary goal is to detect open source libraries in **latest-version Android apps** (from Google Play) that belong to **sensitive categories** (e.g., healthcare, finance, mobility, etc.). This is achieved by reverse-engineering APKs and analyzing their class-level structure.

---

## Pipeline Overview

### Step 1: Filter the list of APKs**

- **Source**: `latest_with-added-date.csv` (from AndroZoo)
- **Purpose**: Filter the list of all available APK files by latest version and marketplace (only Google Play).

### 2. **APK Selection and Tagging**

- **Source**: AndroZoo Metadata API
- **Purpose**: Classify APKs by categories using Ollama LLM.

### 3. **APK Downloading**

- **Source**: AndroZoo App API
- **Purpose**: Download APKs in research category from AndroZoo.

### 4. **Decoding APKs**

- **Purpose**: Use `apktool` to extract smali code and manifest files.

### 5. **License Declaration Lookup**

- **Purpose**: Search for any licensing declarations or references to public repositories.

### 6. **Repository Cloning**

- **Purpose**: Clone GitHub/GitLab/Bitbucket repositories (repo_url) that were identified in step 5.

### 7. **Fingerprint Extraction**

- **Purpose**: Extract smali, kotlin, java, or javascript package prefixes or class signatures to be used as fingerprints.
- **Output**: `fingerprints.csv` with adequate reference to library and repsotiry.

### 8. **Fingerprint Matching in APKs**

- **Purpose**: Match previously extracted fingerprints against classes in decoded APKs.
- **Output**: Full report/log of occurrence of any fingerprints in each file.

### 9. **Result Summarization**

- **Purpose**: Aggregate matched evidence into a one-row-per-library summary.
- **Output**: User-freindly summary report for each app: `summary-reports/[app].csv`.

### 10. **Citation Check**

- **Purpose**: Again check each decoded app with found liceses (repo_url lookup).
- **Output**: Added `cited` column to `summary-reports/[app].csv` as boolean.

---

## Output Files

| File/Folder           | Description                           |
| --------------------- | ------------------------------------- |
| `tagged_apps.csv`     | Categorized APK metadata              |
| `apks/`               | Downloaded APK files                  |
| `decoded/`            | Decompiled APKs using `apktool`       |
| `license_lists/`      | CSVs with license or repo mentions    |
| `repos/`              | Cloned repositories of OSS libraries  |
| `repos_manifest.csv`  | Metadata about cloned repos           |
| `fingerprints.csv`    | Fingerprint database from repos       |
| `reports/`            | Per-app CSV reports with matches      |
| `summary-reports/`    | Condensed summary per app/library     |

---

## Requirements

- Python 3.10+
- [`apktool`](https://ibotpeaches.github.io/Apktool/) in `$PATH`
- [AndroZoo API key](https://androzoo.uni.lu/)
- Git (for cloning repos)
- GitHub ssh private key
- [Ollama](https://ollama.com) running locally or remotely for category classification
- `.env` file with necessary environment variables (e.g., API keys, category list)

---

## Running the Workflow

To execute the full research pipeline, ensure that:
- You have duplicated `sample.env` file and renamed it as `.env`. Then configure all required values such as `ANDROZOO_API_KEY`, `OLLAMA_ENDPOINT`, and `RESEARCH_CATEGORY`.
- You have granted execution permission to `workflow.sh` (e.g., `chmod +x workflow.sh`).

Then run:
```bash
bash ./workflow.sh
```

## Citation / Academic Use

This tool was developed as part of academic research into the use of open-source libraries in mobile applications. Please cite appropriately if used in published work.

---

## Author

**Shayan Ghiaseddin**\
Corvinus University of Budapest\
M.Sc. Business Informatics\
Thesis Project – 2025/Q4

