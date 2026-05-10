# Analysis System: Open Source Libraries in APKs

This repository contains an experimental pipeline for identifying open-source libraries embedded in Android APK files. The system was developed for thesis research on dependency transparency in Android applications. It combines license declaration lookup, repository-based fingerprint generation, static APK analysis, citation checking, and GitHub-based repository metrics.

Version: 1.0.1

---

## Research Goal

The main goal is to detect open-source libraries inside compiled Android APKs, especially when the original application source code is not available. The pipeline can analyze a corpus of APKs from AndroZoo, but it can also accept standalone APK files when a specific app needs to be inspected.

The final output helps answer questions such as:

- Which open-source libraries are present in an APK?
- Which libraries are declared through license information?
- Which libraries are detected only through fingerprints?
- Which libraries appear more fragile based on repository metadata?
- Which APKs show higher intensity of uncited fragile-library usage?

---

## Pipeline Logic

The pipeline is based on a license-first and fingerprint-based workflow.

First, decoded APKs are searched for license declarations and repository URLs. When a public repository is found, the repository is cloned and used as the source of truth for generating package-based fingerprints. These fingerprints are then matched against all decoded APKs. If a known library fingerprint appears in an APK, the library is treated as present in that application.

This makes it possible to detect both:

- **cited libraries**: libraries that are declared in license information;
- **uncited libraries**: libraries detected in code but not declared in the APK license evidence.

---

## Pipeline Overview

### 1. Filter AndroZoo Metadata

- **Source**: AndroZoo metadata CSV
- **Purpose**: Keep latest-version APKs from Google Play and reduce the input list to apps relevant to the research categories.

### 2. Classify and Select APKs

- **Source**: Google Play category data
- **Purpose**: Assign categories such as finance, medical, health and fitness, maps and navigation, and communication.

### 3. Download APKs

- **Source**: AndroZoo API
- **Purpose**: Download APK files selected for the corpus.

### 4. Add Standalone APKs When Needed

- **Purpose**: Allow a specific APK to be injected into the corpus for targeted analysis and comparison with the larger dataset.

### 5. Decode APKs

- **Tool**: `apktool`
- **Purpose**: Decode APK files into readable project folders containing manifest files, resources, assets, and smali code.

### 6. Extract License and Repository Evidence

- **Purpose**: Search decoded APKs for license files, license declarations, package notices, and public repository URLs.
- **Output**: Per-app license evidence files.

### 7. Clone Public Repositories

- **Source**: GitHub, GitLab, and Bitbucket URLs found in license evidence
- **Purpose**: Download source code of declared open-source libraries.

### 8. Generate Library Fingerprints

- **Purpose**: Extract package prefixes and class identifiers from cloned library source code.
- **Languages supported**: Java, Kotlin, smali, and JavaScript.
- **Output**: `fingerprints.csv`.

### 9. Match Fingerprints Against APKs

- **Purpose**: Scan decoded APKs and match known library fingerprints against smali class paths and package evidence.
- **Interpretation**: A matched fingerprint is treated as evidence that the library is embedded in the APK.

### 10. Aggregate Library Evidence

- **Purpose**: Produce one-row-per-APK-per-library summaries with evidence such as matched classes, sample class path, library name, repository URL, and detection origin.

### 11. Check Citation Status

- **Purpose**: Mark whether each detected library was declared through license evidence.
- **Key field**: `cited`.

### 12. Build Final Analysis Dataset

- **Purpose**: Merge APK-level evidence, citation status, repository metadata, and repository metrics into the final dataset used for statistical analysis and visualizations.

### 13. Collect GitHub Metadata

- **Purpose**: Enrich detected libraries with repository-level metadata such as stars, forks, subscribers, issues, archived status, disabled status, update recency, push recency, license information, and repository age.

### 14. Calculate Repository Metrics

- **Metrics**:
  - **Popularity**: combines APK usage and GitHub visibility.
  - **Activity**: estimates maintenance activity using recent push/update signals.
  - **Fragility**: estimates potential maintenance or governance risk.

---

## Main Outputs

| File/Folder | Description |
| --- | --- |
| `tagged_apps.csv` | APK metadata with research categories |
| `apks/` | Downloaded APK files |
| `decoded/` | APKs decoded with `apktool` |
| `license_lists/` | Per-app license and repository evidence |
| `repos/` | Cloned repositories of detected open-source libraries |
| `repos_manifest.csv` | Metadata about cloned repositories |
| `fingerprints.csv` | Library fingerprint database |
| `reports/` | Per-app fingerprint match reports |
| `summary-reports/` | User-friendly per-app library summaries |
| `aggregate-reports/` | Aggregated per-app/per-library evidence |
| `analysis_datasets/` | Merged datasets used for analysis |
| `analysis_outputs/` | CSV and PNG outputs from analysis notebooks |

---

## Key Dataset Fields

Common fields in the generated datasets include:

| Field | Meaning |
| --- | --- |
| `pkg_name` | Android application package name |
| `library_name` | Human-readable library name |
| `library_key` | Normalized library identifier |
| `repo_url` | Public source repository URL |
| `cited` | Whether the library was declared in license evidence |
| `origin` | Detection source, such as license, fingerprint, or both |
| `smali_prefix` | Package/class prefix used for matching |
| `fingerprint_types` | Fingerprint source type, such as Java or Kotlin |
| `classes_matched` | Number of matched classes in the decoded APK |
| `sample_class` | Example matched class |
| `sample_class_file` | File path of example evidence in the decoded APK |
| `popularity_score` | Composite repository popularity score |
| `activity_score` | Composite repository activity score |
| `fragility_score` | Composite repository fragility score |
| `fragility_level` | Low, medium, or high fragility category |

---

## Requirements

- Python 3.10+
- `apktool` available in `$PATH`
- AndroZoo API key
- Git
- GitHub SSH/private key or equivalent access for repository cloning
- `.env` file with required environment variables

---

## Running the Workflow

Create and configure the environment file:

```bash
cp sample.env .env
```

Configure values such as:

```env
ANDROZOO_API_KEY=...
GITHUB_TOKEN=...
GITHUB_SSH_KEY_PATH=...
RESEARCH_CATEGORY=...
```

Give execution permission to the workflow script:

```bash
chmod +x workflow.sh
```

Run the full pipeline:

```bash
bash ./workflow.sh
```

The workflow script coordinates the full process and executes the relevant Python scripts step by step. Intermediate results are stored as CSV files so that each stage remains readable, inspectable, and reusable in later stages.

---

## Practical Use Case

A user can provide a standalone APK, add it to the corpus, and run the workflow to inspect its open-source library ingredients. The pipeline can then report which libraries are present, which ones are cited or uncited, and which detected repositories appear more fragile based on GitHub metadata.

---

## Limitations

- The pipeline uses static analysis and cannot prove runtime execution of a library.
- Obfuscation, repackaging, and partial imports may affect detection accuracy.
- Fingerprint coverage depends on repositories discovered from license evidence.
- Corpus size is limited by hardware resources and lookup time.
- Manual validation is difficult when apps do not expose in-app open-source license pages.

---

## Citation / Academic Use

This tool was developed as part of academic research into open-source library usage and disclosure gaps in Android APKs. Please cite appropriately if used in published work.

---

## Author

**Shayan Ghiaseddin**  
Corvinus University of Budapest  
M.Sc. Business Informatics  
Thesis Project – 2026/Q2