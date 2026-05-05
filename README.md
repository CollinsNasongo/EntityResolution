# EntityResolver

A modular Python framework for ingesting, parsing, standardizing, and preparing datasets for entity resolution.

---

## 🚀 Overview

EntityResolver provides a structured pipeline to:

* Ingest data from HTTP files and APIs
* Safely download and store raw data
* Parse multiple file formats into a unified structure
* Apply schema mappings
* Clean and normalize data using configurable rules
* Generate unique identifiers for downstream matching
* Track ingestion and schema changes over time

---

## 📦 Project Structure

```
entityresolver/
├── connectors/     # HTTP utilities
├── download/       # Stream download logic (with retries, checksum)
├── ingestion/      # Ingestion manager + manifest tracking
├── sources/        # Data sources (HTTP files, APIs)
├── parsing/        # File parsing (CSV, JSON, ZIP, etc.)
├── mapping/        # Column mapping utilities
├── cleaning/       # Normalization + identity generation
├── schema/         # Schema extraction, registry, diffing
├── utils/          # Filesystem + config helpers
```

---

## 🔧 Features

### 1. Ingestion Layer

* Unified `ingest()` interface
* Retry-safe downloads using stream factories
* Temp file safety (prevents partial writes)
* Manifest tracking (attempts, status, metadata)
* Idempotent ingestion with overwrite control

---

### 2. Sources

* `HttpFileSource` → download files from URLs
* `ApiSource` → fetch API data and stream as NDJSON
* Supports retries, headers, params, and pagination

---

### 3. Download Engine

* Chunked streaming downloads
* Progress tracking (tqdm)
* Checksum validation (optional)
* Automatic retries with exponential backoff

---

### 4. Parsing Layer

* Unified loader: `load_dataframe()`
* Supports:

  * CSV / TXT / TSV
  * JSON
  * Excel
  * Parquet
  * ZIP (auto-extract + load)

---

### 5. Mapping

* Apply column mappings:

  ```python
  apply_mapping(df, mapping)
  ```
* Load mappings from JSON config
* Validate mappings against source schema

---

### 6. Cleaning

* Config-driven normalization (rules-based)
* Standard string cleaning (trim, lowercase, null handling)
* Reusable identity generation:

  * UUID-based unique IDs

---

### 7. Schema Management

* Extract schema from DataFrame:

  * columns
  * dtypes
  * row count
* Save schema as JSON
* Registry to track schema history
* Diff schemas to detect structural changes

---

### 8. Utilities

* File system helpers:

  * list files
  * safe delete
  * move/copy files
  * file validation
* JSON config loader

---

## 🧪 Example Usage

### Ingest + Parse + Schema Extraction

```python
from pathlib import Path

from entityresolver.sources import HttpFileSource
from entityresolver.ingestion import ingest, Manifest
from entityresolver.parsing import load_dataframe
from entityresolver.schema import extract_schema

source = HttpFileSource("https://example.com/data.csv")

manifest = Manifest(Path("data/manifest.json"))

file_path = ingest(
    source=source,
    destination=Path("data/data.csv"),
    manifest=manifest,
)

df = load_dataframe(file_path)

schema = extract_schema(df)
```

---

## 📁 Data Flow

```
Source → Ingestion → Raw File
        ↓
     Parsing → DataFrame
        ↓
     Mapping → Standardized Data
        ↓
     Cleaning → Normalized Data
        ↓
     Identity → Matching-ready Data
```

---

## ⚙️ Configuration

* Mapping configs are stored as JSON files
* Cleaning rules are configurable
* Pipeline behavior can be adjusted without changing code

---

## 🧱 Current Scope

This project currently focuses on:

* Data ingestion
* Data parsing
* Data standardization
* Data cleaning
* Schema tracking

Matching and entity resolution engines are **not yet implemented**.

---

## 🧪 Testing

Tests cover:

* Downloader (streaming, retries, checksum)
* HTTP connector
* Ingestion manager
* Manifest tracking

Run tests with:

```bash
pytest
```

---

## 📌 Notes

* Designed for modular extension (new sources, formats, pipelines)
* Built with large datasets in mind (streaming + chunking)
* Compatible with orchestration tools like Airflow

---

## 📄 License

TBD