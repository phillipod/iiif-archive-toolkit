# PRONI Archive Retrieval

Download and search digitised historical archive documents from IIIF-compatible archives, with a focus on PRONI (Public Record Office of Northern Ireland) records.

## Features

- **Download** page images and OCR annotations from IIIF Presentation API v2/v3 manifests
- **Search** locally with boolean queries, phrase matching, and fuzzy OCR-tolerant matching
- Builds a SQLite database with FTS5 full-text search for fast offline querying
- Handles Irish name variations (O'/Mc/Mac prefixes, apostrophe handling)
- Parallel downloads with retry logic

## Install

```sh
pip install .
```

Or for development:

```sh
pip install -e .
```

## Usage

### Download an archive

Provide a local manifest JSON file or a remote URL:

```sh
proni-download /path/to/proni-manifest.json
proni-download https://example.org/manifest.json --output ./my-archive
```

Options:

```
-o, --output DIR         Base output directory (default: ./downloads)
-w, --workers N          Parallel image downloads (default: 6)
--timeout SECS           HTTP timeout in seconds (default: 120)
--overwrite              Re-download existing files
--no-annotations         Skip annotations/text
--no-sqlite              Skip SQLite database creation
--no-search-index        Skip term index
```

### Search the downloaded archive

```sh
proni-search ./downloads/my-archive/archive.db "John O'Neill"
proni-search ./downloads/my-archive/archive.db "smith AND belfast"
proni-search ./downloads/my-archive/archive.db "\"townland name\" OR parish"
```

Options:

```
--table {auto,both,lines,annotations,terms}   Tables to search (default: auto)
--page N               Restrict to a single page
--limit N              Max hits per source
--global-limit N       Max hits across all sources
--width N              Snippet width (default: 140)
--fuzzy                Enable fuzzy matching for OCR errors
--max-distance N       Max edit distance for fuzzy matching (default: 1)
--force-scan           Bypass FTS, scan all rows
--list-tables          List tables in the database
--schema               Show table schemas
```

### Fuzzy search

OCR text often contains errors. Enable fuzzy matching to tolerate minor misspellings:

```sh
proni-search ./downloads/my-archive/archive.db --fuzzy --max-distance 2 "donnelly"
```

## Output structure

Each manifest is downloaded into its own folder:

```
downloads/
  {ReferenceCode} - {Title} - {Date}/
    manifest.json          # Original IIIF manifest
    metadata.json          # Extracted metadata
    pages.csv              # Page index
    annotations.csv        # All OCR annotations
    lines.csv              # Reconstructed text lines
    search_index.csv       # Term frequency index
    ocr.txt                # Combined OCR text
    archive.db             # SQLite database (FTS5 search)
    images/                # Page images
    annotations/           # Per-page annotation JSON
    texts/                 # Per-page extracted text
```

## Requirements

- Python 3.9+
- `requests` (installed automatically)
- SQLite with FTS5 support (included with most Python builds)

## License

GPL-3.0-or-later
