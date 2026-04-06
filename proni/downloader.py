#!/usr/bin/env python3
"""
Download images and annotations from a IIIF Presentation v2/v3 manifest and store
them in a well-organised local folder.

Designed for PRONI-style manifests but works with any IIIF-compatible archive.

Features:
- Accepts a local manifest JSON file or a manifest URL
- Extracts title/reference/date metadata where available
- Creates a clean folder structure for each manifest
- Downloads page images with stable, sortable filenames
- Downloads per-page annotation lists where exposed by the manifest
- Writes metadata.json, manifest.json, pages.csv, and annotation indexes
- Produces per-page text extracts and combined OCR text output
- Builds a generic searchable term index (not limited to parishes/townlands)
- Builds a SQLite archive with raw rows plus full-text search where available
- Skips files that already exist unless --overwrite is used
- Retries transient HTTP failures
- Can download in parallel

Example:
    proni-download /path/to/proni-manifest.json
    proni-download https://example.org/manifest --output ./downloads
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sqlite3
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

try:
    import requests
except ImportError:
    print("This script requires the 'requests' package. Install it with: pip install requests", file=sys.stderr)
    raise

USER_AGENT = "proni-iiif-downloader/1.2"


def sanitize_filename(value: str, max_length: int = 120) -> str:
    value = value.strip()
    value = re.sub(r"[<>:\"/\\|?*\x00-\x1f]", "_", value)
    value = re.sub(r"\s+", " ", value)
    value = value.strip(" .")
    if not value:
        value = "untitled"
    if len(value) > max_length:
        value = value[:max_length].rstrip(" .")
    return value


def metadata_to_dict(metadata: Any) -> Dict[str, str]:
    result: Dict[str, str] = {}
    if isinstance(metadata, list):
        for item in metadata:
            if not isinstance(item, dict):
                continue
            key = flatten_value(item.get("label"))
            val = flatten_value(item.get("value"))
            if key:
                result[key] = val
    return result


def flatten_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        parts = [flatten_value(v) for v in value]
        return " | ".join(p for p in parts if p)
    if isinstance(value, dict):
        if "none" in value or "en" in value:
            parts = []
            for _, vals in value.items():
                if isinstance(vals, list):
                    parts.extend(flatten_value(v) for v in vals)
                else:
                    parts.append(flatten_value(vals))
            return " | ".join(p for p in parts if p)
        parts = [flatten_value(v) for v in value.values()]
        return " | ".join(p for p in parts if p)
    return str(value).strip()


def normalize_for_search(text: str) -> str:
    text = (text or "").lower()
    text = re.sub(r"[^\w\s']+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def tokenize_for_search(text: str) -> List[str]:
    norm = normalize_for_search(text)
    tokens: List[str] = []
    for token in norm.split():
        token = token.strip("'")
        if len(token) < 3:
            continue
        if token.isdigit():
            continue
        tokens.append(token)
    return tokens


def guess_extension_from_url(url: str, default: str = ".jpg") -> str:
    path = urlparse(url).path.lower()
    for ext in [".jpg", ".jpeg", ".png", ".tif", ".tiff", ".jp2", ".webp"]:
        if path.endswith(ext):
            return ext
    return default


def parse_xywh(target: str) -> Tuple[Optional[int], Optional[int], Optional[int], Optional[int]]:
    if not target or "#xywh=" not in target:
        return None, None, None, None
    fragment = target.split("#xywh=", 1)[1]
    parts = fragment.split(",")
    if len(parts) != 4:
        return None, None, None, None
    try:
        return tuple(int(float(p)) for p in parts)  # type: ignore[return-value]
    except ValueError:
        return None, None, None, None


def load_json_source(source: str, timeout: int = 60) -> Tuple[Dict[str, Any], str]:
    if re.match(r"^https?://", source, flags=re.I):
        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT})
        resp = session.get(source, timeout=timeout)
        resp.raise_for_status()
        return resp.json(), source

    path = Path(source)
    with path.open("r", encoding="utf-8") as f:
        return json.load(f), str(path.resolve())


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    return session


def request_with_retries(
    session: requests.Session,
    url: str,
    *,
    stream: bool = False,
    timeout: int = 120,
    retries: int = 4,
    backoff: float = 1.5,
) -> requests.Response:
    last_exc: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, stream=stream, timeout=timeout)
            resp.raise_for_status()
            return resp
        except Exception as exc:
            last_exc = exc
            if attempt == retries:
                break
            time.sleep(backoff ** (attempt - 1))
    assert last_exc is not None
    raise last_exc


def choose_output_root(manifest: Dict[str, Any], output_base: Path) -> Path:
    meta = metadata_to_dict(manifest.get("metadata", []))
    title = flatten_value(manifest.get("label")) or meta.get("Title") or "Untitled manifest"
    ref = meta.get("ReferenceCode") or meta.get("Reference Code") or meta.get("Reference")
    date = meta.get("Content Date") or meta.get("Date")

    parts = []
    if ref:
        parts.append(sanitize_filename(ref))
    parts.append(sanitize_filename(title))
    if date:
        parts.append(sanitize_filename(date))

    dirname = " - ".join(p for p in parts if p)
    return output_base / dirname


def extract_v2_pages(manifest: Dict[str, Any]) -> List[Dict[str, Any]]:
    pages: List[Dict[str, Any]] = []
    sequences = manifest.get("sequences", [])
    for seq_index, sequence in enumerate(sequences, start=1):
        canvases = sequence.get("canvases", [])
        for canvas_index, canvas in enumerate(canvases, start=1):
            canvas_label = flatten_value(canvas.get("label")) or f"canvas-{canvas_index}"
            width = canvas.get("width")
            height = canvas.get("height")
            image_url = None
            service_id = None
            annotation_urls: List[str] = []

            for image_anno in canvas.get("images", []):
                resource = image_anno.get("resource", {})
                image_url = resource.get("@id") or resource.get("id")
                service = resource.get("service", {})
                if isinstance(service, list):
                    service = service[0] if service else {}
                service_id = service.get("@id") or service.get("id")
                if image_url or service_id:
                    break

            for item in canvas.get("otherContent", []):
                if isinstance(item, dict):
                    ann_url = item.get("@id") or item.get("id")
                    if ann_url:
                        annotation_urls.append(ann_url)

            if not image_url and service_id:
                image_url = f"{service_id}/full/full/0/default.jpg"

            if image_url:
                pages.append(
                    {
                        "sequence_index": seq_index,
                        "page_index": len(pages) + 1,
                        "canvas_index": canvas_index,
                        "canvas_id": canvas.get("@id") or canvas.get("id"),
                        "canvas_label": canvas_label,
                        "width": width,
                        "height": height,
                        "image_url": image_url,
                        "service_id": service_id,
                        "annotation_urls": annotation_urls,
                    }
                )
    return pages


def extract_v3_pages(manifest: Dict[str, Any]) -> List[Dict[str, Any]]:
    pages: List[Dict[str, Any]] = []
    items = manifest.get("items", [])
    for canvas_index, canvas in enumerate(items, start=1):
        canvas_label = flatten_value(canvas.get("label")) or f"canvas-{canvas_index}"
        width = canvas.get("width")
        height = canvas.get("height")
        image_url = None
        service_id = None
        annotation_urls: List[str] = []

        for anno_page in canvas.get("items", []):
            for anno in anno_page.get("items", []):
                body = anno.get("body", {})
                if isinstance(body, list):
                    body = body[0] if body else {}
                image_url = body.get("id")
                service = body.get("service", {})
                if isinstance(service, list):
                    service = service[0] if service else {}
                service_id = service.get("@id") or service.get("id")
                if image_url or service_id:
                    break
            if image_url or service_id:
                break

        for ann_page in canvas.get("annotations", []):
            if isinstance(ann_page, dict):
                ann_url = ann_page.get("id") or ann_page.get("@id")
                if ann_url:
                    annotation_urls.append(ann_url)

        if not image_url and service_id:
            image_url = f"{service_id}/full/max/0/default.jpg"

        if image_url:
            pages.append(
                {
                    "sequence_index": 1,
                    "page_index": len(pages) + 1,
                    "canvas_index": canvas_index,
                    "canvas_id": canvas.get("@id") or canvas.get("id"),
                    "canvas_label": canvas_label,
                    "width": width,
                    "height": height,
                    "image_url": image_url,
                    "service_id": service_id,
                    "annotation_urls": annotation_urls,
                }
            )
    return pages


def extract_pages(manifest: Dict[str, Any]) -> List[Dict[str, Any]]:
    if manifest.get("sequences"):
        return extract_v2_pages(manifest)
    if manifest.get("items"):
        return extract_v3_pages(manifest)
    raise ValueError("Manifest does not look like IIIF Presentation v2 or v3.")


def choose_filename(page: Dict[str, Any]) -> str:
    page_no = page["page_index"]
    label = sanitize_filename(page.get("canvas_label") or "")
    ext = guess_extension_from_url(page["image_url"], default=".jpg")
    return f"{page_no:04d} - {label}{ext}"


def annotation_basename(page: Dict[str, Any], index: int = 1) -> str:
    page_no = page["page_index"]
    label = sanitize_filename(page.get("canvas_label") or "")
    suffix = f" - ann{index}" if len(page.get("annotation_urls") or []) > 1 else ""
    return f"{page_no:04d} - {label}{suffix}"


def download_one(session: requests.Session, page: Dict[str, Any], dest_dir: Path, overwrite: bool, timeout: int) -> Dict[str, Any]:
    filename = choose_filename(page)
    dest_path = dest_dir / filename

    result = {
        **page,
        "filename": filename,
        "path": str(dest_path),
        "status": "skipped" if dest_path.exists() and not overwrite else "downloaded",
        "bytes": dest_path.stat().st_size if dest_path.exists() and not overwrite else None,
        "error": "",
    }

    if dest_path.exists() and not overwrite:
        return result

    resp = request_with_retries(session, page["image_url"], stream=True, timeout=timeout)
    tmp_path = dest_path.with_suffix(dest_path.suffix + ".part")

    bytes_written = 0
    with tmp_path.open("wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 256):
            if not chunk:
                continue
            f.write(chunk)
            bytes_written += len(chunk)

    tmp_path.replace(dest_path)
    result["bytes"] = bytes_written
    return result


def fetch_annotation_list(session: requests.Session, url: str, timeout: int) -> Dict[str, Any]:
    resp = request_with_retries(session, url, stream=False, timeout=timeout)
    return resp.json()


def annotation_rows_from_list(page: Dict[str, Any], annotation_url: str, annotation_list: Dict[str, Any], ann_index: int) -> List[Dict[str, Any]]:
    resources = annotation_list.get("resources") or annotation_list.get("items") or []
    rows: List[Dict[str, Any]] = []

    for item_index, item in enumerate(resources, start=1):
        if not isinstance(item, dict):
            continue
        resource = item.get("resource") or item.get("body") or {}
        chars = flatten_value(resource.get("chars") if isinstance(resource, dict) else resource)
        target = item.get("on") or item.get("target") or ""
        x, y, w, h = parse_xywh(target)
        rows.append(
            {
                "page_index": page["page_index"],
                "canvas_index": page["canvas_index"],
                "canvas_label": page.get("canvas_label", ""),
                "annotation_list_index": ann_index,
                "annotation_item_index": item_index,
                "annotation_url": annotation_url,
                "annotation_id": item.get("@id") or item.get("id") or "",
                "target": target,
                "x": x,
                "y": y,
                "w": w,
                "h": h,
                "text": chars,
            }
        )
    return rows


def write_json(path: Path, data: Any) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def write_pages_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fieldnames = [
        "page_index", "canvas_index", "canvas_label", "width", "height", "image_url",
        "service_id", "filename", "path", "status", "bytes", "error",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def write_annotation_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fieldnames = [
        "page_index", "canvas_index", "canvas_label", "annotation_list_index", "annotation_item_index",
        "annotation_url", "annotation_id", "target", "x", "y", "w", "h", "text",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def write_text(path: Path, text: str) -> None:
    with path.open("w", encoding="utf-8") as f:
        f.write(text)


def build_page_text(rows: List[Dict[str, Any]]) -> str:
    parts = [row["text"].strip() for row in rows if (row.get("text") or "").strip()]
    return "\n".join(parts)


def download_annotations(
    session: requests.Session,
    pages: List[Dict[str, Any]],
    annotations_dir: Path,
    texts_dir: Path,
    overwrite: bool,
    timeout: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    all_rows: List[Dict[str, Any]] = []
    index_rows: List[Dict[str, Any]] = []

    for page in pages:
        page_rows: List[Dict[str, Any]] = []
        annotation_urls = page.get("annotation_urls") or []
        if not annotation_urls:
            index_rows.append(
                {
                    "page_index": page["page_index"],
                    "canvas_index": page["canvas_index"],
                    "canvas_label": page.get("canvas_label", ""),
                    "annotation_count": 0,
                    "text_file": "",
                    "json_files": "",
                    "csv_file": "",
                    "status": "none",
                    "error": "",
                }
            )
            continue

        json_files: List[str] = []

        try:
            for ann_index, annotation_url in enumerate(annotation_urls, start=1):
                base = annotation_basename(page, ann_index)
                ann_json_path = annotations_dir / f"{base}.json"
                json_files.append(str(ann_json_path))

                if ann_json_path.exists() and not overwrite:
                    with ann_json_path.open("r", encoding="utf-8") as f:
                        annotation_list = json.load(f)
                else:
                    annotation_list = fetch_annotation_list(session, annotation_url, timeout)
                    write_json(ann_json_path, annotation_list)

                page_rows.extend(annotation_rows_from_list(page, annotation_url, annotation_list, ann_index))

            page_rows.sort(key=lambda r: (r["annotation_list_index"], r["y"] if r["y"] is not None else 10**9, r["x"] if r["x"] is not None else 10**9, r["annotation_item_index"]))
            all_rows.extend(page_rows)

            base = annotation_basename(page, 1).replace(" - ann1", "")
            page_csv_path = annotations_dir / f"{base}.csv"
            page_txt_path = texts_dir / f"{base}.txt"
            write_annotation_csv(page_csv_path, page_rows)
            write_text(page_txt_path, build_page_text(page_rows))

            index_rows.append(
                {
                    "page_index": page["page_index"],
                    "canvas_index": page["canvas_index"],
                    "canvas_label": page.get("canvas_label", ""),
                    "annotation_count": len(page_rows),
                    "text_file": str(page_txt_path),
                    "json_files": " | ".join(json_files),
                    "csv_file": str(page_csv_path),
                    "status": "downloaded",
                    "error": "",
                }
            )
            print(f"[ANN] {page['page_index']:04d} -> {len(page_rows)} text regions")
        except Exception as exc:
            index_rows.append(
                {
                    "page_index": page["page_index"],
                    "canvas_index": page["canvas_index"],
                    "canvas_label": page.get("canvas_label", ""),
                    "annotation_count": 0,
                    "text_file": "",
                    "json_files": " | ".join(json_files),
                    "csv_file": "",
                    "status": "error",
                    "error": str(exc),
                }
            )
            print(f"[ANN-ERR] {page['page_index']:04d}: {exc}", file=sys.stderr)

    index_rows.sort(key=lambda r: r["page_index"])
    all_rows.sort(key=lambda r: (r["page_index"], r["annotation_list_index"], r["y"] if r["y"] is not None else 10**9, r["x"] if r["x"] is not None else 10**9, r["annotation_item_index"]))
    return all_rows, index_rows


def write_annotation_index_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fieldnames = [
        "page_index", "canvas_index", "canvas_label", "annotation_count", "text_file",
        "json_files", "csv_file", "status", "error",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def write_combined_text(path: Path, pages: List[Dict[str, Any]], annotation_rows: List[Dict[str, Any]]) -> None:
    rows_by_page: Dict[int, List[Dict[str, Any]]] = {}
    for row in annotation_rows:
        rows_by_page.setdefault(row["page_index"], []).append(row)

    chunks: List[str] = []
    for page in pages:
        page_rows = rows_by_page.get(page["page_index"], [])
        page_text = build_page_text(page_rows)
        chunks.append(f"=== Page {page['page_index']:04d}: {page.get('canvas_label', '')} ===\n{page_text}\n")

    write_text(path, "\n".join(chunks).rstrip() + "\n")


def build_line_rows(annotation_rows: List[Dict[str, Any]], y_tolerance: int = 18) -> List[Dict[str, Any]]:
    grouped: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for row in annotation_rows:
        grouped[row["page_index"]].append(row)

    line_rows: List[Dict[str, Any]] = []
    for page_index, rows in grouped.items():
        rows = sorted(rows, key=lambda r: (r["y"] if r["y"] is not None else 10**9, r["x"] if r["x"] is not None else 10**9, r["annotation_item_index"]))
        lines: List[List[Dict[str, Any]]] = []
        current: List[Dict[str, Any]] = []
        current_y: Optional[int] = None

        for row in rows:
            text = (row.get("text") or "").strip()
            if not text:
                continue
            y = row.get("y")
            if y is None:
                current.append(row)
                continue
            if current_y is None or abs(y - current_y) <= y_tolerance:
                current.append(row)
                current_y = y if current_y is None else int(round((current_y + y) / 2))
            else:
                lines.append(current)
                current = [row]
                current_y = y
        if current:
            lines.append(current)

        for idx, line in enumerate(lines, start=1):
            line_sorted = sorted(line, key=lambda r: (r["x"] if r["x"] is not None else 10**9, r["annotation_item_index"]))
            line_text = " ".join((r.get("text") or "").strip() for r in line_sorted if (r.get("text") or "").strip())
            if not line_text:
                continue
            xs = [r["x"] for r in line_sorted if r.get("x") is not None]
            ys = [r["y"] for r in line_sorted if r.get("y") is not None]
            x2s = [r["x"] + r["w"] for r in line_sorted if r.get("x") is not None and r.get("w") is not None]
            hs = [r["h"] for r in line_sorted if r.get("h") is not None]
            line_rows.append(
                {
                    "page_index": page_index,
                    "line_index": idx,
                    "text": line_text,
                    "normalized_text": normalize_for_search(line_text),
                    "x": min(xs) if xs else None,
                    "y": min(ys) if ys else None,
                    "w": (max(x2s) - min(xs)) if xs and x2s else None,
                    "h": max(hs) if hs else None,
                }
            )
    line_rows.sort(key=lambda r: (r["page_index"], r["line_index"]))
    return line_rows


def write_line_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fieldnames = ["page_index", "line_index", "text", "normalized_text", "x", "y", "w", "h"]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def build_search_index(pages: List[Dict[str, Any]], annotation_rows: List[Dict[str, Any]], line_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    term_stats: Dict[str, Dict[str, Any]] = {}

    def add_term(term: str, page_index: int, entry_type: str) -> None:
        bucket = term_stats.setdefault(term, {"term": term, "occurrences": 0, "pages": set(), "entry_types": set()})
        bucket["occurrences"] += 1
        bucket["pages"].add(page_index)
        bucket["entry_types"].add(entry_type)

    for page in pages:
        for token in tokenize_for_search(page.get("canvas_label", "")):
            add_term(token, page["page_index"], "page_label")

    for row in annotation_rows:
        for token in tokenize_for_search(row.get("text", "")):
            add_term(token, row["page_index"], "annotation")

    for row in line_rows:
        for token in tokenize_for_search(row.get("text", "")):
            add_term(token, row["page_index"], "line")

    result: List[Dict[str, Any]] = []
    for term, stats in sorted(term_stats.items(), key=lambda kv: (-kv[1]["occurrences"], kv[0])):
        page_indexes = sorted(stats["pages"])
        result.append(
            {
                "term": term,
                "occurrences": stats["occurrences"],
                "page_count": len(page_indexes),
                "page_indexes": ",".join(str(p) for p in page_indexes),
                "entry_types": ",".join(sorted(stats["entry_types"])),
            }
        )
    return result


def write_search_index_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fieldnames = ["term", "occurrences", "page_count", "page_indexes", "entry_types"]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def build_sqlite_database(
    db_path: Path,
    manifest_summary: Dict[str, Any],
    pages: List[Dict[str, Any]],
    page_rows: List[Dict[str, Any]],
    annotation_rows: List[Dict[str, Any]],
    line_rows: List[Dict[str, Any]],
    search_index_rows: List[Dict[str, Any]],
    overwrite: bool,
) -> None:
    if db_path.exists() and overwrite:
        db_path.unlink()

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS manifest (
                manifest_id TEXT,
                source TEXT,
                label TEXT,
                attribution TEXT,
                page_count INTEGER,
                pages_with_annotations INTEGER,
                metadata_json TEXT
            );
            CREATE TABLE IF NOT EXISTS pages (
                page_index INTEGER PRIMARY KEY,
                canvas_index INTEGER,
                canvas_label TEXT,
                width INTEGER,
                height INTEGER,
                image_url TEXT,
                service_id TEXT,
                filename TEXT,
                path TEXT,
                status TEXT,
                bytes INTEGER,
                error TEXT
            );
            CREATE TABLE IF NOT EXISTS annotations (
                page_index INTEGER,
                canvas_index INTEGER,
                canvas_label TEXT,
                annotation_list_index INTEGER,
                annotation_item_index INTEGER,
                annotation_url TEXT,
                annotation_id TEXT,
                target TEXT,
                x INTEGER,
                y INTEGER,
                w INTEGER,
                h INTEGER,
                text TEXT,
                normalized_text TEXT
            );
            CREATE TABLE IF NOT EXISTS lines (
                page_index INTEGER,
                line_index INTEGER,
                text TEXT,
                normalized_text TEXT,
                x INTEGER,
                y INTEGER,
                w INTEGER,
                h INTEGER
            );
            CREATE TABLE IF NOT EXISTS search_terms (
                term TEXT PRIMARY KEY,
                occurrences INTEGER,
                page_count INTEGER,
                page_indexes TEXT,
                entry_types TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_annotations_page ON annotations(page_index);
            CREATE INDEX IF NOT EXISTS idx_lines_page ON lines(page_index);
            CREATE INDEX IF NOT EXISTS idx_search_terms_occurrences ON search_terms(occurrences DESC, term);
            """
        )

        fts_enabled = True
        try:
            cur.execute("CREATE VIRTUAL TABLE IF NOT EXISTS page_fts USING fts5(page_index UNINDEXED, canvas_label, text)")
            cur.execute("CREATE VIRTUAL TABLE IF NOT EXISTS annotation_fts USING fts5(page_index UNINDEXED, text)")
            cur.execute("CREATE VIRTUAL TABLE IF NOT EXISTS line_fts USING fts5(page_index UNINDEXED, text)")
        except sqlite3.OperationalError:
            fts_enabled = False

        for table in ["manifest", "pages", "annotations", "lines", "search_terms"]:
            cur.execute(f"DELETE FROM {table}")
        if fts_enabled:
            for table in ["page_fts", "annotation_fts", "line_fts"]:
                cur.execute(f"DELETE FROM {table}")

        cur.execute(
            "INSERT INTO manifest VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                manifest_summary.get("manifest_id"),
                manifest_summary.get("source"),
                manifest_summary.get("label"),
                manifest_summary.get("attribution"),
                manifest_summary.get("page_count"),
                manifest_summary.get("pages_with_annotations"),
                json.dumps(manifest_summary.get("metadata", {}), ensure_ascii=False),
            ),
        )

        cur.executemany(
            "INSERT INTO pages VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    row.get("page_index"), row.get("canvas_index"), row.get("canvas_label"), row.get("width"), row.get("height"),
                    row.get("image_url"), row.get("service_id"), row.get("filename"), row.get("path"), row.get("status"), row.get("bytes"), row.get("error")
                )
                for row in page_rows
            ],
        )

        cur.executemany(
            "INSERT INTO annotations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    row.get("page_index"), row.get("canvas_index"), row.get("canvas_label"), row.get("annotation_list_index"), row.get("annotation_item_index"),
                    row.get("annotation_url"), row.get("annotation_id"), row.get("target"), row.get("x"), row.get("y"), row.get("w"), row.get("h"),
                    row.get("text"), normalize_for_search(row.get("text", ""))
                )
                for row in annotation_rows
            ],
        )

        cur.executemany(
            "INSERT INTO lines VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (row.get("page_index"), row.get("line_index"), row.get("text"), row.get("normalized_text"), row.get("x"), row.get("y"), row.get("w"), row.get("h"))
                for row in line_rows
            ],
        )

        cur.executemany(
            "INSERT INTO search_terms VALUES (?, ?, ?, ?, ?)",
            [
                (row.get("term"), row.get("occurrences"), row.get("page_count"), row.get("page_indexes"), row.get("entry_types"))
                for row in search_index_rows
            ],
        )

        if fts_enabled:
            page_text_by_index: Dict[int, List[str]] = defaultdict(list)
            for row in line_rows:
                page_text_by_index[row["page_index"]].append(row.get("text", ""))
            cur.executemany(
                "INSERT INTO page_fts VALUES (?, ?, ?)",
                [
                    (page["page_index"], page.get("canvas_label", ""), "\n".join(page_text_by_index.get(page["page_index"], [])))
                    for page in pages
                ],
            )
            cur.executemany(
                "INSERT INTO annotation_fts VALUES (?, ?)",
                [(row.get("page_index"), row.get("text", "")) for row in annotation_rows if row.get("text")],
            )
            cur.executemany(
                "INSERT INTO line_fts VALUES (?, ?)",
                [(row.get("page_index"), row.get("text", "")) for row in line_rows if row.get("text")],
            )

        conn.commit()
    finally:
        conn.close()


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="proni-download",
        description="Download page images and annotations from a IIIF manifest.",
    )
    parser.add_argument("source", help="Path or URL to the manifest JSON")
    parser.add_argument("-o", "--output", default="downloads", help="Base output directory (default: ./downloads)")
    parser.add_argument("-w", "--workers", type=int, default=6, help="Number of parallel image downloads (default: 6)")
    parser.add_argument("--timeout", type=int, default=120, help="HTTP timeout in seconds (default: 120)")
    parser.add_argument("--overwrite", action="store_true", help="Re-download files even if they already exist")
    parser.add_argument("--no-annotations", action="store_true", help="Skip downloading annotation lists/text even if present")
    parser.add_argument("--no-sqlite", action="store_true", help="Skip building the SQLite archive database")
    parser.add_argument("--no-search-index", action="store_true", help="Skip building the generic searchable term index")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    manifest, source_label = load_json_source(args.source, timeout=args.timeout)
    pages = extract_pages(manifest)
    if not pages:
        raise RuntimeError("No page images found in the manifest.")

    output_root = choose_output_root(manifest, Path(args.output))
    images_dir = output_root / "images"
    annotations_dir = output_root / "annotations"
    texts_dir = output_root / "texts"
    output_root.mkdir(parents=True, exist_ok=True)
    images_dir.mkdir(parents=True, exist_ok=True)
    if not args.no_annotations:
        annotations_dir.mkdir(parents=True, exist_ok=True)
        texts_dir.mkdir(parents=True, exist_ok=True)

    meta = metadata_to_dict(manifest.get("metadata", []))
    manifest_summary = {
        "source": source_label,
        "manifest_id": manifest.get("@id") or manifest.get("id"),
        "label": flatten_value(manifest.get("label")),
        "attribution": flatten_value(manifest.get("attribution") or manifest.get("requiredStatement")),
        "metadata": meta,
        "page_count": len(pages),
        "pages_with_annotations": sum(1 for p in pages if p.get("annotation_urls")),
    }

    write_json(output_root / "manifest.json", manifest)
    write_json(output_root / "metadata.json", manifest_summary)

    rows: List[Dict[str, Any]] = []
    session = build_session()

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures = {executor.submit(download_one, session, page, images_dir, args.overwrite, args.timeout): page for page in pages}
        for future in as_completed(futures):
            page = futures[future]
            try:
                row = future.result()
                print(f"[OK] {row['page_index']:04d} -> {row['filename']} ({row['status']})")
            except Exception as exc:
                row = {
                    **page,
                    "filename": choose_filename(page),
                    "path": str(images_dir / choose_filename(page)),
                    "status": "error",
                    "bytes": "",
                    "error": str(exc),
                }
                print(f"[ERR] {page['page_index']:04d} -> {row['filename']}: {exc}", file=sys.stderr)
            rows.append(row)

    rows.sort(key=lambda r: r["page_index"])
    write_pages_csv(output_root / "pages.csv", rows)

    annotation_rows: List[Dict[str, Any]] = []
    annotation_index_rows: List[Dict[str, Any]] = []
    line_rows: List[Dict[str, Any]] = []
    search_index_rows: List[Dict[str, Any]] = []

    if not args.no_annotations:
        annotation_rows, annotation_index_rows = download_annotations(
            session=session,
            pages=pages,
            annotations_dir=annotations_dir,
            texts_dir=texts_dir,
            overwrite=args.overwrite,
            timeout=args.timeout,
        )
        write_annotation_csv(output_root / "annotations.csv", annotation_rows)
        write_annotation_index_csv(output_root / "annotation_pages.csv", annotation_index_rows)
        write_combined_text(output_root / "ocr.txt", pages, annotation_rows)

        line_rows = build_line_rows(annotation_rows)
        write_line_csv(output_root / "lines.csv", line_rows)

        if not args.no_search_index:
            search_index_rows = build_search_index(pages, annotation_rows, line_rows)
            write_search_index_csv(output_root / "search_index.csv", search_index_rows)
            write_json(output_root / "search_index.json", search_index_rows)

    if not args.no_sqlite:
        build_sqlite_database(
            output_root / "archive.db",
            manifest_summary=manifest_summary,
            pages=pages,
            page_rows=rows,
            annotation_rows=annotation_rows,
            line_rows=line_rows,
            search_index_rows=search_index_rows,
            overwrite=args.overwrite,
        )

    ok = sum(1 for r in rows if r["status"] in {"downloaded", "skipped"})
    errors = sum(1 for r in rows if r["status"] == "error")
    ann_errors = sum(1 for r in annotation_index_rows if r.get("status") == "error")

    print()
    print(f"Done. Output folder: {output_root}")
    print(f"Pages found:            {len(rows)}")
    print(f"Successful/skip:        {ok}")
    print(f"Image errors:           {errors}")
    if not args.no_annotations:
        print(f"Pages w/ annotations:   {sum(1 for p in pages if p.get('annotation_urls'))}")
        print(f"Annotation rows:        {len(annotation_rows)}")
        print(f"Line rows:              {len(line_rows)}")
        print(f"Annotation page errors: {ann_errors}")
        if not args.no_search_index:
            print(f"Indexed terms:          {len(search_index_rows)}")
    if not args.no_sqlite:
        print(f"SQLite archive:         {output_root / 'archive.db'}")

    return 1 if (errors or ann_errors) else 0


if __name__ == "__main__":
    raise SystemExit(main())
