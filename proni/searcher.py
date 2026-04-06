#!/usr/bin/env python3
"""
Search a PRONI/IIIF archive database produced by proni-download.

Features:
- Boolean search with AND / OR / NOT and parentheses
- Phrase search with quoted terms
- FTS5 search when available
- LIKE-based fallback search
- Optional fuzzy matching for OCR-tolerant searching
- Optional page filtering
- Displays readable hits with basic scoring
"""

from __future__ import annotations

import argparse
import re
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple


WORD_RE = re.compile(r"[A-Za-z0-9']+")
TOKEN_RE = re.compile(r'\s*(\(|\)|"[^"]*"|AND\b|OR\b|NOT\b|[^\s()]+)', re.IGNORECASE)


@dataclass
class TermNode:
    value: str
    is_phrase: bool = False


@dataclass
class NotNode:
    child: object


@dataclass
class BinNode:
    op: str
    left: object
    right: object


class ParseError(ValueError):
    pass


class BooleanParser:
    def __init__(self, text: str):
        self.tokens = [m.group(1) for m in TOKEN_RE.finditer(text) if m.group(1).strip()]
        self.pos = 0

    def peek(self) -> Optional[str]:
        return self.tokens[self.pos] if self.pos < len(self.tokens) else None

    def take(self) -> Optional[str]:
        tok = self.peek()
        if tok is not None:
            self.pos += 1
        return tok

    def parse(self):
        if not self.tokens:
            raise ParseError("Empty query")
        node = self.parse_or()
        if self.peek() is not None:
            raise ParseError(f"Unexpected token: {self.peek()}")
        return node

    def parse_or(self):
        node = self.parse_and()
        while (tok := self.peek()) and tok.upper() == "OR":
            self.take()
            node = BinNode("OR", node, self.parse_and())
        return node

    def parse_and(self):
        node = self.parse_not()
        while True:
            tok = self.peek()
            if tok is None or tok == ")" or tok.upper() == "OR":
                break
            if tok.upper() == "AND":
                self.take()
            node = BinNode("AND", node, self.parse_not())
        return node

    def parse_not(self):
        tok = self.peek()
        if tok and tok.upper() == "NOT":
            self.take()
            return NotNode(self.parse_not())
        return self.parse_primary()

    def parse_primary(self):
        tok = self.take()
        if tok is None:
            raise ParseError("Unexpected end of query")
        if tok == "(":
            node = self.parse_or()
            if self.take() != ")":
                raise ParseError("Missing closing parenthesis")
            return node
        if tok == ")":
            raise ParseError("Unexpected closing parenthesis")
        if tok.startswith('"') and tok.endswith('"'):
            return TermNode(tok[1:-1], is_phrase=True)
        return TermNode(tok)


@dataclass
class SearchHit:
    source: str
    row: sqlite3.Row
    score: float


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def get_columns(conn: sqlite3.Connection, table_name: str) -> List[str]:
    try:
        rows = conn.execute(f"PRAGMA table_info({quote_ident(table_name)})").fetchall()
    except sqlite3.DatabaseError:
        return []
    return [str(r[1]) for r in rows]


def choose_text_column(columns: Sequence[str]) -> Optional[str]:
    for candidate in ["normalized_text", "text", "content", "chars", "label"]:
        if candidate in columns:
            return candidate
    return None


def choose_page_column(columns: Sequence[str]) -> Optional[str]:
    for candidate in ["page_index", "page", "page_no", "page_number"]:
        if candidate in columns:
            return candidate
    return None


def compact_whitespace(text: str) -> str:
    return " ".join((text or "").split())


def shorten(text: str, width: int) -> str:
    text = compact_whitespace(text)
    if len(text) <= width:
        return text
    return text[: max(1, width - 1)] + "…"


def normalize_simple(text: str) -> str:
    text = text.lower()
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_token(text: str) -> str:
    return re.sub(r"[^a-z0-9]", "", text.lower())


def tokenize_words(text: str) -> List[str]:
    return [m.group(0).lower() for m in WORD_RE.finditer(text or "")]


def token_forms(token: str) -> List[str]:
    token = token.lower()
    forms: List[str] = []

    def add(val: str) -> None:
        val = normalize_token(val)
        if val and val not in forms:
            forms.append(val)

    add(token)
    add(token.replace("'", ""))
    if "'" in token:
        for part in token.split("'"):
            add(part)
    if token.startswith("o'") and len(token) > 2:
        add(token[2:])
    norm = normalize_token(token)
    add(norm)
    if norm.startswith("mc") and len(norm) > 2:
        add(norm[2:])
    if norm.startswith("mac") and len(norm) > 3:
        add(norm[3:])
    return forms


def common_prefix_len(a: str, b: str) -> int:
    n = min(len(a), len(b))
    i = 0
    while i < n and a[i] == b[i]:
        i += 1
    return i


def trigrams(text: str) -> set[str]:
    if len(text) < 3:
        return {text} if text else set()
    return {text[i : i + 3] for i in range(len(text) - 2)}


def levenshtein(a: str, b: str, max_distance: Optional[int] = None) -> int:
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    if max_distance is not None and abs(len(a) - len(b)) > max_distance:
        return max_distance + 1

    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        curr = [i]
        row_min = curr[0]
        for j, cb in enumerate(b, 1):
            ins = curr[j - 1] + 1
            dele = prev[j] + 1
            sub = prev[j - 1] + (0 if ca == cb else 1)
            val = min(ins, dele, sub)
            curr.append(val)
            row_min = min(row_min, val)
        if max_distance is not None and row_min > max_distance:
            return max_distance + 1
        prev = curr
    return prev[-1]


def fuzzy_word_match(query: str, token: str, max_distance: int) -> Optional[Tuple[int, float]]:
    q = normalize_token(query)
    if not q:
        return None

    best_dist: Optional[int] = None
    best_form = ""
    for form in token_forms(token):
        d = levenshtein(q, form, max_distance)
        if d <= max_distance and (best_dist is None or d < best_dist):
            best_dist = d
            best_form = form
            if d == 0:
                break

    if best_dist is None:
        return None

    prefix = common_prefix_len(q, best_form)
    sim = 1.0 - (best_dist / max(len(q), len(best_form), 1))
    q_tri = trigrams(q)
    f_tri = trigrams(best_form)
    tri_overlap = (len(q_tri & f_tri) / max(len(q_tri | f_tri), 1)) if q_tri and f_tri else 0.0

    # Guard against broad false positives like donnel -> doone / dougherty.
    if best_dist > 0:
        if prefix < 3 and tri_overlap < 0.34 and sim < 0.7:
            return None
        if len(q) >= 5 and prefix < 2:
            return None

    score = max(0.5, 2.0 - best_dist * 0.35 + prefix * 0.08 + tri_overlap * 0.5)
    return best_dist, score


def term_matches(term: TermNode, text: str, tokens: List[str], fuzzy: bool, max_distance: int) -> Tuple[bool, float]:
    target = normalize_simple(term.value)
    hay = normalize_simple(text)
    if term.is_phrase:
        if target in hay:
            return True, 3.0
        if fuzzy:
            phrase_tokens = tokenize_words(target)
            if not phrase_tokens:
                return False, 0.0
            for i in range(0, max(0, len(tokens) - len(phrase_tokens) + 1)):
                window = tokens[i : i + len(phrase_tokens)]
                matches = [fuzzy_word_match(a, b, max_distance) for a, b in zip(phrase_tokens, window)]
                if all(m is not None for m in matches):
                    score = sum(m[1] for m in matches if m is not None) / len(phrase_tokens)
                    return True, max(1.2, score + 0.4)
        return False, 0.0

    normalized_tokens = [normalize_token(tok) for tok in tokens]
    target_norm = normalize_token(target)
    if target_norm and target_norm in normalized_tokens:
        return True, 2.0
    if target in hay:
        return True, 1.5
    if fuzzy:
        best_score: Optional[float] = None
        for tok in tokens:
            match = fuzzy_word_match(target, tok, max_distance)
            if match is not None:
                _, score = match
                best_score = score if best_score is None else max(best_score, score)
        if best_score is not None:
            return True, best_score
    return False, 0.0


def eval_expr(node, text: str, tokens: List[str], fuzzy: bool, max_distance: int) -> Tuple[bool, float]:
    if isinstance(node, TermNode):
        return term_matches(node, text, tokens, fuzzy, max_distance)
    if isinstance(node, NotNode):
        matched, _ = eval_expr(node.child, text, tokens, fuzzy, max_distance)
        return (not matched), 0.0
    if isinstance(node, BinNode):
        lm, ls = eval_expr(node.left, text, tokens, fuzzy, max_distance)
        rm, rs = eval_expr(node.right, text, tokens, fuzzy, max_distance)
        if node.op == "AND":
            return lm and rm, ls + rs
        if node.op == "OR":
            return lm or rm, max(ls, rs) + (0.2 if lm and rm else 0.0)
    return False, 0.0


def node_to_fts(node) -> str:
    if isinstance(node, TermNode):
        value = node.value.replace('"', '""')
        return f'"{value}"' if node.is_phrase or " " in value else value
    if isinstance(node, NotNode):
        inner = node_to_fts(node.child)
        return f"NOT ({inner})"
    if isinstance(node, BinNode):
        return f"({node_to_fts(node.left)} {node.op} {node_to_fts(node.right)})"
    raise TypeError("Unknown node type")


def gather_candidate_rows(conn: sqlite3.Connection, table: str, page: Optional[int], limit: Optional[int], node, fuzzy: bool = False) -> List[sqlite3.Row]:
    columns = get_columns(conn, table)
    text_col = choose_text_column(columns)
    if not text_col:
        return []
    page_col = choose_page_column(columns)
    where = []
    params: List[object] = []
    if page is not None and page_col:
        where.append(f"{quote_ident(page_col)} = ?")
        params.append(page)

    positives: List[TermNode] = []

    def collect_positive(n):
        if isinstance(n, TermNode):
            positives.append(n)
        elif isinstance(n, BinNode):
            if n.op in {"AND", "OR"}:
                collect_positive(n.left)
                collect_positive(n.right)

    collect_positive(node)

    like_parts = []
    if not fuzzy:
        for term in positives[:6]:
            needle = normalize_simple(term.value)
            if needle:
                like_parts.append(f"LOWER(COALESCE({quote_ident(text_col)}, '')) LIKE ?")
                params.append(f"%{needle}%")
        if like_parts:
            where.append("(" + " OR ".join(like_parts) + ")")

    sql = f"SELECT * FROM {quote_ident(table)}"
    if where:
        sql += " WHERE " + " AND ".join(where)
    if limit is not None:
        sql += f" LIMIT {max(limit, 50)}"
    return conn.execute(sql, params).fetchall()


def search_fts_table(conn: sqlite3.Connection, table: str, page: Optional[int], limit: Optional[int], node) -> List[sqlite3.Row]:
    columns = get_columns(conn, table)
    page_col = choose_page_column(columns)
    where = [f"{quote_ident(table)} MATCH ?"]
    params: List[object] = [node_to_fts(node)]
    if page is not None and page_col:
        where.append(f"{quote_ident(page_col)} = ?")
        params.append(page)
    sql = (
        f"SELECT *, bm25({quote_ident(table)}) AS rank "
        f"FROM {quote_ident(table)} WHERE {' AND '.join(where)} "
        f"ORDER BY rank LIMIT ?"
    )
    params.append(limit)
    return conn.execute(sql, params).fetchall()


def pick_sources(conn: sqlite3.Connection, table_mode: str) -> List[Tuple[str, str]]:
    sources: List[Tuple[str, str]] = []
    if table_mode in {"both", "lines", "auto"}:
        if table_exists(conn, "line_fts"):
            sources.append(("line_fts", "fts"))
        elif table_exists(conn, "lines"):
            sources.append(("lines", "scan"))
    if table_mode in {"both", "annotations", "auto"}:
        if table_exists(conn, "annotation_fts"):
            sources.append(("annotation_fts", "fts"))
        elif table_exists(conn, "annotations"):
            sources.append(("annotations", "scan"))
    if table_mode == "terms" and table_exists(conn, "search_terms"):
        sources.append(("search_terms", "terms"))
    if table_mode == "auto" and not sources and table_exists(conn, "pages"):
        sources.append(("pages", "scan"))
    return sources


def display_source_name(source_name: str, mode: str, used_base_table: bool) -> str:
    if used_base_table and source_name.endswith("_fts"):
        return source_name[:-4]
    if mode == "scan" and source_name.endswith("_fts"):
        return source_name[:-4]
    return source_name


def render_hit(hit: SearchHit, width: int) -> str:
    data = dict(hit.row)
    page = data.get("page_index")
    line = data.get("line_index")
    ann = data.get("annotation_item_index")
    x = data.get("x")
    y = data.get("y")
    canvas = data.get("canvas_label")
    text = data.get("text") or data.get("normalized_text") or data.get("term") or data.get("normalized_term") or ""

    bits = [f"[{hit.source}]", f"score={hit.score:.2f}"]
    if page is not None:
        bits.append(f"page {page}")
    if line is not None:
        bits.append(f"line {line}")
    if ann is not None:
        bits.append(f"ann {ann}")
    if x is not None and y is not None:
        bits.append(f"xy=({x},{y})")
    if canvas:
        bits.append(shorten(str(canvas), 60))
    return " | ".join(bits) + "\n  " + shorten(str(text), width)


def list_tables(conn: sqlite3.Connection) -> List[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table','view') ORDER BY name").fetchall()
    return [str(r[0]) for r in rows]


def print_schema(conn: sqlite3.Connection, table_names: Iterable[str]) -> None:
    for table in table_names:
        cols = get_columns(conn, table)
        print(f"{table}: {', '.join(cols) if cols else '(no columns found)'}")


def search_terms_table(conn: sqlite3.Connection, query: str, limit: Optional[int]) -> List[SearchHit]:
    if not table_exists(conn, "search_terms"):
        return []
    cols = get_columns(conn, "search_terms")
    term_col = "term" if "term" in cols else ("normalized_term" if "normalized_term" in cols else None)
    if not term_col:
        return []
    rows = conn.execute(
        f"SELECT * FROM search_terms WHERE LOWER(COALESCE({quote_ident(term_col)}, '')) LIKE LOWER(?) ORDER BY COALESCE(occurrences,0) DESC LIMIT ?",
        (f"%{normalize_simple(query)}%", limit),
    ).fetchall()
    return [SearchHit("search_terms", row, float(row["occurrences"]) if "occurrences" in row.keys() else 0.0) for row in rows]


def search_source(conn: sqlite3.Connection, source_name: str, mode: str, node, args) -> Tuple[str, List[SearchHit]]:
    hits: List[SearchHit] = []
    if mode == "terms":
        return source_name, search_terms_table(conn, args.query, args.limit)

    rows: List[sqlite3.Row]
    used_base_table = False
    if mode == "fts" and not args.force_scan and not args.fuzzy:
        try:
            prelimit = None if args.limit is None else max(args.limit * 4, 50)
            rows = search_fts_table(conn, source_name, args.page, prelimit, node)
        except sqlite3.DatabaseError:
            base = source_name.replace("_fts", "s") if source_name.endswith("_fts") else source_name
            prelimit = None if args.limit is None else max(args.limit * 10, 100)
            rows = gather_candidate_rows(conn, base, args.page, prelimit, node, fuzzy=False)
            used_base_table = True
    else:
        base = source_name.replace("_fts", "s") if source_name.endswith("_fts") else source_name
        if args.limit is None:
            prelimit = None
        else:
            prelimit = max(args.limit * (200 if args.fuzzy else 10), 5000 if args.fuzzy else 100)
        rows = gather_candidate_rows(conn, base, args.page, prelimit, node, fuzzy=args.fuzzy)
        used_base_table = base != source_name

    display_name = display_source_name(source_name, mode, used_base_table)
    for row in rows:
        text = row["text"] if "text" in row.keys() else row["normalized_text"] if "normalized_text" in row.keys() else ""
        tokens = tokenize_words(text)
        matched, score = eval_expr(node, text, tokens, args.fuzzy, args.max_distance)
        if matched:
            score += 0.2 if mode == "fts" and not args.force_scan and not args.fuzzy and not used_base_table else 0.0
            hits.append(SearchHit(display_name, row, score))
    hits.sort(key=lambda h: (-h.score, h.row["page_index"] if "page_index" in h.row.keys() and h.row["page_index"] is not None else 10**9))
    return display_name, (hits if args.limit is None else hits[: args.limit])


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="proni-search",
        description="Search a PRONI archive SQLite database.",
    )
    parser.add_argument("db", help="Path to archive.db")
    parser.add_argument("query", nargs="?", help="Search query")
    parser.add_argument("--table", choices=["auto", "both", "lines", "annotations", "terms"], default="auto")
    parser.add_argument("--page", type=int, help="Restrict search to a single page number")
    parser.add_argument("--limit", type=int, help="Maximum hits to return per source. Defaults to unlimited.")
    parser.add_argument("--global-limit", type=int, help="Optional maximum hits across all sources combined")
    parser.add_argument("--width", type=int, default=140, help="Snippet width")
    parser.add_argument("--list-tables", action="store_true", help="List tables/views in the database")
    parser.add_argument("--schema", action="store_true", help="Show table schemas")
    parser.add_argument("--fuzzy", action="store_true", help="Enable fuzzy term matching for OCR/noisy text")
    parser.add_argument("--max-distance", type=int, default=1, help="Max edit distance for fuzzy matching (default: 1)")
    parser.add_argument("--force-scan", action="store_true", help="Bypass FTS and evaluate matches by scanning candidate rows")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"Database not found: {db_path}", file=sys.stderr)
        return 2

    conn = connect(db_path)
    try:
        tables = list_tables(conn)
        if args.list_tables:
            for t in tables:
                print(t)
            if not args.query and not args.schema:
                return 0
        if args.schema:
            print_schema(conn, tables)
            if not args.query:
                return 0
        if not args.query:
            print("A query is required unless --list-tables and/or --schema is used.", file=sys.stderr)
            return 2

        try:
            node = BooleanParser(args.query).parse()
        except ParseError as e:
            print(f"Query parse error: {e}", file=sys.stderr)
            return 2

        sources = pick_sources(conn, args.table)
        if not sources:
            print("No searchable tables found in this database.", file=sys.stderr)
            return 1

        total_printed = 0
        global_limit = args.global_limit if args.global_limit is not None else None
        for source_name, mode in sources:
            if global_limit is not None and total_printed >= global_limit:
                break
            display_name, hits = search_source(conn, source_name, mode, node, args)
            if not hits:
                continue
            print(f"== {display_name} ==")
            per_source_hits = hits if args.limit is None else hits[: args.limit]
            if global_limit is not None:
                remaining = max(0, global_limit - total_printed)
                per_source_hits = per_source_hits[:remaining]
            for hit in per_source_hits:
                print(render_hit(hit, args.width))
                total_printed += 1
            print()

        if total_printed == 0:
            print("No matches found.")
            return 1
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
