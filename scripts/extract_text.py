#!/usr/bin/env python3
from __future__ import annotations

import argparse
import html
import json
import os
import re
from collections import defaultdict
from typing import Dict, Tuple

import yaml
from pymongo import MongoClient


SCRIPT_RE = re.compile(r"<script\b[^>]*>.*?</script>", re.IGNORECASE | re.DOTALL)
STYLE_RE = re.compile(r"<style\b[^>]*>.*?</style>", re.IGNORECASE | re.DOTALL)
TAG_RE = re.compile(r"<[^>]+>")
TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
WS_RE = re.compile(r"\s+")


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise ValueError("YAML root must be a mapping")
    return data


def sanitize_field(value: str) -> str:
    value = value.replace("\t", " ")
    value = value.replace("\r", " ").replace("\n", " ")
    return WS_RE.sub(" ", value).strip()


def extract_title_and_text(raw_html: str) -> Tuple[str, str]:
    title_match = TITLE_RE.search(raw_html)
    title = html.unescape(title_match.group(1)) if title_match else ""
    cleaned = SCRIPT_RE.sub(" ", raw_html)
    cleaned = STYLE_RE.sub(" ", cleaned)
    cleaned = TAG_RE.sub(" ", cleaned)
    cleaned = html.unescape(cleaned)
    cleaned = WS_RE.sub(" ", cleaned).strip()
    return sanitize_field(title), cleaned


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract text from crawled HTML")
    parser.add_argument("config", help="Path to YAML config")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)

    mongo_uri = config["db"]["mongo_uri"]
    db_name = config["db"]["database"]
    docs_name = config["db"]["docs_collection"]
    out_tsv = config["output"]["raw_text_tsv"]

    out_dir = os.path.dirname(out_tsv)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    client = MongoClient(mongo_uri)
    docs_col = client[db_name][docs_name]

    stats: Dict[str, int] = {
        "documents_total": 0,
        "documents_written": 0,
        "raw_html_bytes_total": 0,
        "text_bytes_total": 0,
        "text_chars_total": 0,
    }
    by_source = defaultdict(int)

    cursor = docs_col.find(
        {"raw_html": {"$exists": True, "$ne": ""}},
        {"url": 1, "source": 1, "raw_html": 1},
        no_cursor_timeout=True,
    )

    with open(out_tsv, "w", encoding="utf-8") as out:
        doc_id = 0
        for doc in cursor:
            stats["documents_total"] += 1
            raw_html = str(doc.get("raw_html", ""))
            url = sanitize_field(str(doc.get("url", "")))
            source = sanitize_field(str(doc.get("source", "")))
            title, text = extract_title_and_text(raw_html)
            text = sanitize_field(text)
            if len(text) < 500:
                continue
            doc_id += 1
            by_source[source] += 1
            stats["documents_written"] += 1
            stats["raw_html_bytes_total"] += len(raw_html.encode("utf-8", errors="ignore"))
            stats["text_bytes_total"] += len(text.encode("utf-8", errors="ignore"))
            stats["text_chars_total"] += len(text)
            out.write(f"{doc_id}\t{source}\t{url}\t{title}\t{text}\n")

    if stats["documents_written"] > 0:
        stats["avg_raw_html_bytes"] = int(stats["raw_html_bytes_total"] / stats["documents_written"])
        stats["avg_text_bytes"] = int(stats["text_bytes_total"] / stats["documents_written"])
        stats["avg_text_chars"] = int(stats["text_chars_total"] / stats["documents_written"])
    else:
        stats["avg_raw_html_bytes"] = 0
        stats["avg_text_bytes"] = 0
        stats["avg_text_chars"] = 0

    stats_path = os.path.join(out_dir or ".", "lab1_stats.json")
    report_payload = {
        "stats": stats,
        "sources": dict(by_source),
    }
    with open(stats_path, "w", encoding="utf-8") as handle:
        json.dump(report_payload, handle, ensure_ascii=False, indent=2)

    print(f"Written TSV: {out_tsv}")
    print(f"Written stats: {stats_path}")
    print(json.dumps(report_payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
