#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import os
import time
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Tuple

import requests
import yaml
from pymongo import ASCENDING, MongoClient


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise ValueError("YAML root must be a mapping")
    return data


def now_ts() -> int:
    return int(time.time())


def normalize_url(url: str) -> str:
    parsed = urllib.parse.urlsplit(url.strip())
    scheme = parsed.scheme.lower() or "https"
    netloc = parsed.netloc.lower()
    path = parsed.path or "/"
    if path != "/" and path.endswith("/"):
        path = path[:-1]
    return urllib.parse.urlunsplit((scheme, netloc, path, "", ""))


def http_get(
    url: str,
    timeout_seconds: int,
    user_agent: str,
    extra_headers: Optional[Dict[str, str]] = None,
) -> Tuple[int, bytes, Dict[str, str]]:
    headers = {"User-Agent": user_agent, "Accept": "*/*"}
    if extra_headers:
        headers.update(extra_headers)
    resp = requests.get(url, headers=headers, timeout=timeout_seconds, allow_redirects=True)
    resp_headers = {k.lower(): v for k, v in resp.headers.items()}
    return resp.status_code, resp.content, resp_headers


def parse_sitemap_locs(xml_bytes: bytes) -> List[str]:
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return []
    result: List[str] = []
    for elem in root.iter():
        if elem.tag.endswith("loc") and elem.text:
            value = elem.text.strip()
            if value:
                result.append(value)
    return result


def keep_child_sitemap(url: str, allow_patterns: Iterable[str]) -> bool:
    lowered = url.lower()
    for pat in allow_patterns:
        if pat.lower() in lowered:
            return True
    return False


def keep_article_url(url: str, allowed_prefixes: Iterable[str]) -> bool:
    for prefix in allowed_prefixes:
        if url.startswith(prefix):
            return True
    return False


def ensure_indexes(queue_col, docs_col) -> None:
    queue_col.create_index([("url", ASCENDING)], unique=True)
    queue_col.create_index([("status", ASCENDING)])
    queue_col.create_index([("last_crawled_at", ASCENDING)])
    docs_col.create_index([("url", ASCENDING)], unique=True)
    docs_col.create_index([("source", ASCENDING)])


def reset_in_progress(queue_col) -> None:
    queue_col.update_many(
        {"status": "in_progress"},
        {"$set": {"status": "queued", "updated_at": now_ts()}},
    )


def queue_due_recrawls(queue_col, recrawl_after_hours: int) -> int:
    cutoff = int((datetime.now(timezone.utc) - timedelta(hours=recrawl_after_hours)).timestamp())
    result = queue_col.update_many(
        {"status": "done", "last_crawled_at": {"$lt": cutoff}},
        {"$set": {"status": "queued", "updated_at": now_ts()}},
    )
    return int(result.modified_count)


def seed_queue_from_sitemaps(config: dict, queue_col) -> Tuple[int, int]:
    logic = config["logic"]
    timeout_seconds = int(logic["request_timeout_seconds"])
    user_agent = str(logic["user_agent"])
    max_documents = int(logic["max_documents"])

    discovered_article_urls = 0
    inserted_or_updated = 0

    sources = config["sources"]
    source_count = max(1, len(sources))
    target_per_source = max_documents // source_count + max_documents // 2

    for source in sources:
        source_name = str(source["name"])
        sitemap_index_url = str(source["sitemap_index"])
        allow_patterns = source.get("sitemap_allow_patterns", [])
        allowed_prefixes = source.get("allowed_prefixes", [])
        discovered_for_source = 0

        try:
            _, root_xml, _ = http_get(sitemap_index_url, timeout_seconds, user_agent)
        except requests.RequestException as exc:
            print(f"[seed] failed root sitemap {sitemap_index_url}: {exc}", flush=True)
            continue

        child_candidates = parse_sitemap_locs(root_xml)
        child_sitemaps: List[str] = []
        for child in child_candidates:
            if allow_patterns and not keep_child_sitemap(child, allow_patterns):
                continue
            child_sitemaps.append(child)

        print(f"[seed] {source_name}: child sitemap candidates={len(child_sitemaps)}", flush=True)
        child_limit = max_documents // 200 + 200
        child_sitemaps = child_sitemaps[:child_limit]

        for child_url in child_sitemaps:
            try:
                _, child_xml, _ = http_get(child_url, timeout_seconds, user_agent)
            except requests.RequestException:
                continue
            article_urls = parse_sitemap_locs(child_xml)
            for article_url in article_urls:
                norm = normalize_url(article_url)
                if not keep_article_url(norm, allowed_prefixes):
                    continue
                discovered_article_urls += 1
                update = {
                    "$setOnInsert": {
                        "url": norm,
                        "source": source_name,
                        "status": "queued",
                        "attempts": 0,
                        "added_at": now_ts(),
                    },
                    "$set": {"updated_at": now_ts()},
                }
                result = queue_col.update_one({"url": norm}, update, upsert=True)
                if result.upserted_id is not None or result.modified_count > 0:
                    inserted_or_updated += 1
                discovered_for_source += 1
                if discovered_for_source >= target_per_source:
                    break
            if discovered_for_source >= target_per_source:
                break

    return discovered_article_urls, inserted_or_updated


def claim_next_queue_doc(queue_col, preferred_source: Optional[str]):
    query = {"status": "queued"}
    if preferred_source:
        query["source"] = preferred_source
    return queue_col.find_one_and_update(
        query,
        {"$set": {"status": "in_progress", "updated_at": now_ts()}},
        sort=[("_id", ASCENDING)],
    )


def crawl_documents(config: dict, queue_col, docs_col) -> Dict[str, int]:
    logic = config["logic"]
    delay_seconds = float(logic["delay_seconds"])
    delay_override = os.environ.get("CRAWL_DELAY_SECONDS", "").strip()
    if delay_override:
        try:
            delay_seconds = max(0.0, float(delay_override))
        except ValueError:
            pass
    timeout_seconds = int(logic["request_timeout_seconds"])
    user_agent = str(logic["user_agent"])
    max_documents = int(logic["max_documents"])

    stats = {
        "downloaded": 0,
        "not_modified": 0,
        "updated": 0,
        "failed": 0,
        "queued_remaining": 0,
    }

    source_names = [str(s.get("name", "")) for s in config.get("sources", []) if s.get("name")]
    source_count = len(source_names)
    min_per_source = max_documents // source_count if source_count > 0 else 0
    source_success: Dict[str, int] = {name: 0 for name in source_names}
    source_cursor = 0

    while stats["downloaded"] < max_documents:
        global_docs = docs_col.count_documents({})
        if global_docs >= max_documents:
            break

        preferred_source: Optional[str] = None
        if source_count > 0:
            tries = 0
            while tries < source_count:
                candidate = source_names[source_cursor % source_count]
                source_cursor += 1
                tries += 1
                if source_success.get(candidate, 0) < min_per_source:
                    preferred_source = candidate
                    break

        queue_doc = claim_next_queue_doc(queue_col, preferred_source)
        if queue_doc is None and preferred_source is not None:
            queue_doc = claim_next_queue_doc(queue_col, None)
        if queue_doc is None:
            break

        url = queue_doc["url"]
        source = queue_doc.get("source", "unknown")
        old_doc = docs_col.find_one({"url": url}, {"etag": 1, "last_modified": 1, "content_hash": 1})
        headers: Dict[str, str] = {}
        if old_doc:
            if old_doc.get("etag"):
                headers["If-None-Match"] = str(old_doc["etag"])
            if old_doc.get("last_modified"):
                headers["If-Modified-Since"] = str(old_doc["last_modified"])

        try:
            status, body, resp_headers = http_get(url, timeout_seconds, user_agent, headers)

            if status == 304:
                docs_col.update_one(
                    {"url": url},
                    {"$set": {"crawled_at": now_ts(), "source": source}},
                    upsert=True,
                )
                queue_col.update_one(
                    {"_id": queue_doc["_id"]},
                    {"$set": {"status": "done", "last_crawled_at": now_ts(), "updated_at": now_ts()}},
                )
                stats["not_modified"] += 1
                stats["downloaded"] += 1
                if source in source_success:
                    source_success[source] += 1
            elif status == 200:
                body_text = body.decode("utf-8", errors="ignore")
                body_hash = hashlib.sha256(body).hexdigest()
                changed = old_doc is None or old_doc.get("content_hash") != body_hash

                if changed:
                    docs_col.update_one(
                        {"url": url},
                        {
                            "$set": {
                                "url": url,
                                "raw_html": body_text,
                                "source": source,
                                "crawled_at": now_ts(),
                                "etag": resp_headers.get("etag", ""),
                                "last_modified": resp_headers.get("last-modified", ""),
                                "content_hash": body_hash,
                            }
                        },
                        upsert=True,
                    )
                    stats["updated"] += 1
                else:
                    docs_col.update_one(
                        {"url": url},
                        {"$set": {"crawled_at": now_ts(), "source": source}},
                        upsert=True,
                    )
                    stats["not_modified"] += 1

                queue_col.update_one(
                    {"_id": queue_doc["_id"]},
                    {
                        "$set": {
                            "status": "done",
                            "last_crawled_at": now_ts(),
                            "updated_at": now_ts(),
                            "last_error": "",
                        },
                        "$inc": {"attempts": 1},
                    },
                )
                stats["downloaded"] += 1
                if source in source_success:
                    source_success[source] += 1
            else:
                queue_col.update_one(
                    {"_id": queue_doc["_id"]},
                    {
                        "$set": {
                            "status": "failed",
                            "last_error": f"HTTP {status}",
                            "updated_at": now_ts(),
                        },
                        "$inc": {"attempts": 1},
                    },
                )
                stats["failed"] += 1
        except requests.RequestException as exc:
            queue_col.update_one(
                {"_id": queue_doc["_id"]},
                {
                    "$set": {
                        "status": "failed",
                        "last_error": str(exc),
                        "updated_at": now_ts(),
                    },
                    "$inc": {"attempts": 1},
                },
            )
            stats["failed"] += 1

        if delay_seconds > 0:
            time.sleep(delay_seconds)

    stats["queued_remaining"] = int(queue_col.count_documents({"status": "queued"}))
    for source_name, count in source_success.items():
        stats[f"downloaded_source_{source_name}"] = count
    return stats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Music corpus crawler from sitemap sources")
    parser.add_argument("config", help="Path to YAML config")
    parser.add_argument(
        "--crawl-only",
        action="store_true",
        help="Skip sitemap seeding and only process already queued URLs",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)

    mongo_uri = config["db"]["mongo_uri"]
    db_name = config["db"]["database"]
    queue_name = config["db"]["queue_collection"]
    docs_name = config["db"]["docs_collection"]

    client = MongoClient(mongo_uri)
    db = client[db_name]
    queue_col = db[queue_name]
    docs_col = db[docs_name]

    ensure_indexes(queue_col, docs_col)
    reset_in_progress(queue_col)
    due = queue_due_recrawls(queue_col, int(config["logic"]["recrawl_after_hours"]))
    print(f"[state] moved due recrawls back to queue: {due}", flush=True)

    if args.crawl_only:
        print("[seed] skipped due to --crawl-only", flush=True)
    else:
        discovered, touched = seed_queue_from_sitemaps(config, queue_col)
        print(f"[seed] discovered article URLs={discovered}, queue upsert/updates={touched}", flush=True)

    crawl_stats = crawl_documents(config, queue_col, docs_col)
    done_count = docs_col.count_documents({})
    print("[crawl] stats:", flush=True)
    for key, value in crawl_stats.items():
        print(f"  - {key}: {value}", flush=True)
    print(f"  - documents_in_db: {done_count}", flush=True)


if __name__ == "__main__":
    main()
