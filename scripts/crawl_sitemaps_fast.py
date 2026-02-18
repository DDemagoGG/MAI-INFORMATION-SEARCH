#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import hashlib
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional

import httpx
import yaml
from pymongo import ASCENDING, MongoClient


@dataclass
class RuntimeStats:
    downloaded: int = 0
    updated: int = 0
    not_modified: int = 0
    failed: int = 0


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise ValueError("YAML root must be a mapping")
    return data


def now_ts() -> int:
    return int(time.time())


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


def claim_next_queue_doc(queue_col, preferred_source: Optional[str]):
    query = {"status": "queued"}
    if preferred_source:
        query["source"] = preferred_source
    return queue_col.find_one_and_update(
        query,
        {"$set": {"status": "in_progress", "updated_at": now_ts()}},
        sort=[("_id", ASCENDING)],
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fast async crawler for Mongo queue")
    parser.add_argument("config", help="Path to YAML config")
    parser.add_argument("--workers", type=int, default=int(os.environ.get("FAST_CRAWL_WORKERS", "30")))
    parser.add_argument("--delay", type=float, default=float(os.environ.get("FAST_CRAWL_DELAY", "0.05")))
    parser.add_argument("--retries", type=int, default=2)
    return parser.parse_args()


async def fetch_page(
    client: httpx.AsyncClient,
    url: str,
    timeout_seconds: int,
    retries: int,
    extra_headers: Dict[str, str],
) -> tuple[int, bytes, Dict[str, str]]:
    last_error: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            resp = await client.get(url, headers=extra_headers, timeout=timeout_seconds)
            headers = {k.lower(): v for k, v in resp.headers.items()}
            return resp.status_code, resp.content, headers
        except httpx.RequestError as exc:
            last_error = exc
            if attempt >= retries:
                break
            await asyncio.sleep(min(2.0**attempt, 3.0))
    if last_error is None:
        raise RuntimeError("unknown fetch error")
    raise last_error


async def worker_loop(
    worker_id: int,
    client: httpx.AsyncClient,
    queue_col,
    docs_col,
    source_names: list[str],
    max_documents: int,
    timeout_seconds: int,
    delay_seconds: float,
    retries: int,
    state_lock: asyncio.Lock,
    source_lock: asyncio.Lock,
    source_success: Dict[str, int],
    source_cursor_ref: Dict[str, int],
    min_per_source: int,
    stats: RuntimeStats,
    doc_counter: Dict[str, int],
) -> None:
    while True:
        if doc_counter["value"] >= max_documents:
            return

        preferred_source: Optional[str] = None
        if source_names:
            async with source_lock:
                tries = 0
                while tries < len(source_names):
                    candidate = source_names[source_cursor_ref["value"] % len(source_names)]
                    source_cursor_ref["value"] += 1
                    tries += 1
                    if source_success.get(candidate, 0) < min_per_source:
                        preferred_source = candidate
                        break

        queue_doc = await asyncio.to_thread(claim_next_queue_doc, queue_col, preferred_source)
        if queue_doc is None and preferred_source is not None:
            queue_doc = await asyncio.to_thread(claim_next_queue_doc, queue_col, None)
        if queue_doc is None:
            return

        url = queue_doc["url"]
        source = str(queue_doc.get("source", "unknown"))
        old_doc = await asyncio.to_thread(
            docs_col.find_one,
            {"url": url},
            {"etag": 1, "last_modified": 1, "content_hash": 1},
        )
        req_headers: Dict[str, str] = {}
        if old_doc:
            if old_doc.get("etag"):
                req_headers["If-None-Match"] = str(old_doc["etag"])
            if old_doc.get("last_modified"):
                req_headers["If-Modified-Since"] = str(old_doc["last_modified"])

        try:
            status, body, resp_headers = await fetch_page(client, url, timeout_seconds, retries, req_headers)
            if status == 304:
                await asyncio.to_thread(
                    docs_col.update_one,
                    {"url": url},
                    {"$set": {"crawled_at": now_ts(), "source": source}},
                    True,
                )
                await asyncio.to_thread(
                    queue_col.update_one,
                    {"_id": queue_doc["_id"]},
                    {"$set": {"status": "done", "last_crawled_at": now_ts(), "updated_at": now_ts()}},
                )
                async with state_lock:
                    stats.not_modified += 1
                    stats.downloaded += 1
                    doc_counter["value"] += 1
                    if source in source_success:
                        source_success[source] += 1
            elif status == 200:
                body_hash = hashlib.sha256(body).hexdigest()
                changed = old_doc is None or old_doc.get("content_hash") != body_hash
                if changed:
                    html = body.decode("utf-8", errors="ignore")
                    await asyncio.to_thread(
                        docs_col.update_one,
                        {"url": url},
                        {
                            "$set": {
                                "url": url,
                                "raw_html": html,
                                "source": source,
                                "crawled_at": now_ts(),
                                "etag": resp_headers.get("etag", ""),
                                "last_modified": resp_headers.get("last-modified", ""),
                                "content_hash": body_hash,
                            }
                        },
                        True,
                    )
                    async with state_lock:
                        stats.updated += 1
                else:
                    await asyncio.to_thread(
                        docs_col.update_one,
                        {"url": url},
                        {"$set": {"crawled_at": now_ts(), "source": source}},
                        True,
                    )
                    async with state_lock:
                        stats.not_modified += 1

                await asyncio.to_thread(
                    queue_col.update_one,
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
                async with state_lock:
                    stats.downloaded += 1
                    doc_counter["value"] += 1
                    if source in source_success:
                        source_success[source] += 1
            else:
                await asyncio.to_thread(
                    queue_col.update_one,
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
                async with state_lock:
                    stats.failed += 1
        except Exception as exc:
            await asyncio.to_thread(
                queue_col.update_one,
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
            async with state_lock:
                stats.failed += 1

        if delay_seconds > 0:
            await asyncio.sleep(delay_seconds)

        if worker_id == 0:
            current_done = doc_counter["value"]
            if current_done % 100 == 0:
                print(f"[crawl-fast] documents_in_db={current_done}", flush=True)


async def run_async(config: dict, workers: int, delay_seconds: float, retries: int) -> None:
    mongo_uri = config["db"]["mongo_uri"]
    db_name = config["db"]["database"]
    queue_name = config["db"]["queue_collection"]
    docs_name = config["db"]["docs_collection"]

    logic = config["logic"]
    timeout_seconds = int(logic["request_timeout_seconds"])
    max_documents = int(logic["max_documents"])
    user_agent = str(logic["user_agent"])

    client = MongoClient(mongo_uri)
    db = client[db_name]
    queue_col = db[queue_name]
    docs_col = db[docs_name]

    ensure_indexes(queue_col, docs_col)
    reset_in_progress(queue_col)
    due = queue_due_recrawls(queue_col, int(logic["recrawl_after_hours"]))
    print(f"[state] moved due recrawls back to queue: {due}", flush=True)

    source_names = [str(s.get("name", "")) for s in config.get("sources", []) if s.get("name")]
    source_count = len(source_names)
    min_per_source = max_documents // source_count if source_count > 0 else 0
    source_success: Dict[str, int] = {name: 0 for name in source_names}
    source_cursor_ref = {"value": 0}
    stats = RuntimeStats()
    state_lock = asyncio.Lock()
    source_lock = asyncio.Lock()

    initial_count = int(docs_col.estimated_document_count())
    doc_counter: Dict[str, int] = {"value": initial_count}
    print(f"[state] initial doc_counter={initial_count}", flush=True)

    limits = httpx.Limits(max_connections=workers, max_keepalive_connections=workers)
    async with httpx.AsyncClient(limits=limits, headers={"User-Agent": user_agent, "Accept": "*/*"}) as client:
        tasks = [
            asyncio.create_task(
                worker_loop(
                    worker_id=i,
                    client=client,
                    queue_col=queue_col,
                    docs_col=docs_col,
                    source_names=source_names,
                    max_documents=max_documents,
                    timeout_seconds=timeout_seconds,
                    delay_seconds=delay_seconds,
                    retries=retries,
                    state_lock=state_lock,
                    source_lock=source_lock,
                    source_success=source_success,
                    source_cursor_ref=source_cursor_ref,
                    min_per_source=min_per_source,
                    stats=stats,
                    doc_counter=doc_counter,
                )
            )
            for i in range(workers)
        ]
        await asyncio.gather(*tasks)

    queue_remaining = int(queue_col.count_documents({"status": "queued"}))
    docs_total = int(docs_col.count_documents({}))
    print("[crawl-fast] stats:", flush=True)
    print(f"  - downloaded: {stats.downloaded}", flush=True)
    print(f"  - updated: {stats.updated}", flush=True)
    print(f"  - not_modified: {stats.not_modified}", flush=True)
    print(f"  - failed: {stats.failed}", flush=True)
    print(f"  - queued_remaining: {queue_remaining}", flush=True)
    for source_name, count in source_success.items():
        print(f"  - downloaded_source_{source_name}: {count}", flush=True)
    print(f"  - documents_in_db: {docs_total}", flush=True)


def main() -> None:
    args = parse_args()
    config = load_config(args.config)

    workers = max(1, int(args.workers))
    delay_seconds = max(0.0, float(args.delay))
    retries = max(0, int(args.retries))
    print(f"[crawl-fast] workers={workers}, delay={delay_seconds}, retries={retries}", flush=True)
    asyncio.run(run_async(config, workers, delay_seconds, retries))


if __name__ == "__main__":
    main()
