#!/usr/bin/env python3
from __future__ import annotations

import os
import subprocess
from typing import Dict, List, Tuple

import yaml
from flask import Flask, redirect, render_template, request, url_for


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_CONFIG = os.path.join(ROOT_DIR, "config", "crawler.yaml")


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise ValueError("YAML root must be a mapping")
    return data


def parse_cli_output(raw: str) -> Tuple[int, List[Dict[str, str]]]:
    total = 0
    docs: List[Dict[str, str]] = []
    for line in raw.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if parts[0] == "TOTAL" and len(parts) >= 2:
            try:
                total = int(parts[1])
            except ValueError:
                total = 0
        elif parts[0] == "DOC" and len(parts) >= 4:
            docs.append(
                {
                    "doc_id": parts[1],
                    "title": parts[2],
                    "url": parts[3],
                }
            )
    return total, docs


def create_app() -> Flask:
    cfg_path = os.environ.get("MUSIC_IR_CONFIG", DEFAULT_CONFIG)
    cfg = load_config(cfg_path)
    index_dir = cfg["output"]["index_dir"]

    search_cli = os.environ.get(
        "MUSIC_IR_SEARCH_CLI",
        os.path.join(ROOT_DIR, "cxx", "build", "search_cli"),
    )

    app = Flask(__name__, template_folder="templates")

    @app.get("/")
    def home():
        return render_template("index.html")

    @app.get("/search")
    def search():
        query = request.args.get("q", "").strip()
        if not query:
            return redirect(url_for("home"))

        page = request.args.get("page", "1")
        try:
            page_int = max(1, int(page))
        except ValueError:
            page_int = 1
        limit = 50
        offset = (page_int - 1) * limit

        cmd = [
            search_cli,
            "--index-dir",
            index_dir,
            "--query",
            query,
            "--offset",
            str(offset),
            "--limit",
            str(limit),
        ]
        try:
            completed = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True,
                encoding="utf-8",
                timeout=60,
            )
        except subprocess.CalledProcessError as exc:
            return render_template(
                "results.html",
                query=query,
                total=0,
                docs=[],
                page=page_int,
                has_next=False,
                has_prev=page_int > 1,
                error=f"search_cli failed: {exc.stderr.strip()}",
            )
        except FileNotFoundError:
            return render_template(
                "results.html",
                query=query,
                total=0,
                docs=[],
                page=page_int,
                has_next=False,
                has_prev=page_int > 1,
                error=f"search_cli not found at {search_cli}",
            )

        total, docs = parse_cli_output(completed.stdout)
        has_prev = page_int > 1
        has_next = offset + len(docs) < total
        return render_template(
            "results.html",
            query=query,
            total=total,
            docs=docs,
            page=page_int,
            has_next=has_next,
            has_prev=has_prev,
            error="",
        )

    return app


if __name__ == "__main__":
    app = create_app()
    debug_mode = os.environ.get("MUSIC_IR_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=8080, debug=debug_mode)
