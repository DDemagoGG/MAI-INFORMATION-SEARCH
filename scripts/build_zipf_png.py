#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
from typing import List, Tuple

import matplotlib.pyplot as plt
import yaml


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise ValueError("YAML root must be a mapping")
    return data


def read_term_freq(path: str, limit: int) -> Tuple[List[int], List[int]]:
    ranks: List[int] = []
    freqs: List[int] = []
    with open(path, "r", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if not header:
            return ranks, freqs
        rank = 0
        for row in reader:
            if len(row) < 2:
                continue
            try:
                freq = int(row[1])
            except ValueError:
                continue
            if freq <= 0:
                continue
            rank += 1
            ranks.append(rank)
            freqs.append(freq)
            if limit > 0 and rank >= limit:
                break
    return ranks, freqs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Zipf PNG chart")
    parser.add_argument("config", help="Path to YAML config")
    parser.add_argument("--top", type=int, default=20000, help="How many top terms to draw")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    term_csv = config["output"]["term_freq_csv"]
    out_png = config["output"]["zipf_png"]

    ranks, freqs = read_term_freq(term_csv, args.top)
    if not ranks:
        raise RuntimeError(f"No term frequencies found in {term_csv}")

    c_const = freqs[0]
    zipf = [c_const / rank for rank in ranks]

    out_dir = os.path.dirname(out_png)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    fig, ax = plt.subplots(figsize=(10, 7))
    ax.loglog(ranks, freqs, label="Corpus frequencies", linewidth=1.5)
    ax.loglog(ranks, zipf, label="Zipf (C/r)", linewidth=1.5)
    ax.set_title("Zipf law on music EN corpus")
    ax.set_xlabel("Rank (log)")
    ax.set_ylabel("Frequency (log)")
    ax.grid(True, which="both", alpha=0.3)
    ax.legend()

    fig.savefig(out_png, dpi=300, bbox_inches="tight")
    print(f"Saved ZIPF PNG: {out_png}")


if __name__ == "__main__":
    main()
