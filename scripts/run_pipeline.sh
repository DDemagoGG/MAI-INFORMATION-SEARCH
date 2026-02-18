#!/usr/bin/env bash
set -euo pipefail

CONFIG_PATH="${1:-config/crawler.yaml}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_ABS="${CONFIG_PATH}"
if [[ "${CONFIG_PATH}" != /* ]]; then
  CONFIG_ABS="${ROOT_DIR}/${CONFIG_PATH}"
fi
PYTHON_BIN="python3"
if [[ -x "${ROOT_DIR}/.venv/bin/python" ]]; then
  PYTHON_BIN="${ROOT_DIR}/.venv/bin/python"
fi

echo "[1/7] crawl sitemap corpus"
"${PYTHON_BIN}" "${ROOT_DIR}/scripts/crawl_sitemaps.py" "${CONFIG_ABS}"

echo "[2/7] extract text"
"${PYTHON_BIN}" "${ROOT_DIR}/scripts/extract_text.py" "${CONFIG_ABS}"

echo "[3/7] build C++ tools"
cmake -S "${ROOT_DIR}/cxx" -B "${ROOT_DIR}/cxx/build"
cmake --build "${ROOT_DIR}/cxx/build" -j

RAW_TSV="$("${PYTHON_BIN}" -c "import sys,yaml; cfg=yaml.safe_load(open(sys.argv[1],encoding='utf-8')); print(cfg['output']['raw_text_tsv'])" "${CONFIG_ABS}")"
TOKENIZED="$("${PYTHON_BIN}" -c "import sys,yaml; cfg=yaml.safe_load(open(sys.argv[1],encoding='utf-8')); print(cfg['output']['tokenized_path'])" "${CONFIG_ABS}")"
STEMMED="$("${PYTHON_BIN}" -c "import sys,yaml; cfg=yaml.safe_load(open(sys.argv[1],encoding='utf-8')); print(cfg['output']['stemmed_path'])" "${CONFIG_ABS}")"
TERM_CSV="$("${PYTHON_BIN}" -c "import sys,yaml; cfg=yaml.safe_load(open(sys.argv[1],encoding='utf-8')); print(cfg['output']['term_freq_csv'])" "${CONFIG_ABS}")"
INDEX_DIR="$("${PYTHON_BIN}" -c "import sys,yaml; cfg=yaml.safe_load(open(sys.argv[1],encoding='utf-8')); print(cfg['output']['index_dir'])" "${CONFIG_ABS}")"

mkdir -p "$(dirname "${ROOT_DIR}/${TOKENIZED}")" "$(dirname "${ROOT_DIR}/${TERM_CSV}")" "${ROOT_DIR}/${INDEX_DIR}"

echo "[4/7] tokenizer (C++ with STL)"
"${ROOT_DIR}/cxx/build/tokenizer" "${ROOT_DIR}/${RAW_TSV}" "${ROOT_DIR}/${TOKENIZED}"

echo "[5/7] stemmer + term stats (C++ no STL)"
"${ROOT_DIR}/cxx/build/stemmer" "${ROOT_DIR}/${TOKENIZED}" "${ROOT_DIR}/${STEMMED}"
"${ROOT_DIR}/cxx/build/term_stats" "${ROOT_DIR}/${STEMMED}" "${ROOT_DIR}/${TERM_CSV}"

echo "[6/7] boolean index build (C++ no STL)"
"${ROOT_DIR}/cxx/build/index_builder" "${ROOT_DIR}/${STEMMED}" "${ROOT_DIR}/${RAW_TSV}" "${ROOT_DIR}/${INDEX_DIR}"

echo "[7/7] build Zipf PNG"
"${PYTHON_BIN}" "${ROOT_DIR}/scripts/build_zipf_png.py" "${CONFIG_ABS}"

echo "Pipeline complete."
