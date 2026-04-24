#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ ! -f "erd/.env" ]]; then
  echo "Falta erd/.env. Copia erd/.env.example -> erd/.env y completa credenciales." >&2
  exit 2
fi

PY="${PYTHON_BIN:-python3}"

echo "== ERD weekly refresh =="
echo "Repo: $ROOT_DIR"
echo "Python: $PY"

"$PY" -V

# Requiere que esta maquina tenga VPN FortiClient conectada.
"$PY" erd/build_erd_data_from_env.py --db mixed --with-comments --ai-mode heuristic

git add erd/data erd/data/.ai_explanations_cache.json erd/data/datasets.manifest.json || true

if git diff --cached --quiet; then
  echo "No changes detected in ERD data."
  exit 0
fi

git commit -m "Weekly ERD refresh"
git push

echo "OK: pushed updated ERD data."
