#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PLIST_TEMPLATE="$ROOT_DIR/tools/com.client.erd.weekly.plist"
PLIST_DST="$HOME/Library/LaunchAgents/com.client.erd.weekly.plist"
LOG_DIR="$ROOT_DIR/tools/.logs"

mkdir -p "$LOG_DIR"

if [[ ! -f "$PLIST_TEMPLATE" ]]; then
  echo "No se encontró el template: $PLIST_TEMPLATE" >&2
  exit 2
fi

if [[ ! -x "$ROOT_DIR/tools/weekly_refresh.sh" ]]; then
  chmod +x "$ROOT_DIR/tools/weekly_refresh.sh"
fi

sed "s|__REPO_DIR__|$ROOT_DIR|g" "$PLIST_TEMPLATE" > "$PLIST_DST"

launchctl unload "$PLIST_DST" >/dev/null 2>&1 || true
launchctl load "$PLIST_DST"

echo "OK instalado: $PLIST_DST"
echo "Logs: $LOG_DIR"
echo "Para ejecutar manualmente ahora:"
echo "  launchctl start com.client.erd.weekly"
