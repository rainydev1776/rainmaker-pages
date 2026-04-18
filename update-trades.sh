#!/bin/bash
# Run this nightly after games finish to update the stories page with real trade data.
# Usage: ./update-trades.sh
#
# Edits trades.json with tonight's results, then pushes to GitHub Pages.
# For now, manually edit trades.json — later we can hook this into Supabase.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== Rainmaker Stories — Nightly Update ==="
echo "1. Edit trades.json with tonight's results"
echo "2. Run: git add trades.json && git commit -m 'nightly trade update' && git push"
echo ""
echo "trades.json location: $SCRIPT_DIR/trades.json"
echo ""

# Quick validation
if [ -f trades.json ]; then
  WINS=$(python3 -c "import json; d=json.load(open('trades.json')); print(d['summary']['wins'])" 2>/dev/null)
  LOSSES=$(python3 -c "import json; d=json.load(open('trades.json')); print(d['summary']['losses'])" 2>/dev/null)
  FEED=$(python3 -c "import json; d=json.load(open('trades.json')); print(len(d['feed']))" 2>/dev/null)
  echo "Current: ${WINS}W / ${LOSSES}L / ${FEED} trades in feed"
else
  echo "ERROR: trades.json not found!"
fi
