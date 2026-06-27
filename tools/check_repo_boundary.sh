#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if git status --short | awk '{print $2}' | grep -E '(^/|^\.\./|^~)' >/dev/null 2>&1; then
  echo "repo boundary check failed: git status contains a path outside this repo" >&2
  git status --short >&2
  exit 70
fi

if git ls-files | grep -E '(^\.config/|^\.local/|^\.mozilla/|^\.config/google-chrome/|^sunshine/|^gnome/)' >/dev/null 2>&1; then
  echo "repo boundary check failed: tracked file matches a forbidden global config area" >&2
  git ls-files | grep -E '(^\.config/|^\.local/|^\.mozilla/|^\.config/google-chrome/|^sunshine/|^gnome/)' >&2
  exit 70
fi

if git ls-files | grep -E '(^|/)(\.env|\.env\..*)$' >/dev/null 2>&1; then
  echo "repo boundary check failed: tracked env file" >&2
  git ls-files | grep -E '(^|/)(\.env|\.env\..*)$' >&2
  exit 70
fi

if rg -n -e 'ib_insync|ibapi|databento|Databento|IBKR|Interactive Brokers|systemctl|nomad |docker exec|sqlite|postgres://|mysql://' \
  trade-tui tradectl trade-core >/tmp/trade-terminal-cockpit-boundary-rg.txt 2>/dev/null; then
  echo "repo boundary check failed: cockpit crates must stay projection/command-envelope only" >&2
  cat /tmp/trade-terminal-cockpit-boundary-rg.txt >&2
  exit 70
fi

echo "trade-terminal-cockpit boundary check passed"
