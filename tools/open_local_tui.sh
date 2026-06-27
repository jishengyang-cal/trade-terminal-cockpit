#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -n "${TRADE_TUI_BIN:-}" ]]; then
  BIN="$TRADE_TUI_BIN"
elif [[ -x "$ROOT_DIR/.run/bin/trade-tui" ]]; then
  BIN="$ROOT_DIR/.run/bin/trade-tui"
elif [[ -x "$ROOT_DIR/target/debug/trade-tui" ]]; then
  BIN="$ROOT_DIR/target/debug/trade-tui"
else
  cat >&2 <<'EOF'
No local trade-tui binary was found.

This launcher only opens the local terminal cockpit. It does not run cargo and
does not SSH anywhere.

Build and copy VM-produced binaries first:
  tools/verify_on_google_vm.sh --copy-binaries
EOF
  exit 70
fi

if [[ "$#" -eq 0 ]]; then
  set -- --mock
fi

exec "$BIN" "$@"
