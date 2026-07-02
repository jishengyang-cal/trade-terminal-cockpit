#!/usr/bin/env bash
set -euo pipefail

# Keep this repo wrapper thin so the edit-loop gate does not diverge from other
# trading repos on the same workstation.
exec "${CODEX_EDIT_GATE_BIN:-$HOME/.local/bin/codex-edit-gate}" "$@"
