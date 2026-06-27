#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VM_HOST="${TRADE_COCKPIT_VM_HOST:-jisheng-yang@100.64.118.52}"
REMOTE_DIR="${TRADE_COCKPIT_VM_DIR:-/tmp/trade-terminal-cockpit-verify}"
SSH_TIMEOUT="${TRADE_COCKPIT_SSH_TIMEOUT:-10}"
COPY_BINARIES=0

usage() {
  cat <<'EOF'
Usage: tools/verify_on_google_vm.sh [--copy-binaries]

Runs the repository verification suite on the Google VM instead of compiling on
the local workstation. The VM is only a build/test worker; it is not a frontend
deployment target:
  cargo fmt --all -- --check
  cargo test --workspace
  tools/check_repo_boundary.sh
  tools/smoke_check.sh

Environment:
  TRADE_COCKPIT_VM_HOST     SSH target, default jisheng-yang@100.64.118.52
  TRADE_COCKPIT_VM_DIR      Remote temp dir, default /tmp/trade-terminal-cockpit-verify
  TRADE_COCKPIT_SSH_TIMEOUT SSH connect timeout seconds, default 10

Options:
  --copy-binaries           Copy VM-built trade-tui/tradectl into local .run/bin/
  -h, --help                Show this help
EOF
}

while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --copy-binaries)
      COPY_BINARIES=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 64
      ;;
  esac
done

cd "$ROOT_DIR"

git ls-files -z --cached --others --exclude-standard |
  tar --null --files-from=- -cf - |
  ssh -o ConnectTimeout="$SSH_TIMEOUT" "$VM_HOST" \
    "rm -rf '$REMOTE_DIR' && mkdir -p '$REMOTE_DIR' && tar -xf - -C '$REMOTE_DIR' && cd '$REMOTE_DIR' && git init -q && git add -A"

ssh -o ConnectTimeout="$SSH_TIMEOUT" "$VM_HOST" \
  "set -euo pipefail; cd '$REMOTE_DIR'; cargo fmt --all -- --check; cargo test --workspace; tools/check_repo_boundary.sh; tools/smoke_check.sh"

if [[ "$COPY_BINARIES" -eq 1 ]]; then
  mkdir -p "$ROOT_DIR/.run/bin"
  ssh -o ConnectTimeout="$SSH_TIMEOUT" "$VM_HOST" \
    "cd '$REMOTE_DIR/target/debug' && tar -cf - trade-tui tradectl" |
    tar -xf - -C "$ROOT_DIR/.run/bin"
  chmod +x "$ROOT_DIR/.run/bin/trade-tui" "$ROOT_DIR/.run/bin/tradectl"
fi
