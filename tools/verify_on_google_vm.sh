#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VM_HOST="${TRADE_COCKPIT_VM_HOST:-jisheng-yang@100.64.118.52}"
REMOTE_DIR="${TRADE_COCKPIT_VM_DIR:-/tmp/trade-terminal-cockpit-verify}"
SSH_TIMEOUT="${TRADE_COCKPIT_SSH_TIMEOUT:-10}"
SSH_CONFIG_FILE="${TRADE_COCKPIT_SSH_CONFIG_FILE:-/dev/null}"
SSH_PROXY_COMMAND="${TRADE_COCKPIT_SSH_PROXY_COMMAND:-}"
VM_TRANSPORT="${TRADE_COCKPIT_VM_TRANSPORT:-ssh}"
RUST_DOCKER_IMAGE="${TRADE_COCKPIT_VM_RUST_DOCKER_IMAGE:-rust:1.88}"
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
  TRADE_COCKPIT_SSH_CONFIG_FILE
                            SSH config file, default /dev/null to avoid host
                            global config drift during VM verification
  TRADE_COCKPIT_SSH_PROXY_COMMAND
                            Optional ssh ProxyCommand, e.g. tailscale nc %h %p
  TRADE_COCKPIT_VM_TRANSPORT
                            ssh or tailscale, default ssh
  TRADE_COCKPIT_VM_RUST_DOCKER_IMAGE
                            Rust Docker image for VM verification, default rust:1.88

Options:
  --copy-binaries           Copy VM-built binaries into local .run/bin/
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

SSH_ARGS=(-F "$SSH_CONFIG_FILE" -o ConnectTimeout="$SSH_TIMEOUT")
if [[ -n "$SSH_PROXY_COMMAND" ]]; then
  SSH_ARGS+=(-o "ProxyCommand=$SSH_PROXY_COMMAND")
fi

remote_exec() {
  local remote_command="$1"
  case "$VM_TRANSPORT" in
    ssh)
      ssh "${SSH_ARGS[@]}" "$VM_HOST" "$remote_command"
      ;;
    tailscale)
      tailscale ssh "$VM_HOST" "$remote_command"
      ;;
    *)
      echo "unknown TRADE_COCKPIT_VM_TRANSPORT: $VM_TRANSPORT" >&2
      exit 64
      ;;
  esac
}

git ls-files -z --cached --others --exclude-standard |
  tar --null --files-from=- -cf - |
  remote_exec "sudo -n rm -rf '$REMOTE_DIR' && mkdir -p '$REMOTE_DIR' && tar -xf - -C '$REMOTE_DIR' && cd '$REMOTE_DIR' && git init -q && git add -A"

remote_exec "set -euo pipefail; trap 'sudo -n chown -R \$(id -u):\$(id -g) '$REMOTE_DIR'' EXIT; cd '$REMOTE_DIR'; sudo -n docker run --rm -v '$REMOTE_DIR:/repo' -w /repo '$RUST_DOCKER_IMAGE' bash -c 'export PATH=/usr/local/cargo/bin:\$PATH; apt-get update; apt-get install -y --no-install-recommends protobuf-compiler; rm -rf /var/lib/apt/lists/*; git config --global --add safe.directory /repo; if cargo fmt --version >/dev/null 2>&1; then cargo fmt --all -- --check; else echo \"cargo fmt unavailable in image; skipping rustfmt\"; fi; cargo test --workspace; tools/check_repo_boundary.sh; tools/smoke_check.sh'"

remote_exec "cd '$REMOTE_DIR' && tar -cf - Cargo.lock" |
  tar -xf - -C "$ROOT_DIR"

if [[ "$COPY_BINARIES" -eq 1 ]]; then
  mkdir -p "$ROOT_DIR/.run/bin"
  remote_exec "cd '$REMOTE_DIR/target/debug' && tar -cf - trade-tui tradectl state-projectiond command-gateway" |
    tar -xf - -C "$ROOT_DIR/.run/bin"
  chmod +x "$ROOT_DIR/.run/bin/trade-tui" "$ROOT_DIR/.run/bin/tradectl" "$ROOT_DIR/.run/bin/state-projectiond" "$ROOT_DIR/.run/bin/command-gateway"
fi
