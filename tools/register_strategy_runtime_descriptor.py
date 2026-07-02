#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any


DEFAULT_REGISTRY = Path.home() / ".local/state/hot-runtime/strategy-registry"


def optional_path(value: Path | None) -> str | None:
    if value is None:
        return None
    return str(value.expanduser())


def sanitize(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.+-]+", "_", value).strip("_")


def main() -> int:
    parser = argparse.ArgumentParser(description="Register an account-scoped runtime strategy descriptor for cockpit discovery")
    parser.add_argument("--registry-dir", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--account-mode", choices=["paper", "live", "replay", "sim"], required=True)
    parser.add_argument("--gateway-tier")
    parser.add_argument("--strategy-id", required=True, help="Canonical runtime strategy id/name, not the cockpit instance id")
    parser.add_argument("--strategy-instance-id")
    parser.add_argument("--runtime-strategy-id", type=int)
    parser.add_argument("--runtime-variant")
    parser.add_argument("--component-role", default="strategy")
    parser.add_argument("--service")
    parser.add_argument("--runtime-config", type=Path)
    parser.add_argument("--operator-status", type=Path)
    parser.add_argument("--activation-manifest", type=Path)
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--artifact-path", type=Path)
    parser.add_argument("--lob-dynamics-required", action="store_true")
    parser.add_argument("--output")
    args = parser.parse_args()

    strategy_instance_id = args.strategy_instance_id or f"{args.account_id}/{args.strategy_id}"
    descriptor: dict[str, Any] = {
        "account_id": args.account_id,
        "account_mode": args.account_mode,
        "gateway_tier": args.gateway_tier or args.account_mode,
        "strategy_id": args.strategy_id,
        "strategy_instance_id": strategy_instance_id,
        "component_role": args.component_role,
        "lob_dynamics_required": bool(args.lob_dynamics_required),
    }
    optional_values = {
        "runtime_strategy_id": args.runtime_strategy_id,
        "runtime_variant": args.runtime_variant,
        "service": args.service,
        "runtime_config": optional_path(args.runtime_config),
        "operator_status": optional_path(args.operator_status),
        "activation_manifest": optional_path(args.activation_manifest),
        "env_file": optional_path(args.env_file),
        "artifact_path": optional_path(args.artifact_path),
    }
    for key, value in optional_values.items():
        if value not in {None, ""}:
            descriptor[key] = value

    output = Path(args.output).expanduser() if args.output else None
    if output is None:
        name_parts = [sanitize(args.account_id), sanitize(args.strategy_id)]
        if args.runtime_variant:
            name_parts.append(sanitize(args.runtime_variant))
        output = args.registry_dir.expanduser() / f"{'.'.join(name_parts)}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    tmp = output.with_name(f"{output.name}.tmp")
    tmp.write_text(json.dumps(descriptor, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(output)
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
