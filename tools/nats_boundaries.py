from __future__ import annotations

CONTROL_BUS_STREAM_PREFIXES = ("OPS_",)

FORBIDDEN_TRADING_SUBJECT_PREFIXES = (
    "ops.",
    "control.",
    "hot.runtime.",
    "hfmkt.",
    "hfxuni.",
    "market.raw.",
    "marketdata.raw.",
    "news.raw.",
    "raw.",
    "broker.order.command.",
    "risk.command.",
    "account.command.",
)

FORBIDDEN_TRADING_SUBJECTS = (">", "trading.>")
REQUIRED_EVENT_SUBJECT_PREFIX = "trading.event."
AUDIT_SUBJECT_PREFIXES = ("trading.audit.", "trading.command.")


def normalize_subject(subject: str) -> str:
    return subject.strip().lower()


def is_control_bus_stream(stream: str) -> bool:
    return stream.upper().startswith(CONTROL_BUS_STREAM_PREFIXES)


def is_control_bus_subject(subject: str) -> bool:
    return normalize_subject(subject).startswith(("ops.", "control."))


def is_forbidden_trading_subject(subject: str) -> bool:
    normalized = normalize_subject(subject)
    return normalized in FORBIDDEN_TRADING_SUBJECTS or normalized.startswith(
        FORBIDDEN_TRADING_SUBJECT_PREFIXES
    )


def is_trading_event_subject(subject: str) -> bool:
    normalized = normalize_subject(subject)
    return normalized == "trading.event.>" or normalized.startswith(REQUIRED_EVENT_SUBJECT_PREFIX)


def is_command_audit_subject(subject: str) -> bool:
    normalized = normalize_subject(subject)
    return normalized in {"trading.audit.>", "trading.command.>"} or normalized.startswith(
        AUDIT_SUBJECT_PREFIXES
    )


def subject_compatible(config_subjects: list[str], requested: str) -> bool:
    if not requested:
        return True
    if requested in config_subjects:
        return True
    requested_root = requested.split(".", 1)[0]
    for configured in config_subjects:
        if configured == ">":
            return True
        if configured.endswith(".>") and requested.startswith(configured[:-1]):
            return True
        if requested.endswith(".>") and configured.startswith(requested[:-1]):
            return True
        if configured.split(".", 1)[0] == requested_root:
            return True
    return False


def stream_subjects_are_events(subjects: list[str]) -> bool:
    return bool(subjects) and all(is_trading_event_subject(subject) for subject in subjects)


def stream_subjects_are_command_audit(subjects: list[str]) -> bool:
    return bool(subjects) and all(is_command_audit_subject(subject) for subject in subjects)


def validate_event_stream_boundary(stream: str, subject: str) -> tuple[bool, str]:
    if is_control_bus_stream(stream):
        return False, "event stream must not be an OPS/control-bus stream"
    if (
        is_control_bus_subject(subject)
        or is_forbidden_trading_subject(subject)
        or not is_trading_event_subject(subject)
    ):
        return (
            False,
            "event projection must use trading.event.* and must not subscribe to ops/control/hot/raw/authority namespaces",
        )
    return True, "trading.event projection stream requested"


def validate_audit_stream_boundary(stream: str, subjects: list[str]) -> tuple[bool, str]:
    if is_control_bus_stream(stream):
        return False, "audit stream must not be an OPS/control-bus stream"
    if not stream_subjects_are_command_audit(subjects):
        return False, "audit stream must use only trading.audit.* and trading.command.* subjects"
    return True, "trading command/audit stream requested"


def validate_non_overlapping_streams(
    event_stream: str,
    event_subject: str,
    audit_stream: str,
    audit_subjects: list[str],
) -> tuple[bool, str]:
    if event_stream == audit_stream:
        return False, "event and audit streams must be distinct"
    if is_command_audit_subject(event_subject):
        return False, "event stream subject must not be command/audit namespace"
    if any(is_trading_event_subject(subject) for subject in audit_subjects):
        return False, "audit stream subjects must not include trading.event.*"
    return True, "event and command/audit streams are separated"
