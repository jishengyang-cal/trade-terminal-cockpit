# Tailnet Access

`trade-terminal-cockpit` is a terminal cockpit, not an HTTP frontend. Do not
publish it with Tailscale Serve as a web page.

The supported remote entrypoint is a tailnet SSH session into the trading host,
then running `trade-tui` inside SSH, tmux, or Zellij:

```bash
tools/tailnet_cockpit_url.sh
```

The script prints:

- an SSH URI using MagicDNS when available
- an SSH URI using the tailnet IPv4 address
- the local command to start `trade-tui`

Example local command after connecting:

```bash
cargo run -p trade-tui -- --mock
```

For projection/event testing without broker, database, or service-manager
coupling, use JSONL:

```bash
cargo run -p trade-tui -- --event-jsonl /path/to/events.jsonl --follow
```
