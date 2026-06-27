# Tailnet Access

`trade-terminal-cockpit` is a local terminal cockpit, not an HTTP frontend. Do
not publish it with Tailscale Serve as a web page.

Tailnet access is only for auxiliary remote-operator sessions into the trading
host. It is not the default frontend path and it is not a Google VM deployment
path. The normal operator workflow is local terminal first:

```bash
tools/open_local_tui.sh --mock
```

For a remote operator session, use a tailnet SSH session into the trading host,
then run the local launcher inside that terminal, tmux, or Zellij:

```bash
tools/tailnet_cockpit_url.sh
```

The script prints:

- an SSH URI using MagicDNS when available
- an SSH URI using the tailnet IPv4 address
- the local command to start `trade-tui`

Example local command after connecting:

```bash
tools/open_local_tui.sh --mock
```

For projection/event testing without broker, database, or service-manager
coupling, use JSONL:

```bash
tools/open_local_tui.sh --event-jsonl /path/to/events.jsonl --follow
```
