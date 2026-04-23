# burnage (Claude Code plugin)

A single `SessionEnd` hook that records when a Claude Code session ends into
your burnage Durable Object, so the dashboard and `burnage session ends` can
show session boundaries.

## Prerequisites

1. Your own burnage proxy, deployed and reachable (see the
   [top-level README](https://github.com/ksc98/burnage#readme)).
2. `ANTHROPIC_BASE_URL` in your shell pointing at that proxy — same env var
   you already use to route Claude Code through burnage.
3. `jq` and `cargo` on `PATH`. The first `SessionStart` after install uses
   `cargo` to fetch the `burnage` CLI in the background; `jq` is used by
   the `SessionEnd` hook to read `session_id` from stdin.

The `burnage` CLI is published straight from the `burnage/` workspace
member; no baked-in URL, so it defers entirely to `$ANTHROPIC_BASE_URL`
at runtime.

## Install the plugin

Inside Claude Code:

```
/plugin marketplace add ksc98/burnage
/plugin install burnage@burnage
```

Start a new Claude Code session. If the `burnage` CLI isn't on `PATH`, the
`SessionStart` hook kicks off `cargo install --git https://github.com/ksc98/burnage burnage`
in the background (logs to `/tmp/burnage-install.log`). The first session
after install may end before the CLI finishes building — subsequent
`SessionEnd` events will record normally.

To install the CLI eagerly instead:

```bash
cargo install --git https://github.com/ksc98/burnage burnage
```

## What the hooks do

`hooks/hooks.json` registers two hooks:

**`SessionStart`** — bootstraps the CLI if missing:

```
command -v burnage >/dev/null 2>&1 || { command -v cargo >/dev/null 2>&1 && nohup cargo install --git https://github.com/ksc98/burnage burnage --locked >/tmp/burnage-install.log 2>&1 </dev/null & } ; true
```

No-op once `burnage` is on `PATH`. The install runs detached via `nohup &`
so it never blocks session startup.

**`SessionEnd`** — records the ended session:

```
jq -r '.session_id // empty' | xargs -r -I{} burnage session end {}
```

That's the entire contract:

- Reads the hook event JSON from stdin, extracts `session_id`.
- Runs `burnage session end <id>`, which POSTs
  `{"session_id":"<id>"}` to `$ANTHROPIC_BASE_URL/_cm/session/end`
  with `Authorization: Bearer <your Claude Code OAuth token>`.
- The proxy writes `ended_at` for that session into your Durable Object.

No prompts, no response bodies, no anything else is sent.

## Is it safe?

The plugin never calls out to anything other than the URL in **your own**
`$ANTHROPIC_BASE_URL`. Specifically:

| Concern | Answer |
| ------- | ------ |
| Does the plugin bake in a server URL? | No. The CLI uses `$ANTHROPIC_BASE_URL` (the same var you already set to point at your proxy). |
| Does installing the plugin execute code? | No. Installing the plugin only drops hook config on disk. On first `SessionStart`, the hook runs `cargo install --git https://github.com/ksc98/burnage burnage` if the CLI is missing — detached via `nohup &` so it can't block you, and skipped once the CLI is present. |
| Does the hook run arbitrary code? | No. The `command` strings are the entire hooks, visible in [`hooks/hooks.json`](./hooks/hooks.json). |
| Does the hook send my prompts/responses? | No. It sends only `{"session_id": "<id>"}`. |
| Where does my OAuth token go? | The CLI reads `~/.claude/.credentials.json` and sends the token only as `Authorization: Bearer …` to `$ANTHROPIC_BASE_URL`. |
| What if the CLI isn't installed? | `xargs` fails with "command not found" on session end; nothing else breaks. |

The trust model is exactly the same as running `burnage` interactively: the
proxy operator (you) sees your traffic, and you choose the URL.

## Uninstalling

```
/plugin uninstall burnage@burnage
/plugin marketplace remove burnage
```

The CLI itself stays until you `cargo uninstall burnage`.
