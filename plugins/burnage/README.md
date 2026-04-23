# burnage (Claude Code plugin)

A single `SessionEnd` hook that records when a Claude Code session ends into
your burnage Durable Object, so the dashboard and `burnage session ends` can
show session boundaries.

## Prerequisites

1. Your own burnage proxy, deployed and reachable (see the
   [top-level README](https://github.com/ksc98/burnage#readme)).
2. `ANTHROPIC_BASE_URL` in your shell pointing at that proxy — same env var
   you already use to route Claude Code through burnage.
3. `jq` on `PATH` (used by the hook to read `session_id` from stdin).
4. The `burnage` CLI on `PATH`:

   ```bash
   cargo install --git https://github.com/ksc98/burnage burnage
   ```

   The CLI is published straight from the `burnage/` workspace member; no
   baked-in URL, so it defers entirely to `$ANTHROPIC_BASE_URL` at runtime.

## Install the plugin

Inside Claude Code:

```
/plugin marketplace add ksc98/burnage
/plugin install burnage@burnage
```

Then start a new Claude Code session. When it ends, the hook fires once.

## What the hook does

`hooks/hooks.json` registers one `SessionEnd` hook:

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
| Does installing the plugin execute code? | No. Plugins ship skills, hooks, agents, etc. as data — there is no install script. |
| Does the hook run arbitrary code? | No. The `command` string is the entire hook, visible in [`hooks/hooks.json`](./hooks/hooks.json). |
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
