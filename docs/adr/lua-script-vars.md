---
created: 2026-10-21
updated: 2026-10-21
---

# Lua `vars`: a component's `vars:` map reaches its script as a plain-table global

## Status
Accepted

## Context

A `lua_file` script copied from `examples/` carries deployment settings as locals at its top.
`examples/prusa/prusa.lua` named its InfluxDB bucket in a `local BUCKET = "prusa"` line, and a
deployment that wanted its printers tagged by name instead of MAC address added a hard-coded
`local NAMES = {["10:9c:70:2d:d5:c4"] = "sand"}` to its own copy. Each such edit forks the script:
a later fix to the shipped copy has to be merged into every deployment's by hand.

The script can't read its settings from anywhere else. The sandbox
([`docs/design/lua-api.md`](../design/lua-api.md)'s "Sandboxing") opens no `os` or `io` library,
removes `loadfile` and `dofile`, and has no `require`. Config is the one input a deployment
already owns, and `logit_cli::config::load` already resolves `!env` anywhere in it
([`env-yaml-tag`](env-yaml-tag.md)).

## Decision

`lua` and `lua_file` take an optional `vars:` field, a map with string keys, and the script sees
it as a global named `vars`.

1. **Plain Lua tables, built fresh into each VM** before the script's source runs, beside the
   other globals `ScriptWorker::new_with_vars` installs (`crates/logit-script/src/vars.rs`).
   Top-level code can read it. A write changes only that VM's copy.
2. **Always present.** With no `vars:` in the config the global is an empty table, so
   `vars.bucket or "prusa"` needs no nil check on `vars`.
3. **Type mapping**: a string to a string, a boolean to a boolean, an integer or a float to a
   number, a map to a table with string keys, and a list to an array table indexed from 1.
4. **`logit validate` rejects a `null` at any depth, and an integer larger than 2^53 in
   magnitude** (graph rule 95, `crates/logit-pipeline/src/graph.rs`). A Lua table can't hold
   `nil`: a `null` would vanish from a map and leave a hole that changes `#` in a list. Every
   integer up to 2^53 is exact as a double, the only number type LuaJIT has, and a larger one
   would reach the script silently rounded. The error names the key path (`vars.names.sand`,
   `vars.hosts[0]`) and never the value. Only a literal can trip it, given item 5, so its advice
   to quote the value in the YAML always applies. The config type, `logit_config::LuaVar`,
   deserializes both so validation can name the path; its published schema omits them.
5. **An `!env` value anywhere inside `vars:` stays a string**, even when it looks like a number,
   a boolean, or `null`; a script calls `tonumber` when it wants a number. Everywhere else the
   loader re-parses an `!env` value as a YAML scalar so it can fill a typed field
   ([`env-yaml-tag`](env-yaml-tag.md)). In `vars:` every type is valid, so a re-parse never fails:
   it would turn `API_TOKEN=1234567890123456` into a number the script prints as
   `1.2345678901235e+15`, and give a value past 2^53 a quoting error the environment can't follow.
   `logit_cli::config`'s resolver applies this to a `components.<id>.vars` subtree whose `type` is
   `lua` or `lua_file`.
6. **Nothing per event.** The tables are built once per VM, and a read is a table lookup.

## Alternatives considered

- **A frozen, read-only proxy** (userdata, or a table with `__index` and a refusing
  `__newindex`). LuaJIT, built without its Lua 5.2 compatibility as `logit` builds it, honors
  no `__pairs` or `__ipairs`, and no `__len` on a table, so `ipairs`, `next`, `#`, and
  `table.concat` would see an empty table or raise an error instead of reading the values. The
  sandbox's `pairs` wrapper (`crates/logit-script/src/pairs.rs`) covers `pairs` alone, and only
  for the attribute proxies. Every read would also pay an `__index` call. Freezing guards only a
  script against itself, since each VM has its own copy.
- **Reading the environment or a file from Lua** (`os.getenv`, `io.open`). Either one widens the
  sandbox to the host's environment or filesystem for every script. `!env` inside `vars:` reaches
  the same environment variables through the config loader, which stays the one place they're
  read.
- **Settings outside the config** (a sidecar file per script). A second file to deploy, with no
  `!env` and no `logit validate`.

## Consequences

- One shipped script serves every deployment; `examples/prusa/` reads its bucket and printer names
  from `vars:` and its `logit.yaml` shows the field.
- **`logit validate` can't check which keys a script reads.** It never runs the script, so a key
  the config misspells or omits reads as `nil` at run time. A script that needs a value can
  `assert` it at its top level, which fails the component at load.
- `vars` joins the pinned global allowlist in `the_global_table_is_exactly_the_allowlist`.
- `logit-script` depends on `logit-config` for `LuaVar`.
