# 0001 — Service language: Rust

## Status
Accepted

## Context
`logit` runs as a sidecar or host agent, often alongside the workload it's observing, so its own
resource footprint and latency behavior matter as much as its features. It also needs to embed a
scripting language efficiently, since user-defined transforms are the core value proposition.

## Decision
Write the service in Rust.

## Alternatives considered
- **Go.** Fastest path from the team's existing Python background, huge ecosystem for the exact
  protocols in scope (the OTel Collector, statsd/syslog libraries), trivial static cross-compilation
  including to Windows. Rejected primarily because of the scripting coupling: Go's GC introduces
  latency variance a sidecar should avoid, and there is no good LuaJIT story in Go without cgo,
  which undoes the easy static-binary story that makes Go attractive here in the first place. See
  [0002](0002-scripting-language-lua.md).
- **Zig or C.** Smallest possible footprint (the Fluent Bit approach) and trivial C interop with
  LuaJIT. Rejected for now: manual memory management across a concurrent, multi-protocol pipeline is
  a large amount of risk to take on for a greenfield project, the protocol-implementation ecosystem
  is thin, and Zig is still pre-1.0.

## Consequences
- No garbage collector: predictable memory footprint, no GC-pause latency spikes — important for a
  sidecar sharing resources with the workload it observes.
- `mlua` gives a statically-linked, real LuaJIT with no cgo-equivalent complexity.
- Steeper day-to-day contributor ramp-up than Go, offset by keeping everything containerized
  ([0005](0005-containerized-development.md)) so nobody needs the toolchain installed to contribute.
- Windows is not blocked by this choice (Rust cross-compiles to it), matching the project's
  "don't foreclose it, don't require it" stance on Windows support.
