# Workstream plans

Each plan is a single Markdown file named after the workstream (`slug.md`, no number). A slug is
permanent once written, because other docs and PR descriptions cite it by path. The
`created`/`updated` dates in each file's frontmatter order this index, not the filename.

A plan's workstreams share a stream key that names their branches (`<key>/w<N>`) and prefixes
their PR titles. The key is picked when the work is planned and may be noted in the plan, but the
branches and PRs are its record. See [Branches and PR titles](../../AGENTS.md#branches-and-pr-titles).

| Plan | Created | Updated |
|---|---|---|
| [Enabling plan: native encode without per-field scratch buffers](native-encode-allocations.md) | 2026-10-15 | 2026-10-15 |
| [Enabling plan: a soak harness — faults on a timeline, a watchdog, and a loss ledger](soak-harness.md) | 2026-10-08 | 2026-10-18 |
| [Research note: live reload, socket handover, and what each deployment world needs](live-reload-and-socket-handover.md) | 2026-10-05 | 2026-10-07 |
| [Enabling plan: SigNoz — OTLP-native, verified, no new kind](signoz-relay.md) | 2026-09-24 | 2026-09-24 |
| [Enabling plan: OpenSearch — SS4O documents over `_bulk`](opensearch-relay.md) | 2026-09-24 | 2026-10-05 |
| [Enabling plan: Dynatrace — OTLP and the ingest APIs, OneAgent-local stand-in, intake stand-in](dynatrace-relay.md) | 2026-09-24 | 2026-09-24 |
| [Enabling plan: New Relic — direct APIs and OTLP, shipper-intake stand-in, APM collector stand-in](newrelic-relay.md) | 2026-09-24 | 2026-10-05 |
| [Verification plan: critical sections inventory](critical-sections-inventory.md) | 2026-09-20 | 2026-10-11 |
