---
created: 2026-09-18
updated: 2026-09-18
---

# A disposable Azure VM for perf measurement: `up`/`down` only, no stop and no snapshot

## Status
Accepted

## Context

[`docs/design/performance.md`](../design/performance.md) records `script/perf`'s numbers with an
explicit caveat: they're taken "solo, on battery, with the host otherwise idle" and its own
"Doesn't measure" list names the gap directly — *"Isolation from the rest of the box. This runs
in the same kind of dev-container environment every other `script/*` command does, not a
dedicated, pinned-core bench host."* [`heterogeneous-cores-bench-noise`](../design/memory.md) (and
the equivalent memory note from this workstream) records why that matters concretely: the dev
laptop mixes Zen 5 and Zen 5c cores, so an unpinned run comes out bimodal depending on which core
type the pipeline's worker threads land on — roughly 2x apart, not a few percent. Add thermal
throttling and whatever else the machine is doing (other worktrees running `script/cibuild` while
a measurement is in flight is routine here), and a laptop number is a poor basis for "did this PR
regress performance."

A cloud VM removes all of that by construction, provided it's the right shape: homogeneous cores,
no throttling, and nothing else running on the box. The constraint that shapes everything below is
cost — this must be genuinely $0 when not in use. This subscription (`Visual Studio Enterprise`)
already carries several unrelated resource groups for other work; this change adds a completely
separate one and nothing else.

This ADR does **not** answer [`load-test-harness`](load-test-harness.md)'s deliberately-open "when
the harness runs" question — that ADR leaves cadence open pending hands-on experience running the
harness by hand, and this change supplies a better *machine* for that, not a schedule.

## Decision

`script/vm` is a new host-side script with four subcommands: `up`, `shell`, `status`, `down`.
`up` provisions a VM and warms it to the point of having already run one scenario; `shell` opens a
session on it; `status` reports what's running and its cost so far; `down` deletes every billable
resource. There is no fifth verb.

### The size, and why 4 vCPUs is 4 cores

`Standard_F4as_v6`: 4 vCPU, 16 GiB, AMD EPYC 9004 (Genoa, up to 3.7 GHz boost). Verified via
`az vm list-skus`: `vCPUsPerCore: 1` — SMT is disabled for this whole series, so every vCPU is a
full physical core with no sibling thread to contend with. That single property is what the
laptop's Zen 5 / Zen 5c split can't offer: four *identical* cores, not two kinds. Also verified:
`MaxResourceVolumeMB: 0` (no local temp disk — irrelevant here, nothing needs one),
`EphemeralOSDiskSupported: False` (hence an ordinary managed OS disk, not an ephemeral one),
`HyperVGenerations: V2` (gen2 image required), and `MemoryPreservingMaintenanceSupported: True`
(see Consequences). 16 GiB over the 8 GiB `F4als_v6` variant costs $0.03/hr more and gives real
headroom for a cold release build plus a warm `CARGO_HOME` and `target/`.

Verified live in this subscription: unrestricted and available in `eastus` zones 1/2/3, with
`StandardFasv6Family` quota 0/20 used and regional vCPU quota 2/20 used — room for this VM with no
quota request.

### Debian 13 (gen2), pinned by version for real comparisons, and no in-place upgrade

`Debian:debian-13:13-gen2:latest` — verified present in `eastus`, gen2 as the size requires.
`script/vm-cloud-init.yaml` sets `package_update: true` but **`package_upgrade: false`**: an
in-place upgrade would make the kernel and libc a function of which day the VM happened to be
created, which is exactly the nondeterminism this machine exists to remove, and would ask for a
reboot besides. `logit-vm-metadata.txt` (below) records whichever image version `:latest` actually
resolved to, so a long-running comparison can pin `LOGIT_VM_IMAGE` to that exact URN.

### Lifecycle is `up`/`down` only — no stop, no snapshot

`down` runs `az group delete`. There's no `az vm stop`/`deallocate`/`start` path and no OS-disk
snapshot path. A deallocated VM still bills for its attached disk (a 128 GiB Premium SSD here is
roughly $19/month sitting idle), and a snapshot bills forever, however small. Neither is $0. The
cost of this decision is a cold ~15–20 minute provision-and-build at the start of every session —
paid knowingly, and made bearable by warming the cache as part of `up` (below) so it's the last
time that session pays it.

### A dedicated resource group, vnet, NSG, and public IP

Everything lives in its own resource group (`logit-perf`, tagged `purpose=perf
managed-by=script/vm`), with its own vnet (`10.42.0.0/16`, chosen to avoid colliding with a
192.168/16 home LAN or Docker's own 172.17/172.18 bridges), subnet, NSG, and Standard static public
IP. Nothing is shared with the subscription's other resource groups. `down` refuses to delete a
group that isn't tagged `purpose=perf` — a mistyped `LOGIT_VM_RESOURCE_GROUP` must not be able to
reach one of them.

### SSH is open to the internet, deliberately

The NSG's single inbound rule allows TCP/22 from `*`. Scoping it to the operator's current public
IP was considered and rejected: it's flaky behind CGNAT, and it breaks on every network switch —
for a box that gets torn down and recreated every session, that's a worse failure mode than the
exposure it would prevent, and it would need either a fifth subcommand to refresh the rule or for
every `up` to silently redo it.

What actually protects the box: `az vm create` is given `--ssh-key-values` and no password, so
key-only auth is provisioned by the platform; `vm-cloud-init.yaml` additionally asserts
`ssh_pwauth: false` and `disable_root: true` rather than relying on the image's own defaults, since
port 22 really is reachable from anywhere; there is exactly one authorized key (the dedicated key
below); the only service listening is `sshd`; and the machine exists for hours at a time, not
indefinitely.

### A dedicated, unpassworded ed25519 key that `down` never deletes

The host that runs this script had no SSH keypair of any kind before this change. `up` generates
one at `~/.ssh/logit-perf-vm{,.pub}` with `ssh-keygen -t ed25519 -N ''` if it's absent, and `down`
never removes it — the key's fingerprint stays stable across every VM this script ever creates,
while the VM itself doesn't. No passphrase, because the key grants access to a machine holding
nothing but a clone of a public repo, for a session measured in hours, and a passphrase would mean
either an agent or a prompt on each of `up`'s several `ssh` calls.

Host-key churn is handled the same way: a dedicated `~/.ssh/logit-perf-vm.known_hosts`, referenced
via `-F /dev/null -o UserKnownHostsFile=... -o GlobalKnownHostsFile=/dev/null` on every `ssh` call
(this host's own `~/.ssh/config` carries a global `IdentityAgent` and a `ControlMaster` block that
would otherwise interfere) with `StrictHostKeyChecking=accept-new`. `up` clears that file only on
the path where it actually created a new VM (so re-running `up` against a live one doesn't erase
the very record that would catch a changed key); `down` clears it unconditionally. The ordinary
`~/.ssh/known_hosts` is never touched.

### cloud-init for machine setup; `up` for the cache warm

`vm-cloud-init.yaml` handles everything that has to be true before a login shell means anything:
packages, Docker, sysctls, quieting background services, the clone, and a metadata capture.
Cloning the repo and building it, though, happens over SSH from `up` itself, after cloud-init
finishes — not inside cloud-init. That step is the one that takes most of the wall clock (pulling
`rust:1.98.1-bookworm`, `Dockerfile.dev`'s pinned `cargo install --locked` tool builds, then a
release build of the whole workspace), and its output belongs streaming to whoever is waiting on
it, not buried in `/var/log/cloud-init-output.log` where it's only read after something's already
gone wrong. `script/setup` already makes the same split locally: the image build is something you
watch, not something that happens silently before you're told it's done.

### Docker CE, not Debian's own packaging

`vm-cloud-init.yaml` adds `download.docker.com/linux/debian trixie stable` (verified present) and
installs `docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin`, key
pinned by fingerprint rather than trusted blindly. Three reasons, in order of how much they matter:

1. **Version parity with the dev box.** [`containerized-development`](containerized-development.md)
   develops against current Docker CE and Compose v2. Debian trixie's own `docker.io`/
   `docker-buildx`/`docker-compose` packages are meaningfully older and independently versioned —
   exactly the kind of build-path difference a machine built to produce *comparable* numbers
   shouldn't introduce.
2. **BuildKit features `Dockerfile.dev` actually uses.** Its `# syntax=docker/dockerfile:1` header
   and several `RUN --mount=type=cache` lines need `docker-buildx-plugin`, whose version tracks
   Docker CE's own release, not Debian's separate packaging cadence.
3. **`common.sh` invokes `${DOCKER} compose` as a CLI plugin.** Debian trixie ships no
   `docker-compose-v2`/plugin package at all — only a standalone `docker-compose` binary — so the
   plugin path `common.sh` relies on wouldn't exist without this.

### Sysctls: two raised ceilings, two pinned defaults, two perf knobs

Written to `/etc/sysctl.d/99-logit-perf.conf` and applied by `sysctl --system` in `runcmd`:

- `net.core.rmem_max` / `wmem_max` raised to 16 MiB — a ceiling on what an explicit
  `SO_RCVBUF`/`SO_SNDBUF` request may ask for, not an allocation; it costs nothing until a socket
  asks. **Honestly scoped:** no scenario under `perf/scenarios/` uses UDP today (they're all
  `generate_in`, plus `native-relay`'s loopback TCP pair), and this sysctl doesn't affect TCP
  autotuning at all (that's `net.ipv4.tcp_rmem`, left untouched). This exists so the first UDP
  scenario measures the socket it actually asked for instead of a silently clamped one —
  `crates/logit-inputs/src/udp.rs` already warns exactly when the granted size comes back under 2x
  the requested one — not because anything is clamped right now.
- `net.core.rmem_default` / `wmem_default` **pinned at the stock 212992**, deliberately not
  raised. This is the size a socket gets when nothing asks for one; raising it would change every
  socket in every scenario, make VM numbers incomparable to laptop numbers, and mask a config that
  simply forgot its own `receive_buffer_bytes:`. Pinned explicitly (rather than left as "whatever
  the image ships") so a future Debian default change shows up as a diff to this file instead of
  an unexplained shift in a results table.
- `kernel.perf_event_paranoid = 1` and `kernel.kptr_restrict = 0`, so `perf record`/`perf stat`
  work directly as the admin user without a container, and so kernel frames in a capture resolve
  to symbol names. Debian's kernels are built with `CONFIG_SECURITY_PERF_EVENTS_RESTRICT`, so the
  stock default here is 3 (`perf_event_open` refused outright without `CAP_PERFMON`/
  `CAP_SYS_ADMIN`), not upstream's 2.

### Background work is masked; three things are deliberately left alone

`bootcmd` masks and stops `unattended-upgrades`, `apt-daily(-upgrade)`, `man-db.timer`,
`fstrim.timer`, `e2scrub_all.timer`/`e2scrub_reap`, `dpkg-db-backup.timer`, `logrotate.timer`, and
`systemd-tmpfiles-clean.timer` — each one a real source of a mid-measurement CPU or disk spike on
a box that lives for one session, and it runs in the `init` stage, before `packages:`, specifically
so nothing races the Docker install for the dpkg lock. `walinuxagent` is left running (it reports
provisioning success and serves every `az vm` operation this script makes, and is idle otherwise)
and so is `systemd-timesyncd` (the harness's events/s is wall-clock derived, so a drifting clock
would corrupt a measurement rather than protect one, and Azure's own time source is more accurate
than NTP, not less).

### Everything account-specific is an env var

`LOGIT_VM_SUBSCRIPTION`, `_LOCATION`, `_ZONE`, `_SIZE`, `_IMAGE`, `_OS_DISK_GB`, `_STORAGE_SKU`,
`_RESOURCE_GROUP`, `_NAME`, `_ADMIN_USER`, `_VNET_PREFIX`, `_SUBNET_PREFIX`, `_REPO_URL`,
`_REPO_REF`, `_SSH_KEY`, `_KNOWN_HOSTS`, `_HOURLY_USD` all default sensibly and can be overridden.
`script/vm` never calls `az account set`, so it can't disturb whatever subscription is currently
the CLI's default; it only ever passes `--subscription` explicitly when `LOGIT_VM_SUBSCRIPTION` is
set, and every subcommand prints the resolved subscription first.

## Alternatives considered

- **GitHub Actions larger runners.** No host control, no ability to set the sysctls above, still a
  shared multi-tenant host, and no PMU either (see Consequences) — solves none of the specific
  problems this exists for.
- **A persistent VM with stop/start instead of delete/recreate.** Nonzero idle cost (a stopped VM
  still bills its disk), and configuration drift across weeks is exactly the kind of thing the
  metadata capture below exists to catch, not prevent.
- **An image or snapshot of the warmed VM, to skip the build each time.** Bills for storage
  indefinitely, needs a Compute Gallery or a second persistent resource group to hold it, and goes
  stale against `Dockerfile.dev` the moment that file changes.
- **A preserved static public IP across sessions**, to stop host-key churn. It would have to
  survive `down` to do any good, which means it bills (~$3.65/month for an unassociated Standard
  IP) for as long as it's kept — a nonzero idle cost, which is the one thing the lifecycle decision
  rules out.
- **Restricting SSH to the operator's current public IP.** Rejected above — flaky behind CGNAT,
  breaks on every network switch, and the actual protection here is key-only auth, not source
  filtering.
- **Azure Dedicated Host**, to remove shared last-level cache and memory bandwidth contention
  entirely. Solves a real residual noise source (see Consequences) at roughly two orders of
  magnitude more cost and a minimum-duration commitment; not justified for PR-scale regression
  checks.
- **A bigger or ARM-based size** (e.g. `Standard_D4pls_v6`). ARM is the wrong ISA for comparing
  against the x86_64 dev/CI/production path; a larger x86 size adds cores the pipeline's worker
  count doesn't need and adds cost with no corresponding benefit.
- **Pinning the measured process to a CPU subset** (`taskset`/`docker run --cpuset-cpus`).
  Considered and rejected for this box specifically — see Consequences.

## Consequences

- **No virtualized PMU.** Azure guests get no hardware performance counters, so `perf record`'s
  default `cycles` event (which `crates/logit-perf/src/flamegraph.rs`'s `record_argv` requests —
  it passes no `-e`) isn't available; perf falls back to a software `cpu-clock` timer. The
  flamegraph's function-level "where did on-CPU time go" attribution stays valid — it's still
  time-based sampling at the same frequency — but microarchitectural detail (IPC, cache misses,
  branch prediction) is simply unavailable on this machine. The harness's headline number, CPU
  µs/event, is unaffected either way: it comes from `wait4`'s `ru_utime + ru_stime`
  (`crates/logit-perf/src/rusage.rs`), which needs no PMU at all. `linux-perf` is installed
  directly on the VM (not just inside the profiling container) so `perf stat -e cycles true` gives
  a one-line, recorded answer to "does this box have a PMU" without building anything first. If the
  fallback doesn't fire cleanly, the follow-up is an `-e/--event` flag on `logit-perf flamegraph`
  defaulting to `cpu-clock` — not built here.
- **Host maintenance can pause a run mid-measurement.** `MemoryPreservingMaintenanceSupported:
  True` means a host update can briefly freeze the VM. This inflates wall-clock-derived numbers
  (events/s) but not CPU-time-derived ones (µs/event, since `ru_utime`/`ru_stime` don't advance
  while the vCPU isn't scheduled) — the same argument `load-test-harness.md` already makes for
  preferring that number. For a long unattended run, checking
  `curl -H Metadata:true 'http://169.254.169.254/metadata/scheduledevents?api-version=2020-07-01'`
  for a queued `Freeze`/`Reboot`/`Redeploy` first is a cheap precaution, left manual rather than
  automated.
- **Last-level cache and memory bandwidth are still shared with neighbours**, even though CPU time
  isn't oversubscribed. This is real, unbounded, and — with no PMU — not even measurable from
  inside the guest. It's the largest residual noise source on this machine and the reason
  `--repeat` and medians matter more here than pinning.
- **Cross-session comparisons are weaker than same-session ones.** A new `up` may land on
  different physical hardware and, with `:latest`, a different image version. Azure exposes no
  physical host identity, so "was this the same host as last time" is unanswerable from the guest.
  `script/perf compare` should be run on two results produced within one `up` session; a
  cross-session comparison should cross-check `logit-vm-metadata.txt`'s CPU model, microcode,
  kernel, and Azure image version first, and pin `LOGIT_VM_IMAGE` to an explicit version rather
  than `:latest` if it's going to recur.
- **Pinning is deliberately not used.** `--pin-sender`/`--pin-child`-style flags don't exist on
  `crates/logit-perf`'s CLI, and reserving a core with `taskset`/`--cpuset-cpus` would change what
  gets measured: `crates/logit-cli`'s Tokio runtime sizes its worker pool from
  `available_parallelism()`, so narrowing the CPU set narrows the pipeline's own worker count,
  measuring a different topology rather than a cleaner view of the same one. This VM's whole
  reason to exist — four identical cores, nothing else running — is what makes reserving one
  unnecessary; a `script/perf --cpuset-cpus` pass-through remains available future work if it
  turns out to matter.
- **`script/perf`'s host-side git-SHA computation is moot here, harmlessly.** It exists because a
  *worktree* checkout's `.git` file points outside the container's bind mount; the VM's clone is
  plain, so `.git` is a real directory inside the mount and would work either way. The host-side
  value still wins (nothing changes), and the actual trap is `LOGIT_PERF_GIT_DIRTY`: it flags *any*
  untracked file in `~/logit`, which is exactly why `logit-vm-metadata.txt` is written to
  `$HOME`, not into the clone.
- **`script/vm` is the first `script/*` command that needs a host tool (`az`) and bypasses
  `common.sh`'s `run()`/`build_image()` entirely** — nothing here drives a local Docker daemon.
  It still sources `common.sh` for `ROOT`/`set -e`, the same partial-use pattern `script/image`
  already establishes for the same reason (a build against the *host's* Docker daemon, not the
  containerized one).
- **The uid-1000 assumption is asserted, not merely hoped for.** `runcmd` checks the admin user's
  uid and fails cloud-init loudly if it isn't 1000, since `compose.yaml` hard-codes `user:
  "1000:1000"` and a silent mismatch would show up much later as files owned by the wrong uid.
- **Nothing here runs in CI.** CI has no Azure credentials, and this workflow costs real money —
  entirely consistent with `script/bench`/`script/perf` already being excluded from
  `script/cibuild` for the same "measures the runner, not the code" reason.
- **Cost**: ~$0.305/hour running (VM + 128 GiB Premium SSD + the static IP), $0 once `down`
  completes. A `Makefile` `vm` target defaults its bare form to `status` (read-only) rather than
  `up`, so `make vm` can't accidentally start spending.
