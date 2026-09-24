//! Driving a scenario through a **real UDP socket** instead of an in-process `generate_in`.
//!
//! A generated scenario measures the graph and nothing about intake: no socket, no datagram
//! parsing, no receive buffer to overflow. A `Driven` scenario tests that intake path
//! (docs/adr/udp-intake-batching-and-socket-visibility.md): an ordinary `statsd_in → null_out`
//! config with **no generator**, loaded by this module from inside the `logit-perf` process.
//!
//! The sender runs in this process so the child's `wait4` rusage is **pure receive-side CPU**,
//! keeping `cpu_us_per_event` the gate `compare` treats it as. Pin the sender and the child to
//! disjoint CPUs (`--pin-sender`/`--pin-child`) so they don't contend for one core's time.
//!
//! ## The spec, and why it is a sidecar
//!
//! A `logit` config says what to listen for, not what to send, so the load spec is a second file,
//! `perf/load/<scenario>.yaml`. It lives in its own directory because both `script/validate` and
//! `crates/logit-cli/src/config.rs`'s `every_shipped_config_loads_and_validates` glob
//! `perf/scenarios/*.yaml`, where every file must be a valid `logit` config. `perf/load/README.md`
//! has the format.
//!
//! ## The traffic model
//!
//! The load must look like real statsd traffic, not N copies of one line (the ADR's
//! "Representative traffic, calibrated against a recorded real-client capture"). Two weighted
//! lists carry that:
//!
//! - **`lines:`** (in a shared model file): weighted `logit_core::template` templates covering
//!   the real metric-type mix, hierarchical dotted names, DogStatsD tags on most lines with a
//!   tagless share, and a sampled minority. `{seq%N}` is the cardinality knob, as in
//!   `generate_in`.
//! - **`datagram_mix:`**: weighted packing targets, one line per datagram (an unbuffered client)
//!   or lines packed up to a byte ceiling (a buffered one). This axis decides which half of the
//!   pipeline a number is about, hence three scenarios.
//!
//! Everything is pre-rendered into a **ring** of finished datagrams before the blast (see
//! [`Ring`]), so rendering costs nothing at send time, and the ring is deterministic from the
//! spec's `seed`: two runs of one spec send identical bytes. Weighted choice stays here;
//! `logit_core::template` is only a placeholder grammar.
//!
//! ## The sender
//!
//! `std` threads and blocking sockets, not tokio: a tight `sendmmsg(2)` loop has nothing to
//! overlap, and a runtime would add its own scheduling cost. `sockets` connected sockets stand in
//! for distinct clients (distinct source ports), spread over `threads` OS threads.

use anyhow::{bail, Context};
use logit_core::template::Compiled;
use serde::Deserialize;
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// Datagrams per `sendmmsg(2)` call: under `UIO_MAXIOV` (1024), and large enough to amortize the
/// syscall so the sender is never the bottleneck.
const SEND_BATCH: usize = 64;

/// How many consecutive send attempts may fail with `ENOBUFS`/`EAGAIN` before the blast gives up.
/// Generous: an `ENOBUFS` burst clears in microseconds, so hitting this means the local send path
/// is wedged.
///
/// **This does not catch a dead target.** On a connected UDP socket an ICMP port-unreachable is a
/// pending socket error that the next `sendmmsg` reports and clears, so the send after it
/// succeeds and this counter resets. [`MAX_CONNECTION_REFUSED`] and the [`Abort`] probe catch a
/// dead target.
const MAX_CONSECUTIVE_SEND_ERRORS: u64 = 100_000;

/// How many `ECONNREFUSED`s **one blast** tolerates, across every sender thread, before concluding
/// nothing is listening.
///
/// Counted in total, not consecutively, because the pending-error semantics above make
/// "consecutive" meaningless. Between `ready` and shutdown a connected sender should see none; an
/// ICMP in flight from before the bind could produce a couple, so the threshold is low but not
/// one. A steady stream means a closed socket or an unbound port.
///
/// **Shared across the sender threads**, through one
/// [`AtomicU64`](std::sync::atomic::AtomicU64); a per-thread counter would multiply the
/// tolerance by `threads`. The check is `fetch_add`-then-test, so threads crossing the line
/// together can report up to `MAX_CONNECTION_REFUSED + threads`.
const MAX_CONNECTION_REFUSED: u64 = 64;

/// A one-shot "stop, and here's why" channel the sender polls between `sendmmsg` batches.
///
/// `crate::run` fills it when the child's stderr ends, so a child that dies mid-blast stops the
/// sender instead of a multi-second blast into a dead socket ending in a confusing accounting
/// mismatch. The errno path can't notice (see [`MAX_CONSECUTIVE_SEND_ERRORS`]).
///
/// A `OnceLock<String>` because the cause (the child exiting, or the harness failing to read its
/// stderr) is known only when it happens, and `get()` being `Some` is the abort itself: one piece
/// of shared state, not two. `get()` is a cheap atomic load, fine to poll per batch.
#[derive(Clone, Copy)]
pub struct Abort<'a> {
    pub cause: &'a std::sync::OnceLock<String>,
}

/// The two ways a blast stops early, both shared across sender threads and read per batch.
#[derive(Clone, Copy)]
struct StopConditions<'a> {
    /// The process under test went away; see [`Abort`].
    abort: Option<Abort<'a>>,
    /// `ECONNREFUSED`s so far, across every thread; see [`MAX_CONNECTION_REFUSED`].
    refused: &'a std::sync::atomic::AtomicU64,
}

impl<'a> Abort<'a> {
    /// The cause, once there is one, borrowed for `'a` from the shared `OnceLock` rather than from
    /// `self`.
    fn fired(&self) -> Option<&'a str> {
        self.cause.get().map(String::as_str)
    }
}

// ---------------------------------------------------------------------------------------------
// Spec
// ---------------------------------------------------------------------------------------------

/// One `perf/load/<scenario>.yaml`: what traffic to send, where, and how hard.
///
/// Protocol-agnostic apart from the wire syntax in the model's `lines:`, so a `syslog` or
/// `graphite` UDP scenario needs only a new scenario/spec pair.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoadSpec {
    /// The component id in the scenario config whose `bind:` is the destination, named rather than
    /// repeated as an address so the two files can't disagree about the port.
    pub target: String,
    /// The component id whose `events.received` is the run's denominator. Needed only with
    /// several sinks; a single sink is found by its `role` (`crate::attribute::delivered_at_sink`).
    #[serde(default)]
    pub sink: Option<String>,
    /// Total datagrams to send. This sets the run's length; the denominator is events delivered.
    pub datagrams: u64,
    /// Distinct connected sockets: distinct source ports, standing in for distinct clients.
    #[serde(default = "default_sockets")]
    pub sockets: usize,
    /// OS threads the sockets are spread over.
    #[serde(default = "default_threads")]
    pub threads: usize,
    /// Coarse pacing, in datagrams per second across all threads; absent means as fast as the
    /// sender can go. Applied per `sendmmsg` batch, bursty the way a client's flush interval is.
    #[serde(default)]
    pub rate: Option<u64>,
    /// Seeds the ring's weighted choices, so a scenario sends the same bytes on every run and
    /// machine.
    #[serde(default = "default_seed")]
    pub seed: u64,
    /// How many distinct datagrams to pre-render. The default is large enough that every
    /// `{seq%N}` cardinality is realised in one pass, and prime so cycling it never falls into
    /// lockstep with `sockets`, `threads`, or `SEND_BATCH`.
    #[serde(default = "default_ring_datagrams")]
    pub ring_datagrams: usize,
    /// The shared line model, relative to this spec file. A path, not an inline list, because the
    /// `udp-statsd*` scenarios differ only in `datagram_mix:` and must share one traffic model.
    pub model: PathBuf,
    /// Weighted packing targets; see [`PackingWeight`].
    pub datagram_mix: Vec<PackingWeight>,
}

fn default_sockets() -> usize {
    8
}
fn default_threads() -> usize {
    2
}
fn default_seed() -> u64 {
    20_260_918
}
fn default_ring_datagrams() -> usize {
    8_191
}

/// One entry of `datagram_mix:`: exactly one of `single: true` (one line per datagram, an
/// unbuffered client) or `max_bytes: <n>` (pack lines until the next would exceed `n`).
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackingWeight {
    pub weight: u32,
    #[serde(default)]
    pub single: bool,
    #[serde(default)]
    pub max_bytes: Option<usize>,
}

/// `perf/load/<model>.yaml`: the weighted line templates every spec referencing it shares.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LineModel {
    pub lines: Vec<LineWeight>,
}

/// One weighted line template, in `logit_core::template` syntax with `{seq%N}` placeholders (see
/// [`SeqMod`] for why a bare `{seq}` is rejected).
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LineWeight {
    pub weight: u32,
    pub template: String,
}

/// Reads and validates one load spec, so a malformed one fails here rather than mid-blast or as
/// a run that sends nothing.
pub fn read_spec(path: &Path) -> anyhow::Result<LoadSpec> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let spec: LoadSpec =
        serde_norway::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
    validate_spec(&spec).with_context(|| format!("{}", path.display()))?;
    Ok(spec)
}

fn validate_spec(spec: &LoadSpec) -> anyhow::Result<()> {
    if spec.target.is_empty() {
        bail!("`target` is empty -- it names the component whose `bind:` the load is sent to");
    }
    if spec.datagrams == 0 {
        bail!("`datagrams` is 0 -- there would be nothing to measure");
    }
    if spec.sockets == 0 {
        bail!("`sockets` is 0 -- at least one socket is needed to send anything");
    }
    if spec.threads == 0 {
        bail!("`threads` is 0 -- at least one sender thread is needed");
    }
    if spec.threads > spec.sockets {
        bail!(
            "`threads` ({}) exceeds `sockets` ({}) -- a thread with no socket of its own would \
             send nothing",
            spec.threads,
            spec.sockets
        );
    }
    if spec.ring_datagrams == 0 {
        bail!("`ring_datagrams` is 0 -- the pre-rendered ring would be empty");
    }
    if spec.rate == Some(0) {
        bail!("`rate` is 0 -- omit it entirely for an unpaced blast");
    }
    if spec.datagram_mix.is_empty() {
        bail!("`datagram_mix` is empty -- nothing says how to pack lines into datagrams");
    }
    for (index, entry) in spec.datagram_mix.iter().enumerate() {
        match (entry.single, entry.max_bytes) {
            (true, None) => {}
            (false, Some(max_bytes)) => {
                if max_bytes == 0 {
                    bail!("`datagram_mix[{index}].max_bytes` is 0");
                }
            }
            (true, Some(_)) => bail!(
                "`datagram_mix[{index}]` sets both `single: true` and `max_bytes` -- a \
                 single-line datagram has no packing ceiling to respect"
            ),
            (false, None) => bail!(
                "`datagram_mix[{index}]` sets neither `single: true` nor `max_bytes` -- one of \
                 them says how much to pack into a datagram"
            ),
        }
    }
    if spec.datagram_mix.iter().all(|entry| entry.weight == 0) {
        bail!("every `datagram_mix` weight is 0 -- nothing would ever be chosen");
    }
    Ok(())
}

/// Reads and validates the line model a spec's `model:` names, resolved against the spec file's
/// directory.
pub fn read_model(spec_path: &Path, spec: &LoadSpec) -> anyhow::Result<LineModel> {
    let dir = spec_path
        .parent()
        .with_context(|| format!("{} has no parent directory", spec_path.display()))?;
    let path = dir.join(&spec.model);
    let text =
        std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    let model: LineModel =
        serde_norway::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
    validate_model(&model).with_context(|| format!("{}", path.display()))?;
    Ok(model)
}

fn validate_model(model: &LineModel) -> anyhow::Result<()> {
    if model.lines.is_empty() {
        bail!("`lines` is empty -- there is nothing to send");
    }
    if model.lines.iter().all(|line| line.weight == 0) {
        bail!("every `lines` weight is 0 -- nothing would ever be chosen");
    }
    for (index, line) in model.lines.iter().enumerate() {
        if line.template.is_empty() {
            bail!("`lines[{index}].template` is empty");
        }
        if line.template.contains('\n') {
            bail!(
                "`lines[{index}].template` contains a newline -- one template is one statsd line, \
                 and packing is `datagram_mix`'s job"
            );
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Target resolution
// ---------------------------------------------------------------------------------------------

/// The address a spec's `target` component binds, read straight out of the scenario's YAML.
///
/// Read as a bare [`serde_norway::Value`], as `crate::scenario` reads, with no `!env`
/// resolution. The harness needs the port before spawning the child, so an `!env` `bind:` is
/// refused by name.
pub fn target_addr(scenario_yaml: &str, target: &str) -> anyhow::Result<SocketAddr> {
    let value: serde_norway::Value =
        serde_norway::from_str(scenario_yaml).context("parsing the scenario YAML")?;
    let components = value
        .get("components")
        .and_then(serde_norway::Value::as_mapping)
        .context("no top-level `components` mapping")?;
    let component = components
        .get(serde_norway::Value::from(target))
        .with_context(|| format!("no component named `{target}` (the load spec's `target`)"))?;
    let bind =
        component.get("bind").with_context(|| format!("component `{target}` has no `bind:`"))?;
    // Before `as_str`, which returns a `!env FOO` node's inner scalar: a tagged bind would
    // otherwise fail later as an "invalid socket address" `FOO`.
    if matches!(bind, serde_norway::Value::Tagged(_)) {
        bail!(
            "component `{target}`'s `bind:` carries a YAML tag (`!env`) -- this crate never \
             resolves one (`crate::scenario`'s module doc has why), and the harness has to know \
             the real port before it can send anything, so a driven scenario must write its \
             loopback address out literally"
        );
    }
    let bind = bind
        .as_str()
        .with_context(|| format!("component `{target}`'s `bind:` is not a plain string"))?;

    let addr = bind
        .to_socket_addrs()
        .with_context(|| format!("resolving `{bind}` (component `{target}`'s `bind:`)"))?
        .next()
        .with_context(|| format!("`{bind}` resolved to no address"))?;
    if addr.port() == 0 {
        bail!(
            "component `{target}` binds port 0 -- the kernel picks the port at bind time, which \
             the harness can't know in advance; a driven scenario needs a fixed port"
        );
    }
    Ok(addr)
}

// ---------------------------------------------------------------------------------------------
// Deterministic choice
// ---------------------------------------------------------------------------------------------

/// SplitMix64: a few lines, no `rand` dependency, reproducible run to run, and good enough for
/// weighted choice over a few thousand draws.
struct SplitMix64(u64);

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        SplitMix64(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A weighted index into `cumulative` (a running total, last element = the sum of all weights).
    fn weighted(&mut self, cumulative: &[u64]) -> usize {
        let total = *cumulative.last().expect("a non-empty weight list");
        let draw = self.next_u64() % total;
        cumulative.partition_point(|&bound| bound <= draw)
    }
}

/// Running totals of `weights`, for [`SplitMix64::weighted`]. Zero-weight entries stay in place,
/// so indices match the spec's list, and are never selected.
fn cumulative(weights: impl Iterator<Item = u32>) -> Vec<u64> {
    let mut running = 0u64;
    weights
        .map(|weight| {
            running += u64::from(weight);
            running
        })
        .collect()
}

// ---------------------------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------------------------

/// `{seq%N}`, the only placeholder a load template may use.
///
/// Not a bare `{seq}`: the receiver interns every statsd metric name (`StatsdDecoder`), so an
/// unbounded name would grow the interner for the whole run and measure a leak. `generate_in`
/// applies the same rule to a templated metric name
/// (`crates/logit-inputs/src/generate.rs`'s `resolve_metric_name_var`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SeqMod(u64);

fn resolve_var(name: &str) -> anyhow::Result<SeqMod> {
    if let Some(modulus) = name.strip_prefix("seq%") {
        if !modulus.is_empty() && modulus.bytes().all(|byte| byte.is_ascii_digit()) {
            if let Ok(modulus) = modulus.parse::<u64>() {
                if modulus >= 1 {
                    return Ok(SeqMod(modulus));
                }
            }
        }
    }
    bail!(
        "'{{{name}}}' is not a placeholder a load template substitutes -- only '{{seq%N}}' \
         (N written as digits, at least 1). A bare '{{seq}}' is rejected on purpose: the receiver \
         interns every distinct metric name, so an unbounded one measures a leak rather than a \
         workload"
    )
}

/// One compiled line template, with its own sequence counter.
///
/// **Per-template, not global.** Every `{seq%N}` in one template advances together, so two
/// placeholders in a line co-vary. That's a named simplification: the receiver's cost depends on
/// distinct metric names (interner), distinct tag keys (its `KeyCache`), and line length, not on
/// whether two tag values co-vary. Per-template counters keep templates chosen at different
/// weights from sharing a phase.
#[derive(Debug)]
struct LineRenderer {
    compiled: Compiled<SeqMod>,
    seq: u64,
}

impl LineRenderer {
    fn compile(template: &str) -> anyhow::Result<Self> {
        let parsed = logit_core::template::parse(template)
            .with_context(|| format!("parsing template {template:?}"))?;
        let compiled = parsed
            .compile(resolve_var)
            .with_context(|| format!("compiling template {template:?}"))?;
        Ok(LineRenderer { compiled, seq: 0 })
    }

    fn render(&mut self, out: &mut String) {
        let seq = self.seq;
        self.seq = self.seq.wrapping_add(1);
        self.compiled.render(out, |var, out| {
            use std::fmt::Write;
            let _ = write!(out, "{}", seq % var.0);
        });
    }
}

/// How many `Event`s one rendered statsd line decodes to.
///
/// Usually 1, but `StatsdDecoder` emits one event per `:`-separated value for a counter or gauge
/// (`a:1:2:3|c` is three), while `ms`/`h`/`d`/`s` keep every value in one record. `--verify`
/// asserts an exact delivered-event count, so this must be exact.
fn events_in_line(line: &str) -> u64 {
    let Some((_, rest)) = line.split_once(':') else { return 1 };
    let mut segments = rest.split('|');
    let values = segments.next().unwrap_or("");
    match segments.next() {
        Some("c") | Some("g") => values.split(':').count() as u64,
        _ => 1,
    }
}

// ---------------------------------------------------------------------------------------------
// The ring
// ---------------------------------------------------------------------------------------------

/// Every datagram the blast will send, rendered once up front and then cycled.
///
/// Pre-rendering keeps the sender cheap: at send time each datagram is a slice handed to
/// `sendmmsg`, with no formatting, allocation, or weighted choice left.
#[derive(Debug)]
pub struct Ring {
    datagrams: Vec<Vec<u8>>,
    /// Running totals, `prefix_*[i]` covering `datagrams[..i]`; `[len]` is the whole ring. They
    /// make a count over any stretch of the cycled ring O(1), and differenced give one datagram's
    /// counts.
    prefix_lines: Vec<u64>,
    prefix_events: Vec<u64>,
    prefix_bytes: Vec<u64>,
}

impl Ring {
    /// Renders `spec.ring_datagrams` datagrams from `model`, deterministically from `spec.seed`.
    pub fn render(spec: &LoadSpec, model: &LineModel) -> anyhow::Result<Ring> {
        let mut renderers: Vec<LineRenderer> = model
            .lines
            .iter()
            .map(|line| LineRenderer::compile(&line.template))
            .collect::<anyhow::Result<_>>()?;
        let line_weights = cumulative(model.lines.iter().map(|line| line.weight));
        let packing_weights = cumulative(spec.datagram_mix.iter().map(|entry| entry.weight));

        let mut rng = SplitMix64::new(spec.seed);
        let mut datagrams = Vec::with_capacity(spec.ring_datagrams);
        let mut lines = Vec::with_capacity(spec.ring_datagrams);
        let mut events = Vec::with_capacity(spec.ring_datagrams);
        let mut scratch = String::new();

        for index in 0..spec.ring_datagrams {
            let packing = &spec.datagram_mix[rng.weighted(&packing_weights)];
            let mut buffer: Vec<u8> = Vec::new();
            let mut line_count = 0u64;
            let mut event_count = 0u64;

            loop {
                scratch.clear();
                renderers[rng.weighted(&line_weights)].render(&mut scratch);
                // The `\n` between this line and the previous one, counted in the fit check or
                // the datagram could exceed the ceiling on the wire.
                let separator = usize::from(!buffer.is_empty());
                if let Some(max_bytes) = packing.max_bytes {
                    if !buffer.is_empty() && buffer.len() + separator + scratch.len() > max_bytes {
                        break;
                    }
                    if buffer.is_empty() && scratch.len() > max_bytes {
                        bail!(
                            "datagram {index}: a single rendered line is {} bytes, which exceeds \
                             this `datagram_mix` entry's `max_bytes` ({max_bytes}) on its own -- \
                             raise the ceiling or shorten the template. The line was: {scratch:?}",
                            scratch.len()
                        );
                    }
                }
                if separator == 1 {
                    buffer.push(b'\n');
                }
                buffer.extend_from_slice(scratch.as_bytes());
                line_count += 1;
                event_count += events_in_line(&scratch);
                if packing.single {
                    break;
                }
            }

            datagrams.push(buffer);
            lines.push(line_count);
            events.push(event_count);
        }

        let prefix_lines = prefix_sums(&lines);
        let prefix_events = prefix_sums(&events);
        let prefix_bytes =
            prefix_sums(&datagrams.iter().map(|d| d.len() as u64).collect::<Vec<_>>());
        Ok(Ring { datagrams, prefix_lines, prefix_events, prefix_bytes })
    }

    pub fn len(&self) -> usize {
        self.datagrams.len()
    }

    pub fn datagram(&self, index: u64) -> &[u8] {
        &self.datagrams[(index % self.len() as u64) as usize]
    }

    /// Lines / events / bytes across `count` datagrams starting at ring position `start`, with the
    /// ring cycled as many times as it takes.
    pub fn window(&self, start: u64, count: u64) -> Window {
        Window {
            lines: window_sum(&self.prefix_lines, self.len(), start, count),
            events: window_sum(&self.prefix_events, self.len(), start, count),
            bytes: window_sum(&self.prefix_bytes, self.len(), start, count),
        }
    }

    /// Lines in ring datagram `index` (not cycled -- `index < len()`).
    pub fn lines_at(&self, index: usize) -> u64 {
        self.prefix_lines[index + 1] - self.prefix_lines[index]
    }

    /// Events in ring datagram `index`; not always its line count, since a multi-value counter or
    /// gauge line decodes to one event per value. Test-only: a run uses [`Ring::window`].
    #[cfg(test)]
    pub fn events_at(&self, index: usize) -> u64 {
        self.prefix_events[index + 1] - self.prefix_events[index]
    }

    /// The rendered ring's size and packing distribution, which `run` prints before a driven
    /// scenario: what the weights, ceilings, and template lengths produced, not what was asked.
    pub fn shape(&self) -> RingShape {
        let mut sizes: Vec<usize> = self.datagrams.iter().map(Vec::len).collect();
        let mut lines: Vec<u64> = (0..self.len()).map(|index| self.lines_at(index)).collect();
        sizes.sort_unstable();
        lines.sort_unstable();
        RingShape {
            min_bytes: *sizes.first().unwrap_or(&0),
            median_bytes: *sizes.get(sizes.len() / 2).unwrap_or(&0),
            max_bytes: *sizes.last().unwrap_or(&0),
            min_lines: *lines.first().unwrap_or(&0),
            median_lines: *lines.get(lines.len() / 2).unwrap_or(&0),
            max_lines: *lines.last().unwrap_or(&0),
        }
    }
}

/// The rendered ring's size and packing distribution; see [`Ring::shape`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RingShape {
    pub min_bytes: usize,
    pub median_bytes: usize,
    pub max_bytes: usize,
    pub min_lines: u64,
    pub median_lines: u64,
    pub max_lines: u64,
}

impl std::fmt::Display for RingShape {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}-{} B (median {}), {}-{} lines each (median {})",
            self.min_bytes,
            self.max_bytes,
            self.median_bytes,
            self.min_lines,
            self.max_lines,
            self.median_lines
        )
    }
}

/// Lines, events and bytes over some stretch of the ring.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Window {
    pub lines: u64,
    pub events: u64,
    pub bytes: u64,
}

fn prefix_sums(values: &[u64]) -> Vec<u64> {
    let mut out = Vec::with_capacity(values.len() + 1);
    let mut running = 0u64;
    out.push(0);
    for value in values {
        running += value;
        out.push(running);
    }
    out
}

/// Σ over `count` elements of an endlessly repeated array, starting at index `start`, from its
/// prefix sums. Whole cycles are one multiply; the two partial ends are two lookups each.
fn window_sum(prefix: &[u64], len: usize, start: u64, count: u64) -> u64 {
    if len == 0 || count == 0 {
        return 0;
    }
    let len_u64 = len as u64;
    let whole = prefix[len];
    let start = start % len_u64;
    let cycles = count / len_u64;
    let remainder = count % len_u64;
    let mut total = cycles * whole;
    let end = start + remainder;
    total += if end <= len_u64 {
        prefix[end as usize] - prefix[start as usize]
    } else {
        (whole - prefix[start as usize]) + prefix[(end - len_u64) as usize]
    };
    total
}

// ---------------------------------------------------------------------------------------------
// The plan
// ---------------------------------------------------------------------------------------------

/// A spec, its rendered ring, and the target address. Built once per scenario and reused across
/// every `--repeat`, since the ring is the expensive part and never changes.
pub struct LoadPlan {
    pub spec: LoadSpec,
    pub spec_path: PathBuf,
    pub ring: Ring,
    pub target: SocketAddr,
}

/// The `--rate-scale` a `--verify` run uses when the caller didn't pick one.
///
/// A shipped spec is paced a few percent above what the receiver sustains, so the baseline has a
/// small drop rate to improve on; `--verify` forbids drops. Scaling down lets it run against the
/// shipped specs unedited.
///
/// **A quarter, measured:** at half rate `udp-statsd` and `-packed` verified cleanly, but
/// `udp-statsd-small` still lost 21 of 5,000,000 datagrams. That was at ~53% of capacity, not
/// sustained overload: one momentary receive-side stall that a 2 MiB kernel buffer couldn't absorb
/// at 380,000 datagrams/s. A quarter leaves room for such a stall, at four times a measured run's
/// length.
pub const VERIFY_RATE_SCALE: f64 = 0.25;

impl LoadPlan {
    /// Reads spec + model, renders the ring, and resolves the target address out of the scenario
    /// config `scenario_yaml`.
    pub fn build(spec_path: &Path, scenario_yaml: &str) -> anyhow::Result<LoadPlan> {
        let spec = read_spec(spec_path)?;
        let model = read_model(spec_path, &spec)?;
        let ring = Ring::render(&spec, &model)
            .with_context(|| format!("rendering the datagram ring for {}", spec_path.display()))?;
        let target = target_addr(scenario_yaml, &spec.target)?;
        Ok(LoadPlan { spec, spec_path: spec_path.to_path_buf(), ring, target })
    }

    /// What a complete blast puts on the wire; `--verify` holds the delivered event count to it.
    pub fn expected(&self) -> Window {
        self.ring.window(0, self.spec.datagrams)
    }

    /// Multiplies this plan's `rate` by `scale`, returning the rate the blast will run at.
    ///
    /// Reads a scenario at a chosen operating point without editing its spec: shipped rates sit
    /// slightly above the drop knee, right for a baseline and wrong for a stable CPU µs/event.
    /// `--verify` is this knob at [`VERIFY_RATE_SCALE`] plus an exactness assertion.
    ///
    /// Fails on a spec with no `rate`: an unpaced blast saturates the receiver regardless.
    pub fn scale_rate(&mut self, scale: f64) -> anyhow::Result<u64> {
        if !(scale.is_finite() && scale > 0.0) {
            bail!("--rate-scale must be a finite number greater than 0, got {scale}");
        }
        let rate = self.spec.rate.with_context(|| {
            format!(
                "{} has no `rate:`, so it sends as fast as the sender can go -- there is nothing \
                 to scale, and an unpaced blast saturates the receiver by construction. Add a \
                 `rate:` (see perf/load/README.md's \"Tuning\") or drop the flag",
                self.spec_path.display()
            )
        })?;
        // At least 1: a rate rounded to zero would send nothing.
        let scaled = ((rate as f64 * scale).round() as u64).max(1);
        self.spec.rate = Some(scaled);
        Ok(scaled)
    }
}

// ---------------------------------------------------------------------------------------------
// CPU pinning
// ---------------------------------------------------------------------------------------------

/// A parsed `--pin-sender`/`--pin-child` cpu list (`3`, `2,4`, `2-5`, or any comma-separated mix).
///
/// Pin the sender and child to disjoint CPUs so they don't contend for one core's time. On a box
/// with heterogeneous cores, an unpinned run also lands on either kind by scheduler luck, making
/// every number bimodal by roughly 2× (`perf/load/README.md`'s "Pinning").
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CpuSet {
    cpus: Vec<usize>,
}

/// One CPU number from a `--pin-*` list, range-checked at the point it is read.
///
/// `whole` is the comma-separated entry it came from, so a bad range end reports the range. The
/// `CPU_SETSIZE` check is here so it runs before any range is expanded (see [`CpuSet::parse`]).
fn parse_cpu(text: &str, whole: &str) -> anyhow::Result<usize> {
    let cpu: usize =
        text.parse().with_context(|| format!("`{whole}`: `{text}` is not a CPU number"))?;
    if cpu >= libc::CPU_SETSIZE as usize {
        bail!(
            "`{whole}`: CPU {cpu} is beyond CPU_SETSIZE ({}) -- this box has {} of them",
            libc::CPU_SETSIZE,
            std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0)
        );
    }
    Ok(cpu)
}

impl CpuSet {
    pub fn parse(list: &str) -> anyhow::Result<CpuSet> {
        let mut cpus = Vec::new();
        for part in list.split(',') {
            let part = part.trim();
            if part.is_empty() {
                bail!("`{list}` has an empty entry -- expected e.g. `2`, `2,4` or `2-5`");
            }
            match part.split_once('-') {
                Some((lo, hi)) => {
                    let lo = parse_cpu(lo.trim(), part)?;
                    let hi = parse_cpu(hi.trim(), part)?;
                    if hi < lo {
                        bail!("`{part}` runs backwards");
                    }
                    // Both ends were checked before expanding: `0-99999999999` would otherwise
                    // allocate a hundred billion `usize`s, turning a typo into an OOM.
                    cpus.extend(lo..=hi);
                }
                None => cpus.push(parse_cpu(part, part)?),
            }
        }
        cpus.sort_unstable();
        cpus.dedup();
        if cpus.is_empty() {
            bail!("`{list}` names no CPUs");
        }
        // A backstop: `parse_cpu` already rejected anything out of range. Kept so no other path
        // into `cpus` can produce a set `CPU_SET` would index out of bounds.
        if let Some(&highest) = cpus.last() {
            if highest >= libc::CPU_SETSIZE as usize {
                bail!("CPU {highest} is beyond CPU_SETSIZE ({})", libc::CPU_SETSIZE);
            }
        }
        Ok(CpuSet { cpus })
    }

    /// The `cpu_set_t` this list describes, as a plain value so the `pre_exec` path
    /// (`crate::run`) has nothing to allocate or parse between `fork` and `exec`.
    pub fn to_raw(&self) -> libc::cpu_set_t {
        // SAFETY: `cpu_set_t` is a plain bitmask struct with no padding invariants and no pointers;
        // all-zeroes is its documented "empty set" state, which `CPU_SET` below then fills in.
        let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
        for &cpu in &self.cpus {
            // SAFETY: `set` is a live, uniquely-borrowed `cpu_set_t`, and `cpu` was checked against
            // `CPU_SETSIZE` by `parse`, which is the only constructor.
            unsafe { libc::CPU_SET(cpu, &mut set) };
        }
        set
    }

    /// Pins the *calling thread* to this set.
    pub fn apply_to_current_thread(&self) -> std::io::Result<()> {
        let set = self.to_raw();
        // SAFETY: pid 0 means "the calling thread"; `set` is a live, correctly-sized `cpu_set_t`
        // owned by this frame for the duration of the call, which is exactly what
        // `sched_setaffinity(2)` requires of its mask argument.
        let rc = unsafe {
            libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set as *const _)
        };
        if rc != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    /// The CPUs, ascending and deduplicated. `Display` renders these, so output states the set
    /// applied rather than the string typed.
    pub fn cpus(&self) -> &[usize] {
        &self.cpus
    }
}

impl std::fmt::Display for CpuSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let rendered: Vec<String> = self.cpus().iter().map(usize::to_string).collect();
        f.write_str(&rendered.join(","))
    }
}

// ---------------------------------------------------------------------------------------------
// The blast
// ---------------------------------------------------------------------------------------------

/// What one blast put on the wire. What was received comes from the child's telemetry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LoadOutcome {
    pub sent_datagrams: u64,
    pub sent_lines: u64,
    pub sent_bytes: u64,
    /// Retryable send failures (`ENOBUFS`, `ECONNREFUSED` from an earlier ICMP port-unreachable).
    /// Each one was retried; a datagram is counted in `sent_datagrams` only once it was accepted.
    pub send_errors: u64,
    pub elapsed: Duration,
}

/// Sends `plan.spec.datagrams` datagrams at `plan.target` and returns what happened.
///
/// Blocking: the caller has already waited for `ready`, and a driven scenario's `wall` is `ready`
/// to this returning. `abort` is polled between batches (see [`Abort`]).
pub fn blast(
    plan: &LoadPlan,
    pin: Option<&CpuSet>,
    abort: Option<Abort<'_>>,
) -> anyhow::Result<LoadOutcome> {
    let spec = &plan.spec;
    let sockets = open_sockets(plan.target, spec.sockets)?;

    // Contiguous blocks, not interleaved, so each thread's traffic is one `Ring::window` range.
    let per_thread = spec.datagrams / spec.threads as u64;
    let remainder = spec.datagrams % spec.threads as u64;

    // One counter shared by every sender thread (`MAX_CONNECTION_REFUSED` has why).
    let refused = std::sync::atomic::AtomicU64::new(0);
    let stop = StopConditions { abort, refused: &refused };

    let started = Instant::now();
    let outcomes: Vec<anyhow::Result<LoadOutcome>> = std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(spec.threads);
        let mut next_start = 0u64;
        for thread_index in 0..spec.threads {
            let count = per_thread + u64::from((thread_index as u64) < remainder);
            let start = next_start;
            next_start += count;
            // Each socket belongs to one thread (`sockets >= threads` is validated); a shared
            // socket would serialize two senders on one fd.
            let mine: Vec<&UdpSocket> =
                sockets.iter().skip(thread_index).step_by(spec.threads).collect();
            let rate = spec.rate.map(|rate| rate as f64 / spec.threads as f64);
            handles.push(scope.spawn(move || {
                if let Some(pin) = pin {
                    pin.apply_to_current_thread().with_context(|| {
                        format!("pinning sender thread {thread_index} to CPUs {pin}")
                    })?;
                }
                send_block(&plan.ring, &mine, start, count, rate, plan.target, stop)
            }));
        }
        handles.into_iter().map(|handle| handle.join().expect("sender thread panicked")).collect()
    });

    let mut total = LoadOutcome { elapsed: started.elapsed(), ..LoadOutcome::default() };
    for outcome in outcomes {
        let outcome = outcome?;
        total.sent_datagrams += outcome.sent_datagrams;
        total.sent_lines += outcome.sent_lines;
        total.sent_bytes += outcome.sent_bytes;
        total.send_errors += outcome.send_errors;
    }
    Ok(total)
}

/// `count` distinct sockets, each `connect`ed to `target`.
///
/// Connected, not only bound: `sendmmsg` then needs no per-datagram destination, and a target
/// that isn't listening becomes reportable, since a connected socket surfaces the ICMP
/// port-unreachable as `ECONNREFUSED` on a later send.
fn open_sockets(target: SocketAddr, count: usize) -> anyhow::Result<Vec<UdpSocket>> {
    let local = if target.is_ipv4() { "0.0.0.0:0" } else { "[::]:0" };
    (0..count)
        .map(|index| {
            let socket = UdpSocket::bind(local)
                .with_context(|| format!("binding sender socket {index} on {local}"))?;
            socket
                .connect(target)
                .with_context(|| format!("connecting sender socket {index} to {target}"))?;
            Ok(socket)
        })
        .collect()
}

/// One sender thread: datagrams `start..start + count` of the (cycled) ring, round-robining
/// `sendmmsg` batches across its own sockets.
fn send_block(
    ring: &Ring,
    sockets: &[&UdpSocket],
    start: u64,
    count: u64,
    rate: Option<f64>,
    target: SocketAddr,
    stop: StopConditions<'_>,
) -> anyhow::Result<LoadOutcome> {
    let mut outcome = LoadOutcome::default();
    if count == 0 {
        return Ok(outcome);
    }
    // SAFETY: both are plain C aggregates of integers and pointers with no validity invariants
    // beyond "some bit pattern"; every field that `sendmmsg` reads is overwritten below before the
    // call, and an all-zero `msghdr` is the documented starting point for one.
    let mut iovs: Vec<libc::iovec> = vec![unsafe { std::mem::zeroed() }; SEND_BATCH];
    let mut msgs: Vec<libc::mmsghdr> = vec![unsafe { std::mem::zeroed() }; SEND_BATCH];

    let started = Instant::now();
    let mut done = 0u64;
    let mut socket_cursor = 0usize;

    while done < count {
        // One atomic load per batch, negligible next to the syscall.
        if let Some(cause) = stop.abort.and_then(|abort| abort.fired()) {
            bail!(
                "stopped after {} of {count} datagrams to {target}: {cause}",
                outcome.sent_datagrams,
            );
        }
        let batch = SEND_BATCH.min((count - done) as usize);
        for slot in 0..batch {
            let payload = ring.datagram(start + done + slot as u64);
            iovs[slot] = libc::iovec {
                iov_base: payload.as_ptr() as *mut libc::c_void,
                iov_len: payload.len(),
            };
            let header = &mut msgs[slot].msg_hdr;
            header.msg_name = std::ptr::null_mut();
            header.msg_namelen = 0;
            // The `iovec` this header points at lives in `iovs`, which outlives every `sendmmsg`
            // call below and is never resized -- the two vectors are allocated once per thread and
            // only overwritten in place.
            header.msg_iov = std::ptr::addr_of_mut!(iovs[slot]);
            header.msg_iovlen = 1;
            header.msg_control = std::ptr::null_mut();
            header.msg_controllen = 0;
            header.msg_flags = 0;
            msgs[slot].msg_len = 0;
        }

        let socket = sockets[socket_cursor % sockets.len()];
        socket_cursor += 1;
        let fd = socket.as_raw_fd();

        let mut offset = 0usize;
        let mut consecutive_errors = 0u64;
        while offset < batch {
            // SAFETY: `fd` is this thread's own live, connected UDP socket (borrowed from
            // `sockets`, which outlives this call). `msgs[offset..batch]` is an initialized,
            // uniquely-borrowed run of `mmsghdr`s filled in immediately above, each pointing at an
            // `iovec` in `iovs` that is alive for the whole call and at ring bytes that are
            // immutable and outlive the blast. That is exactly `sendmmsg(2)`'s contract for
            // (sockfd, msgvec, vlen, flags).
            let sent = unsafe {
                libc::sendmmsg(
                    fd,
                    msgs.as_mut_ptr().add(offset),
                    (batch - offset) as libc::c_uint,
                    0,
                )
            };
            if sent > 0 {
                // A short return is ordinary: resubmit the rest.
                let sent = sent as usize;
                for iov in iovs.iter().skip(offset).take(sent) {
                    outcome.sent_bytes += iov.iov_len as u64;
                }
                offset += sent;
                consecutive_errors = 0;
                continue;
            }
            let err = std::io::Error::last_os_error();
            match err.raw_os_error() {
                // Interrupted before anything was sent; retry.
                Some(libc::EINTR) => continue,
                // ENOBUFS: the local send path is momentarily full. EAGAIN shouldn't happen on a
                // blocking socket but may. Both clear on their own, so only a long unbroken run
                // is fatal.
                Some(libc::ENOBUFS) | Some(libc::EAGAIN) => {
                    outcome.send_errors += 1;
                    consecutive_errors += 1;
                    if consecutive_errors > MAX_CONSECUTIVE_SEND_ERRORS {
                        return Err(anyhow::Error::new(err)).with_context(|| {
                            format!(
                                "sendmmsg to {target} failed {consecutive_errors} times in a row \
                                 with no datagram accepted -- the local send path is wedged"
                            )
                        });
                    }
                    std::thread::yield_now();
                }
                // ECONNREFUSED: an earlier datagram's ICMP port-unreachable, a pending error this
                // call reports and clears, so it's counted in total across the blast rather than
                // consecutively (`MAX_CONNECTION_REFUSED`).
                Some(libc::ECONNREFUSED) => {
                    outcome.send_errors += 1;
                    let total = stop.refused.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                    if total > MAX_CONNECTION_REFUSED {
                        return Err(anyhow::Error::new(err)).with_context(|| {
                            format!(
                                "nothing is listening on {target}: {total} ICMP \
                                 port-unreachable errors across all sender threads. A connected \
                                 UDP socket reports these one send late, so the count is the \
                                 signal, not any single failure"
                            )
                        });
                    }
                    // No yield: this call cleared the error, so the resubmit should go through.
                }
                _ => {
                    return Err(anyhow::Error::new(err))
                        .with_context(|| format!("sendmmsg to {target}"))
                }
            }
        }

        done += batch as u64;
        outcome.sent_datagrams += batch as u64;

        if let Some(rate) = rate {
            // Paced against the thread's own start, not the previous batch, so a thread that
            // falls behind catches up rather than drifting.
            let target_elapsed = Duration::from_secs_f64(done as f64 / rate);
            let elapsed = started.elapsed();
            if target_elapsed > elapsed {
                std::thread::sleep(target_elapsed - elapsed);
            }
        }
    }

    let window = ring.window(start, count);
    outcome.sent_lines = window.lines;
    outcome.elapsed = started.elapsed();
    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn model(templates: &[(u32, &str)]) -> LineModel {
        LineModel {
            lines: templates
                .iter()
                .map(|(weight, template)| LineWeight {
                    weight: *weight,
                    template: (*template).to_string(),
                })
                .collect(),
        }
    }

    fn spec(mix: Vec<PackingWeight>) -> LoadSpec {
        LoadSpec {
            target: "statsd".to_string(),
            sink: None,
            datagrams: 1_000,
            sockets: 2,
            threads: 1,
            rate: None,
            seed: 7,
            ring_datagrams: 256,
            model: PathBuf::from("m.yaml"),
            datagram_mix: mix,
        }
    }

    fn single() -> PackingWeight {
        PackingWeight { weight: 1, single: true, max_bytes: None }
    }

    fn packed(max_bytes: usize) -> PackingWeight {
        PackingWeight { weight: 1, single: false, max_bytes: Some(max_bytes) }
    }

    #[test]
    fn a_spec_parses_with_its_defaults() {
        let parsed: LoadSpec = serde_norway::from_str(
            "target: statsd\ndatagrams: 100\nmodel: m.yaml\ndatagram_mix:\n  - { weight: 1, single: true }\n",
        )
        .unwrap();
        assert_eq!(parsed.target, "statsd");
        assert_eq!(parsed.datagrams, 100);
        assert_eq!(parsed.sockets, default_sockets());
        assert_eq!(parsed.threads, default_threads());
        assert_eq!(parsed.rate, None);
        assert_eq!(parsed.ring_datagrams, default_ring_datagrams());
        validate_spec(&parsed).unwrap();
    }

    #[test]
    fn an_unknown_spec_field_is_rejected_rather_than_ignored() {
        let err = serde_norway::from_str::<LoadSpec>(
            "target: s\ndatagrams: 1\nmodel: m.yaml\ndatagram_mix: []\nrat: 5\n",
        )
        .expect_err("a typo'd field must not be silently dropped");
        assert!(format!("{err}").contains("rat"), "{err}");
    }

    #[test]
    fn a_packing_entry_must_pick_exactly_one_of_single_and_max_bytes() {
        let both = spec(vec![PackingWeight { weight: 1, single: true, max_bytes: Some(1432) }]);
        assert!(format!("{:#}", validate_spec(&both).unwrap_err()).contains("both"));

        let neither = spec(vec![PackingWeight { weight: 1, single: false, max_bytes: None }]);
        assert!(format!("{:#}", validate_spec(&neither).unwrap_err()).contains("neither"));
    }

    #[test]
    fn more_threads_than_sockets_is_rejected() {
        let mut spec = spec(vec![single()]);
        spec.threads = 4;
        spec.sockets = 2;
        assert!(format!("{:#}", validate_spec(&spec).unwrap_err()).contains("exceeds"));
    }

    #[test]
    fn a_model_template_may_not_carry_a_newline() {
        let err = validate_model(&model(&[(1, "a:1|c\nb:2|c")])).unwrap_err();
        assert!(format!("{err:#}").contains("newline"), "{err:#}");
    }

    #[test]
    fn a_bare_seq_placeholder_is_rejected_but_a_bounded_one_compiles() {
        let err = LineRenderer::compile("m.{seq}:1|c").unwrap_err();
        assert!(format!("{err:#}").contains("{seq%N}"), "{err:#}");
        LineRenderer::compile("m.{seq%10}:1|c")
            .expect("a bounded placeholder is the supported one");
    }

    #[test]
    fn a_renderer_cycles_each_placeholder_through_its_own_modulus() {
        let mut renderer = LineRenderer::compile("a.{seq%3}.b.{seq%2}:1|c").unwrap();
        let mut rendered = Vec::new();
        for _ in 0..6 {
            let mut out = String::new();
            renderer.render(&mut out);
            rendered.push(out);
        }
        assert_eq!(
            rendered,
            vec![
                "a.0.b.0:1|c",
                "a.1.b.1:1|c",
                "a.2.b.0:1|c",
                "a.0.b.1:1|c",
                "a.1.b.0:1|c",
                "a.2.b.1:1|c",
            ]
        );
    }

    #[test]
    fn events_per_line_counts_multi_value_counters_and_gauges_only() {
        assert_eq!(events_in_line("a:1|c"), 1);
        assert_eq!(events_in_line("a:1:2:3|c"), 3);
        assert_eq!(events_in_line("a:1:2|g"), 2);
        // ms/h/d/s put every value on the line into one record, so the line is one event.
        assert_eq!(events_in_line("a:1:2:3|ms"), 1);
        assert_eq!(events_in_line("a:1:2:3|h|@0.5"), 1);
        assert_eq!(events_in_line("a:1:2:3|d"), 1);
        assert_eq!(events_in_line("a:x:y|s"), 1);
        assert_eq!(events_in_line("a:1|c|@0.1|#k:v"), 1);
    }

    #[test]
    fn a_single_packing_target_renders_exactly_one_line_per_datagram() {
        let spec = spec(vec![single()]);
        let ring =
            Ring::render(&spec, &model(&[(1, "a.{seq%10}:1|c"), (1, "b.{seq%7}:2|g")])).unwrap();
        assert_eq!(ring.len(), spec.ring_datagrams);
        assert!((0..ring.len()).all(|index| ring.lines_at(index) == 1));
        assert!((0..ring.len() as u64).all(|index| !ring.datagram(index).is_empty()));
        for datagram in 0..ring.len() as u64 {
            assert!(
                !ring.datagram(datagram).contains(&b'\n'),
                "a single-line datagram carries no separator"
            );
        }
    }

    #[test]
    fn a_packed_datagram_never_exceeds_its_size_target() {
        let spec = spec(vec![packed(200), packed(1432)]);
        let ring = Ring::render(
            &spec,
            &model(&[
                (3, "svc.app.http.requests.count.{seq%100}:1|c|#env:prod,host:web-{seq%50}"),
                (1, "svc.app.http.latency.{seq%40}:12.5|ms|@0.5"),
            ]),
        )
        .unwrap();
        for index in 0..ring.len() as u64 {
            let size = ring.datagram(index).len();
            assert!(size <= 1432, "{size} exceeds the largest configured ceiling");
        }
        // At these sizes a packed datagram holds at least two lines, so the loop really packs.
        assert!((0..ring.len()).any(|index| ring.lines_at(index) > 1));
        assert_eq!(ring.shape().max_bytes, ring.shape().max_bytes.min(1432));
    }

    #[test]
    fn a_line_longer_than_its_packing_ceiling_is_a_spec_error() {
        let spec = spec(vec![packed(8)]);
        let err = Ring::render(&spec, &model(&[(1, "a.very.long.metric.name:1|c")])).unwrap_err();
        assert!(format!("{err:#}").contains("exceeds"), "{err:#}");
    }

    #[test]
    fn the_ring_is_deterministic_from_its_seed() {
        let templates = model(&[(2, "a.{seq%10}:1|c"), (1, "b.{seq%7}:2|g")]);
        let spec = spec(vec![single(), packed(512)]);
        let first = Ring::render(&spec, &templates).unwrap();
        let second = Ring::render(&spec, &templates).unwrap();
        for index in 0..first.len() as u64 {
            assert_eq!(first.datagram(index), second.datagram(index));
        }

        let mut other_seed = spec.clone();
        other_seed.seed += 1;
        let different = Ring::render(&other_seed, &templates).unwrap();
        assert!(
            (0..first.len() as u64).any(|i| first.datagram(i) != different.datagram(i)),
            "a different seed must produce different traffic"
        );
    }

    #[test]
    fn the_ring_realises_the_cardinality_its_templates_ask_for() {
        let spec = spec(vec![single()]);
        let ring = Ring::render(&spec, &model(&[(1, "app.endpoint.{seq%37}:1|c")])).unwrap();
        let mut seen = std::collections::BTreeSet::new();
        for index in 0..ring.len() as u64 {
            let text = String::from_utf8(ring.datagram(index).to_vec()).unwrap();
            let name = text.split(':').next().unwrap().to_string();
            seen.insert(name);
        }
        assert_eq!(seen.len(), 37, "every one of the 37 distinct names should appear");
    }

    #[test]
    fn the_weighted_mixes_are_honoured_within_tolerance() {
        let mut spec = spec(vec![
            PackingWeight { weight: 80, single: true, max_bytes: None },
            PackingWeight { weight: 20, single: false, max_bytes: Some(1432) },
        ]);
        spec.ring_datagrams = 20_000;
        let ring = Ring::render(&spec, &model(&[(1, "a.{seq%10}:1|c")])).unwrap();
        let singles =
            (0..ring.len()).filter(|&index| ring.lines_at(index) == 1).count() as f64 / 20_000.0;
        assert!((singles - 0.8).abs() < 0.02, "single share was {singles}");
    }

    #[test]
    fn a_line_weight_of_zero_is_never_selected() {
        let spec = spec(vec![single()]);
        let ring =
            Ring::render(&spec, &model(&[(1, "kept.{seq%3}:1|c"), (0, "never.sent:1|c")])).unwrap();
        for index in 0..ring.len() as u64 {
            assert!(!ring.datagram(index).starts_with(b"never"));
        }
    }

    #[test]
    fn a_window_over_the_cycled_ring_matches_a_naive_sum() {
        let spec = spec(vec![single(), packed(300)]);
        let ring =
            Ring::render(&spec, &model(&[(2, "a.{seq%9}:1|c"), (1, "b.{seq%5}:1:2|g")])).unwrap();
        for &(start, count) in &[(0u64, 10u64), (250, 20), (7, 1_000), (0, 0), (300, 512)] {
            let naive: u64 = (0..count)
                .map(|i| ring.events_at(((start + i) % ring.len() as u64) as usize))
                .sum();
            assert_eq!(ring.window(start, count).events, naive, "start={start} count={count}");
        }
    }

    #[test]
    fn expected_totals_cover_the_whole_configured_blast() {
        let mut spec = spec(vec![single()]);
        spec.datagrams = 1_000;
        spec.ring_datagrams = 97;
        let ring = Ring::render(&spec, &model(&[(1, "a.{seq%4}:1|c")])).unwrap();
        let plan = LoadPlan {
            spec: spec.clone(),
            spec_path: PathBuf::from("/nowhere/x.yaml"),
            ring,
            target: "127.0.0.1:1".parse().unwrap(),
        };
        // One line, one event, per datagram in this model.
        assert_eq!(plan.expected().lines, 1_000);
        assert_eq!(plan.expected().events, 1_000);
    }

    const SCENARIO: &str = "components:\n  statsd:\n    type: statsd_in\n    bind: 127.0.0.1:18125\n  out:\n    type: null_out\n    sources: [statsd]\n";

    #[test]
    fn target_addr_reads_the_named_components_bind() {
        assert_eq!(
            target_addr(SCENARIO, "statsd").unwrap(),
            "127.0.0.1:18125".parse::<SocketAddr>().unwrap()
        );
    }

    #[test]
    fn target_addr_rejects_an_unknown_component() {
        let err = target_addr(SCENARIO, "nope").unwrap_err();
        assert!(format!("{err:#}").contains("no component named `nope`"), "{err:#}");
    }

    #[test]
    fn target_addr_rejects_a_component_with_no_bind() {
        let err = target_addr(SCENARIO, "out").unwrap_err();
        assert!(format!("{err:#}").contains("has no `bind:`"), "{err:#}");
    }

    #[test]
    fn target_addr_rejects_an_env_tagged_bind() {
        let yaml = "components:\n  statsd:\n    type: statsd_in\n    bind: !env STATSD_BIND\n";
        let err = target_addr(yaml, "statsd").unwrap_err();
        assert!(format!("{err:#}").contains("!env"), "{err:#}");
    }

    #[test]
    fn target_addr_rejects_port_zero() {
        let yaml = "components:\n  statsd:\n    type: statsd_in\n    bind: 127.0.0.1:0\n";
        let err = target_addr(yaml, "statsd").unwrap_err();
        assert!(format!("{err:#}").contains("port 0"), "{err:#}");
    }

    #[test]
    fn cpu_lists_parse_singletons_lists_and_ranges() {
        assert_eq!(CpuSet::parse("3").unwrap().cpus(), &[3]);
        assert_eq!(CpuSet::parse("2,4").unwrap().cpus(), &[2, 4]);
        assert_eq!(CpuSet::parse("2-5").unwrap().cpus(), &[2, 3, 4, 5]);
        assert_eq!(CpuSet::parse(" 6 , 1-2 ").unwrap().cpus(), &[1, 2, 6]);
        assert_eq!(CpuSet::parse("3,3").unwrap().cpus(), &[3], "duplicates collapse");
        assert_eq!(CpuSet::parse("2-3").unwrap().to_string(), "2,3");
    }

    #[test]
    fn a_malformed_cpu_list_is_rejected() {
        assert!(CpuSet::parse("").is_err());
        assert!(CpuSet::parse("a").is_err());
        assert!(CpuSet::parse("5-2").is_err());
        assert!(CpuSet::parse("1,,2").is_err());
    }

    /// A CPU beyond `CPU_SETSIZE` is rejected before any range is expanded, not after an OOM.
    #[test]
    fn an_out_of_range_cpu_is_rejected_without_expanding_the_range() {
        for list in ["0-99999999999", "99999999999", "99999999999-99999999999", "0-1023,2048"] {
            let err = CpuSet::parse(list).expect_err("{list} is beyond CPU_SETSIZE");
            assert!(format!("{err:#}").contains("CPU_SETSIZE"), "{list}: {err:#}");
        }
        // The largest legal CPU still parses, so the bound is exclusive in the right direction.
        let highest = libc::CPU_SETSIZE as usize - 1;
        assert_eq!(CpuSet::parse(&highest.to_string()).unwrap().cpus(), &[highest]);
    }

    /// `SO_RCVBUF` on a test's own receiving socket, so a test about the *sender* can't fail on a
    /// kernel drop. `std::net::UdpSocket` has no safe setter for it.
    fn set_receive_buffer(socket: &UdpSocket, bytes: libc::c_int) {
        // SAFETY: `socket` is a live, owned `UdpSocket` for the duration of the call, so its fd is
        // valid; `&bytes` is a `c_int` out-living the call and `size_of::<c_int>()` is exactly the
        // length `SO_RCVBUF` expects. Standard `setsockopt(2)` contract.
        let rc = unsafe {
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_RCVBUF,
                std::ptr::addr_of!(bytes).cast(),
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        assert_eq!(rc, 0, "setsockopt(SO_RCVBUF): {}", std::io::Error::last_os_error());
    }

    /// Every datagram reaches a real loopback socket in ring order, with the claimed byte counts.
    #[test]
    fn a_blast_delivers_every_datagram_to_a_real_socket() {
        let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
        receiver.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        // The whole 200-datagram blast lands before this test reads a byte of it, so the receive
        // buffer has to hold all of it at once or the test fails on a kernel drop it is not about.
        // Asked for explicitly rather than left to `rmem_default` (212,992 B on a stock kernel,
        // and each ~30-byte datagram is charged several hundred bytes of `skb->truesize`): this
        // must pass by design, not by margin.
        set_receive_buffer(&receiver, 4 * 1024 * 1024);
        let target = receiver.local_addr().unwrap();

        let mut spec = spec(vec![single()]);
        spec.datagrams = 200;
        spec.ring_datagrams = 64;
        spec.sockets = 2;
        spec.threads = 1;
        let ring = Ring::render(&spec, &model(&[(1, "blast.{seq%8}:1|c")])).unwrap();
        let expected_bytes = ring.window(0, spec.datagrams).bytes;
        let plan = LoadPlan {
            spec: spec.clone(),
            spec_path: PathBuf::from("/nowhere/x.yaml"),
            ring,
            target,
        };

        let outcome = blast(&plan, None, None).unwrap();
        assert_eq!(outcome.sent_datagrams, 200);
        assert_eq!(outcome.sent_lines, 200);
        assert_eq!(outcome.sent_bytes, expected_bytes);

        let mut buffer = [0u8; 2048];
        for index in 0..200u64 {
            let (read, _) = receiver.recv_from(&mut buffer).expect("every datagram arrives");
            assert_eq!(&buffer[..read], plan.ring.datagram(index), "datagram {index}");
        }
    }

    /// A blast at a port nobody listens on fails naming it, rather than running to completion.
    #[test]
    fn a_blast_at_a_port_nobody_listens_on_fails_naming_the_target() {
        // Bound and dropped: the port is almost certainly free, and free is what this needs.
        let target = {
            let probe = UdpSocket::bind("127.0.0.1:0").unwrap();
            probe.local_addr().unwrap()
        };

        let mut spec = spec(vec![single()]);
        spec.datagrams = 100_000;
        spec.ring_datagrams = 64;
        spec.sockets = 1;
        spec.threads = 1;
        let ring = Ring::render(&spec, &model(&[(1, "dead.{seq%4}:1|c")])).unwrap();
        let plan = LoadPlan { spec, spec_path: PathBuf::from("/nowhere/x.yaml"), ring, target };

        let err = blast(&plan, None, None)
            .expect_err("a blast into a port nothing is bound to must fail, not succeed quietly");
        let err = format!("{err:#}");
        assert!(err.contains("nothing is listening on"), "{err}");
        assert!(err.contains(&target.to_string()), "{err}");
    }

    /// The refusal budget is the blast's, not each thread's.
    #[test]
    fn the_connection_refused_budget_is_shared_across_sender_threads() {
        let target = {
            let probe = UdpSocket::bind("127.0.0.1:0").unwrap();
            probe.local_addr().unwrap()
        };

        let mut spec = spec(vec![single()]);
        spec.datagrams = 100_000;
        spec.ring_datagrams = 64;
        spec.sockets = 2;
        spec.threads = 2;
        let ring = Ring::render(&spec, &model(&[(1, "shared.{seq%4}:1|c")])).unwrap();
        let plan = LoadPlan { spec, spec_path: PathBuf::from("/nowhere/x.yaml"), ring, target };

        let err = format!("{:#}", blast(&plan, None, None).expect_err("nothing is listening"));
        assert!(err.contains("nothing is listening on"), "{err}");
        assert!(err.contains(&target.to_string()), "{err}");
        assert!(err.contains("across all sender threads"), "{err}");

        // The reported total is the documented one, not a multiple of it. Both threads can cross
        // the line before either returns, so the bound is `MAX + threads`, not `MAX + 1`.
        let total: u64 = err
            .split(&format!("{target}: "))
            .nth(1)
            .and_then(|rest| rest.split(' ').next())
            .expect("the message quotes the total")
            .parse()
            .expect("...as a number");
        assert!(
            (MAX_CONNECTION_REFUSED + 1..=MAX_CONNECTION_REFUSED + 2).contains(&total),
            "gave up at {total}, expected just past {MAX_CONNECTION_REFUSED} -- a per-thread \
             budget would have run to {}",
            MAX_CONNECTION_REFUSED * 2
        );
    }

    /// A fired abort probe stops the sender promptly and names the cause.
    #[test]
    fn a_fired_abort_probe_stops_the_blast_and_says_why() {
        let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
        set_receive_buffer(&receiver, 4 * 1024 * 1024);
        let target = receiver.local_addr().unwrap();

        let mut spec = spec(vec![single()]);
        spec.datagrams = 1_000_000;
        spec.ring_datagrams = 64;
        spec.sockets = 1;
        spec.threads = 1;
        let ring = Ring::render(&spec, &model(&[(1, "gone.{seq%4}:1|c")])).unwrap();
        let plan = LoadPlan { spec, spec_path: PathBuf::from("/nowhere/x.yaml"), ring, target };

        // Already filled, so the very first batch sees it -- this is about the message and the
        // fact that it stops, not about racing a real child's death.
        let cause = std::sync::OnceLock::new();
        cause.set("the child exited".to_string()).unwrap();
        let err = blast(&plan, None, Some(Abort { cause: &cause }))
            .expect_err("a fired abort must stop the blast");
        let err = format!("{err:#}");
        assert!(err.contains("the child exited"), "{err}");
        assert!(err.contains("of 1000000 datagrams"), "{err}");
    }

    #[test]
    fn a_clear_abort_probe_lets_the_whole_blast_through() {
        let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
        set_receive_buffer(&receiver, 4 * 1024 * 1024);
        let target = receiver.local_addr().unwrap();

        let mut spec = spec(vec![single()]);
        spec.datagrams = 200;
        spec.ring_datagrams = 64;
        spec.sockets = 1;
        spec.threads = 1;
        let ring = Ring::render(&spec, &model(&[(1, "fine.{seq%4}:1|c")])).unwrap();
        let plan = LoadPlan { spec, spec_path: PathBuf::from("/nowhere/x.yaml"), ring, target };

        let cause = std::sync::OnceLock::new();
        let outcome = blast(&plan, None, Some(Abort { cause: &cause })).unwrap();
        assert_eq!(outcome.sent_datagrams, 200);
    }

    fn plan_with_rate(rate: Option<u64>) -> LoadPlan {
        let mut spec = spec(vec![single()]);
        spec.rate = rate;
        spec.ring_datagrams = 16;
        let ring = Ring::render(&spec, &model(&[(1, "r.{seq%4}:1|c")])).unwrap();
        LoadPlan {
            spec,
            spec_path: PathBuf::from("/nowhere/udp-statsd.yaml"),
            ring,
            target: "127.0.0.1:1".parse().unwrap(),
        }
    }

    #[test]
    fn scale_rate_multiplies_the_specs_own_pacing() {
        let mut plan = plan_with_rate(Some(100_000));
        assert_eq!(plan.scale_rate(0.5).unwrap(), 50_000);
        assert_eq!(plan.spec.rate, Some(50_000));

        // `--verify`'s scale is this knob at a fixed value.
        let mut plan = plan_with_rate(Some(760_000));
        assert_eq!(plan.scale_rate(VERIFY_RATE_SCALE).unwrap(), 190_000);
    }

    #[test]
    fn scale_rate_never_rounds_a_rate_down_to_nothing() {
        let mut plan = plan_with_rate(Some(10));
        assert_eq!(plan.scale_rate(0.000_01).unwrap(), 1, "0 would mean `send nothing`");
    }

    #[test]
    fn scale_rate_rejects_a_scale_that_is_not_a_positive_number() {
        for bad in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            let mut plan = plan_with_rate(Some(1_000));
            let err = plan.scale_rate(bad).expect_err("{bad} is not a usable scale");
            assert!(format!("{err:#}").contains("greater than 0"), "{err:#}");
        }
    }

    #[test]
    fn scale_rate_refuses_a_spec_with_no_rate_at_all() {
        let mut plan = plan_with_rate(None);
        let err = plan.scale_rate(0.5).expect_err("there is nothing to scale");
        assert!(format!("{err:#}").contains("nothing"), "{err:#}");
    }

    // ---------------------------------------------------------------------------------------
    // The shipped model, against the real decoder.
    //
    // A load spec that renders lines `StatsdDecoder` rejects would benchmark the malformed-line
    // path and look healthy doing it -- fast, even, since a rejected line never becomes an event.
    // `crate::run`'s self-check catches that at run time from the child's own telemetry; these
    // catch it in CI, against the very decoder the scenario's `statsd_in` will run.
    // ---------------------------------------------------------------------------------------

    fn repo_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
    }

    /// Decodes `datagram` with a real [`logit_inputs::statsd::StatsdDecoder`], returning the
    /// events it produced and every diagnostic key it raised on the way.
    fn decode(datagram: &[u8]) -> (Vec<logit_core::Event>, Vec<String>) {
        use logit_core::telemetry::Registry;
        use logit_core::{Diagnostics, Resource};
        use logit_proto::Decoder;
        use std::sync::Arc;

        let registry = Registry::new();
        let telemetry = registry.telemetry_for("statsd_in", "statsd_in", "listener");
        let mut decoder = logit_inputs::statsd::StatsdDecoder::new(Arc::new(Resource::default()))
            .with_diagnostics(Diagnostics::new("statsd_in").with_telemetry(telemetry));
        let mut events = Vec::new();
        decoder
            .decode_into(bytes::Bytes::copy_from_slice(datagram), 0, &mut events)
            .expect("a whole datagram must decode");
        let keys: Vec<String> = registry
            .drain(0)
            .into_iter()
            .filter_map(|event| match event.attributes.get("key") {
                Some(logit_core::Value::Str(key)) => {
                    Some(String::from_utf8_lossy(key).into_owned())
                }
                _ => None,
            })
            .collect();
        (events, keys)
    }

    /// The shipped `udp-statsd` spec and its model, read off disk the same way the harness does.
    fn shipped_plan(name: &str) -> (LoadSpec, Ring) {
        let spec_path = repo_root().join("perf/load").join(format!("{name}.yaml"));
        let spec = read_spec(&spec_path).expect("the shipped spec parses");
        let model = read_model(&spec_path, &spec).expect("the shipped model parses");
        let ring = Ring::render(&spec, &model).expect("the shipped model renders");
        (spec, ring)
    }

    #[test]
    fn every_line_the_shipped_model_renders_decodes_cleanly() {
        // The mixed spec exercises every packing target, so its ring covers the single-line and
        // the two packed shapes in one pass.
        let (_, ring) = shipped_plan("udp-statsd");
        let mut total_events = 0u64;
        // A slice of the ring, not all 8191 datagrams: this is a debug-profile decode of over a
        // hundred thousand lines otherwise, and the ring is homogeneous by construction -- every
        // template appears within the first few hundred datagrams.
        for index in 0..500u64 {
            let datagram = ring.datagram(index);
            let (events, diagnostics) = decode(datagram);
            assert!(
                diagnostics.is_empty(),
                "datagram {index} raised {diagnostics:?}:\n{}",
                String::from_utf8_lossy(datagram)
            );
            total_events += events.len() as u64;
        }
        // The harness's event count, which `--verify` holds a run to, matches the decoder's.
        assert_eq!(total_events, ring.window(0, 500).events);
    }

    #[test]
    fn all_three_shipped_specs_parse_and_share_one_model() {
        let (mixed, _) = shipped_plan("udp-statsd");
        let (small, small_ring) = shipped_plan("udp-statsd-small");
        let (packed, packed_ring) = shipped_plan("udp-statsd-packed");

        assert_eq!(mixed.model, small.model, "all three share one line model");
        assert_eq!(mixed.model, packed.model);
        assert_eq!(mixed.target, "statsd");

        assert_eq!(small_ring.shape().max_lines, 1, "`-small` is single-line by definition");
        let packed = packed_ring.shape();
        assert!(packed.max_bytes <= 1432, "`-packed` respects DogStatsD's MTU: {packed}");
        assert!(
            packed.min_lines > 1,
            "`-packed` packs, it does not degenerate to one line per datagram: {packed}"
        );
    }

    #[test]
    fn the_committed_real_client_capture_decodes_cleanly_too() {
        // The capture the model is calibrated against (testdata/interop/statsd/, recorded by
        // `script/record-fixtures statsd`). If a real client's own output doesn't decode, the
        // model derived from it is calibrated against something this codebase can't read.
        let dir = repo_root().join("testdata/interop/statsd");
        let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
            .unwrap_or_else(|err| panic!("reading {}: {err}", dir.display()))
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "raw"))
            .collect();
        files.sort();
        assert!(!files.is_empty(), "no capture under {}", dir.display());

        let mut events = 0usize;
        for path in &files {
            let raw = std::fs::read(path).unwrap();
            let (decoded, diagnostics) = decode(&raw);
            assert!(diagnostics.is_empty(), "{} raised {diagnostics:?}", path.display());
            assert!(!decoded.is_empty(), "{} decoded to nothing", path.display());
            events += decoded.len();
        }
        assert!(events > 100, "the capture should carry a real workload, got {events} events");
    }

    #[test]
    fn a_paced_blast_takes_at_least_as_long_as_its_rate_implies() {
        let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
        let target = receiver.local_addr().unwrap();
        let mut spec = spec(vec![single()]);
        spec.datagrams = 256;
        spec.ring_datagrams = 16;
        spec.sockets = 1;
        spec.threads = 1;
        spec.rate = Some(2_000);
        let ring = Ring::render(&spec, &model(&[(1, "p.{seq%4}:1|c")])).unwrap();
        let plan = LoadPlan { spec, spec_path: PathBuf::from("/nowhere/x.yaml"), ring, target };
        let outcome = blast(&plan, None, None).unwrap();
        assert_eq!(outcome.sent_datagrams, 256);
        // 256 datagrams at 2000/s is 128ms; pacing is per 64-datagram batch, so the last batch
        // isn't waited out -- three batches' worth (96ms) is the floor this can assert.
        assert!(
            outcome.elapsed >= Duration::from_millis(90),
            "elapsed {:?} is faster than the configured rate allows",
            outcome.elapsed
        );
    }
}
