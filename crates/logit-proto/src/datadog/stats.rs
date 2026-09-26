//! APM stats: tracers' `/v0.6/stats` `ClientStatsPayload` and the intake's `/api/v0.2/stats`
//! `StatsPayload`, both msgpack under the Go field names, and the DDSketch protobuf their
//! `OkSummary`/`ErrorSummary` carry. The message shapes are `datadog-agent` 7.83.3's
//! `pkg/proto/datadog/trace/stats.proto` (vendored under `crates/logit-proto/proto/datadog/`); the
//! msgpack key names are that tag's `pkg/proto/pbgo/trace/stats_gen.go`. The mapping tables are in
//! [`super`]'s "APM stats" section.
//!
//! The msgpack walk reads into the prost types in [`crate::datadog::generated::trace`] (used here
//! only as plain structs; the wire is never protobuf) and converts from there, so the wire walk and
//! the model mapping stay separate. Every array element is sliced out whole
//! ([`Reader::skip_value`]) before it's parsed, so a malformed group, bucket, or client payload is
//! dropped and counted while the walk resumes after it; only a body that isn't msgpack at all, or
//! a malformed top level, fails the request.

use super::{
    merged_get, DatadogDecoder, DatadogEncoder, ATTR_RESOURCE_NAME, ATTR_SERVICE_NAME,
    ATTR_SPAN_KIND, ATTR_SPAN_TYPE, RESOURCE_ATTR_AGENT_ENV, RESOURCE_ATTR_AGENT_HOSTNAME,
    RESOURCE_ATTR_AGENT_VERSION, RESOURCE_ATTR_TRACER_APP_VERSION,
    RESOURCE_ATTR_TRACER_CONTAINER_ID, RESOURCE_ATTR_TRACER_ENV, RESOURCE_ATTR_TRACER_HOSTNAME,
    RESOURCE_ATTR_TRACER_LANGUAGE_NAME, RESOURCE_ATTR_TRACER_RUNTIME_ID,
    RESOURCE_ATTR_TRACER_VERSION,
};
use crate::datadog::generated::ddsketch::{
    index_mapping::Interpolation, DdSketch as PbSketch, Store,
};
use crate::datadog::generated::trace::{
    ClientGroupedStats, ClientStatsBucket, ClientStatsPayload, StatsPayload,
};
use crate::msgpack::{MsgpackError, Reader, Type, Writer};
use crate::CodecError;
use bytes::Bytes;
use logit_core::interner::{intern, resolve};
use logit_core::{
    AttrMap, Bin, DdSketch, Event, EventBatch, Mapping, MappingKind, MetricKind, MetricRecord,
    Resource, Sum, Temporality, Value,
};
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;

/// `datadog.stats.hits` / `.errors` / `.top_level_hits`: the group's weighted span counts, each a
/// delta monotonic `Sum`.
pub const METRIC_HITS: &str = "datadog.stats.hits";
pub const METRIC_ERRORS: &str = "datadog.stats.errors";
pub const METRIC_TOP_LEVEL_HITS: &str = "datadog.stats.top_level_hits";
/// `datadog.stats.duration`: the group's total span duration, a delta monotonic `Sum` in `ns`.
pub const METRIC_DURATION: &str = "datadog.stats.duration";
/// `datadog.stats.ok_summary` / `.error_summary`: the duration sketches, each a `Distribution`
/// under the wire's own logarithmic mapping.
pub const METRIC_OK_SUMMARY: &str = "datadog.stats.ok_summary";
pub const METRIC_ERROR_SUMMARY: &str = "datadog.stats.error_summary";
/// The unit `datadog.stats.duration` carries.
pub const DURATION_UNIT: &str = "ns";

/// `datadog.stats.name`: the group's operation `Name`. Always present on a decoded group, even
/// empty, because it is what marks an event as APM stats ([`super::is_datadog_stats`]).
pub const ATTR_STATS_NAME: &str = "datadog.stats.name";
pub const ATTR_HTTP_STATUS_CODE: &str = "datadog.stats.http_status_code";
pub const ATTR_DB_TYPE: &str = "datadog.stats.db_type";
pub const ATTR_SYNTHETICS: &str = "datadog.stats.synthetics";
pub const ATTR_IS_TRACE_ROOT: &str = "datadog.stats.is_trace_root";
pub const ATTR_GRPC_STATUS_CODE: &str = "datadog.stats.grpc_status_code";
pub const ATTR_HTTP_METHOD: &str = "datadog.stats.http_method";
pub const ATTR_HTTP_ENDPOINT: &str = "datadog.stats.http_endpoint";
pub const ATTR_SERVICE_SOURCE: &str = "datadog.stats.service_source";
pub const ATTR_PEER_TAGS: &str = "datadog.stats.peer_tags";
pub const ATTR_ADDITIONAL_METRIC_TAGS: &str = "datadog.stats.additional_metric_tags";
pub const ATTR_SPAN_DERIVED_PRIMARY_TAGS: &str = "datadog.stats.span_derived_primary_tags";
/// The bucket's `Duration` (U64 ns) and nonzero `AgentTimeShift` (I64 ns), on each of its groups.
pub const ATTR_BUCKET_DURATION: &str = "datadog.stats.bucket.duration";
pub const ATTR_BUCKET_AGENT_TIME_SHIFT: &str = "datadog.stats.bucket.agent_time_shift";

/// `ClientStatsPayload` fields, as batch resource attributes. `RESOURCE_ATTR_TRACER_HOSTNAME` /
/// `_ENV` / `_APP_VERSION` / `_LANGUAGE_NAME` / `_VERSION` / `_RUNTIME_ID` / `_CONTAINER_ID` are
/// [`super`]'s constants, shared with [`super::traces`]'s `TracerPayload` fields.
pub const RESOURCE_ATTR_STATS_SEQUENCE: &str = "datadog.stats.sequence";
pub const RESOURCE_ATTR_STATS_AGENT_AGGREGATION: &str = "datadog.stats.agent_aggregation";
pub const RESOURCE_ATTR_STATS_SERVICE: &str = "datadog.stats.service";
pub const RESOURCE_ATTR_STATS_TAGS: &str = "datadog.stats.tags";
pub const RESOURCE_ATTR_STATS_GIT_COMMIT_SHA: &str = "datadog.stats.git_commit_sha";
pub const RESOURCE_ATTR_STATS_IMAGE_TAG: &str = "datadog.stats.image_tag";
pub const RESOURCE_ATTR_STATS_PROCESS_TAGS_HASH: &str = "datadog.stats.process_tags_hash";
pub const RESOURCE_ATTR_STATS_PROCESS_TAGS: &str = "datadog.stats.process_tags";
/// `StatsPayload` envelope fields, as batch resource attributes (`agentHostname`, `agentEnv`, and
/// `agentVersion` reuse [`RESOURCE_ATTR_AGENT_HOSTNAME`], [`RESOURCE_ATTR_AGENT_ENV`], and
/// [`RESOURCE_ATTR_AGENT_VERSION`]).
pub const RESOURCE_ATTR_STATS_CLIENT_COMPUTED: &str = "datadog.stats.client_computed";
pub const RESOURCE_ATTR_STATS_SPLIT_PAYLOAD: &str = "datadog.stats.split_payload";

/// The bin limit a decoded stats sketch keeps: the Agent's `LogCollapsingLowestDenseDDSketch(0.01,
/// 2048)` (`pkg/trace/stats/statsraw.go`).
pub const STATS_BIN_LIMIT: u32 = 2048;
const _: () = assert!(STATS_BIN_LIMIT <= Mapping::MAX_BIN_LIMIT);

/// Every per-group string field's attribute, mapped to its wire field by [`group_string_mut`].
/// Written when non-empty on decode, read back as `Str` on encode. `ATTR_SERVICE_NAME` /
/// `ATTR_RESOURCE_NAME` / `ATTR_SPAN_TYPE` / `ATTR_SPAN_KIND` carry the group's `Service` /
/// `Resource` / `Type` / `SpanKind`, under the names a span carries them by (ADR
/// `datadog-agent-and-intake-relay` decision 8); they're [`super`]'s constants, shared with
/// [`super::traces`].
const GROUP_STRINGS: [&str; 10] = [
    ATTR_SERVICE_NAME,
    ATTR_RESOURCE_NAME,
    ATTR_SPAN_TYPE,
    ATTR_SPAN_KIND,
    ATTR_STATS_NAME,
    ATTR_DB_TYPE,
    ATTR_GRPC_STATUS_CODE,
    ATTR_HTTP_METHOD,
    ATTR_HTTP_ENDPOINT,
    ATTR_SERVICE_SOURCE,
];

fn group_string_mut<'g>(g: &'g mut ClientGroupedStats, key: &str) -> &'g mut String {
    match key {
        ATTR_SERVICE_NAME => &mut g.service,
        ATTR_RESOURCE_NAME => &mut g.resource,
        ATTR_SPAN_TYPE => &mut g.r#type,
        ATTR_SPAN_KIND => &mut g.span_kind,
        ATTR_STATS_NAME => &mut g.name,
        ATTR_DB_TYPE => &mut g.db_type,
        ATTR_GRPC_STATUS_CODE => &mut g.grpc_status_code,
        ATTR_HTTP_METHOD => &mut g.http_method,
        ATTR_HTTP_ENDPOINT => &mut g.http_endpoint,
        _ => &mut g.service_source,
    }
}

const GROUP_ARRAYS: [&str; 3] =
    [ATTR_PEER_TAGS, ATTR_ADDITIONAL_METRIC_TAGS, ATTR_SPAN_DERIVED_PRIMARY_TAGS];

fn group_array_mut<'g>(g: &'g mut ClientGroupedStats, key: &str) -> &'g mut Vec<String> {
    match key {
        ATTR_PEER_TAGS => &mut g.peer_tags,
        ATTR_ADDITIONAL_METRIC_TAGS => &mut g.additional_metric_tags,
        _ => &mut g.span_derived_primary_tags,
    }
}

/// Every other attribute a stats event may carry; anything outside these and the two lists above
/// has no wire form.
const GROUP_SCALARS: [&str; 5] = [
    ATTR_HTTP_STATUS_CODE,
    ATTR_SYNTHETICS,
    ATTR_IS_TRACE_ROOT,
    ATTR_BUCKET_DURATION,
    ATTR_BUCKET_AGENT_TIME_SHIFT,
];

const PAYLOAD_STRINGS: [&str; 12] = [
    RESOURCE_ATTR_TRACER_HOSTNAME,
    RESOURCE_ATTR_TRACER_ENV,
    RESOURCE_ATTR_TRACER_APP_VERSION,
    RESOURCE_ATTR_TRACER_LANGUAGE_NAME,
    RESOURCE_ATTR_TRACER_VERSION,
    RESOURCE_ATTR_TRACER_RUNTIME_ID,
    RESOURCE_ATTR_TRACER_CONTAINER_ID,
    RESOURCE_ATTR_STATS_AGENT_AGGREGATION,
    RESOURCE_ATTR_STATS_SERVICE,
    RESOURCE_ATTR_STATS_GIT_COMMIT_SHA,
    RESOURCE_ATTR_STATS_IMAGE_TAG,
    RESOURCE_ATTR_STATS_PROCESS_TAGS,
];

fn payload_string_mut<'p>(p: &'p mut ClientStatsPayload, key: &str) -> &'p mut String {
    match key {
        RESOURCE_ATTR_TRACER_HOSTNAME => &mut p.hostname,
        RESOURCE_ATTR_TRACER_ENV => &mut p.env,
        RESOURCE_ATTR_TRACER_APP_VERSION => &mut p.version,
        RESOURCE_ATTR_TRACER_LANGUAGE_NAME => &mut p.lang,
        RESOURCE_ATTR_TRACER_VERSION => &mut p.tracer_version,
        RESOURCE_ATTR_TRACER_RUNTIME_ID => &mut p.runtime_id,
        RESOURCE_ATTR_TRACER_CONTAINER_ID => &mut p.container_id,
        RESOURCE_ATTR_STATS_AGENT_AGGREGATION => &mut p.agent_aggregation,
        RESOURCE_ATTR_STATS_SERVICE => &mut p.service,
        RESOURCE_ATTR_STATS_GIT_COMMIT_SHA => &mut p.git_commit_sha,
        RESOURCE_ATTR_STATS_IMAGE_TAG => &mut p.image_tag,
        _ => &mut p.process_tags,
    }
}

const PAYLOAD_SCALARS: [&str; 3] =
    [RESOURCE_ATTR_STATS_SEQUENCE, RESOURCE_ATTR_STATS_PROCESS_TAGS_HASH, RESOURCE_ATTR_STATS_TAGS];

const ENVELOPE_KEYS: [&str; 5] = [
    RESOURCE_ATTR_AGENT_HOSTNAME,
    RESOURCE_ATTR_AGENT_ENV,
    RESOURCE_ATTR_AGENT_VERSION,
    RESOURCE_ATTR_STATS_CLIENT_COMPUTED,
    RESOURCE_ATTR_STATS_SPLIT_PAYLOAD,
];

// ---------------------------------------------------------------------------------------------
// DDSketch protobuf
// ---------------------------------------------------------------------------------------------

/// Why a DDSketch protobuf didn't become a [`DdSketch`].
#[derive(Debug, Clone, PartialEq)]
pub(super) enum SketchError {
    /// Not a DDSketch, no mapping, or a mapping no key space can be built from.
    Malformed(String),
    /// An interpolated mapping (`LINEAR`/`QUADRATIC`/`CUBIC`): its keys aren't
    /// [`Mapping::logarithmic`]'s, and re-keying them would be a guess.
    Interpolation,
}

/// A DDSketch protobuf (`sketches-go`'s `ddsketch.proto`) as a [`DdSketch`] under
/// `Mapping::logarithmic(gamma, indexOffset, 2048)`. Each store's sparse `binCounts` and
/// contiguous `contiguousBinCounts` both contribute, a key present in both summing. The summary
/// is derived from the bins, since the protobuf carries none.
pub(super) fn decode_ddsketch(bytes: &[u8]) -> Result<DdSketch, SketchError> {
    let pb = PbSketch::decode(bytes)
        .map_err(|e| SketchError::Malformed(format!("not a DDSketch protobuf: {e}")))?;
    let m = pb.mapping.ok_or_else(|| SketchError::Malformed("no index mapping".into()))?;
    if m.interpolation != Interpolation::None as i32 {
        return Err(SketchError::Interpolation);
    }
    let mapping =
        Mapping::try_logarithmic(m.gamma, m.index_offset, STATS_BIN_LIMIT).ok_or_else(|| {
            SketchError::Malformed(format!(
                "gamma {} / index offset {} is not a mapping",
                m.gamma, m.index_offset
            ))
        })?;
    let positive = store_bins(pb.positive_values.as_ref())?;
    let negative = store_bins(pb.negative_values.as_ref())?;
    Ok(DdSketch::from_parts(mapping, positive, negative, pb.zero_count, None))
}

fn store_bins(store: Option<&Store>) -> Result<Vec<Bin>, SketchError> {
    let Some(store) = store else { return Ok(Vec::new()) };
    let mut bins: Vec<Bin> =
        store.bin_counts.iter().map(|(&key, &count)| Bin { key, count }).collect();
    for (i, &count) in store.contiguous_bin_counts.iter().enumerate() {
        let key = i32::try_from(i)
            .ok()
            .and_then(|i| store.contiguous_bin_index_offset.checked_add(i))
            .ok_or_else(|| SketchError::Malformed("contiguous bins overflow the key".into()))?;
        bins.push(Bin { key, count });
    }
    Ok(bins)
}

/// A [`DdSketch`] as a DDSketch protobuf: the mapping (`interpolation: NONE`), both stores as
/// sparse `binCounts` in ascending key order, and `zeroCount`. Hand-encoded rather than through
/// prost, whose `binCounts` is a `HashMap` and would make the bytes depend on its iteration order.
///
/// An Agent-mapped sketch goes out as `gamma = 1.015625`, `indexOffset = bias + 0.5`, keys
/// unchanged: Datadog's own conversion, reading the Agent's round-half-to-even key as the
/// logarithmic mapping's floor. Only a value on an exact half-way tie keys differently, and the
/// sketch's exact summary has no protobuf field; the caller counts it `agent_mapping`.
pub(super) fn encode_ddsketch(sketch: &DdSketch) -> Vec<u8> {
    let m = sketch.mapping();
    let offset = match m.kind() {
        MappingKind::Logarithmic => m.index_offset(),
        MappingKind::Agent => m.index_offset() + 0.5,
    };
    let mut mapping = Vec::with_capacity(18);
    put_double(&mut mapping, 1, m.gamma());
    put_double(&mut mapping, 2, offset);
    let mut out = Vec::new();
    put_len(&mut out, 1, &mapping);
    put_len(&mut out, 2, &encode_store(sketch.positive_bins()));
    put_len(&mut out, 3, &encode_store(sketch.negative_bins()));
    put_double(&mut out, 4, sketch.zero_count());
    out
}

fn encode_store(bins: &[Bin]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut entry = Vec::with_capacity(16);
    for bin in bins {
        entry.clear();
        prost::encoding::encode_varint(0x08, &mut entry);
        let zigzag = ((bin.key << 1) ^ (bin.key >> 31)) as u32;
        prost::encoding::encode_varint(u64::from(zigzag), &mut entry);
        entry.push(0x11);
        entry.extend_from_slice(&bin.count.to_le_bytes());
        put_len(&mut out, 1, &entry);
    }
    out
}

/// A proto3 `double` field, omitted at its default.
fn put_double(out: &mut Vec<u8>, field: u64, v: f64) {
    if v.to_bits() == 0 {
        return;
    }
    prost::encoding::encode_varint(field << 3 | 1, out);
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_len(out: &mut Vec<u8>, field: u64, bytes: &[u8]) {
    prost::encoding::encode_varint(field << 3 | 2, out);
    prost::encoding::encode_varint(bytes.len() as u64, out);
    out.extend_from_slice(bytes);
}

// ---------------------------------------------------------------------------------------------
// msgpack walk
// ---------------------------------------------------------------------------------------------

/// Consumes a `nil` and reports it: msgp's `IsNil` rule, where a nil field is its zero value.
fn nil(r: &mut Reader<'_>) -> Result<bool, MsgpackError> {
    if r.peek_type()? == Type::Nil {
        r.read_nil()?;
        Ok(true)
    } else {
        Ok(false)
    }
}

/// A map key: msgp's `ReadMapKeyZC` takes a `str` or a `bin`.
fn read_key<'a>(r: &mut Reader<'a>) -> Result<&'a [u8], MsgpackError> {
    if r.peek_type()? == Type::Bin {
        r.read_bin()
    } else {
        r.read_str_bytes()
    }
}

fn read_string(r: &mut Reader<'_>) -> Result<String, MsgpackError> {
    if nil(r)? {
        return Ok(String::new());
    }
    r.read_str().map(str::to_owned)
}

fn read_u64(r: &mut Reader<'_>) -> Result<u64, MsgpackError> {
    if nil(r)? {
        return Ok(0);
    }
    r.read_u64()
}

fn read_i64(r: &mut Reader<'_>) -> Result<i64, MsgpackError> {
    if nil(r)? {
        return Ok(0);
    }
    r.read_i64()
}

fn read_u32(r: &mut Reader<'_>) -> Result<u32, MsgpackError> {
    let v = read_u64(r)?;
    u32::try_from(v).map_err(|_| MsgpackError::Type { expected: "u32", found: Type::Uint })
}

fn read_i32(r: &mut Reader<'_>) -> Result<i32, MsgpackError> {
    let v = read_i64(r)?;
    i32::try_from(v).map_err(|_| MsgpackError::Type { expected: "i32", found: Type::Int })
}

fn read_bool(r: &mut Reader<'_>) -> Result<bool, MsgpackError> {
    if nil(r)? {
        return Ok(false);
    }
    r.read_bool()
}

/// A `bin` (or, tolerated, a `str`) field.
fn read_bytes(r: &mut Reader<'_>) -> Result<Vec<u8>, MsgpackError> {
    if nil(r)? {
        return Ok(Vec::new());
    }
    if r.peek_type()? == Type::Str {
        return r.read_str_bytes().map(<[u8]>::to_vec);
    }
    r.read_bin().map(<[u8]>::to_vec)
}

fn read_strings(r: &mut Reader<'_>) -> Result<Vec<String>, MsgpackError> {
    if nil(r)? {
        return Ok(Vec::new());
    }
    let n = r.read_array_len()?;
    let mut out = Vec::new();
    for _ in 0..n {
        out.push(r.read_str()?.to_owned());
    }
    Ok(out)
}

/// The next whole value's bytes, `r` being a reader over exactly `buf`: a malformed element is
/// then parsed on its own and can be dropped without losing the walk's place.
fn next_value<'a>(buf: &'a [u8], r: &mut Reader<'a>) -> Result<&'a [u8], MsgpackError> {
    let start = buf.len() - r.remaining();
    r.skip_value()?;
    Ok(&buf[start..buf.len() - r.remaining()])
}

/// One `ClientGroupedStats` map (`stats_gen.go`'s keys); unknown keys are skipped.
fn read_group(buf: &[u8]) -> Result<ClientGroupedStats, MsgpackError> {
    let mut r = Reader::new(buf);
    let mut g = ClientGroupedStats::default();
    for _ in 0..r.read_map_len()? {
        match read_key(&mut r)? {
            b"Service" => g.service = read_string(&mut r)?,
            b"Name" => g.name = read_string(&mut r)?,
            b"Resource" => g.resource = read_string(&mut r)?,
            b"HTTPStatusCode" => g.http_status_code = read_u32(&mut r)?,
            b"Type" => g.r#type = read_string(&mut r)?,
            b"DBType" => g.db_type = read_string(&mut r)?,
            b"Hits" => g.hits = read_u64(&mut r)?,
            b"Errors" => g.errors = read_u64(&mut r)?,
            b"Duration" => g.duration = read_u64(&mut r)?,
            b"OkSummary" => g.ok_summary = read_bytes(&mut r)?,
            b"ErrorSummary" => g.error_summary = read_bytes(&mut r)?,
            b"Synthetics" => g.synthetics = read_bool(&mut r)?,
            b"TopLevelHits" => g.top_level_hits = read_u64(&mut r)?,
            b"SpanKind" => g.span_kind = read_string(&mut r)?,
            b"PeerTags" => g.peer_tags = read_strings(&mut r)?,
            b"IsTraceRoot" => g.is_trace_root = read_i32(&mut r)?,
            b"GRPCStatusCode" => g.grpc_status_code = read_string(&mut r)?,
            b"HTTPMethod" => g.http_method = read_string(&mut r)?,
            b"HTTPEndpoint" => g.http_endpoint = read_string(&mut r)?,
            b"srv_src" => g.service_source = read_string(&mut r)?,
            b"SpanDerivedPrimaryTags" => g.span_derived_primary_tags = read_strings(&mut r)?,
            b"AdditionalMetricTags" => g.additional_metric_tags = read_strings(&mut r)?,
            _ => r.skip_value()?,
        }
    }
    Ok(g)
}

impl DatadogDecoder {
    fn stats_skipped(&self, reason: &'static str) {
        self.telemetry.count("logit.input.stats.skipped", 1.0, &[("reason", reason)]);
    }

    fn stats_degraded(&self, reason: &'static str) {
        self.telemetry.count("logit.input.stats.degraded", 1.0, &[("reason", reason)]);
    }

    fn stats_malformed(&mut self, reason: &'static str, err: MsgpackError) {
        self.stats_skipped(reason);
        self.diagnostics
            .warn_throttled("malformed_stats", format!("datadog stats: {reason} dropped: {err}"));
    }

    /// The elements of a `Stats` array, each parsed by `parse` on its own slice; one that fails
    /// is dropped and counted `reason`.
    fn read_elements<'a, T>(
        &mut self,
        buf: &'a [u8],
        r: &mut Reader<'a>,
        reason: &'static str,
        mut parse: impl FnMut(&mut Self, &'a [u8]) -> Result<T, MsgpackError>,
    ) -> Result<Vec<T>, MsgpackError> {
        let mut out = Vec::new();
        if nil(r)? {
            return Ok(out);
        }
        for _ in 0..r.read_array_len()? {
            let item = next_value(buf, r)?;
            match parse(self, item) {
                Ok(v) => out.push(v),
                Err(e) => self.stats_malformed(reason, e),
            }
        }
        Ok(out)
    }

    fn read_bucket(&mut self, buf: &[u8]) -> Result<ClientStatsBucket, MsgpackError> {
        let mut r = Reader::new(buf);
        let mut b = ClientStatsBucket::default();
        for _ in 0..r.read_map_len()? {
            match read_key(&mut r)? {
                b"Start" => b.start = read_u64(&mut r)?,
                b"Duration" => b.duration = read_u64(&mut r)?,
                b"AgentTimeShift" => b.agent_time_shift = read_i64(&mut r)?,
                b"Stats" => {
                    b.stats =
                        self.read_elements(buf, &mut r, "malformed_group", |_, g| read_group(g))?
                }
                _ => r.skip_value()?,
            }
        }
        Ok(b)
    }

    fn read_client_payload(&mut self, buf: &[u8]) -> Result<ClientStatsPayload, MsgpackError> {
        let mut r = Reader::new(buf);
        let mut p = ClientStatsPayload::default();
        for _ in 0..r.read_map_len()? {
            match read_key(&mut r)? {
                b"Hostname" => p.hostname = read_string(&mut r)?,
                b"Env" => p.env = read_string(&mut r)?,
                b"Version" => p.version = read_string(&mut r)?,
                b"Lang" => p.lang = read_string(&mut r)?,
                b"TracerVersion" => p.tracer_version = read_string(&mut r)?,
                b"RuntimeID" => p.runtime_id = read_string(&mut r)?,
                b"Sequence" => p.sequence = read_u64(&mut r)?,
                b"AgentAggregation" => p.agent_aggregation = read_string(&mut r)?,
                b"Service" => p.service = read_string(&mut r)?,
                b"ContainerID" => p.container_id = read_string(&mut r)?,
                b"Tags" => p.tags = read_strings(&mut r)?,
                b"GitCommitSha" => p.git_commit_sha = read_string(&mut r)?,
                b"ImageTag" => p.image_tag = read_string(&mut r)?,
                b"ProcessTagsHash" => p.process_tags_hash = read_u64(&mut r)?,
                b"ProcessTags" => p.process_tags = read_string(&mut r)?,
                b"Stats" => {
                    p.stats =
                        self.read_elements(buf, &mut r, "malformed_bucket", Self::read_bucket)?
                }
                _ => r.skip_value()?,
            }
        }
        Ok(p)
    }

    fn read_stats_payload(&mut self, buf: &[u8]) -> Result<StatsPayload, MsgpackError> {
        let mut r = Reader::new(buf);
        let mut p = StatsPayload::default();
        for _ in 0..r.read_map_len()? {
            match read_key(&mut r)? {
                b"AgentHostname" => p.agent_hostname = read_string(&mut r)?,
                b"AgentEnv" => p.agent_env = read_string(&mut r)?,
                b"AgentVersion" => p.agent_version = read_string(&mut r)?,
                b"ClientComputed" => p.client_computed = read_bool(&mut r)?,
                b"SplitPayload" => p.split_payload = read_bool(&mut r)?,
                b"Stats" => {
                    p.stats = self.read_elements(
                        buf,
                        &mut r,
                        "malformed_payload",
                        Self::read_client_payload,
                    )?
                }
                _ => r.skip_value()?,
            }
        }
        Ok(p)
    }

    /// A tracer's `/v0.6/stats` body: one msgpack `ClientStatsPayload`, as one batch with one
    /// metric event per `ClientGroupedStats` per bucket. `received_at` is unused (every event is
    /// stamped with its bucket's `Start`) and taken for the routes' common shape.
    pub fn decode_client_stats_v06(
        &mut self,
        body: &[u8],
        received_at: i64,
    ) -> Result<EventBatch, CodecError> {
        let _ = received_at;
        let payload = self.read_client_payload(body)?;
        Ok(self.client_batch(&payload, None))
    }

    /// The intake's `/api/v0.2/stats` body: one msgpack `StatsPayload`, as one batch per
    /// `ClientStatsPayload`, each resource carrying the envelope's fields too.
    pub fn decode_stats_payload(
        &mut self,
        body: &[u8],
        received_at: i64,
    ) -> Result<Vec<EventBatch>, CodecError> {
        let _ = received_at;
        let payload = self.read_stats_payload(body)?;
        Ok(payload.stats.iter().map(|client| self.client_batch(client, Some(&payload))).collect())
    }

    fn client_batch(
        &mut self,
        client: &ClientStatsPayload,
        envelope: Option<&StatsPayload>,
    ) -> EventBatch {
        let mut resource = Resource::default();
        let attrs = &mut resource.attributes;
        let mut client = client.clone();
        for key in PAYLOAD_STRINGS {
            let v = std::mem::take(payload_string_mut(&mut client, key));
            if !v.is_empty() {
                attrs.insert(key, Value::str(v));
            }
        }
        if client.sequence != 0 {
            attrs.insert(RESOURCE_ATTR_STATS_SEQUENCE, Value::U64(client.sequence));
        }
        if client.process_tags_hash != 0 {
            attrs.insert(
                RESOURCE_ATTR_STATS_PROCESS_TAGS_HASH,
                Value::U64(client.process_tags_hash),
            );
        }
        if !client.tags.is_empty() {
            attrs.insert(RESOURCE_ATTR_STATS_TAGS, str_array(&client.tags));
        }
        if let Some(env) = envelope {
            for (key, v) in [
                (RESOURCE_ATTR_AGENT_HOSTNAME, &env.agent_hostname),
                (RESOURCE_ATTR_AGENT_ENV, &env.agent_env),
                (RESOURCE_ATTR_AGENT_VERSION, &env.agent_version),
            ] {
                if !v.is_empty() {
                    attrs.insert(key, Value::str(v.as_str()));
                }
            }
            if env.client_computed {
                attrs.insert(RESOURCE_ATTR_STATS_CLIENT_COMPUTED, Value::Bool(true));
            }
            if env.split_payload {
                attrs.insert(RESOURCE_ATTR_STATS_SPLIT_PAYLOAD, Value::Bool(true));
            }
        }
        let mut events = Vec::new();
        for bucket in &client.stats {
            if bucket.stats.is_empty() {
                self.stats_skipped("empty_bucket");
                continue;
            }
            let timestamp = i64::try_from(bucket.start).unwrap_or_else(|_| {
                self.stats_degraded("bucket_start_overflow");
                i64::MAX
            });
            for group in &bucket.stats {
                events.push(self.group_event(bucket, group, timestamp));
            }
        }
        EventBatch { resource: Arc::new(resource), scope: None, events }
    }

    fn group_event(
        &mut self,
        bucket: &ClientStatsBucket,
        group: &ClientGroupedStats,
        timestamp: i64,
    ) -> Event {
        let mut attrs = AttrMap::new();
        let mut g = group.clone();
        for key in GROUP_STRINGS {
            let v = std::mem::take(group_string_mut(&mut g, key));
            // The operation name is always kept: it marks the event as APM stats.
            if !v.is_empty() || key == ATTR_STATS_NAME {
                attrs.insert(key, Value::str(v));
            }
        }
        for key in GROUP_ARRAYS {
            let v = group_array_mut(&mut g, key);
            if !v.is_empty() {
                attrs.insert(key, str_array(v));
            }
        }
        if g.http_status_code != 0 {
            attrs.insert(ATTR_HTTP_STATUS_CODE, Value::U64(u64::from(g.http_status_code)));
        }
        if g.synthetics {
            attrs.insert(ATTR_SYNTHETICS, Value::Bool(true));
        }
        match g.is_trace_root {
            0 => {}
            1 => attrs.insert(ATTR_IS_TRACE_ROOT, Value::str("true")),
            2 => attrs.insert(ATTR_IS_TRACE_ROOT, Value::str("false")),
            _ => self.stats_degraded("unknown_trilean"),
        }
        attrs.insert(ATTR_BUCKET_DURATION, Value::U64(bucket.duration));
        if bucket.agent_time_shift != 0 {
            attrs.insert(ATTR_BUCKET_AGENT_TIME_SHIFT, Value::I64(bucket.agent_time_shift));
        }

        let mut records = Vec::with_capacity(6);
        for (name, v) in [
            (METRIC_HITS, g.hits),
            (METRIC_ERRORS, g.errors),
            (METRIC_TOP_LEVEL_HITS, g.top_level_hits),
            (METRIC_DURATION, g.duration),
        ] {
            let f = v as f64;
            if f as u64 != v {
                self.stats_degraded("inexact_count");
            }
            let mut record = MetricRecord::new(intern(name), MetricKind::counter(f));
            if name == METRIC_DURATION {
                record.unit = Some(intern(DURATION_UNIT));
            }
            records.push(record);
        }
        for (name, bytes) in
            [(METRIC_OK_SUMMARY, &g.ok_summary), (METRIC_ERROR_SUMMARY, &g.error_summary)]
        {
            if bytes.is_empty() {
                continue;
            }
            match decode_ddsketch(bytes) {
                Ok(sketch) => {
                    records.push(MetricRecord::new(intern(name), MetricKind::Distribution(sketch)))
                }
                Err(SketchError::Interpolation) => self.stats_skipped("interpolation"),
                Err(SketchError::Malformed(why)) => {
                    self.stats_skipped("bad_sketch");
                    self.diagnostics.warn_throttled(
                        "bad_stats_sketch",
                        format!("datadog stats: {name} dropped: {why}"),
                    );
                }
            }
        }
        let mut event = Event::metric(timestamp, attrs, records.remove(0));
        event.metrics.extend(records);
        event
    }
}

fn str_array(v: &[String]) -> Value {
    Value::Array(v.iter().map(|s| Value::str(s.as_str())).collect())
}

// ---------------------------------------------------------------------------------------------
// Encode
// ---------------------------------------------------------------------------------------------

impl DatadogEncoder {
    fn stats_out_skipped(&self, reason: &'static str) {
        self.telemetry.count("logit.output.stats.skipped", 1.0, &[("reason", reason)]);
    }

    fn stats_out_degraded(&self, reason: &'static str) {
        self.telemetry.count("logit.output.stats.degraded", 1.0, &[("reason", reason)]);
    }

    fn tag_dropped(&self, reason: &'static str) {
        self.telemetry.count("logit.output.tags.dropped", 1.0, &[("reason", reason)]);
    }

    /// A tracer's `/v0.6/stats` body: one msgpack `ClientStatsPayload` from every event that is
    /// APM stats ([`super::is_datadog_stats`]); `None` when there is none.
    pub fn encode_client_stats_v06(&mut self, batch: &EventBatch) -> Option<Bytes> {
        let client = self.client_payload(batch, false)?;
        let mut w = Writer::new();
        write_client_payload(&mut w, &client);
        Some(Bytes::from(w.into_inner()))
    }

    /// The intake's `/api/v0.2/stats` body: one msgpack `StatsPayload` wrapping the batch's one
    /// `ClientStatsPayload`, its envelope from the batch resource; `None` when nothing is stats.
    pub fn encode_stats_payload(&mut self, batch: &EventBatch) -> Option<Bytes> {
        let client = self.client_payload(batch, true)?;
        let attrs = &batch.resource.attributes;
        let text = |key| self.carrier_str(attrs.get(key)).unwrap_or_default();
        let flag = |key| matches!(attrs.get(key), Some(Value::Bool(true)));
        let envelope = StatsPayload {
            agent_hostname: text(RESOURCE_ATTR_AGENT_HOSTNAME),
            agent_env: text(RESOURCE_ATTR_AGENT_ENV),
            stats: Vec::new(),
            agent_version: text(RESOURCE_ATTR_AGENT_VERSION),
            client_computed: flag(RESOURCE_ATTR_STATS_CLIENT_COMPUTED),
            split_payload: flag(RESOURCE_ATTR_STATS_SPLIT_PAYLOAD),
        };
        for key in [RESOURCE_ATTR_STATS_CLIENT_COMPUTED, RESOURCE_ATTR_STATS_SPLIT_PAYLOAD] {
            if matches!(attrs.get(key), Some(v) if !matches!(v, Value::Bool(_))) {
                self.tag_dropped("unrepresentable");
            }
        }
        let mut w = Writer::new();
        w.write_map_len(6);
        w.write_str("AgentHostname");
        w.write_str(&envelope.agent_hostname);
        w.write_str("AgentEnv");
        w.write_str(&envelope.agent_env);
        w.write_str("Stats");
        w.write_array_len(1);
        write_client_payload(&mut w, &client);
        w.write_str("AgentVersion");
        w.write_str(&envelope.agent_version);
        w.write_str("ClientComputed");
        w.write_bool(envelope.client_computed);
        w.write_str("SplitPayload");
        w.write_bool(envelope.split_payload);
        Some(Bytes::from(w.into_inner()))
    }

    /// The batch's stats events as one `ClientStatsPayload`, buckets rebuilt by grouping on
    /// `(timestamp, bucket.duration, bucket.agent_time_shift)` in first-seen order. `envelope`
    /// says whether the caller writes a `StatsPayload` envelope from the resource's
    /// `datadog.agent.*` fields; without one they have no wire form.
    fn client_payload(&mut self, batch: &EventBatch, envelope: bool) -> Option<ClientStatsPayload> {
        let resource = &*batch.resource;
        let mut buckets: Vec<ClientStatsBucket> = Vec::new();
        let mut index: HashMap<(i64, u64, i64), usize> = HashMap::new();
        for event in &batch.events {
            if !super::is_datadog_stats(resource, event) {
                continue;
            }
            let (group, duration, shift) = self.group(resource, event);
            let i = *index.entry((event.timestamp, duration, shift)).or_insert_with(|| {
                let start = u64::try_from(event.timestamp).unwrap_or_else(|_| {
                    self.stats_out_degraded("negative_timestamp");
                    0
                });
                buckets.push(ClientStatsBucket {
                    start,
                    duration,
                    stats: Vec::new(),
                    agent_time_shift: shift,
                });
                buckets.len() - 1
            });
            buckets[i].stats.push(group);
        }
        if buckets.is_empty() {
            return None;
        }
        let mut client = ClientStatsPayload { stats: buckets, ..Default::default() };
        let attrs = &resource.attributes;
        for key in PAYLOAD_STRINGS {
            if let Some(v) = self.carrier_str(attrs.get(key)) {
                *payload_string_mut(&mut client, key) = v;
            }
        }
        client.sequence = self.carrier_u64(attrs.get(RESOURCE_ATTR_STATS_SEQUENCE));
        client.process_tags_hash =
            self.carrier_u64(attrs.get(RESOURCE_ATTR_STATS_PROCESS_TAGS_HASH));
        client.tags = self.carrier_strings(attrs.get(RESOURCE_ATTR_STATS_TAGS));
        // A resource attribute with no field here, and not a group field every event falls back
        // to, has no wire form.
        for (key, _) in attrs.iter() {
            let key = resolve(key);
            if !is_group_key(key)
                && !PAYLOAD_STRINGS.contains(&key)
                && !PAYLOAD_SCALARS.contains(&key)
                && !(envelope && ENVELOPE_KEYS.contains(&key))
            {
                self.tag_dropped("no_wire_form");
            }
        }
        Some(client)
    }

    /// One event as a `ClientGroupedStats`, plus its bucket's duration and time shift. Group
    /// fields read from the event, falling back to the resource.
    fn group(&self, resource: &Resource, event: &Event) -> (ClientGroupedStats, u64, i64) {
        let mut g = ClientGroupedStats::default();
        for key in GROUP_STRINGS {
            if let Some(v) = self.carrier_str(merged_get(resource, event, key)) {
                *group_string_mut(&mut g, key) = v;
            }
        }
        for key in GROUP_ARRAYS {
            *group_array_mut(&mut g, key) = self.carrier_strings(merged_get(resource, event, key));
        }
        g.http_status_code = match merged_get(resource, event, ATTR_HTTP_STATUS_CODE) {
            None => 0,
            Some(v) => match as_u64(v).and_then(|v| u32::try_from(v).ok()) {
                Some(code) => code,
                None => {
                    self.tag_dropped("unrepresentable");
                    0
                }
            },
        };
        g.synthetics = match merged_get(resource, event, ATTR_SYNTHETICS) {
            None => false,
            Some(Value::Bool(b)) => *b,
            Some(_) => {
                self.tag_dropped("unrepresentable");
                false
            }
        };
        g.is_trace_root = match merged_get(resource, event, ATTR_IS_TRACE_ROOT) {
            None => 0,
            Some(v) => match v.as_str() {
                Some("true") => 1,
                Some("false") => 2,
                _ => {
                    self.tag_dropped("unrepresentable");
                    0
                }
            },
        };
        let duration = self.carrier_u64(merged_get(resource, event, ATTR_BUCKET_DURATION));
        let shift = match merged_get(resource, event, ATTR_BUCKET_AGENT_TIME_SHIFT) {
            None => 0,
            Some(v) => match as_i64(v) {
                Some(s) => s,
                None => {
                    self.tag_dropped("unrepresentable");
                    0
                }
            },
        };
        for (key, _) in event.attributes.iter() {
            if !is_group_key(resolve(key)) {
                self.tag_dropped("no_wire_form");
            }
        }

        for record in &event.metrics {
            let name = resolve(record.name);
            match (name, &record.kind) {
                (METRIC_HITS, MetricKind::Sum(s)) if is_delta(s) => g.hits = self.count(s.value),
                (METRIC_ERRORS, MetricKind::Sum(s)) if is_delta(s) => {
                    g.errors = self.count(s.value)
                }
                (METRIC_TOP_LEVEL_HITS, MetricKind::Sum(s)) if is_delta(s) => {
                    g.top_level_hits = self.count(s.value)
                }
                (METRIC_DURATION, MetricKind::Sum(s)) if is_delta(s) => {
                    g.duration = self.count(s.value)
                }
                (METRIC_OK_SUMMARY, MetricKind::Distribution(d)) => g.ok_summary = self.summary(d),
                (METRIC_ERROR_SUMMARY, MetricKind::Distribution(d)) => {
                    g.error_summary = self.summary(d)
                }
                _ => self.stats_out_skipped("unrecognized_record"),
            }
        }
        (g, duration, shift)
    }

    /// A weighted count back to the wire's `uint64`: rounded to the nearest integer, which is
    /// what the Agent's own stochastic rounding lands on on average.
    fn count(&self, v: f64) -> u64 {
        if !(v.is_finite() && v >= 0.0) {
            self.stats_out_degraded("bad_count");
            return 0;
        }
        // `u64::MAX as f64` is exactly 2^64, which is what a wire `u64::MAX` decodes to, so that
        // round trip stays silent; only a value strictly above it saturates lossily.
        if v > u64::MAX as f64 {
            self.stats_out_degraded("count_overflow");
            return u64::MAX;
        }
        let r = v.round();
        if r != v {
            self.stats_out_degraded("fractional_count");
        }
        r as u64
    }

    fn summary(&self, sketch: &DdSketch) -> Vec<u8> {
        let m = sketch.mapping();
        match m.kind() {
            MappingKind::Agent => self.stats_out_degraded("agent_mapping"),
            MappingKind::Logarithmic => {
                if m.bin_limit() != STATS_BIN_LIMIT {
                    self.stats_out_degraded("bin_limit");
                }
                if sketch.stats_exact() && sketch.count() > 0 {
                    self.stats_out_degraded("exact_summary");
                }
            }
        }
        encode_ddsketch(sketch)
    }

    fn carrier_str(&self, v: Option<&Value>) -> Option<String> {
        match v? {
            v @ Value::Str(_) => v.as_str().map(str::to_owned),
            _ => {
                self.tag_dropped("unrepresentable");
                None
            }
        }
    }

    fn carrier_u64(&self, v: Option<&Value>) -> u64 {
        match v {
            None => 0,
            Some(v) => as_u64(v).unwrap_or_else(|| {
                self.tag_dropped("unrepresentable");
                0
            }),
        }
    }

    fn carrier_strings(&self, v: Option<&Value>) -> Vec<String> {
        match v {
            None => Vec::new(),
            Some(Value::Array(items)) => items
                .iter()
                .filter_map(|item| {
                    let s = item.as_str().map(str::to_owned);
                    if s.is_none() {
                        self.tag_dropped("unrepresentable");
                    }
                    s
                })
                .collect(),
            Some(_) => {
                self.tag_dropped("unrepresentable");
                Vec::new()
            }
        }
    }
}

fn is_delta(s: &Sum) -> bool {
    s.temporality == Temporality::Delta
}

fn is_group_key(key: &str) -> bool {
    GROUP_STRINGS.contains(&key) || GROUP_ARRAYS.contains(&key) || GROUP_SCALARS.contains(&key)
}

fn as_u64(v: &Value) -> Option<u64> {
    match v {
        Value::U64(u) => Some(*u),
        Value::I64(i) => u64::try_from(*i).ok(),
        _ => None,
    }
}

fn as_i64(v: &Value) -> Option<i64> {
    match v {
        Value::I64(i) => Some(*i),
        Value::U64(u) => i64::try_from(*u).ok(),
        _ => None,
    }
}

/// Every key, always, in the Go struct's field order, as msgp's `EncodeMsg` writes them.
fn write_client_payload(w: &mut Writer, p: &ClientStatsPayload) {
    w.write_map_len(16);
    for (key, v) in [("Hostname", &p.hostname), ("Env", &p.env), ("Version", &p.version)] {
        w.write_str(key);
        w.write_str(v);
    }
    w.write_str("Stats");
    w.write_array_len(p.stats.len());
    for b in &p.stats {
        write_bucket(w, b);
    }
    for (key, v) in
        [("Lang", &p.lang), ("TracerVersion", &p.tracer_version), ("RuntimeID", &p.runtime_id)]
    {
        w.write_str(key);
        w.write_str(v);
    }
    w.write_str("Sequence");
    w.write_u64(p.sequence);
    for (key, v) in [
        ("AgentAggregation", &p.agent_aggregation),
        ("Service", &p.service),
        ("ContainerID", &p.container_id),
    ] {
        w.write_str(key);
        w.write_str(v);
    }
    w.write_str("Tags");
    write_strings(w, &p.tags);
    for (key, v) in [("GitCommitSha", &p.git_commit_sha), ("ImageTag", &p.image_tag)] {
        w.write_str(key);
        w.write_str(v);
    }
    w.write_str("ProcessTagsHash");
    w.write_u64(p.process_tags_hash);
    w.write_str("ProcessTags");
    w.write_str(&p.process_tags);
}

fn write_bucket(w: &mut Writer, b: &ClientStatsBucket) {
    w.write_map_len(4);
    w.write_str("Start");
    w.write_u64(b.start);
    w.write_str("Duration");
    w.write_u64(b.duration);
    w.write_str("Stats");
    w.write_array_len(b.stats.len());
    for g in &b.stats {
        write_group(w, g);
    }
    w.write_str("AgentTimeShift");
    w.write_i64(b.agent_time_shift);
}

fn write_group(w: &mut Writer, g: &ClientGroupedStats) {
    w.write_map_len(22);
    for (key, v) in [("Service", &g.service), ("Name", &g.name), ("Resource", &g.resource)] {
        w.write_str(key);
        w.write_str(v);
    }
    w.write_str("HTTPStatusCode");
    w.write_u64(u64::from(g.http_status_code));
    for (key, v) in [("Type", &g.r#type), ("DBType", &g.db_type)] {
        w.write_str(key);
        w.write_str(v);
    }
    for (key, v) in [("Hits", g.hits), ("Errors", g.errors), ("Duration", g.duration)] {
        w.write_str(key);
        w.write_u64(v);
    }
    w.write_str("OkSummary");
    w.write_bin(&g.ok_summary);
    w.write_str("ErrorSummary");
    w.write_bin(&g.error_summary);
    w.write_str("Synthetics");
    w.write_bool(g.synthetics);
    w.write_str("TopLevelHits");
    w.write_u64(g.top_level_hits);
    w.write_str("SpanKind");
    w.write_str(&g.span_kind);
    w.write_str("PeerTags");
    write_strings(w, &g.peer_tags);
    w.write_str("IsTraceRoot");
    w.write_i64(i64::from(g.is_trace_root));
    for (key, v) in [
        ("GRPCStatusCode", &g.grpc_status_code),
        ("HTTPMethod", &g.http_method),
        ("HTTPEndpoint", &g.http_endpoint),
        ("srv_src", &g.service_source),
    ] {
        w.write_str(key);
        w.write_str(v);
    }
    w.write_str("SpanDerivedPrimaryTags");
    write_strings(w, &g.span_derived_primary_tags);
    w.write_str("AdditionalMetricTags");
    write_strings(w, &g.additional_metric_tags);
}

fn write_strings(w: &mut Writer, v: &[String]) {
    w.write_array_len(v.len());
    for s in v {
        w.write_str(s);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datadog::generated::ddsketch::IndexMapping;
    use logit_core::Registry;

    const TS: u64 = 1_700_000_000_000_000_000;
    const GAMMA: f64 = 1.02020202020202;

    fn counted(registry: &Registry, metric: &str, reason: &str) -> f64 {
        let mut total = 0.0;
        for event in registry.drain(0) {
            if event.attributes.get("reason").and_then(|v| v.as_str()) != Some(reason) {
                continue;
            }
            for m in &event.metrics {
                if let (true, MetricKind::Sum(s)) = (resolve(m.name) == metric, &m.kind) {
                    total += s.value;
                }
            }
        }
        total
    }

    fn decoder() -> (DatadogDecoder, Arc<Registry>) {
        let registry = Registry::new();
        let d = DatadogDecoder::new().with_telemetry(registry.telemetry_for(
            "datadog_in",
            "datadog_in",
            "listener",
        ));
        (d, registry)
    }

    fn encoder() -> (DatadogEncoder, Arc<Registry>) {
        let registry = Registry::new();
        let e = DatadogEncoder::new().with_telemetry(registry.telemetry_for(
            "datadog_out",
            "datadog_out",
            "sink",
        ));
        (e, registry)
    }

    fn pb_sketch(
        interpolation: Interpolation,
        positive: Store,
        negative: Store,
        zero: f64,
    ) -> Vec<u8> {
        PbSketch {
            mapping: Some(IndexMapping {
                gamma: GAMMA,
                index_offset: 0.0,
                interpolation: interpolation as i32,
            }),
            positive_values: Some(positive),
            negative_values: Some(negative),
            zero_count: zero,
        }
        .encode_to_vec()
    }

    fn sparse(bins: &[(i32, f64)]) -> Store {
        Store { bin_counts: bins.iter().copied().collect(), ..Store::default() }
    }

    fn log_mapping() -> Mapping {
        Mapping::logarithmic(GAMMA, 0.0, STATS_BIN_LIMIT)
    }

    #[test]
    fn ddsketch_sparse_bins_decode_under_the_wire_mapping() {
        let bytes =
            pb_sketch(Interpolation::None, sparse(&[(5, 2.0), (-3, 1.5)]), Store::default(), 0.0);
        let s = decode_ddsketch(&bytes).unwrap();
        assert_eq!(*s.mapping(), log_mapping());
        assert_eq!(s.positive_bins(), &[Bin { key: -3, count: 1.5 }, Bin { key: 5, count: 2.0 }]);
        assert!(s.negative_bins().is_empty());
        assert!(!s.stats_exact(), "the protobuf carries no summary");
    }

    #[test]
    fn ddsketch_contiguous_bins_start_at_the_offset_and_sum_with_sparse_ones() {
        let store = Store {
            bin_counts: [(11, 4.0)].into_iter().collect(),
            contiguous_bin_counts: vec![1.0, 0.0, 3.0],
            contiguous_bin_index_offset: 10,
        };
        let s =
            decode_ddsketch(&pb_sketch(Interpolation::None, store, Store::default(), 0.0)).unwrap();
        assert_eq!(
            s.positive_bins(),
            &[
                Bin { key: 10, count: 1.0 },
                Bin { key: 11, count: 4.0 },
                Bin { key: 12, count: 3.0 }
            ]
        );
        let both = Store {
            bin_counts: [(10, 4.0)].into_iter().collect(),
            contiguous_bin_counts: vec![1.0],
            contiguous_bin_index_offset: 10,
        };
        let s =
            decode_ddsketch(&pb_sketch(Interpolation::None, both, Store::default(), 0.0)).unwrap();
        assert_eq!(s.positive_bins(), &[Bin { key: 10, count: 5.0 }]);
    }

    #[test]
    fn ddsketch_negative_store_and_zero_count() {
        let bytes = pb_sketch(Interpolation::None, Store::default(), sparse(&[(7, 2.0)]), 3.0);
        let s = decode_ddsketch(&bytes).unwrap();
        assert_eq!(s.negative_bins(), &[Bin { key: 7, count: 2.0 }]);
        assert_eq!(s.zero_count(), 3.0);
        assert_eq!(s.count(), 5);
        assert!(s.min().unwrap() < 0.0);
        assert_eq!(s.max(), Some(0.0));
    }

    #[test]
    fn ddsketch_interpolated_mapping_is_refused() {
        for i in [Interpolation::Linear, Interpolation::Quadratic, Interpolation::Cubic] {
            let bytes = pb_sketch(i, sparse(&[(1, 1.0)]), Store::default(), 0.0);
            assert_eq!(decode_ddsketch(&bytes), Err(SketchError::Interpolation));
        }
    }

    #[test]
    fn ddsketch_without_a_usable_mapping_is_malformed() {
        let no_mapping = PbSketch { zero_count: 1.0, ..PbSketch::default() }.encode_to_vec();
        assert!(matches!(decode_ddsketch(&no_mapping), Err(SketchError::Malformed(_))));
        let bad_gamma = PbSketch {
            mapping: Some(IndexMapping { gamma: 0.5, ..IndexMapping::default() }),
            ..PbSketch::default()
        }
        .encode_to_vec();
        assert!(matches!(decode_ddsketch(&bad_gamma), Err(SketchError::Malformed(_))));
        assert!(matches!(decode_ddsketch(&[0xff, 0xff]), Err(SketchError::Malformed(_))));
        let overflow = Store {
            contiguous_bin_counts: vec![1.0, 1.0],
            contiguous_bin_index_offset: i32::MAX,
            ..Store::default()
        };
        let bytes = pb_sketch(Interpolation::None, overflow, Store::default(), 0.0);
        assert!(matches!(decode_ddsketch(&bytes), Err(SketchError::Malformed(_))));
    }

    #[test]
    fn ddsketch_encode_is_deterministic_and_decodes_to_the_same_sketch() {
        let mut s = DdSketch::with_mapping(log_mapping());
        for v in [-40.0, -3.5, 0.0, 1e-3, 1.0, 2.5, 250.0, 1e9] {
            s.add_count(v, 1.5);
        }
        let s = decode_ddsketch(&encode_ddsketch(&s)).unwrap();
        let bytes = encode_ddsketch(&s);
        assert_eq!(decode_ddsketch(&bytes).unwrap(), s);
        assert_eq!(encode_ddsketch(&decode_ddsketch(&bytes).unwrap()), bytes);
        // prost reads the hand-encoded form back as the same message.
        let pb = PbSketch::decode(&bytes[..]).unwrap();
        let m = pb.mapping.unwrap();
        assert_eq!((m.gamma, m.index_offset, m.interpolation), (GAMMA, 0.0, 0));
        assert_eq!(pb.zero_count, s.zero_count());
        assert_eq!(pb.positive_values.unwrap().bin_counts.len(), s.positive_bins().len());
        assert_eq!(pb.negative_values.unwrap().bin_counts.len(), s.negative_bins().len());
    }

    #[test]
    fn ddsketch_agent_mapping_goes_out_with_the_half_offset() {
        let mut s = DdSketch::new();
        s.add(12.0);
        let pb = PbSketch::decode(&encode_ddsketch(&s)[..]).unwrap();
        let m = pb.mapping.unwrap();
        assert_eq!(m.gamma, Mapping::AGENT_GAMMA);
        assert_eq!(m.index_offset, Mapping::agent().index_offset() + 0.5);
        let back = decode_ddsketch(&encode_ddsketch(&s)).unwrap();
        assert_eq!(back.positive_bins(), s.positive_bins(), "keys unchanged");
        // The same value keys the same under both readings, away from a tie.
        let mut relogged = DdSketch::with_mapping(*back.mapping());
        relogged.add(12.0);
        assert_eq!(relogged.positive_bins()[0].key, s.positive_bins()[0].key);
    }

    fn full_group() -> ClientGroupedStats {
        let mut ok = DdSketch::with_mapping(log_mapping());
        ok.add(1_000.0);
        let mut err = DdSketch::with_mapping(log_mapping());
        err.add(5_000.0);
        ClientGroupedStats {
            service: "web".into(),
            name: "http.request".into(),
            resource: "GET /".into(),
            http_status_code: 200,
            r#type: "web".into(),
            db_type: "postgres".into(),
            hits: 10,
            errors: 2,
            duration: 12_345,
            ok_summary: encode_ddsketch(&ok),
            error_summary: encode_ddsketch(&err),
            synthetics: true,
            top_level_hits: 7,
            span_kind: "server".into(),
            peer_tags: vec!["peer.service:db".into()],
            is_trace_root: 1,
            grpc_status_code: "0".into(),
            http_method: "GET".into(),
            http_endpoint: "/".into(),
            service_source: "opt.service_mapping".into(),
            span_derived_primary_tags: vec!["region:eu".into()],
            additional_metric_tags: vec!["team:a".into()],
        }
    }

    fn client(groups: Vec<ClientGroupedStats>) -> ClientStatsPayload {
        ClientStatsPayload {
            hostname: "host1".into(),
            env: "prod".into(),
            version: "1.2.3".into(),
            stats: vec![ClientStatsBucket {
                start: TS,
                duration: 10_000_000_000,
                stats: groups,
                agent_time_shift: -5,
            }],
            lang: "go".into(),
            tracer_version: "v1.60.0".into(),
            runtime_id: "rid".into(),
            sequence: 42,
            agent_aggregation: "counts".into(),
            service: "web".into(),
            container_id: "cid".into(),
            tags: vec!["a:b".into()],
            git_commit_sha: "abc".into(),
            image_tag: "v9".into(),
            process_tags_hash: 99,
            process_tags: "entrypoint.name:web".into(),
        }
    }

    fn body(p: &ClientStatsPayload) -> Vec<u8> {
        let mut w = Writer::new();
        write_client_payload(&mut w, p);
        w.into_inner()
    }

    fn strs(v: &[&str]) -> Value {
        Value::Array(v.iter().map(|s| Value::str(*s)).collect())
    }

    #[test]
    fn every_group_field_lands_on_the_event() {
        let batch =
            decoder().0.decode_client_stats_v06(&body(&client(vec![full_group()])), 0).unwrap();
        assert_eq!(batch.events.len(), 1);
        let e = &batch.events[0];
        assert_eq!(e.timestamp, TS as i64);
        let a = &e.attributes;
        let s = |k| a.get(k).and_then(Value::as_str);
        assert_eq!(s(ATTR_SERVICE_NAME), Some("web"));
        assert_eq!(s(ATTR_RESOURCE_NAME), Some("GET /"));
        assert_eq!(s(ATTR_SPAN_TYPE), Some("web"));
        assert_eq!(s(ATTR_SPAN_KIND), Some("server"));
        assert_eq!(s(ATTR_STATS_NAME), Some("http.request"));
        assert_eq!(s(ATTR_DB_TYPE), Some("postgres"));
        assert_eq!(s(ATTR_GRPC_STATUS_CODE), Some("0"));
        assert_eq!(s(ATTR_HTTP_METHOD), Some("GET"));
        assert_eq!(s(ATTR_HTTP_ENDPOINT), Some("/"));
        assert_eq!(s(ATTR_SERVICE_SOURCE), Some("opt.service_mapping"));
        assert_eq!(s(ATTR_IS_TRACE_ROOT), Some("true"));
        assert_eq!(a.get(ATTR_HTTP_STATUS_CODE), Some(&Value::U64(200)));
        assert_eq!(a.get(ATTR_SYNTHETICS), Some(&Value::Bool(true)));
        assert_eq!(a.get(ATTR_PEER_TAGS), Some(&strs(&["peer.service:db"])));
        assert_eq!(a.get(ATTR_ADDITIONAL_METRIC_TAGS), Some(&strs(&["team:a"])));
        assert_eq!(a.get(ATTR_SPAN_DERIVED_PRIMARY_TAGS), Some(&strs(&["region:eu"])));
        assert_eq!(a.get(ATTR_BUCKET_DURATION), Some(&Value::U64(10_000_000_000)));
        assert_eq!(a.get(ATTR_BUCKET_AGENT_TIME_SHIFT), Some(&Value::I64(-5)));
        let names: Vec<&str> = e.metrics.iter().map(|m| resolve(m.name)).collect();
        assert_eq!(
            names,
            [
                METRIC_HITS,
                METRIC_ERRORS,
                METRIC_TOP_LEVEL_HITS,
                METRIC_DURATION,
                METRIC_OK_SUMMARY,
                METRIC_ERROR_SUMMARY
            ]
        );
        assert_eq!(e.metrics[0].kind, MetricKind::counter(10.0));
        assert_eq!(e.metrics[1].kind, MetricKind::counter(2.0));
        assert_eq!(e.metrics[2].kind, MetricKind::counter(7.0));
        assert_eq!(e.metrics[3].kind, MetricKind::counter(12_345.0));
        assert_eq!(e.metrics[3].unit.map(resolve), Some("ns"));
        let MetricKind::Distribution(ok) = &e.metrics[4].kind else { panic!("ok summary") };
        assert_eq!(*ok.mapping(), log_mapping());
        assert_eq!(ok.count(), 1);
    }

    #[test]
    fn every_payload_field_lands_on_the_resource() {
        let batch =
            decoder().0.decode_client_stats_v06(&body(&client(vec![full_group()])), 0).unwrap();
        let a = &batch.resource.attributes;
        let s = |k| a.get(k).and_then(Value::as_str);
        assert_eq!(s(RESOURCE_ATTR_TRACER_HOSTNAME), Some("host1"));
        assert_eq!(s(RESOURCE_ATTR_TRACER_ENV), Some("prod"));
        assert_eq!(s(RESOURCE_ATTR_TRACER_APP_VERSION), Some("1.2.3"));
        assert_eq!(s(RESOURCE_ATTR_TRACER_LANGUAGE_NAME), Some("go"));
        assert_eq!(s(RESOURCE_ATTR_TRACER_VERSION), Some("v1.60.0"));
        assert_eq!(s(RESOURCE_ATTR_TRACER_RUNTIME_ID), Some("rid"));
        assert_eq!(s(RESOURCE_ATTR_TRACER_CONTAINER_ID), Some("cid"));
        assert_eq!(s(RESOURCE_ATTR_STATS_AGENT_AGGREGATION), Some("counts"));
        assert_eq!(s(RESOURCE_ATTR_STATS_SERVICE), Some("web"));
        assert_eq!(s(RESOURCE_ATTR_STATS_GIT_COMMIT_SHA), Some("abc"));
        assert_eq!(s(RESOURCE_ATTR_STATS_IMAGE_TAG), Some("v9"));
        assert_eq!(s(RESOURCE_ATTR_STATS_PROCESS_TAGS), Some("entrypoint.name:web"));
        assert_eq!(a.get(RESOURCE_ATTR_STATS_SEQUENCE), Some(&Value::U64(42)));
        assert_eq!(a.get(RESOURCE_ATTR_STATS_PROCESS_TAGS_HASH), Some(&Value::U64(99)));
        assert_eq!(a.get(RESOURCE_ATTR_STATS_TAGS), Some(&strs(&["a:b"])));
    }

    #[test]
    fn zero_and_empty_fields_are_omitted_but_the_name_is_kept() {
        let p = ClientStatsPayload {
            stats: vec![ClientStatsBucket {
                start: TS,
                duration: 0,
                stats: vec![ClientGroupedStats::default()],
                agent_time_shift: 0,
            }],
            ..Default::default()
        };
        let batch = decoder().0.decode_client_stats_v06(&body(&p), 0).unwrap();
        assert!(batch.resource.attributes.is_empty());
        let a = &batch.events[0].attributes;
        assert_eq!(a.get(ATTR_STATS_NAME), Some(&Value::str("")));
        assert_eq!(a.get(ATTR_BUCKET_DURATION), Some(&Value::U64(0)));
        assert_eq!(a.len(), 2, "{a:?}");
        assert_eq!(batch.events[0].metrics.len(), 4, "no summaries for empty bytes");
    }

    #[test]
    fn is_trace_root_false_and_an_unknown_trilean() {
        let (mut d, registry) = decoder();
        let mut g = ClientGroupedStats { is_trace_root: 2, ..ClientGroupedStats::default() };
        let batch = d.decode_client_stats_v06(&body(&client(vec![g.clone()])), 0).unwrap();
        assert_eq!(batch.events[0].attributes.get(ATTR_IS_TRACE_ROOT), Some(&Value::str("false")));
        g.is_trace_root = 7;
        let batch = d.decode_client_stats_v06(&body(&client(vec![g])), 0).unwrap();
        assert_eq!(batch.events[0].attributes.get(ATTR_IS_TRACE_ROOT), None);
        assert_eq!(counted(&registry, "logit.input.stats.degraded", "unknown_trilean"), 1.0);
    }

    #[test]
    fn a_bad_or_interpolated_summary_drops_only_that_record() {
        let (mut d, registry) = decoder();
        let linear = pb_sketch(Interpolation::Linear, sparse(&[(1, 1.0)]), Store::default(), 0.0);
        let g = ClientGroupedStats {
            name: "n".into(),
            ok_summary: vec![0xff, 0xff],
            error_summary: linear,
            ..ClientGroupedStats::default()
        };
        let batch = d.decode_client_stats_v06(&body(&client(vec![g])), 0).unwrap();
        assert_eq!(batch.events[0].metrics.len(), 4);
        let drained = registry.drain(0);
        let count = |reason: &str| {
            drained
                .iter()
                .filter(|e| e.attributes.get("reason").and_then(Value::as_str) == Some(reason))
                .count()
        };
        assert_eq!(count("bad_sketch"), 1);
        assert_eq!(count("interpolation"), 1);
    }

    /// A hand-written v0.6 body: keys out of order, an unknown key, `nil`s for zero values, a
    /// malformed group between two good ones, and an empty bucket.
    fn tolerant_body() -> Vec<u8> {
        let mut w = Writer::new();
        w.write_map_len(4);
        w.write_str("Unknown");
        w.write_array_len(2);
        w.write_u64(1);
        w.write_str("x");
        w.write_str("Stats");
        w.write_array_len(2);
        // Bucket 1: three groups, the middle one malformed.
        w.write_map_len(3);
        w.write_str("Stats");
        w.write_array_len(3);
        w.write_map_len(3);
        w.write_str("Hits");
        w.write_u64(3);
        w.write_str("Name");
        w.write_str("a");
        w.write_str("Service");
        w.write_nil();
        w.write_map_len(1);
        w.write_str("Hits");
        w.write_str("not a number");
        w.write_map_len(1);
        w.write_str("Name");
        w.write_str("c");
        w.write_str("Start");
        w.write_i64(1_000);
        w.write_str("Duration");
        w.write_u64(10);
        // Bucket 2: no groups.
        w.write_map_len(1);
        w.write_str("Start");
        w.write_u64(2_000);
        w.write_str("Env");
        w.write_nil();
        w.write_str("Hostname");
        w.write_str("h");
        w.into_inner()
    }

    #[test]
    fn a_malformed_group_is_dropped_and_the_rest_decodes() {
        let (mut d, registry) = decoder();
        let batch = d.decode_client_stats_v06(&tolerant_body(), 0).unwrap();
        let names: Vec<_> = batch
            .events
            .iter()
            .map(|e| e.attributes.get(ATTR_STATS_NAME).unwrap().clone())
            .collect();
        assert_eq!(names, [Value::str("a"), Value::str("c")]);
        assert_eq!(batch.events[0].timestamp, 1_000);
        assert_eq!(batch.events[0].metrics[0].kind, MetricKind::counter(3.0));
        assert_eq!(batch.events[0].attributes.get(ATTR_SERVICE_NAME), None, "nil is empty");
        assert_eq!(
            batch.resource.attributes.get(RESOURCE_ATTR_TRACER_HOSTNAME),
            Some(&Value::str("h"))
        );
        let drained = registry.drain(0);
        let reasons: Vec<_> = drained
            .iter()
            .filter_map(|e| e.attributes.get("reason").and_then(Value::as_str).map(str::to_owned))
            .collect();
        assert!(reasons.contains(&"malformed_group".to_string()), "{reasons:?}");
        assert!(reasons.contains(&"empty_bucket".to_string()), "{reasons:?}");
    }

    #[test]
    fn an_unparseable_body_is_malformed() {
        let mut d = DatadogDecoder::new();
        assert!(matches!(
            d.decode_client_stats_v06(&[0x92, 0x01], 0),
            Err(CodecError::Malformed(_))
        ));
        let mut truncated = tolerant_body();
        truncated.truncate(truncated.len() - 3);
        assert!(d.decode_client_stats_v06(&truncated, 0).is_err());
        assert!(d.decode_stats_payload(b"", 0).is_err());
        // A payload-level field of the wrong type fails the one-payload route.
        let mut w = Writer::new();
        w.write_map_len(1);
        w.write_str("Sequence");
        w.write_str("x");
        assert!(d.decode_client_stats_v06(w.as_slice(), 0).is_err());
    }

    #[test]
    fn stats_payload_envelope_rides_on_every_batch() {
        let mut w = Writer::new();
        w.write_map_len(6);
        w.write_str("AgentHostname");
        w.write_str("agent-host");
        w.write_str("AgentEnv");
        w.write_str("staging");
        w.write_str("AgentVersion");
        w.write_str("7.83.3");
        w.write_str("ClientComputed");
        w.write_bool(true);
        w.write_str("SplitPayload");
        w.write_bool(true);
        w.write_str("Stats");
        w.write_array_len(3);
        write_client_payload(&mut w, &client(vec![full_group()]));
        w.write_str("not a payload");
        write_client_payload(
            &mut w,
            &ClientStatsPayload { hostname: "h2".into(), ..Default::default() },
        );
        let (mut d, registry) = decoder();
        let batches = d.decode_stats_payload(w.as_slice(), 0).unwrap();
        assert_eq!(batches.len(), 2);
        for b in &batches {
            let a = &b.resource.attributes;
            assert_eq!(a.get(RESOURCE_ATTR_AGENT_HOSTNAME), Some(&Value::str("agent-host")));
            assert_eq!(a.get(RESOURCE_ATTR_AGENT_ENV), Some(&Value::str("staging")));
            assert_eq!(a.get(RESOURCE_ATTR_AGENT_VERSION), Some(&Value::str("7.83.3")));
            assert_eq!(a.get(RESOURCE_ATTR_STATS_CLIENT_COMPUTED), Some(&Value::Bool(true)));
            assert_eq!(a.get(RESOURCE_ATTR_STATS_SPLIT_PAYLOAD), Some(&Value::Bool(true)));
        }
        assert_eq!(counted(&registry, "logit.input.stats.skipped", "malformed_payload"), 1.0);
        let out = DatadogEncoder::new().encode_stats_payload(&batches[0]).unwrap();
        let again = DatadogDecoder::new().decode_stats_payload(&out, 0).unwrap();
        assert_eq!(again, batches[..1]);
    }

    #[test]
    fn encode_rebuilds_the_wire_payload() {
        let p = client(vec![
            full_group(),
            ClientGroupedStats { name: "b".into(), ..Default::default() },
        ]);
        let batch = DatadogDecoder::new().decode_client_stats_v06(&body(&p), 0).unwrap();
        let out = DatadogEncoder::new().encode_client_stats_v06(&batch).unwrap();
        assert_eq!(&out[..], &body(&p)[..], "every field, in the Go field order");
    }

    #[test]
    fn buckets_regroup_by_start_duration_and_shift() {
        let mut p = client(vec![full_group()]);
        p.stats.push(ClientStatsBucket {
            start: TS + 10,
            duration: 10,
            stats: vec![ClientGroupedStats { name: "x".into(), ..Default::default() }],
            agent_time_shift: 0,
        });
        let mut batch = DatadogDecoder::new().decode_client_stats_v06(&body(&p), 0).unwrap();
        // Bucket 2's group first, then bucket 1's: one bucket per key, in first-seen order.
        batch.events.reverse();
        let out = DatadogEncoder::new().encode_client_stats_v06(&batch).unwrap();
        let again = DatadogDecoder::new().decode_client_stats_v06(&out, 0).unwrap();
        assert_eq!(again.events, batch.events);
    }

    fn stats_batch(metrics: Vec<MetricRecord>, extra: &[(&str, Value)]) -> EventBatch {
        let mut attrs = AttrMap::new();
        attrs.insert(ATTR_STATS_NAME, "op");
        for (k, v) in extra {
            attrs.insert(k, v.clone());
        }
        let mut metrics = metrics.into_iter();
        let mut event = Event::metric(TS as i64, attrs, metrics.next().unwrap());
        event.metrics.extend(metrics);
        EventBatch { resource: Arc::new(Resource::default()), scope: None, events: vec![event] }
    }

    #[test]
    fn encode_counts_what_it_cannot_carry() {
        let (mut e, registry) = encoder();
        let mut agent = DdSketch::new();
        agent.add(3.0);
        let batch = stats_batch(
            vec![
                MetricRecord::new(intern(METRIC_HITS), MetricKind::counter(2.5)),
                MetricRecord::new(intern(METRIC_ERRORS), MetricKind::counter(-1.0)),
                MetricRecord::new(intern(METRIC_TOP_LEVEL_HITS), MetricKind::counter(2e19)),
                // Exactly 2^64, what a wire `u64::MAX` decodes to: saturates without a count.
                MetricRecord::new(intern(METRIC_DURATION), MetricKind::counter(u64::MAX as f64)),
                MetricRecord::new(intern("other"), MetricKind::Gauge(1.0)),
                MetricRecord::new(intern(METRIC_OK_SUMMARY), MetricKind::Distribution(agent)),
            ],
            &[("env", Value::str("prod")), (ATTR_HTTP_STATUS_CODE, Value::str("200"))],
        );
        let out = e.encode_client_stats_v06(&batch).unwrap();
        let drained = registry.drain(0);
        let n = |metric: &str, reason: &str| {
            drained
                .iter()
                .filter(|ev| ev.attributes.get("reason").and_then(Value::as_str) == Some(reason))
                .flat_map(|ev| ev.metrics.iter())
                .filter(|m| resolve(m.name) == metric)
                .map(|m| match m.kind {
                    MetricKind::Sum(s) => s.value,
                    _ => 0.0,
                })
                .sum::<f64>()
        };
        assert_eq!(n("logit.output.stats.degraded", "fractional_count"), 1.0);
        assert_eq!(n("logit.output.stats.degraded", "bad_count"), 1.0);
        assert_eq!(n("logit.output.stats.degraded", "count_overflow"), 1.0, "2e19 only");
        assert_eq!(n("logit.output.stats.degraded", "agent_mapping"), 1.0);
        assert_eq!(n("logit.output.stats.skipped", "unrecognized_record"), 1.0);
        assert_eq!(n("logit.output.tags.dropped", "no_wire_form"), 1.0);
        assert_eq!(n("logit.output.tags.dropped", "unrepresentable"), 1.0);
        let back = DatadogDecoder::new().decode_client_stats_v06(&out, 0).unwrap();
        let m = &back.events[0].metrics;
        assert_eq!(m[0].kind, MetricKind::counter(3.0), "2.5 rounds half away from zero");
        assert_eq!(m[1].kind, MetricKind::counter(0.0));
        let by_name = |name: &str| m.iter().find(|r| resolve(r.name) == name).unwrap().kind.clone();
        assert_eq!(by_name(METRIC_TOP_LEVEL_HITS), MetricKind::counter(u64::MAX as f64));
        assert_eq!(by_name(METRIC_DURATION), MetricKind::counter(u64::MAX as f64));

        // A wire `u64::MAX` decodes to 2^64 and re-encodes to `u64::MAX` without a count.
        let (mut e2, registry2) = encoder();
        let out2 = e2.encode_client_stats_v06(&back).unwrap();
        let overflowed = registry2
            .drain(0)
            .iter()
            .filter(|ev| {
                ev.attributes.get("reason").and_then(Value::as_str) == Some("count_overflow")
            })
            .count();
        assert_eq!(overflowed, 0);
        let again = DatadogDecoder::new().decode_client_stats_v06(&out2, 0).unwrap();
        assert_eq!(again.events[0].metrics, back.events[0].metrics);
    }

    #[test]
    fn nothing_to_send_is_none() {
        let mut e = DatadogEncoder::new();
        let empty =
            EventBatch { resource: Arc::new(Resource::default()), scope: None, events: vec![] };
        assert!(e.encode_client_stats_v06(&empty).is_none());
        assert!(e.encode_stats_payload(&empty).is_none());
        let plain = Event::metric(
            1,
            AttrMap::new(),
            MetricRecord::new(intern("m"), MetricKind::Gauge(1.0)),
        );
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![plain],
        };
        assert!(e.encode_client_stats_v06(&batch).is_none());
    }
}
