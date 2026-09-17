//! The flat-sample assembler: suffixed sample names (`x_bucket{le=…}`, `x_sum`, `x_count`,
//! `x_created`) back into [`MetricFamily`]s. Syntax-independent, like [`super`]'s model mapping and
//! unlike [`super::text`] -- nothing here knows what a line, a label ref or a protobuf field looks
//! like.
//!
//! **Why this is its own module.** Prometheus' wire shapes are *flat*: every format the project
//! speaks -- text 0.0.4, OpenMetrics, and remote-write 1.0/2.0 alike -- carries a histogram as a
//! handful of independently-named series and leaves it to the reader to notice that
//! `x_bucket`/`x_sum`/`x_count` are one metric. That reassembly is the same problem in all three,
//! and getting it wrong in one of them but not the others would be an invisible divergence in a
//! mapping table [`super`]'s module doc claims is shared. So it lives once, here, and both syntax
//! modules feed it [`Sample`]s.
//!
//! ## What an assembler decides
//!
//! | Input | Decision |
//! |---|---|
//! | a sample name that *is* a declared family's name | that family, in the role its type gives a bare name ([`bare_name_role`]): a counter/gauge/stateset/unknown value, a summary's quantile line. A histogram has no bare-named sample, so this is `skipped{reason="unknown_suffix"}` |
//! | a sample name that is a declared family's name plus a suffix that type gives meaning to ([`SUFFIXES`], [`suffix_applies`]) | that family, in the suffix's role |
//! | a sample name that is a declared family's name plus a suffix that type has *no* meaning for (`x_sum` under `# TYPE x counter`) | `skipped{reason="unknown_suffix"}` |
//! | any other sample name | a fresh family of the assembler's *implicit* type -- [`FamilyType::Untyped`] for text 0.0.4, [`FamilyType::Unknown`] for OpenMetrics and remote-write |
//! | `le`/`quantile` | part of the [`Point`], not of the series identity: stripped from the label set before it becomes the series key |
//! | a second value for one (family, label set, role) | `skipped{reason="duplicate_series"}`, first wins ([`replace_once`]) |
//! | a label set naming one label twice | `skipped{reason="duplicate_label"}` -- an invalid label set, so the whole sample goes |
//! | a series whose samples don't add up to its type's value (a counter with only a `_created`, a histogram with no buckets) | `skipped{reason="incomplete_series"}` at [`Assembler::finish`] |
//!
//! Declarations ([`Assembler::declare_type`]/[`Assembler::declare_help`]/[`Assembler::declare_unit`])
//! may arrive in any order and either side of the samples they describe -- a `# TYPE` for an
//! already-implicit family retypes it in place, which is the leniency [`super::text`]'s module doc
//! promises. Samples already *routed* into another family are not re-homed by a late declaration;
//! they stay where they landed. A second declaration that *conflicts* with the first is
//! `skipped{reason="duplicate_type"|"duplicate_metadata"}` and the first wins; a second that says
//! the same thing is neither counted nor an error, since every format lets a producer repeat
//! itself and a counter an operator reads as "input was dropped" should not fire when nothing was.
//!
//! ## Declaring lazily
//!
//! A transport that carries a request's metadata separately from its samples (remote-write) can
//! hand over a whole [`Declarations`] table up front with [`Assembler::with_declarations`], and
//! several assemblers can share one by reference. A declared family is then materialized **only
//! when a sample name actually routes to it** -- one hash lookup per suffix on the miss path in
//! [`Assembler::route`], and nothing at all for a family nobody sampled.
//!
//! That laziness is a bound, not a micro-optimization. Remote-write decodes into one assembler per
//! distinct sample timestamp, and both the timestamp count and the declaration count come off the
//! wire; replaying every declaration into every group would let a small compressed body ask for
//! `groups x declarations` accumulators, each with an owned name, a `Vec` and a `HashMap` that live
//! until [`Assembler::finish`]. Declaring on demand makes the cost proportional to the samples the
//! request actually carries.
//!
//! ## What an assembler does *not* decide
//!
//! Anything a dialect or a transport owns: how a body is tokenized, whether a name is well-formed
//! for its syntax, what a timestamp's unit is, whether a `# EOF` is required, and -- the one that
//! needed care to split -- how a `_created` sample's *instant* is read. That value is a timestamp,
//! and reading it back out of the already-parsed `f64` would have rounded it at epoch magnitude
//! (19 significant digits against an `f64`'s 15-16), so [`Sample::value_text`] carries the source
//! token and [`parse_created_seconds`] reads it digit by digit. A transport whose created
//! timestamps arrive in their own integer field -- remote-write 2.0's `Sample.start_timestamp` --
//! has no such token, passes `None`, and hands the instant over through
//! [`Assembler::push_created`] instead.
//!
//! Four further entry points exist for facts a transport carries in a field of its own rather than
//! as a suffixed sample: [`Assembler::push_created`], [`Assembler::push_stale`] (remote-write's
//! stale-marker NaN, which is a property of the *series* rather than a value),
//! [`Assembler::push_exemplar`] and [`Assembler::describe`]. The first two route by sample
//! name exactly as [`Assembler::push`] does, so they reach the same series the samples did; the
//! last two route over what *already exists* and create nothing, because an exemplar or a help
//! string is a fact about a series, not a reason for one to exist.
//!
//! [`Point`]: super::Point

use super::{FamilyType, MetricFamily, Point, PrometheusDecoder, Series};
use logit_core::trace::{parse_span_id, parse_trace_id};
use logit_core::{parse_decimal_nanos, AttrMap, Exemplar, TraceRef, Value};
use std::collections::HashMap;

/// Which sample of a family a name carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    /// The family's own value sample: a counter's `_total`, a gauge/unknown/info/stateset line.
    Primary,
    Bucket,
    Sum,
    Count,
    Created,
    Quantile,
}

/// Every suffix any Prometheus format gives meaning to. Order matters only in that a longer suffix
/// must be tried before a shorter one it ends with; none of these overlap that way today.
const SUFFIXES: [(&str, Role); 8] = [
    ("_total", Role::Primary),
    ("_info", Role::Primary),
    ("_created", Role::Created),
    ("_bucket", Role::Bucket),
    ("_gcount", Role::Count),
    ("_gsum", Role::Sum),
    ("_count", Role::Count),
    ("_sum", Role::Sum),
];

/// Whether `role` means anything for a family of type `kind`, and -- for a `_gsum`/`_gcount` vs
/// `_sum`/`_count` pair -- which spelling that type uses.
fn suffix_applies(kind: FamilyType, suffix: &str, role: Role) -> bool {
    match role {
        Role::Primary => match kind {
            FamilyType::Counter => suffix == "_total",
            FamilyType::Info => suffix == "_info",
            _ => false,
        },
        Role::Created => kind.has_created(),
        Role::Bucket => matches!(kind, FamilyType::Histogram | FamilyType::GaugeHistogram),
        Role::Sum | Role::Count => match kind {
            FamilyType::Histogram | FamilyType::Summary => suffix == "_sum" || suffix == "_count",
            FamilyType::GaugeHistogram => suffix == "_gsum" || suffix == "_gcount",
            _ => false,
        },
        Role::Quantile => false,
    }
}

/// The role a bare family name plays for its own type: a summary's quantile lines and a stateset's
/// state lines are named exactly the family name, a histogram's samples never are.
fn bare_name_role(kind: FamilyType) -> Option<Role> {
    match kind {
        FamilyType::Counter
        | FamilyType::Gauge
        | FamilyType::StateSet
        | FamilyType::Unknown
        | FamilyType::Untyped => Some(Role::Primary),
        FamilyType::Summary => Some(Role::Quantile),
        FamilyType::Histogram | FamilyType::GaugeHistogram | FamilyType::Info => None,
    }
}

/// A family's declared type and metadata, keyed by the family's own (base) name. A transport that
/// carries a request's metadata separately from its samples builds one of these and shares it
/// across every assembler the request needs -- see the module doc's "Declaring lazily" section.
pub(super) type Declarations = HashMap<String, Declaration>;

/// One entry of a [`Declarations`] table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Declaration {
    pub kind: FamilyType,
    pub help: Option<String>,
    pub unit: Option<String>,
}

/// One flat sample on its way into a family -- what every Prometheus syntax hands the assembler,
/// with `le`/`quantile` still in `labels` (the assembler strips whichever the role calls for).
pub(super) struct Sample<'a> {
    pub name: &'a str,
    pub labels: Vec<(String, String)>,
    pub value: f64,
    /// The value token exactly as it appeared, for the one role whose "value" is an instant rather
    /// than a number -- see this module's doc. `None` from a transport that has no source token.
    pub value_text: Option<&'a str>,
    pub timestamp: Option<i64>,
    pub exemplar: Option<Exemplar>,
}

/// One family under construction. `base` is the name as it appeared on its declaration (or the bare
/// sample name that implied the family); `total_suffix` records that a counter's value sample
/// arrived as `<base>_total`, which is what decides the family's model-facing name -- see
/// [`super`]'s "Family naming" table.
struct FamilyAccum {
    base: String,
    kind: FamilyType,
    typed: bool,
    help: Option<String>,
    unit: Option<String>,
    total_suffix: bool,
    series: Vec<SeriesAccum>,
    series_index: HashMap<Vec<(String, String)>, usize>,
}

#[derive(Default)]
struct SeriesAccum {
    labels: Vec<(String, String)>,
    /// This series carries no reading at all -- a remote-write stale marker. It wins over every
    /// accumulated value below, which is the point: a producer that marks a series stale is saying
    /// the samples stop here, not that they take some particular value.
    stale: bool,
    value: Option<f64>,
    buckets: Vec<(f64, u64)>,
    sum: Option<f64>,
    count: Option<u64>,
    quantiles: Vec<(f64, f64)>,
    timestamp: Option<i64>,
    created: Option<i64>,
    exemplars: Vec<Exemplar>,
}

/// Where one sample landed: the family, the series within it, the role the sample name gave it, and
/// the `le`/`quantile` value already lifted out of the label set for the roles that carry one.
struct Slot {
    family: usize,
    series: usize,
    role: Role,
    extra: Option<f64>,
}

/// Flat samples in, [`MetricFamily`]s out. See the module doc for every decision it makes.
pub(super) struct Assembler<'a> {
    /// The type a family gets when nothing declared one: `Untyped` in text 0.0.4, `Unknown` in
    /// OpenMetrics and remote-write. The two are the same semantics under two spellings, kept apart
    /// so a relay re-emits the one it received ([`FamilyType`]'s own doc).
    implicit: FamilyType,
    /// Declarations this assembler may materialize on demand, shared by reference with every other
    /// assembler decoding the same request -- see the module doc's "Declaring lazily" section.
    declarations: Option<&'a Declarations>,
    families: Vec<FamilyAccum>,
    index: HashMap<String, usize>,
}

impl<'a> Assembler<'a> {
    pub(super) fn new(implicit: FamilyType) -> Self {
        Assembler { implicit, declarations: None, families: Vec::new(), index: HashMap::new() }
    }

    /// Declarations to materialize lazily, as sample names route to them. Sharing one table across
    /// the assemblers of one request is the point: see the module doc.
    pub(super) fn with_declarations(mut self, declarations: &'a Declarations) -> Self {
        self.declarations = Some(declarations);
        self
    }

    /// `# TYPE base <kind>`: the first type wins, a second *conflicting* one is counted, and a
    /// second identical one is neither counted nor an error (the formats permit a producer to
    /// repeat itself).
    pub(super) fn declare_type(
        &mut self,
        base: &str,
        kind: FamilyType,
        decoder: &mut PrometheusDecoder,
    ) {
        let idx = self.family_index(base);
        if self.families[idx].typed {
            if self.families[idx].kind != kind {
                decoder.skipped("duplicate_type");
            }
            return;
        }
        self.families[idx].kind = kind;
        self.families[idx].typed = true;
    }

    /// `# HELP base <text>`, already unescaped; `None` for an empty one, which is *absent* rather
    /// than `Some("")` ([`MetricFamily::help`]'s own doc). Only a *conflicting* second `# HELP` is
    /// counted, as with [`Assembler::declare_type`]: a producer repeating itself is not a dropped
    /// input, and remote-write 2.0 repeats a family's metadata on every one of its wire series.
    pub(super) fn declare_help(
        &mut self,
        base: &str,
        help: Option<String>,
        decoder: &mut PrometheusDecoder,
    ) {
        let idx = self.family_index(base);
        self.set_help(idx, help, decoder);
    }

    /// `# UNIT base <unit>` -- the same rules as [`Assembler::declare_help`].
    pub(super) fn declare_unit(
        &mut self,
        base: &str,
        unit: Option<String>,
        decoder: &mut PrometheusDecoder,
    ) {
        let idx = self.family_index(base);
        self.set_unit(idx, unit, decoder);
    }

    fn set_help(&mut self, idx: usize, help: Option<String>, decoder: &mut PrometheusDecoder) {
        match &self.families[idx].help {
            Some(existing) if Some(existing) == help.as_ref() => {}
            Some(_) => decoder.skipped("duplicate_metadata"),
            None => self.families[idx].help = help,
        }
    }

    fn set_unit(&mut self, idx: usize, unit: Option<String>, decoder: &mut PrometheusDecoder) {
        match &self.families[idx].unit {
            Some(existing) if Some(existing) == unit.as_ref() => {}
            Some(_) => decoder.skipped("duplicate_metadata"),
            None => self.families[idx].unit = unit,
        }
    }

    /// The help and unit a transport attached to a *series* rather than to a family, applied to
    /// whichever family that series' samples already landed in. Creates nothing, changes no type,
    /// and never displaces a description the family already has.
    ///
    /// Remote-write 2.0's `Metadata` is per series and carries no family name, so a series whose
    /// type is `UNSPECIFIED` says nothing about which family it belongs to: `foo_bucket` might be a
    /// histogram's bucket line or a gauge that happens to be called that. Declaring a family from
    /// it would create a `foo_bucket` family that then *beats* a sibling series' `HISTOGRAM`
    /// declaration of `foo`, since [`Assembler::route`] prefers an exact name match over the suffix
    /// scan -- leaving that histogram bucket-less. Waiting until the sample has routed asks the
    /// question the other way round, which is the only way it has an answer.
    ///
    /// Silent rather than counted when the family is already described: the series never claimed to
    /// be describing *that* family -- it was describing itself, and which family that turned out to
    /// mean is this assembler's conclusion, not the sender's. Reporting a `duplicate_metadata` the
    /// producer did not commit would be reporting our own inference.
    pub(super) fn describe(
        &mut self,
        sample_name: &str,
        help: Option<String>,
        unit: Option<String>,
    ) {
        let Some((family, _)) = self.route_existing(sample_name) else { return };
        if self.families[family].help.is_none() {
            if let Some(help) = help {
                self.families[family].help = Some(help);
            }
        }
        if self.families[family].unit.is_none() {
            if let Some(unit) = unit {
                self.families[family].unit = Some(unit);
            }
        }
    }

    /// One flat sample. Returns whether it landed -- a caller that reports how much of a request it
    /// stored (remote-write's `X-Prometheus-Remote-Write-Samples-Written`) needs to know, and a
    /// caller that doesn't can ignore it.
    pub(super) fn push(&mut self, sample: Sample<'_>, decoder: &mut PrometheusDecoder) -> bool {
        let Sample { name, labels, value, value_text, timestamp, exemplar } = sample;
        let Some(slot) = self.slot(name, labels, decoder) else { return false };
        let series = &mut self.families[slot.family].series[slot.series];
        let duplicate = match slot.role {
            Role::Primary => replace_once(&mut series.value, value),
            Role::Sum => replace_once(&mut series.sum, value),
            Role::Count => match count_value(value) {
                Some(c) => replace_once(&mut series.count, c),
                None => {
                    decoder.skipped("malformed_line");
                    return false;
                }
            },
            Role::Created => match created_nanos(value, value_text) {
                Some(ts) => replace_once(&mut series.created, ts),
                None => {
                    decoder.skipped("malformed_line");
                    return false;
                }
            },
            Role::Bucket => {
                let bound = slot.extra.unwrap_or(f64::NAN);
                match count_value(value) {
                    Some(c) if !bound.is_nan() => {
                        if series.buckets.iter().any(|(b, _)| *b == bound) {
                            true
                        } else {
                            series.buckets.push((bound, c));
                            false
                        }
                    }
                    _ => {
                        decoder.skipped("malformed_line");
                        return false;
                    }
                }
            }
            Role::Quantile => {
                let q = slot.extra.unwrap_or(f64::NAN);
                if q.is_nan() {
                    decoder.skipped("malformed_line");
                    return false;
                }
                if series.quantiles.iter().any(|(existing, _)| *existing == q) {
                    true
                } else {
                    series.quantiles.push((q, value));
                    false
                }
            }
        };
        if duplicate {
            decoder.skipped("duplicate_series");
            return false;
        }
        // A timestamp rides the family's value-bearing samples; a `_created` sample's own "value"
        // is the creation instant, so it never sets one.
        if slot.role != Role::Created {
            if let Some(ts) = timestamp {
                series.timestamp = Some(ts);
            }
        }
        if matches!(slot.role, Role::Primary | Role::Bucket) {
            if let Some(exemplar) = exemplar {
                series.exemplars.push(exemplar);
            }
        }
        true
    }

    /// The creation instant of the series `sample_name` names, from a transport that carries it in
    /// a field of its own (remote-write 2.0's `Sample.start_timestamp`) rather than as a
    /// `<base>_created` sample. Routes by sample name like every other entry point, then sets
    /// `created` whatever role that name plays -- `foo_total` and `foo_created` name the same
    /// series, and only one of them is a *sample* of it.
    ///
    /// Repeating the same instant is not a duplicate: a 2.0 series repeats `start_timestamp` on
    /// every one of its samples, and each of those reaches this once. A *different* instant for one
    /// series is `skipped{reason="duplicate_series"}`, first wins, as everywhere else.
    pub(super) fn push_created(
        &mut self,
        sample_name: &str,
        labels: Vec<(String, String)>,
        created_nanos: i64,
        decoder: &mut PrometheusDecoder,
    ) -> bool {
        let Some(slot) = self.slot(sample_name, labels, decoder) else { return false };
        let series = &mut self.families[slot.family].series[slot.series];
        match series.created {
            Some(existing) if existing == created_nanos => true,
            Some(_) => {
                decoder.skipped("duplicate_series");
                false
            }
            None => {
                series.created = Some(created_nanos);
                true
            }
        }
    }

    /// A stale marker for the series `sample_name` names: it has gone away, and there is no reading
    /// to record. See [`super::Point::Stale`].
    pub(super) fn push_stale(
        &mut self,
        sample_name: &str,
        labels: Vec<(String, String)>,
        timestamp_nanos: Option<i64>,
        decoder: &mut PrometheusDecoder,
    ) -> bool {
        let Some(slot) = self.slot(sample_name, labels, decoder) else { return false };
        let series = &mut self.families[slot.family].series[slot.series];
        series.stale = true;
        if let Some(ts) = timestamp_nanos {
            series.timestamp = Some(ts);
        }
        true
    }

    /// An exemplar the transport already attached to a series of its own accord, rather than one
    /// riding a sample line. Returns whether it was stored.
    ///
    /// Two differences from [`Assembler::push`], both deliberate. There is no role filter:
    /// OpenMetrics only *has* somewhere to write an exemplar on a `_total` or `_bucket` line,
    /// whereas remote-write carries them in a per-series field, so the producer has already said
    /// which series it meant and dropping one for sitting on the "wrong" sample would lose data the
    /// wire really carried. And this **creates nothing** -- no family, no series: an exemplar is an
    /// example of a reading, so a series with no reading here is one this assembler should not be
    /// made to invent (it would come back out as an `incomplete_series` skip, having swallowed the
    /// exemplar on the way).
    pub(super) fn push_exemplar(
        &mut self,
        sample_name: &str,
        mut labels: Vec<(String, String)>,
        exemplar: Exemplar,
    ) -> bool {
        let Some((family, role)) = self.route_existing(sample_name) else { return false };
        match role {
            Role::Bucket => drop(take_label(&mut labels, "le")),
            Role::Quantile => drop(take_label(&mut labels, "quantile")),
            _ => {}
        }
        labels.sort_by(|a, b| a.0.cmp(&b.0));
        let Some(series) = self.families[family].series_index.get(&labels).copied() else {
            return false;
        };
        self.families[family].series[series].exemplars.push(exemplar);
        true
    }

    /// The family a sample name belongs to, the series its label set names, and the role the name
    /// gives it -- everything a `push*` call needs before it can write anything down. Creates the
    /// family and the series if this is their first mention.
    fn slot(
        &mut self,
        name: &str,
        mut labels: Vec<(String, String)>,
        decoder: &mut PrometheusDecoder,
    ) -> Option<Slot> {
        let (family, role, total_suffix) = self.route(name, decoder)?;
        // `le`/`quantile` are part of the point, not the series identity: strip them out before the
        // label set becomes the series key.
        let extra = match role {
            Role::Bucket => match take_label(&mut labels, "le").map(|v| parse_number(&v)) {
                Some(Some(bound)) => Some(bound),
                _ => {
                    decoder.skipped("malformed_line");
                    return None;
                }
            },
            Role::Quantile => match take_label(&mut labels, "quantile").map(|v| parse_number(&v)) {
                Some(Some(q)) => Some(q),
                _ => {
                    decoder.skipped("malformed_line");
                    return None;
                }
            },
            _ => None,
        };
        labels.sort_by(|a, b| a.0.cmp(&b.0));
        if labels.windows(2).any(|w| w[0].0 == w[1].0) {
            decoder.skipped("duplicate_label");
            return None;
        }

        if total_suffix {
            self.families[family].total_suffix = true;
        }
        let accum = &mut self.families[family];
        let series = match accum.series_index.get(&labels) {
            Some(i) => *i,
            None => {
                let i = accum.series.len();
                accum.series_index.insert(labels.clone(), i);
                accum.series.push(SeriesAccum { labels, ..SeriesAccum::default() });
                i
            }
        };
        Some(Slot { family, series, role, extra })
    }

    /// Which family and role a sample name belongs to: an exact family-name match first (so a gauge
    /// genuinely called `foo_sum` beats a histogram called `foo`), then a known suffix over a
    /// declared family, then a fresh implicit family.
    fn route(
        &mut self,
        name: &str,
        decoder: &mut PrometheusDecoder,
    ) -> Option<(usize, Role, bool)> {
        self.materialize(name, decoder);
        if let Some(idx) = self.index.get(name).copied() {
            return match bare_name_role(self.families[idx].kind) {
                Some(role) => Some((idx, role, false)),
                None => {
                    decoder.skipped("unknown_suffix");
                    None
                }
            };
        }
        for (suffix, role) in SUFFIXES {
            let Some(base) = name.strip_suffix(suffix) else { continue };
            let Some(idx) = self.index.get(base).copied() else { continue };
            let kind = self.families[idx].kind;
            if suffix_applies(kind, suffix, role) {
                return Some((idx, role, suffix == "_total"));
            }
            decoder.skipped("unknown_suffix");
            return None;
        }
        let idx = self.family_index(name);
        Some((idx, Role::Primary, false))
    }

    /// Creates any *declared* family `name` could route to, if a sample has not already brought it
    /// into existence. One hash lookup for the name itself plus one per suffix, and only on the
    /// miss path -- see the module doc's "Declaring lazily" section for why this is on demand
    /// rather than replayed into every assembler up front.
    fn materialize(&mut self, name: &str, decoder: &mut PrometheusDecoder) {
        let Some(declarations) = self.declarations else { return };
        if declarations.is_empty() {
            return;
        }
        if !self.index.contains_key(name) {
            if let Some(declaration) = declarations.get(name) {
                self.declare_from(name, declaration.clone(), decoder);
            }
        }
        for (suffix, _) in SUFFIXES {
            let Some(base) = name.strip_suffix(suffix) else { continue };
            if self.index.contains_key(base) {
                continue;
            }
            if let Some(declaration) = declarations.get(base) {
                self.declare_from(base, declaration.clone(), decoder);
            }
        }
    }

    fn declare_from(
        &mut self,
        base: &str,
        declaration: Declaration,
        decoder: &mut PrometheusDecoder,
    ) {
        let Declaration { kind, help, unit } = declaration;
        self.declare_type(base, kind, decoder);
        let idx = self.family_index(base);
        if help.is_some() {
            self.set_help(idx, help, decoder);
        }
        if unit.is_some() {
            self.set_unit(idx, unit, decoder);
        }
    }

    /// The family and role a sample name routes to **among the families that already exist** --
    /// the read-only half of [`Assembler::route`], with no implicit family created, no declaration
    /// materialized and nothing counted. What [`Assembler::push_exemplar`] and
    /// [`Assembler::describe_untyped`] route over.
    fn route_existing(&self, name: &str) -> Option<(usize, Role)> {
        if let Some(idx) = self.index.get(name).copied() {
            return bare_name_role(self.families[idx].kind).map(|role| (idx, role));
        }
        for (suffix, role) in SUFFIXES {
            let Some(base) = name.strip_suffix(suffix) else { continue };
            let Some(idx) = self.index.get(base).copied() else { continue };
            return suffix_applies(self.families[idx].kind, suffix, role).then_some((idx, role));
        }
        None
    }

    fn family_index(&mut self, name: &str) -> usize {
        if let Some(idx) = self.index.get(name) {
            return *idx;
        }
        let idx = self.families.len();
        self.families.push(FamilyAccum {
            base: name.to_string(),
            kind: self.implicit,
            typed: false,
            help: None,
            unit: None,
            total_suffix: false,
            series: Vec::new(),
            series_index: HashMap::new(),
        });
        self.index.insert(name.to_string(), idx);
        idx
    }

    /// The assembled families, canonically ordered (families by name, series by label set; labels
    /// were sorted as each sample arrived). A family whose every series was incomplete disappears
    /// entirely rather than being emitted empty.
    pub(super) fn finish(self, decoder: &mut PrometheusDecoder) -> Vec<MetricFamily> {
        let mut out = Vec::with_capacity(self.families.len());
        for family in self.families {
            let name = match family.kind {
                FamilyType::Counter if family.total_suffix => format!("{}_total", family.base),
                _ => family.base.clone(),
            };
            let mut series: Vec<Series> = family
                .series
                .into_iter()
                .filter_map(|accum| finish_series(accum, family.kind, decoder))
                .collect();
            if series.is_empty() {
                continue;
            }
            series.sort_by(|a, b| a.labels.cmp(&b.labels));
            out.push(MetricFamily {
                name,
                kind: family.kind,
                help: family.help,
                unit: family.unit,
                series,
            });
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }
}

/// The *family* name a sample name belongs to, given its family's type -- the inverse of the
/// routing table above, for a transport that declares a type against a series without naming the
/// family (remote-write 2.0's per-series `Metadata`, which has a type but no family-name field).
///
/// Only a suffix that the type actually gives meaning to is stripped, so a `gauge` genuinely called
/// `foo_sum` keeps its name and a `histogram`'s `foo_sum` resolves to `foo`. A name that *is* only
/// its suffix (`_total`) keeps it: stripping there would leave a family with no name at all.
pub(super) fn family_base(sample_name: &str, kind: FamilyType) -> &str {
    for (suffix, role) in SUFFIXES {
        if !suffix_applies(kind, suffix, role) {
            continue;
        }
        if let Some(base) = sample_name.strip_suffix(suffix) {
            if !base.is_empty() {
                return base;
            }
        }
    }
    sample_name
}

/// Sets `slot` unless it already holds a value; returns whether this was a duplicate (the first
/// value always wins, which is what a strict parser rejecting the body would effectively have
/// kept).
fn replace_once<T>(slot: &mut Option<T>, value: T) -> bool {
    if slot.is_some() {
        return true;
    }
    *slot = Some(value);
    false
}

/// An accumulated series → its [`Point`], or `None` (counted) when the samples seen don't add up to
/// a value of the family's type.
fn finish_series(
    accum: SeriesAccum,
    kind: FamilyType,
    decoder: &mut PrometheusDecoder,
) -> Option<Series> {
    let SeriesAccum {
        labels,
        stale,
        value,
        mut buckets,
        sum,
        count,
        mut quantiles,
        timestamp,
        created,
        exemplars,
    } = accum;
    // A stale marker is the whole point: it says this series has no reading, so whatever else
    // arrived for it does not get to supply one. Checked before the per-type completeness rules
    // below, which would otherwise report an `incomplete_series` for a series that is complete in
    // the only way a stale one can be.
    if stale {
        return Some(Series { labels, point: Point::Stale, timestamp, created, exemplars });
    }
    let point = match kind {
        FamilyType::Counter => Point::Counter(value_or_skip(value, decoder)?),
        FamilyType::Gauge => Point::Gauge(value_or_skip(value, decoder)?),
        FamilyType::Unknown | FamilyType::Untyped => Point::Unknown(value_or_skip(value, decoder)?),
        FamilyType::Info => {
            value_or_skip(value, decoder)?;
            Point::Info
        }
        FamilyType::StateSet => Point::StateSet(value_or_skip(value, decoder)? != 0.0),
        FamilyType::Histogram | FamilyType::GaugeHistogram => {
            if buckets.is_empty() {
                decoder.skipped("incomplete_series");
                return None;
            }
            buckets.sort_by(|a, b| a.0.total_cmp(&b.0));
            // Every conforming producer sends a `+Inf` bucket; when one is missing the total has to
            // come from somewhere, and `_count` (else the highest bucket) is that somewhere.
            let highest = buckets.last().map(|(_, c)| *c).unwrap_or(0);
            if buckets.last().map(|(b, _)| b.is_finite()).unwrap_or(true) {
                buckets.push((f64::INFINITY, count.unwrap_or(highest).max(highest)));
            }
            let total = buckets.last().map(|(_, c)| *c).unwrap_or(0);
            // The `+Inf` bucket is the total; a `_count`/`_gcount` claiming otherwise is a producer
            // bug, and the model has exactly one place to put a total.
            if count.is_some_and(|c| c != total) {
                decoder.degraded("histogram_count_mismatch");
            }
            Point::Histogram { buckets, sum, count: total }
        }
        FamilyType::Summary => {
            if quantiles.is_empty() && sum.is_none() && count.is_none() && created.is_none() {
                decoder.skipped("incomplete_series");
                return None;
            }
            quantiles.sort_by(|a, b| a.0.total_cmp(&b.0));
            Point::Summary { quantiles, sum, count }
        }
    };
    Some(Series { labels, point, timestamp, created, exemplars })
}

fn value_or_skip(value: Option<f64>, decoder: &mut PrometheusDecoder) -> Option<f64> {
    match value {
        Some(v) => Some(v),
        None => {
            decoder.skipped("incomplete_series");
            None
        }
    }
}

/// Removes and returns a label by name -- how `le`/`quantile` leave the series' own label set.
fn take_label(labels: &mut Vec<(String, String)>, name: &str) -> Option<String> {
    let pos = labels.iter().position(|(k, _)| k == name)?;
    Some(labels.remove(pos).1)
}

/// A label value or sample value as a number: Rust's own `f64` grammar already covers Go's
/// `ParseFloat` shapes plus case-insensitive `nan`/`inf`/`infinity` with an optional sign, which is
/// exactly what the exposition formats allow and a superset of what protobuf can carry.
pub(super) fn parse_number(s: &str) -> Option<f64> {
    if s.is_empty() {
        return None;
    }
    s.parse::<f64>().ok()
}

/// A count sample (`_count`, `_gcount`, a bucket) as the `u64` the model holds. OpenMetrics writes
/// these as floats (`42.0`), and a `gaugehistogram`'s may genuinely be fractional -- rounded here,
/// since [`logit_core::Histogram`]'s bucket counts are integers.
fn count_value(v: f64) -> Option<u64> {
    if v.is_finite() && v >= 0.0 {
        Some(v.round() as u64)
    } else {
        None
    }
}

/// A `_created` sample's instant, in unix nanoseconds: from the source token when there is one
/// ([`parse_created_seconds`], digit-exact), else from the already-parsed `f64`, which is all a
/// transport that carries the value as a protobuf double ever has.
fn created_nanos(value: f64, text: Option<&str>) -> Option<i64> {
    match text {
        Some(text) => parse_created_seconds(text),
        None => {
            if !value.is_finite() {
                return None;
            }
            let nanos = value * 1e9;
            if nanos.abs() >= i64::MAX as f64 {
                return None;
            }
            Some(nanos.round() as i64)
        }
    }
}

/// Decimal *seconds* → unix nanoseconds, read digit by digit rather than through an `f64`: an
/// epoch-nanosecond instant needs 19 significant digits and an `f64` holds 15-16, so a float round
/// trip would silently move the sample in time. Covers the plain `[SIGN] DIGIT+ ["." DIGIT*]` form
/// every real producer emits; OpenMetrics' `realnumber` production also permits an exponent
/// (`1.605281325e9`), which has no digit-exact reading at all, so that form -- and only that form
/// -- falls back to `f64`, accepting its ~1µs resolution at epoch magnitude rather than rejecting a
/// legal timestamp.
///
/// The scale is fixed at seconds because a `_created` sample's value is decimal seconds in *both*
/// exposition dialects (text 0.0.4 has no `_created` of its own, so one found there is read the
/// OpenMetrics way). A sample *timestamp*'s unit is dialect-dependent and stays
/// [`super::text`]'s business.
pub(super) fn parse_created_seconds(s: &str) -> Option<i64> {
    parse_scaled_decimal(s, 1_000_000_000)
}

/// [`parse_created_seconds`] over an arbitrary unit: `scale` is the nanoseconds one source unit is
/// worth (`1_000_000` for milliseconds, `1_000_000_000` for seconds).
pub(super) fn parse_scaled_decimal(s: &str, scale: i64) -> Option<i64> {
    let (negative, digits) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    if let Some(nanos) = parse_decimal_nanos(digits, scale) {
        return Some(if negative { -nanos } else { nanos });
    }
    let value: f64 = digits.parse().ok()?;
    if !value.is_finite() {
        return None;
    }
    let nanos = value * scale as f64;
    if nanos.abs() >= i64::MAX as f64 {
        return None;
    }
    let nanos = nanos.round() as i64;
    Some(if negative { -nanos } else { nanos })
}

/// Exemplar labels → an [`Exemplar`]: `trace_id`/`span_id` become a [`TraceRef`] when both are
/// valid hex (and are consumed); every other label, and an invalid id, stays in
/// `filtered_attributes` rather than being silently dropped. The all-zero-is-invalid rule is
/// [`TraceRef::from_bytes`]'s, applied here as everywhere else.
///
/// Shared because every format spells an exemplar's trace reference as those two labels --
/// OpenMetrics' ` # {trace_id="…"} 0.5` and remote-write's `Exemplar.labels` are the same pair of
/// strings arriving through different syntax.
///
/// [`TraceRef::from_bytes`]: logit_core::TraceRef::from_bytes
pub(super) fn exemplar_from_labels(
    mut labels: Vec<(String, String)>,
    value: f64,
    timestamp: i64,
) -> Exemplar {
    let trace_id = labels.iter().position(|(k, _)| k == "trace_id").and_then(|i| {
        let id = parse_trace_id(&labels[i].1)?;
        Some((i, id))
    });
    let trace = trace_id.map(|(pos, trace_id)| {
        labels.remove(pos);
        let span = labels.iter().position(|(k, _)| k == "span_id").and_then(|i| {
            let id = parse_span_id(&labels[i].1)?;
            Some((i, id))
        });
        let span_id = span.map(|(pos, id)| {
            labels.remove(pos);
            id
        });
        TraceRef { trace_id, span_id, flags: 0 }
    });
    let mut filtered_attributes = AttrMap::new();
    for (key, value) in labels {
        filtered_attributes.insert(&key, Value::str(value));
    }
    Exemplar { timestamp, value, trace, filtered_attributes }
}
