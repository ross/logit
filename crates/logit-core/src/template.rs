//! A tiny `{name}` placeholder template: parsed once, name-resolved once, rendered per event.
//!
//! **The parser knows no names.** [`parse`] only splits literal text from placeholders; deciding
//! which names mean something -- and rejecting the rest -- belongs entirely to the consumer, which
//! walks [`Template::vars`] (config validation) and then [`Template::compile`]s each name into its
//! own resolved value type (construction). That split is why this module lives in `logit-core`
//! rather than next to its first consumer: the graph validation that rejects an unknown name and
//! the component that actually renders live in different crates, and an implementation crate may
//! not depend on `logit-config` (`docs/design/pipeline-graph.md`'s crate layout).
//!
//! First consumer: `generate_in`'s event template (`docs/plans/load-test-harness.md`), which
//! resolves `seq`/`seq%N` into a counter and renders a log body, attribute values, and a metric
//! name per generated event. The shape is deliberately consumer-agnostic because a second one is
//! already foreseen: a `stdio_out` line format, where the same `{name}` syntax would resolve
//! `timestamp`, `log.message`, `attributes.host` and friends into field accessors against the
//! event being written. Nothing in this module needs to change for that -- only the `resolve`
//! closure passed to [`Template::compile`] and the `var` closure passed to [`Compiled::render`].
//!
//! Syntax: `{name}` is a placeholder; `{{` and `}}` are a literal `{` and `}`. `name` is the raw
//! text between the braces, passed through verbatim -- not trimmed, not validated, not split -- so
//! a consumer whose names have their own inner syntax (`seq%1000`, `attributes.host`) receives it
//! exactly as written.
//!
//! Hot path: [`Compiled::render`] walks pre-resolved segments, appending literals and calling the
//! consumer's closure for each placeholder. No string matching, no name lookup, and no allocation
//! beyond growing the caller's `String` -- so a caller that renders into a cleared, already-grown
//! scratch buffer allocates nothing at all.

use bytes::Bytes;

/// Why a template string couldn't be parsed. Three shapes, each a typo rather than anything a
/// consumer could meaningfully recover from -- and each carrying the byte offset of the character
/// that made the template ambiguous, since a configured template is often long enough (a whole
/// JSON log line, say) that naming the offset is the difference between a fixable error and a hunt.
///
/// Hand-rolled rather than `thiserror`-derived: `logit-core` has no `thiserror` dependency and
/// keeps its dependency list to the event model's own needs ([`crate::TimestampError`] and
/// `HllDecodeError` are the precedent).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemplateError {
    /// A `{` with no closing `}` after it.
    Unterminated { at: usize },
    /// A `{}` naming nothing. Rejected rather than treated as a literal `{}`: a consumer's
    /// resolver would have to invent a meaning for the empty name, and `{{}}` already writes a
    /// literal empty pair.
    EmptyName { at: usize },
    /// A `}` that closes no placeholder. Rejected rather than passed through as a literal, so a
    /// mistyped `{name}}` is an error instead of silently rendering a trailing brace.
    StrayBrace { at: usize },
}

impl std::fmt::Display for TemplateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TemplateError::Unterminated { at } => write!(
                f,
                "unterminated '{{' at byte {at} -- a placeholder must be closed with '}}', and a \
                 literal brace is written '{{{{'"
            ),
            TemplateError::EmptyName { at } => write!(
                f,
                "empty placeholder '{{}}' at byte {at} -- a placeholder must name something"
            ),
            TemplateError::StrayBrace { at } => {
                write!(f, "stray '}}' at byte {at} -- a literal brace is written '}}}}'")
            }
        }
    }
}

impl std::error::Error for TemplateError {}

/// One piece of a parsed [`Template`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Segment {
    /// Literal text, already unescaped (`{{`/`}}` collapsed to one brace). [`Bytes`], not
    /// `String`, because a consumer whose template is entirely literal clones the whole thing onto
    /// every event -- a refcount bump rather than a copy (see [`Template::literal`]).
    Lit(Bytes),
    /// A placeholder's raw name, exactly as written between the braces.
    Var(String),
}

/// A parsed template: literal text interleaved with placeholder names.
///
/// Adjacent literals are merged at parse time, so two `Lit` segments never sit next to each other
/// and an all-literal template is exactly one segment. That's what makes [`Template::literal`] a
/// reliable "is there anything at all to render per event?" test rather than a heuristic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Template {
    pub segments: Vec<Segment>,
}

/// What [`Template::literal`] returns for an empty template -- a `&Bytes` has to point somewhere,
/// and an empty template has no segment of its own to point at.
static EMPTY_LITERAL: Bytes = Bytes::new();

impl Template {
    /// The placeholder names, in the order they appear, with duplicates repeated -- a consumer
    /// validating a template (`logit-pipeline::graph`'s rules) walks this to reject a name it
    /// doesn't know, before anything is constructed.
    pub fn vars(&self) -> impl Iterator<Item = &str> {
        self.segments.iter().filter_map(|segment| match segment {
            Segment::Var(name) => Some(name.as_str()),
            Segment::Lit(_) => None,
        })
    }

    /// Whether this template has no placeholders at all -- the case a consumer can render once and
    /// reuse forever instead of per event.
    pub fn is_literal(&self) -> bool {
        self.segments.iter().all(|segment| matches!(segment, Segment::Lit(_)))
    }

    /// The whole template as one literal, when [`Template::is_literal`] holds (including the empty
    /// template, which yields empty bytes). `None` as soon as any placeholder is present.
    ///
    /// This is the fast path's entry point: a consumer that gets `Some` here can clone these
    /// `Bytes` onto every event and never touch [`Compiled::render`] at all.
    pub fn literal(&self) -> Option<&Bytes> {
        match self.segments.as_slice() {
            [] => Some(&EMPTY_LITERAL),
            [Segment::Lit(bytes)] => Some(bytes),
            _ => None,
        }
    }

    /// Resolves every placeholder name once, up front, into whatever the consumer wants to carry
    /// on the hot path -- its own `enum` of pre-computed accessors, typically. `resolve` returning
    /// `Err` is how a consumer rejects a name it doesn't recognize; the first failure
    /// short-circuits and is returned as-is, so the error type stays entirely the consumer's.
    pub fn compile<V, E>(
        &self,
        mut resolve: impl FnMut(&str) -> Result<V, E>,
    ) -> Result<Compiled<V>, E> {
        let mut segments = Vec::with_capacity(self.segments.len());
        for segment in &self.segments {
            segments.push(match segment {
                // The literal becomes a `str` here, once, so `render` needs no UTF-8 check per
                // event. `parse` builds every literal from a `&str`, so this lossy conversion is
                // exact for any template that came from it; it stays lossy rather than fallible
                // only because `Segment::Lit` is a public `Bytes` a caller could hand-build.
                Segment::Lit(bytes) => CompiledSegment::Lit(
                    String::from_utf8_lossy(bytes).into_owned().into_boxed_str(),
                ),
                Segment::Var(name) => CompiledSegment::Var(resolve(name)?),
            });
        }
        Ok(Compiled { segments })
    }
}

/// One piece of a [`Compiled`] template: literal text, or a consumer-resolved placeholder value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompiledSegment<V> {
    /// The same literal text [`Segment::Lit`] carries, held as a `str` so [`Compiled::render`]
    /// can append it with no per-render UTF-8 check.
    Lit(Box<str>),
    Var(V),
}

/// A [`Template`] whose placeholder names have been resolved into `V` -- the form the hot path
/// walks. Built by [`Template::compile`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Compiled<V> {
    pub segments: Vec<CompiledSegment<V>>,
}

impl<V> Compiled<V> {
    /// Appends this template's rendering to `out`, calling `var` for each placeholder with its
    /// resolved value and the same `out` to append to.
    ///
    /// Nothing is allocated here beyond whatever growing `out` needs, and nothing is matched by
    /// name -- so the intended shape is one `String` scratch buffer per worker, `clear`ed and
    /// re-rendered per event, which stops allocating entirely once it has grown to the widest
    /// rendering it has seen.
    pub fn render(&self, out: &mut String, mut var: impl FnMut(&V, &mut String)) {
        for segment in &self.segments {
            match segment {
                CompiledSegment::Lit(lit) => out.push_str(lit),
                CompiledSegment::Var(value) => var(value, out),
            }
        }
    }
}

/// Parses a template: `{name}` placeholders in literal text, `{{`/`}}` for a literal brace.
///
/// See this module's doc comment for the syntax, and for what is deliberately *not* checked here
/// (anything at all about the names).
pub fn parse(input: &str) -> Result<Template, TemplateError> {
    let mut segments = Vec::new();
    // One accumulator for all literal text, flushed only when a placeholder starts or the input
    // ends -- which is what merges adjacent literals (an escaped brace between two runs of plain
    // text, say) into a single segment without a second pass.
    let mut lit = String::new();
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'{' if bytes.get(i + 1) == Some(&b'{') => {
                lit.push('{');
                i += 2;
            }
            b'{' => {
                let rest = &input[i + 1..];
                let Some(end) = rest.find('}') else {
                    return Err(TemplateError::Unterminated { at: i });
                };
                let name = &rest[..end];
                if name.is_empty() {
                    return Err(TemplateError::EmptyName { at: i });
                }
                if !lit.is_empty() {
                    segments.push(Segment::Lit(Bytes::from(std::mem::take(&mut lit).into_bytes())));
                }
                segments.push(Segment::Var(name.to_string()));
                i += end + 2;
            }
            b'}' if bytes.get(i + 1) == Some(&b'}') => {
                lit.push('}');
                i += 2;
            }
            b'}' => return Err(TemplateError::StrayBrace { at: i }),
            _ => {
                // Copy the whole run up to the next brace in one `push_str` rather than a char at
                // a time. Both braces are ASCII, so the run's end is always a char boundary -- a
                // multi-byte UTF-8 sequence never contains a `{`/`}` byte.
                let next = bytes[i..]
                    .iter()
                    .position(|b| *b == b'{' || *b == b'}')
                    .map_or(bytes.len(), |offset| i + offset);
                lit.push_str(&input[i..next]);
                i = next;
            }
        }
    }
    if !lit.is_empty() {
        segments.push(Segment::Lit(Bytes::from(lit.into_bytes())));
    }
    Ok(Template { segments })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Write as _;

    /// A readable spelling of the expected segments, so a test reads as the template it parses.
    fn lit(text: &str) -> Segment {
        Segment::Lit(Bytes::from(text.as_bytes().to_vec()))
    }

    fn var(name: &str) -> Segment {
        Segment::Var(name.to_string())
    }

    #[test]
    fn a_template_with_no_placeholders_is_one_literal_segment() {
        let template = parse("GET /index.html 200").unwrap();
        assert_eq!(template.segments, vec![lit("GET /index.html 200")]);
        assert!(template.is_literal());
        assert_eq!(template.literal().map(|b| b.as_ref()), Some(&b"GET /index.html 200"[..]));
        assert_eq!(template.vars().collect::<Vec<_>>(), Vec::<&str>::new());
    }

    #[test]
    fn an_empty_template_is_an_empty_literal() {
        let template = parse("").unwrap();
        assert_eq!(template.segments, Vec::new());
        assert!(template.is_literal());
        assert_eq!(template.literal().map(|b| b.as_ref()), Some(&b""[..]));
    }

    #[test]
    fn placeholders_and_literals_interleave_in_order() {
        let template = parse("host-{seq%10}.{zone}:8080").unwrap();
        assert_eq!(
            template.segments,
            vec![lit("host-"), var("seq%10"), lit("."), var("zone"), lit(":8080")]
        );
        assert!(!template.is_literal());
        assert_eq!(template.literal(), None);
        assert_eq!(template.vars().collect::<Vec<_>>(), vec!["seq%10", "zone"]);
    }

    /// A placeholder's name reaches the consumer byte for byte -- no trimming, no splitting on the
    /// inner syntax a consumer's own names may have. `generate_in`'s `seq%1000` depends on it.
    #[test]
    fn a_placeholder_name_is_passed_through_verbatim() {
        let template = parse("{ seq % 1000 }{attributes.host}").unwrap();
        assert_eq!(template.vars().collect::<Vec<_>>(), vec![" seq % 1000 ", "attributes.host"]);
    }

    #[test]
    fn a_template_that_is_only_a_placeholder_has_no_literal_segments() {
        let template = parse("{seq}").unwrap();
        assert_eq!(template.segments, vec![var("seq")]);
        assert!(!template.is_literal());
        assert_eq!(template.literal(), None);
    }

    #[test]
    fn a_repeated_placeholder_is_reported_once_per_occurrence() {
        let template = parse("{seq}-{seq}").unwrap();
        assert_eq!(template.vars().collect::<Vec<_>>(), vec!["seq", "seq"]);
    }

    #[test]
    fn doubled_braces_are_literal_braces() {
        let template = parse("{{\"seq\": {seq}}}").unwrap();
        assert_eq!(template.segments, vec![lit("{\"seq\": "), var("seq"), lit("}")]);
        assert_eq!(template.vars().collect::<Vec<_>>(), vec!["seq"]);
    }

    /// The escape is what makes an all-literal template with braces in it still a single segment:
    /// adjacent literals are merged at parse time, never left for a second pass.
    #[test]
    fn escaped_braces_merge_into_the_surrounding_literal() {
        let template = parse("a{{b}}c").unwrap();
        assert_eq!(template.segments, vec![lit("a{b}c")]);
        assert!(template.is_literal());
        assert_eq!(template.literal().map(|b| b.as_ref()), Some(&b"a{b}c"[..]));
    }

    #[test]
    fn a_doubled_brace_pair_can_wrap_a_name_without_substituting_it() {
        let template = parse("{{seq}}").unwrap();
        assert_eq!(template.segments, vec![lit("{seq}")]);
    }

    #[test]
    fn multibyte_literal_text_survives_a_round_trip() {
        let template = parse("héllo-{seq}-wörld").unwrap();
        assert_eq!(template.segments, vec![lit("héllo-"), var("seq"), lit("-wörld")]);
    }

    #[test]
    fn an_unterminated_placeholder_is_rejected() {
        assert_eq!(parse("host-{seq"), Err(TemplateError::Unterminated { at: 5 }));
        assert_eq!(parse("{"), Err(TemplateError::Unterminated { at: 0 }));
        assert!(parse("{").unwrap_err().to_string().contains("unterminated '{'"));
    }

    #[test]
    fn an_empty_placeholder_is_rejected() {
        assert_eq!(parse("host-{}"), Err(TemplateError::EmptyName { at: 5 }));
        assert!(parse("{}").unwrap_err().to_string().contains("empty placeholder '{}'"));
    }

    #[test]
    fn a_stray_closing_brace_is_rejected() {
        assert_eq!(parse("host}"), Err(TemplateError::StrayBrace { at: 4 }));
        // The brace closing `{seq}` is consumed by the placeholder, so the *next* one is stray --
        // a mistyped `}}` escape, not a second escape.
        assert_eq!(parse("{seq}}"), Err(TemplateError::StrayBrace { at: 5 }));
        assert!(parse("}").unwrap_err().to_string().contains("stray '}'"));
    }

    /// What a consumer's resolved value type actually looks like: one small enum, and a resolver
    /// that rejects everything else. `generate_in`'s own is this, near enough.
    #[derive(Debug, PartialEq, Eq)]
    enum Resolved {
        Seq,
        SeqMod(u64),
    }

    fn resolve(name: &str) -> Result<Resolved, String> {
        if name == "seq" {
            return Ok(Resolved::Seq);
        }
        match name.strip_prefix("seq%") {
            Some(modulus) => modulus
                .parse::<u64>()
                .map(Resolved::SeqMod)
                .map_err(|_| format!("bad modulus in '{name}'")),
            None => Err(format!("unknown placeholder '{name}'")),
        }
    }

    #[test]
    fn compile_resolves_every_placeholder_and_keeps_literals_in_order() {
        let compiled = parse("host-{seq%10}/{seq}").unwrap().compile(resolve).unwrap();
        assert_eq!(
            compiled.segments,
            vec![
                CompiledSegment::Lit("host-".into()),
                CompiledSegment::Var(Resolved::SeqMod(10)),
                CompiledSegment::Lit("/".into()),
                CompiledSegment::Var(Resolved::Seq),
            ]
        );
    }

    #[test]
    fn compile_returns_the_resolvers_own_error_for_an_unknown_name() {
        let err = parse("host-{hostname}").unwrap().compile(resolve).unwrap_err();
        assert_eq!(err, "unknown placeholder 'hostname'");
    }

    #[test]
    fn compile_of_an_all_literal_template_never_calls_the_resolver() {
        let compiled = parse("plain")
            .unwrap()
            .compile(|name| -> Result<Resolved, String> {
                panic!("resolver should not be called, got {name:?}")
            })
            .unwrap();
        assert_eq!(compiled.segments, vec![CompiledSegment::Lit("plain".into())]);
    }

    fn render_seq(compiled: &Compiled<Resolved>, seq: u64, out: &mut String) {
        compiled.render(out, |resolved, out| match resolved {
            Resolved::Seq => {
                let _ = write!(out, "{seq}");
            }
            Resolved::SeqMod(modulus) => {
                let _ = write!(out, "{}", seq % modulus);
            }
        });
    }

    #[test]
    fn render_appends_literals_and_delegates_placeholders() {
        let compiled = parse("host-{seq%10}/{seq}").unwrap().compile(resolve).unwrap();
        let mut out = String::new();
        render_seq(&compiled, 123, &mut out);
        assert_eq!(out, "host-3/123");
    }

    #[test]
    fn render_appends_to_whatever_the_caller_already_wrote() {
        let compiled = parse("{seq}").unwrap().compile(resolve).unwrap();
        let mut out = String::from("seq=");
        render_seq(&compiled, 7, &mut out);
        assert_eq!(out, "seq=7");
    }

    /// The hot-path guarantee: a scratch `String` that has already grown to hold one rendering
    /// renders the next one with no reallocation at all. Exact equality on `capacity`, not a
    /// bound -- a realloc is precisely what this test exists to catch (`AGENTS.md`).
    #[test]
    fn rendering_into_a_cleared_scratch_string_reallocates_nothing() {
        let compiled = parse("host-{seq%10}/{seq} done").unwrap().compile(resolve).unwrap();
        let mut out = String::new();
        render_seq(&compiled, 4, &mut out);
        assert_eq!(out, "host-4/4 done");
        let grown = out.capacity();

        out.clear();
        render_seq(&compiled, 5, &mut out);
        assert_eq!(out, "host-5/5 done");
        assert_eq!(out.capacity(), grown, "a second render into a cleared buffer reallocated");
    }
}
