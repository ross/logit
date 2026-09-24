//! A tiny `{name}` placeholder template: parsed once, name-resolved once, rendered per event.
//!
//! **The parser knows no names.** [`parse`] only splits literal text from placeholders. The
//! consumer decides which names mean something: it walks [`Template::vars`] at config validation,
//! then [`Template::compile`]s each name into its own value type at construction. The module
//! lives in `logit-core` because those two steps are in different crates, and an implementation
//! crate may not depend on `logit-config` (`docs/design/pipeline-graph.md`'s "Crate layout").
//! Consumers today: `generate_in`'s event template (`seq`/`seq%N`) and `logit-perf`'s load specs.
//!
//! Syntax: `{name}` is a placeholder; `{{` and `}}` are a literal `{` and `}`. `name` is the raw
//! text between the braces, not trimmed, validated, or split, so a consumer's inner syntax
//! (`seq%1000`, `attributes.host`) arrives as written.
//!
//! Hot path: [`Compiled::render`] does no name lookup and allocates nothing beyond growing the
//! caller's `String`, so rendering into a cleared, already-grown scratch buffer allocates nothing.

use bytes::Bytes;

/// Why a template string couldn't be parsed: a typo, reported with the byte offset of the
/// offending character, since a configured template can be a whole JSON log line.
///
/// Hand-rolled rather than `thiserror`-derived: `logit-core` has no `thiserror` dependency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemplateError {
    /// A `{` with no closing `}` after it.
    Unterminated { at: usize },
    /// A `{}` naming nothing. Not a literal: `{{}}` writes that.
    EmptyName { at: usize },
    /// A `}` that closes no placeholder, so a mistyped `{name}}` is an error rather than a
    /// trailing brace.
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
    /// Literal text, already unescaped. [`Bytes`] so an all-literal template clones onto every
    /// event as a refcount bump (see [`Template::literal`]).
    Lit(Bytes),
    /// A placeholder's raw name, as written between the braces.
    Var(String),
}

/// A parsed template: literal text interleaved with placeholder names.
///
/// `parse` merges adjacent literals, so an all-literal template is one segment; that's what makes
/// [`Template::literal`] exact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Template {
    pub segments: Vec<Segment>,
}

/// What [`Template::literal`] borrows for an empty template, which has no segment to point at.
static EMPTY_LITERAL: Bytes = Bytes::new();

impl Template {
    /// The placeholder names in order, duplicates repeated, for validation to reject unknown ones.
    pub fn vars(&self) -> impl Iterator<Item = &str> {
        self.segments.iter().filter_map(|segment| match segment {
            Segment::Var(name) => Some(name.as_str()),
            Segment::Lit(_) => None,
        })
    }

    /// Whether this template has no placeholders, so it can be rendered once rather than per event.
    pub fn is_literal(&self) -> bool {
        self.segments.iter().all(|segment| matches!(segment, Segment::Lit(_)))
    }

    /// The whole template as one literal (empty bytes for an empty template), or `None` if any
    /// placeholder is present. On `Some`, a consumer clones these `Bytes` onto every event and
    /// never calls [`Compiled::render`].
    pub fn literal(&self) -> Option<&Bytes> {
        match self.segments.as_slice() {
            [] => Some(&EMPTY_LITERAL),
            [Segment::Lit(bytes)] => Some(bytes),
            _ => None,
        }
    }

    /// Resolves every placeholder name once into the consumer's hot-path value type. The first
    /// `Err` from `resolve` (an unrecognized name) is returned as-is.
    pub fn compile<V, E>(
        &self,
        mut resolve: impl FnMut(&str) -> Result<V, E>,
    ) -> Result<Compiled<V>, E> {
        let mut segments = Vec::with_capacity(self.segments.len());
        for segment in &self.segments {
            segments.push(match segment {
                // Converted once so `render` needs no per-event UTF-8 check. Exact for anything
                // `parse` built; lossy only because a caller can hand-build a `Segment::Lit`.
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
    /// [`Segment::Lit`]'s text as a `str`, so rendering needs no UTF-8 check.
    Lit(Box<str>),
    Var(V),
}

/// A [`Template`] with its placeholder names resolved into `V`, built by [`Template::compile`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Compiled<V> {
    pub segments: Vec<CompiledSegment<V>>,
}

impl<V> Compiled<V> {
    /// Appends this template's rendering to `out`, calling `var` with each placeholder's value
    /// and `out`.
    ///
    /// Allocates only to grow `out`: use one scratch `String` per worker, cleared per event, and
    /// it stops allocating once it reaches the widest rendering.
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
/// Names aren't checked at all; see the module doc.
pub fn parse(input: &str) -> Result<Template, TemplateError> {
    let mut segments = Vec::new();
    // Flushed only when a placeholder starts or input ends, which merges adjacent literals.
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
                // Copy the run up to the next brace at once. Braces are ASCII, so the run ends on
                // a char boundary: no multi-byte UTF-8 sequence contains a `{`/`}` byte.
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

    /// A placeholder's name reaches the consumer byte for byte, untrimmed and unsplit.
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

    /// Escaped braces merge into one literal segment with the text around them.
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
        // `{seq}` consumes the first `}`, so the next one is stray, not an escape.
        assert_eq!(parse("{seq}}"), Err(TemplateError::StrayBrace { at: 5 }));
        assert!(parse("}").unwrap_err().to_string().contains("stray '}'"));
    }

    /// A consumer's resolved type and resolver, roughly `generate_in`'s.
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

    /// A grown scratch `String` renders the next event with no reallocation (exact `capacity`).
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
