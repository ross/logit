//! A deliberately minimal glob subset: a literal path, or one with a `*` in its final path
//! component (`/var/log/app/*.log`), matching any run of non-`/` characters. No `**`, `?`,
//! `[...]`, or escaping -- `docs/adr/file-tailing-and-docker-json-logs.md` explains why this is
//! enough for both callers (`tail_in`'s own config-validated `paths`, and `docker_in`'s
//! internally-built `<root>/*/*-json.log`) without pulling in a glob crate. Graph validation
//! (rule 26, `crates/logit-pipeline/src/graph.rs`) already rejects a `tail_in` `*` outside the
//! final component before this ever runs; [`PathPattern::new`] itself never rejects anything --
//! a pattern that can't usefully match just never matches, the same as an empty directory.

use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
enum Matcher {
    /// No `*` in the final component -- matches exactly one name.
    Literal(String),
    /// A `*` in the final component, split at it: `prefix* suffix` (either half may be empty).
    Wildcard { prefix: String, suffix: String },
}

/// One `paths:`/discovery entry, split into the directory to scan and how to recognize a match
/// within it. Scanning one level at a time (never recursive) is deliberate: both callers name a
/// specific directory, not a subtree.
#[derive(Debug, Clone)]
pub struct PathPattern {
    dir: PathBuf,
    matcher: Matcher,
}

impl PathPattern {
    /// `path`'s parent becomes the scanned directory; its final component becomes the matcher.
    /// A `path` with no parent (bare `"app.log"`) scans `.` -- not a shape either caller
    /// produces (both always build absolute paths), but not rejected here either, matching this
    /// module's "never rejects, just may never match" contract.
    pub fn new(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref();
        let dir = path.parent().map(Path::to_path_buf).unwrap_or_default();
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("").to_string();
        let matcher = match name.split_once('*') {
            Some((prefix, suffix)) => {
                Matcher::Wildcard { prefix: prefix.to_string(), suffix: suffix.to_string() }
            }
            None => Matcher::Literal(name),
        };
        Self { dir, matcher }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn matches_name(&self, name: &str) -> bool {
        match &self.matcher {
            Matcher::Literal(literal) => name == literal,
            Matcher::Wildcard { prefix, suffix } => {
                // `len() >=` (not `>`) matters when prefix and suffix together exhaust the whole
                // name and the wildcard itself matches zero characters, e.g. pattern `x*y`
                // against name `xy`.
                name.len() >= prefix.len() + suffix.len()
                    && name.starts_with(prefix.as_str())
                    && name.ends_with(suffix.as_str())
            }
        }
    }

    /// Every currently-matching file path in this pattern's directory -- a plain, non-recursive
    /// `read_dir`. Synchronous: called only from [`crate::tail::driver::Tailer::scan`], on the
    /// `poll_interval`/wake cadence (seconds, not the per-line hot path), so a brief blocking
    /// directory listing here costs nothing worth threading through `spawn_blocking` for. A
    /// directory that doesn't exist (yet, or anymore) is silently treated as empty -- discovery
    /// is expected to handle "not there yet" by trying again next cycle, not by erroring.
    pub fn scan(&self) -> Vec<PathBuf> {
        let mut out = Vec::new();
        let Ok(entries) = std::fs::read_dir(&self.dir) else {
            return out;
        };
        for entry in entries.flatten() {
            let Ok(name) = entry.file_name().into_string() else {
                continue; // non-UTF-8 name: can't match a str-based pattern, skip it
            };
            if self.matches_name(&name) {
                out.push(self.dir.join(name));
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_literal_path_matches_only_its_own_name() {
        let p = PathPattern::new("/var/log/app.log");
        assert_eq!(p.dir(), Path::new("/var/log"));
        assert!(p.matches_name("app.log"));
        assert!(!p.matches_name("app.log.1"));
        assert!(!p.matches_name("other.log"));
    }

    #[test]
    fn a_trailing_star_matches_any_suffix() {
        let p = PathPattern::new("/var/log/app/*.log");
        assert!(p.matches_name("access.log"));
        assert!(p.matches_name(".log")); // the wildcard may match zero characters
        assert!(!p.matches_name("access.log.1"));
        assert!(!p.matches_name("access.txt"));
    }

    #[test]
    fn a_leading_star_matches_any_prefix() {
        let p = PathPattern::new("/var/lib/docker/containers/*/deadbeef-json.log");
        // Not this module's own scenario (docker_in builds a two-level pattern differently), but
        // exercises the mirror-image `prefix=""` case the same matcher must also get right.
        let _ = p;
        let name_pattern = PathPattern::new("/x/*-json.log");
        assert!(name_pattern.matches_name("abc123-json.log"));
        assert!(!name_pattern.matches_name("abc123-json.log.1"));
    }

    #[test]
    fn scanning_a_missing_directory_returns_no_matches() {
        let p = PathPattern::new("/this/path/does/not/exist/*.log");
        assert_eq!(p.scan(), Vec::<PathBuf>::new());
    }

    #[test]
    fn scan_lists_only_matching_entries_in_the_directory() {
        let dir = crate::tail::test_support::scratch_dir("pattern-scan");
        std::fs::write(dir.join("a.log"), b"").unwrap();
        std::fs::write(dir.join("b.log"), b"").unwrap();
        std::fs::write(dir.join("b.log.1"), b"").unwrap();
        std::fs::write(dir.join("readme.txt"), b"").unwrap();

        let p = PathPattern::new(dir.join("*.log"));
        let mut matched: Vec<String> = p
            .scan()
            .into_iter()
            .map(|p| p.file_name().unwrap().to_str().unwrap().to_string())
            .collect();
        matched.sort();
        assert_eq!(matched, vec!["a.log".to_string(), "b.log".to_string()]);

        std::fs::remove_dir_all(&dir).ok();
    }
}
