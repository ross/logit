//! A minimal glob: a literal path, or one `*` in the final path component
//! (`/var/log/app/*.log`) matching any run of characters, including none. No `**`, `?`, `[...]`,
//! or escaping (`docs/adr/file-tailing-and-docker-json-logs.md`'s "Alternatives considered"); a
//! second `*` in the final component is matched literally. Graph rule 26 rejects a `tail_in` `*`
//! outside the final component. [`PathPattern::new`] itself rejects nothing: a pattern that can't
//! match never matches, like an empty directory.
//!
//! [`PathPattern::docker_containers`] isn't a glob: it's `docker_in`'s two-level walk under
//! `root`.

use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
enum Matcher {
    /// No `*` in the final component: matches one name.
    Literal(String),
    /// The final component split at its first `*`; either half may be empty.
    Wildcard { prefix: String, suffix: String },
    /// `docker_in`'s discovery; see [`PathPattern::docker_containers`].
    DockerContainers,
}

/// One `paths:`/discovery entry: the directory to scan and how to match a name in it.
///
/// Never recursive: every caller names a directory, not a subtree. `docker_in`'s two-level walk
/// is the one fixed exception.
#[derive(Debug, Clone)]
pub struct PathPattern {
    dir: PathBuf,
    matcher: Matcher,
}

impl PathPattern {
    /// `path`'s parent becomes the scanned directory and its final component the matcher.
    ///
    /// A bare `"app.log"` gets an empty directory, whose `read_dir` fails, so it never matches.
    /// Neither caller builds one.
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

    /// `docker_in`'s discovery: every `<root>/<id>/<id>-json.log` where `<root>/<id>` is a
    /// directory, following Docker's naming. An unreadable or empty `root` yields no matches until
    /// a later scan.
    pub fn docker_containers(root: impl Into<PathBuf>) -> Self {
        Self { dir: root.into(), matcher: Matcher::DockerContainers }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    // Every matcher, `DockerContainers` included, needs only `dir` watched
    // (`Tailer::reconcile_watches`). A container directory arriving or leaving is a direct child
    // of `root`; a log file appearing inside an existing container directory, its rotation, and a
    // `config.v2.json` change wait for the `poll_interval` tick
    // (`docs/adr/docker-container-identity-and-minimal-watches.md`). A tailed file gets its own
    // watch (`Tailer::open_tracked`, `Watcher::watch_file`).

    fn matches_name(&self, name: &str) -> bool {
        match &self.matcher {
            Matcher::Literal(literal) => name == literal,
            Matcher::Wildcard { prefix, suffix } => {
                // The length check stops prefix and suffix overlapping (`x*x` must not match
                // `x`); `>=`, not `>`, lets `*` match zero characters (`x*y` matches `xy`).
                name.len() >= prefix.len() + suffix.len()
                    && name.starts_with(prefix.as_str())
                    && name.ends_with(suffix.as_str())
            }
            Matcher::DockerContainers => false, // scan() below never calls this for this variant
        }
    }

    /// Every currently matching path: one `read_dir`, or for `docker_in` one subdirectory level
    /// deeper.
    ///
    /// Blocking, but only [`crate::tail::driver::Tailer::scan`] calls it, on the rescan cadence,
    /// never per line. A `read_dir` failure, including a directory that doesn't exist yet, returns
    /// no matches rather than an error.
    pub fn scan(&self) -> Vec<PathBuf> {
        if self.matcher == Matcher::DockerContainers {
            return self.scan_docker_containers();
        }
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

    fn scan_docker_containers(&self) -> Vec<PathBuf> {
        let mut out = Vec::new();
        let Ok(entries) = std::fs::read_dir(&self.dir) else {
            return out;
        };
        for entry in entries.flatten() {
            let Ok(is_dir) = entry.file_type().map(|t| t.is_dir()) else { continue };
            if !is_dir {
                continue; // `config.v2.json` and the log live inside the id directory
            }
            let Ok(name) = entry.file_name().into_string() else { continue };
            let log_path = self.dir.join(&name).join(format!("{name}-json.log"));
            if log_path.is_file() {
                out.push(log_path);
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
        // The empty-prefix case; `docker_in` discovery isn't a glob and is tested below.
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

    #[test]
    fn docker_containers_finds_one_log_per_container_directory() {
        let root = crate::tail::test_support::scratch_dir("pattern-docker");
        for id in [
            "aaaa000000000000000000000000000000000000000000000000000000000000",
            "bbbb111111111111111111111111111111111111111111111111111111111111",
        ] {
            let dir = root.join(id);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join(format!("{id}-json.log")), b"").unwrap();
            std::fs::write(dir.join("config.v2.json"), b"{}").unwrap();
        }
        // Not a container directory.
        std::fs::write(root.join("stray-file.log"), b"").unwrap();

        let p = PathPattern::docker_containers(&root);
        let mut matched: Vec<PathBuf> = p.scan();
        matched.sort();
        let mut expected: Vec<PathBuf> = vec![
            root.join("aaaa000000000000000000000000000000000000000000000000000000000000")
                .join("aaaa000000000000000000000000000000000000000000000000000000000000-json.log"),
            root.join("bbbb111111111111111111111111111111111111111111111111111111111111")
                .join("bbbb111111111111111111111111111111111111111111111111111111111111-json.log"),
        ];
        expected.sort();
        assert_eq!(matched, expected);
        assert_eq!(p.dir(), root.as_path());

        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn docker_containers_skips_a_directory_missing_its_own_json_log() {
        let root = crate::tail::test_support::scratch_dir("pattern-docker-incomplete");
        std::fs::create_dir_all(root.join("no-log-here")).unwrap();

        let p = PathPattern::docker_containers(&root);
        assert_eq!(p.scan(), Vec::<PathBuf>::new());

        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn docker_containers_dir_is_just_root_regardless_of_what_it_currently_holds() {
        // Only `root` is watched, whatever it holds
        // (`docs/adr/docker-container-identity-and-minimal-watches.md`).
        let root = crate::tail::test_support::scratch_dir("pattern-docker-dir");
        let id_a = "aaaa000000000000000000000000000000000000000000000000000000000000";
        let id_b = "bbbb111111111111111111111111111111111111111111111111111111111111";
        std::fs::create_dir_all(root.join(id_a)).unwrap();
        std::fs::write(root.join(id_a).join(format!("{id_a}-json.log")), b"").unwrap();
        std::fs::create_dir_all(root.join(id_b)).unwrap();
        std::fs::write(root.join("stray.txt"), b"").unwrap();

        let p = PathPattern::docker_containers(&root);
        assert_eq!(p.dir(), root.as_path());

        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn a_wildcard_patterns_dir_is_its_own_directory() {
        let dir = crate::tail::test_support::scratch_dir("pattern-wildcard-dir");
        let p = PathPattern::new(dir.join("*.log"));
        assert_eq!(p.dir(), dir.as_path());
        std::fs::remove_dir_all(&dir).ok();
    }
}
