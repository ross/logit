//! A minimal glob: a literal path, or one `*` in the final path component
//! (`/var/log/app/*.log`) matching any run of characters, including none. No `**`, `?`, `[...]`,
//! or escaping (`docs/adr/file-tailing-and-docker-json-logs.md`'s "Alternatives considered"); a
//! second `*` in the final component is matched literally. Graph rule 26 rejects a `tail_in` `*`
//! outside the final component. [`PathPattern::new`] itself rejects nothing: a pattern that can't
//! match never matches, like an empty directory.
//!
//! [`PathPattern::docker_containers`] isn't a glob: it's `docker_in`'s two-level walk under
//! `root`. Nor is [`PathPattern::pod_logs`], `cri_in`'s three-level walk of the kubelet's pod log
//! directory.
//!
//! **A failure to look is no information.** A scan that couldn't list a directory or `stat` a
//! path says nothing about whether a tracked file is still there, so `Tailer::scan` (`driver.rs`)
//! retires no tracked file such a failure could have named
//! (`docs/adr/tail-discovery-failure-and-resume-identity.md`, decision 1). What each operation
//! reports, here and in `Tailer::scan`:
//!
//! | Operation | Ok | `NotFound` / `NotADirectory` | Other error |
//! |---|---|---|---|
//! | `read_dir(dir)`, any matcher | list | `Ok(Scan::default())`: an empty listing, not counted (the "not there yet" case) | `Err`: the listing failed |
//! | a `ReadDir` iterator item | use | `Err` (the listing is incomplete) | `Err` |
//! | pods `read_dir` of a pod or container directory, or an item of it | list | absent (removed mid-walk) | [`Scan::failed_dirs`]: that subtree's listing failed |
//! | pods `entry.file_type()` of a pod or container directory | a directory is walked; anything else is absent (a symlink isn't followed) | absent | [`Scan::failed_dirs`] |
//! | docker `entry.file_type()` | a directory is walked; anything else is absent (a symlink isn't followed) | absent | [`Scan::unknown`] (the built log path) |
//! | docker log-path `metadata` | a regular file is matched; anything else is absent | absent (the log isn't written yet; a later scan finds it) | [`Scan::unknown`] |
//! | `Tailer::scan`'s per-path `metadata`, once per distinct matched path | a regular file, or a symlink to one (a cri-dockerd pods entry), is discovered as its target's inode; anything else is absent | absent, a dangling symlink included | unknown |
//!
//! A name that isn't UTF-8 is skipped: no pattern can name it. A tracked path that is absent is
//! retired. A tracked path that is unknown, or that a failed listing [covers](PathPattern::covers),
//! is kept as it is.

use logit_pipeline::fault::{self, sites, Op, Point};
use std::fs::DirEntry;
use std::io;
use std::path::{Path, PathBuf};

/// The fault-seam point before a pattern directory's `read_dir`, and before each entry of it.
pub(crate) const READ_DIR: Point = Point::new(sites::TAIL_SCAN, Op::ReadDir);
/// The fault-seam point before a `stat`: `docker_in`'s two per-container checks (the `root`
/// entry's type and the log's `metadata`, both keyed on the built log path) and
/// `Tailer::scan`'s per-path `metadata`.
pub(crate) const STAT: Point = Point::new(sites::TAIL_SCAN, Op::Stat);
/// The fault-seam point before `Tailer::open_tracked` opens a discovered file.
pub(crate) const OPEN: Point = Point::new(sites::TAIL_SCAN, Op::Open);
/// The fault-seam point before `Tailer::open_tracked` reads a resumed file's head.
pub(crate) const HEAD_READ: Point = Point::new(sites::TAIL_SCAN, Op::Read);

/// Whether a failed `read_dir` or `stat` means the path isn't there, rather than that it couldn't
/// be looked at. See the module doc's table.
pub(crate) fn is_absent(err: &io::Error) -> bool {
    matches!(err.kind(), io::ErrorKind::NotFound | io::ErrorKind::NotADirectory)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Matcher {
    /// No `*` in the final component: matches one name.
    Literal(String),
    /// The final component split at its first `*`; either half may be empty.
    Wildcard { prefix: String, suffix: String },
    /// `docker_in`'s discovery; see [`PathPattern::docker_containers`].
    DockerContainers,
    /// `cri_in`'s discovery; see [`PathPattern::pod_logs`].
    PodLogs,
}

/// What one [`PathPattern::scan`] found.
#[derive(Debug, Default)]
pub struct Scan {
    /// Every path this pattern names now.
    pub matched: Vec<PathBuf>,
    /// Paths this pattern may name that couldn't be checked (`docker_in` only): neither matched
    /// nor absent.
    pub unknown: Vec<PathBuf>,
    /// The error that put `unknown[0]` there, for the driver's `scan_error` diagnostic.
    pub first_unknown_error: Option<io::Error>,
    /// Directories below the pattern's own whose listing failed (`cri_in` only), with the error:
    /// no path under one is known to be gone.
    pub failed_dirs: Vec<(PathBuf, io::Error)>,
}

impl Scan {
    fn push_unknown(&mut self, path: PathBuf, err: io::Error) {
        if self.unknown.is_empty() {
            self.first_unknown_error = Some(err);
        }
        self.unknown.push(path);
    }
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
    /// A bare `"app.log"` gets an empty directory, whose `read_dir` fails with `NotFound`, so it
    /// never matches. Neither caller builds one.
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
    /// directory, following Docker's naming. A missing or empty `root` yields no matches until a
    /// later scan; an unreadable one is an error ([`PathPattern::scan`]).
    pub fn docker_containers(root: impl Into<PathBuf>) -> Self {
        Self { dir: root.into(), matcher: Matcher::DockerContainers }
    }

    /// `cri_in`'s discovery: every `<root>/<namespace>_<pod>_<uid>/<container>/<file>` whose pod
    /// directory splits into its three parts ([`pod_dir_parts`]), whose container directory
    /// name is valid ([`is_container_dir`]), and whose file name is a live or uncompressed
    /// rotated log ([`log_restart_count`]). Directories are walked without following a symlink;
    /// a file name is matched whatever it is, and `Tailer::scan`'s `stat` decides. Anything
    /// else is skipped uncounted, as a name outside a glob is (`docs/adr/cri-container-logs.md`,
    /// section 2).
    pub fn pod_logs(root: impl Into<PathBuf>) -> Self {
        Self { dir: root.into(), matcher: Matcher::PodLogs }
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
            // scan() below never calls this for these variants
            Matcher::DockerContainers | Matcher::PodLogs => false,
        }
    }

    /// Whether this pattern could name `path`, whatever is on disk now: for a glob, `path` is in
    /// [`PathPattern::dir`] and its name matches; for `docker_in`, it's
    /// `<root>/<name>/<name>-json.log`. `Tailer::scan` keeps every tracked path a failed listing
    /// covers.
    pub fn covers(&self, path: &Path) -> bool {
        let file_name = path.file_name().and_then(|n| n.to_str());
        match &self.matcher {
            Matcher::DockerContainers => {
                let Some(container_dir) = path.parent() else { return false };
                let dir_name = container_dir.file_name().and_then(|n| n.to_str());
                container_dir.parent() == Some(self.dir.as_path())
                    && dir_name.is_some_and(|dir_name| {
                        file_name.and_then(|f| f.strip_suffix("-json.log")) == Some(dir_name)
                    })
            }
            Matcher::PodLogs => {
                let container_dir = path.parent();
                let pod_dir = container_dir.and_then(Path::parent);
                pod_dir.and_then(Path::parent) == Some(self.dir.as_path())
                    && name_of(pod_dir).and_then(pod_dir_parts).is_some()
                    && name_of(container_dir).is_some_and(is_container_dir)
                    && file_name.and_then(log_restart_count).is_some()
            }
            Matcher::Literal(_) | Matcher::Wildcard { .. } => {
                path.parent() == Some(self.dir.as_path())
                    && file_name.is_some_and(|name| self.matches_name(name))
            }
        }
    }

    /// Every path this pattern names now: one `read_dir`, and for `docker_in` one `stat` per
    /// container directory. The module doc's table says what each failure reports.
    ///
    /// Blocking, but only [`crate::tail::driver::Tailer::scan`] calls it, on the rescan cadence,
    /// never per line.
    pub fn scan(&self) -> io::Result<Scan> {
        let entries = match fault::check(READ_DIR, &self.dir, 0)
            .and_then(|()| std::fs::read_dir(&self.dir))
        {
            Ok(entries) => entries,
            Err(err) if is_absent(&err) => return Ok(Scan::default()),
            Err(err) => return Err(err),
        };
        let mut out = Scan::default();
        for entry in entries {
            // An entry's error fails the whole listing: a partial one can't say what's gone.
            let entry = fault::check(READ_DIR, &self.dir, 0).and(entry)?;
            let Ok(name) = entry.file_name().into_string() else {
                continue; // non-UTF-8 name: can't match a str-based pattern, skip it
            };
            if self.matcher == Matcher::DockerContainers {
                self.scan_container(&entry, &name, &mut out);
            } else if self.matcher == Matcher::PodLogs {
                if pod_dir_parts(&name).is_some() {
                    self.scan_pod(&entry, self.dir.join(name), &mut out);
                }
            } else if self.matches_name(&name) {
                out.matched.push(self.dir.join(name));
            }
        }
        Ok(out)
    }

    /// One pod directory of `cri_in`'s walk: each valid container directory in it, and each log
    /// name in those.
    fn scan_pod(&self, entry: &DirEntry, pod_dir: PathBuf, out: &mut Scan) {
        if !walkable(entry, &pod_dir, out) {
            return;
        }
        let Some(containers) = list_dir(&pod_dir, out) else { return };
        for (name, entry) in containers {
            if !is_container_dir(&name) {
                continue;
            }
            let container_dir = pod_dir.join(&name);
            if !walkable(&entry, &container_dir, out) {
                continue;
            }
            let Some(files) = list_dir(&container_dir, out) else { continue };
            for (file, _) in files {
                if log_restart_count(&file).is_some() {
                    out.matched.push(container_dir.join(file));
                }
            }
        }
    }

    /// One `root` entry of `docker_in`'s walk: `<root>/<name>/<name>-json.log` if `name` is a
    /// directory holding that log.
    fn scan_container(&self, entry: &DirEntry, name: &str, out: &mut Scan) {
        let log_path = self.dir.join(name).join(format!("{name}-json.log"));
        match fault::check(STAT, &log_path, 0).and_then(|()| entry.file_type()) {
            Ok(file_type) if file_type.is_dir() => {}
            // `config.v2.json` and the log live inside the id directory.
            Ok(_) => return,
            Err(err) if is_absent(&err) => return,
            Err(err) => {
                out.push_unknown(log_path, err);
                return;
            }
        }
        match fault::check(STAT, &log_path, 0).and_then(|()| std::fs::metadata(&log_path)) {
            Ok(meta) if meta.is_file() => out.matched.push(log_path),
            Ok(_) => {}
            Err(err) if is_absent(&err) => {}
            Err(err) => out.push_unknown(log_path, err),
        }
    }
}

/// Whether a directory `entry` at `path` is one to walk: a directory, not a symlink to one. A type
/// that can't be read puts `path` in [`Scan::failed_dirs`].
fn walkable(entry: &DirEntry, path: &Path, out: &mut Scan) -> bool {
    match fault::check(STAT, path, 0).and_then(|()| entry.file_type()) {
        Ok(file_type) => file_type.is_dir(),
        Err(err) if is_absent(&err) => false,
        Err(err) => {
            out.failed_dirs.push((path.to_path_buf(), err));
            false
        }
    }
}

/// `dir`'s UTF-8 entries, or `None` when it's gone (absent) or its listing failed (recorded in
/// [`Scan::failed_dirs`]). An entry's error fails the whole listing, as for `root`.
fn list_dir(dir: &Path, out: &mut Scan) -> Option<Vec<(String, DirEntry)>> {
    let listing = fault::check(READ_DIR, dir, 0).and_then(|()| std::fs::read_dir(dir));
    let entries = match listing {
        Ok(entries) => entries,
        Err(err) if is_absent(&err) => return None,
        Err(err) => {
            out.failed_dirs.push((dir.to_path_buf(), err));
            return None;
        }
    };
    let mut names = Vec::new();
    for entry in entries {
        match fault::check(READ_DIR, dir, 0).and(entry) {
            Ok(entry) => {
                if let Ok(name) = entry.file_name().into_string() {
                    names.push((name, entry));
                }
            }
            Err(err) if is_absent(&err) => return None,
            Err(err) => {
                out.failed_dirs.push((dir.to_path_buf(), err));
                return None;
            }
        }
    }
    Some(names)
}

/// `path`'s final component, when there is one and it's UTF-8.
pub(crate) fn name_of(path: Option<&Path>) -> Option<&str> {
    path.and_then(Path::file_name).and_then(|n| n.to_str())
}

/// A kubelet pod directory name, `<namespace>_<pod name>_<pod uid>`, split at its first two `_`:
/// a namespace and a pod name can't hold one, and the rest is the uid, which a static pod's
/// manifest can set to anything. `None` unless all three parts are non-empty.
pub(crate) fn pod_dir_parts(name: &str) -> Option<(&str, &str, &str)> {
    let mut parts = name.splitn(3, '_');
    let namespace = parts.next().filter(|p| !p.is_empty())?;
    let pod = parts.next().filter(|p| !p.is_empty())?;
    let uid = parts.next().filter(|p| !p.is_empty())?;
    Some((namespace, pod, uid))
}

/// Whether `name` can be a kubelet container directory: non-empty, with no `_` or `.`, which a
/// container name (a DNS label) can't hold.
pub(crate) fn is_container_dir(name: &str) -> bool {
    !name.is_empty() && !name.contains(['_', '.'])
}

/// The restart count a kubelet log file name carries: `<n>.log`, the live file, or
/// `<n>.log.<YYYYMMDD-HHMMSS>`, a rotated one not yet compressed, where `<n>` is decimal digits
/// that fit an `i64`. `None` for every other name, `.gz` and `.tmp` included.
pub(crate) fn log_restart_count(name: &str) -> Option<i64> {
    let (count, rest) = name.split_once(".log")?;
    let rotated = match rest.strip_prefix('.') {
        None if rest.is_empty() => true,
        Some(stamp) => is_rotation_stamp(stamp),
        None => false,
    };
    if !rotated || count.is_empty() || !count.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    count.parse().ok()
}

/// `YYYYMMDD-HHMMSS`, the kubelet's rotation suffix, by shape.
fn is_rotation_stamp(stamp: &str) -> bool {
    let b = stamp.as_bytes();
    b.len() == 15
        && b[8] == b'-'
        && b[..8].iter().all(u8::is_ascii_digit)
        && b[9..].iter().all(u8::is_ascii_digit)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pod_log_names_follow_the_kubelet_layout() {
        assert_eq!(pod_dir_parts("default_web-0_1b2c-3d"), Some(("default", "web-0", "1b2c-3d")));
        assert_eq!(
            pod_dir_parts("kube-system_etcd_my_static_uid"),
            Some(("kube-system", "etcd", "my_static_uid"))
        );
        for bad in ["default_web", "default__uid", "_web_uid", "default_web_", "plain"] {
            assert_eq!(pod_dir_parts(bad), None, "{bad}");
        }
        assert!(is_container_dir("nginx"));
        for bad in ["", "a_b", "a.b"] {
            assert!(!is_container_dir(bad), "{bad}");
        }
        assert_eq!(log_restart_count("0.log"), Some(0));
        assert_eq!(log_restart_count("12.log.20261030-141516"), Some(12));
        assert_eq!(log_restart_count("9223372036854775807.log"), Some(i64::MAX));
        for bad in [
            "0.log.20261030-141516.gz",
            "0.log.tmp",
            "0.log.1",
            "0.log.new",
            "0.log.2026103-141516",
            "0.log.20261030_141516",
            ".log",
            "+1.log",
            "x.log",
            "9223372036854775808.log",
            "0.logx",
        ] {
            assert_eq!(log_restart_count(bad), None, "{bad}");
        }
    }

    /// A scratch pods tree: one valid pod with a live, a rotated, a compressed, and a tmp file,
    /// and the shapes the walk skips.
    fn pods_tree(label: &str) -> (PathBuf, Vec<PathBuf>) {
        let root = crate::tail::test_support::scratch_dir(label);
        let container = root.join("default_web-0_0123-abcd").join("nginx");
        std::fs::create_dir_all(&container).unwrap();
        for name in ["0.log", "0.log.20261030-141516", "0.log.20261029-101010.gz", "0.log.tmp"] {
            std::fs::write(container.join(name), b"").unwrap();
        }
        for skipped in ["default_web-0", "notapod", "default_web-0_0123-abcd/bad.name"] {
            std::fs::create_dir_all(root.join(skipped)).unwrap();
            std::fs::write(root.join(skipped).join("0.log"), b"").unwrap();
        }
        std::fs::create_dir_all(root.join("default_web-0_0123-abcd/bad.name/x")).unwrap();
        std::fs::write(root.join("default_web-0_0123-abcd/bad.name/x/0.log"), b"").unwrap();
        // A symlinked container directory isn't followed.
        std::os::unix::fs::symlink(&container, root.join("default_web-0_0123-abcd/linked"))
            .unwrap();
        let mut want = vec![container.join("0.log"), container.join("0.log.20261030-141516")];
        want.sort();
        (root, want)
    }

    #[test]
    fn pod_logs_finds_live_and_uncompressed_rotated_files_only() {
        let (root, want) = pods_tree("pattern-pods");
        let p = PathPattern::pod_logs(&root);
        let scan = p.scan().unwrap();
        let mut matched = scan.matched;
        matched.sort();
        assert_eq!(matched, want);
        assert!(scan.unknown.is_empty() && scan.failed_dirs.is_empty());
        assert_eq!(p.dir(), root.as_path());
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn pod_logs_covers_agrees_with_scan() {
        let (root, want) = pods_tree("pattern-pods-covers");
        let p = PathPattern::pod_logs(&root);
        let mut every = Vec::new();
        let mut stack = vec![root.clone()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).unwrap() {
                let path = entry.unwrap().path();
                if path.symlink_metadata().unwrap().is_dir() {
                    stack.push(path);
                } else {
                    every.push(path);
                }
            }
        }
        assert!(every.len() > want.len());
        for path in &every {
            assert_eq!(p.covers(path), want.contains(path), "{}", path.display());
        }
        assert!(!p
            .covers(&root.with_file_name("elsewhere").join("default_web-0_0123-abcd/nginx/0.log")));
        std::fs::remove_dir_all(&root).ok();
    }

    /// An unreadable container directory is a failed subtree, not an empty one: the driver keeps
    /// what it tracks there.
    #[test]
    fn pod_logs_reports_a_failed_subdirectory_listing() {
        let (root, want) = pods_tree("pattern-pods-failed");
        let other = root.join("default_db-0_4567-ef01").join("pg");
        std::fs::create_dir_all(&other).unwrap();
        std::fs::write(other.join("0.log"), b"").unwrap();

        let scope = fault::scope(&other);
        scope.fail(READ_DIR, fault::errno::EACCES);
        let scan = PathPattern::pod_logs(&root).scan().unwrap();
        let mut matched = scan.matched;
        matched.sort();
        assert_eq!(matched, want);
        assert_eq!(scan.failed_dirs.len(), 1);
        assert_eq!(scan.failed_dirs[0].0, other);
        assert_eq!(scan.failed_dirs[0].1.raw_os_error(), Some(fault::errno::EACCES));
        drop(scope);
        std::fs::remove_dir_all(&root).ok();
    }

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

    fn is_empty(scan: &Scan) -> bool {
        scan.matched.is_empty() && scan.unknown.is_empty()
    }

    #[test]
    fn scanning_a_missing_directory_is_an_empty_listing_not_an_error() {
        let p = PathPattern::new("/this/path/does/not/exist/*.log");
        assert!(is_empty(&p.scan().expect("a missing directory is not a failed listing")));
        let docker = PathPattern::docker_containers("/this/path/does/not/exist");
        assert!(is_empty(&docker.scan().expect("a missing root is not a failed listing")));
    }

    #[test]
    fn scanning_a_directory_path_that_is_a_file_is_an_empty_listing() {
        let dir = crate::tail::test_support::scratch_dir("pattern-enotdir");
        std::fs::write(dir.join("plain"), b"").unwrap();

        // `read_dir` of a regular file fails `ENOTDIR`: the directory isn't there.
        let p = PathPattern::new(dir.join("plain").join("*.log"));
        assert!(is_empty(&p.scan().expect("ENOTDIR is an empty listing")));
        let docker = PathPattern::docker_containers(dir.join("plain"));
        assert!(is_empty(&docker.scan().expect("ENOTDIR is an empty listing")));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn scan_reports_a_failed_read_dir_as_an_error_not_an_empty_match() {
        let dir = crate::tail::test_support::scratch_dir("pattern-read-dir-fails");
        std::fs::write(dir.join("a.log"), b"").unwrap();
        let id = "c".repeat(64);
        std::fs::create_dir_all(dir.join(&id)).unwrap();
        std::fs::write(dir.join(&id).join(format!("{id}-json.log")), b"").unwrap();

        let scope = fault::scope(&dir);
        scope.fail(READ_DIR, fault::errno::EACCES);
        for p in [PathPattern::new(dir.join("*.log")), PathPattern::docker_containers(&dir)] {
            let err = p.scan().expect_err("a failed listing must not read as no matches");
            assert_eq!(err.raw_os_error(), Some(fault::errno::EACCES), "{p:?}");
        }
        drop(scope);

        assert_eq!(PathPattern::new(dir.join("*.log")).scan().unwrap().matched.len(), 1);
        assert_eq!(PathPattern::docker_containers(&dir).scan().unwrap().matched.len(), 1);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Two containers under a scratch `root`, and each one's log path: `bad`'s first.
    fn two_containers(label: &str) -> (PathBuf, PathBuf, PathBuf) {
        let root = crate::tail::test_support::scratch_dir(label);
        let logs: Vec<PathBuf> = ["d".repeat(64), "e".repeat(64)]
            .iter()
            .map(|id| {
                std::fs::create_dir_all(root.join(id)).unwrap();
                let log = root.join(id).join(format!("{id}-json.log"));
                std::fs::write(&log, b"").unwrap();
                log
            })
            .collect();
        (root, logs[0].clone(), logs[1].clone())
    }

    /// The `root` entry's type check is the first `Stat` point under a container directory.
    #[test]
    fn docker_containers_reports_an_unreadable_container_dir_as_unknown() {
        let (root, bad, good) = two_containers("pattern-docker-unreadable-dir");

        let scope = fault::scope(bad.parent().unwrap());
        scope.fail_nth(STAT, 1, fault::errno::EACCES).record();
        let scan = PathPattern::docker_containers(&root).scan().unwrap();
        assert_eq!(scan.matched, vec![good]);
        assert_eq!(scan.unknown, vec![bad.clone()]);
        assert_eq!(
            scan.first_unknown_error.and_then(|e| e.raw_os_error()),
            Some(fault::errno::EACCES)
        );
        assert!(scope.hits().is_empty(), "an unreadable container isn't stat'd further");
        drop(scope);

        std::fs::remove_dir_all(&root).ok();
    }

    /// The log's `metadata` is the second `Stat` point under a container directory.
    #[test]
    fn docker_containers_reports_an_unstatable_log_as_unknown() {
        let (root, bad, good) = two_containers("pattern-docker-unknown");

        let scope = fault::scope(bad.parent().unwrap());
        scope.fail_nth(STAT, 2, fault::errno::EACCES);
        let scan = PathPattern::docker_containers(&root).scan().unwrap();
        assert_eq!(scan.matched, vec![good.clone()]);
        assert_eq!(scan.unknown, vec![bad.clone()]);
        drop(scope);

        // Absent, not unknown: an injected `ENOENT` is a log not written yet.
        let scope = fault::scope(bad.parent().unwrap());
        scope.fail_nth(STAT, 2, fault::errno::ENOENT);
        let scan = PathPattern::docker_containers(&root).scan().unwrap();
        assert_eq!(scan.matched, vec![good]);
        assert!(scan.unknown.is_empty());
        drop(scope);

        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn covers_agrees_with_scan_for_both_matchers() {
        let dir = crate::tail::test_support::scratch_dir("pattern-covers");
        for name in ["a.log", "b.log", "b.log.1", "readme.txt"] {
            std::fs::write(dir.join(name), b"").unwrap();
        }
        let id = "f".repeat(64);
        std::fs::create_dir_all(dir.join(&id)).unwrap();
        std::fs::write(dir.join(&id).join(format!("{id}-json.log")), b"").unwrap();
        std::fs::write(dir.join(&id).join(format!("{id}-json.log.1")), b"").unwrap();
        std::fs::write(dir.join(&id).join("config.v2.json"), b"{}").unwrap();

        let patterns = [
            PathPattern::new(dir.join("*.log")),
            PathPattern::new(dir.join("b.log*")),
            PathPattern::new(dir.join("a.log")),
            PathPattern::docker_containers(&dir),
        ];
        // Every file under `dir`, one and two levels down.
        let mut every_path = Vec::new();
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                every_path.extend(std::fs::read_dir(&path).unwrap().map(|e| e.unwrap().path()));
            } else {
                every_path.push(path);
            }
        }
        for p in &patterns {
            let matched = p.scan().unwrap().matched;
            assert!(!matched.is_empty(), "{p:?} is vacuous");
            for path in &every_path {
                assert_eq!(p.covers(path), matched.contains(path), "{p:?} on {}", path.display());
            }
        }
        // A path elsewhere is never covered, whatever its name.
        let elsewhere = dir.with_file_name("elsewhere");
        assert!(!patterns[0].covers(&elsewhere.join("a.log")));
        assert!(!patterns[3].covers(&elsewhere.join(&id).join(format!("{id}-json.log"))));
        assert!(!patterns[3].covers(&dir.join(&id).join("other-json.log")));

        std::fs::remove_dir_all(&dir).ok();
    }

    proptest::proptest! {
        /// `x*y` matches the names that start with `x`, end with `y`, and are long enough
        /// for the two not to overlap; a literal pattern matches only itself.
        #[test]
        fn matches_name_agrees_with_the_prefix_suffix_model(
            prefix in "[ab-]{0,3}",
            suffix in "[ab-]{0,3}",
            name in "[ab-]{0,7}",
        ) {
            let p = PathPattern::new(format!("/d/{prefix}*{suffix}"));
            let model = name.starts_with(&prefix)
                && name.ends_with(&suffix)
                && name.len() >= prefix.len() + suffix.len();
            proptest::prop_assert_eq!(p.matches_name(&name), model);

            // `/d/` alone would parse as the literal `d` in `/`.
            let literal = format!("{prefix}{suffix}");
            if !literal.is_empty() {
                let p = PathPattern::new(format!("/d/{literal}"));
                proptest::prop_assert_eq!(p.matches_name(&name), name == literal);
            }
        }
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
            .unwrap()
            .matched
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
        let mut matched: Vec<PathBuf> = p.scan().unwrap().matched;
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
        let scan = p.scan().unwrap();
        assert_eq!(scan.matched, Vec::<PathBuf>::new());
        assert!(scan.unknown.is_empty(), "a log not written yet is absent, not unknown");

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
