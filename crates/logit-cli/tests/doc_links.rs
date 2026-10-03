//! Checks that every relative inline markdown link in the repo resolves to a file or directory.
//!
//! Anchors (`#heading`) are stripped and not verified; checking them against the target's
//! headings is the natural extension. Links inside fenced code blocks and inline code spans are
//! examples, not links, so they are skipped, as are `http(s)`, `mailto:`, and pure-anchor targets.
//! Build output, VCS and tool state, scratch space, and harness results are not authored docs, so
//! those directories are not walked, nor is any dot-directory.
//! `docs/adr/TEMPLATE.md` is allowlisted because its `slug.md` links are placeholders.

use std::fs;
use std::path::{Path, PathBuf};

const SKIP_DIRS: &[&str] = &[
    "target",
    "tmp",
    "node_modules",
    "fuzz/target",
    "fuzz/corpus",
    "fuzz/artifacts",
    "perf/results",
    "perf/bins",
];

const ALLOWLIST: &[&str] = &["docs/adr/TEMPLATE.md"];

fn collect(root: &Path, dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        if path.is_dir() {
            let rel = path.strip_prefix(root).unwrap().to_string_lossy().into_owned();
            // A bare name such as `target` is skipped at any depth; a path such as
            // `fuzz/target` only where it appears.
            if name.starts_with('.')
                || SKIP_DIRS.contains(&rel.as_str())
                || (!name.contains('/') && ["target", "node_modules"].contains(&name.as_str()))
            {
                continue;
            }
            collect(root, &path, out);
        } else if path.extension().is_some_and(|e| e == "md") {
            out.push(path);
        }
    }
}

/// Blanks inline code spans so a `](` inside one is not read as a link.
fn blank_code_spans(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut in_span = false;
    for c in line.chars() {
        if c == '`' {
            in_span = !in_span;
            out.push(' ');
        } else if in_span {
            out.push(' ');
        } else {
            out.push(c);
        }
    }
    out
}

/// The raw targets of every `](target)` on a line.
fn targets(line: &str) -> Vec<String> {
    let mut found = Vec::new();
    let mut rest = line;
    while let Some(at) = rest.find("](") {
        let body = &rest[at + 2..];
        let mut depth = 0usize;
        let mut end = None;
        for (i, c) in body.char_indices() {
            match c {
                '(' => depth += 1,
                ')' if depth == 0 => {
                    end = Some(i);
                    break;
                }
                ')' => depth -= 1,
                _ => {}
            }
        }
        let Some(end) = end else { break };
        found.push(body[..end].to_string());
        rest = &body[end..];
    }
    found
}

/// The path part of a link target, or `None` when it is not a relative file link.
fn link_path(raw: &str) -> Option<String> {
    let raw = raw.trim();
    let target = match raw.strip_prefix('<') {
        Some(inner) => inner.split('>').next().unwrap_or(""),
        None => raw.split_whitespace().next().unwrap_or(""),
    };
    if target.starts_with('#') || target.starts_with("mailto:") || target.contains("://") {
        return None;
    }
    let path = target.split('#').next().unwrap_or("");
    (!path.is_empty()).then(|| path.to_string())
}

#[test]
fn every_relative_markdown_link_resolves() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let root = root.canonicalize().unwrap();
    let mut files = Vec::new();
    collect(&root, &root, &mut files);
    files.sort();

    let mut broken = Vec::new();
    for file in &files {
        let rel = file.strip_prefix(&root).unwrap().to_string_lossy().into_owned();
        if ALLOWLIST.contains(&rel.as_str()) {
            continue;
        }
        let dir = file.parent().unwrap();
        let text = fs::read_to_string(file).unwrap();
        let mut fenced = false;
        for (n, line) in text.lines().enumerate() {
            let trimmed = line.trim_start();
            if trimmed.starts_with("```") || trimmed.starts_with("~~~") {
                fenced = !fenced;
                continue;
            }
            if fenced {
                continue;
            }
            for raw in targets(&blank_code_spans(line)) {
                let Some(path) = link_path(&raw) else { continue };
                let resolved = match path.strip_prefix('/') {
                    Some(abs) => root.join(abs),
                    None => dir.join(&path),
                };
                if !resolved.exists() {
                    broken.push(format!("{rel}:{} -> {path}", n + 1));
                }
            }
        }
    }
    assert!(
        broken.is_empty(),
        "{} broken relative markdown link(s):\n{}",
        broken.len(),
        broken.join("\n")
    );
}
