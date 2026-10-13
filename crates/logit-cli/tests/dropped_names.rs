//! Checks that every `logit.*.dropped` counter the shipped code names is documented in
//! `docs/design/internal-telemetry.md`, where an operator alerting on any nonzero `*.dropped`
//! (ADR `telemetry-loss-vocabulary`) looks it up.
//!
//! A name is a string literal of the form `"logit.<dotted lowercase>.dropped"` in a `.rs` file
//! under `crates/*/src`. Test code is left out, because a negative assertion names a retired
//! counter to show it's gone: a file whose name contains `test`, and each file from its first
//! inline `#[cfg(test)]` module on. A `#[cfg(test)] mod x;` declaration doesn't end the scan, since
//! shipped code follows it, and a `#[cfg(test)]` item outside an inline module is scanned too.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

fn rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            rust_sources(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// Every `"logit.<name>.dropped"` literal in `text`, without its quotes.
fn dropped_literals(text: &str) -> Vec<&str> {
    let mut names = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find("\"logit.") {
        let after = &rest[start + 1..];
        let end = after.find('"').unwrap_or(after.len());
        let name = &after[..end];
        if name.ends_with(".dropped")
            && name.chars().all(|c| c.is_ascii_lowercase() || c == '.' || c == '_')
        {
            names.push(name);
        }
        rest = &after[end.min(after.len())..];
    }
    names
}

/// `text` up to its first `#[cfg(test)]` attribute on an inline module: a `mod` line that doesn't
/// end in `;`. A `#[path]` module is a declaration, so it never ends the scan.
fn shipped_part(text: &str) -> &str {
    let mut offset = 0;
    let mut previous_was_cfg_test = false;
    for line in text.split_inclusive('\n') {
        let trimmed = line.trim_start();
        if previous_was_cfg_test
            && trimmed.starts_with("mod ")
            && !trimmed.trim_end().ends_with(';')
        {
            let cfg_line = text[..offset].trim_end_matches('\n').rfind('\n').map_or(0, |i| i + 1);
            return &text[..cfg_line];
        }
        previous_was_cfg_test = trimmed.trim_end() == "#[cfg(test)]";
        offset += line.len();
    }
    text
}

#[test]
fn every_dropped_counter_is_documented() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..").canonicalize().unwrap();
    let doc = fs::read_to_string(root.join("docs/design/internal-telemetry.md")).unwrap();
    let mut files = Vec::new();
    for krate in fs::read_dir(root.join("crates")).unwrap() {
        let src = krate.unwrap().path().join("src");
        if src.is_dir() {
            rust_sources(&src, &mut files);
        }
    }
    let mut names = BTreeSet::new();
    for file in &files {
        if file.file_name().unwrap().to_string_lossy().contains("test") {
            continue;
        }
        let text = fs::read_to_string(file).unwrap();
        names.extend(dropped_literals(shipped_part(&text)).into_iter().map(str::to_owned));
    }
    assert!(
        names.contains("logit.component.batches.dropped"),
        "the scan found no `logit.component.batches.dropped`, so it reads nothing: {names:?}"
    );
    let missing: Vec<_> = names.iter().filter(|name| !doc.contains(name.as_str())).collect();
    assert!(
        missing.is_empty(),
        "these counters are emitted but not in docs/design/internal-telemetry.md: {missing:?}"
    );
}

#[test]
fn the_scan_reads_literals_and_stops_at_test_code() {
    let text = "let a = \"logit.input.frames.dropped\";\nlet b = \"logit.input.frames\";\n\
                #[cfg(test)]\nfn helper() { \"logit.input.kernel.dropped\" }\n\
                #[cfg(test)]\nmod verification;\nlet c = \"logit.input.logs.dropped\";\n\
                #[cfg(test)]\nmod tests { \"logit.output.tags.dropped\" }\n";
    assert_eq!(
        dropped_literals(shipped_part(text)),
        ["logit.input.frames.dropped", "logit.input.kernel.dropped", "logit.input.logs.dropped"]
    );
    assert_eq!(dropped_literals(text).len(), 4);
}
