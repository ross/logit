//! Writes `fuzz/seeds/<target>/*` from [`logit_fuzz_seedgen::generate`]. Each target directory
//! is cleared of generated seeds first; a `regress-*` file is a minimized crash input someone
//! committed by hand, and survives.
//!
//! `--check` writes nothing: it compares what `generate` produces with the committed seeds,
//! prints each added, removed, or changed file, and exits 1 on any difference. Both modes treat a
//! target directory no fuzz target uses as stale; write mode removes it unless it holds a
//! `regress-*` file, which it names and leaves.

use std::collections::BTreeSet;
use std::path::Path;
use std::process::ExitCode;

fn is_regress(path: &Path) -> bool {
    path.file_name().and_then(|name| name.to_str()).is_some_and(|name| name.starts_with("regress-"))
}

/// The generated (non-`regress-*`) files committed under `dir`, by name.
fn committed(dir: &Path) -> std::io::Result<BTreeSet<String>> {
    let mut names = BTreeSet::new();
    if !dir.is_dir() {
        return Ok(names);
    }
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_file() && !is_regress(&path) {
            if let Some(name) = path.file_name() {
                names.insert(name.to_string_lossy().into_owned());
            }
        }
    }
    Ok(names)
}

/// Target directories under `out` that `generate` no longer writes, each with the `regress-*`
/// files it holds. Write mode removes one with none and refuses one with any, because a
/// `regress-*` file is a crash input someone committed by hand.
fn orphans(
    seeds: &logit_fuzz_seedgen::Seeds,
    out: &Path,
) -> std::io::Result<Vec<(String, Vec<String>)>> {
    let mut found = Vec::new();
    for entry in std::fs::read_dir(out)? {
        let path = entry?.path();
        let Some(target) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !path.is_dir() || seeds.contains_key(target) {
            continue;
        }
        let mut regress = Vec::new();
        for file in std::fs::read_dir(&path)? {
            let file = file?.path();
            if is_regress(&file) {
                regress.push(file.file_name().unwrap_or_default().to_string_lossy().into_owned());
            }
        }
        regress.sort();
        found.push((target.to_owned(), regress));
    }
    found.sort();
    Ok(found)
}

fn check(seeds: &logit_fuzz_seedgen::Seeds, out: &Path) -> std::io::Result<ExitCode> {
    let mut differences = Vec::new();
    for (target, files) in seeds {
        let dir = out.join(target);
        let on_disk = committed(&dir)?;
        for (name, bytes) in files {
            if !on_disk.contains(name) {
                differences.push(format!("added:   fuzz/seeds/{target}/{name}"));
            } else if std::fs::read(dir.join(name))? != *bytes {
                differences.push(format!("changed: fuzz/seeds/{target}/{name}"));
            }
        }
        for name in on_disk.iter().filter(|name| !files.contains_key(*name)) {
            differences.push(format!("removed: fuzz/seeds/{target}/{name}"));
        }
    }
    for (target, regress) in orphans(seeds, out)? {
        if regress.is_empty() {
            differences.push(format!("removed: fuzz/seeds/{target}/ (no fuzz target writes it)"));
        } else {
            differences.push(format!(
                "orphan:  fuzz/seeds/{target}/ holds {}; remove the directory by hand",
                regress.join(", ")
            ));
        }
    }
    if differences.is_empty() {
        println!("fuzz seeds are current");
        return Ok(ExitCode::SUCCESS);
    }
    for line in &differences {
        eprintln!("{line}");
    }
    eprintln!(
        "fuzz seeds are stale ({} differences); run script/unsafe-check fuzz-seed and commit the result",
        differences.len()
    );
    Ok(ExitCode::FAILURE)
}

fn main() -> std::io::Result<ExitCode> {
    let check_only = match std::env::args().nth(1).as_deref() {
        None => false,
        Some("--check") => true,
        Some(other) => {
            eprintln!("usage: seedgen [--check] (got {other})");
            return Ok(ExitCode::from(2));
        }
    };
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let testdata = manifest.join("../../testdata");
    let out = manifest.join("../seeds");

    let (seeds, skipped) = logit_fuzz_seedgen::generate(&testdata)?;
    if check_only {
        return check(&seeds, &out);
    }
    let mut refused = false;
    for (target, regress) in orphans(&seeds, &out)? {
        if regress.is_empty() {
            std::fs::remove_dir_all(out.join(&target))?;
            println!("removed orphaned target directory {target}");
        } else {
            eprintln!(
                "fuzz/seeds/{target}/ is written by no target and holds {}; remove it by hand",
                regress.join(", ")
            );
            refused = true;
        }
    }
    for (target, files) in &seeds {
        let dir = out.join(target);
        std::fs::create_dir_all(&dir)?;
        for entry in std::fs::read_dir(&dir)? {
            let path = entry?.path();
            if path.is_file() && !is_regress(&path) {
                std::fs::remove_file(&path)?;
            }
        }
        for (name, bytes) in files {
            std::fs::write(dir.join(name), bytes)?;
        }
        let total: usize = files.values().map(Vec::len).sum();
        println!("{target:<18} {:>3} seeds {total:>8} bytes", files.len());
    }
    for seed in &skipped {
        println!("skipped (over {} bytes): {seed}", logit_fuzz_seedgen::MAX_SEED_BYTES);
    }
    Ok(if refused { ExitCode::FAILURE } else { ExitCode::SUCCESS })
}
