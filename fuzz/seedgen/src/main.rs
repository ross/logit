//! Writes `fuzz/seeds/<target>/*` from [`logit_fuzz_seedgen::generate`]. Each target directory
//! is cleared of generated seeds first; a `regress-*` file is a minimized crash input someone
//! committed by hand, and survives.

use std::path::Path;

fn main() -> std::io::Result<()> {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let testdata = manifest.join("../../testdata");
    let out = manifest.join("../seeds");

    let (seeds, skipped) = logit_fuzz_seedgen::generate(&testdata)?;
    for (target, files) in &seeds {
        let dir = out.join(target);
        std::fs::create_dir_all(&dir)?;
        for entry in std::fs::read_dir(&dir)? {
            let path = entry?.path();
            let regress = path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("regress-"));
            if path.is_file() && !regress {
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
    Ok(())
}
