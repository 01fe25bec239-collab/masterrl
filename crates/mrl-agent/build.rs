//! Embeds the Python sources and pinned environment used by built-in worker jobs.

use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

fn runtime_files(dir: &Path, out: &mut Vec<PathBuf>) -> io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let kind = entry.file_type()?;
        if kind.is_dir() && entry.file_name() != "__pycache__" {
            runtime_files(&path, out)?;
        } else if kind.is_file()
            && matches!(
                path.extension().and_then(|ext| ext.to_str()),
                Some("py" | "pyi" | "json" | "yaml" | "typed")
            )
        {
            out.push(path);
        }
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = Path::new(&std::env::var("CARGO_MANIFEST_DIR")?)
        .join("../../python")
        .canonicalize()?;
    let runtime = root.join("mrl-runtime/src");
    let mut files = Vec::new();
    runtime_files(&runtime, &mut files)?;
    for name in [
        "builtin-env/uv.lock",
        "builtin-env/pyproject.toml",
        "mrl-runtime/pyproject.toml",
    ] {
        let file = root.join(name);
        files.push(file);
    }
    files.sort();

    let output = Path::new(&std::env::var("OUT_DIR")?).join("builtin_runtime.rs");
    let mut out = BufWriter::new(std::fs::File::create(output)?);
    writeln!(out, "const FILES: &[(&str, &[u8])] = &[")?;
    for file in files {
        println!("cargo:rerun-if-changed={}", file.display());
        let name = file
            .strip_prefix(&root)?
            .to_string_lossy()
            .replace('\\', "/");
        writeln!(
            out,
            "    ({name:?}, include_bytes!({:?})),",
            file.to_string_lossy()
        )?;
    }
    writeln!(out, "];")?;
    out.flush()?;
    Ok(())
}
