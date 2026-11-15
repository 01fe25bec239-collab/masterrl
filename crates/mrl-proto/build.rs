//! Compiles every file under `proto/` into Rust (prost messages, tonic clients and servers).
//! protox parses the protos, so no system `protoc` is needed. Also writes `messages.rs`, the
//! full-name -> Rust-type table the golden round-trip test walks.

use std::error::Error;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::{env, fs};

fn proto_files(dir: &Path, root: &Path, out: &mut Vec<String>) -> Result<(), Box<dyn Error>> {
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            proto_files(&path, root, out)?;
        } else if path.extension().is_some_and(|e| e == "proto") {
            let rel = path
                .strip_prefix(root)?
                .to_string_lossy()
                .replace('\\', "/");
            out.push(rel);
        }
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let root = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?).join("../../proto");
    println!("cargo:rerun-if-changed={}", root.display());

    let mut files = Vec::new();
    proto_files(&root.join("mrl"), &root, &mut files)?;
    files.sort();
    let fds = protox::compile(&files, [&root])?;

    let mut table = String::from("&[\n");
    for file in fds.file.iter().filter(|f| f.package().starts_with("mrl.")) {
        let package = file.package();
        let module = package.trim_start_matches("mrl.").replace('.', "::");
        for message in &file.message_type {
            let name = message.name();
            writeln!(
                table,
                "    (\"{package}.{name}\", roundtrip::<crate::{module}::{name}>),"
            )?;
        }
    }
    table.push(']');
    fs::write(
        PathBuf::from(env::var("OUT_DIR")?).join("messages.rs"),
        table,
    )?;

    // BTreeMap keeps map encoding sorted by key, the same order as protobuf's deterministic
    // serialization in Python, so both languages produce identical bytes.
    tonic_prost_build::configure()
        .btree_map(".")
        .compile_fds(fds)?;
    Ok(())
}
