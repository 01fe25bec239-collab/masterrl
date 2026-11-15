//! Golden round-trip: every binary golden under `tests/contracts/golden/` (written by
//! `tools/gen/proto.py` with every field set) must decode into its prost type and encode back
//! to the same bytes. prost drops unknown fields, so a field missing on the Rust side fails.

use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;

type Roundtrip = fn(&[u8]) -> Result<Vec<u8>, prost::DecodeError>;

fn roundtrip<T: prost::Message + Default>(bytes: &[u8]) -> Result<Vec<u8>, prost::DecodeError> {
    Ok(T::decode(bytes)?.encode_to_vec())
}

const MESSAGES: &[(&str, Roundtrip)] = include!(concat!(env!("OUT_DIR"), "/messages.rs"));

fn goldens() -> Vec<(String, Vec<u8>)> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/contracts/golden");
    let mut out: Vec<(String, Vec<u8>)> = fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", dir.display()))
        .map(|entry| entry.expect("readable golden dir").path())
        .filter(|path| path.extension().is_some_and(|e| e == "binpb"))
        .filter(|path| path.file_name().is_some_and(|n| n != "descriptors.binpb"))
        .map(|path| {
            let name = path.file_name().expect("file name").to_string_lossy();
            let name = name.trim_end_matches(".binpb").to_string();
            (name, fs::read(&path).expect("readable golden"))
        })
        .collect();
    out.sort();
    out
}

/// `mrl.worker.v1.WorkerMessage.hello` -> `mrl.worker.v1.WorkerMessage`.
fn message_name(golden: &str) -> String {
    golden.split('.').take(4).collect::<Vec<_>>().join(".")
}

#[test]
fn every_golden_round_trips_byte_for_byte() {
    let goldens = goldens();
    assert!(!goldens.is_empty(), "no goldens found; run `just gen`");
    for (golden, bytes) in &goldens {
        let name = message_name(golden);
        let (_, roundtrip) = MESSAGES
            .iter()
            .find(|(n, _)| *n == name)
            .unwrap_or_else(|| panic!("golden {golden} names unknown message {name}"));
        let again = roundtrip(bytes).unwrap_or_else(|e| panic!("{golden}: decode failed: {e}"));
        assert_eq!(&again, bytes, "{golden}: re-encoded bytes differ");
    }
}

#[test]
fn every_message_has_a_golden() {
    let covered: BTreeSet<String> = goldens().iter().map(|(g, _)| message_name(g)).collect();
    let missing: Vec<&str> = MESSAGES
        .iter()
        .map(|(n, _)| *n)
        .filter(|n| !covered.contains(*n))
        .collect();
    assert!(missing.is_empty(), "messages without a golden: {missing:?}");
}
