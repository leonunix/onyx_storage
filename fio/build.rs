fn main() {
    // Deliberately NOT asserting on FIO_SOURCE_DIR.
    //
    // This crate compiles to a staticlib and includes none of fio's headers —
    // only `src/fio_bridge.c` does, and the Makefile checks for the tree
    // before it invokes `cc`. Asserting here bought nothing and cost the
    // ability to run `cargo test --manifest-path fio/Cargo.toml` anywhere but
    // a machine with a configured fio checkout, which is exactly where the
    // protocol framing tests need to run.
    println!("cargo:rerun-if-changed=src/fio_bridge.c");
    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-env-changed=FIO_SOURCE_DIR");
}
