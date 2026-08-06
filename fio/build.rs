use std::{env, path::PathBuf};

fn main() {
    let fio = PathBuf::from(env::var_os("FIO_SOURCE_DIR").expect(
        "FIO_SOURCE_DIR must point to the configured fio source tree",
    ));
    for required in ["fio.h", "config-host.h"] {
        assert!(fio.join(required).is_file(), "{} is missing", fio.join(required).display());
    }

    println!("cargo:rerun-if-changed=src/fio_bridge.c");
    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-env-changed=FIO_SOURCE_DIR");
}
