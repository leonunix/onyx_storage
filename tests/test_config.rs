use onyx_storage::config::*;

#[test]
fn parse_minimal_config() {
    let toml_str = r#"
[meta]
rocksdb_path = "/data/lv1/rocksdb"

[storage]
data_device = "/dev/vg0/lv3"

[buffer]
device = "/dev/vg0/lv2"

[ublk]
"#;
    let config: OnyxConfig = toml::from_str(toml_str).unwrap();
    assert_eq!(config.meta.block_cache_mb, 256);
    assert_eq!(config.storage.block_size, 4096);
    assert_eq!(config.buffer.capacity_mb, 16384);
    assert_eq!(config.ublk.nr_queues, 4);
}

#[test]
fn parse_full_config() {
    let toml_str = r#"
[meta]
rocksdb_path = "/data/rocksdb"
block_cache_mb = 512
wal_dir = "/data/wal"

[storage]
data_device = "/dev/vg0/data"
block_size = 4096
use_hugepages = true
default_compression = "Lz4"

[buffer]
device = "/dev/vg0/buf"
capacity_mb = 8192
flush_watermark_pct = 90

[ublk]
nr_queues = 8
queue_depth = 256
io_buf_bytes = 2097152
"#;
    let config: OnyxConfig = toml::from_str(toml_str).unwrap();
    assert_eq!(config.meta.block_cache_mb, 512);
    assert!(config.storage.use_hugepages);
    assert_eq!(config.buffer.flush_watermark_pct, 90);
    assert_eq!(config.ublk.nr_queues, 8);
}

/// Loading config from nonexistent file → error.
#[test]
fn load_nonexistent_config() {
    let result = OnyxConfig::load(std::path::Path::new("/nonexistent/config.toml"));
    assert!(result.is_err());
}

/// Loading config from invalid TOML → error.
#[test]
fn load_invalid_toml() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad.toml");
    std::fs::write(&path, "this is not valid toml {{{{").unwrap();
    let result = OnyxConfig::load(&path);
    assert!(result.is_err());
}
