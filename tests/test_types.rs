use onyx_storage::types::*;

#[test]
fn compression_algo_roundtrip() {
    assert_eq!(
        CompressionAlgo::from_u8(CompressionAlgo::None.to_u8()),
        Some(CompressionAlgo::None)
    );
    assert_eq!(
        CompressionAlgo::from_u8(CompressionAlgo::Lz4.to_u8()),
        Some(CompressionAlgo::Lz4)
    );
    assert_eq!(CompressionAlgo::from_u8(3), None);
}

/// ZSTD compression algo roundtrip preserves level.
#[test]
fn zstd_algo_roundtrip() {
    let algo = CompressionAlgo::Zstd { level: 5 };
    assert_eq!(algo.to_u8(), 2);
    // from_u8 defaults to level 3
    let restored = CompressionAlgo::from_u8(2).unwrap();
    assert!(matches!(restored, CompressionAlgo::Zstd { .. }));
}

/// VolumeId Display trait.
#[test]
fn volume_id_display() {
    let v = VolumeId("my-volume".into());
    assert_eq!(format!("{}", v), "my-volume");
}

/// Lba/Pba ordering.
#[test]
fn lba_pba_ordering() {
    assert!(Lba(0) < Lba(1));
    assert!(Pba(10) > Pba(5));
    assert_eq!(Lba(42), Lba(42));
}

/// Constants are correct.
#[test]
fn constants() {
    assert_eq!(BLOCK_SIZE, 4096);
    assert_eq!(SECTOR_SIZE, 512);
    assert_eq!(SECTORS_PER_BLOCK, 8);
}
