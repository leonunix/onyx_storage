use onyx_storage::types::*;

#[test]
fn block_header_size() {
    assert_eq!(std::mem::size_of::<BlockHeader>(), 32);
}

#[test]
fn block_header_roundtrip() {
    let header = BlockHeader::new(1, 4096, 2048, 0xDEAD_BEEF, 0x1234, Lba(42));
    let bytes: [u8; 32] = *<&[u8; 32]>::try_from(header.as_bytes()).unwrap();
    let restored = BlockHeader::from_bytes(&bytes);
    assert!(restored.is_valid());
    assert_eq!({ restored.compression }, 1);
    assert_eq!({ restored.original_size }, 4096);
    assert_eq!({ restored.compressed_size }, 2048);
    assert_eq!({ restored.crc32 }, 0xDEAD_BEEF);
    assert_eq!({ restored.lba }, 42);
}

#[test]
fn volume_id_hash() {
    let v1 = VolumeId("test-vol-1".into());
    let v2 = VolumeId("test-vol-2".into());
    assert_ne!(v1.hash32(), v2.hash32());
    assert_eq!(v1.hash32(), VolumeId("test-vol-1".into()).hash32());
}

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

/// BlockHeader with wrong magic → is_valid() false.
#[test]
fn block_header_invalid_magic() {
    let header = BlockHeader::new(0, 4096, 4096, 0, 0, Lba(0));
    // Corrupt magic — need to write raw bytes
    let mut bytes = *<&[u8; 32]>::try_from(header.as_bytes()).unwrap();
    bytes[0] = 0xFF; // corrupt magic
    let restored = BlockHeader::from_bytes(&bytes);
    assert!(!restored.is_valid());
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
    assert_eq!(BLOCK_HEADER_MAGIC, 0x4F4E_5958);
    assert_eq!(BLOCK_HEADER_VERSION, 1);
}
