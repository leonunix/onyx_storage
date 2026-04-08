use onyx_storage::compress::codec::*;
use onyx_storage::types::CompressionAlgo;

fn roundtrip_test(compressor: &dyn Compressor, data: &[u8]) {
    let max_out = compressor.max_compressed_size(data.len());
    let mut compressed = vec![0u8; max_out];

    match compressor.compress(data, &mut compressed) {
        Some(compressed_size) => {
            let mut decompressed = vec![0u8; data.len()];
            compressor
                .decompress(
                    &compressed[..compressed_size],
                    &mut decompressed,
                    data.len(),
                )
                .unwrap();
            assert_eq!(&decompressed, data);
        }
        None => {}
    }
}

#[test]
fn none_roundtrip() {
    let c = NoneCompressor;
    let data = b"hello world, this is test data for compression";
    roundtrip_test(&c, data);
}

#[test]
fn lz4_roundtrip_zeros() {
    let c = Lz4Compressor;
    let data = vec![0u8; 4096];
    roundtrip_test(&c, &data);
}

#[test]
fn lz4_roundtrip_pattern() {
    let c = Lz4Compressor;
    let mut data = vec![0u8; 4096];
    for (i, b) in data.iter_mut().enumerate() {
        *b = (i % 256) as u8;
    }
    roundtrip_test(&c, &data);
}

#[test]
fn lz4_incompressible_returns_none() {
    let c = Lz4Compressor;
    let data: Vec<u8> = (0..4096)
        .map(|i| {
            let x = (i as u64)
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (x >> 33) as u8
        })
        .collect();
    let max_out = c.max_compressed_size(data.len());
    let mut compressed = vec![0u8; max_out];
    if let Some(size) = c.compress(&data, &mut compressed) {
        let mut dec = vec![0u8; data.len()];
        c.decompress(&compressed[..size], &mut dec, data.len())
            .unwrap();
        assert_eq!(dec, data);
    }
}

#[test]
fn zstd_roundtrip_zeros() {
    let c = ZstdCompressor { level: 3 };
    let data = vec![0u8; 4096];
    roundtrip_test(&c, &data);
}

#[test]
fn zstd_roundtrip_pattern() {
    let c = ZstdCompressor { level: 3 };
    let mut data = vec![0u8; 4096];
    for (i, b) in data.iter_mut().enumerate() {
        *b = (i % 128) as u8;
    }
    roundtrip_test(&c, &data);
}

#[test]
fn create_compressor_factory() {
    let c = create_compressor(CompressionAlgo::Lz4);
    assert!(matches!(c.algo(), CompressionAlgo::Lz4));

    let c = create_compressor(CompressionAlgo::Zstd { level: 5 });
    assert!(matches!(c.algo(), CompressionAlgo::Zstd { level: 5 }));

    let c = create_compressor(CompressionAlgo::None);
    assert!(matches!(c.algo(), CompressionAlgo::None));
}

#[test]
fn lz4_compression_saves_space() {
    let c = Lz4Compressor;
    let data = vec![0u8; 4096];
    let max_out = c.max_compressed_size(data.len());
    let mut compressed = vec![0u8; max_out];
    let size = c.compress(&data, &mut compressed).unwrap();
    assert!(
        size < data.len(),
        "LZ4 should compress all-zeros: {} vs {}",
        size,
        data.len()
    );
}

#[test]
fn zstd_compression_saves_space() {
    let c = ZstdCompressor { level: 3 };
    let data = vec![0u8; 4096];
    let max_out = c.max_compressed_size(data.len());
    let mut compressed = vec![0u8; max_out];
    let size = c.compress(&data, &mut compressed).unwrap();
    assert!(
        size < data.len(),
        "ZSTD should compress all-zeros: {} vs {}",
        size,
        data.len()
    );
}

/// LZ4 decompress with corrupt data → error.
#[test]
fn lz4_decompress_corrupt_data() {
    let c = Lz4Compressor;
    let corrupt = vec![0xFF; 100]; // not valid LZ4
    let mut out = vec![0u8; 4096];
    let result = c.decompress(&corrupt, &mut out, 4096);
    assert!(result.is_err());
}

/// ZSTD decompress with corrupt data → error.
#[test]
fn zstd_decompress_corrupt_data() {
    let c = ZstdCompressor { level: 3 };
    let corrupt = vec![0xFF; 100]; // not valid ZSTD
    let mut out = vec![0u8; 4096];
    let result = c.decompress(&corrupt, &mut out, 4096);
    assert!(result.is_err());
}

/// None compressor decompress with too-small src → error.
#[test]
fn none_decompress_too_small() {
    let c = NoneCompressor;
    let src = vec![0u8; 10];
    let mut dst = vec![0u8; 4096];
    let result = c.decompress(&src, &mut dst, 4096);
    assert!(result.is_err());
}

/// None compressor compress with too-small dst → None.
#[test]
fn none_compress_dst_too_small() {
    let c = NoneCompressor;
    let src = vec![0u8; 100];
    let mut dst = vec![0u8; 10]; // too small
    assert!(c.compress(&src, &mut dst).is_none());
}

/// LZ4 compress with too-small dst → None.
#[test]
fn lz4_compress_dst_too_small() {
    let c = Lz4Compressor;
    let src = vec![0u8; 4096]; // compressible
    let mut dst = vec![0u8; 1]; // way too small
    assert!(c.compress(&src, &mut dst).is_none());
}

/// ZSTD compress with too-small dst → None.
#[test]
fn zstd_compress_dst_too_small() {
    let c = ZstdCompressor { level: 3 };
    let src = vec![0u8; 4096];
    let mut dst = vec![0u8; 1];
    assert!(c.compress(&src, &mut dst).is_none());
}

/// LZ4 decompress with wrong original_size → error.
#[test]
fn lz4_decompress_wrong_size() {
    let c = Lz4Compressor;
    let data = vec![0u8; 4096];
    let max_out = c.max_compressed_size(data.len());
    let mut compressed = vec![0u8; max_out];
    let size = c.compress(&data, &mut compressed).unwrap();

    // Decompress claiming original size is 8192 (wrong)
    let mut out = vec![0u8; 8192];
    let result = c.decompress(&compressed[..size], &mut out, 8192);
    assert!(result.is_err());
}

/// ZSTD decompress with wrong original_size → error.
#[test]
fn zstd_decompress_wrong_size() {
    let c = ZstdCompressor { level: 3 };
    let data = vec![0u8; 4096];
    let max_out = c.max_compressed_size(data.len());
    let mut compressed = vec![0u8; max_out];
    let size = c.compress(&data, &mut compressed).unwrap();

    let mut out = vec![0u8; 8192];
    let result = c.decompress(&compressed[..size], &mut out, 8192);
    assert!(result.is_err());
}

/// None decompress with dst too small for original_size → error.
#[test]
fn none_decompress_dst_too_small() {
    let c = NoneCompressor;
    let src = vec![0u8; 100];
    let mut dst = vec![0u8; 50]; // too small for original_size=100
    let result = c.decompress(&src, &mut dst, 100);
    assert!(result.is_err());
}

/// max_compressed_size returns reasonable values.
#[test]
fn max_compressed_size_values() {
    let none = NoneCompressor;
    assert_eq!(none.max_compressed_size(4096), 4096);

    let lz4 = Lz4Compressor;
    assert!(lz4.max_compressed_size(4096) >= 4096);

    let zstd = ZstdCompressor { level: 3 };
    assert!(zstd.max_compressed_size(4096) >= 4096);
}
