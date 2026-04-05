use crate::error::{OnyxError, OnyxResult};
use crate::types::CompressionAlgo;

/// Compressor trait for pluggable compression algorithms
pub trait Compressor: Send + Sync {
    fn algo(&self) -> CompressionAlgo;

    /// Compress src into dst. Returns compressed size, or None if compression
    /// doesn't save space (compressed >= original).
    fn compress(&self, src: &[u8], dst: &mut [u8]) -> Option<usize>;

    /// Decompress src into dst. dst must be pre-sized to original_size.
    fn decompress(&self, src: &[u8], dst: &mut [u8], original_size: usize) -> OnyxResult<()>;

    /// Upper bound on compressed output size for a given input size.
    fn max_compressed_size(&self, input_len: usize) -> usize;
}

/// No compression — pass-through
pub struct NoneCompressor;

impl Compressor for NoneCompressor {
    fn algo(&self) -> CompressionAlgo {
        CompressionAlgo::None
    }

    fn compress(&self, src: &[u8], dst: &mut [u8]) -> Option<usize> {
        if dst.len() < src.len() {
            return None;
        }
        dst[..src.len()].copy_from_slice(src);
        Some(src.len())
    }

    fn decompress(&self, src: &[u8], dst: &mut [u8], original_size: usize) -> OnyxResult<()> {
        if src.len() < original_size || dst.len() < original_size {
            return Err(OnyxError::Compress("buffer too small".into()));
        }
        dst[..original_size].copy_from_slice(&src[..original_size]);
        Ok(())
    }

    fn max_compressed_size(&self, input_len: usize) -> usize {
        input_len
    }
}

/// LZ4 compression via lz4_flex
pub struct Lz4Compressor;

impl Compressor for Lz4Compressor {
    fn algo(&self) -> CompressionAlgo {
        CompressionAlgo::Lz4
    }

    fn compress(&self, src: &[u8], dst: &mut [u8]) -> Option<usize> {
        let compressed = lz4_flex::compress(src);
        if compressed.len() >= src.len() {
            return None;
        }
        if compressed.len() > dst.len() {
            return None;
        }
        dst[..compressed.len()].copy_from_slice(&compressed);
        Some(compressed.len())
    }

    fn decompress(&self, src: &[u8], dst: &mut [u8], original_size: usize) -> OnyxResult<()> {
        let decompressed = lz4_flex::decompress(src, original_size)
            .map_err(|e| OnyxError::Compress(format!("LZ4 decompress: {}", e)))?;
        if decompressed.len() != original_size {
            return Err(OnyxError::Compress(format!(
                "LZ4 size mismatch: got {}, expected {}",
                decompressed.len(),
                original_size
            )));
        }
        dst[..original_size].copy_from_slice(&decompressed);
        Ok(())
    }

    fn max_compressed_size(&self, input_len: usize) -> usize {
        lz4_flex::block::get_maximum_output_size(input_len)
    }
}

/// ZSTD compression
pub struct ZstdCompressor {
    pub level: i32,
}

impl Compressor for ZstdCompressor {
    fn algo(&self) -> CompressionAlgo {
        CompressionAlgo::Zstd { level: self.level }
    }

    fn compress(&self, src: &[u8], dst: &mut [u8]) -> Option<usize> {
        let compressed = zstd::encode_all(src, self.level).ok()?;
        if compressed.len() >= src.len() {
            return None;
        }
        if compressed.len() > dst.len() {
            return None;
        }
        dst[..compressed.len()].copy_from_slice(&compressed);
        Some(compressed.len())
    }

    fn decompress(&self, src: &[u8], dst: &mut [u8], original_size: usize) -> OnyxResult<()> {
        let decompressed = zstd::decode_all(src)
            .map_err(|e| OnyxError::Compress(format!("ZSTD decompress: {}", e)))?;
        if decompressed.len() != original_size {
            return Err(OnyxError::Compress(format!(
                "ZSTD size mismatch: got {}, expected {}",
                decompressed.len(),
                original_size
            )));
        }
        dst[..original_size].copy_from_slice(&decompressed);
        Ok(())
    }

    fn max_compressed_size(&self, input_len: usize) -> usize {
        // ZSTD worst case is slightly larger than input
        zstd::zstd_safe::compress_bound(input_len)
    }
}

/// Create a compressor for the given algorithm
pub fn create_compressor(algo: CompressionAlgo) -> Box<dyn Compressor> {
    match algo {
        CompressionAlgo::None => Box::new(NoneCompressor),
        CompressionAlgo::Lz4 => Box::new(Lz4Compressor),
        CompressionAlgo::Zstd { level } => Box::new(ZstdCompressor { level }),
    }
}
