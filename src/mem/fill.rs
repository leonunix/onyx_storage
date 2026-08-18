//! Cover-or-zero assembly of an O_DIRECT buffer.
//!
//! # Why this exists
//!
//! The LV3 writer used to `fill(0)` the whole buffer and then copy payloads over
//! it. That was 10.56 ms per writer cycle on the box (2026-08-17) zeroing bytes
//! that were overwritten on the very next line — `payload/slab` measured
//! **100.0 %**, so the zeroing was provably pure waste.
//!
//! Simply deleting the `fill(0)` is **not** correct once buffers come from
//! [`crate::mem::SlabArena`], because a recycled slot is dirty and the chunklet
//! write path submits the **whole buffer length**, not just the payload prefix
//! (`OwnedBatchOp::len = buffer.len()`). Handing dirty heap bytes to LV3 would be
//! an information leak (possibly another volume's plaintext) and would make the
//! on-disk content of padding nondeterministic.
//!
//! So the gaps have to be zeroed — exactly the gaps, and *all* of them. This type
//! makes that structural instead of arithmetic: the caller declares each payload
//! region in ascending order, and `SlabFill` zeroes everything in between and
//! after. When [`SlabFill::finish`] returns, **every byte of the buffer has either
//! been declared by the caller or zeroed here**; there is no third possibility and
//! no offset for the caller to compute wrong.
//!
//! The completeness of a *declared* region is the payload's own contract:
//! `CompressedPayload::copy_to` asserts `dst.len() >= self.len()` and writes
//! exactly `self.len()` bytes, and the writer always declares `payload_len()`.

/// Sequential cover-or-zero writer over one O_DIRECT buffer.
pub struct SlabFill<'a> {
    buf: &'a mut [u8],
    /// Every byte below this offset is either caller-declared or zeroed.
    covered: usize,
    zeroed: usize,
}

impl<'a> SlabFill<'a> {
    pub fn new(buf: &'a mut [u8]) -> Self {
        Self {
            buf,
            covered: 0,
            zeroed: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Declare `[offset, offset + len)` as caller-filled and return it for
    /// writing. Any gap since the previous region is zeroed first.
    ///
    /// Regions must be declared in ascending, non-overlapping order — that is
    /// how the writer already lays out a run (members are assigned disjoint
    /// ascending subextents in `write_passthrough_batch`), and it is what lets
    /// one cursor prove completeness. Overlap or a backwards region is a caller
    /// bug: it would mean two units share bytes on disk.
    pub fn region(&mut self, offset: usize, len: usize) -> &mut [u8] {
        assert!(
            offset >= self.covered,
            "slab regions must be ascending and disjoint: offset {} < covered {}",
            offset,
            self.covered
        );
        let end = offset
            .checked_add(len)
            .expect("slab region length overflows");
        assert!(
            end <= self.buf.len(),
            "slab region {}..{} exceeds the {}-byte buffer",
            offset,
            end,
            self.buf.len()
        );
        if offset > self.covered {
            self.buf[self.covered..offset].fill(0);
            self.zeroed += offset - self.covered;
        }
        self.covered = end;
        &mut self.buf[offset..end]
    }

    /// Zero the trailing gap and return the number of bytes this fill had to
    /// zero in total. A healthy full-stripe cycle returns 0.
    pub fn finish(mut self) -> usize {
        let total = self.buf.len();
        if self.covered < total {
            self.buf[self.covered..].fill(0);
            self.zeroed += total - self.covered;
            self.covered = total;
        }
        self.zeroed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every byte is either declared or zeroed — the property the removed
    /// blanket `fill(0)` used to provide.
    #[test]
    fn gaps_before_between_and_after_regions_are_zeroed() {
        let mut buf = vec![0xA5u8; 64];
        let mut fill = SlabFill::new(&mut buf);
        fill.region(8, 4).fill(0x11);
        fill.region(20, 8).fill(0x22);
        let zeroed = fill.finish();

        assert_eq!(zeroed, 64 - 4 - 8);
        assert!(buf[..8].iter().all(|&b| b == 0));
        assert!(buf[8..12].iter().all(|&b| b == 0x11));
        assert!(buf[12..20].iter().all(|&b| b == 0));
        assert!(buf[20..28].iter().all(|&b| b == 0x22));
        assert!(buf[28..].iter().all(|&b| b == 0));
    }

    /// The measured production shape: payload covers every byte, so nothing is
    /// zeroed at all. This is the 10.56 ms/cycle that used to be spent.
    #[test]
    fn fully_covered_buffer_zeroes_nothing() {
        let mut buf = vec![0xA5u8; 24 * 1024];
        let mut fill = SlabFill::new(&mut buf);
        for i in 0..6 {
            fill.region(i * 4096, 4096).fill(0x33);
        }
        assert_eq!(fill.finish(), 0);
        assert!(buf.iter().all(|&b| b == 0x33));
    }

    #[test]
    fn empty_declaration_still_zeroes_everything() {
        let mut buf = vec![0xA5u8; 4096];
        assert_eq!(SlabFill::new(&mut buf).finish(), 4096);
        assert!(buf.iter().all(|&b| b == 0));
    }

    /// A zero-length payload must not leave its allocated blocks dirty.
    #[test]
    fn zero_length_region_leaves_no_dirty_bytes() {
        let mut buf = vec![0xA5u8; 8192];
        let mut fill = SlabFill::new(&mut buf);
        fill.region(0, 0);
        assert_eq!(fill.finish(), 8192);
        assert!(buf.iter().all(|&b| b == 0));
    }

    #[test]
    #[should_panic(expected = "ascending and disjoint")]
    fn overlapping_regions_panic() {
        let mut buf = vec![0u8; 64];
        let mut fill = SlabFill::new(&mut buf);
        fill.region(16, 16);
        fill.region(24, 8);
    }

    #[test]
    #[should_panic(expected = "exceeds the")]
    fn region_past_the_end_panics() {
        let mut buf = vec![0u8; 64];
        let mut fill = SlabFill::new(&mut buf);
        fill.region(32, 64);
    }
}
