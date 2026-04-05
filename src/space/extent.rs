use crate::types::Pba;

/// A contiguous range of physical blocks
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Extent {
    pub start: Pba,
    pub count: u32,
}

impl Extent {
    pub fn new(start: Pba, count: u32) -> Self {
        Self { start, count }
    }

    pub fn single(pba: Pba) -> Self {
        Self {
            start: pba,
            count: 1,
        }
    }

    pub fn end_pba(&self) -> Pba {
        Pba(self.start.0 + self.count as u64)
    }

    pub fn contains(&self, pba: Pba) -> bool {
        pba.0 >= self.start.0 && pba.0 < self.end_pba().0
    }
}

/// Ordered by start PBA for BTreeSet
impl Ord for Extent {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.start.0.cmp(&other.start.0)
    }
}

impl PartialOrd for Extent {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
