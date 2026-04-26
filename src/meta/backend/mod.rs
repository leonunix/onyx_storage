#![allow(dead_code)]

pub(crate) mod codec;
pub(crate) mod rocks;

#[cfg(feature = "metadb")]
pub(crate) mod metadb;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MetaBackendKind {
    Rocks,
    #[cfg(feature = "metadb")]
    Metadb,
}

impl MetaBackendKind {
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Rocks => "rocksdb",
            #[cfg(feature = "metadb")]
            Self::Metadb => "metadb",
        }
    }
}
