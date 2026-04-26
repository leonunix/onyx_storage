/// Marker for the existing RocksDB-backed metadata implementation.
///
/// The current `MetaStore` methods still live in `src/meta/store/*`. This
/// marker gives the metadb migration a stable namespace without moving the
/// production RocksDB code in the first integration commit.
#[derive(Debug, Default)]
pub(crate) struct RocksBackend;
