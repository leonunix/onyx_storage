pub(crate) mod backend;
pub mod schema;
pub mod store;

/// Classify a metadb persistence error message as fatal (durable-corruption /
/// unrecoverable) vs transient. Matches on the `Display` substrings the metadb
/// layer emits, so both the checkpoint worker (`MetaDbError`) and the
/// durability-watermark thread (the `OnyxError::Config` it gets wrapped in)
/// classify the same failures identically.
///
/// Fatal today:
/// - `"capacity exhausted"` — the meta device is full; the checkpoint aborted
///   cleanly (no corruption) but every retry will hit the same wall.
/// - `"persistence subsystem failed"` — metadb has declared its page store
///   unusable; a restart is required.
///
/// Everything else (transient IO, apply-gate contention) is non-fatal and only
/// contributes to the consecutive-failure count.
pub(crate) fn is_fatal_meta_failure(msg: &str) -> bool {
    msg.contains("capacity exhausted") || msg.contains("persistence subsystem failed")
}

#[cfg(test)]
mod tests {
    use super::is_fatal_meta_failure;

    #[test]
    fn classifies_fatal_metadb_failures() {
        // The Display strings the metadb layer actually emits, and the
        // OnyxError::Config wrapper the durability thread sees.
        assert!(is_fatal_meta_failure(
            "meta device capacity exhausted: need 10 pages, capacity 8"
        ));
        assert!(is_fatal_meta_failure(
            "Configuration error: metadb checkpoint failed: metadb persistence subsystem failed; restart required"
        ));
        // Transient / unrelated failures are not fatal.
        assert!(!is_fatal_meta_failure("IO error: disk hiccup"));
        assert!(!is_fatal_meta_failure("apply gate busy"));
        assert!(!is_fatal_meta_failure(""));
    }
}
