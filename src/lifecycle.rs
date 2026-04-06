use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

/// Per-volume lifecycle locks used to serialize same-name create/delete/open
/// transitions against foreground IO and flusher metadata commits.
#[derive(Default)]
pub struct VolumeLifecycleManager {
    locks: Mutex<HashMap<String, Arc<RwLock<()>>>>,
}

impl VolumeLifecycleManager {
    fn lock_for(&self, vol_id: &str) -> Arc<RwLock<()>> {
        let mut locks = self.locks.lock().unwrap();
        locks
            .entry(vol_id.to_string())
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone()
    }

    pub fn with_read_lock<T>(&self, vol_id: &str, f: impl FnOnce() -> T) -> T {
        let lock = self.lock_for(vol_id);
        let _guard = lock.read().unwrap();
        f()
    }

    pub fn with_write_lock<T>(&self, vol_id: &str, f: impl FnOnce() -> T) -> T {
        let lock = self.lock_for(vol_id);
        let _guard = lock.write().unwrap();
        f()
    }
}
