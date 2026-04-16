use crate::error::OnyxResult;
use crate::meta::schema::*;
use crate::types::Pba;

use super::MetaStore;

impl MetaStore {
    pub fn get_refcount(&self, pba: Pba) -> OnyxResult<u32> {
        let cf = self.db.cf_handle(CF_REFCOUNT).unwrap();
        let key = encode_refcount_key(pba);
        match self.db.get_cf(&cf, &key)? {
            Some(data) => Ok(decode_refcount_value(&data).unwrap_or(0)),
            None => Ok(0),
        }
    }

    /// Batch-read refcounts for multiple PBAs in one RocksDB multi_get_cf call.
    pub fn multi_get_refcounts(&self, pbas: &[Pba]) -> OnyxResult<Vec<u32>> {
        if pbas.is_empty() {
            return Ok(Vec::new());
        }
        let cf = self.db.cf_handle(CF_REFCOUNT).unwrap();
        let keys: Vec<[u8; 8]> = pbas.iter().map(|pba| encode_refcount_key(*pba)).collect();
        let results = self
            .db
            .multi_get_cf(keys.iter().map(|k| (&cf, k.as_slice())));
        let mut out = Vec::with_capacity(pbas.len());
        for result in results {
            match result {
                Ok(Some(data)) => out.push(decode_refcount_value(&data).unwrap_or(0)),
                Ok(None) => out.push(0),
                Err(e) => return Err(crate::error::OnyxError::Meta(e)),
            }
        }
        Ok(out)
    }

    fn set_refcount_locked(&self, pba: Pba, count: u32) -> OnyxResult<()> {
        let cf = self.db.cf_handle(CF_REFCOUNT).unwrap();
        let key = encode_refcount_key(pba);
        if count == 0 {
            self.db.delete_cf(&cf, &key)?;
        } else {
            self.db.put_cf(&cf, &key, &encode_refcount_value(count))?;
        }
        Ok(())
    }

    pub fn set_refcount(&self, pba: Pba, count: u32) -> OnyxResult<()> {
        let _refcount_guards = self.lock_refcount_pbas([pba]);
        self.set_refcount_locked(pba, count)
    }

    pub fn increment_refcount(&self, pba: Pba) -> OnyxResult<u32> {
        let _refcount_guards = self.lock_refcount_pbas([pba]);
        let current = self.get_refcount(pba)?;
        let new_count = current + 1;
        self.set_refcount_locked(pba, new_count)?;
        Ok(new_count)
    }

    pub fn decrement_refcount(&self, pba: Pba) -> OnyxResult<u32> {
        let _refcount_guards = self.lock_refcount_pbas([pba]);
        let current = self.get_refcount(pba)?;
        let new_count = current.saturating_sub(1);
        self.set_refcount_locked(pba, new_count)?;
        Ok(new_count)
    }
}
