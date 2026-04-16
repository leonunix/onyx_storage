use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum FlushFailStage {
    BeforeIoWrite,
    BeforeMetaWrite,
}

#[derive(Debug, Clone)]
struct FlushFailRule {
    vol_id: String,
    start_lba: Lba,
    stage: FlushFailStage,
    remaining_hits: Option<u32>,
}

static TEST_FAIL_RULES: OnceLock<Mutex<Vec<FlushFailRule>>> = OnceLock::new();
static TEST_DEDUP_HIT_FAIL_RULES: OnceLock<Mutex<Vec<DedupHitFailRule>>> = OnceLock::new();
#[doc(hidden)]
pub struct PackedPauseState {
    hit: bool,
    released: bool,
}

struct PackedPauseHook {
    vol_id: String,
    state: Arc<(Mutex<PackedPauseState>, Condvar)>,
}

#[derive(Debug, Clone)]
struct DedupHitFailRule {
    vol_id: String,
    lba: Lba,
    remaining_hits: Option<u32>,
}

static TEST_PACKED_PAUSE_HOOK: OnceLock<Mutex<Option<PackedPauseHook>>> = OnceLock::new();

fn test_fail_rules() -> &'static Mutex<Vec<FlushFailRule>> {
    TEST_FAIL_RULES.get_or_init(|| Mutex::new(Vec::new()))
}

fn test_dedup_hit_fail_rules() -> &'static Mutex<Vec<DedupHitFailRule>> {
    TEST_DEDUP_HIT_FAIL_RULES.get_or_init(|| Mutex::new(Vec::new()))
}

fn test_packed_pause_hook() -> &'static Mutex<Option<PackedPauseHook>> {
    TEST_PACKED_PAUSE_HOOK.get_or_init(|| Mutex::new(None))
}

pub(super) fn maybe_inject_test_failure(
    vol_id: &str,
    start_lba: Lba,
    stage: FlushFailStage,
) -> OnyxResult<()> {
    let mut rules = test_fail_rules().lock().unwrap();
    if let Some(idx) = rules.iter().position(|rule| {
        rule.vol_id == vol_id && rule.start_lba == start_lba && rule.stage == stage
    }) {
        let mut remove = false;
        if let Some(remaining) = rules[idx].remaining_hits.as_mut() {
            if *remaining > 0 {
                *remaining -= 1;
            }
            if *remaining == 0 {
                remove = true;
            }
        }
        if remove {
            rules.remove(idx);
        }
        return Err(crate::error::OnyxError::Io(std::io::Error::other(format!(
            "injected flush failure at {:?} for {}:{}",
            stage, vol_id, start_lba.0
        ))));
    }
    Ok(())
}

pub(super) fn maybe_inject_test_failure_packed(
    fragments: &[crate::packer::packer::SlotFragment],
    stage: FlushFailStage,
) -> OnyxResult<()> {
    for frag in fragments {
        maybe_inject_test_failure(&frag.unit.vol_id, frag.unit.start_lba, stage)?;
    }
    Ok(())
}

pub(super) fn maybe_inject_dedup_hit_failure(vol_id: &str, lba: Lba) -> OnyxResult<()> {
    let mut rules = test_dedup_hit_fail_rules().lock().unwrap();
    if let Some(idx) = rules
        .iter()
        .position(|rule| rule.vol_id == vol_id && rule.lba == lba)
    {
        let mut remove = false;
        if let Some(remaining) = rules[idx].remaining_hits.as_mut() {
            if *remaining > 0 {
                *remaining -= 1;
            }
            if *remaining == 0 {
                remove = true;
            }
        }
        if remove {
            rules.remove(idx);
        }
        return Err(crate::error::OnyxError::Io(std::io::Error::other(format!(
            "injected dedup hit failure for {}:{}",
            vol_id, lba.0
        ))));
    }
    Ok(())
}

pub(super) fn maybe_pause_before_packed_meta_write(
    fragments: &[crate::packer::packer::SlotFragment],
) -> OnyxResult<()> {
    let state = {
        let hook = test_packed_pause_hook().lock().unwrap();
        let Some(hook) = hook.as_ref() else {
            return Ok(());
        };
        if !fragments.iter().any(|f| f.unit.vol_id == hook.vol_id) {
            return Ok(());
        }
        hook.state.clone()
    };

    let (lock, cv) = &*state;
    let mut guard = lock.lock().unwrap();
    guard.hit = true;
    cv.notify_all();
    while !guard.released {
        guard = cv.wait(guard).unwrap();
    }
    Ok(())
}

#[doc(hidden)]
pub fn install_test_failpoint(
    vol_id: &str,
    start_lba: Lba,
    stage: FlushFailStage,
    remaining_hits: Option<u32>,
) {
    let mut rules = test_fail_rules().lock().unwrap();
    rules.push(FlushFailRule {
        vol_id: vol_id.to_string(),
        start_lba,
        stage,
        remaining_hits,
    });
}

#[doc(hidden)]
pub fn clear_test_failpoint(vol_id: &str, start_lba: Lba, stage: FlushFailStage) {
    let mut rules = test_fail_rules().lock().unwrap();
    rules.retain(|rule| {
        !(rule.vol_id == vol_id && rule.start_lba == start_lba && rule.stage == stage)
    });
}

#[doc(hidden)]
pub fn install_test_dedup_hit_failpoint(vol_id: &str, lba: Lba, remaining_hits: Option<u32>) {
    let mut rules = test_dedup_hit_fail_rules().lock().unwrap();
    rules.push(DedupHitFailRule {
        vol_id: vol_id.to_string(),
        lba,
        remaining_hits,
    });
}

#[doc(hidden)]
pub fn clear_test_dedup_hit_failpoint(vol_id: &str, lba: Lba) {
    let mut rules = test_dedup_hit_fail_rules().lock().unwrap();
    rules.retain(|rule| !(rule.vol_id == vol_id && rule.lba == lba));
}

#[doc(hidden)]
pub fn install_test_packed_pause_hook(vol_id: &str) -> Arc<(Mutex<PackedPauseState>, Condvar)> {
    let state = Arc::new((
        Mutex::new(PackedPauseState {
            hit: false,
            released: false,
        }),
        Condvar::new(),
    ));
    let mut hook = test_packed_pause_hook().lock().unwrap();
    *hook = Some(PackedPauseHook {
        vol_id: vol_id.to_string(),
        state: state.clone(),
    });
    state
}

#[doc(hidden)]
pub fn clear_test_packed_pause_hook() {
    let mut hook = test_packed_pause_hook().lock().unwrap();
    *hook = None;
}

#[doc(hidden)]
pub fn wait_for_test_packed_pause_hit(
    state: &Arc<(Mutex<PackedPauseState>, Condvar)>,
    timeout: Duration,
) -> bool {
    let (lock, cv) = &**state;
    let guard = lock.lock().unwrap();
    let (guard, _) = cv.wait_timeout_while(guard, timeout, |s| !s.hit).unwrap();
    guard.hit
}

#[doc(hidden)]
pub fn release_test_packed_pause_hook(state: &Arc<(Mutex<PackedPauseState>, Condvar)>) {
    let (lock, cv) = &**state;
    let mut guard = lock.lock().unwrap();
    guard.released = true;
    cv.notify_all();
}
