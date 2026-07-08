//! onyx-side chunklet operator jobs: spawn fsck / rebalance background jobs
//! against a real sparse-file pool and poll them to completion. The underlying
//! pool ops are covered exhaustively in the chunklet crate; this pins onyx's
//! job glue (spawn → run real op → format the report → registry polling), which
//! is the surface the IPC handlers + CLI drive.

use onyx_storage::chunklet_ops::{self, ChunkletJobView};
use onyx_storage::config::{ChunkletConfig, ChunkletIoBackend};
use std::time::{Duration, Instant};

fn poll_done(id: u64) -> ChunkletJobView {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let v = chunklet_ops::job_view(id).expect("job exists");
        if v.state != "running" {
            return v;
        }
        assert!(Instant::now() < deadline, "job {id} timed out: {v:?}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn fsck_and_rebalance_jobs_run_and_report() {
    let dir = tempfile::tempdir().unwrap();
    let devices: Vec<_> = (0..8).map(|i| dir.path().join(format!("pd{i}"))).collect();
    for p in &devices {
        std::fs::File::create(p).unwrap().set_len(4 << 30).unwrap();
    }
    let cfg = ChunkletConfig {
        enabled: true,
        devices,
        io_backend: ChunkletIoBackend::Sync,
        spare_pct: 0,
        ..Default::default()
    };
    let (pool, ..) = onyx_storage::chunklet_pool::init_pool(&cfg).unwrap();

    // fsck: complete fresh pool → runs, reclaims nothing, not skipped.
    let fid = chunklet_ops::start_fsck(&pool).unwrap();
    let f = poll_done(fid);
    assert_eq!(f.state, "done", "fsck detail={}", f.detail);
    assert!(
        f.detail.contains("skipped_incomplete=false") && f.detail.contains("reclaimed=0"),
        "fresh complete pool has no orphans: {}",
        f.detail
    );

    // rebalance: a freshly-allocated pool is already balanced → converges
    // without getting stuck (0 or a few trivial moves).
    let rid = chunklet_ops::start_rebalance(&pool, 20.0, 8).unwrap();
    let r = poll_done(rid);
    assert_eq!(r.state, "done", "rebalance detail={}", r.detail);
    assert!(
        r.detail.contains("stuck=false"),
        "balanced pool must not be stuck: {}",
        r.detail
    );
}
