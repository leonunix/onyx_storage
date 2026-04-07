mod common;

use std::thread;
use std::time::Duration;

use common::{patterned_payload, seeded_rng, EngineHarness, HarnessOptions, ShadowModel};
use onyx_storage::dedup::config::DedupConfig;
use onyx_storage::gc::config::GcConfig;
use onyx_storage::types::{CompressionAlgo, BLOCK_SIZE};
use rand::Rng;

#[test]
fn randomized_lifecycle_matches_shadow_model() {
    let mut env = EngineHarness::new(HarnessOptions {
        dedup: DedupConfig {
            enabled: false,
            ..Default::default()
        },
        ..Default::default()
    });
    let mut model = ShadowModel::default();
    let volume_names = ["vol-a", "vol-b", "vol-c"];

    for seed in [7_u64, 19, 43] {
        let mut rng = seeded_rng(seed);
        for step in 0..120_u64 {
            let existing: Vec<String> = model.volumes.keys().cloned().collect();
            let roll = rng.gen_range(0..100);

            if existing.is_empty() || roll < 12 {
                let candidates: Vec<&str> = volume_names
                    .iter()
                    .copied()
                    .filter(|name| !model.volumes.contains_key(*name))
                    .collect();
                if let Some(name) = candidates.get(rng.gen_range(0..candidates.len().max(1))) {
                    let size_blocks = [32_u64, 48, 64][rng.gen_range(0..3)];
                    let size_bytes = size_blocks * BLOCK_SIZE as u64;
                    env.engine()
                        .create_volume(name, size_bytes, CompressionAlgo::None)
                        .unwrap();
                    model.create_volume(name, size_bytes, CompressionAlgo::None);
                }
            } else if roll < 62 {
                let name = existing[rng.gen_range(0..existing.len())].clone();
                let volume = model.volumes.get(&name).unwrap().clone();
                let len_options = [512_usize, 1024, 2048, 3072, 4096, 8192, 12288];
                let len = len_options[rng.gen_range(0..len_options.len())]
                    .min(volume.size_bytes as usize);
                let max_offset = volume.size_bytes as usize - len;
                let offset = rng.gen_range(0..=max_offset) as u64;
                let payload = patterned_payload(&mut rng, seed ^ step, len);
                let handle = env.engine().open_volume(&name).unwrap();
                handle.write(offset, &payload).unwrap();
                assert_eq!(handle.read(offset, len).unwrap(), payload);
                model.write(&name, offset, &payload);
            } else if roll < 82 {
                let name = existing[rng.gen_range(0..existing.len())].clone();
                let volume = model.volumes.get(&name).unwrap().clone();
                let len_options = [256_usize, 512, 1536, 4096, 8192];
                let len = len_options[rng.gen_range(0..len_options.len())]
                    .min(volume.size_bytes as usize);
                let max_offset = volume.size_bytes as usize - len;
                let offset = rng.gen_range(0..=max_offset) as u64;
                let handle = env.engine().open_volume(&name).unwrap();
                let actual = handle.read(offset, len).unwrap();
                let expected = model.read(&name, offset, len);
                assert_eq!(actual, expected, "seed={seed} step={step}");
            } else if roll < 90 {
                let name = existing[rng.gen_range(0..existing.len())].clone();
                env.engine().delete_volume(&name).unwrap();
                model.delete_volume(&name);
            } else if roll < 96 {
                env.wait_for_drain(Duration::from_secs(5));
                env.reopen();
            } else {
                env.wait_for_drain(Duration::from_secs(5));
            }

            if step % 20 == 0 {
                env.wait_for_drain(Duration::from_secs(5));
                env.audit_consistency();
                env.assert_matches_model(&model);
            }
        }
    }

    env.wait_for_drain(Duration::from_secs(10));
    env.audit_consistency();
    env.assert_matches_model(&model);
    env.assert_no_internal_errors();
}

#[test]
fn metamorphic_workload_preserves_logical_image_across_configs() {
    let configs = [
        (
            "baseline-none",
            HarnessOptions {
                dedup: DedupConfig {
                    enabled: false,
                    ..Default::default()
                },
                ..Default::default()
            },
        ),
        (
            "dedup-lz4",
            HarnessOptions {
                dedup: DedupConfig {
                    enabled: true,
                    workers: 2,
                    ..Default::default()
                },
                ..Default::default()
            },
        ),
        (
            "dedup-zstd-gc",
            HarnessOptions {
                gc: GcConfig {
                    enabled: true,
                    scan_interval_ms: 20,
                    ..Default::default()
                },
                dedup: DedupConfig {
                    enabled: true,
                    workers: 2,
                    rescan_interval_ms: 20,
                    ..Default::default()
                },
                ..Default::default()
            },
        ),
    ];

    let mut canonical_model: Option<ShadowModel> = None;
    let mut canonical_images: Option<std::collections::BTreeMap<String, Vec<u8>>> = None;

    for (label, options) in configs {
        let mut env = EngineHarness::new(options);
        let mut model = ShadowModel::default();

        env.engine()
            .create_volume("alpha", 64 * BLOCK_SIZE as u64, CompressionAlgo::None)
            .unwrap();
        env.engine()
            .create_volume("beta", 64 * BLOCK_SIZE as u64, CompressionAlgo::None)
            .unwrap();
        model.create_volume("alpha", 64 * BLOCK_SIZE as u64, CompressionAlgo::None);
        model.create_volume("beta", 64 * BLOCK_SIZE as u64, CompressionAlgo::None);

        let shared_a = (0..(8 * BLOCK_SIZE as usize))
            .map(|idx| ((idx * 13 + 7) % 251) as u8)
            .collect::<Vec<_>>();
        let shared_b = (0..(8 * BLOCK_SIZE as usize))
            .map(|idx| ((idx * 29 + 11) % 253) as u8)
            .collect::<Vec<_>>();
        let unique_a = (0..(4 * BLOCK_SIZE as usize))
            .map(|idx| ((idx * 17 + 19) % 255) as u8)
            .collect::<Vec<_>>();
        let unique_b = (0..(4 * BLOCK_SIZE as usize))
            .map(|idx| ((idx * 7 + 101) % 256) as u8)
            .collect::<Vec<_>>();

        for (name, offset, payload) in [
            ("alpha", 0_u64, shared_a.clone()),
            ("alpha", 16 * BLOCK_SIZE as u64, shared_b.clone()),
            ("alpha", 40 * BLOCK_SIZE as u64, unique_a.clone()),
        ] {
            let handle = env.engine().open_volume(name).unwrap();
            handle.write(offset, &payload).unwrap();
            assert_eq!(handle.read(offset, payload.len()).unwrap(), payload);
            model.write(name, offset, &payload);
        }

        env.wait_for_drain(Duration::from_secs(5));

        for (name, offset, payload) in [
            ("beta", 0_u64, shared_a.clone()),
            ("beta", 24 * BLOCK_SIZE as u64, shared_b.clone()),
            ("beta", 48 * BLOCK_SIZE as u64, unique_a.clone()),
            ("alpha", 8 * BLOCK_SIZE as u64, unique_b.clone()),
        ] {
            let handle = env.engine().open_volume(name).unwrap();
            handle.write(offset, &payload).unwrap();
            assert_eq!(handle.read(offset, payload.len()).unwrap(), payload);
            model.write(name, offset, &payload);
        }

        env.wait_for_drain(Duration::from_secs(5));
        thread::sleep(Duration::from_millis(120));
        env.audit_consistency();
        env.assert_matches_model(&model);
        env.assert_no_internal_errors();

        let metrics = env.engine().metrics_snapshot();
        if label != "baseline-none" {
            assert!(
                metrics.dedup_hits > 0,
                "{label} should record dedup hits for duplicate payloads"
            );
        }
        let images = model
            .volumes
            .iter()
            .map(|(name, volume)| {
                let handle = env.engine().open_volume(name).unwrap();
                (name.clone(), handle.read(0, volume.data.len()).unwrap())
            })
            .collect::<std::collections::BTreeMap<_, _>>();

        if let Some(expected) = &canonical_model {
            assert_eq!(expected.volumes, model.volumes, "model drift in {label}");
        } else {
            canonical_model = Some(model.clone());
        }

        if let Some(expected_images) = &canonical_images {
            assert_eq!(expected_images, &images, "logical image drift in {label}");
        } else {
            canonical_images = Some(images);
        }

        env.reopen();
        env.wait_for_drain(Duration::from_secs(5));
        env.audit_consistency();
    }
}

#[test]
fn mixed_workload_with_periodic_restart_stays_consistent() {
    let mut env = EngineHarness::new(HarnessOptions {
        dedup: DedupConfig {
            enabled: false,
            ..Default::default()
        },
        ..Default::default()
    });
    let mut model = ShadowModel::default();
    let volume_names = ["mix-a", "mix-b"];

    for name in volume_names {
        env.engine()
            .create_volume(name, 128 * BLOCK_SIZE as u64, CompressionAlgo::None)
            .unwrap();
        model.create_volume(name, 128 * BLOCK_SIZE as u64, CompressionAlgo::None);
    }

    for round in 0..5_u64 {
        for (volume, base_block) in [("mix-a", 0_u64), ("mix-b", 32_u64)] {
            let handle = env.engine().open_volume(volume).unwrap();
            for step in 0..12_u64 {
                let offset = (base_block + step * 2) * BLOCK_SIZE as u64;
                let len = if step % 2 == 0 {
                    2 * BLOCK_SIZE as usize
                } else {
                    BLOCK_SIZE as usize
                };
                let byte = ((round * 41 + step * 9) & 0xFF) as u8;
                let payload = if step % 3 == 0 {
                    vec![byte; len]
                } else if step % 3 == 1 {
                    (0..len)
                        .map(|idx| byte.wrapping_add((idx % 64) as u8))
                        .collect::<Vec<_>>()
                } else {
                    vec![0u8; len]
                };
                handle.write(offset, &payload).unwrap();
                assert_eq!(handle.read(offset, len).unwrap(), payload);
                model.write(volume, offset, &payload);
            }
        }

        env.wait_for_drain(Duration::from_secs(10));
        env.audit_consistency();
        env.assert_matches_model(&model);
        env.reopen();
    }

    env.wait_for_drain(Duration::from_secs(10));
    env.audit_consistency();
    env.assert_matches_model(&model);
    env.assert_no_internal_errors();
}

#[test]
fn crash_recovery_replays_acknowledged_buffer_entries() {
    let mut env = EngineHarness::new(HarnessOptions {
        gc: GcConfig {
            enabled: false,
            ..Default::default()
        },
        dedup: DedupConfig {
            enabled: true,
            workers: 2,
            ..Default::default()
        },
        ..Default::default()
    });
    env.shutdown();

    let status = std::process::Command::new(env!("CARGO_BIN_EXE_stability_harness"))
        .arg("write-and-abort")
        .arg("--meta-dir")
        .arg(env.meta_path())
        .arg("--buffer")
        .arg(env.buffer_path())
        .arg("--data")
        .arg(env.data_path())
        .status()
        .unwrap();

    assert!(
        !status.success(),
        "helper must abort to simulate a crash, got {status:?}"
    );

    let mut reopened = env;
    reopened.reopen();
    reopened.wait_for_drain(Duration::from_secs(10));

    let handle = reopened.engine().open_volume("crash-vol").unwrap();
    assert_eq!(handle.read(0, 4096).unwrap(), vec![0xAA; 4096]);
    assert_eq!(handle.read(4096, 4096).unwrap(), vec![0xAA; 4096]);
    reopened.assert_no_internal_errors();
}
