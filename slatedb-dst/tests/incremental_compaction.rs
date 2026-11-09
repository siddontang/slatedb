//! Checks reads and recovery after an incremental compaction commit.
#![cfg(dst)]

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use rand::Rng;
use rstest::rstest;
use slatedb::admin::Admin;
use slatedb::compactor::{
    CompactionScheduler, CompactionSchedulerSupplier, CompactionSpec, CompactionStatus, Compactor,
    CompactorStateView, SourceId,
};
use slatedb::config::{
    CompactionWorkerOptions, CompactorOptions, DbReaderOptions, FlushOptions, FlushType,
    ScanOptions,
};
use slatedb::{
    CompactorBuilder, Db, DbRand, DbReader, Error, IterationOrder, Settings, WriteBatch,
};
use slatedb_common::clock::{MockSystemClock, SystemClock};
use slatedb_dst::{
    utils::dst_seeds, Actor, ActorCtx, Harness, Operation, StreamDirection, Toxic, ToxicKind,
};

const KEY_COUNT: usize = 1024;

#[rstest]
#[case::single_range(1)]
#[case::parallel_ranges(4)]
fn test_dst_incremental_compaction_recovery(
    #[case] max_subcompactions: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    for seed in dst_seeds(1)? {
        log::info!("incremental compaction seed={seed}, ranges={max_subcompactions}");
        let first = run_once(seed, max_subcompactions)?;
        assert_eq!(
            first,
            run_once(seed, max_subcompactions)?,
            "non-determinism for seed {seed}"
        );
    }
    Ok(())
}

fn run_once(
    seed: u64,
    max_subcompactions: usize,
) -> Result<(u64, chrono::DateTime<chrono::Utc>), Error> {
    let rand = Arc::new(DbRand::new(seed));
    let clock = Arc::new(MockSystemClock::new());
    Harness::new("incremental", seed, |ctx| async move {
        let db_seed = ctx.rand().rng().next_u64();
        let db = Db::builder(ctx.path().clone(), ctx.main_object_store())
            .with_system_clock(ctx.system_clock())
            .with_seed(db_seed)
            .with_settings(Settings {
                compactor_options: None,
                garbage_collector_options: None,
                flush_interval: None,
                l0_sst_size_bytes: 1024 * 1024,
                ..Settings::default()
            })
            .build()
            .await?;
        Ok(Arc::new(db))
    })
    .with_rand(rand.clone())
    .with_system_clock(clock.clone())
    .actor(
        "incremental",
        IncrementalCompactionActor { max_subcompactions },
    )
    .run()?;
    let next_random = rand.rng().next_u64();
    Ok((next_random, clock.now()))
}

struct ManualScheduler;

impl CompactionSchedulerSupplier for ManualScheduler {
    fn compaction_scheduler(
        &self,
        _options: &CompactorOptions,
    ) -> Box<dyn CompactionScheduler + Send + Sync> {
        Box::new(Self)
    }
}

impl CompactionScheduler for ManualScheduler {
    fn propose(&self, _state: &CompactorStateView) -> Vec<CompactionSpec> {
        Vec::new()
    }
}

struct IncrementalCompactionActor {
    max_subcompactions: usize,
}

#[async_trait]
impl Actor for IncrementalCompactionActor {
    async fn run(&mut self, ctx: &ActorCtx) -> Result<(), Error> {
        let admin = Admin::builder(ctx.path().clone(), ctx.main_object_store())
            .with_system_clock(ctx.system_clock())
            .with_seed(ctx.rand().rng().next_u64())
            .build();
        let (compactor, task) = start_compactor(ctx, self.max_subcompactions);

        // Build two overlapping runs. Each run has several physical SSTs.
        for generation in 0..2 {
            let mut batch = WriteBatch::new();
            for index in 0..KEY_COUNT {
                if generation == 1 && index.is_multiple_of(5) {
                    batch.delete(key(index));
                } else {
                    batch.put(key(index), Bytes::from(vec![generation; 256]));
                }
            }
            ctx.db().write(batch).await?;
            ctx.db()
                .flush_with_options(FlushOptions {
                    flush_type: FlushType::MemTable,
                })
                .await?;
            let manifest = admin.read_manifest(None).await?.expect("manifest");
            let sources = manifest
                .l0()
                .iter()
                .map(|view| SourceId::SstView(view.id))
                .collect();
            let job = admin
                .submit_compaction(CompactionSpec::new(sources, u32::from(generation)))
                .await?;
            let deadline = ctx.system_clock().now() + chrono::Duration::seconds(30);
            loop {
                let current = admin.read_compaction(job.id(), None).await?.expect("job");
                if current.status() == CompactionStatus::Completed {
                    break;
                }
                assert_ne!(current.status(), CompactionStatus::Failed);
                assert!(
                    ctx.system_clock().now() < deadline,
                    "L0 compaction did not finish"
                );
                ctx.system_clock().sleep(Duration::from_millis(1)).await;
            }
        }
        let before = admin.read_manifest(None).await?.expect("manifest");
        assert_eq!(before.compacted().len(), 2);
        assert!(before
            .compacted()
            .iter()
            .all(|run| run.sst_views().len() > 1));

        // Give heartbeats and the coordinator time to publish output before the job finishes.
        ctx.startup_ctx().failure_controller().add_toxic(Toxic {
            name: "slow-output".to_string(),
            kind: ToxicKind::Latency {
                latency: Duration::from_millis(20),
                jitter: Duration::from_millis(10),
            },
            direction: StreamDirection::Upstream,
            toxicity: 1.0,
            operations: vec![Operation::PutOpts],
            path_prefix: Some(format!("{}/compacted", ctx.path())),
        });
        let job = admin
            .submit_compaction(CompactionSpec::new(
                vec![SourceId::SortedRun(1), SourceId::SortedRun(0)],
                0,
            ))
            .await?;
        let deadline = ctx.system_clock().now() + chrono::Duration::seconds(30);
        loop {
            assert!(ctx.system_clock().now() < deadline, "no incremental commit");
            let manifest = admin.read_manifest(None).await?.expect("manifest");
            assert_eq!(
                manifest.compacted().len(),
                2,
                "job finished without an incremental commit"
            );
            if manifest.compacted() != before.compacted() {
                break;
            }
            ctx.system_clock().sleep(Duration::from_millis(1)).await;
        }
        compactor.stop().await?;
        task.await.expect("compactor task")?;

        let interrupted = admin.read_compaction(job.id(), None).await?.expect("job");
        assert_eq!(interrupted.status(), CompactionStatus::Scheduled);
        assert_eq!(interrupted.subcompactions().len(), self.max_subcompactions);
        assert!(interrupted
            .subcompactions()
            .iter()
            .any(|sub| !sub.output_ssts().is_empty()));
        let partial = admin.read_manifest(None).await?.expect("manifest");
        assert_eq!(
            partial.compacted().len(),
            2,
            "job already committed its final output"
        );
        assert_ne!(partial.compacted(), before.compacted());
        verify_rows(ctx).await?;

        // Resume from the stored progress and the projected manifest input views.
        ctx.startup_ctx().failure_controller().clear_toxics();
        let (replacement, replacement_task) = start_compactor(ctx, self.max_subcompactions);
        let deadline = ctx.system_clock().now() + chrono::Duration::seconds(30);
        let finished = loop {
            let current = admin.read_compaction(job.id(), None).await?.expect("job");
            if current.status() == CompactionStatus::Completed {
                break current;
            }
            assert_ne!(current.status(), CompactionStatus::Failed);
            assert!(
                ctx.system_clock().now() < deadline,
                "compaction did not resume"
            );
            ctx.system_clock().sleep(Duration::from_millis(1)).await;
        };
        let output_ids: HashSet<_> = finished.output_ssts().iter().map(|sst| sst.id).collect();
        for sub in interrupted.subcompactions().iter() {
            assert!(sub
                .output_ssts()
                .iter()
                .all(|sst| output_ids.contains(&sst.id)));
        }
        replacement.stop().await?;
        replacement_task.await.expect("replacement task")?;
        let final_manifest = admin.read_manifest(None).await?.expect("manifest");
        assert_eq!(final_manifest.compacted().len(), 1);
        verify_rows(ctx).await?;
        ctx.shutdown_token().cancel();
        Ok(())
    }
}

fn start_compactor(
    ctx: &ActorCtx,
    max_subcompactions: usize,
) -> (Compactor, tokio::task::JoinHandle<Result<(), Error>>) {
    let compactor = CompactorBuilder::new(ctx.path().clone(), ctx.main_object_store())
        .with_system_clock(ctx.system_clock())
        .with_seed(ctx.rand().rng().next_u64())
        .with_scheduler_supplier(Arc::new(ManualScheduler))
        .with_options(CompactorOptions {
            poll_interval: Duration::from_millis(1),
            commit_compacted_interval: Duration::from_millis(1),
            worker_heartbeat_timeout: Duration::from_millis(100),
            enable_experimental_incremental_compaction: true,
            enable_trivial_move: false,
            worker: Some(CompactionWorkerOptions {
                max_sst_size: 4096,
                max_subcompactions,
                compactions_poll_interval: Duration::from_millis(1),
                heartbeat_interval: Duration::from_millis(1),
                ..CompactionWorkerOptions::default()
            }),
            ..CompactorOptions::default()
        })
        .build();
    ctx.swap_compactor(compactor.clone());
    let running = compactor.clone();
    (compactor, tokio::spawn(async move { running.run().await }))
}

async fn verify_rows(ctx: &ActorCtx) -> Result<(), Error> {
    let reader_seed = ctx.rand().rng().next_u64();
    let reader = DbReader::builder(ctx.path().clone(), ctx.main_object_store())
        .with_system_clock(ctx.system_clock())
        .with_seed(reader_seed)
        .with_options(DbReaderOptions {
            skip_wal_replay: true,
            ..DbReaderOptions::default()
        })
        .build()
        .await?;
    let expected: Vec<_> = (0..KEY_COUNT)
        .filter(|index| !index.is_multiple_of(5))
        .map(key)
        .collect();
    for index in 0..KEY_COUNT {
        let value = reader.get(key(index)).await?;
        let expected = (!index.is_multiple_of(5)).then(|| Bytes::from(vec![1; 256]));
        assert_eq!(value, expected, "point read at key {index}");
    }
    for order in [IterationOrder::Ascending, IterationOrder::Descending] {
        let options = ScanOptions::new().with_order(order);
        let mut scan = reader.scan_with_options(.., &options).await?;
        let mut actual = Vec::new();
        while let Some(row) = scan.next().await? {
            assert_eq!(row.value, Bytes::from(vec![1; 256]));
            actual.push(row.key);
        }
        if matches!(order, IterationOrder::Descending) {
            actual.reverse();
        }
        assert_eq!(actual, expected, "scan order {order:?}");
    }
    reader.close().await
}

fn key(index: usize) -> Bytes {
    Bytes::from(format!("key/{index:04}"))
}
