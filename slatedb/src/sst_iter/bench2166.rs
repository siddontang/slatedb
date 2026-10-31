use super::*;
use crate::block_cache_policy::BlockCachePolicy;
use crate::config::{
    DurabilityLevel, FlushOptions, FlushType, ScanOptions, Settings, WriteOptions,
};
use crate::db_cache::foyer::{FoyerCache, FoyerCacheOptions};
use crate::db_cache::{DbCache, SplitCache};
use crate::db_state::SsTableView;
use crate::format::sst::SsTableFormat;
use crate::tablestore::TableStoreKind;
use crate::{Db, WriteBatch};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::throttle::{ThrottleConfig, ThrottledStore};
use std::hint::black_box;
use std::time::{Duration, Instant};

const ROWS: usize = 2048;
const VALUE_BYTES: usize = 128;

struct SstFixture {
    table: SsTableView,
    store: Arc<TableStore>,
    objects: Arc<ThrottledStore<InMemory>>,
    rows: usize,
    blocks: usize,
}

pub(super) fn cache(blocks: bool) -> Arc<dyn DbCache> {
    let memory = Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
        max_capacity: 64 * 1024 * 1024,
        shards: 4,
    }));
    Arc::new(
        SplitCache::new()
            .with_block_cache(blocks.then(|| memory.clone() as Arc<dyn DbCache>))
            .with_meta_cache(Some(memory))
            .build(),
    )
}

async fn sst_fixture(rows: usize, blocks_cached: bool) -> SstFixture {
    let objects = Arc::new(ThrottledStore::new(
        InMemory::new(),
        ThrottleConfig::default(),
    ));
    let store = Arc::new(TableStore::new(
        objects.clone(),
        SsTableFormat::default(),
        Path::from("bench2166"),
        Some(cache(blocks_cached)),
        TableStoreKind::Main,
        BlockCachePolicy::default(),
    ));
    let mut builder = store.table_builder();
    for key in 0..rows {
        builder
            .add_value(
                &(key as u64).to_be_bytes(),
                &[7; VALUE_BYTES],
                Some(1),
                None,
            )
            .await
            .unwrap();
    }
    let table = SsTableView::identity(
        store
            .write_sst(
                &SsTableId::from(ulid::Ulid::new()),
                &builder.build().await.unwrap(),
                None,
            )
            .await
            .unwrap(),
    );
    let index = store
        .read_index(&table.sst, true, None, &ReadTrace::none(), None)
        .await
        .unwrap();
    let blocks = index.borrow().block_meta().len();
    let fixture = SstFixture {
        table,
        store,
        objects,
        rows,
        blocks,
    };
    assert_eq!(
        scan_sst(&fixture, &sst_options(1, 1, false), false).await,
        rows
    );
    fixture
}

fn sst_options(tasks: usize, bytes: usize, descending: bool) -> SstIteratorOptions {
    SstIteratorOptions {
        max_fetch_tasks: tasks,
        target_bytes_to_fetch: bytes,
        eager_spawn: true,
        order: if descending {
            IterationOrder::Descending
        } else {
            IterationOrder::Ascending
        },
        ..SstIteratorOptions::default()
    }
}

async fn scan_sst(fixture: &SstFixture, options: &SstIteratorOptions, first: bool) -> usize {
    let mut iter = SstIterator::new_owned_initialized(
        ..,
        fixture.table.clone(),
        fixture.store.clone(),
        options.clone(),
    )
    .await
    .unwrap()
    .unwrap();
    let mut count = 0;
    while let Some(row) = iter.next().await.unwrap() {
        black_box(row);
        count += 1;
        if first {
            break;
        }
    }
    count
}

async fn db_fixture(ssts: usize, blocks_cached: bool) -> Db {
    let db = Db::builder("bench2166-db", Arc::new(InMemory::new()))
        .with_settings(Settings {
            compactor_options: None,
            garbage_collector_options: None,
            flush_interval: None,
            l0_sst_size_bytes: 64 * 1024 * 1024,
            l0_max_ssts: 32,
            l0_max_ssts_per_key: 32,
            ..Settings::default()
        })
        .with_db_cache(cache(blocks_cached), 0)
        .build()
        .await
        .unwrap();
    for version in 0..ssts {
        let mut batch = WriteBatch::new();
        for key in 0..ROWS {
            batch.put((key as u64).to_be_bytes(), [version as u8; VALUE_BYTES]);
        }
        db.write_with_options(batch, &WriteOptions::default())
            .await
            .unwrap();
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();
    }
    assert_eq!(scan_db(&db, 1, 1, false, false).await, ROWS);
    db
}

async fn scan_db(db: &Db, tasks: usize, bytes: usize, first: bool, descending: bool) -> usize {
    let options = ScanOptions {
        durability_filter: DurabilityLevel::Remote,
        max_fetch_tasks: tasks,
        read_ahead_bytes: bytes,
        cache_blocks: true,
        order: if descending {
            IterationOrder::Descending
        } else {
            IterationOrder::Ascending
        },
        ..ScanOptions::default()
    };
    let mut iter = db.scan_with_options(.., &options).await.unwrap();
    let mut count = 0;
    while let Some(row) = iter.next().await.unwrap() {
        black_box(row);
        count += 1;
        if first {
            break;
        }
    }
    count
}

async fn measure<F, Fut>(
    name: &str,
    samples: usize,
    expected: usize,
    mut run: F,
) -> serde_json::Value
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = usize>,
{
    for _ in 0..samples.min(100) {
        assert_eq!(run().await, expected);
    }
    let mut elapsed = Vec::with_capacity(samples);
    for _ in 0..samples {
        let start = Instant::now();
        let rows = run().await;
        let ns = start.elapsed().as_nanos() as u64;
        assert_eq!(rows, expected);
        elapsed.push(ns);
    }
    let mut sorted = elapsed.clone();
    sorted.sort_unstable();
    let percentile = |percent: usize| sorted[(samples - 1) * percent / 100];
    let value = serde_json::json!({
        "case": name, "samples": samples, "rows": expected,
        "p50_ns": percentile(50), "p95_ns": percentile(95), "p99_ns": percentile(99),
        "mean_ns": elapsed.iter().sum::<u64>() as f64 / samples as f64,
        "raw_ns": elapsed,
    });
    println!(
        "{name}: p50={} ns, p95={} ns, n={samples}",
        percentile(50),
        percentile(95)
    );
    value
}

async fn run_benchmark() -> Vec<serde_json::Value> {
    let filter = std::env::var("SLATEDB_BENCH2166_FILTER").unwrap_or_default();
    let scale = std::env::var("SLATEDB_BENCH2166_SCALE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1);
    let small = sst_fixture(16, true).await;
    let large = sst_fixture(ROWS, true).await;
    let uncached = sst_fixture(16, false).await;
    assert_eq!(small.blocks, 1);
    assert!(large.blocks > 32);
    println!(
        "fixture: {ROWS} rows, {} blocks, {VALUE_BYTES}-byte values",
        large.blocks
    );
    let mut results = Vec::new();
    for (name, fixture, tasks, bytes, first, desc, samples) in [
        ("sst/warm/first/1block", &small, 1, 1, true, false, 2000),
        ("sst/warm/full/1block", &small, 1, 1, false, false, 2000),
        ("sst/warm/full/multiblock", &large, 1, 1, false, false, 300),
        (
            "sst/warm/full/batch64k",
            &large,
            1,
            65536,
            false,
            false,
            300,
        ),
        ("sst/warm/full/descending", &large, 1, 1, false, true, 300),
        ("sst/warm/full/tasks4", &large, 4, 1, false, false, 300),
        (
            "sst/uncached/full/1block",
            &uncached,
            1,
            1,
            false,
            false,
            1000,
        ),
    ] {
        if !name.contains(&filter) {
            continue;
        }
        let options = sst_options(tasks, bytes, desc);
        results.push(
            measure(
                name,
                samples * scale,
                if first { 1 } else { fixture.rows },
                || scan_sst(fixture, &options, first),
            )
            .await,
        );
    }
    if filter.is_empty() || "sst/delay1ms/full/1block".contains(&filter) {
        uncached
            .objects
            .config_mut(|c| c.wait_get_per_call = Duration::from_millis(1));
        let options = sst_options(1, 1, false);
        results.push(
            measure("sst/delay1ms/full/1block", 100 * scale, 16, || {
                scan_sst(&uncached, &options, false)
            })
            .await,
        );
    }
    for ssts in [1, 8] {
        let db = db_fixture(ssts, true).await;
        for (suffix, tasks, bytes, first, desc, samples) in [
            ("first", 1, 1, true, false, 1000),
            ("full", 1, 1, false, false, 200),
            ("batch64k", 1, 65536, false, false, 200),
            ("descending", 1, 1, false, true, 200),
            ("tasks4", 4, 1, false, false, 200),
        ] {
            let name = format!("db/warm/{ssts}sst/{suffix}");
            if !name.contains(&filter) {
                continue;
            }
            results.push(
                measure(&name, samples * scale, if first { 1 } else { ROWS }, || {
                    scan_db(&db, tasks, bytes, first, desc)
                })
                .await,
            );
        }
        db.close().await.unwrap();
    }
    results
}

#[test]
#[ignore = "Run this benchmark in the release profile."]
fn issue2166_scan_benchmark() {
    let worker = std::env::var("SLATEDB_BENCH2166_CALLER").unwrap_or_default() == "worker";
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let results = runtime.block_on(async {
        if worker {
            tokio::spawn(run_benchmark()).await.unwrap()
        } else {
            run_benchmark().await
        }
    });
    let output = std::env::var("SLATEDB_BENCH2166_OUTPUT").expect("Set a JSON output path.");
    std::fs::write(
        output,
        serde_json::to_vec_pretty(&serde_json::json!({
            "caller": if worker { "worker" } else { "external" },
            "workers": 2, "rows": ROWS, "value_bytes": VALUE_BYTES, "results": results,
        }))
        .unwrap(),
    )
    .unwrap();
}
