use super::bench2166::cache;
use crate::config::{
    DurabilityLevel, FlushOptions, FlushType, ScanOptions, Settings, WriteOptions,
};
use crate::{Db, WriteBatch};
use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::aws::AmazonS3Builder;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetRange, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug)]
struct CountedStore {
    inner: Arc<dyn ObjectStore>,
    gets: AtomicU64,
    get_errors: AtomicU64,
    not_found: AtomicU64,
    range_bytes: AtomicU64,
}

impl std::fmt::Display for CountedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountedStore")
    }
}

#[async_trait]
impl ObjectStore for CountedStore {
    async fn put_opts(
        &self,
        p: &Path,
        v: PutPayload,
        o: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(p, v, o).await
    }
    async fn put_multipart_opts(
        &self,
        p: &Path,
        o: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(p, o).await
    }
    async fn get_opts(&self, p: &Path, o: GetOptions) -> object_store::Result<GetResult> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        if let Some(GetRange::Bounded(range)) = &o.range {
            self.range_bytes
                .fetch_add(range.end - range.start, Ordering::Relaxed);
        }
        let result = self.inner.get_opts(p, o).await;
        if let Err(error) = &result {
            if matches!(error, object_store::Error::NotFound { .. }) {
                self.not_found.fetch_add(1, Ordering::Relaxed);
            } else {
                self.get_errors.fetch_add(1, Ordering::Relaxed);
            }
        }
        result
    }
    fn delete_stream(
        &self,
        p: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(p)
    }
    fn list(&self, p: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(p)
    }
    async fn list_with_delimiter(&self, p: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(p).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, o: CopyOptions) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, o).await
    }
}

#[derive(Clone, Copy)]
struct Case {
    name: &'static str,
    s3: bool,
    cached: bool,
    rows: usize,
    clients: usize,
    workers: usize,
    fetch_tasks: usize,
    read_ahead: usize,
    yield_between_queries: bool,
}

fn cases() -> Vec<Case> {
    let memory = Case {
        name: "memory/warm/1client",
        s3: false,
        cached: true,
        rows: 2048,
        clients: 1,
        workers: 2,
        fetch_tasks: 1,
        read_ahead: 1,
        yield_between_queries: true,
    };
    let s3 = Case {
        s3: true,
        cached: false,
        name: "s3/uncached/1client",
        ..memory
    };
    vec![
        memory,
        Case {
            name: "memory/warm/8clients",
            clients: 8,
            ..memory
        },
        Case {
            name: "memory/warm/32clients",
            clients: 32,
            ..memory
        },
        Case {
            name: "memory/warm/large/8clients",
            rows: 32768,
            clients: 8,
            ..memory
        },
        Case {
            name: "memory/warm/noyield/8clients",
            clients: 8,
            yield_between_queries: false,
            ..memory
        },
        Case {
            name: "memory/warm/large/oneworker/8clients",
            rows: 32768,
            workers: 1,
            clients: 8,
            ..memory
        },
        Case {
            name: "memory/warm/tasks4/8clients",
            clients: 8,
            fetch_tasks: 4,
            ..memory
        },
        s3,
        Case {
            name: "s3/uncached/8clients",
            clients: 8,
            ..s3
        },
        Case {
            name: "s3/uncached/32clients",
            clients: 32,
            ..s3
        },
        Case {
            name: "s3/uncached/batch64k/8clients",
            clients: 8,
            read_ahead: 65536,
            ..s3
        },
        Case {
            name: "s3/uncached/tasks4/8clients",
            clients: 8,
            fetch_tasks: 4,
            ..s3
        },
        Case {
            name: "s3/warm/8clients",
            clients: 8,
            cached: true,
            ..s3
        },
    ]
}

async fn fixture(case: Case) -> (Arc<Db>, Arc<CountedStore>) {
    let inner: Arc<dyn ObjectStore> = if case.s3 {
        Arc::new(
            AmazonS3Builder::new()
                .with_endpoint(std::env::var("SLATEDB_RUSTFS_ENDPOINT").unwrap())
                .with_bucket_name(std::env::var("SLATEDB_RUSTFS_BUCKET").unwrap())
                .with_access_key_id(std::env::var("SLATEDB_RUSTFS_ACCESS_KEY").unwrap())
                .with_secret_access_key(std::env::var("SLATEDB_RUSTFS_SECRET_KEY").unwrap())
                .with_region("us-east-1")
                .with_allow_http(true)
                .with_virtual_hosted_style_request(false)
                .build()
                .unwrap(),
        )
    } else {
        Arc::new(InMemory::new())
    };
    let objects = Arc::new(CountedStore {
        inner,
        gets: AtomicU64::new(0),
        get_errors: AtomicU64::new(0),
        not_found: AtomicU64::new(0),
        range_bytes: AtomicU64::new(0),
    });
    let prefix = format!("bench2166-extended/{}", ulid::Ulid::new());
    let db = Arc::new(
        Db::builder(prefix, objects.clone())
            .with_settings(Settings {
                compactor_options: None,
                garbage_collector_options: None,
                flush_interval: None,
                l0_sst_size_bytes: 64 * 1024 * 1024,
                l0_max_ssts: 32,
                l0_max_ssts_per_key: 32,
                ..Settings::default()
            })
            .with_db_cache(cache(case.cached), 0)
            .build()
            .await
            .unwrap(),
    );
    let mut batch = WriteBatch::new();
    for key in 0..case.rows {
        batch.put((key as u64).to_be_bytes(), [7_u8; 128]);
    }
    db.write_with_options(batch, &WriteOptions::default())
        .await
        .unwrap();
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .unwrap();
    assert_eq!(scan(&db, case).await, case.rows);
    (db, objects)
}

async fn scan(db: &Db, case: Case) -> usize {
    let options = ScanOptions {
        durability_filter: DurabilityLevel::Remote,
        cache_blocks: case.cached,
        max_fetch_tasks: case.fetch_tasks,
        read_ahead_bytes: case.read_ahead,
        ..ScanOptions::default()
    };
    let mut iter = db.scan_with_options(.., &options).await.unwrap();
    let mut count = 0;
    while let Some(row) = iter.next().await.unwrap() {
        assert_eq!(row.key.as_ref(), (count as u64).to_be_bytes());
        assert_eq!(row.value.as_ref(), &[7_u8; 128]);
        black_box(row);
        count += 1;
    }
    count
}

struct ClientResult {
    within_window: u64,
    latencies_ns: Vec<u64>,
    first_start_delay_ns: u64,
}

fn quantiles(values: &[u64]) -> serde_json::Value {
    assert!(!values.is_empty());
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    serde_json::json!({
        "n": sorted.len(), "p50_ns": sorted[(sorted.len()-1)/2],
        "p95_ns": sorted[(sorted.len()-1)*95/100],
        "p99_ns": sorted[(sorted.len()-1)*99/100], "max_ns": sorted[sorted.len()-1],
    })
}

async fn measure_case(case: Case, duration: Duration) -> serde_json::Value {
    let (db, objects) = fixture(case).await;
    let start = Instant::now() + Duration::from_millis(100);
    let end = start + duration;
    let mut clients = Vec::new();
    for _ in 0..case.clients {
        let db = db.clone();
        clients.push(tokio::spawn(async move {
            tokio::time::sleep_until(start.into()).await;
            let first_start_delay_ns =
                Instant::now().saturating_duration_since(start).as_nanos() as u64;
            let mut result = ClientResult {
                within_window: 0,
                latencies_ns: Vec::new(),
                first_start_delay_ns,
            };
            while Instant::now() < end {
                let query_start = Instant::now();
                assert_eq!(scan(&db, case).await, case.rows);
                let complete = Instant::now();
                result
                    .latencies_ns
                    .push(complete.duration_since(query_start).as_nanos() as u64);
                if complete <= end {
                    result.within_window += 1;
                }
                if case.yield_between_queries {
                    tokio::task::yield_now().await;
                }
            }
            result
        }));
    }
    let handle = tokio::runtime::Handle::current();
    let probe_driver = std::thread::spawn(move || {
        let mut tasks = Vec::new();
        let mut due = start;
        while due < end {
            if let Some(wait) = due.checked_duration_since(Instant::now()) {
                std::thread::sleep(wait);
            }
            let enqueued = Instant::now();
            tasks.push(handle.spawn(async move {
                (
                    enqueued.duration_since(start).as_nanos() as u64,
                    Instant::now().duration_since(enqueued).as_nanos() as u64,
                )
            }));
            due += Duration::from_millis(1);
        }
        tasks
    });
    objects.gets.store(0, Ordering::Relaxed);
    objects.get_errors.store(0, Ordering::Relaxed);
    objects.not_found.store(0, Ordering::Relaxed);
    objects.range_bytes.store(0, Ordering::Relaxed);
    let mut counts = Vec::new();
    let mut query_ns = Vec::new();
    let mut first_start_ns = Vec::new();
    for client in clients {
        let result = client.await.unwrap();
        counts.push(result.within_window);
        query_ns.extend(result.latencies_ns);
        first_start_ns.push(result.first_start_delay_ns);
    }
    let mut probe_ns = Vec::new();
    let mut probe_trace = Vec::new();
    for task in probe_driver.join().unwrap() {
        let value = task.await.unwrap();
        probe_ns.push(value.1);
        probe_trace.push(value);
    }
    let gets = objects.gets.load(Ordering::Relaxed);
    let errors = objects.get_errors.load(Ordering::Relaxed);
    let not_found = objects.not_found.load(Ordering::Relaxed);
    let range_bytes = objects.range_bytes.load(Ordering::Relaxed);
    assert_eq!(errors, 0);
    if case.s3 && !case.cached {
        assert!(gets >= query_ns.len() as u64);
    }
    let total = counts.iter().sum::<u64>();
    let squares = counts.iter().map(|n| (*n as f64).powi(2)).sum::<f64>();
    let jain = (total as f64).powi(2) / (counts.len() as f64 * squares);
    let value = serde_json::json!({
        "case": case.name, "clients": case.clients, "workers": case.workers,
        "rows": case.rows, "cached": case.cached, "fetch_tasks": case.fetch_tasks,
        "read_ahead_bytes": case.read_ahead, "yield_between_queries": case.yield_between_queries,
        "duration_ms": duration.as_millis(), "queries_per_second": total as f64 / duration.as_secs_f64(),
        "completed_in_window": total, "completed_including_drain": query_ns.len(),
        "per_client_completed": counts, "jain_fairness": jain,
        "query": quantiles(&query_ns), "probe_queue_delay": quantiles(&probe_ns),
        "first_client_start": quantiles(&first_start_ns),
        "get_requests": gets, "get_errors": errors, "not_found_probes": not_found,
        "range_bytes_requested": range_bytes,
        "gets_per_query": gets as f64 / query_ns.len() as f64,
        "query_raw_ns": query_ns, "probe_trace_ns": probe_trace,
    });
    println!(
        "{}: {:.1} queries/s, Jain={:.4}, query p99={} us, probe p99={} us, GETs={}",
        case.name,
        total as f64 / duration.as_secs_f64(),
        jain,
        value["query"]["p99_ns"].as_u64().unwrap() / 1000,
        value["probe_queue_delay"]["p99_ns"].as_u64().unwrap() / 1000,
        gets
    );
    db.close().await.unwrap();
    value
}

#[test]
#[ignore = "Run this benchmark in the release profile."]
fn issue2166_concurrency_and_s3() {
    let filter = std::env::var("SLATEDB_BENCH2166_EXT_FILTER").unwrap_or_default();
    let ms: u64 = std::env::var("SLATEDB_BENCH2166_EXT_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2000);
    let mut results = Vec::new();
    for case in cases() {
        if !case.name.contains(&filter) {
            continue;
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(case.workers)
            .enable_all()
            .build()
            .unwrap();
        results.push(runtime.block_on(measure_case(case, Duration::from_millis(ms))));
    }
    let output = std::env::var("SLATEDB_BENCH2166_OUTPUT").expect("Set a JSON output path.");
    std::fs::write(
        output,
        serde_json::to_vec_pretty(&serde_json::json!({"results": results})).unwrap(),
    )
    .unwrap();
}
