async fn scan_inline_fixture() -> (
    Arc<TableStore>,
    SsTableView,
    Arc<PointReadCache>,
    Arc<dyn ObjectStore>,
) {
    let cache = point_read_cache();
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = Arc::new(TableStore::new(
        objects.clone(),
        SsTableFormat {
            block_size: 256,
            ..SsTableFormat::default()
        },
        Path::from("scan-inline"),
        Some(cache.clone()),
        TableStoreKind::Main,
        BlockCachePolicy::default(),
    ));
    let mut builder = store.table_builder();
    for key in 0..8_u64 {
        builder
            .add(RowEntry::new_value(&key.to_be_bytes(), &[7; 128], 1))
            .await
            .unwrap();
    }
    let table = SsTableView::identity(
        store
            .write_sst(&test_sst_id(2166), &builder.build().await.unwrap(), None)
            .await
            .unwrap(),
    );
    let index = store
        .read_index(&table.sst, true, None, &ReadTrace::none(), None)
        .await
        .unwrap();
    assert_eq!(index.borrow().block_meta().len(), 8);
    let mut warm = SstIterator::new_owned_initialized(
        ..,
        table.clone(),
        store.clone(),
        SstIteratorOptions::default(),
    )
    .await
    .unwrap()
    .unwrap();
    while warm.next().await.unwrap().is_some() {}
    (store, table, cache, objects)
}

fn scan_inline_options(order: IterationOrder, bytes: usize, eager: bool) -> SstIteratorOptions {
    SstIteratorOptions {
        order,
        target_bytes_to_fetch: bytes,
        eager_spawn: eager,
        ..SstIteratorOptions::default()
    }
}

#[rstest]
#[tokio::test(flavor = "current_thread")]
async fn scan_inline_retains_cancelled_init(
    #[values(IterationOrder::Ascending, IterationOrder::Descending)] order: IterationOrder,
    #[values(1, 4096)] bytes: usize,
    #[values(false, true)] eager: bool,
) {
    use std::sync::atomic::Ordering::SeqCst;
    let (store, table, cache, _) = scan_inline_fixture().await;
    cache.mode.store(1, SeqCst);
    let mut iter = InternalSstIterator::new_owned(
        ..,
        table,
        store,
        scan_inline_options(order, bytes, eager),
        None,
    )
    .unwrap()
    .unwrap();
    {
        let mut init = Box::pin(iter.init());
        assert!(futures::poll!(init.as_mut()).is_pending());
    }
    let started = cache.started.load(SeqCst);
    assert_eq!(started, if bytes == 1 { 1 } else { 8 });
    assert_eq!(cache.dropped.load(SeqCst), 0);
    cache.mode.store(0, SeqCst);
    cache.release.notify_waiters();
    iter.init().await.unwrap();
    let mut keys = Vec::new();
    while let Some(row) = iter.next().await.unwrap() {
        keys.push(row.key);
    }
    let expected: Vec<Bytes> = match order {
        IterationOrder::Ascending => (0..8_u64)
            .map(|k| Bytes::copy_from_slice(&k.to_be_bytes()))
            .collect(),
        IterationOrder::Descending => (0..8_u64)
            .rev()
            .map(|k| Bytes::copy_from_slice(&k.to_be_bytes()))
            .collect(),
    };
    assert_eq!(keys, expected);
    assert_eq!(cache.started.load(SeqCst), started);
    assert_eq!(cache.dropped.load(SeqCst), started);
}

#[rstest]
#[tokio::test(flavor = "current_thread")]
async fn scan_inline_drop_releases_pending_fetch(
    #[values(IterationOrder::Ascending, IterationOrder::Descending)] order: IterationOrder,
    #[values(1, 4096)] bytes: usize,
) {
    use std::sync::atomic::Ordering::SeqCst;
    let (store, table, cache, _) = scan_inline_fixture().await;
    cache.mode.store(1, SeqCst);
    let mut iter = InternalSstIterator::new_owned(
        ..,
        table,
        store,
        scan_inline_options(order, bytes, true),
        None,
    )
    .unwrap()
    .unwrap();
    {
        let mut init = Box::pin(iter.init());
        assert!(futures::poll!(init.as_mut()).is_pending());
    }
    let started = cache.started.load(SeqCst);
    assert!(started > 0);
    assert_eq!(cache.dropped.load(SeqCst), 0);
    drop(iter);
    assert_eq!(cache.dropped.load(SeqCst), started);
}

#[rstest]
#[tokio::test(flavor = "current_thread")]
async fn scan_inline_retains_cancelled_later_fetch(
    #[values(IterationOrder::Ascending, IterationOrder::Descending)] order: IterationOrder,
) {
    use std::sync::atomic::Ordering::SeqCst;
    let (store, table, cache, _) = scan_inline_fixture().await;
    let mut iter =
        InternalSstIterator::new_owned(.., table, store, scan_inline_options(order, 1, true), None)
            .unwrap()
            .unwrap();
    iter.init().await.unwrap();
    cache.mode.store(1, SeqCst);
    {
        let mut fetch = Box::pin(iter.next_iter(true));
        assert!(futures::poll!(fetch.as_mut()).is_pending());
    }
    assert_eq!(cache.started.load(SeqCst), 1);
    assert_eq!(cache.dropped.load(SeqCst), 0);
    cache.mode.store(0, SeqCst);
    cache.release.notify_waiters();
    let mut block = iter.next_iter(true).await.unwrap().unwrap();
    let expected = match order {
        IterationOrder::Ascending => 1_u64,
        IterationOrder::Descending => 6_u64,
    };
    assert_eq!(
        block.next().await.unwrap().unwrap().key.as_ref(),
        expected.to_be_bytes()
    );
    assert_eq!(cache.started.load(SeqCst), 1);
    assert_eq!(cache.dropped.load(SeqCst), 1);
}

#[rstest]
#[tokio::test]
async fn scan_inline_preserves_fetch_errors(
    #[values(IterationOrder::Ascending, IterationOrder::Descending)] order: IterationOrder,
    #[values(1, 4096)] bytes: usize,
    #[values(0, 1, 2)] failure: u8,
) {
    use futures::TryStreamExt;
    let (store, table, cache, objects) = scan_inline_fixture().await;
    evict_point_blocks(&cache).await;
    if failure == 2 {
        cache.mode.store(2, std::sync::atomic::Ordering::SeqCst);
    } else {
        let objects_list: Vec<_> = objects.list(None).try_collect().await.unwrap();
        assert_eq!(objects_list.len(), 1);
        let object = &objects_list[0];
        if failure == 0 {
            objects.delete(&object.location).await.unwrap();
        } else {
            objects
                .put(
                    &object.location,
                    Bytes::from(vec![0; object.size as usize]).into(),
                )
                .await
                .unwrap();
        }
    }
    let mut iter = InternalSstIterator::new_owned(
        ..,
        table,
        store,
        scan_inline_options(order, bytes, true),
        None,
    )
    .unwrap()
    .unwrap();
    let result = iter.init().await;
    if failure == 2 {
        assert!(matches!(result, Err(SlateDBError::BackgroundTaskPanic(_))));
    } else {
        assert!(result.is_err());
    }
}

#[rstest]
#[tokio::test]
async fn scan_inline_keeps_multi_task_reads_spawned(
    #[values(IterationOrder::Ascending, IterationOrder::Descending)] order: IterationOrder,
    #[values(1, 4096)] bytes: usize,
    #[values(1, 4)] tasks: usize,
    #[values(false, true)] eager: bool,
) {
    let (store, table, _, _) = scan_inline_fixture().await;
    let dead = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .build()
        .unwrap();
    let handle = dead.handle().clone();
    dead.shutdown_background();
    let mut options = scan_inline_options(order, bytes, eager);
    options.max_fetch_tasks = tasks;
    let mut iter = InternalSstIterator::new_owned(.., table, store, options, None)
        .unwrap()
        .unwrap();
    let result = {
        let _entered = handle.enter();
        iter.init().await
    };
    assert_eq!(result.is_ok(), tasks == 1);
    if tasks > 1 {
        assert!(matches!(
            result,
            Err(SlateDBError::BackgroundTaskCancelled(_))
        ));
    }
}
