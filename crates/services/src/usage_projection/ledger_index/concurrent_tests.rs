// Concurrent observation: readers on the shared index while a writer mutates a
// file-backed WAL database through the five-connection pool.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_readers_and_writer_on_file_db() {
    let directory = tempfile::tempdir().unwrap();
    let db = file_db(&directory.path().join("concurrent.db")).await;
    project(&db, "p").await;
    let index = Arc::new(UsageLedgerIndex::new(db.clone()));
    let stop = Arc::new(AtomicBool::new(false));
    let mut readers = Vec::new();
    for r in 0..3usize {
        let index = index.clone();
        let stop = stop.clone();
        readers.push(tokio::spawn(async move {
            let mut last = 0i64;
            let mut reads = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let aggregate = index.operations().await.unwrap();
                assert!(
                    aggregate.tokens.input_tokens >= last,
                    "tokens went backwards"
                );
                last = aggregate.tokens.input_tokens;
                reads += 1;
                if r == 0 {
                    index
                        .agents(&["a".to_owned(), "b".to_owned(), "c".to_owned()])
                        .await
                        .unwrap();
                } else if r == 1 {
                    index
                        .agent_execution_stats(&["a".to_owned()])
                        .await
                        .unwrap();
                }
            }
            reads
        }));
    }
    let writer = {
        let db = db.clone();
        tokio::spawn(async move {
            for k in 0..250usize {
                let run = format!("run-{k}");
                let e =
                    execution(&db, &run, "p", AGENTS[k % 3], db::ExecutionStatus::Running).await;
                let i = admit(&db, &format!("inv-{k}"), "p", &run, AGENTS[(k + 1) % 3]).await;
                let i = start(&db, &i).await;
                let i = settle(&db, &i).await;
                for j in 0..3 {
                    event(
                        &db,
                        &i,
                        &format!("ev-{k}-{j}"),
                        AGENTS[(k + 1) % 3],
                        j != 1,
                        j,
                    )
                    .await;
                }
                finish_at(&db, &e, k % 4 == 0, &timestamp((k * 37) as u64)).await;
            }
        })
    };
    writer.await.unwrap();
    stop.store(true, Ordering::Relaxed);
    let mut reads = 0;
    for reader in readers {
        reads += reader.await.unwrap();
    }
    assert_reference_full(&db, &index, 0, 0).await;
    let cold = UsageLedgerIndex::new(db.clone());
    assert_reference_full(&db, &cold, 0, 1).await;
    println!("AUDIT concurrent: {reads} index reads while 250 runs / 750 events were written");
}
