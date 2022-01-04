use std::{
    rc::Rc,
    sync::Arc,
    time::{Duration, Instant},
};

use glommio::{sync::Semaphore, LocalExecutorBuilder, Placement};
use kvproto::raft_serverpb::RaftLocalState;
use log::debug;
use raft::eraftpb::Entry;
use raft_engine::{Config, Engine, LogBatch, MessageExt, ReadableSize};
use rand::thread_rng;
use rand_distr::{Distribution, Normal};

#[derive(Clone)]
pub struct MessageExtTyped;

impl MessageExt for MessageExtTyped {
    type Entry = Entry;

    fn index(e: &Self::Entry) -> u64 {
        e.index
    }
}

fn main() {
    env_logger::init();

    let config = Config {
        dir: "append-compact-purge-data".to_owned(),
        purge_threshold: ReadableSize::gb(2),
        batch_compression_threshold: ReadableSize::kb(0),
        ..Default::default()
    };
    let engine = Arc::new(Engine::open(config).expect("Open raft engine"));
    let mut handles = Vec::new();
    for i in 0..2 {
        let engine = engine.clone();
        let handle = LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(move || run(engine, i == 0))
            .unwrap();
        handles.push(handle);
    }
    let purge = LocalExecutorBuilder::new(Placement::Unbound)
        .spawn(move || async move {
            loop {
                for region in engine.purge_expired_files().unwrap() {
                    let state = engine
                        .get_message::<RaftLocalState>(region, b"last_index")
                        .unwrap()
                        .unwrap();
                    engine.compact_to_async(region, state.last_index - 7).await;
                    println!(
                        "[EXAMPLE] force compact {} to {}",
                        region,
                        state.last_index - 7
                    );
                }
                glommio::timer::sleep(Duration::from_secs(1)).await;
            }
        })
        .unwrap();
    for handle in handles {
        handle.join().unwrap();
    }
    purge.join().unwrap();
}

async fn run(engine: Arc<Engine>, compact: bool) {
    let compact_offset = 32; // In src/purge.rs, it's the limit for rewrite.

    let mut rand_regions = Normal::new(128.0, 96.0)
        .unwrap()
        .sample_iter(thread_rng())
        .map(|x| x as u64);
    let mut rand_compacts = Normal::new(compact_offset as f64, 16.0)
        .unwrap()
        .sample_iter(thread_rng())
        .map(|x| x as u64);

    let mut entry = Entry::new();
    entry.set_data(vec![b'x'; 17462].into());
    let init_state = RaftLocalState {
        last_index: 0,
        ..Default::default()
    };
    let sem = Rc::new(Semaphore::new(256));
    loop {
        let acquire_begin = Instant::now();
        let permit = Semaphore::acquire_static_permit(&sem, 1).await.unwrap();
        let acquire_elapsed = acquire_begin.elapsed();
        if acquire_elapsed > Duration::from_millis(50) {
            debug!("acquire elapsed {:?}", acquire_elapsed);
        }

        let mut batch = LogBatch::with_capacity(256);
        let region = rand_regions.next().unwrap();
        let state = engine
            .get_message::<RaftLocalState>(region, b"last_index")
            .unwrap()
            .unwrap_or_else(|| init_state.clone());

        let mut e = entry.clone();
        e.index = state.last_index + 1;
        batch.add_entries::<MessageExtTyped>(region, &[e]).unwrap();
        batch
            .put_message(region, b"last_index".to_vec(), &state)
            .unwrap();
        let engine2 = engine.clone();
        glommio::spawn_local(async move {
            engine2.write_async(&mut batch).await.unwrap();
            drop(permit);
        })
        .detach();

        // engine.write(&mut batch, true).unwrap();

        if compact && state.last_index % compact_offset == 0 {
            let rand_compact_offset = rand_compacts.next().unwrap();
            if state.last_index > rand_compact_offset {
                let compact_to = state.last_index - rand_compact_offset;
                let engine2 = engine.clone();
                glommio::spawn_local(async move {
                    engine2.compact_to_async(region, compact_to).await;
                    println!("[EXAMPLE] compact {} to {}", region, compact_to);
                })
                .detach();
            }
        }
        // glommio::yield_if_needed().await;
    }
}
