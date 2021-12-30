use std::sync::Arc;

use glommio::{LocalExecutorBuilder, Placement};
use kvproto::raft_serverpb::RaftLocalState;
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

    let engine2 = engine.clone();
    let handle1 = LocalExecutorBuilder::new(Placement::Fixed(0))
        .io_memory(256 << 10)
        .spawn(move || run(engine2))
        .unwrap();
    let handle2 = LocalExecutorBuilder::new(Placement::Fixed(0))
        .io_memory(256 << 10)
        .spawn(move || run(engine))
        .unwrap();
    handle1.join().unwrap();
    handle2.join().unwrap();
}

async fn run(engine: Arc<Engine>) {
    let compact_offset = 32; // In src/purge.rs, it's the limit for rewrite.

    let mut rand_regions = Normal::new(128.0, 96.0)
        .unwrap()
        .sample_iter(thread_rng())
        .map(|x| x as u64);
    let mut rand_compacts = Normal::new(compact_offset as f64, 16.0)
        .unwrap()
        .sample_iter(thread_rng())
        .map(|x| x as u64);

    let mut batch = LogBatch::with_capacity(256);
    let mut entry = Entry::new();
    entry.set_data(vec![b'x'; 1024 * 32].into());
    let init_state = RaftLocalState {
        last_index: 0,
        ..Default::default()
    };
    loop {
        for _ in 0..1024 {
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
            engine.write_async(&mut batch).await.unwrap();
            // engine.write(&mut batch, true).unwrap();

            if state.last_index % compact_offset == 0 {
                let rand_compact_offset = rand_compacts.next().unwrap();
                if state.last_index > rand_compact_offset {
                    let compact_to = state.last_index - rand_compact_offset;
                    engine.compact_to_async(region, compact_to).await;
                    println!("[EXAMPLE] compact {} to {}", region, compact_to);
                }
            }
        }
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
    }
}
