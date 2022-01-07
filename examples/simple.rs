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
            .spawn(move || run(engine, i + 5))
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
                    engine
                        .compact_to_async(region, state.last_index - 10000)
                        .await;
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
}

async fn run(engine: Arc<Engine>, region: u64) {
    let mut entry = Entry::new();
    entry.set_data(vec![b'x'; 17462].into());
    let mut state = RaftLocalState {
        last_index: 0,
        ..Default::default()
    };

    let semaphore = Rc::new(Semaphore::new(256));

    loop {
        let mut batch = LogBatch::with_capacity(256);
        state.last_index += 1;
        let mut e = entry.clone();
        e.index = state.last_index;
        batch.add_entries::<MessageExtTyped>(region, &[e]).unwrap();
        batch
            .put_message(region, b"last_index".to_vec(), &state)
            .unwrap();
        // engine.write_async(&mut batch).await.unwrap();
        let permit = semaphore.acquire_static_permit(1).await.unwrap();
        let engine_ = engine.clone();
        glommio::spawn_local(async move {
            engine_.write_async(&mut batch).await.unwrap();
            drop(permit);
        })
        .detach();
        // let got_entry = engine
        //     .get_entry::<MessageExtTyped>(region, state.last_index)
        //     .unwrap();

        if state.last_index > 10000 && state.last_index % 10000 == 0 {
            engine
                .compact_to_async(region, state.last_index - 10000)
                .await;
        }
        // println!("{:?}", got_entry);
    }
}
