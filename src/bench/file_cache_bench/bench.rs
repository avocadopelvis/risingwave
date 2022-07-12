use std::time::Instant;

use bytes::{Buf, BufMut};
use clap::Parser;
use futures::future;
use itertools::Itertools;
use rand::prelude::SliceRandom;
use rand::Rng;
use risingwave_storage::hummock::file_cache::cache::{FileCache, FileCacheOptions};
use risingwave_storage::hummock::file_cache::coding::CacheKey;
use tokio::sync::oneshot;

#[derive(Parser, Debug, Clone)]
struct Args {
    #[clap(short, long)]
    path: String,
    #[clap(long, default_value = "1073741824")] // 1 GiB
    capacity: usize,
    #[clap(long, default_value = "134217728")] // 2 * 64 MiB
    total_buffer_capacity: usize,
    #[clap(long, default_value = "67108864")] // 64 MiB
    cache_file_fallocate_unit: usize,

    #[clap(long, default_value = "1048576")] // 1 MiB
    bs: usize,
    #[clap(long, default_value = "8")]
    concurrency: usize,
    #[clap(long, default_value = "268435456")] // 256 MiB/s
    throughput: usize,
    #[clap(long, default_value = "600")] // 600s
    time: u64,
    #[clap(long, default_value = "2")]
    read: usize,
    #[clap(long, default_value = "1")]
    write: usize,
    #[clap(long, default_value = "0.5")]
    miss: f64,
}

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
struct Index {
    sst: u32,
    idx: u32,
}

impl CacheKey for Index {
    fn encoded_len() -> usize {
        8
    }

    fn encode(&self, mut buf: &mut [u8]) {
        buf.put_u32(self.sst);
        buf.put_u32(self.idx);
    }

    fn decode(mut buf: &[u8]) -> Self {
        let sst = buf.get_u32();
        let idx = buf.get_u32();
        Self { sst, idx }
    }
}

pub async fn run() {
    let args = Args::parse();

    let options = FileCacheOptions {
        dir: args.path.clone(),
        capacity: args.capacity,
        total_buffer_capacity: args.total_buffer_capacity,
        cache_file_fallocate_unit: args.cache_file_fallocate_unit,
        filters: vec![],
        flush_buffer_hooks: vec![],
    };

    let cache: FileCache<Index> = FileCache::open(options).await.unwrap();

    let (txs, rxs): (Vec<_>, Vec<_>) = (0..args.concurrency).map(|_| oneshot::channel()).unzip();

    let futures = rxs
        .into_iter()
        .enumerate()
        .map(|(id, rx)| bench(id, args.clone(), cache.clone(), args.time, rx))
        .collect_vec();

    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        for tx in txs {
            tx.send(()).unwrap();
        }
    });
    future::join_all(futures).await;
}

async fn bench(
    id: usize,
    args: Args,
    cache: FileCache<Index>,
    time: u64,
    mut stop: oneshot::Receiver<()>,
) {
    let start = Instant::now();

    let mut rng = rand::thread_rng();
    let sst = id as u32;
    let mut idx = 0;

    let nkey = Index {
        sst: u32::MAX,
        idx: u32::MAX,
    };

    loop {
        match stop.try_recv() {
            Err(oneshot::error::TryRecvError::Empty) => {}
            _ => return,
        }
        if start.elapsed().as_secs() >= time {
            return;
        }

        let mut keys = Vec::with_capacity(args.write);
        for _ in 0..args.write {
            idx += 1;
            let key = Index { sst, idx };
            let value = vec![b'x'; args.bs];
            cache.insert(key.clone(), value).unwrap();
            keys.push(key);
        }
        for _ in 0..args.read {
            let key = if rng.gen_range(0f64..1f64) < args.miss {
                &nkey
            } else {
                keys.choose(&mut rng).unwrap()
            };
            cache.get(key).await.unwrap();
        }
    }
}
