// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use log::{error, info};
use parking_lot::{Mutex, RwLock};

use crate::config::Config;
use crate::engine::read_entry_bytes_from_file;
use crate::event_listener::EventListener;
use crate::log_batch::{Command, LogBatch, LogItemContent, OpType};
use crate::memtable::{MemTable, MemTableAccessor};
use crate::metrics::*;
use crate::pipe_log::{FileBlockHandle, FileId, FileSeq, LogQueue, PipeLog};
use crate::{GlobalStats, Result};

// Force compact region with oldest 20% logs.
const FORCE_COMPACT_RATIO: f64 = 0.2;
// Only rewrite region with oldest 70% logs.
const REWRITE_RATIO: f64 = 0.7;
// Only rewrite region with stale logs less than this threshold.
const MAX_REWRITE_ENTRIES_PER_REGION: usize = 32;
const MAX_REWRITE_BATCH_BYTES: usize = 128 * 1024;

pub struct PurgeManager<P>
where
    P: PipeLog,
{
    cfg: Arc<Config>,
    memtables: MemTableAccessor,
    pipe_log: Arc<P>,
    global_stats: Arc<GlobalStats>,
    listeners: Vec<Arc<dyn EventListener>>,

    // Only one thread can run `purge_expired_files` at a time.
    purge_mutex: Arc<Mutex<()>>,
}

impl<P> PurgeManager<P>
where
    P: PipeLog,
{
    pub fn new(
        cfg: Arc<Config>,
        memtables: MemTableAccessor,
        pipe_log: Arc<P>,
        global_stats: Arc<GlobalStats>,
        listeners: Vec<Arc<dyn EventListener>>,
    ) -> PurgeManager<P> {
        PurgeManager {
            cfg,
            memtables,
            pipe_log,
            global_stats,
            listeners,
            purge_mutex: Arc::new(Mutex::new(())),
        }
    }

    pub fn purge_expired_files(&self) -> Result<Vec<u64>> {
        let _t = StopWatch::new(&ENGINE_PURGE_DURATION_HISTOGRAM);
        let guard = self.purge_mutex.try_lock();
        let mut should_compact = vec![];
        if guard.is_none() {
            error!("Failed to purge expired files: locked");
            return Ok(should_compact);
        }

        if self.needs_rewrite_log_files(LogQueue::Rewrite) {
            self.rewrite_rewrite_queue()?;
            self.purge_to(
                LogQueue::Rewrite,
                self.pipe_log.file_span(LogQueue::Rewrite).1,
            )?;
        }

        if self.needs_rewrite_log_files(LogQueue::Append) {
            if let (Some(rewrite_watermark), Some(compact_watermark)) =
                self.append_queue_watermarks()
            {
                info!("start purging {} {}", rewrite_watermark, compact_watermark);
                should_compact =
                    self.rewrite_or_compact_append_queue(rewrite_watermark, compact_watermark)?;
                let (first_append, latest_append) = self.pipe_log.file_span(LogQueue::Append);
                let append_queue_barrier =
                    self.listeners.iter().fold(latest_append, |barrier, l| {
                        l.first_file_not_ready_for_purge(LogQueue::Append)
                            .map_or(barrier, |f| std::cmp::min(f, barrier))
                    });
                if append_queue_barrier == first_append && first_append < latest_append {
                    error!("Failed to purge expired files: blocked by barrier");
                }
                // Ordering: make sure we rewrite all tombstones before `append_queue_barrier`.
                self.rewrite_append_queue_tombstones()?;
                self.purge_to(LogQueue::Append, append_queue_barrier)?;
            }
        }
        Ok(should_compact)
    }

    /// Rewrite append files with seqno no larger than `watermark`. When it's None,
    /// rewrite the entire queue. Returns the number of purged files.
    #[cfg(test)]
    pub fn must_rewrite_append_queue(
        &self,
        watermark: Option<FileSeq>,
        exit_after_step: Option<u64>,
    ) {
        let _lk = self.purge_mutex.try_lock().unwrap();
        let (_, last) = self.pipe_log.file_span(LogQueue::Append);
        let watermark = watermark.map_or(last, |w| std::cmp::min(w, last));
        if watermark == last {
            self.pipe_log.rotate(LogQueue::Append).unwrap();
        }
        self.rewrite_memtables(self.memtables.collect(|_| true), 0, Some(watermark))
            .unwrap();
        if exit_after_step == Some(1) {
            return;
        }
        self.rewrite_append_queue_tombstones().unwrap();
        if exit_after_step == Some(2) {
            return;
        }
        self.purge_to(
            LogQueue::Append,
            self.pipe_log.file_span(LogQueue::Append).1,
        )
        .unwrap();
    }

    #[cfg(test)]
    pub fn must_rewrite_rewrite_queue(&self) {
        let _lk = self.purge_mutex.try_lock().unwrap();
        self.rewrite_rewrite_queue().unwrap();
        self.purge_to(
            LogQueue::Rewrite,
            self.pipe_log.file_span(LogQueue::Rewrite).1,
        )
        .unwrap();
    }

    pub(crate) fn needs_rewrite_log_files(&self, queue: LogQueue) -> bool {
        let (first_file, active_file) = self.pipe_log.file_span(queue);
        if active_file == first_file {
            return false;
        }

        let total_size = self.pipe_log.total_size(queue);
        match queue {
            LogQueue::Append => total_size > self.cfg.purge_threshold.0 as usize,
            LogQueue::Rewrite => {
                let compacted_rewrites_ratio = self.global_stats.deleted_rewrite_entries() as f64
                    / self.global_stats.rewrite_entries() as f64;
                total_size > self.cfg.purge_rewrite_threshold.unwrap().0 as usize
                    && compacted_rewrites_ratio > self.cfg.purge_rewrite_garbage_ratio
            }
        }
    }

    // Returns (rewrite_watermark, compact_watermark).
    // Files older than compact_watermark should be compacted;
    // Files between compact_watermark and rewrite_watermark should be rewritten.
    fn append_queue_watermarks(&self) -> (Option<FileSeq>, Option<FileSeq>) {
        let queue = LogQueue::Append;

        let (first_file, active_file) = self.pipe_log.file_span(queue);
        if active_file == first_file {
            // Can't rewrite or force compact the active file.
            return (None, None);
        }
        let oldest = crate::file_pipe_log::WRITING_SEQS
            .lock()
            .iter()
            .next()
            .map(|(seq, _)| *seq)
            .unwrap_or(u64::MAX);
        let active_file = std::cmp::min(active_file, oldest);

        let rewrite_watermark = self.pipe_log.file_at(queue, REWRITE_RATIO);
        let compact_watermark = self.pipe_log.file_at(queue, FORCE_COMPACT_RATIO);
        debug_assert!(active_file - 1 > 0);
        (
            Some(std::cmp::min(rewrite_watermark, active_file - 1)),
            Some(std::cmp::min(compact_watermark, active_file - 1)),
        )
    }

    fn rewrite_or_compact_append_queue(
        &self,
        rewrite_watermark: FileSeq,
        compact_watermark: FileSeq,
    ) -> Result<Vec<u64>> {
        let _t = StopWatch::new(&ENGINE_REWRITE_APPEND_DURATION_HISTOGRAM);
        debug_assert!(compact_watermark <= rewrite_watermark);
        let mut should_compact = Vec::with_capacity(16);

        let memtables = self.memtables.collect(|t| {
            if let Some(f) = t.min_file_seq(LogQueue::Append) {
                let sparse = t
                    .entries_count_before(FileId::new(LogQueue::Append, rewrite_watermark))
                    < MAX_REWRITE_ENTRIES_PER_REGION;
                if f < compact_watermark && !sparse {
                    should_compact.push(t.region_id());
                } else if f < rewrite_watermark {
                    return sparse;
                }
            }
            false
        });

        self.rewrite_memtables(
            memtables,
            MAX_REWRITE_ENTRIES_PER_REGION,
            Some(rewrite_watermark),
        )?;

        Ok(should_compact)
    }

    // Rewrites the entire rewrite queue into new log files.
    fn rewrite_rewrite_queue(&self) -> Result<()> {
        let _t = StopWatch::new(&ENGINE_REWRITE_REWRITE_DURATION_HISTOGRAM);
        self.pipe_log.rotate(LogQueue::Rewrite)?;

        let memtables = self
            .memtables
            .collect(|t| t.min_file_seq(LogQueue::Rewrite).is_some());

        self.rewrite_memtables(memtables, 0 /*expect_rewrites_per_memtable*/, None)?;
        self.global_stats.reset_rewrite_counters();
        Ok(())
    }

    fn rewrite_append_queue_tombstones(&self) -> Result<()> {
        let mut log_batch = self.memtables.take_cleaned_region_logs();
        self.rewrite_impl(
            &mut log_batch,
            None, /*rewrite_watermark*/
            true, /*sync*/
        )
    }

    // Exclusive.
    fn purge_to(&self, queue: LogQueue, seq: FileSeq) -> Result<()> {
        let min_seq = self.memtables.fold(seq, |min, t| {
            t.min_file_seq(queue).map_or(min, |m| std::cmp::min(min, m))
        });

        let purged = self.pipe_log.purge_to(FileId {
            queue,
            seq: min_seq,
        })?;
        if purged > 0 {
            info!("purged {} expired log files for queue {:?}", purged, queue);
            // for listener in &self.listeners {
            //     listener.post_purge(FileId {
            //         queue,
            //         seq: min_seq - 1,
            //     });
            // }
        }
        Ok(())
    }

    fn rewrite_memtables(
        &self,
        memtables: Vec<Arc<RwLock<MemTable>>>,
        expect_rewrites_per_memtable: usize,
        rewrite: Option<FileSeq>,
    ) -> Result<()> {
        let mut log_batch = LogBatch::default();
        let mut total_size = 0;
        for memtable in memtables {
            let mut entry_indexes = Vec::with_capacity(expect_rewrites_per_memtable);
            let mut entries = Vec::with_capacity(expect_rewrites_per_memtable);
            let mut kvs = Vec::new();
            let region_id = {
                let m = memtable.read();
                if let Some(rewrite) = rewrite {
                    m.fetch_entry_indexes_before(rewrite, &mut entry_indexes)?;
                    m.fetch_kvs_before(rewrite, &mut kvs);
                } else {
                    m.fetch_rewritten_entry_indexes(&mut entry_indexes)?;
                    m.fetch_rewritten_kvs(&mut kvs);
                }
                m.region_id()
            };

            let mut cursor = 0;
            while cursor < entry_indexes.len() {
                let entry =
                    read_entry_bytes_from_file(self.pipe_log.as_ref(), &entry_indexes[cursor])?;
                total_size += entry.len();
                entries.push(entry);
                if total_size > MAX_REWRITE_BATCH_BYTES {
                    let mut take_entries = Vec::with_capacity(expect_rewrites_per_memtable);
                    std::mem::swap(&mut take_entries, &mut entries);
                    let mut take_entry_indexes = entry_indexes.split_off(cursor + 1);
                    std::mem::swap(&mut take_entry_indexes, &mut entry_indexes);
                    log_batch.add_raw_entries(region_id, take_entry_indexes, take_entries)?;
                    self.rewrite_impl(&mut log_batch, rewrite, false)?;
                    total_size = 0;
                    cursor = 0;
                } else {
                    cursor += 1;
                }
            }
            if !entries.is_empty() {
                log_batch.add_raw_entries(region_id, entry_indexes, entries)?;
            }
            for (k, v) in kvs {
                log_batch.put(region_id, k, v);
            }
        }
        self.rewrite_impl(&mut log_batch, rewrite, true)
    }

    fn rewrite_impl(
        &self,
        log_batch: &mut LogBatch,
        rewrite_watermark: Option<FileSeq>,
        sync: bool,
    ) -> Result<()> {
        let len = log_batch.finish_populate(self.cfg.batch_compression_threshold.0 as usize)?;
        if len == 0 {
            return self.pipe_log.maybe_sync(LogQueue::Rewrite, sync);
        }
        let file_handle = self
            .pipe_log
            .append(LogQueue::Rewrite, log_batch.encoded_bytes())?;
        self.pipe_log.maybe_sync(LogQueue::Rewrite, sync)?;
        log_batch.finish_write(file_handle);
        for item in log_batch.drain() {
            let raft = item.raft_group_id;
            let memtable = self.memtables.get_or_insert(raft);
            match item.content {
                LogItemContent::EntryIndexes(entries_to_add) => {
                    let entry_indexes = entries_to_add.0;
                    memtable.write().rewrite(entry_indexes, rewrite_watermark);
                }
                LogItemContent::Kv(kv) => match kv.op_type {
                    OpType::Put => {
                        let key = kv.key;
                        memtable
                            .write()
                            .rewrite_key(key, rewrite_watermark, file_handle.id.seq);
                    }
                    _ => unreachable!(),
                },
                LogItemContent::Command(Command::Clean) => {}
                _ => unreachable!(),
            }
        }
        for listener in &self.listeners {
            listener.post_apply_memtables(file_handle.id);
        }
        if rewrite_watermark.is_none() {
            BACKGROUND_REWRITE_BYTES
                .rewrite
                .observe(file_handle.len as f64);
        } else {
            BACKGROUND_REWRITE_BYTES
                .append
                .observe(file_handle.len as f64);
        }
        Ok(())
    }
}

pub struct PurgeHook {
    // Append queue log files that are not yet fully applied to MemTable must not be
    // purged even when not referenced by any MemTable.
    // In order to identify them, maintain a per-file reference counter for all active
    // log files in append queue. No need to track rewrite queue because it is only
    // written by purge thread.
    active_log_files: RwLock<VecDeque<(FileSeq, AtomicUsize)>>,
}

impl PurgeHook {
    pub fn new() -> Self {
        PurgeHook {
            active_log_files: Default::default(),
        }
    }
}

impl EventListener for PurgeHook {
    fn post_new_log_file(&self, file_id: FileId) {
        if file_id.queue == LogQueue::Append {
            let mut active_log_files = self.active_log_files.write();
            if let Some(seq) = active_log_files.back().map(|x| x.0) {
                assert_eq!(
                    seq + 1,
                    file_id.seq,
                    "active log files should be contiguous"
                );
            }
            let counter = AtomicUsize::new(0);
            active_log_files.push_back((file_id.seq, counter));
        }
    }

    fn on_append_log_file(&self, handle: FileBlockHandle) {
        if handle.id.queue == LogQueue::Append {
            let active_log_files = self.active_log_files.read();
            assert!(!active_log_files.is_empty());
            let front = active_log_files[0].0;
            let counter = &active_log_files[(handle.id.seq - front) as usize].1;
            counter.fetch_add(1, Ordering::Release);
        }
    }

    fn post_apply_memtables(&self, file_id: FileId) {
        if file_id.queue == LogQueue::Append {
            let active_log_files = self.active_log_files.read();
            assert!(!active_log_files.is_empty());
            let front = active_log_files[0].0;
            let counter = &active_log_files[(file_id.seq - front) as usize].1;
            counter.fetch_sub(1, Ordering::Release);
        }
    }

    fn first_file_not_ready_for_purge(&self, queue: LogQueue) -> Option<FileSeq> {
        if queue == LogQueue::Append {
            let active_log_files = self.active_log_files.read();
            for (id, counter) in active_log_files.iter() {
                if counter.load(Ordering::Acquire) > 0 {
                    return Some(*id);
                }
            }
        }
        None
    }

    fn post_purge(&self, file_id: FileId) {
        if file_id.queue == LogQueue::Append {
            let mut active_log_files = self.active_log_files.write();
            assert!(!active_log_files.is_empty());
            let front = active_log_files[0].0;
            if front <= file_id.seq {
                let mut purged = active_log_files.drain(0..=(file_id.seq - front) as usize);
                assert_eq!(purged.next_back().unwrap().0, file_id.seq);
            }
        }
    }
}
