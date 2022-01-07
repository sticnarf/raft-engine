// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use log::{error, info};
use protobuf::{parse_from_bytes, Message};

use crate::config::{Config, RecoveryMode};
use crate::consistency::ConsistencyChecker;
use crate::event_listener::EventListener;
use crate::file_builder::*;
use crate::file_pipe_log::debug::LogItemReader;
use crate::file_pipe_log::{FilePipeLog, FilePipeLogBuilder};
use crate::log_batch::{Command, LogBatch, MessageExt};
use crate::memtable::{EntryIndex, MemTableAccessor, MemTableRecoverContext};
use crate::metrics::*;
use crate::pipe_log::{FileBlockHandle, FileId, LogQueue, PipeLog};
use crate::purge::{PurgeHook, PurgeManager};
use crate::write_barrier::{WriteBarrier, Writer};
use crate::{Error, GlobalStats, Result};

pub struct Engine<B = DefaultFileBuilder>
where
    B: FileBuilder,
{
    cfg: Arc<Config>,
    listeners: Vec<Arc<dyn EventListener>>,

    stats: Arc<GlobalStats>,
    memtables: MemTableAccessor,
    pipe_log: Arc<FilePipeLog<B>>,
    purge_manager: PurgeManager<FilePipeLog<B>>,

    write_barrier: WriteBarrier<LogBatch, Result<FileBlockHandle>>,

    _phantom: PhantomData<B>,
}

impl Engine<DefaultFileBuilder> {
    pub fn open(cfg: Config) -> Result<Engine<DefaultFileBuilder>> {
        Self::open_with_listeners(cfg, vec![])
    }

    pub fn open_with_listeners(
        cfg: Config,
        listeners: Vec<Arc<dyn EventListener>>,
    ) -> Result<Engine<DefaultFileBuilder>> {
        Self::open_with(cfg, Arc::new(DefaultFileBuilder), listeners)
    }
}

impl<B> Engine<B>
where
    B: FileBuilder,
{
    pub fn open_with_file_builder(cfg: Config, file_builder: Arc<B>) -> Result<Engine<B>> {
        Self::open_with(cfg, file_builder, vec![])
    }

    pub fn open_with(
        mut cfg: Config,
        file_builder: Arc<B>,
        mut listeners: Vec<Arc<dyn EventListener>>,
    ) -> Result<Engine<B>> {
        cfg.sanitize()?;
        listeners.push(Arc::new(PurgeHook::new()) as Arc<dyn EventListener>);

        let start = Instant::now();
        let mut builder = FilePipeLogBuilder::new(cfg.clone(), file_builder, listeners.clone());
        builder.scan()?;
        let (append, rewrite) = builder.recover::<MemTableRecoverContext>()?;
        let pipe_log = Arc::new(builder.finish()?);
        rewrite.merge_append_context(append);
        let (memtables, stats) = rewrite.finish();
        info!("Recovering raft logs takes {:?}", start.elapsed());

        let cfg = Arc::new(cfg);
        let purge_manager = PurgeManager::new(
            cfg.clone(),
            memtables.clone(),
            pipe_log.clone(),
            stats.clone(),
            listeners.clone(),
        );

        Ok(Self {
            cfg,
            listeners,
            stats,
            memtables,
            pipe_log,
            purge_manager,
            write_barrier: Default::default(),
            _phantom: PhantomData,
        })
    }

    /// Writes the content of `log_batch` into the engine and returns written bytes.
    /// If `sync` is true, the write will be followed by a call to `fdatasync` on
    /// the log file.
    pub fn write(&self, log_batch: &mut LogBatch, mut sync: bool) -> Result<usize> {
        let start = Instant::now();
        let len = log_batch.finish_populate(self.cfg.batch_compression_threshold.0 as usize)?;
        let block_handle = {
            let mut writer = Writer::new(log_batch, sync, start);
            if let Some(mut group) = self.write_barrier.enter(&mut writer) {
                let now = Instant::now();
                let _t = StopWatch::new_with(&ENGINE_WRITE_LEADER_DURATION_HISTOGRAM, now);
                for writer in group.iter_mut() {
                    ENGINE_WRITE_PREPROCESS_DURATION_HISTOGRAM.observe(
                        now.saturating_duration_since(writer.start_time)
                            .as_secs_f64(),
                    );
                    sync |= writer.sync;
                    let log_batch = writer.get_payload();
                    let res = if !log_batch.is_empty() {
                        self.pipe_log
                            .append(LogQueue::Append, log_batch.encoded_bytes())
                    } else {
                        // TODO(tabokie): use Option<FileBlockHandle> instead.
                        Ok(FileBlockHandle {
                            id: FileId::new(LogQueue::Append, 0),
                            offset: 0,
                            len: 0,
                        })
                    };
                    writer.set_output(res);
                }
                if let Err(e) = self.pipe_log.maybe_sync(LogQueue::Append, sync) {
                    panic!(
                        "Cannot sync {:?} queue due to IO error: {}",
                        LogQueue::Append,
                        e
                    );
                }
            }
            writer.finish()?
        };

        let mut now = Instant::now();
        if len > 0 {
            log_batch.finish_write(block_handle);
            self.memtables.apply(log_batch.drain(), LogQueue::Append);
            for listener in &self.listeners {
                listener.post_apply_memtables(block_handle.id);
            }
            let end = Instant::now();
            ENGINE_WRITE_APPLY_DURATION_HISTOGRAM
                .observe(end.saturating_duration_since(now).as_secs_f64());
            now = end;
        }
        ENGINE_WRITE_DURATION_HISTOGRAM.observe(now.saturating_duration_since(start).as_secs_f64());
        ENGINE_WRITE_SIZE_HISTOGRAM.observe(len as f64);
        Ok(len)
    }

    pub async fn write_async(&self, log_batch: &mut LogBatch) -> Result<usize> {
        let start = Instant::now();
        let len = log_batch.finish_populate(self.cfg.batch_compression_threshold.0 as usize)?;
        let padded_len = (len + 4095) / 4096 * 4096;
        let block_handle = if !log_batch.is_empty() {
            let res = self
                .pipe_log
                .appender
                .append_async(log_batch.encoded_bytes())
                .await?;
            res
        } else {
            FileBlockHandle {
                id: FileId::new(LogQueue::Append, 0),
                offset: 0,
                len: 0,
            }
        };

        let mut now = Instant::now();
        if len > 0 {
            log_batch.finish_write(block_handle);
            self.memtables.apply(log_batch.drain(), LogQueue::Append);
            // for listener in &self.listeners {
            //     listener.post_apply_memtables(block_handle.id);
            // }
            let end = Instant::now();
            ENGINE_WRITE_APPLY_DURATION_HISTOGRAM
                .observe(end.saturating_duration_since(now).as_secs_f64());
            now = end;
        }
        ENGINE_WRITE_DURATION_HISTOGRAM.observe(now.saturating_duration_since(start).as_secs_f64());
        ENGINE_WRITE_SIZE_HISTOGRAM.observe(padded_len as f64);
        Ok(padded_len)
    }

    /// Synchronizes the Raft engine.
    pub fn sync(&self) -> Result<()> {
        // TODO(tabokie): use writer.
        self.pipe_log.maybe_sync(LogQueue::Append, true)
    }

    pub fn get_message<S: Message>(&self, region_id: u64, key: &[u8]) -> Result<Option<S>> {
        let _t = StopWatch::new(&ENGINE_READ_MESSAGE_DURATION_HISTOGRAM);
        if let Some(memtable) = self.memtables.get(region_id) {
            if let Some(value) = memtable.read().get(key) {
                return Ok(Some(parse_from_bytes(&value)?));
            }
        }
        Ok(None)
    }

    pub fn get_entry<M: MessageExt>(
        &self,
        region_id: u64,
        log_idx: u64,
    ) -> Result<Option<M::Entry>> {
        let _t = StopWatch::new(&ENGINE_READ_ENTRY_DURATION_HISTOGRAM);
        if let Some(memtable) = self.memtables.get(region_id) {
            if let Some(idx) = memtable.read().get_entry(log_idx) {
                ENGINE_READ_ENTRY_COUNT_HISTOGRAM.observe(1.0);
                return Ok(Some(
                    read_entry_from_file::<M, _>(self.pipe_log.as_ref(), &idx).map_err(|e| {
                        error!(
                            "failed to load entry from file, region {}, idx {}",
                            region_id, log_idx
                        );
                        e
                    })?,
                ));
            } else {
                error!(
                    "failed to find entry from memtable, region {}, idx {}",
                    region_id, log_idx
                );
            }
        }
        Ok(None)
    }

    /// Purges expired logs files and returns a set of Raft group ids that need
    /// to be compacted.
    pub fn purge_expired_files(&self) -> Result<Vec<u64>> {
        // TODO: Move this to a dedicated thread.
        self.stats.flush_metrics();
        self.purge_manager.purge_expired_files()
    }

    /// Returns count of fetched entries.
    pub fn fetch_entries_to<M: MessageExt>(
        &self,
        region_id: u64,
        begin: u64,
        end: u64,
        max_size: Option<usize>,
        vec: &mut Vec<M::Entry>,
    ) -> Result<usize> {
        let _t = StopWatch::new(&ENGINE_READ_ENTRY_DURATION_HISTOGRAM);
        if let Some(memtable) = self.memtables.get(region_id) {
            let mut ents_idx: Vec<EntryIndex> = Vec::with_capacity((end - begin) as usize);
            memtable
                .read()
                .fetch_entries_to(begin, end, max_size, &mut ents_idx)?;
            for i in ents_idx.iter() {
                vec.push(read_entry_from_file::<M, _>(self.pipe_log.as_ref(), i)?);
            }
            ENGINE_READ_ENTRY_COUNT_HISTOGRAM.observe(ents_idx.len() as f64);
            return Ok(ents_idx.len());
        }
        Ok(0)
    }

    pub fn first_index(&self, region_id: u64) -> Option<u64> {
        if let Some(memtable) = self.memtables.get(region_id) {
            return memtable.read().first_index();
        }
        None
    }

    pub fn last_index(&self, region_id: u64) -> Option<u64> {
        if let Some(memtable) = self.memtables.get(region_id) {
            return memtable.read().last_index();
        }
        None
    }

    /// Deletes log entries before `index` in the specified Raft group. Returns
    /// the number of deleted entries.
    pub fn compact_to(&self, region_id: u64, index: u64) -> u64 {
        let first_index = match self.first_index(region_id) {
            Some(index) => index,
            None => return 0,
        };

        let mut log_batch = LogBatch::default();
        log_batch.add_command(region_id, Command::Compact { index });
        if let Err(e) = self.write(&mut log_batch, false) {
            error!("Failed to write Compact command: {}", e);
        }

        self.first_index(region_id).unwrap_or(index) - first_index
    }

    pub async fn compact_to_async(&self, region_id: u64, index: u64) -> u64 {
        let first_index = match self.first_index(region_id) {
            Some(index) => index,
            None => return 0,
        };

        let mut log_batch = LogBatch::default();
        log_batch.add_command(region_id, Command::Compact { index });
        if let Err(e) = self.write_async(&mut log_batch).await {
            error!("Failed to write Compact command: {}", e);
        }

        self.first_index(region_id).unwrap_or(index) - first_index
    }

    pub fn raft_groups(&self) -> Vec<u64> {
        self.memtables.fold(vec![], |mut v, m| {
            v.push(m.region_id());
            v
        })
    }

    // For testing.
    pub fn file_span(&self, queue: LogQueue) -> (u64, u64) {
        self.pipe_log.file_span(queue)
    }

    pub fn get_used_size(&self) -> usize {
        self.pipe_log.total_size(LogQueue::Append) + self.pipe_log.total_size(LogQueue::Rewrite)
    }
}

impl Engine<DefaultFileBuilder> {
    pub fn consistency_check(path: &Path) -> Result<Vec<(u64, u64)>> {
        Self::consistency_check_with_file_builder(path, Arc::new(DefaultFileBuilder))
    }

    pub fn unsafe_truncate(
        path: &Path,
        mode: &str,
        queue: Option<LogQueue>,
        raft_groups: &[u64],
    ) -> Result<()> {
        Self::unsafe_truncate_with_file_builder(
            path,
            mode,
            queue,
            raft_groups,
            Arc::new(DefaultFileBuilder),
        )
    }

    pub fn dump(path: &Path) -> Result<LogItemReader<DefaultFileBuilder>> {
        Self::dump_with_file_builder(path, Arc::new(DefaultFileBuilder))
    }
}

impl<B> Engine<B>
where
    B: FileBuilder,
{
    /// Returns a list of corrupted Raft groups, including their ids and last valid
    /// log index. Head or tail corruption cannot be detected.
    pub fn consistency_check_with_file_builder(
        path: &Path,
        file_builder: Arc<B>,
    ) -> Result<Vec<(u64, u64)>> {
        if !path.exists() {
            return Err(Error::InvalidArgument(format!(
                "raft-engine directory '{}' does not exist.",
                path.to_str().unwrap()
            )));
        }

        let cfg = Config {
            dir: path.to_str().unwrap().to_owned(),
            recovery_mode: RecoveryMode::TolerateAnyCorruption,
            ..Default::default()
        };
        let mut builder = FilePipeLogBuilder::new(cfg, file_builder, Vec::new());
        builder.scan()?;
        let (append, rewrite) = builder.recover::<ConsistencyChecker>()?;
        let mut map = rewrite.finish();
        for (id, index) in append.finish() {
            map.entry(id).or_insert(index);
        }
        let mut list: Vec<(u64, u64)> = map.into_iter().collect();
        list.sort_unstable();
        Ok(list)
    }

    /// Truncates Raft groups to remove possible corruptions.
    #[allow(unused_variables)]
    pub fn unsafe_truncate_with_file_builder(
        path: &Path,
        mode: &str,
        queue: Option<LogQueue>,
        raft_groups: &[u64],
        file_builder: Arc<B>,
    ) -> Result<()> {
        todo!();
    }

    /// Dumps all operations.
    pub fn dump_with_file_builder(path: &Path, file_builder: Arc<B>) -> Result<LogItemReader<B>> {
        if !path.exists() {
            return Err(Error::InvalidArgument(format!(
                "raft-engine directory or file '{}' does not exist.",
                path.to_str().unwrap()
            )));
        }

        if path.is_dir() {
            LogItemReader::new_directory_reader(file_builder, path)
        } else {
            LogItemReader::new_file_reader(file_builder, path)
        }
    }
}

pub(crate) fn read_entry_from_file<M, P>(pipe_log: &P, ent_idx: &EntryIndex) -> Result<M::Entry>
where
    M: MessageExt,
    P: PipeLog,
{
    let buf = pipe_log.read_bytes(ent_idx.entries.unwrap())?;
    let e = LogBatch::parse_entry::<M>(&buf, ent_idx)?;
    assert_eq!(M::index(&e), ent_idx.index);
    Ok(e)
}

pub(crate) fn read_entry_bytes_from_file<P>(pipe_log: &P, ent_idx: &EntryIndex) -> Result<Vec<u8>>
where
    P: PipeLog,
{
    let entries_buf = pipe_log.read_bytes(ent_idx.entries.unwrap())?;
    LogBatch::parse_entry_bytes(&entries_buf, ent_idx)
}
