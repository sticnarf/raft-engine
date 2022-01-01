// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_lock::Mutex as AsyncMutex;
use crossbeam::utils::CachePadded;
use fail::fail_point;
use fs2::FileExt;
use futures_lite::future::block_on;
use glommio::io::{DmaFile, OpenOptions};
use libc::*;
use log::{error, info, warn};
use parking_lot::RwLock;

use crate::config::Config;
use crate::event_listener::EventListener;
use crate::file_builder::FileBuilder;
use crate::metrics::*;
use crate::pipe_log::{FileBlockHandle, FileId, FileSeq, LogQueue, PipeLog};
use crate::{Error, Result};

use super::async_pipe::AsyncPipe;
use super::format::{lock_file_path, FileNameExt};
use super::log_file::{build_file_reader, build_file_writer, LogFd, LogFileWriter};

struct FileCollection {
    first_seq: FileSeq,
    active_seq: FileSeq,
    fds: VecDeque<Arc<LogFd>>,
}

struct ActiveFile<B: FileBuilder> {
    seq: FileSeq,
    path: PathBuf,
    writer: LogFileWriter<B>,
}

thread_local! {
    static DMA_FILE: RefCell<Option<(FileSeq, Rc<DmaFile>)>> = RefCell::new(None);
}

pub struct SinglePipe<B: FileBuilder> {
    queue: LogQueue,
    dir: String,
    target_file_size: usize,
    bytes_per_sync: usize,
    file_builder: Arc<B>,
    listeners: Vec<Arc<dyn EventListener>>,

    files: CachePadded<RwLock<FileCollection>>,
    active_file: CachePadded<AsyncMutex<ActiveFile<B>>>,
}

impl<B: FileBuilder> Drop for SinglePipe<B> {
    fn drop(&mut self) {
        let mut active_file = block_on(self.active_file.lock());
        if let Err(e) = active_file.writer.close() {
            error!("error while closing sigle pipe: {}", e);
        }
    }
}

impl<B: FileBuilder> SinglePipe<B> {
    pub fn open(
        cfg: &Config,
        file_builder: Arc<B>,
        listeners: Vec<Arc<dyn EventListener>>,
        queue: LogQueue,
        mut first_seq: FileSeq,
        mut fds: VecDeque<Arc<LogFd>>,
    ) -> Result<Self> {
        let create_file = first_seq == 0;
        let active_seq = if create_file {
            first_seq = 1;
            let file_id = FileId {
                queue,
                seq: first_seq,
            };
            let fd = Arc::new(LogFd::create(&file_id.build_file_path(&cfg.dir))?);
            fds.push_back(fd);
            first_seq
        } else {
            first_seq + fds.len() as u64 - 1
        };

        for seq in first_seq..=active_seq {
            for listener in &listeners {
                listener.post_new_log_file(FileId { queue, seq });
            }
        }

        let active_fd = fds.back().unwrap().clone();
        let file_id = FileId {
            queue,
            seq: active_seq,
        };
        let active_file = ActiveFile {
            seq: active_seq,
            path: file_id.build_file_path(&cfg.dir),
            writer: build_file_writer(
                file_builder.as_ref(),
                &file_id.build_file_path(&cfg.dir),
                active_fd,
                create_file,
            )?,
        };

        let total_files = fds.len();
        let pipe = Self {
            queue,
            dir: cfg.dir.clone(),
            target_file_size: cfg.target_file_size.0 as usize,
            bytes_per_sync: cfg.bytes_per_sync.0 as usize,
            file_builder,
            listeners,

            files: CachePadded::new(RwLock::new(FileCollection {
                first_seq,
                active_seq,
                fds,
            })),
            active_file: CachePadded::new(AsyncMutex::new(active_file)),
        };
        pipe.flush_metrics(total_files);
        Ok(pipe)
    }

    fn sync_dir(&self) -> Result<()> {
        let path = PathBuf::from(&self.dir);
        std::fs::File::open(path).and_then(|d| d.sync_all())?;
        Ok(())
    }

    fn get_fd(&self, file_seq: FileSeq) -> Result<Arc<LogFd>> {
        let files = self.files.read();
        if file_seq < files.first_seq || file_seq > files.active_seq {
            return Err(Error::Corruption("file seqno out of range".to_owned()));
        }
        Ok(files.fds[(file_seq - files.first_seq) as usize].clone())
    }

    fn rotate_imp(&self, active_file: &mut ActiveFile<B>) -> Result<()> {
        let _t = StopWatch::new(&LOG_ROTATE_DURATION_HISTOGRAM);
        let seq = active_file.seq + 1;
        debug_assert!(seq > 1);

        let file_id = FileId {
            queue: self.queue,
            seq,
        };
        let path = file_id.build_file_path(&self.dir);
        let fd = Arc::new(LogFd::create(&path)?);
        self.sync_dir()?;
        let new_file = ActiveFile {
            seq,
            path: path.clone(),
            writer: build_file_writer(
                self.file_builder.as_ref(),
                &path,
                fd.clone(),
                true, /*create*/
            )?,
        };

        active_file.writer.close()?;
        *active_file = new_file;

        let len = {
            let mut files = self.files.write();
            debug_assert!(files.active_seq + 1 == seq);
            files.active_seq = seq;
            files.fds.push_back(fd);
            for listener in &self.listeners {
                listener.post_new_log_file(FileId {
                    queue: self.queue,
                    seq,
                });
            }
            files.fds.len()
        };
        self.flush_metrics(len);
        Ok(())
    }

    fn flush_metrics(&self, len: usize) {
        match self.queue {
            LogQueue::Append => LOG_FILE_COUNT.append.set(len as i64),
            LogQueue::Rewrite => LOG_FILE_COUNT.rewrite.set(len as i64),
        }
    }
}

impl<B: FileBuilder> SinglePipe<B> {
    fn read_bytes(&self, handle: FileBlockHandle) -> Result<Vec<u8>> {
        let fd = self.get_fd(handle.id.seq)?;
        let mut reader = build_file_reader(
            self.file_builder.as_ref(),
            &handle.id.build_file_path(&self.dir),
            fd,
        )?;
        reader.read(handle)
    }

    fn append(&self, bytes: &[u8]) -> Result<FileBlockHandle> {
        fail_point!("file_pipe_log::append");
        let mut active_file = block_on(self.active_file.lock());
        let seq = active_file.seq;
        let writer = &mut active_file.writer;

        let start_offset = writer.offset();
        if let Err(e) = writer.write(bytes, self.target_file_size) {
            if let Err(te) = writer.truncate() {
                panic!(
                    "error when truncate {} after error: {}, get: {}",
                    seq, e, te
                );
            }
            return Err(e);
        }
        let handle = FileBlockHandle {
            id: FileId {
                queue: self.queue,
                seq,
            },
            offset: start_offset as u64,
            len: writer.offset() - start_offset,
        };
        for listener in &self.listeners {
            listener.on_append_log_file(handle);
        }
        Ok(handle)
    }

    pub async fn append_async(&self, bytes: &[u8]) -> Result<FileBlockHandle> {
        let lock_begin = Instant::now();
        let mut active_file = self.active_file.lock().await;
        let lock_elapsed = lock_begin.elapsed();
        if lock_elapsed > Duration::from_millis(100) {
            warn!(
                "lock active file too long: {:?}, {}",
                lock_elapsed, active_file.seq
            );
        }
        let seq = active_file.seq;
        let file = DMA_FILE.with(|file| file.borrow().clone());

        async fn open_file(seq: u64, path: &Path) -> Result<Rc<DmaFile>> {
            let file = Rc::new(
                OpenOptions::new()
                    .write(true)
                    .custom_flags(O_DSYNC)
                    .dma_open(path)
                    .await?,
            );
            DMA_FILE.with(|cell| *cell.borrow_mut() = Some((seq, file.clone())));
            Ok(file)
        }

        let file = match file {
            Some((curr_seq, file)) if seq == curr_seq => file,
            Some((_, file)) => {
                DMA_FILE.with(|cell| *cell.borrow_mut() = None);
                file.close_rc().await?;
                open_file(seq, &active_file.path).await?
            }
            None => open_file(seq, &active_file.path).await?,
        };
        let start_offset = active_file.writer.written as u64;
        let padded_len = (bytes.len() + 4095) / 4096 * 4096;
        active_file.writer.written += padded_len;
        drop(active_file);

        let mut buf = file.alloc_dma_buffer(padded_len);
        buf.as_bytes_mut()[..bytes.len()].copy_from_slice(bytes);
        file.write_at(buf, start_offset).await?;

        let handle = FileBlockHandle {
            id: FileId {
                queue: self.queue,
                seq,
            },
            offset: start_offset,
            len: padded_len,
        };
        for listener in &self.listeners {
            listener.on_append_log_file(handle);
        }
        Ok(handle)
    }

    fn maybe_sync(&self, force: bool) -> Result<()> {
        let mut active_file = block_on(self.active_file.lock());
        let seq = active_file.seq;
        let writer = &mut active_file.writer;
        if writer.offset() >= self.target_file_size {
            if let Err(e) = self.rotate_imp(&mut active_file) {
                panic!("error when rotate [{:?}:{}]: {}", self.queue, seq, e);
            }
        } else if writer.since_last_sync() >= self.bytes_per_sync || force {
            if let Err(e) = writer.sync() {
                panic!("error when sync [{:?}:{}]: {}", self.queue, seq, e,);
            }
        }

        Ok(())
    }

    pub async fn maybe_rotate_async(&self) -> Result<()> {
        let mut active_file = self.active_file.lock().await;
        let seq = active_file.seq;
        let writer = &mut active_file.writer;
        if writer.offset() >= self.target_file_size {
            if let Err(e) = self.rotate_imp(&mut active_file) {
                panic!("error when rotate [{:?}:{}]: {}", self.queue, seq, e);
            }
        }

        Ok(())
    }

    fn file_span(&self) -> (FileSeq, FileSeq) {
        let files = self.files.read();
        (files.first_seq, files.active_seq)
    }

    fn total_size(&self) -> usize {
        let files = self.files.read();
        (files.active_seq - files.first_seq + 1) as usize * self.target_file_size
    }

    fn rotate(&self) -> Result<()> {
        self.rotate_imp(&mut *block_on(self.active_file.lock()))
    }

    fn purge_to(&self, file_seq: FileSeq) -> Result<usize> {
        let (purged, remained) = {
            let mut files = self.files.write();
            if file_seq > files.active_seq {
                return Err(box_err!("Purge active or newer files"));
            }
            let end_offset = file_seq.saturating_sub(files.first_seq) as usize;
            files.fds.drain(..end_offset);
            files.first_seq = file_seq;
            (end_offset, files.fds.len())
        };
        self.flush_metrics(remained);
        for seq in file_seq - purged as u64..file_seq {
            let file_id = FileId {
                queue: self.queue,
                seq,
            };
            let path = file_id.build_file_path(&self.dir);
            #[cfg(feature = "failpoints")]
            {
                let remove_failure = || {
                    fail::fail_point!("file_pipe_log::remove_file_failure", |_| true);
                    false
                };
                if remove_failure() {
                    continue;
                }
            }
            if let Err(e) = fs::remove_file(&path) {
                warn!("Remove purged log file {:?} failed: {}", path, e);
            }
        }
        Ok(purged)
    }
}

pub struct DualPipes<B: FileBuilder> {
    pub appender: AsyncPipe,
    pub rewriter: SinglePipe<B>,

    _lock_file: File,
}

impl<B: FileBuilder> DualPipes<B> {
    pub fn open(dir: &str, appender: AsyncPipe, rewriter: SinglePipe<B>) -> Result<Self> {
        let lock_file = File::create(lock_file_path(dir))?;
        lock_file.try_lock_exclusive().map_err(|e| {
            Error::Other(box_err!(
                "Failed to lock file: {}, maybe another instance is using this directory.",
                e
            ))
        })?;

        // TODO: remove this dependency.
        debug_assert_eq!(LogQueue::Append as usize, 0);
        debug_assert_eq!(LogQueue::Rewrite as usize, 1);
        Ok(Self {
            appender,
            rewriter,
            _lock_file: lock_file,
        })
    }
}

impl<B: FileBuilder> PipeLog for DualPipes<B> {
    #[inline]
    fn read_bytes(&self, handle: FileBlockHandle) -> Result<Vec<u8>> {
        match handle.id.queue {
            LogQueue::Append => self.appender.read_bytes(handle),
            LogQueue::Rewrite => self.rewriter.read_bytes(handle),
        }
    }

    #[inline]
    fn append(&self, queue: LogQueue, bytes: &[u8]) -> Result<FileBlockHandle> {
        match queue {
            LogQueue::Append => todo!(),
            LogQueue::Rewrite => self.rewriter.append(bytes),
        }
    }

    #[inline]
    fn maybe_sync(&self, queue: LogQueue, force: bool) -> Result<()> {
        match queue {
            LogQueue::Append => todo!(),
            LogQueue::Rewrite => self.rewriter.maybe_sync(force),
        }
    }

    #[inline]
    fn file_span(&self, queue: LogQueue) -> (FileSeq, FileSeq) {
        match queue {
            LogQueue::Append => self.appender.file_span(),
            LogQueue::Rewrite => self.rewriter.file_span(),
        }
    }

    #[inline]
    fn total_size(&self, queue: LogQueue) -> usize {
        match queue {
            LogQueue::Append => self.appender.total_size(),
            LogQueue::Rewrite => self.rewriter.total_size(),
        }
    }

    #[inline]
    fn rotate(&self, queue: LogQueue) -> Result<()> {
        match queue {
            LogQueue::Append => todo!(),
            LogQueue::Rewrite => self.rewriter.rotate(),
        }
    }

    #[inline]
    fn purge_to(&self, file_id: FileId) -> Result<usize> {
        match file_id.queue {
            LogQueue::Append => self.appender.purge_to(file_id.seq),
            LogQueue::Rewrite => self.rewriter.purge_to(file_id.seq),
        }
    }
}
