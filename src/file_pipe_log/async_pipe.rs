use std::{
    collections::{BTreeMap, VecDeque},
    fs,
    mem::{self, MaybeUninit},
    ops::Deref,
    rc::Rc,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread,
};

use crossbeam::utils::CachePadded;
use futures_lite::Future;
use glommio::io::{DmaFile, OpenOptions};
use glommio::sync::RwLock as GRwLock;
use libc::*;
use log::{debug, warn};
use parking_lot::{Mutex, RwLock};

use crate::{
    file_builder::DefaultFileBuilder, Config, Error, FileBlockHandle, FileId, FileSeq, LogQueue,
    Result,
};

use super::{
    format::LogFileHeader,
    log_file::{build_file_reader, LogFd},
    FileNameExt,
};

thread_local! {
    static DMA_FILE: Rc<GRwLock<Option<(FileSeq, RcFile)>>> = Rc::new(GRwLock::new(None));
}

const PAGE_SIZE: u64 = 4096;
const FILE_PAGE_COUNT: u64 = 32798;

lazy_static! {
    pub static ref WRITING_SEQS: CachePadded<Mutex<BTreeMap<FileSeq, usize>>> = Default::default();
}

pub struct AsyncPipe {
    dir: String,
    files: Arc<CachePadded<RwLock<FileCollection>>>,
    seq_page: CachePadded<AtomicU64>,
}

impl AsyncPipe {
    pub fn open(
        cfg: &Config,
        mut first_seq: FileSeq,
        mut fds: VecDeque<Arc<LogFd>>,
    ) -> Result<Self> {
        let create_file = first_seq == 0;
        let active_seq = if create_file {
            first_seq = 1;
            let file_id = FileId {
                queue: LogQueue::Append,
                seq: first_seq,
            };
            let fd = Arc::new(LogFd::create(&file_id.build_file_path(&cfg.dir))?);
            fds.push_back(fd);
            first_seq
        } else {
            first_seq + fds.len() as u64 - 1
        };
        let active_file_size = fds.back().unwrap().file_size()? as u64;
        let mut active_page = active_file_size / PAGE_SIZE;
        // create header if necessary
        let mut header = Vec::with_capacity(LogFileHeader::len());
        LogFileHeader::default().encode(&mut header)?;
        if active_page == 0 {
            fds.back().unwrap().write(0, &header)?;
            active_page = 1;
        }
        let seq_page = CachePadded::new(AtomicU64::new(active_seq << 16 | active_page));

        // create one more reserved file
        {
            let reserved_file_id = FileId {
                queue: LogQueue::Append,
                seq: active_seq + 1,
            };
            let reserved = LogFd::create(&reserved_file_id.build_file_path(&cfg.dir))?;
            reserved.write(0, &header)?;
        }

        let pipe = Self {
            dir: cfg.dir.clone(),
            files: Arc::new(CachePadded::new(RwLock::new(FileCollection {
                first_seq,
                fds,
            }))),
            seq_page,
        };
        Ok(pipe)
    }

    async fn open_file(&self, seq: u64) -> Result<RcFile> {
        let file_id = FileId {
            queue: LogQueue::Append,
            seq,
        };
        let path = file_id.build_file_path(&self.dir);
        Ok(RcFile::new(
            seq,
            OpenOptions::new()
                .write(true)
                .create(true)
                // .custom_flags(O_DSYNC)
                .dma_open(&path)
                .await?,
        ))
    }

    fn create_file(&self, seq: u64) -> impl Future<Output = Result<RcFile>> {
        let file_id = FileId {
            queue: LogQueue::Append,
            seq,
        };
        let path = file_id.build_file_path(&self.dir);
        let files = self.files.clone();
        async move {
            let file = RcFile::new(
                seq,
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .custom_flags(O_DSYNC)
                    .dma_open(&path)
                    .await?,
            );
            // file.pre_allocate(PAGE_SIZE * FILE_PAGE_COUNT).await?;
            let mut buf = file.alloc_dma_buffer(4096);
            // FIXME(sticnarf): optimize
            let mut header = Vec::with_capacity(LogFileHeader::len());
            LogFileHeader::default().encode(&mut header)?;
            buf.as_bytes_mut().copy_from_slice(&header);
            file.write_at(buf, 0).await?;

            let fd = Arc::new(LogFd::open(&path)?);
            files.write().fds.push_back(fd);

            Ok(file)
        }
    }

    /// Returns (seq, offset)
    fn allocate(&self, len: usize) -> Result<(u64, u64)> {
        let inc = (len as u64 + PAGE_SIZE - 1) / PAGE_SIZE;
        let mut curr = self.seq_page.load(Ordering::Relaxed);
        loop {
            let (seq, page) = (curr >> 16, curr & 0xffff);
            let (res_seq, res_page) = if page + inc > FILE_PAGE_COUNT {
                (seq + 1, 1) // 1 reserved for file header
            } else {
                (seq, page)
            };
            let new_seq_page = res_seq << 16 | (res_page + inc);
            match self.seq_page.compare_exchange_weak(
                curr,
                new_seq_page,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    if res_seq > seq {
                        let fut = self.create_file(res_seq + 1);
                        glommio::spawn_local(fut).detach();
                    }
                    return Ok((res_seq, res_page * PAGE_SIZE));
                }
                Err(new_curr) => curr = new_curr,
            }
        }
    }

    pub async fn append_async(&self, bytes: &[u8]) -> Result<FileBlockHandle> {
        assert!(bytes.len() & 4095 == 0);

        let curr_file = DMA_FILE.with(|cell| cell.clone());
        let mut guard = curr_file.write().await.unwrap();
        let (seq, offset) = self.allocate(bytes.len())?;
        let file = match &*guard {
            Some((curr_seq, file)) if seq == *curr_seq => file.clone(),
            Some(_) => {
                let file = self.open_file(seq).await?;
                *guard = Some((seq, file.clone()));
                debug!("thread {:?} open file {}", thread::current().id(), seq);
                file
            }
            None => {
                let file = self.open_file(seq).await?;
                *guard = Some((seq, file.clone()));
                debug!("thread {:?} open file {}", thread::current().id(), seq);
                file
            }
        };
        drop(guard);

        let mut buf = file.alloc_dma_buffer(bytes.len());
        buf.as_bytes_mut()[..bytes.len()].copy_from_slice(bytes);
        file.write_at(buf, offset).await?;
        file.fdatasync().await?;

        let handle = FileBlockHandle {
            id: FileId {
                queue: LogQueue::Append,
                seq,
            },
            offset,
            len: bytes.len(),
        };
        Ok(handle)
    }

    fn active_seq(&self) -> u64 {
        self.seq_page.load(Ordering::Acquire) >> 16
    }

    pub fn file_span(&self) -> (FileSeq, FileSeq) {
        let files = self.files.read();
        (files.first_seq, self.active_seq())
    }

    pub fn total_size(&self) -> usize {
        let (first_seq, active_seq) = self.file_span();
        ((active_seq - first_seq + 1) * PAGE_SIZE * FILE_PAGE_COUNT) as usize
    }

    pub fn purge_to(&self, file_seq: FileSeq) -> Result<usize> {
        let (purged, _remained) = {
            let mut files = self.files.write();
            if file_seq > self.active_seq() {
                return Err(box_err!("Purge active or newer files"));
            }
            let end_offset = file_seq.saturating_sub(files.first_seq) as usize;
            files.fds.drain(..end_offset);
            files.first_seq = file_seq;
            (end_offset, files.fds.len())
        };
        for seq in file_seq - purged as u64..file_seq {
            let file_id = FileId {
                queue: LogQueue::Append,
                seq,
            };
            let path = file_id.build_file_path(&self.dir);
            if let Err(e) = fs::remove_file(&path) {
                warn!("Remove purged log file {:?} failed: {}", path, e);
            }
        }
        Ok(purged)
    }

    fn get_fd(&self, file_seq: FileSeq) -> Result<Arc<LogFd>> {
        let files = self.files.read();
        if file_seq < files.first_seq || file_seq > self.active_seq() {
            return Err(Error::Corruption("file seqno out of range".to_owned()));
        }
        Ok(files.fds[(file_seq - files.first_seq) as usize].clone())
    }

    pub fn read_bytes(&self, handle: FileBlockHandle) -> Result<Vec<u8>> {
        let fd = self.get_fd(handle.id.seq)?;
        let mut reader = build_file_reader(
            &DefaultFileBuilder,
            &handle.id.build_file_path(&self.dir),
            fd,
        )?;
        reader.read(handle)
    }
}

struct FileCollection {
    first_seq: FileSeq,
    fds: VecDeque<Arc<LogFd>>,
}

struct RcFile(FileSeq, MaybeUninit<Rc<DmaFile>>);

impl RcFile {
    fn new(seq: FileSeq, file: DmaFile) -> RcFile {
        *WRITING_SEQS.lock().entry(seq).or_insert(0) += 1;
        RcFile(seq, MaybeUninit::new(Rc::new(file)))
    }
}

impl Clone for RcFile {
    fn clone(&self) -> RcFile {
        unsafe { RcFile(self.0, MaybeUninit::new(self.1.assume_init_ref().clone())) }
    }
}

impl Deref for RcFile {
    type Target = DmaFile;

    fn deref(&self) -> &DmaFile {
        unsafe { self.1.assume_init_ref() }
    }
}

impl Drop for RcFile {
    fn drop(&mut self) {
        let file = unsafe { mem::replace(&mut self.1, MaybeUninit::uninit()).assume_init() };
        if let Ok(file) = Rc::try_unwrap(file) {
            let seq = self.0;
            glommio::spawn_local(async move {
                let _ = file.close().await;
                {
                    let mut seqs = WRITING_SEQS.lock();
                    let v = seqs.get_mut(&seq).unwrap();
                    debug!(
                        "thread {:?} remove file seq {}",
                        thread::current().id(),
                        seq
                    );
                    if *v == 1 {
                        debug!("remove file seq {}", seq);
                        seqs.remove(&seq);
                    } else {
                        *v -= 1;
                    }
                }
            })
            .detach();
        }
    }
}