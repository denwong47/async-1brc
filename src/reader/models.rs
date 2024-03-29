//! The reader model.

use deadqueue::unlimited::Queue;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt},
    sync::watch,
};

use super::super::config;
use super::func;

#[cfg(feature = "timed")]
use super::super::timed::TimedOperation;

#[cfg(feature = "timed")]
pub static READER_LOCK_TIMED: std::sync::OnceLock<std::sync::Arc<TimedOperation>> =
    std::sync::OnceLock::new();

#[cfg(feature = "timed")]
pub static READER_READ_TIMED: std::sync::OnceLock<std::sync::Arc<TimedOperation>> =
    std::sync::OnceLock::new();

#[cfg(feature = "timed")]
pub static READER_LINE_TIMED: std::sync::OnceLock<std::sync::Arc<TimedOperation>> =
    std::sync::OnceLock::new();

pub struct RowsReader {
    queue: Queue<Vec<u8>>,
    chunk_size: usize,
    max_chunk_size: usize,
    in_progress: AtomicBool,
    in_queue: AtomicUsize,
    closed: watch::Sender<bool>,
}

#[allow(dead_code)]
impl Default for RowsReader {
    fn default() -> Self {
        Self::new()
    }
}

impl RowsReader {
    pub fn new() -> Self {
        let (closed, _) = watch::channel(false);

        Self {
            queue: Queue::new(),
            chunk_size: config::CHUNK_SIZE,
            max_chunk_size: config::MAX_CHUNK_SIZE,
            in_progress: AtomicBool::new(false),
            in_queue: AtomicUsize::new(0),
            closed,
        }
    }

    /// Create a new instance with custom chunk sizes.
    pub fn with_chunk_sizes(chunk_size: usize, max_chunk_size: usize) -> Self {
        let (closed, _) = watch::channel(false);

        Self {
            queue: Queue::new(),
            chunk_size: usize::max(config::MAX_LINE_LENGTH, chunk_size),
            max_chunk_size,
            in_progress: AtomicBool::new(false),
            in_queue: AtomicUsize::new(0),
            closed,
        }
    }

    /// Check if the reader is in progress.
    pub fn in_progress(&self) -> bool {
        self.in_progress.load(Ordering::Relaxed)
    }

    /// Increment the in_queue counter.
    fn in_queue_increment(&self) -> usize {
        #[cfg(feature = "debug")]
        println!("RowsReader: in_queue_increment() incremented in_queue.");

        self.in_queue.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Decrement the in_queue counter.
    fn in_queue_decrement(&self) -> usize {
        #[cfg(feature = "debug")]
        println!("RowsReader: in_queue_decrement() decremented in_queue.");

        self.in_queue.fetch_sub(1, Ordering::Relaxed) - 1
    }

    /// Return when the reader will no longer yield any more data.
    pub async fn closed(&self) -> Result<(), tokio::sync::watch::error::RecvError> {
        let mut rx = self.closed.subscribe();

        rx.wait_for(|v| *v).await?;

        loop {
            if self.queue.is_empty() {
                break;
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        Ok(())
    }

    /// Pop the next buffer from the queue.
    pub async fn pop(&self) -> Option<Vec<u8>> {
        #[cfg(feature = "timed")]
        let _counter = READER_LOCK_TIMED
            .get_or_init(|| TimedOperation::new("RowsReader::pop()"))
            .start();

        self.in_queue_increment();

        let result = tokio::select! {
            _ = self.closed() => None,
            bytes = self.queue.pop() => {
                Some(bytes)
            }
        };

        self.in_queue_decrement();
        result
    }

    /// Read the file and push the chunks to the queue.
    pub async fn read(&self, mut buffer: impl AsyncReadExt + AsyncBufRead + std::marker::Unpin) {
        if self
            .in_progress
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            panic!(
                "RowsReader is already in progress! Do not call read() twice on the same instance."
            )
        }

        let mut buffer_read = vec![0; self.chunk_size];
        let mut buffer_export = Vec::<u8>::with_capacity(self.max_chunk_size);

        let mut buffer_line = Vec::<u8>::with_capacity(config::MAX_LINE_LENGTH);

        loop {
            let bytes_read = {
                #[cfg(feature = "timed")]
                let _counter = READER_READ_TIMED
                    .get_or_init(|| TimedOperation::new("RowsReader::read()[fixed length]"))
                    .start();

                buffer.read(&mut buffer_read).await.unwrap()
            };

            #[cfg(feature = "debug")]
            println!("RowsReader: read() read {bytes_read} bytes.");

            func::clone_buffer(&mut buffer_read[..bytes_read], &mut buffer_export);

            if bytes_read == 0 // if nothing is read
                || func::buffer_full(&buffer_export, self.chunk_size) // if the buffer is full
                || self.in_queue.load(Ordering::Relaxed) > 0
            // if something is waiting
            {
                // Read until the end of line anyway
                let bytes_read = {
                    #[cfg(feature = "timed")]
                    let _counter = READER_LINE_TIMED
                        .get_or_init(|| TimedOperation::new("RowsReader::read()[line]"))
                        .start();

                    buffer.read_until(b'\n', &mut buffer_line).await.unwrap()
                };

                #[cfg(feature = "debug")]
                println!("RowsReader: read() read {bytes_read} bytes up to a new line.");

                func::transfer_buffer(&mut buffer_line, &mut buffer_export);
                let _bytes_pushed = func::push_buffer(&mut buffer_export, &self.queue);

                #[cfg(feature = "debug")]
                println!("RowsReader: read() flushed {_bytes_pushed} bytes to queue.");

                if bytes_read == 0 {
                    #[cfg(feature = "debug")]
                    println!("RowsReader: read() finished.");

                    self.closed.send_replace(true);

                    break;
                }
            }
        }
    }
}
