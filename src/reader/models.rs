//! The reader model.

use deadqueue::unlimited::Queue;
use std::sync::atomic::{AtomicBool, Ordering};
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
    output_queue: Queue<Vec<u8>>,
    input_queue: Queue<Vec<u8>>,
    chunk_size: usize,
    max_chunk_size: usize,
    in_progress: AtomicBool,
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
            output_queue: Queue::new(),
            input_queue: Queue::new(),
            chunk_size: config::CHUNK_SIZE,
            max_chunk_size: config::MAX_CHUNK_SIZE,
            in_progress: AtomicBool::new(false),
            closed,
        }
    }

    /// Create a new instance with custom chunk sizes.
    pub fn with_chunk_sizes(chunk_size: usize, max_chunk_size: usize) -> Self {
        let (closed, _) = watch::channel(false);

        Self {
            output_queue: Queue::new(),
            input_queue: Queue::new(),
            chunk_size: usize::max(config::MAX_LINE_LENGTH, chunk_size),
            max_chunk_size,
            in_progress: AtomicBool::new(false),
            closed,
        }
    }

    /// Check if the reader is in progress.
    pub fn in_progress(&self) -> bool {
        self.in_progress.load(Ordering::Relaxed)
    }

    /// Return when the reader will no longer yield any more data.
    pub async fn closed(&self) -> Result<(), tokio::sync::watch::error::RecvError> {
        let mut rx = self.closed.subscribe();

        rx.wait_for(|v| *v).await?;

        loop {
            if self.output_queue.is_empty() {
                break;
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        Ok(())
    }

    /// Pop the next buffer from the queue.
    pub async fn fill(&self, mut buffer: Vec<u8>) -> Option<Vec<u8>> {
        #[cfg(feature = "timed")]
        let _counter = READER_LOCK_TIMED
            .get_or_init(|| TimedOperation::new("RowsReader::pop()"))
            .start();

        buffer.clear();
        self.input_queue.push(buffer);

        let result = tokio::select! {
            _ = self.closed() => None,
            bytes = self.output_queue.pop() => {
                Some(bytes)
            }
        };

        result
    }

    /// Push buffer to the queue and reset the buffer.
    pub async fn export_buffer(&self, buffer_export: &mut Vec<u8>) -> usize {
        if !buffer_export.is_empty() {
            #[cfg(feature = "debug")]
            println!("RowsReader: export_buffer() waiting for available buffer from input_queue.");

            let mut buffer_new = self.input_queue.pop().await;

            #[cfg(feature = "debug")]
            println!(
                "RowsReader: export_buffer() has got a buffer of capacity {}.",
                buffer_new.capacity()
            );

            {
                #[cfg(feature = "timed")]
                let _counter = func::MEM_SWAP_TIMED
                    .get_or_init(|| TimedOperation::new("mem_swap"))
                    .start();
                std::mem::swap(&mut buffer_new, buffer_export);
            }

            let len = buffer_new.len();
            self.output_queue.push(buffer_new);
            len
        } else {
            #[cfg(feature = "debug")]
            println!("RowsReader: push_buffer() skipped empty buffer.");
            0
        }
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
                || !self.input_queue.is_empty()
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
                let _bytes_pushed = self.export_buffer(&mut buffer_export).await;

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
