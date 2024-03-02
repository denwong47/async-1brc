//! Helper functions for the reader.

use super::super::config;

#[cfg(feature = "timed")]
use super::super::timed::TimedOperation;

#[cfg(feature = "timed")]
pub static CLONE_BUFFER_TIMED: std::sync::OnceLock<std::sync::Arc<TimedOperation>> =
    std::sync::OnceLock::new();

#[cfg(feature = "timed")]
pub static MEM_SWAP_TIMED: std::sync::OnceLock<std::sync::Arc<TimedOperation>> =
    std::sync::OnceLock::new();

/// Transfer the buffer from the read buffer to the export buffer.
///
/// This will leave the read buffer empty.
pub fn transfer_buffer(buffer_read: &mut Vec<u8>, buffer_export: &mut Vec<u8>) {
    buffer_export.append(buffer_read);
}

/// Shift the buffer from the read buffer to the export buffer.
pub fn clone_buffer(buffer_read: &mut [u8], buffer_export: &mut Vec<u8>) {
    #[cfg(feature = "timed")]
    let _counter = CLONE_BUFFER_TIMED
        .get_or_init(|| TimedOperation::new("clone_buffer"))
        .start();

    buffer_export.extend_from_slice(buffer_read);
}

/// Push buffer to the queue and reset the buffer.
pub fn push_buffer(
    buffer_export: &mut Vec<u8>,
    queue: &deadqueue::unlimited::Queue<Vec<u8>>,
) -> usize {
    if !buffer_export.is_empty() {
        let mut buffer_new = Vec::<u8>::with_capacity(buffer_export.capacity());

        {
            #[cfg(feature = "timed")]
            let _counter = MEM_SWAP_TIMED
                .get_or_init(|| TimedOperation::new("mem_swap"))
                .start();
            std::mem::swap(&mut buffer_new, buffer_export);
        }

        let len = buffer_new.len();
        queue.push(buffer_new);
        len
    } else {
        #[cfg(feature = "debug")]
        println!("RowsReader: push_buffer() skipped empty buffer.");
        0
    }
}

/// Check if the buffer is full.
pub fn buffer_full(buffer_export: &Vec<u8>, chunk_size: usize) -> bool {
    let _result =
        buffer_export.len() >= buffer_export.capacity() - chunk_size - config::MAX_LINE_LENGTH;

    #[cfg(feature = "debug")]
    if _result {
        println!("RowsReader: buffer_full() buffer full: {}", _result);
    }

    _result
}
