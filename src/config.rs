//! Configuration for the reader.

pub const MAX_LINE_LENGTH: usize = 30;

pub const CHUNK_SIZE: usize = 8192; // Max buffer capacity 2097152 - higher does not change anything.

pub const MAX_CHUNK_SIZE: usize = CHUNK_SIZE * 256 + MAX_LINE_LENGTH;

pub const NUMBER_OF_THREADS: usize = 8;