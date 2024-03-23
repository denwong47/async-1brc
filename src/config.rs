//! Configuration for the reader.

pub const MAX_LINE_LENGTH: usize = 30;

pub const CHUNK_SIZE: usize = 65536 * 8; // Max buffer capacity 2097152 - higher does not change anything.

pub const MAX_CHUNK_SIZE: usize = CHUNK_SIZE * 16 + MAX_LINE_LENGTH;

pub const NUMBER_OF_THREADS: usize = 8;

pub const MEASURMENTS_PATH: &str = "/Volumes/RAMDisk/measurements.txt";

pub const OUTPUT_PATH: &str = "data/output.txt";

#[cfg(feature = "assert")]
pub const BASELINE_PATH: &str = "../1brc/out_expected.txt";
