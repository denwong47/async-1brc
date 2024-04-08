//! A simple implementation using [`memmap::Mmap`] as well as [`rayon::iter::ParallelIterator`]
//! to read the file and parse the records in parallel.
//!
//! The file is sliced into given number of chunks, equal to the number of threads, then
//! each chunk creates a [`StationRecords`] instance to parse the records, before reducing
//! down to a single instance.
//!
//! This implementation serves as a baseline for the performance comparison with the async
//! implementation. This is expected to be faster, but less efficient in terms of memory
//! usage, and have limited scalability. This also does not support a streaming input
//! as the async implementation does.
use clap::Parser;
use std::time::Instant;

use async_1brc::{parser::models::StationRecords, reader::sync::*, CliArgs};

#[cfg(feature = "assert")]
use async_1brc::assertion;

fn main() {
    let args = CliArgs::parse();

    println!(
        "Parameters:\n\
        - File: {}",
        args.file
    );

    #[cfg(feature = "bench")]
    let start = Instant::now();

    let reader = MmapReader::from_path(&args.file).with_chunks(args.threads);

    let records = StationRecords::read_from_iterator(reader.iter::<b'\n'>());

    records.export_file_blocking(&args.output);

    #[cfg(feature = "bench")]
    println!("elapsed time: {:?}", start.elapsed());

    #[cfg(feature = "assert")]
    '_assertion: {
        if cfg!(any(
            feature = "noparse",
            feature = "noparse-name",
            feature = "noparse-value"
        )) {
            println!("Cannot perform assertions when parsing is partially/fully disabled. Assertion aborted.");
            return;
        }

        println!("Checking the number of records...");
        let output_len = records.len();
        println!("The number of records: {}", output_len);
        assert_eq!(output_len, 1_000_000_000);

        println!("Matching the output and the baseline files...");
        assertion::match_files_blocking(&args.output, &args.baseline);

        println!("All assertions passed.")
    }
}
