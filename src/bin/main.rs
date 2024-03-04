use std::sync::Arc;
use clap::Parser;

#[cfg(feature = "bench")]
use tokio::time::Instant;

#[cfg(feature = "assert")]
use async_1brc::assertion;

use async_1brc::{CliArgs, parser, reader};

#[tokio::main]
async fn main() {
    let args = CliArgs::parse();

    println!(
        "Parameters:\n\
        - File: {}\n\
        - Output: {}\n\
        - Threads: {}\n\
        - Chunk size: {}\n\
        - Max chunk size: {}\n",
        args.file, args.output, args.threads, args.chunk_size, args.max_chunk_size
    );

    #[cfg(feature = "debug")]
    println!("Starting the reader coroutine.");

    #[cfg(feature = "bench")]
    let start = Instant::now();

    let reader = Arc::new(reader::RowsReader::with_chunk_sizes(
        args.chunk_size,
        args.max_chunk_size,
    ));

    let (_, records) = tokio::join!(
        async {
            let file = tokio::fs::File::open(&args.file).await.unwrap();
            let buffer = tokio::io::BufReader::with_capacity(args.chunk_size, file);

            reader.read(buffer).await
        },
        parser::task::read_from_reader(Arc::clone(&reader), args.threads),
    );

    records.export_file(&args.output).await;

    #[cfg(feature = "bench")]
    println!("Elapsed time: {:?}", start.elapsed());

    #[cfg(feature = "timed")]
    '_timed: {
        println!("Reporting the total time spent in the operations...");
        if let Some(ops) = reader::READER_READ_TIMED.get() {
            ops.report()
        }
        if let Some(ops) = reader::READER_LINE_TIMED.get() {
            ops.report()
        }
        if let Some(ops) = reader::READER_LOCK_TIMED.get() {
            ops.report()
        }
        if let Some(ops) = reader::func::CLONE_BUFFER_TIMED.get() {
            ops.report()
        }
        if let Some(ops) = reader::func::MEM_SWAP_TIMED.get() {
            ops.report()
        }
        #[cfg(feature = "timed-extreme")]
        {
            parser::line::PARSE_NAME_TIMED.get().map(|ops| ops.report());
            parser::line::PARSE_VALUE_TIMED
                .get()
                .map(|ops| ops.report());
            parser::models::HASH_INSERT_TIMED
                .get()
                .map(|ops| ops.report());
        }
    }

    #[cfg(feature = "assert")]
    '_assertion: {
        println!("Checking the number of records...");
        let output_len = records.len();
        println!("The number of records: {}", output_len);
        assert_eq!(output_len, 1_000_000_000);

        println!("Matching the output and the baseline files...");
        assertion::match_files(&args.output, &args.baseline).await;

        println!("All assertions passed.")
    }
}
