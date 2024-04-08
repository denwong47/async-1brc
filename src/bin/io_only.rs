//! This is just to test the reader on its own to see how long it takes to read the whole file.
//!
//! This forms a baseline performance for our reader for this device.

#[cfg(feature = "bench")]
use tokio::time::Instant;

use clap::Parser;

use async_1brc::{reader, CliArgs};

/// The number of trials to run the benchmark.
const TRIALS: usize = 8;

#[tokio::main]
async fn main() {
    let args = CliArgs::parse();

    println!(
        "Parameters:\n\
        - File: {}\n\
        - Chunk size: {}\n\
        - Max chunk size: {}\n",
        args.file, args.chunk_size, args.max_chunk_size
    );

    let mut trials = Vec::with_capacity(TRIALS);

    for trial in 0..TRIALS {
        #[cfg(feature = "debug")]
        println!("Starting the reader coroutine.");

        #[cfg(feature = "bench")]
        let start = Instant::now();

        let reader = reader::RowsReader::with_chunk_sizes(args.chunk_size, args.max_chunk_size);

        let file = tokio::fs::File::open(&args.file).await.unwrap();
        let bufreader = tokio::io::BufReader::with_capacity(args.chunk_size, file);

        let mut count = 0;
        tokio::select! {
            _ = reader.read(bufreader) => {},
            _ = async {
                let mut buffer = Vec::with_capacity(args.max_chunk_size);
                while let Some(bytes) = reader.fill(buffer).await {
                    buffer = bytes;
                    count += 1;
                }
            } => {}
        };

        let elapsed = start.elapsed();

        #[cfg(feature = "bench")]
        {
            println!("Trial #{} completed.", trial + 1);
            println!("Elapsed time: {:?}", &elapsed);
            println!("Total chunks read: {}\n", count);
        }

        trials.push(elapsed);
    }

    let mean = trials.iter().sum::<std::time::Duration>() / TRIALS as u32;
    let max = trials.iter().max().unwrap();
    let min = trials.iter().min().unwrap();

    println!("Benchmark results over a total of {} runs:", TRIALS);
    println!("- Mean elapsed time: {:?}", mean);
    println!("- Max elapsed time: {:?}", max);
    println!("- Min elapsed time: {:?}", min);
}
