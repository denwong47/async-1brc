use clap::Parser;

use tokio::time::Instant;

use async_1brc::CliArgs;

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

    let file = std::fs::File::open(&args.file).unwrap();
    let buffer = unsafe { memmap::MmapOptions::new().map(&file).unwrap() };

    let start = Instant::now();
    let count = buffer.split(|&c| c == b'\n').count();
    println!("Elapsed time for {} lines: {:?}", count, start.elapsed());
}
