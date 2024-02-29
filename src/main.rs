mod config;
mod parser;
mod reader;
use std::sync::Arc;

#[cfg(feature="bench")]
use tokio::time::Instant;

#[tokio::main]
async fn main() {
    #[cfg(feature="debug")]
    println!("Starting the reader coroutine.");

    #[cfg(feature="bench")]
    let start = Instant::now();

    let reader = Arc::new(reader::RowsReader::with_chunk_sizes(config::CHUNK_SIZE, config::MAX_CHUNK_SIZE));

    let (_, records) = tokio::join!(
        reader.read("../1brc/measurements.txt"),
        parser::task::read_from_reader(Arc::clone(&reader), config::NUMBER_OF_THREADS),
    );
    
    records.to_file("data/output.txt").await;

    #[cfg(feature="bench")]
    println!("Elapsed time: {:?}", start.elapsed());
}
