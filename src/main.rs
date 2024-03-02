mod config;
mod parser;
mod reader;
use std::sync::Arc;

#[cfg(feature = "bench")]
use tokio::time::Instant;

#[cfg(feature = "assert")]
mod assertion;

#[tokio::main]
async fn main() {
    #[cfg(feature = "debug")]
    println!("Starting the reader coroutine.");

    #[cfg(feature = "bench")]
    let start = Instant::now();

    let reader = Arc::new(reader::RowsReader::with_chunk_sizes(
        config::CHUNK_SIZE,
        config::MAX_CHUNK_SIZE,
    ));

    let (_, records) = tokio::join!(
        reader.read(config::MEASURMENTS_PATH),
        parser::task::read_from_reader(Arc::clone(&reader), config::NUMBER_OF_THREADS),
    );

    records.export_file(config::OUTPUT_PATH).await;

    #[cfg(feature = "bench")]
    println!("Elapsed time: {:?}", start.elapsed());

    #[cfg(feature = "assert")]
    '_assertion: {
        println!("Checking the number of records...");
        let output_len = records.len();
        println!("The number of records: {}", output_len);
        assert_eq!(output_len, 1_000_000_000);

        println!("Matching the output and the baseline files...");
        assertion::match_files(config::OUTPUT_PATH, config::BASELINE_PATH).await;

        println!("All assertions passed.")
    }
}
