//! Task to create a number of threads to read from the same [`RowsReader`].

use super::super::reader::RowsReader;
use super::models::StationRecords;
use std::sync::Arc;


/// Create X number of concurrent consumers to read from the same [`RowsReader`].
pub async fn read_from_reader(reader: Arc<RowsReader>, threads: usize) -> StationRecords {
    // If there is only one thread, we can just read from the reader directly.
    if threads <= 1 {
        // Somehow changing this to just awaiting the inner function call makes the code slower??
        // This may be because tokio will spawn a new thread for the inner function call, leaving
        // the main thread to continue with the rest of the code.
        return tokio::spawn(async move { StationRecords::read_from_reader(&reader).await })
            .await
            .unwrap();
    }

    let mut handles = Vec::with_capacity(threads);

    for _i in 0..threads {
        let local_reader = Arc::clone(&reader);
        handles.push(tokio::spawn(async move {
            #[cfg(feature = "debug")]
            println!("task::read_from_reader() spawned consumer #{}", _i);

            StationRecords::read_from_reader(&local_reader).await
        }));
    }

    let mut records = StationRecords::new();
    for (_i, handle) in handles.into_iter().enumerate() {
        records += handle.await.unwrap();

        #[cfg(feature = "debug")]
        println!("task::read_from_reader() consumer #{} finished.", _i);
    }

    records
}
