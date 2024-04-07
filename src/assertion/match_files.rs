//! Match the output and the baseline files.

use std::path::Path;
use tokio::{
    fs::File,
    io::{AsyncReadExt, BufReader},
};

#[cfg(feature = "sync")]
use memmap::Mmap;

/// The size of the chunk to match the files.
const MATCH_CHUNK_SIZE: usize = 32;

/// Match the output and the baseline files.
pub async fn match_files(output_path: impl AsRef<Path>, baseline_path: impl AsRef<Path>) {
    let output_file = File::open(output_path).await.unwrap();
    let baseline_file = File::open(baseline_path).await.unwrap();

    let mut output_reader = BufReader::new(output_file);
    let mut baseline_reader = BufReader::new(baseline_file);

    let mut output_buffer = vec![0; MATCH_CHUNK_SIZE];
    let mut baseline_buffer = vec![0; MATCH_CHUNK_SIZE];

    loop {
        let (output_bytes, baseline_bytes) = tokio::join!(
            output_reader.read(&mut output_buffer),
            baseline_reader.read(&mut baseline_buffer)
        );

        match (output_bytes, baseline_bytes) {
            (Ok(0), Ok(0)) => {
                break;
            }
            (Ok(i), Ok(j)) if i == j => {
                if output_buffer[..i] != baseline_buffer[..j] {
                    panic!(
                        "The files differ at the following position:\noutput:{}\nbaseline:{}",
                        String::from_utf8_lossy(&output_buffer[..i]),
                        String::from_utf8_lossy(&baseline_buffer[..j])
                    )
                }
            }
            (Ok(i), Ok(j)) => {
                panic!(
                    "The files have different sizes: {} and {};\noutput:{}\nbaseline:{}",
                    i,
                    j,
                    String::from_utf8_lossy(&output_buffer[..i]),
                    String::from_utf8_lossy(&baseline_buffer[..j])
                );
            }
            _ => {
                panic!("Error reading the files.");
            }
        }
    }
}

#[cfg(feature = "sync")]
/// Match the output and the baseline files.
pub fn match_files_blocking(output_path: impl AsRef<Path>, baseline_path: impl AsRef<Path>) {
    let output_file = std::fs::File::open(output_path).unwrap();
    let baseline_file = std::fs::File::open(baseline_path).unwrap();

    let output_reader = unsafe { Mmap::map(&output_file).unwrap() };
    let baseline_reader = unsafe { Mmap::map(&baseline_file).unwrap() };

    let mut cursor = 0;
    loop {
        let output_chunk =
            &output_reader[cursor..(cursor + MATCH_CHUNK_SIZE).min(output_reader.len())];
        let baseline_chunk =
            &baseline_reader[cursor..(cursor + MATCH_CHUNK_SIZE).min(baseline_reader.len())];

        if output_chunk != baseline_chunk {
            panic!(
                "The files differ at the following position:\noutput:{}\nbaseline:{}",
                String::from_utf8_lossy(output_chunk),
                String::from_utf8_lossy(baseline_chunk)
            )
        }

        cursor += MATCH_CHUNK_SIZE;

        if cursor >= output_reader.len() {
            break;
        }
    }
}
