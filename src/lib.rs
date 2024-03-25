#![feature(portable_simd, iter_array_chunks, iter_map_windows)]

pub mod config;
pub mod parser;
pub mod reader;

mod args;
pub use args::CliArgs;

#[cfg(feature = "assert")]
pub mod assertion;

#[cfg(feature = "timed")]
pub mod timed;
