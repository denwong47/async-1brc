//! The reader coroutine.

pub mod func;

mod models;
pub use models::*;

#[cfg(feature = "sync")]
pub mod sync;
