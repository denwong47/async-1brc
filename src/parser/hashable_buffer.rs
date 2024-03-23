//! A [`u8`] buffer that just use its first 7 characters as the hash.

/// A [`u8`] buffer that just use its first 7 characters as the hash.
///
/// This will cause hash collisions if two identically sized buffer contains identical
/// first 7 characters; however this is considered not a problem for the purpose of this
/// crate.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct LiteHashBuffer {
    buffer: Vec<u8>,
}

impl LiteHashBuffer {
    /// Create a new instance with a buffer.
    pub fn new(buffer: Vec<u8>) -> Self {
        Self { buffer }
    }
}

impl<T> From<T> for LiteHashBuffer
where
    T: Into<Vec<u8>>,
{
    // Create a new instance with a buffer.
    fn from(buffer: T) -> Self {
        Self::new(buffer.into())
    }
}

impl std::hash::Hash for LiteHashBuffer {
    // Invoke write_u64() on the length of the buffer followed by the first 7 bytes of
    // the buffer.
    //
    // This allows the buffer to be hashed with [`nohash`] without actually hashing the
    // buffer.
    //
    // This however did not appear to be as fast as GxHash in itself.
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write_u64(
            self.buffer
                .iter()
                .take(7)
                .enumerate()
                .fold(self.buffer.len() as u64, |acc, (pos, &byte)| {
                    acc | ((byte as u64) << (pos * 8))
                }),
        )
    }
}

#[cfg(feature = "nohash")]
impl nohash::IsEnabled for LiteHashBuffer {}

impl std::ops::Deref for LiteHashBuffer {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}
