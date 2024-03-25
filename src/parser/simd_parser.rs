//! A trial to use SIMD to parse the lines in the buffer.
//!
//! This is not expected to make a big difference in performance, since there is not a lot of
//! actual SIMD operations possible in this case.

use std::{collections::VecDeque, simd::cmp::SimdPartialEq, sync::OnceLock};

use super::{func, models};

/// The positions of the separators in a line; the first one being the semi-colon, and the second
/// one being the new line.
pub type SepPositions = [usize; 2];

const LANE_WIDTH: usize = 64;

static SEMI_COLON: OnceLock<std::simd::Simd<u8, LANE_WIDTH>> = OnceLock::new();
static NEW_LINE: OnceLock<std::simd::Simd<u8, LANE_WIDTH>> = OnceLock::new();

/// Find all the separators in a chunk of 64 bytes using SIMD.
fn find_separators_simd(chunk: &[u8]) -> VecDeque<SepPositions> {
    let chunk_simd = std::simd::Simd::from_slice(chunk);
    let semi_colon = SEMI_COLON.get_or_init(|| std::simd::Simd::splat(b';'));
    let new_line = NEW_LINE.get_or_init(|| std::simd::Simd::splat(b'\n'));

    let mask = chunk_simd.simd_eq(*semi_colon) | chunk_simd.simd_eq(*new_line);

    [0].into_iter()
        .chain((0..LANE_WIDTH).filter(|i| mask.test(*i)))
        // This is necessary because we don't start with a separator,
        // so the first `y` will NOT count any separator, while any subsequent
        // `y` will count the separator.
        .map_windows(|[x, y]| if x == &0 { y - x } else { y - x - 1 })
        .array_chunks::<2>()
        .collect()
    // This will discard the last separator if it is not a new line.
}

/// Find all the separators in a chunk of bytes by iterating over them.
///
/// This function is used as a fallback when the chunk is shorter than 64 bytes.
fn find_separators_iter(chunk: &[u8]) -> VecDeque<SepPositions> {
    [0].into_iter()
        .chain(chunk.iter().enumerate().filter_map(|(id, &byte)| {
            if byte == b';' || byte == b'\n' {
                Some(id)
            } else {
                None
            }
        }))
        // This is necessary because we don't start with a separator,
        // so the first `y` will NOT count any separator, while any subsequent
        // `y` will count the separator.
        .map_windows(|[x, y]| if x == &0 { y - x } else { y - x - 1 })
        .array_chunks::<2>()
        .collect()
}

/// Find all the separators in a chunk of bytes.
fn find_separators(chunk: &[u8]) -> VecDeque<SepPositions> {
    if chunk.len() >= LANE_WIDTH {
        find_separators_simd(chunk)
    } else {
        find_separators_iter(chunk)
    }
}

/// A parser that reads lines from a buffer and extracts the values from them.
pub struct LineParser {
    cursor: usize,
    buffer: Vec<u8>,
    next: VecDeque<SepPositions>,
}

impl LineParser {
    /// Create a new `LineParser` from a buffer.
    pub fn new(buffer: Vec<u8>) -> Self {
        Self {
            cursor: 0,
            buffer,
            next: VecDeque::with_capacity(8),
        }
    }

    /// Parse the next line from the buffer.
    pub fn parse_line(&mut self) -> Option<(Vec<u8>, i16)> {
        if self.next.is_empty() && self.cursor < self.buffer.len() {
            self.next = find_separators(
                &self.buffer[self.cursor..(self.cursor + LANE_WIDTH).min(self.buffer.len())],
            );
        }

        if self.next.is_empty() {
            return None;
        }

        let [semi_colon, new_line] = self
            .next
            .pop_front()
            .expect("Unreachable, the next separators should be present.");

        let name = &self.buffer[self.cursor..self.cursor + semi_colon];
        self.cursor += semi_colon + 1;
        let value = &self.buffer[self.cursor..self.cursor + new_line];
        self.cursor += new_line + 1;

        Some((name.to_vec(), func::digits_to_number(value.iter().copied())))
    }

    /// Parse all the bytes in the buffer.
    pub fn parse_bytes(bytes: Vec<u8>, records: &mut models::StationRecords) {
        let mut parser = Self::new(bytes);

        while let Some((name, value)) = parser.parse_line() {
            records.insert(name.into(), value);
        }
    }
}

impl Iterator for LineParser {
    type Item = (Vec<u8>, i16);

    /// Iterate over the lines in the buffer.
    fn next(&mut self) -> Option<Self::Item> {
        self.parse_line()
    }
}

/// FIXME These tests needs to be expanded by a lot.
#[cfg(test)]
mod test {
    use super::*;

    static SAMPLE_CHUNK: &[u8] =
        b"station 1;1.23\nstation 2;4.56\nstation 3;7.89\nstation 15;0.12\n12345678";

    #[test]
    fn find_separators_simd_in_text() {
        let chunk = SAMPLE_CHUNK.to_vec();
        let result_simd = find_separators_simd(&chunk);
        let result_iter = find_separators_iter(&chunk);
        assert_eq!(result_iter, [[9, 4], [9, 4], [9, 4], [10, 4]]);
        assert_eq!(result_simd, [[9, 4], [9, 4], [9, 4], [10, 4]]);
    }

    #[test]
    fn parse_line_in_text() {
        let parser = LineParser::new(SAMPLE_CHUNK.to_vec());

        for (real, expected) in parser.zip(vec![
            (b"station 1".to_vec(), 123),
            (b"station 2".to_vec(), 456),
            (b"station 3".to_vec(), 789),
            (b"station 15".to_vec(), 12),
        ]) {
            println!("{:?} {:?}", real, expected);
            assert_eq!(real, expected);
        }
    }
}
