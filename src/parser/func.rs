//! Parsing utility functions.

/// An unsafe conversion from a guaranteed ASCII encoded digit to a digit.
pub fn u8_to_digit(byte: u8) -> u8 {
    byte & 15
}

/// Builds a number from a slice of ASCII encoded digits.
pub fn digits_to_number(digits: impl Iterator<Item = u8>) -> i16 {
    let mut multiplier = 1;

    digits.fold(0, |acc, digit| match digit {
        i if i.is_ascii_digit() => acc * 10 + u8_to_digit(i) as i16,
        b'-' => {
            multiplier = -1;
            acc
        }
        _ => acc,
    }) * multiplier
}

/// An unsafe conversion from a guaranteed set of ASCII bytes into a String.
pub fn bytes_to_string(bytes: &[u8]) -> std::borrow::Cow<'_, str> {
    String::from_utf8_lossy(bytes)
}
