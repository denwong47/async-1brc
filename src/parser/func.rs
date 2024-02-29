//! Parsing utility functions.

/// An unsafe conversion from a guaranteed ASCII encoded digit to a digit.
pub fn u8_to_digit(
    byte: u8,
) -> u8 {
    byte & 15
}

/// An unsafe conversion from a guaranteed set of ASCII bytes into a String.
pub fn bytes_to_string(
    bytes: &[u8],
) -> std::borrow::Cow<'_, str> {
    String::from_utf8_lossy(bytes)
}