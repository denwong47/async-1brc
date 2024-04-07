//! Parsing a 1BRC line, synchronously.

use super::{func, models};

/// Parse bytes into a [`models::StationRecords`].
///
/// This will parse the bytes into an existing [`models::StationRecords`], potentially local
/// to the caller's thread.
///
/// These parsing functions expect perfect input; if the input is not perfect, the behavior is
/// undefined.
#[allow(unreachable_code, unused_variables, unused_mut)]
pub fn parse_bytes(bytes: &[u8], records: &mut models::StationRecords) {
    #[cfg(feature = "debug")]
    let mut counter = 0;

    bytes
        .split(|&byte| byte == b'\n')
        .filter(|bytes| bytes.len() > 0)
        .for_each(|line| {
            #[cfg(feature = "debug")]
            '_debug: {
                counter += 1;
                if counter % 500_000 == 0 {
                    println!("Parsing line #{}...", counter);
                }
            }

            let mut line_split = line.split(|&byte| byte == b';');

            if let (Some(name), Some(value_raw), None) =
                (line_split.next(), line_split.next(), line_split.next())
            {
                records.insert(name.into(), parse_value(value_raw));
            } else {
                panic!(
                    "parse_bytes() found an invalid line: {:?}",
                    func::bytes_to_string(line)
                );
            }
        });
}

/// Parse value.
pub fn parse_value(bytes: &[u8]) -> i16 {
    let mut multiplier: i16 = 1;

    if bytes[0] == b'-' {
        multiplier = -1;
    }

    bytes.iter().fold(0, |acc, digit| match *digit {
        i if i.is_ascii_digit() => acc * 10 + func::u8_to_digit(i) as i16,
        _ => acc,
    }) * multiplier
}
