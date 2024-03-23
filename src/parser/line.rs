//! Parsing a 1BRC line.

use std::io::{BufRead, Cursor};

use tokio::io::AsyncReadExt;

use super::super::config;
use super::{func, models, LiteHashBuffer};

#[cfg(feature = "timed-extreme")]
use super::super::timed::TimedOperation;

#[cfg(feature = "timed-extreme")]
pub static PARSE_NAME_TIMED: std::sync::OnceLock<std::sync::Arc<TimedOperation>> =
    std::sync::OnceLock::new();

#[cfg(feature = "timed-extreme")]
pub static PARSE_VALUE_TIMED: std::sync::OnceLock<std::sync::Arc<TimedOperation>> =
    std::sync::OnceLock::new();

/// Parse bytes into a [`models::StationRecords`].
///
/// This will parse the bytes into an existing [`models::StationRecords`], potentially local
/// to the caller's thread.
///
/// These parsing functions expect perfect input; if the input is not perfect, the behavior is
/// undefined.
#[allow(unreachable_code)]
#[allow(unused_variables)]
pub async fn parse_bytes(bytes: Vec<u8>, records: &mut models::StationRecords) {
    #[cfg(feature = "noparse")]
    {
        // This will prevent any parsing from being done at all; all data will be discarded.
        // This is just for testing purposes.
        records.insert("some place".as_bytes().into(), 0);
        return;
    }

    let mut name = Vec::with_capacity(config::MAX_LINE_LENGTH);
    let mut digits = Vec::with_capacity(4);
    let mut buffer = Cursor::new(bytes.as_slice());

    while let Some(name) = parse_name(&mut buffer, &mut name).await {
        let value = parse_value(&mut buffer, &mut digits).await;

        // #[cfg(feature="debug")]
        // println!("parse_bytes() found: {} {}", func::bytes_to_string(&name), value);

        records.insert(name, value)
    }
}

/// Parse name.
///
/// This expects the buffer to be at the start of the name, and ends at the semicolon.
/// No other characters are allowed to terminate the name; if the buffer ends before the semicolon,
/// the behavior is undefined.
pub async fn parse_name(buffer: &mut Cursor<&[u8]>, name: &mut Vec<u8>) -> Option<LiteHashBuffer> {
    // #[cfg(feature = "noparse-name")]
    // {
    //     buffer.read_until(b';', &mut Vec::with_capacity(config::MAX_LINE_LENGTH)).unwrap();
    //     return Some("some place".as_bytes().into());
    // }

    #[cfg(feature = "timed-extreme")]
    let _counter = PARSE_NAME_TIMED
        .get_or_init(|| TimedOperation::new("parse_name()"))
        .start();

    match buffer.read_until(b';', name) {
        Ok(count) if count > 0 => {
            Some({
                let mut name_with_semicolon = name.split_off(0);
                name_with_semicolon.pop();
                name_with_semicolon.into()
            })
        },
        Ok(_) => {
            #[cfg(feature = "debug")]
            println!("parse_name() had an EOF.");
            None
        }
        // This is normal behaviour when the buffer has ended.
        Err(_err) => {
            #[cfg(feature = "debug")]
            println!("parse_name() read_u8() error: {}", _err);

            None
        }
    }
}

/// Parse value.
///
/// This will parse a single decimal float from the buffer.
/// It is returned as a 16-bit integer, with the last digit being the decimal;
/// for example, 123.4 will be returned as 1234.
///
/// If the value contains more than 1 decimal point, the behavior is undefined.
pub async fn parse_value<'a>(buffer: &mut Cursor<&[u8]>, digits: &mut Vec<u8>) -> i16 {
    // #[cfg(feature = "noparse-value")]
    // {
    //     buffer.read_until(b'\n', &mut Vec::with_capacity(config::MAX_LINE_LENGTH)).await;
    //     return 0;
    // }

    let mut multiplier: i16 = 1;

    #[cfg(feature = "timed-extreme")]
    let _counter = PARSE_VALUE_TIMED
        .get_or_init(|| TimedOperation::new("parse_value()"))
        .start();

    loop {
        match buffer.read_u8().await {
            Ok(b'-') => {
                // This does not care if the '-' is in the middle of the number;
                // this is to safe computation.
                multiplier = -1;
            }
            Ok(b'\n') => {
                break;
            }
            Ok(b'.') => {}
            Ok(ascii) => {
                // This is safe because we know that the byte is a digit.
                digits.push(func::u8_to_digit(ascii));
            }
            Err(_err) => {
                #[cfg(feature = "debug")]
                println!("parse_value() read_u8() error: {}", _err);

                break;
            }
        }
    }

    digits
        .drain(..)
        .fold(0, |acc, digit| acc * 10 + digit as i16)
        * multiplier
}

#[cfg(test)]
mod test {
    use super::*;

    macro_rules! expand_parse_value_tests {
        ($((
            $name:ident,
            $input:expr,
            $expected:expr
        )),*$(,)?) => {
            $(
                #[tokio::test]
                async fn $name() {
                    let mut buffer = Cursor::new($input.as_bytes());
                    let mut digits = Vec::with_capacity(4);

                    assert_eq!(
                        parse_value(&mut buffer, &mut digits).await,
                        $expected
                    );

                    assert!(buffer.read_u8().await.is_err());
                }
            )*
        };
    }

    expand_parse_value_tests!(
        (parse_value_0, "0", 0),
        (parse_value_10, "10", 10),
        (parse_value_5354, "535.4", 5354),
        (parse_value_neg_0, "-0", 0),
        (parse_value_neg_1, "-1", -1),
        (parse_value_neg_5354, "-535.4", -5354),
        (parse_value_0_newline, "0\n", 0),
        (parse_value_10_newline, "10\n", 10),
        (parse_value_5354_newline, "535.4\n", 5354),
        (parse_value_neg_0_newline, "-0\n", 0),
        (parse_value_neg_1_newline, "-1\n", -1),
        (parse_value_neg_5354_newline, "-535.4\n", -5354),
    );

    macro_rules! expand_parse_name_tests {
        ($((
            $name:ident,
            $input:expr,
            $expected:expr
        )),*$(,)?) => {
            $(
                #[tokio::test]
                async fn $name() {
                    let mut buffer = Cursor::new($input.as_bytes());

                    let mut name = Vec::with_capacity(config::MAX_LINE_LENGTH);

                    assert_eq!(
                        parse_name(&mut buffer, &mut name).await,
                        $expected.map(|text| text.as_bytes().to_vec().into())
                    );
                }
            )*
        };
    }

    expand_parse_name_tests!(
        (parse_name_name1, "abc;", Some("abc")),
        (parse_name_name2, "10;", Some("10")),
        (parse_name_name3, "hello, world!;", Some("hello, world!")),
        (
            parse_name_unterminated,
            "hello, world!",
            Option::<&str>::None
        ),
        (
            parse_name_trailing_texts,
            "hello, world!;123.4",
            Some("hello, world!")
        ),
        (parse_name_multi_lines, "jack;1.2\njill:3.4", Some("jack")),
    );

    macro_rules! expand_parse_bytes_tests {
        ($((
            $name:ident,
            $input:expr,
            $expected:expr
        )),*$(,)?) => {
            $(
                #[tokio::test]
                async fn $name() {
                    let mut records = models::StationRecords::new();
                    let bytes = $input.as_bytes().to_vec();

                    parse_bytes(bytes, &mut records).await;

                    assert_eq!(
                        records.get(&$expected.0.to_vec().into()).unwrap().sum,
                        $expected.1
                    );
                }
            )*
        };
    }

    expand_parse_bytes_tests!(
        (parse_bytes_single_line, "jack;1.2", ("jack".as_bytes(), 12)),
        (
            parse_bytes_single_line_with_newline,
            "jack;1.2\n",
            ("jack".as_bytes(), 12)
        ),
        (
            parse_bytes_multiple_jills,
            "jill;3.4\njack;1.2\njill;2.3\njill;4.5\n",
            ("jill".as_bytes(), 102)
        ),
    );
}
