//! Parsing a 1BRC line.

use tokio::io::{AsyncBufReadExt, AsyncReadExt};

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
#[allow(unreachable_code, unused_variables, unused_mut)]
// Unused mut is used to prevent warnings when the `nohash` feature is disabled.
pub async fn parse_bytes<R>(mut bytes: R, records: &mut models::StationRecords)
where
    R: AsyncReadExt + AsyncBufReadExt + Unpin,
{
    #[cfg(feature = "noparse")]
    {
        // This will prevent any parsing from being done at all; all data will be discarded.
        // This is just for testing purposes.
        records.insert("some place".as_bytes().into(), 0);
        return;
    }

    let mut name = Vec::with_capacity(config::MAX_LINE_LENGTH);
    let mut digits = Vec::with_capacity(5);

    while let Some(name) = parse_name(&mut bytes, &mut name).await {
        let value = parse_value(&mut bytes, &mut digits).await;

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
pub async fn parse_name<R>(buffer: &mut R, name: &mut Vec<u8>) -> Option<LiteHashBuffer>
where
    R: AsyncBufReadExt + Unpin,
{
    // #[cfg(feature = "noparse-name")]
    // {
    //     buffer.read_until(b';', &mut Vec::with_capacity(config::MAX_LINE_LENGTH)).unwrap();
    //     return Some("some place".as_bytes().into());
    // }

    #[cfg(feature = "timed-extreme")]
    let _counter = PARSE_NAME_TIMED
        .get_or_init(|| TimedOperation::new("parse_name()"))
        .start();

    match buffer.read_until(b';', name).await {
        Ok(count) if count > 0 => Some({
            let mut name_with_semicolon = name.split_off(0);
            name_with_semicolon.pop();
            // `into` is used here to convert the `Vec<u8>` into a `LiteHashBuffer`...
            // ...or just to shutup rust analyzer.
            name_with_semicolon
        }),
        Ok(_) => {
            #[cfg(feature = "debug")]
            println!("parse_name() had an EOF.");
            None
        }
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
///
/// # Warning
///
/// This function expects each line to be terminated with a newline character.
/// It will always drop the last character - which is expected to be a newline -
/// regardless of what it actually is. This requires strict conformance to the
/// input format.
pub async fn parse_value<R>(buffer: &mut R, digits: &mut Vec<u8>) -> i16
where
    R: AsyncBufReadExt + Unpin,
{
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

    let len = buffer.read_until(b'\n', digits).await.expect(
        "parse_value() failed to read until newline; this should never happen, as measurement.txt is \
        guaranteed to have a newline.",
    );

    if digits[0] == b'-' {
        multiplier = -1;
    }

    digits
        .drain(..)
        .take(len - 1)
        .fold(0, |acc, digit| match digit {
            i if i.is_ascii_digit() => acc * 10 + func::u8_to_digit(i) as i16,
            _ => acc,
        })
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
                    let mut bytes = $input.as_bytes().to_vec();
                    bytes.push(b'\n');
                    let mut digits = Vec::with_capacity(5);

                    let mut buffer = &bytes[..];

                    assert_eq!(
                        parse_value(&mut buffer, &mut digits).await,
                        $expected
                    );
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
                    let bytes = $input.as_bytes().to_vec();
                    let mut buffer = &bytes[..];
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
            // This is actually a bug - because the function tries to pop the semi-colon,
            // it will always truncate the last character if the string does not end
            // with a semi-colon.
            //
            // With a perfectly formatted file like the measurements, this is not a problem.
            parse_name_unterminated,
            "hello, world!",
            Some("hello, world")
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
                    let buffer = &bytes[..];

                    parse_bytes(buffer, &mut records).await;

                    assert_eq!(
                        records.get(&$expected.0.to_vec().into()).unwrap().sum,
                        $expected.1
                    );
                }
            )*
        };
    }

    expand_parse_bytes_tests!(
        (
            parse_bytes_single_line,
            "jack;1.2\n",
            ("jack".as_bytes(), 12)
        ),
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
