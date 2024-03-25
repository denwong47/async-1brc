//! Definitions of type aliases.

use std::path::Path;

use itertools::Itertools;
use tokio::{fs::File, io::AsyncWriteExt};

use super::{func, line, LiteHashBuffer};

use crate::reader::RowsReader;

#[cfg(feature = "timed")]
use super::super::timed::TimedOperation;

#[cfg(feature = "timed-extreme")]
pub static HASH_INSERT_TIMED: std::sync::OnceLock<std::sync::Arc<TimedOperation>> =
    std::sync::OnceLock::new();

#[cfg(feature = "nohash")]
pub use std::hash::BuildHasherDefault;

/// Statistics of a single station.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StationStats {
    pub min: i16,
    pub max: i16,
    pub sum: i32,
    pub count: usize,
}

impl Default for StationStats {
    fn default() -> Self {
        Self {
            min: i16::MAX,
            max: i16::MIN,
            sum: 0,
            count: 0,
        }
    }
}

impl StationStats {
    /// Create a new [`StationStats`] with a single value.
    pub fn new(value: i16) -> Self {
        Self {
            min: value,
            max: value,
            sum: value as i32,
            count: 1,
        }
    }

    /// Append a single value to the stats.
    pub fn extend(&mut self, value: i16) {
        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }

        self.sum += value as i32;
        self.count += 1;
    }

    /// Export the stats to a 1BRC format string.
    pub fn export_text(&self, name: &[u8]) -> String {
        format!(
            "{name}={min:.1}/{avg:.1}/{max:.1}",
            name = func::bytes_to_string(name),
            min = self.min as f32 / 10.0,
            avg = self.sum as f32 / self.count as f32 / 10.0,
            max = self.max as f32 / 10.0,
        )
    }
}

impl From<i16> for StationStats {
    fn from(value: i16) -> Self {
        Self::new(value)
    }
}

impl std::ops::Add for StationStats {
    type Output = Self;

    /// Combine two [`StationStats`] together.
    fn add(mut self, rhs: Self) -> Self::Output {
        self += rhs;
        self
    }
}

impl std::ops::AddAssign for StationStats {
    /// Combine two [`StationStats`] together.
    fn add_assign(&mut self, rhs: Self) {
        self.min = self.min.min(rhs.min);
        self.max = self.max.max(rhs.max);
        self.sum += rhs.sum;
        self.count += rhs.count;
    }
}

impl std::ops::AddAssign<Option<Self>> for StationStats {
    /// Combine two [`StationStats`] together.
    fn add_assign(&mut self, rhs: Option<Self>) {
        if let Some(rhs) = rhs {
            self.min = self.min.min(rhs.min);
            self.max = self.max.max(rhs.max);
            self.sum += rhs.sum;
            self.count += rhs.count;
        }
    }
}

/// Records of multiple stations.
/// This internally uses a HashMap to keep the stats.
/// This used to have a BTreeSet to keep the names in order, but it was removed for
/// performance reasons.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StationRecords {
    #[cfg(not(feature = "nohash"))]
    stats: gxhash::GxHashMap<LiteHashBuffer, StationStats>,

    #[cfg(feature = "nohash")]
    stats: std::collections::HashMap<
        LiteHashBuffer,
        StationStats,
        BuildHasherDefault<nohash::NoHashHasher<u64>>,
    >,
}

impl Default for StationRecords {
    #[cfg(not(feature = "nohash"))]
    fn default() -> Self {
        Self {
            // The actual number of stations is 400-ish.
            stats: std::collections::HashMap::with_capacity_and_hasher(
                500,
                gxhash::GxBuildHasher::default(),
            ),
        }
    }

    #[cfg(feature = "nohash")]
    fn default() -> Self {
        Self {
            // The actual number of stations is 400-ish.
            stats: std::collections::HashMap::with_capacity_and_hasher(
                500,
                BuildHasherDefault::default(),
            ),
        }
    }
}

impl StationRecords {
    /// Create a new empty [`StationRecords`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a new record by mutating the [`StationRecords`] in place.
    pub fn insert(&mut self, name: LiteHashBuffer, value: i16) {
        #[cfg(feature = "timed-extreme")]
        let _counter = HASH_INSERT_TIMED
            .get_or_init(|| TimedOperation::new("StationRecords::insert()"))
            .start();

        // Since we hold a mutable reference, this is essentially a mutex around both fields.
        self.stats
            .entry(name)
            .and_modify(|stats| stats.extend(value))
            .or_insert(StationStats {
                min: value,
                max: value,
                sum: value as i32,
                count: 1,
            });
    }

    /// Get the stats of a single station.
    pub fn get(&self, name: &LiteHashBuffer) -> Option<&StationStats> {
        self.stats.get(name)
    }

    /// Calculate the length of the records.
    #[cfg(feature = "assert")]
    pub fn len(&self) -> usize {
        self.stats.values().map(|stats| stats.count).sum()
    }

    /// Check if the records are empty.
    #[cfg(feature = "assert")]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterate through the records in an arbitrary order.
    #[allow(dead_code)]
    pub fn iter(
        &self,
    ) -> IterStationRecords<std::collections::hash_map::Keys<LiteHashBuffer, StationStats>> {
        IterStationRecords {
            iter: self.stats.keys(),
            records: self,
        }
    }

    /// Iterate through the records in an alphabetical order of the station names.
    pub fn iter_sorted(&self) -> IterStationRecords<std::vec::IntoIter<&LiteHashBuffer>> {
        let mut names = self.stats.keys().collect_vec();
        names.sort();

        IterStationRecords {
            iter: names.into_iter(),
            records: self,
        }
    }

    /// Export the results to a text in the 1BRC format.
    #[allow(dead_code)]
    pub fn export_text(&self) -> String {
        "{".to_owned()
            + &itertools::join(
                self.iter_sorted()
                    .map(|(name, stats)| stats.export_text(name)),
                ", ",
            )
            + "}\n"
    }

    /// Export the results to a file in the 1BRC format.
    pub async fn export_file(&self, path: impl AsRef<Path>) {
        #[cfg(feature = "timed")]
        let _ops = TimedOperation::new("StationRecords::export_file()");
        #[cfg(feature = "timed")]
        let _counter = _ops.start();

        let mut file = File::create(path).await.unwrap();

        file.write_all(self.export_text().as_bytes()).await.unwrap();
    }

    /// The main asynchronous function to read from a [`RowsReader`] and parse the data into itself.
    pub async fn read_from_reader(reader: &RowsReader) -> Self {
        let mut records = Self::new();

        while let Some(buffer) = reader.pop().await {
            #[cfg(feature = "debug")]
            println!(
                "read_from_reader() found {len} bytes of data.",
                len = buffer.len()
            );

            line::parse_bytes(&buffer[..], &mut records).await;
        }

        #[cfg(feature = "debug")]
        println!("read_from_reader() finished.");

        records
    }
}

impl std::ops::AddAssign for StationRecords {
    fn add_assign(&mut self, mut rhs: Self) {
        rhs.stats.drain().for_each(|(name, rhs_stats)| {
            self.stats
                .entry(name.clone())
                .and_modify(|lhs_stats| *lhs_stats += rhs_stats)
                .or_insert_with(
                    // This is safe because we know that the name exists in either BTreeSet.
                    || rhs_stats,
                );
        });
    }
}

impl std::ops::Add for StationRecords {
    type Output = Self;

    fn add(mut self, rhs: Self) -> Self::Output {
        self += rhs;
        self
    }
}

impl std::iter::Sum for StationRecords {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.reduce(|a, b| a + b).unwrap_or_default()
    }
}

/// An iterator over the records of a [`StationRecords`].
pub struct IterStationRecords<'a, T>
where
    T: Iterator<Item = &'a LiteHashBuffer>,
{
    iter: T,
    records: &'a StationRecords,
}

impl<'a, T> std::iter::Iterator for IterStationRecords<'a, T>
where
    T: Iterator<Item = &'a LiteHashBuffer>,
{
    type Item = (&'a [u8], &'a StationStats);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .map(|name| (name.as_slice(), self.records.get(name).unwrap()))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn station_stats_extend() {
        let mut stats = StationStats::default();
        stats.extend(1);
        stats.extend(2);
        stats.extend(3);
        stats.extend(4);
        stats.extend(5);
        stats.extend(6);
        stats.extend(7);
        stats.extend(8);
        stats.extend(9);
        stats.extend(10);

        assert_eq!(stats.min, 1);
        assert_eq!(stats.max, 10);
        assert_eq!(stats.sum, 55);
        assert_eq!(stats.count, 10);
    }

    #[test]
    fn station_stats_export() {
        let mut stats = StationStats::new(10);
        stats.extend(60);
        stats.extend(40);
        stats.extend(20);
        stats.extend(50);
        stats.extend(30);

        assert_eq!(
            &stats.export_text(b"station1".as_ref()),
            "station1=1.0/3.5/6.0"
        );
    }

    #[test]
    fn station_records_insert() {
        let mut records = StationRecords::new();
        records.insert(b"station1".to_vec(), 1);
        records.insert(b"station2".to_vec(), 2);

        records.insert(b"station1".to_vec(), 3);
        records.insert(b"station1".to_vec(), 4);
        records.insert(b"station1".to_vec(), 5);

        let stats1 = records.get(&b"station1".into()).unwrap();

        assert_eq!(stats1.min, 1);
        assert_eq!(stats1.max, 5);
        assert_eq!(stats1.sum, 13);
        assert_eq!(stats1.count, 4);

        let stats2 = records.get(&b"station2".into()).unwrap();

        assert_eq!(stats2.min, 2);
        assert_eq!(stats2.max, 2);
        assert_eq!(stats2.sum, 2);
        assert_eq!(stats2.count, 1);

        assert!(records.get(&b"station3".into()).is_none());
    }

    #[test]
    fn station_records_add() {
        let mut records1 = StationRecords::new();
        records1.insert(b"station1".to_vec(), 1);
        records1.insert(b"station2".to_vec(), 2);

        let mut records2 = StationRecords::new();
        records2.insert(b"station1".to_vec(), 3);
        records2.insert(b"station1".to_vec(), 4);
        records2.insert(b"station1".to_vec(), 5);
        records2.insert(b"station2".to_vec(), 6);
        records2.insert(b"station2".to_vec(), 7);
        records2.insert(b"station2".to_vec(), 8);

        let records = records1 + records2;

        let stats1 = records.get(&b"station1".into()).unwrap();

        assert_eq!(stats1.min, 1);
        assert_eq!(stats1.max, 5);
        assert_eq!(stats1.sum, 13);
        assert_eq!(stats1.count, 4);

        let stats2 = records.get(&b"station2".into()).unwrap();

        assert_eq!(stats2.min, 2);
        assert_eq!(stats2.max, 8);
        assert_eq!(stats2.sum, 23);
        assert_eq!(stats2.count, 4);
    }

    #[test]
    fn station_records_iter() {
        let mut records = StationRecords::new();
        records.insert(b"this".to_vec(), 4);
        records.insert(b"that".to_vec(), 5);
        records.insert(b"foo".to_vec(), 1);
        records.insert(b"bar".to_vec(), 2);
        records.insert(b"baz".to_vec(), 3);

        let mut iter = records.iter_sorted();

        assert_eq!(
            iter.next(),
            Some((&b"bar"[..], records.get(&b"bar".into()).unwrap()))
        );
        assert_eq!(
            iter.next(),
            Some((&b"baz"[..], records.get(&b"baz".into()).unwrap()))
        );
        assert_eq!(
            iter.next(),
            Some((&b"foo"[..], records.get(&b"foo".into()).unwrap()))
        );
        assert_eq!(
            iter.next(),
            Some((&b"that"[..], records.get(&b"that".into()).unwrap()))
        );
        assert_eq!(
            iter.next(),
            Some((&b"this"[..], records.get(&b"this".into()).unwrap()))
        );
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn station_records_export() {
        let mut records = StationRecords::new();
        records.insert(b"this".to_vec(), 4);
        records.insert(b"that".to_vec(), 5);
        records.insert(b"foo".to_vec(), 1);
        records.insert(b"bar".to_vec(), 2);
        records.insert(b"baz".to_vec(), 3);

        assert_eq!(
            records.export_text(),
            "{bar=0.2/0.2/0.2, baz=0.3/0.3/0.3, foo=0.1/0.1/0.1, that=0.5/0.5/0.5, this=0.4/0.4/0.4}\n"
        );
    }
}
