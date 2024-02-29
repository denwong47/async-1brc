//! Definitions of type aliases.

use std::{
    collections::BTreeSet,
    path::Path,
};

use tokio::{fs::File, io::AsyncWriteExt};

use super::{func, line};

use crate::reader::RowsReader;

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
    pub fn to_text(&self, name: &[u8]) -> String {
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
/// This internally uses a BTreeSet to keep the names sorted,
/// and a HashMap to keep the stats.
/// This allows for O(1) lookup for the stats, and a O(1) retrieval of the ordered names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StationRecords {
    names: BTreeSet<Vec<u8>>,
    stats: gxhash::GxHashMap<Vec<u8>, StationStats>,
}

impl Default for StationRecords {
    fn default() -> Self {
        Self {
            names: BTreeSet::new(),
            // The actual number of stations is 
            stats: gxhash::GxHashMap::with_capacity_and_hasher(500, gxhash::GxBuildHasher::default()),
        }
    }
}

impl StationRecords {
    /// Create a new empty [`StationRecords`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a new record by mutating the [`StationRecords`] in place.
    pub fn insert(
        &mut self,
        name: Vec<u8>,
        value: i16,
    ) {
        // Since we hold a mutable reference, this is essentially a mutex around both fields.
        self.names.insert(name.clone());
        self.stats.entry(name).and_modify(|stats| stats.extend(value)).or_insert(StationStats {
            min: value,
            max: value,
            sum: value as i32,
            count: 1,
        });
    }

    /// Get the stats of a single station.
    pub fn get(&self, name: &[u8]) -> Option<&StationStats> {
        self.stats.get(name)
    }

    /// Iterate through the records in an alphabetical order of the station names.
    pub fn iter(&self) -> IterStationRecords {
        IterStationRecords {
            iter: self.names.iter(),
            records: self,
        }
    }

    /// Export the results to a text in the 1BRC format.
    #[allow(dead_code)]
    pub fn to_text(&self) -> String {
        "{".to_owned()
        + &itertools::join(
            self.iter().map(
                |(name, stats)| stats.to_text(name)
            ),
            ", "
        )
        + "}"
    }

    /// Export the results to a file in the 1BRC format.
    pub async fn to_file(&self, path: impl AsRef<Path>) {
        let mut file = File::create(path).await.unwrap();

        file.write(self.to_text().as_bytes()).await.unwrap();
    }

    /// The main asynchronous function to read from a [`RowsReader`] and parse the data into itself.
    pub async fn read_from_reader(reader: &RowsReader) -> Self {
        let mut records = Self::new();

        while let Some(buffer) = reader.pop().await {
            #[cfg(feature="debug")]
            println!("read_from_reader() found {len} bytes of data.", len=buffer.len());

            line::parse_bytes(buffer, &mut records).await;
        }

        #[cfg(feature="debug")]
        println!("read_from_reader() finished.");

        records
    }
}

impl std::ops::AddAssign for StationRecords {
    fn add_assign(&mut self, mut rhs: Self) {
        self.names.append(&mut rhs.names);

        self.names.iter().for_each(|name| {
            self.stats
            .entry(name.clone())
            .and_modify(|stats| {
                *stats += rhs.stats.remove(name)
            })
            .or_insert_with(
                // This is safe because we know that the name exists in either BTreeSet.
                || rhs.stats.remove(name).unwrap()
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
pub struct IterStationRecords<'a> {
    iter: std::collections::btree_set::Iter<'a, Vec<u8>>,
    records: &'a StationRecords,
}

impl<'a> std::iter::Iterator for IterStationRecords<'a> {
    type Item = (&'a [u8], &'a StationStats);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|name| (name.as_slice(), self.records.get(name).unwrap()))
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

        assert_eq!(&stats.to_text(&(b"station1".to_vec())), "station1=1.0/3.5/6.0");
    }
    
    #[test]
    fn station_records_insert() {
        let mut records = StationRecords::new();
        records.insert(b"station1".to_vec(), 1);
        records.insert(b"station2".to_vec(), 2);

        records.insert(b"station1".to_vec(), 3);
        records.insert(b"station1".to_vec(), 4);
        records.insert(b"station1".to_vec(), 5);

        let stats1 = records.get(b"station1").unwrap();

        assert_eq!(stats1.min, 1);
        assert_eq!(stats1.max, 5);
        assert_eq!(stats1.sum, 13);
        assert_eq!(stats1.count, 4);

        let stats2 = records.get(b"station2").unwrap();

        assert_eq!(stats2.min, 2);
        assert_eq!(stats2.max, 2);
        assert_eq!(stats2.sum, 2);
        assert_eq!(stats2.count, 1);

        assert!(records.get(b"station3").is_none());
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

        let stats1 = records.get(b"station1").unwrap();

        assert_eq!(stats1.min, 1);
        assert_eq!(stats1.max, 5);
        assert_eq!(stats1.sum, 13);
        assert_eq!(stats1.count, 4);

        let stats2 = records.get(b"station2").unwrap();

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

        let mut iter = records.iter();

        assert_eq!(iter.next(), Some((&b"bar"[..], records.get(b"bar").unwrap())));
        assert_eq!(iter.next(), Some((&b"baz"[..], records.get(b"baz").unwrap())));
        assert_eq!(iter.next(), Some((&b"foo"[..], records.get(b"foo").unwrap())));
        assert_eq!(iter.next(), Some((&b"that"[..], records.get(b"that").unwrap())));
        assert_eq!(iter.next(), Some((&b"this"[..], records.get(b"this").unwrap())));
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
            records.to_text(),
            "{bar=0.2/0.2/0.2, baz=0.3/0.3/0.3, foo=0.1/0.1/0.1, that=0.5/0.5/0.5, this=0.4/0.4/0.4}"
        );
    }
}