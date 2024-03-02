//! A timed

use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc,
};
use tokio::time::Instant;

/// An operation that needs to be timed.
///
/// This struct is used to measure the time spent in a particular operation,
/// and the number of times it has been called. This can be used in multiple
/// concurrent threads, and the results will be the cumulative wall time spent.
///
/// Upon dropping the operation, the total time spent and the number of calls
/// will be printed to the console.
///
/// # Note
/// When used with [`std::sync::OnceLock`] as a `static` variable, [`Drop`] will
/// not be called, and the total time spent will not be printed. In such a case,
/// use the [`TimedOperation::report`] method to print the total time spent before
/// the program exits.
///
/// # Limitations
/// This has a limited resolution of 1 nanosecond, any time spent less than that
/// could be rounded down to zero, and thus not be counted. This is not a problem
/// for most operations, except for a very large number of very fast operations.
///
/// There is also a limit of 2^64 nanoseconds, or 584 years, before the counter
/// overflows.
///
/// # Performance Penalty
/// The performance penalty will impact the overall run time, but should not have
/// a significant impact on the time spent in the operation itself. The performance
/// penalty is due to the atomic operations used to update the counters.
///
/// This also makes nested use of this struct inaccurate.
///
/// # Example
/// ```
/// use std::sync::Arc;
/// use async_1brc::timed::TimedOperation;
/// use tokio::time::{sleep, Duration};
///
/// #[tokio::main]
/// async fn main() {
///     let op = TimedOperation::new("test");
///     let handles = (0..5).into_iter().map(
///         |_| {
///             let op = Arc::clone(&op);
///             async move {
///                 let _counter = op.start();
///                 sleep(Duration::from_millis(100)).await;
///             }
///         }
///     );
///     
///     for handle in handles {
///         handle.await;
///     }
///
///     assert_eq!(op.count(), 5);
///     assert!(op.duration() >= Duration::from_millis(100) * 5);
/// }
#[derive(Debug)]
pub struct TimedOperation {
    name: String,
    ns: AtomicU64,
    max: AtomicU64,
    count: AtomicUsize,
}

#[allow(dead_code)]
impl TimedOperation {
    pub fn new(name: impl AsRef<str>) -> Arc<Self> {
        Arc::new(Self {
            name: name.as_ref().to_string(),
            ns: AtomicU64::default(),
            max: AtomicU64::default(),
            count: AtomicUsize::default(),
        })
    }

    /// Starts a new counter for the operation.
    ///
    /// The counter will be stopped when it goes out of scope,
    /// or when the `drop` method is called.
    pub fn start(self: &Arc<Self>) -> TimedOperationCounter {
        TimedOperationCounter {
            parent: Arc::clone(self),
            start: Instant::now(),
        }
    }

    /// Get the total number of calls made to the operation.
    pub fn count(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    /// Get the total time spent in the operation.
    ///
    /// This does not include the time spent in any active counters.
    pub fn ns(&self) -> u64 {
        self.ns.load(Ordering::Relaxed)
    }

    /// Get the maximum time spent in the operation.
    pub fn max_ns(&self) -> u64 {
        self.max.load(Ordering::Relaxed)
    }

    /// Get the maximum duration spent in the operation.
    pub fn max(&self) -> tokio::time::Duration {
        std::time::Duration::from_nanos(self.max_ns())
    }

    /// Get the total duration spent in the operation.
    pub fn duration(&self) -> tokio::time::Duration {
        std::time::Duration::from_nanos(self.ns())
    }

    /// Report the total time spent in the operation.
    pub fn report(&self) {
        let duration = self.duration();
        let count = self.count();
        let max = self.max();
        println!(
            "{} has had {} calls, totalling {:?}, with a maximum of {:?}.",
            self.name, count, duration, max
        );
    }
}

impl Drop for TimedOperation {
    fn drop(&mut self) {
        self.report()
    }
}

/// A counter linked to a [`TimedOperation`] instance.
///
/// [`TimedOperation`] is only used to store the total time spent and the number
/// of calls made to a particular operation; it does not actually submit any measurements.
/// To add measurements to the operation, use [`TimedOperation::start`], which will
/// instantiate a new [`TimedOperationCounter`] instance.
///
/// Upon dropping this counter, the time spent in the operation will be added to the
/// parent [`TimedOperation`] instance.
pub struct TimedOperationCounter {
    parent: Arc<TimedOperation>,
    start: Instant,
}

impl Drop for TimedOperationCounter {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed().as_nanos() as u64;
        self.parent.ns.fetch_add(elapsed, Ordering::Relaxed);
        self.parent.max.fetch_max(elapsed, Ordering::Relaxed);
        self.parent.count.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn single_call() {
        let op = TimedOperation::new("test");
        {
            let _counter = op.start();
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        assert_eq!(op.count(), 1);
        assert!(op.ns() >= 100);
    }

    #[tokio::test]
    async fn sequential_calls() {
        let op = TimedOperation::new("test");

        const REPEAT: u64 = 5;
        for _ in 0..REPEAT {
            let _counter = op.start();
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        assert_eq!(op.count(), REPEAT as usize);
        assert!(op.ns() >= 100 * REPEAT);
    }

    #[tokio::test]
    async fn concurrent_calls() {
        let op = TimedOperation::new("test");

        const REPEAT: u64 = 5;
        let handles = (0..REPEAT).map(|_| {
            let op = Arc::clone(&op);
            tokio::spawn(async move {
                let _counter = op.start();
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            })
        });

        for handle in handles {
            handle.await.unwrap();
        }

        assert_eq!(op.count(), REPEAT as usize);
        assert!(op.ns() >= 100 * REPEAT);
    }
}
