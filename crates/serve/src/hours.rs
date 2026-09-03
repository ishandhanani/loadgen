// SPDX-License-Identifier: Apache-2.0

//! Shard-hour selection for replay bundles.

use std::collections::BTreeSet;

use anyhow::{Result, bail};
use chrono::DateTime;

/// Maximum shard-hours one interactive bundle may scan.
pub const MAX_SHARD_HOURS: usize = 12;

const MS_PER_HOUR: i64 = 3_600_000;

/// One UTC hour, counted from the Unix epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ShardHour(pub i64);

impl ShardHour {
    pub fn from_epoch_ms(ms: i64) -> Self {
        Self(ms.div_euclid(MS_PER_HOUR))
    }

    fn datetime(self) -> DateTime<chrono::Utc> {
        DateTime::from_timestamp(self.0 * 3600, 0).expect("hour index within chrono range")
    }

    /// `YYYY-MM-DD`, the `date=` partition value.
    pub fn date(self) -> String {
        self.datetime().format("%Y-%m-%d").to_string()
    }

    /// `HH`, the `hour=` partition value.
    pub fn hour(self) -> String {
        self.datetime().format("%H").to_string()
    }

    /// `YYYY-MM-DD/HH` for logs.
    pub fn label(self) -> String {
        self.datetime().format("%Y-%m-%d/%H").to_string()
    }
}

/// Expand request timestamps to the sorted set of shard hours to scan.
///
/// Shard hour keys lag request arrival (a 00:xx request can land in `hour=01`),
/// so every hour is widened by one hour on each side. More than
/// [`MAX_SHARD_HOURS`] hours is rejected as too large for an interactive export.
pub fn expand_hours(timestamps_ms: impl IntoIterator<Item = i64>) -> Result<Vec<ShardHour>> {
    let mut hours = BTreeSet::new();
    for ts in timestamps_ms {
        let hour = ShardHour::from_epoch_ms(ts);
        for delta in -1..=1 {
            hours.insert(ShardHour(hour.0 + delta));
        }
    }
    if hours.len() > MAX_SHARD_HOURS {
        bail!(
            "session spans {} shard-hours (> {MAX_SHARD_HOURS}); too large for an interactive export",
            hours.len()
        );
    }
    Ok(hours.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn widens_each_hour_by_one_on_both_sides() {
        // 2026-01-02T03:20:00Z
        let ts = 1_767_324_000_000_i64;
        let hours = expand_hours([ts]).unwrap();
        assert_eq!(hours.len(), 3);
        assert_eq!(hours[0].label(), "2026-01-02/02");
        assert_eq!(hours[1].label(), "2026-01-02/03");
        assert_eq!(hours[2].label(), "2026-01-02/04");
        assert_eq!(hours[1].date(), "2026-01-02");
        assert_eq!(hours[1].hour(), "03");
    }

    #[test]
    fn merges_overlapping_neighbors() {
        let base = 1_767_324_000_000_i64;
        let hours = expand_hours([base, base + MS_PER_HOUR]).unwrap();
        assert_eq!(hours.len(), 4);
    }

    #[test]
    fn rejects_more_than_twelve_hours() {
        let base = 1_767_324_000_000_i64;
        let ok = expand_hours((0..10).map(|i| base + i * MS_PER_HOUR)).unwrap();
        assert_eq!(ok.len(), 12);
        let error = expand_hours((0..11).map(|i| base + i * MS_PER_HOUR)).unwrap_err();
        assert!(error.to_string().contains("13 shard-hours (> 12)"));
    }
}
