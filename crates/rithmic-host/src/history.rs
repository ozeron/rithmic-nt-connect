//! Pure history replay window sizing.

use crate::{Error, Result};

/// Default tick / 1-second bar replay slice (seconds). ≤10k rows at 1s
/// without relying on pagination (seconds are denser / noisier).
pub const DEFAULT_TICK_SLICE_SECS: i32 = 2 * 60 * 60;
/// Default 1-minute bar replay slice (seconds). 40 calendar days covers a
/// month task plus ``window_utc`` ±6h padding as one paginated plant RPC.
pub const DEFAULT_BAR_SLICE_SECS: i32 = 40 * 24 * 60 * 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum TimeBarType {
    SecondBar = 1,
    MinuteBar = 2,
    DailyBar = 3,
    WeeklyBar = 4,
}

/// Slice length for a time-bar replay. Daily/weekly must be one (or few) wide
/// windows: short slices miss end-of-day markers and retry-empty for minutes.
pub fn bar_slice_secs(bar_type: TimeBarType, period: i32) -> i32 {
    match bar_type {
        TimeBarType::DailyBar | TimeBarType::WeeklyBar => i32::MAX / 4,
        // 60m / 15m: plant paginates; keep multi-month client chunks modest.
        TimeBarType::MinuteBar if period >= 60 => 180 * 24 * 60 * 60,
        TimeBarType::MinuteBar if period >= 15 => 90 * 24 * 60 * 60,
        TimeBarType::MinuteBar => DEFAULT_BAR_SLICE_SECS,
        TimeBarType::SecondBar => DEFAULT_TICK_SLICE_SECS,
    }
}

/// Inclusive `[start, end]` slices of at most `step_secs`.
///
/// Adjacent slices share the boundary second; callers must dedup.
pub fn window_slices(start: i32, end: i32, step_secs: i32) -> Vec<(i32, i32)> {
    if start > end || step_secs < 1 {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut cur = start;
    while cur <= end {
        let nxt = match cur.checked_add(step_secs) {
            Some(v) if v < end => v,
            _ => end,
        };
        out.push((cur, nxt));
        if nxt >= end {
            break;
        }
        cur = nxt;
    }
    out
}

/// Map Rithmic wire `bar_type` int (1..4) to [`TimeBarType`].
pub fn parse_time_bar_type(bar_type: i32) -> Result<TimeBarType> {
    match bar_type {
        1 => Ok(TimeBarType::SecondBar),
        2 => Ok(TimeBarType::MinuteBar),
        3 => Ok(TimeBarType::DailyBar),
        4 => Ok(TimeBarType::WeeklyBar),
        other => Err(Error::Protocol(format!(
            "unsupported time bar type {other}; expected 1=second, 2=minute, 3=daily, 4=weekly"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn empty_when_start_after_end() {
        assert!(window_slices(10, 9, 60).is_empty());
    }

    #[test]
    fn single_slice_when_shorter_than_step() {
        assert_eq!(window_slices(0, 100, 900), vec![(0, 100)]);
    }

    #[test]
    fn adjacent_slices_share_boundary() {
        assert_eq!(window_slices(0, 1800, 900), vec![(0, 900), (900, 1800)]);
    }

    #[test]
    fn exact_multiple_does_not_add_empty_tail() {
        assert_eq!(window_slices(0, 900, 900), vec![(0, 900)]);
    }

    #[test]
    fn daily_lookback_is_one_slice() {
        let step = bar_slice_secs(TimeBarType::DailyBar, 1);
        let start = 1_720_000_000;
        let end = start + 40 * 86_400;
        assert_eq!(window_slices(start, end, step).len(), 1);
    }
}
