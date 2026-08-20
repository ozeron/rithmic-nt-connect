//! History-window helpers: slice long ranges, retry, sort, dedup.
//!
//! rithmic-rs `*_all` lifts the silent 10_000-record cap per request, but a full
//! session is still too large for one call. Slice ticks (default 15 minutes) and
//! time bars (default 4 hours). Boundary overlaps are removed by dedup.

use std::collections::HashSet;
use std::time::Duration;

use rithmic_rs::TimeBarType;

use crate::dto::{HistoryBarDto, HistoryTickDto};
use crate::error::{Error, Result};

/// Default tick replay slice (seconds). Matches Lucid / example windowing.
pub const DEFAULT_TICK_SLICE_SECS: i32 = 15 * 60;
/// Default time-bar replay slice (seconds) for 1-minute bars.
pub const DEFAULT_BAR_SLICE_SECS: i32 = 4 * 60 * 60;

/// Slice length for a time-bar replay. Daily/weekly must be one (or few) wide
/// windows: 4-hour slices miss end-of-day markers and retry-empty for minutes.
pub fn bar_slice_secs(bar_type: TimeBarType, period: i32) -> i32 {
    match bar_type {
        TimeBarType::DailyBar | TimeBarType::WeeklyBar => i32::MAX / 4,
        TimeBarType::MinuteBar if period >= 60 => 24 * 60 * 60,
        TimeBarType::MinuteBar if period >= 15 => 12 * 60 * 60,
        TimeBarType::MinuteBar => DEFAULT_BAR_SLICE_SECS,
        TimeBarType::SecondBar => DEFAULT_TICK_SLICE_SECS,
        _ => DEFAULT_BAR_SLICE_SECS,
    }
}

/// True when `v` looks like `YYYYMMDD` in 1900–2100 (not a Unix second).
pub fn is_yyyymmdd(v: i32) -> bool {
    if !(19_00_01_01..=21_00_12_31).contains(&v) {
        return false;
    }
    let y = v / 10_000;
    let m = (v / 100) % 100;
    let d = v % 100;
    (1900..=2100).contains(&y) && (1..=12).contains(&m) && (1..=31).contains(&d)
}

/// Calendar date `YYYYMMDD` (UTC) for a Unix timestamp.
pub fn unix_to_yyyymmdd_utc(unix_sec: i64) -> i32 {
    let z = unix_sec.div_euclid(86_400) + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    let y = y + i64::from(m <= 2);
    (y * 10_000 + m * 100 + d) as i32
}

/// Unix seconds at UTC midnight for `YYYYMMDD`.
///
/// Returns `None` when the date falls outside the `i32` Unix-seconds range
/// (before ~1901-12-14 or after ~2038-01-19).
pub fn yyyymmdd_to_unix_utc(ymd: i32) -> Option<i32> {
    let y = ymd / 10_000;
    let m = (ymd / 100) % 100;
    let d = ymd % 100;
    let y = y - i32::from(m <= 2);
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400) as u32;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) as u32 + 2) / 5 + d as u32 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = i64::from(era) * 146_097 + i64::from(doe) - 719_468;
    i32::try_from(days * 86_400).ok()
}

/// `start_index` / `finish_index` for a time-bar replay from a **Unix** window.
///
/// Lucid daily/weekly replays use calendar `YYYYMMDD`, not Unix seconds.
/// Minute/second bars keep Unix seconds. Always converts daily/weekly from
/// Unix (do not pass pre-encoded YYYYMMDD here — use the value as-is on the
/// wire only from callers that already speak calendar indexes).
pub fn bar_replay_index(bar_type: TimeBarType, unix_sec: i32) -> i32 {
    match bar_type {
        TimeBarType::DailyBar | TimeBarType::WeeklyBar => unix_to_yyyymmdd_utc(i64::from(unix_sec)),
        _ => unix_sec,
    }
}

/// Daily/weekly markers are `YYYYMMDD`; convert those to Unix seconds.
pub fn marker_to_ssboe(marker: i32) -> Option<i32> {
    if is_yyyymmdd(marker) {
        yyyymmdd_to_unix_utc(marker)
    } else {
        Some(marker)
    }
}

/// Attempts for a transient plant error on one slice.
pub const DEFAULT_TRANSIENT_RETRIES: u32 = 3;
/// Extra attempts when a slice returns no rows (venue empty-glitch).
pub const DEFAULT_EMPTY_RETRIES: u32 = 2;

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

/// `(ts_event_ns, close_price bits, num_trades, volume)` for tick identity.
pub fn tick_dedup_key(tick: &HistoryTickDto) -> (u64, u64, u64, u64) {
    (
        tick.ts_event_ns.unwrap_or(0),
        tick.close_price.map(f64::to_bits).unwrap_or(0),
        tick.num_trades.unwrap_or(0),
        tick.volume.unwrap_or(0),
    )
}

/// `(ts_event_ns, o/h/l/c bits, volume)` for bar identity.
pub fn bar_dedup_key(bar: &HistoryBarDto) -> (u64, u64, u64, u64, u64, u64) {
    (
        bar.ts_event_ns.unwrap_or(0),
        bar.open_price.map(f64::to_bits).unwrap_or(0),
        bar.high_price.map(f64::to_bits).unwrap_or(0),
        bar.low_price.map(f64::to_bits).unwrap_or(0),
        bar.close_price.map(f64::to_bits).unwrap_or(0),
        bar.volume.unwrap_or(0),
    )
}

/// Keep first occurrence; preserve input order.
fn dedup_by<T, K: Eq + std::hash::Hash>(items: Vec<T>, key_fn: impl Fn(&T) -> K) -> Vec<T> {
    let mut seen = HashSet::new();
    items
        .into_iter()
        .filter(|item| seen.insert(key_fn(item)))
        .collect()
}

/// Keep first occurrence; preserve input order.
pub fn dedup_ticks(ticks: Vec<HistoryTickDto>) -> Vec<HistoryTickDto> {
    dedup_by(ticks, tick_dedup_key)
}

/// Keep first occurrence; preserve input order.
pub fn dedup_bars(bars: Vec<HistoryBarDto>) -> Vec<HistoryBarDto> {
    dedup_by(bars, bar_dedup_key)
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

/// Connection / plant-down errors that are safe to retry on a read-only replay.
pub fn is_transient(err: &Error) -> bool {
    match err {
        Error::NotConnected { .. } | Error::ChannelClosed { .. } | Error::ChannelLagged { .. } => {
            true
        }
        Error::Rithmic(text) | Error::Session(text) => {
            let lower = text.to_ascii_lowercase();
            lower.contains("connection closed")
                || lower.contains("not connected")
                || lower.contains("forced logout")
                || lower.contains("channel closed")
                || lower.contains("channel lagged")
                || lower.contains("connection issue")
        }
        _ => false,
    }
}

pub(crate) async fn load_sliced<T, F, Fut>(
    start: i32,
    end: i32,
    step_secs: i32,
    mut load_slice: F,
    sort_key: impl Fn(&T) -> u64,
    dedup: impl Fn(Vec<T>) -> Vec<T>,
) -> Result<Vec<T>>
where
    F: FnMut(i32, i32) -> Fut,
    Fut: std::future::Future<Output = Result<Vec<T>>>,
{
    if start > end {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for (slice_start, slice_end) in window_slices(start, end, step_secs) {
        let chunk = load_slice_with_retry(&mut load_slice, slice_start, slice_end).await?;
        out.extend(chunk);
    }
    out.sort_by_key(|row| sort_key(row));
    Ok(dedup(out))
}

async fn load_slice_with_retry<T, F, Fut>(
    load_slice: &mut F,
    start: i32,
    end: i32,
) -> Result<Vec<T>>
where
    F: FnMut(i32, i32) -> Fut,
    Fut: std::future::Future<Output = Result<Vec<T>>>,
{
    let mut last_transient: Option<Error> = None;
    let mut empty_tries = 0u32;
    let mut transient_tries = 0u32;
    loop {
        match load_slice(start, end).await {
            Ok(chunk) if !chunk.is_empty() => return Ok(chunk),
            Ok(chunk) => {
                // Empty success clears a prior transient — do not revive it.
                let _ = last_transient.take();
                empty_tries += 1;
                if empty_tries > DEFAULT_EMPTY_RETRIES {
                    return Ok(chunk);
                }
                sleep_backoff(empty_tries.saturating_sub(1)).await;
            }
            Err(err) if is_transient(&err) => {
                last_transient = Some(err);
                transient_tries += 1;
                if transient_tries >= DEFAULT_TRANSIENT_RETRIES {
                    return Err(last_transient.take().expect("transient recorded"));
                }
                sleep_backoff(transient_tries.saturating_sub(1)).await;
            }
            Err(err) => return Err(err),
        }
    }
}

async fn sleep_backoff(attempt: u32) {
    let millis = 50u64.saturating_mul(u64::from(attempt + 1));
    tokio::time::sleep(Duration::from_millis(millis)).await;
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
    fn dedup_ticks_keeps_first() {
        let a = HistoryTickDto {
            symbol: Some("NQU6".into()),
            exchange: Some("CME".into()),
            open_price: None,
            high_price: None,
            low_price: None,
            close_price: Some(1.25),
            volume: None,
            num_trades: Some(1),
            ssboe: None,
            usecs: None,
            ts_event_ns: Some(10),
        };
        let mut b = a.clone();
        b.volume = Some(99);
        let out = dedup_ticks(vec![a.clone(), b, a.clone()]);
        // Same ts/price/trades but different volume → distinct rows.
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].volume, None);
        assert_eq!(out[1].volume, Some(99));
    }

    #[test]
    fn transient_classifies_connection_text() {
        assert!(is_transient(&Error::Rithmic("Forced logout".into())));
        assert!(!is_transient(&Error::Config("missing".into())));
    }

    #[test]
    fn daily_lookback_is_one_slice() {
        let step = bar_slice_secs(TimeBarType::DailyBar, 1);
        let start = 1_720_000_000;
        let end = start + 40 * 86_400;
        assert_eq!(window_slices(start, end, step).len(), 1);
    }

    #[test]
    fn unix_ymd_roundtrip_epoch() {
        assert_eq!(unix_to_yyyymmdd_utc(0), 19_700_101);
        assert_eq!(yyyymmdd_to_unix_utc(19_700_101), Some(0));
    }

    #[test]
    fn unix_ymd_roundtrip_sample() {
        let ymd = 20_260_813;
        let unix = yyyymmdd_to_unix_utc(ymd).expect("in range");
        assert_eq!(unix_to_yyyymmdd_utc(i64::from(unix)), ymd);
        assert_eq!(unix % 86_400, 0);
    }

    #[test]
    fn daily_replay_index_converts_unix() {
        let unix = yyyymmdd_to_unix_utc(20_260_704).expect("in range");
        assert_eq!(bar_replay_index(TimeBarType::DailyBar, unix), 20_260_704);
        // Unix seconds that numerically look like YYYYMMDD still convert (no heuristic).
        assert_eq!(
            bar_replay_index(TimeBarType::DailyBar, 20_200_101),
            unix_to_yyyymmdd_utc(20_200_101)
        );
        assert_eq!(bar_replay_index(TimeBarType::MinuteBar, unix), unix);
    }

    #[test]
    fn yyyymmdd_out_of_i32_range_is_none() {
        assert!(yyyymmdd_to_unix_utc(21_00_12_31).is_none());
    }

    #[test]
    fn daily_marker_to_ssboe_is_utc_midnight() {
        assert_eq!(
            marker_to_ssboe(20_260_812),
            yyyymmdd_to_unix_utc(20_260_812)
        );
        assert_eq!(marker_to_ssboe(1_720_000_000), Some(1_720_000_000));
    }
}
