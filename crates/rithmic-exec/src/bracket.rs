//! Exact bracket geometry over fixed-point prices, supplied by the boundary.

/// Signed distance from the entry fill (stop: adverse, target: favorable).
/// Initial exits must be strictly protective/profitable; adjustments may be
/// zero (breakeven) or negative (a stop locked into profit).
pub fn ticks(
    reference: i64,
    exit: i64,
    tick: i64,
    buy: bool,
    stop: bool,
    initial: bool,
) -> Option<i32> {
    if tick <= 0 {
        return None;
    }
    let delta = if buy == stop {
        reference.checked_sub(exit)?
    } else {
        exit.checked_sub(reference)?
    };
    if delta % tick != 0 {
        return None;
    }
    let ticks = i32::try_from(delta / tick).ok()?;
    if initial && ticks <= 0 {
        return None;
    }
    Some(ticks)
}

#[cfg(test)]
mod tests {
    use super::ticks;
    #[test]
    fn exact_signed_geometry() {
        assert_eq!(ticks(10000, 9900, 25, true, true, true), Some(4));
        assert_eq!(ticks(10000, 10100, 25, false, true, true), Some(4));
        assert_eq!(ticks(10000, 10100, 25, true, false, true), Some(4));
        assert_eq!(ticks(10000, 9900, 25, false, false, true), Some(4));
        assert_eq!(ticks(10000, 9901, 25, true, true, true), None);
        assert_eq!(ticks(10000, 10100, 25, true, true, true), None);
        assert_eq!(ticks(10000, 10000, 25, true, true, false), Some(0));
        assert_eq!(ticks(10000, 10025, 25, true, true, false), Some(-1));
        assert_eq!(ticks(0, i64::MAX, 1, false, true, true), None);
    }
}
