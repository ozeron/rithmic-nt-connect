//! Order-book update types (the venue `OrderBook.update_type` closed set).

/// What one venue order-book message is, by its `update_type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookUpdate {
    /// `CLEAR_ORDER_BOOK` (1): drop every level.
    Clear,
    /// `NO_BOOK` (2): the instrument has no book; drop every level.
    NoBook,
    /// `SNAPSHOT_IMAGE` (3): the levels are the whole book.
    Snapshot,
    /// `BEGIN` (4): first part of a multi-message update.
    Begin,
    /// `MIDDLE` (5): a middle part of a multi-message update.
    Middle,
    /// `END` (6): the last part of a multi-message update.
    End,
    /// `SOLO` (7): a complete update in one message.
    Solo,
}

impl BookUpdate {
    /// The update type for a venue value; `None` for a value outside the set
    /// (the caller rejects it rather than guess).
    pub fn from_wire(value: i32) -> Option<Self> {
        Some(match value {
            1 => Self::Clear,
            2 => Self::NoBook,
            3 => Self::Snapshot,
            4 => Self::Begin,
            5 => Self::Middle,
            6 => Self::End,
            7 => Self::Solo,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Clear => "clear",
            Self::NoBook => "no_book",
            Self::Snapshot => "snapshot",
            Self::Begin => "begin",
            Self::Middle => "middle",
            Self::End => "end",
            Self::Solo => "solo",
        }
    }
}

/// The update kind name for a venue `update_type`, or `None` when unknown.
pub fn book_update_kind(value: i32) -> Option<&'static str> {
    BookUpdate::from_wire(value).map(BookUpdate::as_str)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_venue_value_maps() {
        let names: Vec<_> = (1..=7).map(|v| book_update_kind(v).unwrap()).collect();
        assert_eq!(
            names,
            ["clear", "no_book", "snapshot", "begin", "middle", "end", "solo"]
        );
    }

    #[test]
    fn values_outside_the_set_are_unknown() {
        for v in [0, 8, -1, 99] {
            assert_eq!(book_update_kind(v), None, "{v}");
        }
    }
}
