//! Bounded recency map used by the fill dedup store and the untracked-status book.
//!
//! Every hit (`has`, `get`) refreshes recency, so keys that keep being observed
//! are not evicted by one-shot churn; inserting past the bound evicts the
//! least-recently-used entry first.

use std::collections::{BTreeMap, HashMap};

pub struct Lru<V> {
    entries: HashMap<String, (V, u64)>,
    by_recency: BTreeMap<u64, String>,
    clock: u64,
    max_size: usize,
}

impl<V> Lru<V> {
    pub fn new(max_size: usize) -> Self {
        Self {
            entries: HashMap::new(),
            by_recency: BTreeMap::new(),
            clock: 0,
            max_size,
        }
    }

    fn touch(&mut self, key: &str) {
        self.clock += 1;
        if let Some((_, tick)) = self.entries.get_mut(key) {
            self.by_recency.remove(tick);
            *tick = self.clock;
            self.by_recency.insert(self.clock, key.to_string());
        }
    }

    /// Whether `key` is present; refreshes recency on a hit.
    pub fn has(&mut self, key: &str) -> bool {
        let hit = self.entries.contains_key(key);
        if hit {
            self.touch(key);
        }
        hit
    }

    /// The stored value; refreshes recency on a hit.
    pub fn get(&mut self, key: &str) -> Option<&V> {
        if !self.entries.contains_key(key) {
            return None;
        }
        self.touch(key);
        self.entries.get(key).map(|(value, _)| value)
    }

    /// Record `key`, refresh recency, and evict the oldest past the bound.
    pub fn insert(&mut self, key: &str, value: V) {
        self.clock += 1;
        if let Some((_, old_tick)) = self.entries.insert(key.to_string(), (value, self.clock)) {
            self.by_recency.remove(&old_tick);
        }
        self.by_recency.insert(self.clock, key.to_string());
        while self.entries.len() > self.max_size {
            let Some((_, oldest)) = self.by_recency.pop_first() else {
                break;
            };
            self.entries.remove(&oldest);
        }
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.by_recency.clear();
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evicts_the_oldest_past_the_bound() {
        let mut lru = Lru::new(2);
        lru.insert("a", 1);
        lru.insert("b", 2);
        lru.insert("c", 3);
        assert!(!lru.has("a"));
        assert!(lru.has("b") && lru.has("c"));
        assert_eq!(lru.len(), 2);
    }

    #[test]
    fn a_hit_refreshes_recency() {
        let mut lru = Lru::new(2);
        lru.insert("a", 1);
        lru.insert("b", 2);
        assert!(lru.has("a")); // a is now newer than b
        lru.insert("c", 3);
        assert!(lru.has("a"));
        assert!(!lru.has("b"), "b was least recently used");
    }

    #[test]
    fn get_refreshes_and_reinsert_replaces_without_growing() {
        let mut lru = Lru::new(2);
        lru.insert("a", 1);
        lru.insert("b", 2);
        assert_eq!(lru.get("a"), Some(&1));
        lru.insert("b", 20);
        assert_eq!(lru.len(), 2);
        lru.insert("c", 3);
        assert!(!lru.has("a"), "a was older than the refreshed b");
        assert_eq!(lru.get("b"), Some(&20));
        assert_eq!(lru.get("missing"), None);
    }

    #[test]
    fn clear_empties_everything() {
        let mut lru = Lru::new(3);
        lru.insert("a", ());
        lru.clear();
        assert!(lru.is_empty());
        assert!(!lru.has("a"));
        lru.insert("a", ());
        assert_eq!(lru.len(), 1);
    }

    #[test]
    fn a_zero_bound_keeps_nothing() {
        let mut lru = Lru::new(0);
        lru.insert("a", 1);
        assert!(lru.is_empty());
    }
}
