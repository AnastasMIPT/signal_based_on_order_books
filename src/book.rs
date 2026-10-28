use std::collections::BTreeMap;

use serde::Deserialize;

use crate::fixed::Fixed;

/// One price level: `[price, quantity]`, exactly as exchanges encode it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub struct Level {
    pub price: Fixed,
    pub qty: Fixed,
}

impl Level {
    pub fn new(price: Fixed, qty: Fixed) -> Self {
        Self { price, qty }
    }
}

/// Full L2 book. Owned exclusively by its feed task, never shared.
#[derive(Debug, Default)]
pub struct OrderBook {
    bids: BTreeMap<Fixed, Fixed>,
    asks: BTreeMap<Fixed, Fixed>,
}

impl OrderBook {
    pub fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
    }

    /// Quantities are absolute; zero removes the level (removing an unknown level is a no-op).
    pub fn apply(&mut self, bids: &[Level], asks: &[Level]) {
        apply_side(&mut self.bids, bids);
        apply_side(&mut self.asks, asks);
    }

    pub fn best_bid(&self) -> Option<Level> {
        self.bids
            .iter()
            .next_back()
            .map(|(&p, &q)| Level::new(p, q))
    }

    pub fn best_ask(&self) -> Option<Level> {
        self.asks.iter().next().map(|(&p, &q)| Level::new(p, q))
    }

    pub fn is_crossed(&self) -> bool {
        matches!((self.best_bid(), self.best_ask()), (Some(b), Some(a)) if b.price >= a.price)
    }

    /// Best `n` bids, highest price first.
    pub fn top_bids(&self, n: usize) -> Vec<Level> {
        self.bids
            .iter()
            .rev()
            .take(n)
            .map(|(&p, &q)| Level::new(p, q))
            .collect()
    }

    /// Best `n` asks, lowest price first.
    pub fn top_asks(&self, n: usize) -> Vec<Level> {
        self.asks
            .iter()
            .take(n)
            .map(|(&p, &q)| Level::new(p, q))
            .collect()
    }

    pub fn depth(&self) -> (usize, usize) {
        (self.bids.len(), self.asks.len())
    }
}

fn apply_side(side: &mut BTreeMap<Fixed, Fixed>, levels: &[Level]) {
    for l in levels {
        if l.qty.is_zero() {
            side.remove(&l.price);
        } else {
            side.insert(l.price, l.qty);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lv(p: &str, q: &str) -> Level {
        Level::new(Fixed::parse(p).unwrap(), Fixed::parse(q).unwrap())
    }

    #[test]
    fn insert_update_remove() {
        let mut b = OrderBook::default();
        b.apply(&[lv("100", "1"), lv("99", "2")], &[lv("101", "3")]);
        assert_eq!(b.best_bid(), Some(lv("100", "1")));
        assert_eq!(b.best_ask(), Some(lv("101", "3")));

        b.apply(&[lv("100", "5")], &[]);
        assert_eq!(b.best_bid(), Some(lv("100", "5")));

        b.apply(&[lv("100", "0"), lv("50", "0")], &[]);
        assert_eq!(b.best_bid(), Some(lv("99", "2")));
        assert_eq!(b.depth(), (1, 1));
    }

    #[test]
    fn top_levels_are_ordered_from_touch() {
        let mut b = OrderBook::default();
        b.apply(
            &[lv("98", "1"), lv("100", "1"), lv("99", "1")],
            &[lv("103", "1"), lv("101", "1"), lv("102", "1")],
        );
        let bids: Vec<_> = b.top_bids(2).iter().map(|l| l.price.to_string()).collect();
        let asks: Vec<_> = b.top_asks(2).iter().map(|l| l.price.to_string()).collect();
        assert_eq!(bids, ["100", "99"]);
        assert_eq!(asks, ["101", "102"]);
    }

    #[test]
    fn detects_crossed_book() {
        let mut b = OrderBook::default();
        b.apply(&[lv("100", "1")], &[lv("101", "1")]);
        assert!(!b.is_crossed());
        b.apply(&[lv("101", "1")], &[]);
        assert!(b.is_crossed());
    }

    #[test]
    fn deserializes_level_from_pair() {
        let l: Level = serde_json::from_str(r#"["2352.05","2.760"]"#).unwrap();
        assert_eq!(l, lv("2352.05", "2.76"));
    }
}
