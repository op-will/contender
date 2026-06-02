use tokio::sync::watch;

use crate::generator::named_txs::ExecutionRequest;

/// Produces the [`ExecutionRequest`]s to send on a given spam tick. This is the
/// seam the spam loop pulls from: the uniform single-pool behavior and the
/// two-pool priority-ratio blend are two implementations of it.
pub trait BatchComposer: Send {
    /// Returns the batch for `tick` (0-based, the global tick index). Returning
    /// fewer than the nominal batch size is allowed (e.g. a stream ran dry).
    fn compose(&mut self, tick: u64) -> Vec<ExecutionRequest>;
}

/// Replays a fixed set of pre-built chunks round-robin: `chunk[tick % len]`.
/// This preserves the original single-pool spam behavior exactly.
pub struct ReplayComposer {
    chunks: Vec<Vec<ExecutionRequest>>,
}

impl ReplayComposer {
    pub fn new(chunks: Vec<Vec<ExecutionRequest>>) -> Self {
        Self { chunks }
    }
}

impl BatchComposer for ReplayComposer {
    fn compose(&mut self, tick: u64) -> Vec<ExecutionRequest> {
        if self.chunks.is_empty() {
            return vec![];
        }
        self.chunks[(tick as usize) % self.chunks.len()].clone()
    }
}

/// Blends two pre-generated, per-pool tx streams by a live priority ratio.
/// Normal traffic is a constant baseline: every tick draws a full `batch_size`
/// from the normal stream regardless of the slider. The slider only adds
/// priority traffic on top — each call also advances the priority cursor by
/// `M = round(batch_size * pct/100)`, so the per-tick total is `batch_size + M`
/// (at 100% that is `2 * batch_size`: full normal rate plus an equal priority
/// rate). This models a fixed normal load with the slider controlling the
/// *additional* priority load over the reserved-blockspace cap.
///
/// Cursors only ever advance forward. Nonces are assigned later by
/// `prepare_tx_request` from the live per-address nonce map (the streams' baked
/// nonces are not used), so blending is nonce-safe as long as the two pools use
/// disjoint from-addresses — which they do, since they are different pools.
pub struct PriorityRatioComposer {
    priority_txs: Vec<ExecutionRequest>,
    normal_txs: Vec<ExecutionRequest>,
    priority_cursor: usize,
    normal_cursor: usize,
    batch_size: usize,
    priority_pct: watch::Receiver<u8>,
}

impl PriorityRatioComposer {
    pub fn new(
        priority_txs: Vec<ExecutionRequest>,
        normal_txs: Vec<ExecutionRequest>,
        batch_size: usize,
        priority_pct: watch::Receiver<u8>,
    ) -> Self {
        Self {
            priority_txs,
            normal_txs,
            priority_cursor: 0,
            normal_cursor: 0,
            batch_size,
            priority_pct,
        }
    }

    /// `round(batch_size * pct / 100)`, with `pct` clamped to `[0, 100]`, so the
    /// result is always in `[0, batch_size]`. This is the count of *additional*
    /// priority txs added on top of the constant normal baseline.
    fn priority_count(batch_size: usize, pct: u8) -> usize {
        let pct = pct.min(100) as usize;
        ((batch_size * pct) + 50) / 100
    }
}

impl BatchComposer for PriorityRatioComposer {
    fn compose(&mut self, _tick: u64) -> Vec<ExecutionRequest> {
        let pct = *self.priority_pct.borrow();
        let want_priority = Self::priority_count(self.batch_size, pct);
        let want_normal = self.batch_size;

        let mut batch = take(&self.priority_txs, &mut self.priority_cursor, want_priority);
        batch.extend(take(&self.normal_txs, &mut self.normal_cursor, want_normal));
        batch
    }
}

/// Takes up to `n` requests from `src` starting at `*cursor`, advancing the
/// cursor past what it returned (clamped at the end of the slice).
fn take(src: &[ExecutionRequest], cursor: &mut usize, n: usize) -> Vec<ExecutionRequest> {
    let end = (*cursor + n).min(src.len());
    let slice = src[*cursor..end].to_vec();
    *cursor = end;
    slice
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generator::named_txs::NamedTxRequest;
    use alloy::primitives::Address;
    use alloy::rpc::types::TransactionRequest;
    use rstest::rstest;

    /// Builds a dummy request whose `from` encodes which pool/index it came from,
    /// so tests can assert exactly which txs a batch drew from each stream.
    fn req(tag: u64) -> ExecutionRequest {
        let tx = TransactionRequest {
            from: Some(Address::with_last_byte(tag as u8)),
            ..Default::default()
        };
        ExecutionRequest::Tx(Box::new(NamedTxRequest {
            name: Some(format!("tx-{tag}")),
            kind: None,
            tx,
        }))
    }

    fn stream(prefix: u64, n: usize) -> Vec<ExecutionRequest> {
        (0..n).map(|i| req(prefix + i as u64)).collect()
    }

    fn watch_pct(pct: u8) -> watch::Receiver<u8> {
        watch::channel(pct).1
    }

    #[test]
    fn replay_composer_round_robins_chunks() {
        let mut c = ReplayComposer::new(vec![stream(0, 1), stream(10, 1), stream(20, 1)]);
        let first_of = |b: &[ExecutionRequest]| match &b[0] {
            ExecutionRequest::Tx(t) => t.name.clone().unwrap(),
            _ => unreachable!(),
        };
        assert_eq!(first_of(&c.compose(0)), "tx-0");
        assert_eq!(first_of(&c.compose(1)), "tx-10");
        assert_eq!(first_of(&c.compose(2)), "tx-20");
        assert_eq!(first_of(&c.compose(3)), "tx-0");
    }

    #[test]
    fn replay_composer_empty_is_empty() {
        let mut c = ReplayComposer::new(vec![]);
        assert!(c.compose(0).is_empty());
    }

    #[rstest]
    #[case(0, 0)]
    #[case(50, 6)]
    #[case(100, 11)]
    #[case(64, 7)]
    fn priority_ratio_composer_adds_priority_on_top_of_full_normal(
        #[case] pct: u8,
        #[case] expect_priority: usize,
    ) {
        let batch = 11;
        let mut c = PriorityRatioComposer::new(
            stream(1000, batch * 4),
            stream(2000, batch * 4),
            batch,
            watch_pct(pct),
        );
        let out = c.compose(0);
        // Normal is a constant full-rate baseline; priority is added on top.
        assert_eq!(out.len(), batch + expect_priority);
        let from_priority = out
            .iter()
            .filter(|r| match r {
                ExecutionRequest::Tx(t) => t.name.as_deref().unwrap().starts_with("tx-1"),
                _ => false,
            })
            .count();
        assert_eq!(from_priority, expect_priority);
        assert_eq!(c.priority_cursor, expect_priority);
        assert_eq!(c.normal_cursor, batch);
    }

    #[test]
    fn priority_ratio_composer_clamps_over_100() {
        assert_eq!(PriorityRatioComposer::priority_count(11, 200), 11);
    }

    #[test]
    fn priority_ratio_composer_drains_gracefully() {
        let batch = 5;
        // Only 3 priority txs available but pct=100 wants 5; priority drains to
        // the 3 remaining without panicking, while normal still supplies its
        // full baseline of 5 — so the batch is 3 + 5 = 8.
        let mut c =
            PriorityRatioComposer::new(stream(0, 3), stream(100, 100), batch, watch_pct(100));
        let out = c.compose(0);
        assert_eq!(out.len(), 8);
    }
}
