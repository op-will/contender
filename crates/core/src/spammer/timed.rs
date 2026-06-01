use std::pin::Pin;
use std::time::Duration;

use futures::Stream;
use futures::StreamExt;
use tokio::time::{interval, MissedTickBehavior};

use crate::generator::seeder::rand_seed::SeedGenerator;
use crate::{
    db::DbOps,
    generator::{templater::Templater, PlanConfig},
    test_scenario::TestScenario,
};

use super::spammer_trait::SpamRunContext;
use super::tx_callback::OnBatchSent;
use super::{OnTxSent, SpamTrigger, Spammer};

pub struct TimedSpammer {
    wait_interval: Duration,
    context: SpamRunContext,
}

impl TimedSpammer {
    pub fn new(wait_interval: Duration) -> Self {
        Self {
            wait_interval,
            context: SpamRunContext::new(),
        }
    }
}

impl<F, D, S, P> Spammer<F, D, S, P> for TimedSpammer
where
    F: OnTxSent + OnBatchSent + Send + Sync + 'static,
    D: DbOps + Send + Sync + 'static,
    S: SeedGenerator + Send + Sync + Clone,
    P: PlanConfig<String> + Templater<String> + Send + Sync + Clone,
{
    fn on_spam(
        &self,
        _scenario: &mut TestScenario<D, S, P>,
    ) -> impl std::future::Future<Output = crate::Result<Pin<Box<dyn Stream<Item = SpamTrigger> + Send>>>>
    {
        let wait_interval = self.wait_interval;
        async move {
            // Use tokio::time::interval for consistent timing that doesn't drift
            // even when batch processing takes variable time
            let mut tick_interval = interval(wait_interval);
            // Skip the first immediate tick - we want to wait before the first batch
            tick_interval.tick().await;
            // If processing takes longer than interval, delay the next tick
            // rather than bursting. Burst causes cascading delays because queued
            // ticks fire immediately, giving deferred task collections no
            // background processing time.
            tick_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

            Ok(
                futures::stream::unfold((0u64, tick_interval), |(tick, mut interval)| async move {
                    interval.tick().await;
                    Some((SpamTrigger::Tick(tick), (tick + 1, interval)))
                })
                .boxed(),
            )
        }
    }

    fn duration_units(periods: u64) -> crate::db::SpamDuration {
        crate::db::SpamDuration::Seconds(periods)
    }

    fn context(&self) -> &SpamRunContext {
        &self.context
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::{network::AnyNetwork, providers::ProviderBuilder};
    use contender_bundle_provider::bundle::BundleType;
    use std::sync::Arc;
    use std::time::Instant;
    use tokio::sync::OnceCell;
    use tokio_util::sync::CancellationToken;

    use crate::{
        db::MockDb,
        generator::{agent_pools::AgentSpec, util::test::spawn_anvil, RandSeed},
        test_scenario::{tests::MockConfig, TestScenario, TestScenarioParams},
    };

    static PROM: OnceCell<prometheus::Registry> = OnceCell::const_new();
    static HIST: OnceCell<prometheus::HistogramVec> = OnceCell::const_new();

    async fn mock_scenario() -> TestScenario<MockDb, RandSeed, MockConfig> {
        let anvil = spawn_anvil();
        let _provider = Arc::new(
            ProviderBuilder::new()
                .network::<AnyNetwork>()
                .connect_http(anvil.endpoint_url()),
        );
        let seed = RandSeed::seed_from_str("777777777777");
        TestScenario::new(
            MockConfig,
            MockDb.into(),
            seed,
            TestScenarioParams {
                rpc_url: anvil.endpoint_url(),
                builder_rpc_url: None,
                txs_rpc_url: None,
                signers: crate::util::default_signers(),
                agent_spec: AgentSpec::default(),
                tx_type: alloy::consensus::TxType::Legacy,
                bundle_type: BundleType::default(),
                pending_tx_timeout: Duration::from_secs(12),
                extra_msg_handles: None,
                sync_nonces_after_batch: true,
                rpc_batch_size: 0,
                gas_price: None,
                scenario_label: None,
                send_raw_tx_sync: false,
                flashblocks_ws_url: None,
            },
            None,
            (&PROM, &HIST).into(),
            &CancellationToken::new(),
        )
        .await
        .unwrap()
    }

    // The timed spammer's trigger stream must fire at its configured interval,
    // not assume a fixed one-second period. A 100ms interval should emit ~10
    // ticks in the time a 1000ms interval emits one, proving sub-second pacing.
    #[tokio::test]
    async fn ticks_fire_at_configured_sub_second_interval() {
        let mut scenario = mock_scenario().await;

        let spammer = TimedSpammer::new(Duration::from_millis(100));
        let stream = Spammer::<crate::spammer::NilCallback, MockDb, RandSeed, MockConfig>::on_spam(
            &spammer,
            &mut scenario,
        )
        .await
        .unwrap();

        let start = Instant::now();
        let ticks: Vec<_> = stream.take(5).collect().await;
        let elapsed = start.elapsed();

        assert_eq!(ticks.len(), 5);
        // 5 ticks at 100ms each: first tick after the initial wait, so ~5
        // intervals. Allow generous slack for scheduling, but it must be far
        // under the ~5s a one-second period would take.
        assert!(
            elapsed >= Duration::from_millis(450),
            "expected >=450ms for 5x100ms ticks, got {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_millis(2000),
            "5x100ms ticks took {elapsed:?}; pacing is not sub-second"
        );
    }
}
