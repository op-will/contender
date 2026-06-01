use std::sync::atomic::AtomicBool;
use std::time::Duration;
use std::{pin::Pin, sync::Arc};

use contender_engine_provider::DEFAULT_BLOCK_TIME;
use futures::Stream;
use futures::StreamExt;
use tracing::info;

use super::tx_callback::OnBatchSent;
use super::OnTxSent;
use super::SpamTrigger;
use crate::db::SpamDuration;
use crate::spammer::CallbackError;
use crate::{
    db::DbOps,
    generator::{seeder::rand_seed::SeedGenerator, templater::Templater, PlanConfig},
    test_scenario::TestScenario,
    Result,
};

#[derive(Clone)]
pub struct SpamRunContext {
    done_sending: Arc<AtomicBool>,
    done_fcu: Arc<AtomicBool>,
}

impl SpamRunContext {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for SpamRunContext {
    fn default() -> Self {
        Self {
            done_sending: Arc::new(AtomicBool::new(false)),
            done_fcu: Arc::new(AtomicBool::new(false)),
        }
    }
}

pub trait Spammer<F, D, S, P>
where
    F: OnTxSent + OnBatchSent + Send + Sync + 'static,
    D: DbOps + Send + Sync + 'static,
    S: SeedGenerator + Send + Sync + Clone,
    P: PlanConfig<String> + Templater<String> + Send + Sync + Clone,
{
    fn context(&self) -> &SpamRunContext;

    fn on_spam(
        &self,
        scenario: &mut TestScenario<D, S, P>,
    ) -> impl std::future::Future<Output = Result<Pin<Box<dyn Stream<Item = SpamTrigger> + Send>>>>;

    fn duration_units(periods: u64) -> SpamDuration;

    fn spam_rpc(
        &self,
        scenario: &mut TestScenario<D, S, P>,
        txs_per_period: u64,
        num_periods: u64,
        run_id: Option<u64>,
        sent_tx_callback: Arc<F>,
    ) -> impl std::future::Future<Output = crate::Result<()>> {
        async move {
            let tx_req_chunks = scenario
                .get_spam_tx_chunks(txs_per_period, num_periods)
                .await?;
            let mut composer = crate::spammer::ReplayComposer::new(tx_req_chunks);
            self.run_with_composer(
                scenario,
                &mut composer,
                num_periods,
                run_id,
                sent_tx_callback,
            )
            .await
        }
    }

    /// Like [`Self::spam_rpc`], but blends two pre-generated pool streams by a
    /// live `priority_pct` (0..=100) on each tick. Used by the control server's
    /// priority-ratio runs. `batch_size` is the per-tick tx count (`txs_per_period`).
    #[allow(clippy::too_many_arguments)]
    fn spam_rpc_priority_ratio(
        &self,
        scenario: &mut TestScenario<D, S, P>,
        batch_size: u64,
        num_periods: u64,
        priority_txs: Vec<crate::generator::named_txs::ExecutionRequest>,
        normal_txs: Vec<crate::generator::named_txs::ExecutionRequest>,
        priority_pct: tokio::sync::watch::Receiver<u8>,
        run_id: Option<u64>,
        sent_tx_callback: Arc<F>,
    ) -> impl std::future::Future<Output = crate::Result<()>> {
        async move {
            let mut composer = crate::spammer::PriorityRatioComposer::new(
                priority_txs,
                normal_txs,
                batch_size as usize,
                priority_pct,
            );
            self.run_with_composer(
                scenario,
                &mut composer,
                num_periods,
                run_id,
                sent_tx_callback,
            )
            .await
        }
    }

    /// Shared run body: spawns the FCU background task, drives `execute_spammer`
    /// with the given composer for `num_periods` ticks, then finalizes (latency
    /// metrics, nonce sync). Both `spam_rpc` and `spam_rpc_priority_ratio` use it.
    fn run_with_composer(
        &self,
        scenario: &mut TestScenario<D, S, P>,
        composer: &mut dyn super::BatchComposer,
        num_periods: u64,
        run_id: Option<u64>,
        sent_tx_callback: Arc<F>,
    ) -> impl std::future::Future<Output = crate::Result<()>> {
        async move {
            let run_id = run_id.unwrap_or(scenario.db.num_runs().map_err(|e| e.into())?);
            let is_fcu_done = self.context().done_fcu.clone();
            let is_sending_done = self.context().done_sending.clone();
            let auth_provider = scenario.auth_provider.clone();

            // run loop in background to call fcu when spamming is done
            let fcu_handle: tokio::task::JoinHandle<Result<()>> =
                crate::spawn_with_session(async move {
                    if let Some(auth_client) = &auth_provider {
                        loop {
                            let fcu_done = is_fcu_done.load(std::sync::atomic::Ordering::SeqCst);
                            let sending_done =
                                is_sending_done.load(std::sync::atomic::Ordering::SeqCst);
                            if fcu_done {
                                info!("FCU is done, stopping block production...");
                                break;
                            }
                            if sending_done {
                                auth_client
                                    .advance_chain(DEFAULT_BLOCK_TIME)
                                    .await
                                    .map_err(|e| {
                                        is_fcu_done
                                            .store(true, std::sync::atomic::Ordering::SeqCst);
                                        CallbackError::AuthProvider(e)
                                    })?;
                            } else {
                                tokio::time::sleep(Duration::from_secs(1)).await;
                            }
                        }
                    }
                    Ok(())
                });

            let mut cursor = self.on_spam(scenario).await?.take(num_periods as usize);

            scenario
                .execute_spammer(&mut cursor, composer, sent_tx_callback)
                .await?;
            self.context()
                .done_sending
                .store(true, std::sync::atomic::Ordering::SeqCst);

            fcu_handle.await.map_err(CallbackError::Join)??;
            self.context()
                .done_fcu
                .store(true, std::sync::atomic::Ordering::SeqCst);

            let latency_metrics = scenario.collect_latency_metrics();
            scenario
                .db
                .insert_latency_metrics(run_id, &latency_metrics)
                .map_err(|e| e.into())?;

            scenario.sync_nonces().await?;

            info!("done. run_id: {run_id}");
            Ok(())
        }
    }
}
