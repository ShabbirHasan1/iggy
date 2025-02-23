use super::benchmark::{BenchmarkFutures, Benchmarkable};
use crate::{
    args::common::IggyBenchArgs, benchmarks::common::*,
    utils::finish_condition::BenchmarkFinishCondition,
};
use async_trait::async_trait;
use iggy_bench_report::benchmark_kind::BenchmarkKind;
use integration::test_server::ClientFactory;
use std::sync::Arc;
use tracing::info;

pub struct ConsumerGroupBenchmark {
    args: Arc<IggyBenchArgs>,
    client_factory: Arc<dyn ClientFactory>,
}

impl ConsumerGroupBenchmark {
    pub fn new(args: Arc<IggyBenchArgs>, client_factory: Arc<dyn ClientFactory>) -> Self {
        Self {
            args,
            client_factory,
        }
    }
}

#[async_trait]
impl Benchmarkable for ConsumerGroupBenchmark {
    async fn run(&mut self) -> BenchmarkFutures {
        self.check_streams().await?;
        let cf = &self.client_factory;
        let cg_count = self.args.number_of_consumer_groups();
        let consumers = self.args.consumers();
        let rl = compute_rate_limit(self.args.rate_limit(), consumers, 1);
        let total_data = self.args.total_messages_size();
        let batches = self.args.message_batches();
        let fc = BenchmarkFinishCondition::new(total_data, batches);

        init_consumer_groups_if_needed(cf, &self.args, cg_count).await?;

        info!("Starting consumer group benchmark with {consumers} consumers in {cg_count} group(s)...");

        build_consumer_futures(cf, self.args.clone(), fc, rl, true)
    }

    fn kind(&self) -> BenchmarkKind {
        self.args.kind()
    }

    fn args(&self) -> &IggyBenchArgs {
        &self.args
    }

    fn client_factory(&self) -> &Arc<dyn ClientFactory> {
        &self.client_factory
    }
}
