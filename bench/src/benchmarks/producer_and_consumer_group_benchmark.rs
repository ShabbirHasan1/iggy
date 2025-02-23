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

pub struct ProducerAndConsumerGroupBenchmark {
    args: Arc<IggyBenchArgs>,
    client_factory: Arc<dyn ClientFactory>,
}

impl ProducerAndConsumerGroupBenchmark {
    pub fn new(args: Arc<IggyBenchArgs>, client_factory: Arc<dyn ClientFactory>) -> Self {
        Self {
            args,
            client_factory,
        }
    }
}

#[async_trait]
impl Benchmarkable for ProducerAndConsumerGroupBenchmark {
    async fn run(&mut self) -> BenchmarkFutures {
        self.init_streams().await?;
        let cg_count = self.args.number_of_consumer_groups();
        let cf = &self.client_factory;
        let streams = self.args.streams();
        let batches = self.args.message_batches();
        let partitions_count = self.args.number_of_partitions();
        let producers = self.args.producers();
        let consumers = self.args.consumers();
        let total_data = self.args.total_messages_size();
        let producers_rl = compute_rate_limit(self.args.rate_limit(), producers, 2);
        let consumers_rl = compute_rate_limit(self.args.rate_limit(), consumers, 2);
        let total_data_per_producer = compute_messages_size(total_data, producers, 2);
        let total_data_per_consumer = compute_messages_size(total_data, consumers, 2);
        let producers_fc = BenchmarkFinishCondition::new(total_data_per_producer, batches);
        let consumers_fc = BenchmarkFinishCondition::new(total_data_per_consumer, batches);

        init_consumer_groups_if_needed(cf, &self.args, cg_count).await?;

        let mut all_futures = Vec::with_capacity((producers + consumers) as usize);

        let mut producer_futures = build_producer_futures(
            self.client_factory.clone(),
            self.args.clone(),
            producers,
            streams,
            partitions_count,
            producers_fc,
            producers_rl,
        )?;

        all_futures.append(&mut producer_futures);

        info!("Created {} producer(s).", producers);

        let mut consumer_futures =
            build_consumer_futures(cf, self.args.clone(), consumers_fc, consumers_rl, true)?;

        all_futures.append(&mut consumer_futures);

        info!("Starting producer and consumer group benchmark...");
        Ok(all_futures)
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
