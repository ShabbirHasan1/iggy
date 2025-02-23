use crate::args::common::IggyBenchArgs;
use crate::benchmarks::benchmark::{BenchmarkFutures, Benchmarkable};
use crate::benchmarks::common::*;
use crate::utils::finish_condition::BenchmarkFinishCondition;
use async_trait::async_trait;
use iggy_bench_report::benchmark_kind::BenchmarkKind;
use integration::test_server::ClientFactory;
use std::sync::Arc;
use tracing::info;

pub struct ProducerAndConsumerBenchmark {
    args: Arc<IggyBenchArgs>,
    client_factory: Arc<dyn ClientFactory>,
}

impl ProducerAndConsumerBenchmark {
    pub fn new(args: Arc<IggyBenchArgs>, client_factory: Arc<dyn ClientFactory>) -> Self {
        Self {
            args,
            client_factory,
        }
    }
}

#[async_trait]
impl Benchmarkable for ProducerAndConsumerBenchmark {
    async fn run(&mut self) -> BenchmarkFutures {
        self.init_streams().await?;
        let cf = &self.client_factory;
        let msg_size = self.args.total_messages_size();
        let batches = self.args.message_batches();
        let producers = self.args.producers();
        let consumers = self.args.consumers();
        let producers_rl = compute_rate_limit(self.args.rate_limit(), producers, 2);
        let consumers_rl = compute_rate_limit(self.args.rate_limit(), consumers, 2);
        let total_data_producers = compute_messages_size(msg_size, producers, 2);
        let total_data_consumers = compute_messages_size(msg_size, consumers, 2);
        let producers_fc = BenchmarkFinishCondition::new(total_data_producers, batches);
        let consumers_fc = BenchmarkFinishCondition::new(total_data_consumers, batches);

        let mut futures_vec = build_producer_futures(
            self.client_factory.clone(),
            self.args.clone(),
            producers,
            1,
            self.args.number_of_partitions(),
            producers_fc,
            producers_rl,
        )?;

        let mut consumer_futures =
            build_consumer_futures(cf, self.args.clone(), consumers_fc, consumers_rl, false)?;

        futures_vec.append(&mut consumer_futures);

        info!("Starting producer & consumer benchmark...");

        Ok(futures_vec)
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
