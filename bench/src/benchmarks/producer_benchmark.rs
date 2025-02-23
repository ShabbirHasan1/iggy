use crate::args::common::IggyBenchArgs;
use crate::benchmarks::benchmark::{BenchmarkFutures, Benchmarkable};
use crate::benchmarks::common::*;
use crate::utils::finish_condition::BenchmarkFinishCondition;
use async_trait::async_trait;
use iggy_bench_report::benchmark_kind::BenchmarkKind;
use integration::test_server::ClientFactory;
use std::sync::Arc;
use tracing::info;

pub struct ProducerBenchmark {
    args: Arc<IggyBenchArgs>,
    client_factory: Arc<dyn ClientFactory>,
}

impl ProducerBenchmark {
    pub fn new(args: Arc<IggyBenchArgs>, client_factory: Arc<dyn ClientFactory>) -> Self {
        Self {
            args,
            client_factory,
        }
    }
}

#[async_trait]
impl Benchmarkable for ProducerBenchmark {
    async fn run(&mut self) -> BenchmarkFutures {
        self.init_streams().await?;
        let producers = self.args.producers();
        let total_data = self.args.total_messages_size();
        let batches = self.args.message_batches();
        let bytes_per_producer = compute_messages_size(total_data, producers, 1);
        let rl = compute_rate_limit(self.args.rate_limit(), producers, 1);
        let fc = BenchmarkFinishCondition::new(bytes_per_producer, batches);

        let futures_vec = build_producer_futures(
            self.client_factory.clone(),
            self.args.clone(),
            producers,
            1,
            self.args.number_of_partitions(),
            fc,
            rl,
        );

        info!("Created {} producer(s).", producers);
        futures_vec
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
