use crate::args::common::IggyBenchArgs;
use crate::benchmarks::benchmark::{BenchmarkFutures, Benchmarkable};
use crate::benchmarks::common::*;
use crate::utils::finish_condition::BenchmarkFinishCondition;
use async_trait::async_trait;
use iggy_bench_report::benchmark_kind::BenchmarkKind;
use integration::test_server::ClientFactory;
use std::sync::Arc;
use tracing::info;

pub struct ConsumerBenchmark {
    args: Arc<IggyBenchArgs>,
    client_factory: Arc<dyn ClientFactory>,
}

impl ConsumerBenchmark {
    pub fn new(args: Arc<IggyBenchArgs>, client_factory: Arc<dyn ClientFactory>) -> Self {
        Self {
            args,
            client_factory,
        }
    }
}

#[async_trait]
impl Benchmarkable for ConsumerBenchmark {
    async fn run(&mut self) -> BenchmarkFutures {
        self.check_streams().await?;
        let cf = &self.client_factory;
        let total_data = self.args.total_messages_size();
        let batches = self.args.message_batches();
        let consumers_count = self.args.consumers();
        let rl = compute_rate_limit(self.args.rate_limit(), consumers_count, 1);
        let messages_size = compute_messages_size(total_data, consumers_count, 1);
        let fc = BenchmarkFinishCondition::new(messages_size, batches);

        info!("Created {} consumer(s).", consumers_count);

        build_consumer_futures(cf, self.args.clone(), fc, rl, false)
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
