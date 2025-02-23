use crate::args::common::IggyBenchArgs;
use crate::benchmarks::benchmark::{BenchmarkFutures, Benchmarkable};
use crate::benchmarks::common::*;
use crate::utils::finish_condition::BenchmarkFinishCondition;
use async_trait::async_trait;
use iggy_bench_report::benchmark_kind::BenchmarkKind;
use integration::test_server::ClientFactory;
use std::sync::Arc;
use tracing::info;

pub struct EndToEndProducingConsumerBenchmark {
    args: Arc<IggyBenchArgs>,
    client_factory: Arc<dyn ClientFactory>,
}

impl EndToEndProducingConsumerBenchmark {
    pub fn new(args: Arc<IggyBenchArgs>, client_factory: Arc<dyn ClientFactory>) -> Self {
        Self {
            args,
            client_factory,
        }
    }
}

#[async_trait]
impl Benchmarkable for EndToEndProducingConsumerBenchmark {
    async fn run(&mut self) -> BenchmarkFutures {
        self.init_streams().await?;
        let cf = self.client_factory.clone();
        let pc_count = self.args.producers();
        let batches = self.args.message_batches();
        let total_data = self.args.total_messages_size();
        let total_data_per_pc = compute_messages_size(total_data, pc_count, 2);
        let fc = BenchmarkFinishCondition::new(total_data_per_pc, batches);
        let rl = compute_rate_limit(self.args.rate_limit(), pc_count, 1);

        info!("Creating {} producing consumer(s)", pc_count);

        build_producing_consumers_futures(cf, self.args.clone(), fc, rl, false)
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
