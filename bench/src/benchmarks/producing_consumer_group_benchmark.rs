use crate::args::common::IggyBenchArgs;
use crate::benchmarks::benchmark::{BenchmarkFutures, Benchmarkable};
use crate::benchmarks::common::*;
use crate::utils::finish_condition::BenchmarkFinishCondition;
use async_trait::async_trait;
use iggy_bench_report::benchmark_kind::BenchmarkKind;
use integration::test_server::ClientFactory;
use std::sync::Arc;
use tracing::info;

pub struct EndToEndProducingConsumerGroupBenchmark {
    args: Arc<IggyBenchArgs>,
    client_factory: Arc<dyn ClientFactory>,
}

impl EndToEndProducingConsumerGroupBenchmark {
    pub fn new(args: Arc<IggyBenchArgs>, client_factory: Arc<dyn ClientFactory>) -> Self {
        Self {
            args,
            client_factory,
        }
    }
}

#[async_trait]
impl Benchmarkable for EndToEndProducingConsumerGroupBenchmark {
    async fn run(&mut self) -> BenchmarkFutures {
        self.init_streams().await?;
        let cg_count = self.args.number_of_consumer_groups();
        let pc_count = self.args.producers();
        let cf = self.client_factory.clone();
        let data_per_pc = compute_messages_size(self.args.total_messages_size(), pc_count, 2);
        let batches = self.args.message_batches();
        let fc = BenchmarkFinishCondition::new(data_per_pc, batches);
        let rl = compute_rate_limit(self.args.rate_limit(), pc_count, 1);

        init_consumer_groups_if_needed(&cf, &self.args, cg_count).await?;

        info!(
            "Creating {pc_count} producing consumer(s) which are part of {cg_count} consumer group(s)"
        );

        build_producing_consumers_futures(cf, self.args.clone(), fc, rl, true)
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
