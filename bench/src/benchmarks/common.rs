use super::benchmark::BenchmarkFutures;
use super::*;
use crate::{
    actors::{consumer::Consumer, producer::Producer, producing_consumer::ProducingConsumer},
    args::common::IggyBenchArgs,
    utils::finish_condition::BenchmarkFinishCondition,
};
use futures::FutureExt;
use iggy::{
    client::ConsumerGroupClient, clients::client::IggyClient, error::IggyError,
    messages::poll_messages::PollingKind, utils::byte_size::IggyByteSize,
};
use integration::test_server::{login_root, ClientFactory};
use std::sync::Arc;
use tracing::{error, info};

pub fn compute_rate_limit(
    total_rate: Option<IggyByteSize>,
    actors: u32,
    factor: u64,
) -> Option<IggyByteSize> {
    total_rate.map(|rate_limit| (rate_limit.as_bytes_u64() / factor / (actors as u64)).into())
}

pub fn compute_messages_size(
    total_size: Option<IggyByteSize>,
    actors: u32,
    factor: u64,
) -> Option<IggyByteSize> {
    total_size.map(|sz| (sz.as_bytes_u64() / factor / (actors as u64)).into())
}

pub async fn init_consumer_groups_if_needed(
    client_factory: &Arc<dyn ClientFactory>,
    args: &IggyBenchArgs,
    consumer_groups_count: u32,
) -> Result<(), IggyError> {
    let start_stream_id = args.start_stream_id();
    let topic_id: u32 = 1;
    let client = client_factory.create_client().await;
    let client = IggyClient::create(client, None, None);
    login_root(&client).await;
    for i in 1..=consumer_groups_count {
        let consumer_group_id = CONSUMER_GROUP_BASE_ID + i;
        let stream_id = start_stream_id + i;
        let consumer_group_name = format!("{}-{}", CONSUMER_GROUP_NAME_PREFIX, consumer_group_id);
        info!(
            "Creating test consumer group: name={}, id={}, stream={}, topic={}",
            consumer_group_name, consumer_group_id, stream_id, topic_id
        );
        match client
            .create_consumer_group(
                &stream_id.try_into().unwrap(),
                &topic_id.try_into().unwrap(),
                &consumer_group_name,
                Some(consumer_group_id),
            )
            .await
        {
            Err(IggyError::ConsumerGroupIdAlreadyExists(_, _)) => {
                info!(
                    "Consumer group with id {} already exists",
                    consumer_group_id
                );
            }
            Err(err) => {
                error!("Error when creating consumer group: {err}");
            }
            Ok(_) => {}
        }
    }
    Ok(())
}

pub fn build_producer_futures(
    client_factory: Arc<dyn ClientFactory>,
    args: Arc<IggyBenchArgs>,
    producers_count: u32,
    stream_offset: u32,
    partitions_count: u32,
    finish_condition: Arc<BenchmarkFinishCondition>,
    rate_limit: Option<IggyByteSize>,
) -> BenchmarkFutures {
    let mut futures = Vec::with_capacity(producers_count as usize);
    let warmup_time = args.warmup_time();
    let messages_per_batch = args.messages_per_batch();
    let message_size = args.message_size();
    for producer_id in 1..=producers_count {
        let client_factory = client_factory.clone();
        let args = args.clone();
        let finish_condition = finish_condition.clone();
        let stream_id = args.start_stream_id() + stream_offset + (producer_id % args.streams());
        let fut = async move {
            let producer = Producer::new(
                client_factory,
                args.kind(),
                producer_id,
                stream_id,
                partitions_count,
                messages_per_batch,
                message_size,
                finish_condition,
                warmup_time,
                args.sampling_time(),
                args.moving_average_window(),
                rate_limit,
            );
            producer.run().await
        }
        .boxed();
        futures.push(fut);
    }
    Ok(futures)
}

pub fn build_consumer_futures(
    client_factory: &Arc<dyn ClientFactory>,
    args: Arc<IggyBenchArgs>,
    finish_condition: Arc<BenchmarkFinishCondition>,
    rate_limit: Option<IggyByteSize>,
    use_consumer_groups: bool,
) -> BenchmarkFutures {
    let consumers = args.consumers();
    let mut futures = Vec::with_capacity(consumers as usize);
    let warmup_time = args.warmup_time();
    let messages_per_batch = args.messages_per_batch();
    let polling_kind = if use_consumer_groups {
        PollingKind::Next
    } else {
        PollingKind::Offset
    };
    for consumer_id in 1..=consumers {
        let client_factory = client_factory.clone();
        let args = args.clone();
        let finish_condition = finish_condition.clone();
        let stream_id = if use_consumer_groups {
            args.start_stream_id() + 1 + (consumer_id % args.number_of_consumer_groups())
        } else {
            args.start_stream_id() + consumer_id
        };
        let consumer_group_id = if use_consumer_groups {
            Some(CONSUMER_GROUP_BASE_ID + (consumer_id % args.number_of_consumer_groups()))
        } else {
            None
        };
        let fut = async move {
            let consumer = Consumer::new(
                client_factory,
                args.kind(),
                consumer_id,
                consumer_group_id,
                stream_id,
                messages_per_batch,
                finish_condition,
                warmup_time,
                args.sampling_time(),
                args.moving_average_window(),
                polling_kind,
                rate_limit,
            );
            consumer.run().await
        }
        .boxed();
        futures.push(fut);
    }
    Ok(futures)
}

#[allow(clippy::too_many_arguments)]
pub fn build_producing_consumers_futures(
    client_factory: Arc<dyn ClientFactory>,
    args: Arc<IggyBenchArgs>,
    finish_condition: Arc<BenchmarkFinishCondition>,
    rate_limit: Option<IggyByteSize>,
    use_consumer_groups: bool,
) -> BenchmarkFutures {
    let producing_consumers = args.producers();
    let streams = args.streams();
    let mut futures = Vec::with_capacity(producing_consumers as usize);
    let partitions = args.number_of_partitions();
    let cg_count = args.number_of_consumer_groups();
    let warmup_time = args.warmup_time();
    let messages_per_batch = args.messages_per_batch();
    let message_size = args.message_size();
    let start_stream_id = args.start_stream_id();
    let start_consumer_group_id = CONSUMER_GROUP_BASE_ID;

    for actor_id in 1..=producing_consumers {
        let client_factory_clone = client_factory.clone();
        let args_clone = args.clone();
        let finish_clone = finish_condition.clone();
        let consumer_group_id = if use_consumer_groups {
            Some(start_consumer_group_id + 1 + (actor_id % cg_count))
        } else {
            None
        };
        let stream_id = if use_consumer_groups {
            start_stream_id + 1 + (actor_id % cg_count)
        } else {
            start_stream_id + 1 + (actor_id % streams)
        };
        let polling_kind = if use_consumer_groups {
            PollingKind::Next
        } else {
            PollingKind::Offset
        };
        let fut = async move {
            info!(
                "Executing producing consumer #{}{} stream_id={}",
                actor_id,
                if use_consumer_groups {
                    format!(" in group={}", consumer_group_id.unwrap())
                } else {
                    "".to_string()
                },
                stream_id
            );
            let actor = ProducingConsumer::new(
                client_factory_clone,
                args_clone.kind(),
                actor_id,
                consumer_group_id,
                stream_id,
                partitions,
                messages_per_batch,
                message_size,
                finish_clone,
                warmup_time,
                args_clone.sampling_time(),
                args_clone.moving_average_window(),
                rate_limit,
                polling_kind,
            );
            actor.run().await
        }
        .boxed();
        futures.push(fut);
    }
    Ok(futures)
}
