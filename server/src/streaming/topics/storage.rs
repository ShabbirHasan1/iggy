use crate::state::states::TopicState;
use crate::streaming::partitions::partition::Partition;
use crate::streaming::storage::TopicStorage;
use crate::streaming::topics::consumer_group::ConsumerGroup;
use crate::streaming::topics::topic::Topic;
use anyhow::Context;
use async_trait::async_trait;
use futures::future::join_all;
use iggy::compression::compression_algorithm::CompressionAlgorithm;
use iggy::error::IggyError;
use iggy::locking::IggySharedMut;
use iggy::locking::IggySharedMutFn;
use iggy::utils::byte_size::IggyByteSize;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use tokio::fs;
use tokio::fs::create_dir;
use tokio::sync::{Mutex, RwLock};
use tracing::{error, info};

#[derive(Debug)]
pub struct FileTopicStorage;

unsafe impl Send for FileTopicStorage {}
unsafe impl Sync for FileTopicStorage {}

#[derive(Debug, Serialize, Deserialize)]
struct ConsumerGroupData {
    id: u32,
    name: String,
}

#[async_trait]
impl TopicStorage for FileTopicStorage {
    async fn load(&self, topic: &mut Topic, state: TopicState) -> Result<(), IggyError> {
        info!("Loading topic {} from disk...", topic);
        if !Path::new(&topic.path).exists() {
            return Err(IggyError::TopicIdNotFound(topic.topic_id, topic.stream_id));
        }

        topic.created_at = state.created_at.to_micros();
        topic.message_expiry = state.message_expiry;
        topic.compression_algorithm = state.compression_algorithm;
        topic.max_topic_size = state.max_topic_size;
        topic.replication_factor = state.replication_factor.unwrap_or(1);

        for consumer_group in state.consumer_groups.into_values() {
            let consumer_group = ConsumerGroup::new(
                topic.topic_id,
                consumer_group.id,
                &consumer_group.name,
                topic.get_partitions_count(),
            );
            topic
                .consumer_groups
                .insert(consumer_group.group_id, RwLock::new(consumer_group));
        }

        let dir_entries = fs::read_dir(&topic.partitions_path).await
            .with_context(|| format!("Failed to read partition with ID: {} for stream with ID: {} for topic with ID: {} and path: {}",
                                     topic.topic_id, topic.stream_id, topic.topic_id, &topic.partitions_path));
        if let Err(err) = dir_entries {
            return Err(IggyError::CannotReadPartitions(err));
        }

        let mut unloaded_partitions = Vec::new();
        let mut dir_entries = dir_entries.unwrap();
        while let Some(dir_entry) = dir_entries.next_entry().await.unwrap_or(None) {
            let metadata = dir_entry.metadata().await;
            if metadata.is_err() || metadata.unwrap().is_file() {
                continue;
            }

            let name = dir_entry.file_name().into_string().unwrap();
            let partition_id = name.parse::<u32>();
            if partition_id.is_err() {
                error!("Invalid partition ID file with name: '{}'.", name);
                continue;
            }

            let partition_id = partition_id.unwrap();
            let partition = Partition::create(
                topic.stream_id,
                topic.topic_id,
                partition_id,
                false,
                topic.config.clone(),
                topic.storage.clone(),
                topic.message_expiry,
                topic.messages_count_of_parent_stream.clone(),
                topic.messages_count.clone(),
                topic.size_of_parent_stream.clone(),
                topic.size_bytes.clone(),
                topic.segments_count_of_parent_stream.clone(),
            );
            unloaded_partitions.push(partition);
        }

        let stream_id = topic.stream_id;
        let topic_id = topic.topic_id;
        let loaded_partitions = Arc::new(Mutex::new(Vec::new()));
        let mut load_partitions = Vec::new();
        for mut partition in unloaded_partitions {
            let loaded_partitions = loaded_partitions.clone();
            let load_partition = tokio::spawn(async move {
                match partition.load().await {
                    Ok(_) => {
                        loaded_partitions.lock().await.push(partition);
                    }
                    Err(error) => {
                        error!(
                            "Failed to load partition with ID: {} for stream with ID: {stream_id} and topic with ID: {topic_id}. Error: {error}",
                            partition.partition_id);
                    }
                }
            });
            load_partitions.push(load_partition);
        }

        join_all(load_partitions).await;
        for partition in loaded_partitions.lock().await.drain(..) {
            topic
                .partitions
                .insert(partition.partition_id, IggySharedMut::new(partition));
        }

        topic.load_messages_from_disk_to_cache().await?;

        info!("Loaded topic {topic}");

        Ok(())
    }

    async fn save(&self, topic: &Topic) -> Result<(), IggyError> {
        if !Path::new(&topic.path).exists() && create_dir(&topic.path).await.is_err() {
            return Err(IggyError::CannotCreateTopicDirectory(
                topic.topic_id,
                topic.stream_id,
                topic.path.clone(),
            ));
        }

        if !Path::new(&topic.partitions_path).exists()
            && create_dir(&topic.partitions_path).await.is_err()
        {
            return Err(IggyError::CannotCreatePartitionsDirectory(
                topic.stream_id,
                topic.topic_id,
            ));
        }

        info!(
            "Saving {} partition(s) for topic {topic}...",
            topic.partitions.len()
        );
        for (_, partition) in topic.partitions.iter() {
            let partition = partition.write().await;
            partition.persist().await?;
        }

        info!("Saved topic {topic}");

        Ok(())
    }

    async fn delete(&self, topic: &Topic) -> Result<(), IggyError> {
        info!("Deleting topic {topic}...");
        if fs::remove_dir_all(&topic.path).await.is_err() {
            return Err(IggyError::CannotDeleteTopicDirectory(
                topic.topic_id,
                topic.stream_id,
                topic.path.clone(),
            ));
        }

        info!(
            "Deleted topic with ID: {} for stream with ID: {}.",
            topic.topic_id, topic.stream_id
        );

        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct TopicData {
    name: String,
    created_at: u64,
    message_expiry: Option<u32>,
    compression_algorithm: CompressionAlgorithm,
    max_topic_size: Option<IggyByteSize>,
    replication_factor: u8,
}
