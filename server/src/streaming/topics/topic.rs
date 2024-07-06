use crate::configs::system::SystemConfig;
use crate::streaming::partitions::partition::Partition;
use crate::streaming::storage::SystemStorage;
use crate::streaming::topics::consumer_group::ConsumerGroup;
use core::fmt;
use iggy::compression::compression_algorithm::CompressionAlgorithm;
use iggy::error::IggyError;
use iggy::locking::IggySharedMut;
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::timestamp::IggyTimestamp;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug)]
pub struct Topic {
    pub stream_id: u32,
    pub topic_id: u32,
    pub name: String,
    pub path: String,
    pub partitions_path: String,
    pub(crate) size_bytes: Rc<AtomicU64>,
    pub(crate) size_of_parent_stream: Rc<AtomicU64>,
    pub(crate) messages_count_of_parent_stream: Rc<AtomicU64>,
    pub(crate) messages_count: Rc<AtomicU64>,
    pub(crate) segments_count_of_parent_stream: Rc<AtomicU32>,
    pub(crate) config: Arc<SystemConfig>,
    pub(crate) partitions: RefCell<HashMap<u32, Partition>>,
    pub(crate) storage: Rc<SystemStorage>,
    pub(crate) consumer_groups: HashMap<u32, ConsumerGroup>,
    pub(crate) consumer_groups_ids: HashMap<String, u32>,
    pub(crate) current_consumer_group_id: AtomicU32,
    pub(crate) current_partition_id: AtomicU32,
    pub message_expiry: Option<u32>,
    pub compression_algorithm: CompressionAlgorithm,
    pub max_topic_size: Option<IggyByteSize>,
    pub replication_factor: u8,
    pub created_at: u64,
}

impl Clone for Topic {
    fn clone(&self) -> Self {
        Self {
            stream_id: self.stream_id.clone(),
            topic_id: self.topic_id.clone(),
            name: self.name.clone(),
            path: self.path.clone(),
            partitions_path: self.partitions_path.clone(),
            size_bytes: self.size_bytes.clone(),
            size_of_parent_stream: self.size_of_parent_stream.clone(),
            messages_count_of_parent_stream: self.messages_count_of_parent_stream.clone(),
            messages_count: self.messages_count.clone(),
            segments_count_of_parent_stream: self.segments_count_of_parent_stream.clone(),
            config: self.config.clone(),
            partitions: self.partitions.clone(),
            storage: self.storage.clone(),
            consumer_groups: self.consumer_groups.clone(),
            consumer_groups_ids: self.consumer_groups_ids.clone(),
            current_consumer_group_id: AtomicU32::new(
                self.current_consumer_group_id.load(Ordering::SeqCst),
            ),
            current_partition_id: AtomicU32::new(self.current_partition_id.load(Ordering::SeqCst)),
            message_expiry: self.message_expiry.clone(),
            compression_algorithm: self.compression_algorithm.clone(),
            max_topic_size: self.max_topic_size.clone(),
            replication_factor: self.replication_factor.clone(),
            created_at: self.created_at.clone(),
        }
    }
}

impl Topic {
    pub fn empty(
        stream_id: u32,
        topic_id: u32,
        size_of_parent_stream: Rc<AtomicU64>,
        messages_count_of_parent_stream: Rc<AtomicU64>,
        segments_count_of_parent_stream: Rc<AtomicU32>,
        config: Arc<SystemConfig>,
        storage: Rc<SystemStorage>,
    ) -> Topic {
        Topic::create(
            stream_id,
            topic_id,
            "",
            0,
            config,
            storage,
            size_of_parent_stream,
            messages_count_of_parent_stream,
            segments_count_of_parent_stream,
            None,
            Default::default(),
            None,
            1,
        )
        .unwrap()
        .0
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create(
        stream_id: u32,
        topic_id: u32,
        name: &str,
        partitions_count: u32,
        config: Arc<SystemConfig>,
        storage: Rc<SystemStorage>,
        size_of_parent_stream: Rc<AtomicU64>,
        messages_count_of_parent_stream: Rc<AtomicU64>,
        segments_count_of_parent_stream: Rc<AtomicU32>,
        message_expiry: Option<u32>,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: Option<IggyByteSize>,
        replication_factor: u8,
    ) -> Result<(Topic, Vec<u32>), IggyError> {
        let path = config.get_topic_path(stream_id, topic_id);
        let partitions_path = config.get_partitions_path(stream_id, topic_id);
        let mut topic = Topic {
            stream_id,
            topic_id,
            name: name.to_string(),
            partitions: RefCell::new(HashMap::new()),
            path,
            partitions_path,
            storage,
            size_bytes: Rc::new(AtomicU64::new(0)),
            size_of_parent_stream,
            messages_count_of_parent_stream,
            messages_count: Rc::new(AtomicU64::new(0)),
            segments_count_of_parent_stream,
            consumer_groups: HashMap::new(),
            consumer_groups_ids: HashMap::new(),
            current_consumer_group_id: AtomicU32::new(1),
            current_partition_id: AtomicU32::new(1),
            message_expiry: match message_expiry {
                Some(expiry) => match expiry {
                    0 => None,
                    _ => Some(expiry),
                },
                None => match config.retention_policy.message_expiry.as_secs() {
                    0 => None,
                    expiry => Some(expiry),
                },
            },
            compression_algorithm,
            max_topic_size,
            replication_factor,
            config,
            created_at: IggyTimestamp::now().to_micros(),
        };

        let partition_ids = topic.add_partitions(partitions_count)?;
        Ok((topic, partition_ids))
    }

    pub fn get_size(&self) -> IggyByteSize {
        IggyByteSize::from(self.size_bytes.load(Ordering::SeqCst))
    }

    pub fn get_partitions(&self) -> Vec<Partition> {
        self.partitions.borrow().values().cloned().collect()
    }

    pub fn get_partition(&self, partition_id: u32) -> Result<Partition, IggyError> {
        match self.partitions.borrow().get(&partition_id) {
            Some(partition_arc) => Ok(partition_arc.clone()),
            None => Err(IggyError::PartitionNotFound(
                partition_id,
                self.topic_id,
                self.stream_id,
            )),
        }
    }
}

impl fmt::Display for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        let max_topic_size = match self.max_topic_size {
            Some(size) => size.as_human_string_with_zero_as_unlimited(),
            None => "unlimited".to_owned(),
        };
        write!(f, "ID: {}, ", self.topic_id)?;
        write!(f, "stream ID: {}, ", self.stream_id)?;
        write!(f, "name: {}, ", self.name)?;
        write!(f, "path: {}, ", self.path)?;
        write!(
            f,
            "partitions count: {:?}, ",
            self.partitions.borrow().len()
        )?;
        write!(f, "message expiry (s): {:?}, ", self.message_expiry)?;
        write!(f, "max topic size (B): {:?}, ", max_topic_size)?;
        write!(f, "replication factor: {}, ", self.replication_factor)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use crate::streaming::storage::tests::get_test_system_storage;

    #[monoio::test]
    async fn should_be_created_given_valid_parameters() {
        let storage = Rc::new(get_test_system_storage());
        let stream_id = 1;
        let topic_id = 2;
        let name = "test";
        let partitions_count = 3;
        let message_expiry = 10;
        let compression_algorithm = CompressionAlgorithm::None;
        let max_topic_size = IggyByteSize::from_str("2 GB").unwrap();
        let replication_factor = 1;
        let config = Arc::new(SystemConfig::default());
        let path = config.get_topic_path(stream_id, topic_id);
        let size_of_parent_stream = Rc::new(AtomicU64::new(0));
        let messages_count_of_parent_stream = Rc::new(AtomicU64::new(0));
        let segments_count_of_parent_stream = Rc::new(AtomicU32::new(0));

        let topic = Topic::create(
            stream_id,
            topic_id,
            name,
            partitions_count,
            config,
            storage,
            messages_count_of_parent_stream,
            size_of_parent_stream,
            segments_count_of_parent_stream,
            Some(message_expiry),
            compression_algorithm,
            Some(max_topic_size),
            replication_factor,
        )
        .unwrap()
        .0;

        assert_eq!(topic.stream_id, stream_id);
        assert_eq!(topic.topic_id, topic_id);
        assert_eq!(topic.path, path);
        assert_eq!(topic.name, name);
        assert_eq!(
            topic.partitions.borrow().iter().len(),
            partitions_count as usize
        );
        assert_eq!(topic.message_expiry, Some(message_expiry));

        for (id, partition) in topic.partitions.borrow().iter() {
            assert_eq!(partition.stream_id, stream_id);
            assert_eq!(partition.topic_id, topic.topic_id);
            assert_eq!(partition.partition_id, *id);
            assert_eq!(partition.segments.len(), 1);
        }
    }
}
