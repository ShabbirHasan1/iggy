use human_repr::HumanCount;
use iggy::utils::byte_size::IggyByteSize;
use std::{
    num::NonZeroU32,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
};

const MINIMUM_MSG_PAYLOAD_SIZE: usize = 100;

#[derive(Debug, Clone, Copy, PartialEq)]
enum BenchmarkFinishConditionType {
    ByMessageBatchesUserSize,
    ByMessageBatchesCount,
}

pub struct BenchmarkFinishCondition {
    kind: BenchmarkFinishConditionType,
    total: u64,
    left_total: Arc<AtomicI64>,
}

impl BenchmarkFinishCondition {
    pub fn new(batches_size: Option<IggyByteSize>, batches_count: Option<NonZeroU32>) -> Arc<Self> {
        Arc::new(match (batches_size, batches_count) {
            (None, Some(count)) => Self {
                kind: BenchmarkFinishConditionType::ByMessageBatchesCount,
                total: count.get() as u64,
                left_total: Arc::new(AtomicI64::new(count.get() as i64)),
            },
            (Some(size), None) => Self {
                kind: BenchmarkFinishConditionType::ByMessageBatchesUserSize,
                total: size.as_bytes_u64(),
                left_total: Arc::new(AtomicI64::new(size.as_bytes_u64() as i64)),
            },
            _ => unreachable!(),
        })
    }

    pub fn account(&self, size_to_subtract: u64) {
        match self.kind {
            BenchmarkFinishConditionType::ByMessageBatchesUserSize => {
                self.left_total
                    .fetch_sub(size_to_subtract as i64, Ordering::AcqRel);
            }
            BenchmarkFinishConditionType::ByMessageBatchesCount => {
                self.left_total.fetch_sub(1, Ordering::AcqRel);
            }
        }
    }

    pub fn check(&self) -> bool {
        self.left() <= 0
    }

    pub fn total(&self) -> u64 {
        self.total
    }

    pub fn total_str(&self) -> String {
        match self.kind {
            BenchmarkFinishConditionType::ByMessageBatchesUserSize => {
                format!("messages of size: {}", self.total.human_count_bytes())
            }

            BenchmarkFinishConditionType::ByMessageBatchesCount => {
                format!("{} messages", self.total.human_count_bare())
            }
        }
    }

    pub fn left(&self) -> i64 {
        self.left_total.load(Ordering::Relaxed)
    }

    pub fn status(&self) -> String {
        format!("{}/{}", self.total() - self.left() as u64, self.total())
    }

    pub fn max_capacity(&self) -> usize {
        let value = self.left_total.load(Ordering::Relaxed);
        if self.kind == BenchmarkFinishConditionType::ByMessageBatchesUserSize {
            value as usize / MINIMUM_MSG_PAYLOAD_SIZE
        } else {
            value as usize
        }
    }
}
