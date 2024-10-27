use crate::streaming::segments::segment::Segment;
use iggy::error::IggyError;
use iggy::error::IggyError::InvalidOffset;
use memmap2::Mmap;
use std::ops::Deref;
use std::ops::Index as IndexOp;

#[derive(Debug)]
pub enum Indexes {
    InMemory(Vec<Index>),
    MemoryMapped { mmap: Mmap },
}

impl From<Vec<Index>> for Indexes {
    fn from(indexes: Vec<Index>) -> Self {
        Indexes::InMemory(indexes)
    }
}

impl Indexes {
    pub fn push(&mut self, index: Index) {
        match self {
            Indexes::InMemory(vec) => {
                vec.push(index);
            }
            Indexes::MemoryMapped { .. } => {
                panic!("Cannot push to memory-mapped indexes");
            }
        }
    }
}

impl Deref for Indexes {
    type Target = [Index];

    fn deref(&self) -> &Self::Target {
        match self {
            Indexes::InMemory(vec) => vec.as_slice(),
            Indexes::MemoryMapped { mmap, .. } => {
                let bytes = &mmap[..];
                let len = bytes.len() / std::mem::size_of::<Index>();
                let ptr = bytes.as_ptr() as *const Index;
                unsafe { std::slice::from_raw_parts(ptr, len) }
            }
        }
    }
}

impl IndexOp<usize> for Indexes {
    type Output = Index;

    fn index(&self, idx: usize) -> &Self::Output {
        &self.deref()[idx]
    }
}

impl Default for Indexes {
    fn default() -> Self {
        Indexes::InMemory(Vec::new())
    }
}

impl<'a> IntoIterator for &'a Indexes {
    type Item = &'a Index;
    type IntoIter = std::slice::Iter<'a, Index>;

    fn into_iter(self) -> Self::IntoIter {
        self.deref().iter()
    }
}

#[repr(C)]
#[derive(Debug, Eq, Clone, Copy, Default)]
pub struct Index {
    pub offset: u32,
    pub position: u32,
    pub timestamp: u64,
}

impl PartialEq<Self> for Index {
    fn eq(&self, other: &Self) -> bool {
        self.offset == other.offset
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct IndexRange {
    pub start: Index,
    pub end: Index,
}

impl Segment {
    pub fn get_indexes_slice(&self) -> &[Index] {
        match &self.indexes {
            Some(indexes) => indexes,
            None => &[],
        }
    }

    pub fn load_highest_lower_bound_index(
        &self,
        start_offset: u32,
        end_offset: u32,
    ) -> Result<IndexRange, IggyError> {
        let indices = self.get_indexes_slice();
        let starting_offset_idx = binary_search_index(indices, start_offset);
        let ending_offset_idx = binary_search_index(indices, end_offset);

        match (starting_offset_idx, ending_offset_idx) {
            (Some(starting_offset_idx), Some(ending_offset_idx)) => Ok(IndexRange {
                start: indices[starting_offset_idx],
                end: indices[ending_offset_idx],
            }),
            (Some(starting_offset_idx), None) => Ok(IndexRange {
                start: indices[starting_offset_idx],
                end: *indices.last().unwrap(),
            }),
            (None, _) => Err(InvalidOffset(start_offset as u64 + self.start_offset)),
        }
    }
}

fn binary_search_index(indices: &[Index], offset: u32) -> Option<usize> {
    match indices.binary_search_by(|index| index.offset.cmp(&offset)) {
        Ok(index) => Some(index),
        Err(index) => {
            if index < indices.len() {
                Some(index)
            } else {
                None
            }
        }
    }
}

impl IndexRange {
    pub fn max_range() -> Self {
        Self {
            start: Index {
                offset: 0,
                position: 0,
                timestamp: 0,
            },
            end: Index {
                offset: u32::MAX - 1,
                position: u32::MAX,
                timestamp: u64::MAX,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configs::system::{SegmentConfig, SystemConfig};
    use crate::streaming::storage::tests::get_test_system_storage;
    use iggy::utils::expiry::IggyExpiry;
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;

    fn create_segment() -> Segment {
        let storage = Arc::new(get_test_system_storage());
        let stream_id = 1;
        let topic_id = 2;
        let partition_id = 3;
        let start_offset = 0;
        let config = Arc::new(SystemConfig {
            segment: SegmentConfig {
                cache_indexes: true,
                ..Default::default()
            },
            ..Default::default()
        });

        Segment::create(
            stream_id,
            topic_id,
            partition_id,
            start_offset,
            config,
            storage,
            IggyExpiry::NeverExpire,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
        )
    }

    fn create_test_indices(segment: &mut Segment) {
        let indexes = vec![
            Index {
                offset: 5,
                position: 0,
                timestamp: 1000,
            },
            Index {
                offset: 20,
                position: 100,
                timestamp: 2000,
            },
            Index {
                offset: 35,
                position: 200,
                timestamp: 3000,
            },
            Index {
                offset: 50,
                position: 300,
                timestamp: 4000,
            },
            Index {
                offset: 65,
                position: 400,
                timestamp: 5000,
            },
        ];
        if let Some(Indexes::InMemory(vec)) = segment.indexes.as_mut() {
            vec.extend(indexes);
        }
    }

    #[test]
    fn should_find_both_indices() {
        let mut segment = create_segment();
        create_test_indices(&mut segment);
        let result = segment.load_highest_lower_bound_index(15, 45).unwrap();

        assert_eq!(result.start.offset, 20);
        assert_eq!(result.end.offset, 50);
    }

    #[test]
    fn start_and_end_index_should_be_equal() {
        let mut segment = create_segment();
        create_test_indices(&mut segment);
        let result_end_range = segment.load_highest_lower_bound_index(65, 100).unwrap();

        assert_eq!(result_end_range.start.offset, 65);
        assert_eq!(result_end_range.end.offset, 65);

        let result_start_range = segment.load_highest_lower_bound_index(0, 5).unwrap();
        assert_eq!(result_start_range.start.offset, 5);
        assert_eq!(result_start_range.end.offset, 5);
    }

    #[test]
    fn should_clamp_last_index_when_out_of_range() {
        let mut segment = create_segment();
        create_test_indices(&mut segment);
        let result = segment.load_highest_lower_bound_index(5, 100).unwrap();

        assert_eq!(result.start.offset, 5);
        assert_eq!(result.end.offset, 65);
    }

    #[test]
    fn should_return_err_when_both_indices_out_of_range() {
        let mut segment = create_segment();
        create_test_indices(&mut segment);

        let result = segment.load_highest_lower_bound_index(100, 200);
        assert!(result.is_err());
    }
}
