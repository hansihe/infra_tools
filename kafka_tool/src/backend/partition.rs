use std::{collections::BTreeMap, ops::Range};

use lasso::Spur;
use roaring::RoaringBitmap;
use rskafka::{client::partition::OffsetAt, record::RecordAndOffset};

#[derive(Default)]
pub struct BrokerData {
    partitions: BTreeMap<(Spur, i32), PartitionData>,
}

impl BrokerData {
    pub fn get(&self, topic: Spur, partition: i32) -> Option<&PartitionData> {
        self.partitions.get(&(topic, partition))
    }
    pub fn get_mut(&mut self, topic: Spur, partition: i32) -> &mut PartitionData {
        self.partitions.entry((topic, partition)).or_default()
    }
    pub fn iter(&self) -> impl Iterator<Item = (Spur, i32, &PartitionData)> {
        self.partitions.iter().map(|v| (v.0 .0, v.0 .1, v.1))
    }
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (Spur, i32, &mut PartitionData)> {
        self.partitions.iter_mut().map(|v| (v.0 .0, v.0 .1, v.1))
    }
}

#[derive(Default)]
pub struct PartitionData {
    offsets: Range<Option<i64>>,
    chunks: BTreeMap<i64, Vec<RecordAndOffset>>,
    subscriptions: BTreeMap<u64, Option<Range<i64>>>,
    // Bitmap of loaded record offsets
    // This is maintained incrementally.
    loaded_records: RoaringBitmap,
    // Bitmap of subscribed records. Not source of truth,
    // built from subscriptions + fixed logic.
    subscribed_records: RoaringBitmap,
    requested_loads_dirty: bool,
}

impl PartitionData {
    pub fn start_iter_rev(&self, offset: i64) -> RecordIterator<'_> {
        RecordIterator {
            partitions: self,
            current: self.get_first_chunk_before_offset(offset),
            offset,
        }
    }

    /// If true, the caller should assume that the output of
    /// `get_requested_loads` will have changed since the last
    /// call.
    pub fn requested_loads_dirty(&self) -> bool {
        self.requested_loads_dirty
    }

    /// If true, this partition has subscriptions. The caller
    /// might want to regularly refresh offsets.
    pub fn has_any_subscription(&self) -> bool {
        !self.subscriptions.is_empty()
    }

    fn get_first_chunk_before_offset(&self, offset: i64) -> Option<&Vec<RecordAndOffset>> {
        self.chunks.range(..=offset).last().map(|v| v.1)
    }

    pub fn set_broker_offset(&mut self, at: OffsetAt, offset: i64) {
        self.requested_loads_dirty = true;
        match at {
            OffsetAt::Earliest => self.offsets.start = Some(offset),
            OffsetAt::Latest => self.offsets.end = Some(offset),
        }
    }

    pub fn offset_range(&self) -> Option<Range<i64>> {
        match (self.offsets.start, self.offsets.end) {
            (Some(start), Some(end)) => Some(start..end),
            _ => None,
        }
    }

    pub fn insert_chunk(&mut self, chunk: Vec<RecordAndOffset>) {
        let first_offset = chunk[0].offset;
        self.loaded_records
            .insert_range((first_offset as u32)..(first_offset as u32 + chunk.len() as u32));
        self.chunks.insert(first_offset, chunk);
        self.requested_loads_dirty = true;
    }

    pub fn put_subscription(&mut self, id: u64, range: Option<Range<i64>>) {
        // TODO only if changed
        self.requested_loads_dirty = true;
        self.subscriptions.insert(id, range);
    }
    pub fn remove_subscription(&mut self, id: u64) {
        self.subscriptions.remove(&id);
    }

    pub fn get_requested_loads(&mut self) -> Vec<Range<i64>> {
        self.build_subscribed_bitmap();
        let to_load = &self.subscribed_records - &self.loaded_records;
        let ranges = bitmap_contiguous_ranges(&to_load);
        self.requested_loads_dirty = false;
        ranges
    }

    fn build_subscribed_bitmap(&mut self) {
        // TODO maybe update incrementally eventually?

        self.subscribed_records.clear();
        for range_opt in self.subscriptions.values() {
            if let Some(range) = range_opt.clone() {
                assert!(range.start >= 0 && range.start <= u32::MAX as i64);
                assert!(range.end >= 0 && range.end <= u32::MAX as i64);
                self.subscribed_records
                    .insert_range((range.start as u32)..(range.end as u32));
            }
        }
        if let Some(range) = self.offset_range() {
            let early = range.start as u32;
            let late = range.end as u32;

            // As a heuristic, we always load 20 last elements.
            self.subscribed_records
                .insert_range((late.saturating_sub(20).max(early))..late);

            // Initial start query starts at +50 so that it lessens the chance
            // of us querying before start.
            // Only used for time range mapping initially anyway, so it doesn't
            // matter very much.
            self.subscribed_records.insert((early + 50).min(late));
        }
    }
}

pub struct RecordIterator<'a> {
    partitions: &'a PartitionData,
    current: Option<&'a Vec<RecordAndOffset>>,
    offset: i64,
}

impl<'a> RecordIterator<'a> {
    pub fn next(&mut self) -> Option<&RecordAndOffset> {
        let current = self.current?;
        // If we are beyond start of current chunk, fetch next
        if current[0].offset > self.offset {
            self.current = self.partitions.get_first_chunk_before_offset(self.offset);
        }

        let current = self.current?;
        let diff = self.offset - current[0].offset;
        self.offset -= 1;
        assert!(diff >= 0);
        current.get(diff as usize)
    }
}

/// TODO implement in `roaring` crate, we can make a very
/// optimized implementation of iterating over contiguous
/// ranges of elements.
pub fn bitmap_contiguous_ranges(bitmap: &RoaringBitmap) -> Vec<Range<i64>> {
    let mut out = Vec::new();
    let mut range_start: i64 = -2;
    let mut next_contig: i64 = -2;
    for elem in bitmap.iter() {
        if elem as i64 == next_contig {
            // tight loop is just an integer add
            next_contig += 1;
        } else {
            if range_start >= 0 {
                out.push(range_start..next_contig);
            }
            range_start = elem as i64;
            next_contig = elem as i64 + 1;
        }
    }
    if range_start >= 0 {
        out.push(range_start..next_contig);
    }
    out
}
