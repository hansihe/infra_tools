use std::collections::{BTreeMap, VecDeque};
use std::sync::Mutex;
use std::time::Instant;

use rskafka::record::Record;

// Entry in the message cache.
struct MessageEntry {
    record: Record,
    offset: i64,
    last_accessed: Instant,
    pinned: bool,
}

// Manages the storage and eviction of messages.
pub struct MessageCache {
    entries: BTreeMap<i64, MessageEntry>, // Keyed by offset
    lru_queue: VecDeque<i64>,             // Offsets of unpinned messages for LRU
    total_size: usize,
    max_size: usize,
}

impl MessageCache {
    pub fn new(max_size: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            lru_queue: VecDeque::new(),
            total_size: 0,
            max_size,
        }
    }

    // Adds messages to the cache.
    pub fn add_messages(
        &mut self,
        messages: Vec<(i64, Record)>,
        subscription_manager: &SubscriptionManager,
    ) {
        for (offset, record) in messages {
            let size = record.approximate_size();

            let pinned = subscription_manager.is_offset_subscribed(offset);

            let entry = MessageEntry {
                record,
                offset,
                last_accessed: Instant::now(),
                pinned,
            };

            self.entries.insert(offset, entry);

            if !pinned {
                self.lru_queue.push_back(offset);
            }

            self.total_size += size;
        }

        self.evict_if_needed();
    }

    // Evicts unpinned messages based on LRU policy to maintain memory constraints.
    pub fn evict_if_needed(&mut self) {
        while self.total_size > self.max_size {
            if let Some(offset) = self.lru_queue.pop_front() {
                if let Some(entry) = self.entries.remove(&offset) {
                    self.total_size -= entry.record.approximate_size();
                }
            } else {
                // No more unpinned messages to evict
                break;
            }
        }
    }

    // Retrieves a message by offset.
    pub fn get_message(&mut self, offset: i64) -> Option<&Record> {
        if let Some(entry) = self.entries.get_mut(&offset) {
            entry.last_accessed = Instant::now();
            if !entry.pinned {
                // Move to the back of the LRU queue
                self.lru_queue.retain(|&o| o != offset);
                self.lru_queue.push_back(offset);
            }
            Some(&entry.record)
        } else {
            None
        }
    }

    // Retrieves messages within a range.
    pub fn get_messages_in_range(&mut self, start_offset: i64, end_offset: i64) -> Vec<&Record> {
        let mut records = Vec::new();
        for (&offset, entry) in self.entries.range_mut(start_offset..=end_offset) {
            entry.last_accessed = Instant::now();
            if !entry.pinned {
                // Move to the back of the LRU queue
                self.lru_queue.retain(|&o| o != offset);
                self.lru_queue.push_back(offset);
            }
            records.push(&entry.record);
        }
        records
    }

    // Updates the pinned status of messages based on current subscriptions.
    pub fn update_pins(&mut self, subscription_manager: &SubscriptionManager) {
        for entry in self.entries.values_mut() {
            let was_pinned = entry.pinned;
            entry.pinned = subscription_manager.is_offset_subscribed(entry.offset);

            if was_pinned && !entry.pinned {
                // Now unpinned, add to LRU queue
                self.lru_queue.push_back(entry.offset);
            } else if !was_pinned && entry.pinned {
                // Now pinned, remove from LRU queue
                self.lru_queue.retain(|&o| o != entry.offset);
            }
        }

        self.evict_if_needed();
    }
}

// Represents a subscription to a range of offsets.
pub struct Subscription {
    start_offset: i64,
    end_offset: i64,
}

// Manages active subscriptions.
pub struct SubscriptionManager {
    subscriptions: Mutex<Vec<Subscription>>,
}

impl SubscriptionManager {
    pub fn new() -> Self {
        Self {
            subscriptions: Mutex::new(Vec::new()),
        }
    }

    pub fn add_subscription(&self, subscription: Subscription) {
        let mut subs = self.subscriptions.lock().unwrap();
        subs.push(subscription);
    }

    pub fn remove_subscription(&self, subscription: &Subscription) {
        let mut subs = self.subscriptions.lock().unwrap();
        subs.retain(|s| {
            s.start_offset != subscription.start_offset || s.end_offset != subscription.end_offset
        });
    }

    pub fn is_offset_subscribed(&self, offset: i64) -> bool {
        let subs = self.subscriptions.lock().unwrap();
        subs.iter()
            .any(|s| offset >= s.start_offset && offset <= s.end_offset)
    }
}
