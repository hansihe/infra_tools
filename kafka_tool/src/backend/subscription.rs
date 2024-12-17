use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, AtomicU8, Ordering},
        Arc, Weak,
    },
};

use tokio::{
    select,
    sync::{
        mpsc::{UnboundedReceiver, UnboundedSender},
        Notify,
    },
};

struct SharedStateInner {
    notify_dirty: Notify,
    next_id: AtomicU64,
}
type SharedState = Arc<SharedStateInner>;

pub struct SubscriptionManager<T: Subscription> {
    shared: SharedState,
    subscriptions: BTreeMap<u64, Weak<T>>,
    new_subscriptions: UnboundedReceiver<Arc<T>>,
}

pub struct SubscriptionCreator<T: Subscription> {
    shared: SharedState,
    sender: UnboundedSender<Arc<T>>,
}

impl<T: Subscription> SubscriptionCreator<T> {
    pub fn create(&self, create: impl FnOnce(SubscriptionInner) -> T) -> Arc<T> {
        let id = self
            .shared
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let inner = SubscriptionInner {
            id,
            shared: self.shared.clone(),
            flags: AtomicU8::new(0),
        };
        let typ = Arc::new(create(inner));
        self.sender.send(typ.clone()).unwrap();
        typ
    }
}

#[derive(Debug, Copy, Clone)]
pub enum SubscriptionChange {
    Created(u64),
    Removed(u64),
    Modified(u64),
}

impl SubscriptionChange {
    pub fn id(self) -> u64 {
        match self {
            SubscriptionChange::Created(id) => id,
            SubscriptionChange::Removed(id) => id,
            SubscriptionChange::Modified(id) => id,
        }
    }
}

impl<T: Subscription> SubscriptionManager<T> {
    pub fn new() -> (Self, SubscriptionCreator<T>) {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let manager = SubscriptionManager {
            shared: Arc::new(SharedStateInner {
                notify_dirty: Notify::new(),
                next_id: AtomicU64::new(0),
            }),
            subscriptions: BTreeMap::new(),
            new_subscriptions: receiver,
        };
        let shared = manager.shared.clone();
        (manager, SubscriptionCreator { sender, shared })
    }

    pub async fn wait_change(&mut self) -> Vec<SubscriptionChange> {
        select! {
            new_sub_ret = self.new_subscriptions.recv() => {
                // TODO maintain ref to channel internally, it should
                // never close.
                let new_sub = new_sub_ret.unwrap();
                self.subscriptions.insert(new_sub.inner().id, Arc::downgrade(&new_sub));
                vec![SubscriptionChange::Created(new_sub.inner().id)]
            },
            _ = self.shared.notify_dirty.notified() => {
                let mut out = Vec::new();

                // Remove subscriptions with no strong references
                // Collect dirty subscriptions
                self.subscriptions.retain(|k, v| {
                    if let Some(sub) = v.upgrade() {
                        let inner = sub.inner();
                        if inner.ack_dirty() {
                            out.push(SubscriptionChange::Modified(inner.id));
                        }
                        true
                    } else {
                        out.push(SubscriptionChange::Removed(*k));
                        false
                    }
                });

                out
            },
        }
    }

    pub fn try_get_by_id(&self, id: u64) -> Option<Arc<T>> {
        self.subscriptions.get(&id).and_then(|v| v.upgrade())
    }
}

pub trait Subscription {
    fn inner(&self) -> &SubscriptionInner;
}

const FLAG_DIRTY: u8 = 0b1;

pub struct SubscriptionInner {
    id: u64,
    shared: SharedState,
    flags: AtomicU8,
}

impl SubscriptionInner {
    pub fn mark_dirty(&self) {
        self.flags.fetch_or(FLAG_DIRTY, Ordering::Release);
        self.shared.notify_dirty.notify_one();
    }
    fn ack_dirty(&self) -> bool {
        self.flags.fetch_and(!FLAG_DIRTY, Ordering::Relaxed) != 0
    }
}

// ========

//pub mod topic {
//    use lasso::Spur;
//
//    use super::{Subscription, SubscriptionCreator, SubscriptionInner, SubscriptionManager};
//
//    pub type TopicSubscriptionManager = SubscriptionManager<TopicSubscription>;
//    pub type TopicSubscriptionCreator = SubscriptionCreator<TopicSubscription>;
//
//    pub struct TopicSubscription {
//        pub topic: Spur,
//        inner: SubscriptionInner,
//    }
//
//    impl Subscription for TopicSubscription {
//        fn inner(&self) -> &SubscriptionInner {
//            &self.inner
//        }
//    }
//}

pub mod range {
    use std::{ops::Range, sync::Arc};

    use lasso::Spur;
    use parking_lot::Mutex;

    use super::{Subscription, SubscriptionCreator, SubscriptionInner, SubscriptionManager};

    pub type RangeSubscriptionManager = SubscriptionManager<RangeSubscription>;
    pub type RangeSubscriptionCreator = SubscriptionCreator<RangeSubscription>;

    pub struct RangeSubscription {
        pub topic: Spur,
        pub partition: i32,
        typ_inner: Mutex<RangeSubscriptionInner>,
        inner: SubscriptionInner,
    }
    impl RangeSubscription {
        pub fn create(
            creator: &RangeSubscriptionCreator,
            topic: Spur,
            partition: i32,
            range: Option<Range<i64>>,
        ) -> Arc<Self> {
            creator.create(|inner| Self {
                topic,
                partition,
                typ_inner: Mutex::new(RangeSubscriptionInner { range }),
                inner,
            })
        }
    }

    struct RangeSubscriptionInner {
        range: Option<Range<i64>>,
    }

    impl RangeSubscription {
        pub fn range(&self) -> Option<Range<i64>> {
            self.typ_inner.lock().range.clone()
        }

        pub fn set_range(&self, range: Option<Range<i64>>) {
            let mut typ_inner = self.typ_inner.lock();
            if range != typ_inner.range {
                typ_inner.range = range;
                self.inner.mark_dirty();
            }
        }
    }

    impl Subscription for RangeSubscription {
        fn inner(&self) -> &SubscriptionInner {
            &self.inner
        }
    }
}
