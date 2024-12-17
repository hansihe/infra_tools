use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    ops::Range,
    sync::Arc,
    time::Duration,
};

use eframe::egui::Context;
use kafka::{kafka_connect, list_topics};
use lasso::{Spur, ThreadedRodeo};
use partition::BrokerData;
use rskafka::{
    client::{
        partition::{OffsetAt, PartitionClient},
        Client,
    },
    record::RecordAndOffset,
};
use subscription::range::{RangeSubscriptionCreator, RangeSubscriptionManager};
use tokio::{
    select,
    sync::{
        mpsc::{self, UnboundedReceiver},
        Mutex,
    },
    task::JoinSet,
};

use crate::{config::structure::StructureConfig, lua::ConnectStrategy, util::MaybeFut};

mod kafka;
//mod offset_poller;
//mod connection;
mod partition;
pub mod subscription;

pub type State = Arc<InnerState>;

pub struct InnerState {
    pub lua: mlua::Lua,
    pub lua_state: Arc<crate::lua::LuaData>,
    pub structure_config: StructureConfig,
    pub rodeo: ThreadedRodeo,
    pub gui_context: Context,
    pub data: Mutex<Data>,
    pub range_sub_creator: RangeSubscriptionCreator,
    command_sender: mpsc::UnboundedSender<Command>,
}

impl InnerState {
    //pub fn watch_topic(&self, topic: Spur) {
    //    self.command_sender
    //        .send(Command::WatchTopic(topic))
    //        .unwrap();
    //}
    //pub fn unwatch_topic(&self, topic: Spur) {
    //    self.command_sender
    //        .send(Command::UnwatchTopic(topic))
    //        .unwrap();
    //}
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ConnectionStatus {
    Connecting,
    Connected,
}

pub struct Data {
    pub connection_status: ConnectionStatus,
    pub topics: Vec<Spur>,
    //pub watched_topics: BTreeSet<Spur>,
    pub data: BrokerData,
}

enum Command {
    WatchTopic(Spur),
    UnwatchTopic(Spur),
}

pub fn spawn(
    ctx: Context,
    structure_config: StructureConfig,
    lua: mlua::Lua,
    lua_state: Arc<crate::lua::LuaData>,
    connect_strategy: ConnectStrategy,
) -> State {
    let (sender, receiver) = mpsc::unbounded_channel();

    let (range_sub_manager, range_sub_creator) =
        subscription::range::RangeSubscriptionManager::new();

    let state = InnerState {
        lua,
        lua_state,
        structure_config,
        rodeo: ThreadedRodeo::new(),
        gui_context: ctx,
        data: Mutex::new(Data {
            connection_status: ConnectionStatus::Connecting,
            topics: Vec::new(),
            //watched_topics: BTreeSet::new(),
            //offsets: BTreeMap::new(),
            data: BrokerData::default(),
        }),
        range_sub_creator,
        command_sender: sender,
    };
    let state = Arc::new(state);

    let loop_state = state.clone();
    tokio::spawn(event_loop(
        loop_state,
        receiver,
        range_sub_manager,
        connect_strategy,
    ));

    state
}

type ClientsState = Arc<ClientsStateInner>;
struct ClientsStateInner {
    state: State,
    client: Client,
    partition_clients: Mutex<BTreeMap<(Spur, i32), Arc<PartitionClient>>>,
}

impl ClientsStateInner {
    async fn get_partition_client(&self, topic: Spur, partition: i32) -> Arc<PartitionClient> {
        let mut part_clients = self.partition_clients.lock().await;
        match part_clients.entry((topic, partition)) {
            std::collections::btree_map::Entry::Vacant(vacant_entry) => {
                let part_client = self
                    .client
                    .partition_client(
                        &self.state.rodeo[topic],
                        partition,
                        rskafka::client::partition::UnknownTopicHandling::Error,
                    )
                    .await
                    .unwrap();
                vacant_entry.insert(Arc::new(part_client)).clone()
            }
            std::collections::btree_map::Entry::Occupied(occupied_entry) => {
                occupied_entry.get().clone()
            }
        }
    }
}

struct InternalState {
    state: State,
    partitions: BTreeMap<Spur, BTreeSet<i32>>,
}

impl InternalState {
    fn new(state: State) -> Self {
        Self {
            state,
            partitions: BTreeMap::new(),
        }
    }
}

enum TaskResult {
    OffsetChanged {
        topic: Spur,
        partition: i32,
        at: OffsetAt,
        offset: i64,
    },
    RecordsFetched {
        topic: Spur,
        partition: i32,
        requested_range: Range<i64>,
        records: Vec<RecordAndOffset>,
        late_offset: i64,
    },
}

async fn event_loop(
    state: State,
    mut command_receiver: UnboundedReceiver<Command>,
    mut range_sub_manager: RangeSubscriptionManager,
    connect_strategy: ConnectStrategy,
) {
    let mut internal = InternalState::new(state.clone());

    let client = kafka_connect(connect_strategy, &state).await.unwrap();
    let clients = Arc::new(ClientsStateInner {
        state: state.clone(),
        client,
        partition_clients: Mutex::new(BTreeMap::new()),
    });

    let (partitions, topics) = list_topics(&state, &clients.client).await.unwrap();
    internal.partitions = partitions;
    //let multi = clients.client.multi_client().await.unwrap();

    {
        let mut data = state.data.lock().await;
        data.connection_status = ConnectionStatus::Connected;
        data.topics = topics;
        state.gui_context.request_repaint();
    }

    let mut offset_refresh_interval = tokio::time::interval(Duration::from_secs(2));
    offset_refresh_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    offset_refresh_interval.reset_immediately();
    let mut active_refresh_offset_tasks = 0;

    let mut task_join_set: JoinSet<TaskResult> = JoinSet::new();
    let mut active_range_requests: HashSet<(Spur, i32, Range<i64>)> = HashSet::new();

    loop {
        let has_tasks = !task_join_set.is_empty();
        select! {
            ret = MaybeFut::new_if(task_join_set.join_next(), has_tasks) => {
                let result = ret.unwrap().unwrap();
                match result {
                    TaskResult::OffsetChanged { topic, partition, at, offset } => {
                        let mut data = state.data.lock().await;
                        data.data.get_mut(topic, partition).set_broker_offset(at, offset);
                        active_refresh_offset_tasks -= 1;
                    },
                    TaskResult::RecordsFetched { topic, partition, records, late_offset, .. } => {
                        if records.len() > 0 {
                            let mut data = state.data.lock().await;
                            let part_data = data.data.get_mut(topic, partition);
                            part_data.set_broker_offset(OffsetAt::Latest, late_offset);
                            part_data.insert_chunk(records);
                        }
                        // TODO kick off next fetch task for range
                    },
                }
            },

            _ = offset_refresh_interval.tick() => {
                if active_refresh_offset_tasks == 0 {
                    let data = state.data.lock().await;
                    for (topic, partition, part_data) in data.data.iter() {
                        if part_data.has_any_subscription() {
                            for at in [OffsetAt::Earliest, OffsetAt::Latest].iter().cloned() {
                                active_refresh_offset_tasks += 1;
                                let client = clients
                                    .get_partition_client(topic, partition)
                                    .await;
                                task_join_set.spawn(async move {
                                    let resp = client.get_offset(at).await;
                                    TaskResult::OffsetChanged {
                                        topic,
                                        partition,
                                        at,
                                        offset: resp.unwrap(),
                                    }
                                });
                            }
                        }
                    }
                }
            }
            command_ret = command_receiver.recv() => {
                let command = command_ret.unwrap();

                match command {
                    Command::WatchTopic(_name) => {
                        //let mut data = state.data.lock().await;
                        //data.watched_topics.insert(name);
                        //internal.rebuild_base_offset_query(&data);

                        //if let Some(partitions) = internal.partitions.get(&name) {
                        //    for partition in partitions.iter().cloned() {
                        //        data.data.get_mut(name, partition);
                        //    }
                        //}
                    },
                    Command::UnwatchTopic(_name) => {
                        //let mut data = state.data.lock().await;
                        //data.watched_topics.remove(&name);
                        //internal.rebuild_base_offset_query(&data);
                    },
                }
            }
            changes = range_sub_manager.wait_change() => {
                let mut data = state.data.lock().await;
                for change in changes.iter() {
                    let sub_id = change.id();
                    if let Some(sub) = range_sub_manager.try_get_by_id(sub_id) {
                        let part_data = data.data.get_mut(sub.topic, sub.partition);
                        match change {
                            subscription::SubscriptionChange::Created(_) => {
                                log::info!("subscription {} created ({}:{})", sub_id, &state.rodeo[sub.topic], sub.partition);
                                part_data.put_subscription(sub_id, sub.range());
                            },
                            subscription::SubscriptionChange::Modified(_) => part_data.put_subscription(sub_id, sub.range()),
                            subscription::SubscriptionChange::Removed(_) => {
                                log::info!("subscription {} removed ({}:{})", sub_id, &state.rodeo[sub.topic], sub.partition);
                                part_data.remove_subscription(sub_id)
                            },
                        }
                    }
                }

                for (topic, part, part_data) in data.data.iter_mut().filter(|v| v.2.requested_loads_dirty())
                {
                    let requested_loads = part_data.get_requested_loads();
                    for requested_load in requested_loads.iter().cloned() {
                        if active_range_requests.insert((topic, part, requested_load.clone())) {
                            log::info!("fetching for {}:{} from offset {}..", &state.rodeo[topic], part, requested_load.start);
                            let client = clients.get_partition_client(topic, part).await;
                            let state = state.clone();
                            task_join_set.spawn(async move {
                                let (records, high_watermark) = client
                                    .fetch_records(requested_load.start, 0..500_000, 0)
                                    .await
                                    .unwrap();

                                if records.len() == 0 {
                                    log::info!("fetched for {}:{} from offset {}.. - no records returned!", &state.rodeo[topic], part, requested_load.start);
                                } else {
                                    log::info!("fetched for {}:{} from offset {}..{}", &state.rodeo[topic], part, records[0].offset, records.last().unwrap().offset);
                                }

                                TaskResult::RecordsFetched {
                                    topic,
                                    partition: part,
                                    records,
                                    late_offset: high_watermark,
                                    requested_range: requested_load
                                }
                            });
                        }
                    }
                }
            }
        }
    }
}
