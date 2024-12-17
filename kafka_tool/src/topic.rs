use rskafka::{client::Client, topic::Topic};

const CHUNK_SIZE: usize = 4096;

pub struct TopicState {}

#[derive(Debug, Clone, Copy)]
struct OffsetRange {
    start: i64,
    end: i64,
}

impl TopicState {
    pub async fn new(client: &mut Client, topic: Topic) {
        let earliest_offsets = client
            .fetch_topic_offsets(&topic, FetchOffset::Earliest)
            .unwrap();

        let latest_offsets = client
            .fetch_topic_offsets(&topic, FetchOffset::Latest)
            .unwrap();

        let mut ranges = vec![OffsetRange { start: 0, end: 0 }; earliest_offsets.len()];

        for o in earliest_offsets {
            ranges[o.partition as usize].start = o.offset;
        }

        for o in latest_offsets {
            ranges[o.partition as usize].end = o.offset;
        }
    }
}

struct Chunked {
    start_offset: i64,
    chunks: Vec<Option<Chunk>>,
}

struct Chunk {
    start_offset: i64,
}
