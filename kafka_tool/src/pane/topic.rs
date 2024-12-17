use std::{num::NonZeroUsize, sync::Arc};

use eframe::egui::{self, scroll_area::ScrollBarVisibility, Color32, Stroke, Ui, WidgetText};
use egui_json_tree::DefaultExpand;
use egui_virtual_list::VirtualList;
use lasso::Spur;
use lru::LruCache;
use mlua::LuaSerdeExt;
use rskafka::record::RecordAndOffset;

use crate::{
    backend::{subscription::range::RangeSubscription, State},
    config::structure::TopicConfig,
};

pub struct TopicPane {
    state: State,
    topic: Spur,
    list: VirtualList,
    topic_config_idx: Option<usize>,
    subscriptions: Vec<Arc<RangeSubscription>>,
    formatted_cache: LruCache<i64, serde_json::Value>,
}

impl TopicPane {
    pub fn new(state: State, topic: Spur) -> Self {
        let topic_str = &state.rodeo[topic];

        let range_subscription =
            RangeSubscription::create(&state.range_sub_creator, topic, 0, None);

        //let data = state.data.blocking_lock();

        let mut list = VirtualList::new();
        list.over_scan(200.0);

        TopicPane {
            topic,
            list,
            topic_config_idx: state
                .structure_config
                .topic
                .iter()
                .enumerate()
                .find(|(_idx, v)| {
                    // TODO cache regex
                    let regex = regex::Regex::new(&v.regex).unwrap();
                    regex.is_match(topic_str)
                })
                .map(|(idx, _v)| idx),
            state: state.clone(),
            subscriptions: vec![range_subscription],
            formatted_cache: LruCache::new(NonZeroUsize::new(1024).unwrap()),
        }
    }

    pub fn title(&self) -> WidgetText {
        format!("Topic - {}", &self.state.rodeo[self.topic]).into()
    }

    pub fn ui(&mut self, ui: &mut egui::Ui) -> egui_tiles::UiResponse {
        let state = self.state.clone();
        let data = self.state.data.blocking_lock();

        let idx = (self.topic, 0);
        let part_data = data.data.get(idx.0, idx.1);

        ui.label(format!(
            "offsets: {:?}",
            part_data.and_then(|v| v.offset_range())
        ));

        egui::ScrollArea::vertical()
            .scroll_bar_visibility(ScrollBarVisibility::AlwaysVisible)
            .show(ui, |ui| {
                ui.set_width(ui.available_width());

                if let Some(part_data) = part_data {
                    let mut count = 0;
                    if let Some(range) = part_data.offset_range() {
                        count = range.end - range.start;
                    }
                    assert!(count >= 0);

                    let mut iter = None;

                    let topic_config = self
                        .topic_config_idx
                        .map(|v| &self.state.structure_config.topic[v]);

                    let record_formatter = topic_config.map(|v| {
                        let lock = self.state.lua_state.record_formatters.lock().unwrap();
                        lock.get(&v.formatter).unwrap().clone()
                    });

                    let mut min_offset = i64::MAX;
                    let mut max_offset = i64::MIN;

                    self.list
                        .ui_custom_layout(ui, count as usize, |ui, start_index| {
                            ui.set_width(ui.available_width());
                            let range = part_data.offset_range().unwrap();

                            let start_offset = range.end - start_index as i64;
                            min_offset = min_offset.min(start_offset);
                            max_offset = max_offset.max(start_offset);

                            if iter.is_none() {
                                // TODO this correct?
                                iter = Some(part_data.start_iter_rev(start_offset - 1));
                            }

                            let record = iter.as_mut().unwrap().next();
                            list_element_ui(
                                ui,
                                self.topic,
                                topic_config,
                                &state,
                                record,
                                &record_formatter,
                                &mut self.formatted_cache,
                            );

                            1
                        });

                    //ui.input(|input| {
                    //    println!("scroll time: {}", input.time_since_last_scroll());
                    //    println!("scroll vel: {:?}", input.smooth_scroll_delta);
                    //    //println!("mouse active: {}", input.key_down(egui::Key::));
                    //});

                    if count > 0 {
                        let lowest = crate::util::round_to_nearest(min_offset, 50, false);
                        let highest = crate::util::round_to_nearest(max_offset, 50, true);
                        self.subscriptions[0].set_range(Some(lowest..highest));
                    }
                }
            });

        egui_tiles::UiResponse::None
    }
}

fn list_element_ui(
    ui: &mut Ui,
    topic: Spur,
    topic_config: Option<&TopicConfig>,
    state: &State,
    record: Option<&RecordAndOffset>,
    record_formatter: &Option<mlua::Function>,
    formatted_cache: &mut LruCache<i64, serde_json::Value>,
) {
    let stroke = Stroke::new(1.0, Color32::DARK_GRAY);

    egui::Frame::default()
        .stroke(stroke)
        .outer_margin(2.0)
        .inner_margin(2.0)
        .show(ui, |ui| {
            ui.set_width(ui.available_width());

            if let Some(record) = record {
                egui::menu::bar(ui, |ui| {
                    ui.label(format!(
                        "offset: {} ts: {}",
                        record.offset, record.record.timestamp
                    ));
                });

                if let Some(formatter) = &record_formatter {
                    // Formatting is cached
                    let formatted = formatted_cache.get_or_insert(record.offset, || {
                        let data = state.lua.create_table().unwrap();

                        data.set(
                            "data",
                            // TODO no copy?
                            mlua::BString::new(record.record.value.clone().unwrap()),
                        )
                        .unwrap();

                        let formatted_lua: mlua::Value = formatter
                            .call((data, topic_config.as_ref().unwrap().extra.clone()))
                            .unwrap();

                        let formatted: serde_json::Value =
                            state.lua.from_value(formatted_lua).unwrap();

                        formatted
                    });

                    egui_json_tree::JsonTree::new(("json_tree", topic, record.offset), formatted)
                        .default_expand(DefaultExpand::ToLevel(1))
                        .show(ui);
                } else {
                    ui.label(format!("{:?}", record));
                }
            } else {
                ui.spinner();
            }
        });
}
