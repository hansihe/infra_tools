use std::path::PathBuf;

use anyhow::Context;
use backend::ConnectionStatus;
use eframe::{
    egui::{self, CollapsingHeader, Ui, WidgetText},
    App,
};

use egui_tiles::TileId;
use egui_virtual_list::VirtualList;
use lua::init_lua;
use tokio::runtime::Runtime;

mod backend;
mod config;
mod lua;
mod pane;
mod util;

struct KafkaManApp {
    state: backend::State,

    topic_prefix_filter: String,
    topic_filter: String,

    topics_virtual_list: VirtualList,

    tree: egui_tiles::Tree<TreePane>,
}

impl App for KafkaManApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        let data = self.state.data.blocking_lock();

        egui::SidePanel::left("settings")
            .min_width(200.0)
            .show(ctx, |ui| {
                if data.connection_status == ConnectionStatus::Connected {
                    CollapsingHeader::new("Topic Browser")
                        .default_open(true)
                        .show(ui, |ui| {
                            ui.label("Prefix filter");
                            ui.add(
                                egui::TextEdit::singleline(&mut self.topic_prefix_filter)
                                    .hint_text("Ex: staging."),
                            );

                            ui.label("Filter");
                            ui.add(
                                egui::TextEdit::singleline(&mut self.topic_filter)
                                    .hint_text("Ex: employee-events-v2"),
                            );

                            let matching_topics = data
                                .topics
                                .iter()
                                .filter(|v| {
                                    let vs = &self.state.rodeo[**v];
                                    vs.starts_with(&self.topic_prefix_filter)
                                        && vs.contains(&self.topic_filter)
                                })
                                .map(|v| {
                                    (v, &self.state.rodeo[*v][self.topic_prefix_filter.len()..])
                                })
                                .collect::<Vec<_>>();
                            let matching_topics_len = matching_topics.len();

                            ui.label("Topics");
                            egui::Frame::group(ui.style()).show(ui, |ui| {
                                egui::ScrollArea::vertical()
                                    .id_salt("available_topics")
                                    .max_height(300.0)
                                    .auto_shrink([false, false])
                                    .show(ui, |ui| {
                                        ui.set_width(ui.available_width());

                                        self.topics_virtual_list.ui_custom_layout(
                                            ui,
                                            matching_topics_len,
                                            |ui, start_index| {
                                                let (full, short) = matching_topics[start_index];
                                                if ui.button(short).clicked() {
                                                    let id = self.tree.tiles.insert_pane(
                                                        TreePane::Topic(pane::TopicPane::new(
                                                            self.state.clone(),
                                                            *full,
                                                        )),
                                                    );
                                                    let tab_id =
                                                        self.tree.tiles.insert_tab_tile(vec![id]);
                                                    self.tree.root = Some(tab_id);
                                                }
                                                1
                                            },
                                        );
                                    });
                            });
                        });
                } else {
                    ui.label("Connecting..");
                    ui.spinner();
                }
            });

        std::mem::drop(data);

        egui::CentralPanel::default()
            .frame(egui::Frame::none())
            .show(ctx, |ui| {
                let mut behaviour = TreeBehavior;
                self.tree.ui(&mut behaviour, ui);
            });
    }
}

enum TreePane {
    Intro,
    Topic(pane::TopicPane),
}

impl TreePane {
    pub fn title(&self) -> WidgetText {
        match self {
            TreePane::Intro => "Intro".into(),
            TreePane::Topic(topic_pane) => topic_pane.title(),
        }
    }
}

struct TreeBehavior;

impl egui_tiles::Behavior<TreePane> for TreeBehavior {
    fn pane_ui(
        &mut self,
        ui: &mut Ui,
        _tile_id: egui_tiles::TileId,
        pane: &mut TreePane,
    ) -> egui_tiles::UiResponse {
        let mut response = egui_tiles::UiResponse::None;
        egui::Frame::central_panel(ui.style()).show(ui, |ui| {
            ui.set_width(ui.available_width());
            ui.set_height(ui.available_height());

            match pane {
                TreePane::Intro => {
                    ui.label("hello!");
                    response = egui_tiles::UiResponse::None;
                }
                TreePane::Topic(topic_pane) => {
                    response = topic_pane.ui(ui);
                }
            }
        });
        response
    }

    fn tab_title_for_pane(&mut self, pane: &TreePane) -> WidgetText {
        pane.title()
    }

    fn tab_bar_height(&self, _style: &egui::Style) -> f32 {
        24.0
    }

    fn gap_width(&self, _style: &egui::Style) -> f32 {
        2.0
    }

    fn is_tab_closable(&self, _tiles: &egui_tiles::Tiles<TreePane>, _tile_id: TileId) -> bool {
        true
    }

    fn simplification_options(&self) -> egui_tiles::SimplificationOptions {
        egui_tiles::SimplificationOptions {
            prune_empty_tabs: true,
            prune_empty_containers: true,
            prune_single_child_tabs: true,
            prune_single_child_containers: true,
            all_panes_must_have_tabs: true,
            join_nested_linear_containers: true,
        }
    }
}

fn main() -> anyhow::Result<()> {
    env_logger::init();

    let rt = Runtime::new().unwrap();
    let _enter = rt.enter();

    let mut config_folder: PathBuf;
    let config: config::structure::StructureConfig;
    {
        let config_path_str = std::env::var("KAFKA_TOOL_CONFIG_PATH")
            .context("expected the `KAFKA_TOOL_CONFIG_PATH` environment variable to be set")?;

        let mut config_file_path = PathBuf::new();
        config_file_path.push(config_path_str);

        config_folder = config_file_path.clone();
        config_folder.pop();

        let config_bytes = std::fs::read(&config_file_path).unwrap();
        let config_str = String::from_utf8(config_bytes).unwrap();
        config = toml::from_str(&config_str).unwrap();
    }

    let (lua, lua_state, connect_strategy) = init_lua(&config, &config_folder).unwrap();

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([1200.0, 800.0]),
        ..Default::default()
    };

    eframe::run_native(
        "Kafka Tool",
        options,
        Box::new(|cc| {
            let state = backend::spawn(
                cc.egui_ctx.clone(),
                config,
                lua.clone(),
                lua_state.clone(),
                connect_strategy,
            );

            let mut tiles = egui_tiles::Tiles::default();
            let intro = tiles.insert_pane(TreePane::Intro);
            let root = tiles.insert_tab_tile(vec![intro]);
            let tree = egui_tiles::Tree::new("main_tree", root, tiles);

            let a = KafkaManApp {
                state,
                topic_prefix_filter: String::new(),
                topic_filter: String::new(),
                topics_virtual_list: VirtualList::new(),
                tree,
            };
            Ok(Box::new(a))
        }),
    )
    .map_err(|v| anyhow::anyhow!(v.to_string()))?;

    Ok(())
}
