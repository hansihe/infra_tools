use std::collections::HashMap;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct StructureConfig {
    //#[serde(default)]
    //pub show_unknown_topics: bool,
    //#[serde(default)]
    //pub lua_package_path: String,
    #[serde(default)]
    pub lua_require: Vec<String>,

    pub topic: Vec<TopicConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TopicConfig {
    /// Visual name for the topic
    pub name: String,
    /// Regex to match the topic identifier
    pub regex: String,
    pub formatter: String,
    /// Data
    #[serde(default)]
    pub extra: HashMap<String, String>,
}
