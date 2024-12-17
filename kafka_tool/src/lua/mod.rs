use std::{
    collections::{BTreeMap, HashMap},
    env::args,
    path::Path,
    sync::{Arc, Mutex},
};

use anyhow::Context;
use mlua::{Function, LuaSerdeExt};
use serde::Deserialize;

fn lua_module(
    lua: &mlua::Lua,
    lua_state_inner: Arc<LuaData>,
    data_dir: String,
) -> anyhow::Result<mlua::Table> {
    let table = lua.create_table()?;

    table.set(
        "parse_json",
        lua.create_function(|lua, (data,): (mlua::BString,)| {
            match serde_json::from_slice::<serde_json::Value>(&*data) {
                Ok(val) => Ok(lua.to_value(&val)?),
                Err(e) => Err(mlua::Error::RuntimeError(e.to_string())),
            }
        })?,
    )?;

    table.set(
        "prepare_protobuf",
        lua.create_function(|lua, (inputs, includes): (Vec<String>, Vec<String>)| {
            use protobuf::reflect;
            let mut descriptors = protobuf_parse::Parser::new()
                .pure()
                .includes(includes)
                .inputs(inputs)
                .parse_and_typecheck()
                .unwrap()
                .file_descriptors;

            let mut dyns = Vec::new();
            for descr in descriptors.drain(..) {
                dyns.push(reflect::FileDescriptor::new_dynamic(descr, &dyns).unwrap());
            }

            let table = lua.create_table()?;

            let mut message_map = HashMap::new();
            for descr in dyns.iter() {
                for message in descr.messages() {
                    message_map.insert(message.full_name().to_string(), message.clone());

                    let mt = lua.create_table()?;

                    let message_inner = message.clone();
                    mt.set(
                        "parse",
                        lua.create_function(move |lua, (data,): (mlua::BString,)| {
                            match message_inner.parse_from_bytes(&data) {
                                Ok(msg) => {
                                    // TODO options?
                                    let json_str =
                                        protobuf_json_mapping::print_to_string(&*msg).unwrap();
                                    let json_val: serde_json::Value =
                                        serde_json::from_str(&json_str).unwrap();
                                    Ok(lua.to_value(&json_val))
                                }
                                Err(e) => Err(mlua::Error::RuntimeError(e.to_string())),
                            }
                        })?,
                    )?;
                    table.set(message.full_name(), mt)?;
                }
            }

            //table.set("get_message", value)

            Ok(table)
        })?,
    )?;

    table.set("DATADIR", data_dir)?;

    table.set(
        "register_record_formatter",
        lua.create_function(move |_lua, (name, fmter): (String, Function)| {
            let mut lock = lua_state_inner.record_formatters.lock().unwrap();
            lock.insert(name, fmter);
            Ok(())
        })?,
    )?;

    Ok(table)
}

#[derive(Default)]
pub struct LuaData {
    pub record_formatters: Mutex<BTreeMap<String, Function>>,
}

pub fn init_lua(
    config: &crate::config::structure::StructureConfig,
    rel_path: &Path,
) -> anyhow::Result<(mlua::Lua, Arc<LuaData>, ConnectStrategy)> {
    let lua = mlua::Lua::new();
    let lua_state = Arc::new(LuaData::default());

    let path_str = rel_path.to_str().context("invalid path")?.to_owned();

    let args: Vec<_> = args().skip(1).collect();
    lua.globals().set("arg", args)?;

    lua.globals().get::<mlua::Table>("package")?.set(
        "path",
        format!("{}/?.lua;{}/?/init.lua;;", path_str, path_str),
    )?;

    let lua_state_inner = lua_state.clone();
    lua.globals()
        .get::<mlua::Table>("package")?
        .get::<mlua::Table>("preload")?
        .set(
            "kafka_tool",
            lua.create_function(move |lua, ()| {
                Ok(lua_module(lua, lua_state_inner.clone(), path_str.clone()).unwrap())
            })?,
        )?;

    let lua_require: mlua::Function = lua.globals().get("require")?;
    for file_path in config.lua_require.iter() {
        let _: mlua::Value = lua_require.call((&**file_path,))?;
    }

    let bootstrap: mlua::Function = lua
        .globals()
        .get("Bootstrap")
        .context("error getting bootstrap function from lua script")?;

    let ret_value: mlua::Value = bootstrap
        .call(())
        .context("error while calling bootstrap function")?;

    let connect_strategy: ConnectStrategy = lua
        .from_value(ret_value)
        .context("error while deserializing bootstrap return value")?;

    Ok((lua, lua_state, connect_strategy))
}
#[derive(Debug, Deserialize)]
#[serde(tag = "strategy")]
pub enum ConnectStrategy {
    #[serde(rename = "kubernetes_service")]
    KubernetesService {
        namespace: String,
        service: String,
        service_port: u16,
    },
}
