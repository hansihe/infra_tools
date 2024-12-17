//! A lot of the K8S interaction code is adapted from:
//! https://github.com/hcavarsan/kftray/tree/dc72684b014601bc5fc7fe2ffbdfc6650103a6a9
//! License: MIT

use anyhow::{bail, Context as _};
use kube::Client;
use tokio::io::{AsyncRead, AsyncWrite};

pub mod apis;
pub mod build_env;
pub mod client;
pub mod config_dir;
//pub mod domain_name;
pub mod kube_types;
pub mod socks5;

pub async fn create_client(k8s_context: Option<String>) -> anyhow::Result<Client> {
    //let context: String;
    //if let Some(ctx) = k8s_context {
    //    context = ctx;
    //} else {
    //    let (_, _, contexts) = client::create_client(None, None).await?;
    //    if contexts.len() == 1 {
    //        context = contexts[0].clone();
    //    } else {
    //        bail!("please choose a k8s context: {:?}", contexts);
    //    }
    //}

    //let (client, _config, _contexts) =
    //    client::create_client_with_specific_context(None, Some(&context))
    //        .await
    //        .unwrap();
    //let client = client.context("attempting to get client")?;

    let (client, _kubeconfig) = client::create_client(None, k8s_context.as_deref()).await?;
    Ok(client)
}

/// Copy data between two peers
/// Using 2 different generators, because they could be different structs with same traits.
pub async fn transfer<I, O>(mut inbound: I, mut outbound: O) -> anyhow::Result<()>
where
    I: AsyncRead + AsyncWrite + Unpin,
    O: AsyncRead + AsyncWrite + Unpin,
{
    match tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await {
        Ok(res) => log::info!("transfer closed ({}, {})", res.0, res.1),
        Err(err) => log::error!("transfer error: {:?}", err),
    };

    Ok(())
}
