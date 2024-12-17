use core::str;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{bail, Context};
use socks5_proto::handshake::{
    Method as HandshakeMethod, Request as HandshakeRequest, Response as HandshakeResponse,
};
use socks5_proto::{Address, Command, Error, ProtocolError, Reply, Request, Response};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::task::JoinHandle;
use tokio::{io::AsyncWriteExt as _, net::TcpListener};

use crate::apis::Cluster;
use crate::kube_types::AnyReady;

/// Listens on an address as a SOCKS5 proxy, forwards any connections
/// to entities in the specified kubernetes cluster with the provided
/// namespace as default.
pub async fn listen_socks5_forward(
    cluster: Arc<Cluster>,
    default_ns: String,
    addr: impl ToSocketAddrs,
) -> anyhow::Result<(JoinHandle<()>, SocketAddr)> {
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;

    let handle = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let apis_inner = cluster.clone();
            let default_ns = default_ns.clone();
            tokio::spawn(async move {
                if let Err(error) = accept_conn(apis_inner, default_ns, stream).await {
                    log::warn!("terminated k8s socks5 connection: {:?}", error);
                }
            });
        }
    });

    Ok((handle, local_addr))
}

async fn accept_conn(
    cluster: Arc<Cluster>,
    default_ns: String,
    mut stream: TcpStream,
) -> anyhow::Result<()> {
    let hs_req = HandshakeRequest::read_from(&mut stream).await?;

    if hs_req.methods.contains(&HandshakeMethod::NONE) {
        let hs_resp = HandshakeResponse::new(HandshakeMethod::NONE);
        hs_resp.write_to(&mut stream).await?;
    } else {
        let hs_resp = HandshakeResponse::new(HandshakeMethod::UNACCEPTABLE);
        hs_resp.write_to(&mut stream).await?;
        let _ = stream.shutdown().await;
        Err(Error::Protocol(
            ProtocolError::NoAcceptableHandshakeMethod {
                version: socks5_proto::SOCKS_VERSION,
                chosen_method: HandshakeMethod::NONE,
                methods: hs_req.methods,
            },
        ))?;
    }

    let req = match Request::read_from(&mut stream).await {
        Ok(req) => req,
        Err(err) => {
            let resp = Response::new(Reply::GeneralFailure, Address::unspecified());
            resp.write_to(&mut stream).await?;
            let _ = stream.shutdown().await;
            Err(err)?;
            unreachable!()
        }
    };

    match req.command {
        Command::Connect => match &req.address {
            Address::SocketAddress(socket_addr) => {
                unimplemented!("k8s port forward for ip not implemented: {}", socket_addr);
            }
            Address::DomainAddress(vec, port) => {
                let addr = str::from_utf8(vec).unwrap();

                let entity = cluster
                    .resolve_domain_name(addr, &default_ns)
                    .await
                    .unwrap()
                    .with_context(|| format!("domain name not found: {}", addr))
                    .unwrap();

                let pod = cluster
                    .find_pod_by_resolved_entity(&entity, &AnyReady)
                    .await
                    .unwrap();

                log::info!(
                    "found pod {} in {} for socks forward",
                    pod.name,
                    pod.namespace
                );

                let mut port_forward = cluster
                    .get_namespace(&pod.namespace)
                    .pod
                    .portforward(&pod.name, &[*port])
                    .await
                    .unwrap();
                let mut remote_stream = port_forward.take_stream(*port).unwrap();

                // TODO is there a way to actually get this?
                // As far as I can tell the from address of k8s port-forward is unspecified.
                let resp = Response::new(Reply::Succeeded, Address::unspecified());
                resp.write_to(&mut stream).await?;

                log::info!("successfully forwarded port {}, relaying traffic..", *port);

                crate::transfer(&mut remote_stream, &mut stream)
                    .await
                    .unwrap();

                Ok(())
            }
        },
        Command::Associate => {
            log::error!("attempted to ASSOCIATE over k8s socks5 proxy, operation is not supported");
            bail!("associate not supported")
        }
        Command::Bind => {
            log::error!("attempted to BIND over k8s socks5 proxy, operation is not supported");
            bail!("bind not supported")
        }
    }
}
