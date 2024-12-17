use std::{
    collections::HashMap,
    net::{SocketAddr, ToSocketAddrs},
    sync::Arc,
    time::Duration,
};

use anyhow::{bail, Context};
use clap::Parser;
use clap_verbosity_flag::Verbosity;
use colored::Colorize;
use deku::DekuContainerRead;
use dist::PString;
use epmd::NodeDetails;
use etf::{consts::dist_flags, p_dist_message, Atom, AtomCache, EtfTerm};
use futures::{SinkExt, StreamExt};
use k8s_util::{
    apis::Apis,
    kube_types::{PodName, TargetSelector},
};
use nom::AsBytes;
use rand::{distributions::Alphanumeric, thread_rng, Rng as _};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
    sync::OnceCell,
};
use tokio_util::{
    bytes::BytesMut,
    codec::{Framed, LengthDelimitedCodec},
};

mod dist;
mod epmd;
mod etf;
mod proto;

const DEFAULT_COOKIE_ENV_VARS: [&'static str; 4] = [
    "RELEASE_COOKIE",
    "ERLANG_COOKIE",
    "K8S_NODE_COOKIE",
    "ELIXIR_REMSH_COOKIE",
];

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(flatten)]
    verbose: Verbosity,
    #[arg(short, long)]
    context: Option<String>,
    #[arg(short, long)]
    namespace: String,
    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Subcommand, Debug)]
enum Commands {
    ProxyDeployment {
        deployment_name: String,
        /// Port to use for connecting to EPMD
        #[arg(long, default_value = "4369")]
        epmd_port: u16,
        /// Manually sets the erlang distribution cookie to use instead
        /// of randomly generating it
        #[arg(long)]
        local_cookie: Option<String>,
        /// Sets env var(s) in deployment to look in to resolve target cookie
        #[arg(long)]
        remote_cookie_env_var: Vec<String>,
    },
}

#[derive(Default, Debug)]
struct State {
    creation: OnceCell<u32>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    env_logger::Builder::new()
        .filter_level(args.verbose.log_level_filter())
        .init();

    let namespace = args.namespace;

    let client = k8s_util::create_client(None)
        .await
        .context("attempting to make k8s client")?;
    let apis = Arc::new(Apis::namespaced(&client, &namespace));

    match args.command {
        Commands::ProxyDeployment {
            deployment_name,
            epmd_port,
            local_cookie,
            remote_cookie_env_var,
        } => {
            let mut rng = thread_rng();

            let in_cookie: String = if let Some(cookie) = local_cookie {
                cookie
            } else {
                (0..24).map(|_| rng.sample(Alphanumeric) as char).collect()
            };

            let deployment = apis
                .deployment
                .get(&deployment_name)
                .await
                .context("failed to get deployment")?;

            let pod_spec = deployment
                .spec
                .context("deployment has no spec")?
                .template
                .spec
                .context("deployment podtemplate has no spec")?;

            let container_spec = &pod_spec.containers[0];

            let mut env = HashMap::new();
            apis.resolve_env(&container_spec.env, &container_spec.env_from, &mut env)
                .await
                .context("when resolving environment for pods in deployment")?;

            let target_cookie = if remote_cookie_env_var.len() > 0 {
                remote_cookie_env_var.iter().find_map(|v| env.get(&*v))
            } else {
                DEFAULT_COOKIE_ENV_VARS.iter().find_map(|v| env.get(*v))
            };
            let target_cookie =
                target_cookie.context("Could not find cookie in pod environment.")?;

            let target_selector = TargetSelector::DeploymentName(deployment_name.clone());

            let short_name;
            {
                let target_pod_name = apis
                    .find_pod(&target_selector)
                    .await
                    .context("when finding pod")?;
                println!(
                    "{}",
                    format!("! selected pod `{}`", target_pod_name.name).blue()
                );

                let remote_epmd_socket =
                    get_port_forward(&apis, &target_pod_name, DEFAULT_EPMD_PORT)
                        .await
                        .context("when getting EPMD port forward")?;
                let all_nodes = epmd::read_all_nodes_sock(remote_epmd_socket).await?;

                if all_nodes.len() != 1 {
                    log::warn!(
                        "Found multiple nodes registered in EPMD in {} pod: {:?}",
                        target_pod_name.name,
                        all_nodes
                    );
                    bail!(
                        "found EPMD in target pod, but had {} registered nodes, expected 1",
                        all_nodes.len()
                    );
                }

                short_name = all_nodes[0].name.clone();
            }

            let long_name: String = format!("{}@127.0.0.1", short_name);

            println!("");
            println!("How to connect:");
            println!("iex:");
            println!(
                "  * Start IEX with `iex --name debug@127.0.0.1 --cookie {} --hidden`",
                in_cookie
            );
            println!("  * Run `Node.connect(:\"{}\")`", long_name);
            println!("LiveBook:");
            println!("  * Run `Node.connect(:\"{}\")`", long_name);
            println!("");

            let epmd_addr: SocketAddr = ("localhost", epmd_port).to_socket_addrs()?.next().unwrap();

            let dist_bind = TcpListener::bind(("127.0.0.1", 0))
                .await
                .context("when binding local dist port")?;
            let dist_port = dist_bind.local_addr().unwrap().port();

            let _arc_env = Arc::new(env.clone());

            let arc_state = Arc::new(State::default());

            let _handle = tokio::spawn(dist_accept_loop(
                dist_bind,
                arc_state.clone(),
                apis.clone(),
                long_name.clone(),
                in_cookie.clone(),
                target_cookie.clone(),
                target_selector.clone(),
            ));

            let details = NodeDetails {
                name: short_name.clone(),
                port: dist_port,
            };
            let registration = epmd::register_node(&epmd_addr, &details)
                .await
                .context("when registering node in EPMD")?;
            arc_state.creation.set(registration.creation).unwrap();

            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }

    //Ok(())
}

async fn dist_accept_loop(
    dist_bind: TcpListener,
    state: Arc<State>,
    apis: Arc<Apis>,
    virtual_long_name: String,
    in_cookie: String,
    target_cookie: String,
    target_selector: TargetSelector,
) -> anyhow::Result<()> {
    loop {
        let (stream, _addr) = dist_bind.accept().await?;

        let state = state.clone();
        let apis = apis.clone();
        let virtual_long_name = virtual_long_name.clone();
        let in_cookie = in_cookie.clone();
        let target_cookie = target_cookie.clone();
        let target_selector = target_selector.clone();
        tokio::spawn(async move {
            let result = handle_erl_fun(
                stream,
                state,
                apis,
                virtual_long_name,
                in_cookie,
                target_cookie,
                target_selector,
            )
            .await;
            log::warn!("connection closed: {:?}", result);
        });
    }
}

async fn handle_erl_fun(
    mut stream: TcpStream,
    _state: Arc<State>,
    apis: Arc<Apis>,
    virtual_long_name: String,
    in_cookie: String,
    target_cookie: String,
    target_selector: TargetSelector,
) -> anyhow::Result<()> {
    // 1. Receive `SendName` from in (A)
    // 2. Lookup target pod in K8s
    // 3. Connect to EPMD in target pod, lookup target node name and port
    // 4. Connect to BEAM in target pod (B), send `SendName`
    // 5. Receive `RecvStatus` from (B), send to (A)
    // 6. Receive `RecvChallenge` from (B), send to (A)

    let data = dist::read_framed(&mut stream)
        .await
        .context("when reading dist message")?;
    let (_, in_send_name) =
        dist::SendName::from_bytes((&data, 0)).context("when parsing dist message")?;

    log::info!("local node A, remote node B, proxy P");
    log::info!(
        "A->P    SendName: name: {} flags: {} creation: {}",
        in_send_name.name.as_str().unwrap(),
        in_send_name.flags,
        in_send_name.creation
    );

    //let in_name_me = in_send_name.flags & NAME_ME_MASK != 0;
    //if !in_name_me {
    //    bail!("connecting node was named!");
    //}

    let target_pod_name = apis
        .find_pod(&target_selector)
        .await
        .context("when finding pod")?;
    println!(
        "{}",
        format!("! selected pod `{}`", target_pod_name.name).blue()
    );

    let remote_epmd_socket = get_port_forward(&apis, &target_pod_name, DEFAULT_EPMD_PORT)
        .await
        .context("when getting EPMD port forward")?;
    let all_nodes = epmd::read_all_nodes_sock(remote_epmd_socket).await?;

    if all_nodes.len() != 1 {
        log::warn!(
            "Found multiple nodes registered in EPMD in {} pod: {:?}",
            target_pod_name.name,
            all_nodes
        );
        bail!(
            "found EPMD in target pod, but had {} registered nodes, expected 1",
            all_nodes.len()
        );
    }

    let node_details = &all_nodes[0];
    //let remote_epmd_socket = get_empd_port_forward(&apis, &target_pod_name)
    //    .await
    //    .context("when getting EPMD port forward")?;
    //epmd::get_dist_port_stream(remote_epmd_socket, &node_details.name)
    //    .await
    //    .context("when getting port details for target node from EPMD")?;

    println!(
        "{}",
        format!(
            "! forwarding distribution connection to node '{}' on port {} in pod '{}'",
            node_details.name, node_details.port, target_pod_name.name
        )
        .blue()
    );

    let mut target_dist_sock = get_port_forward(&apis, &target_pod_name, node_details.port)
        .await
        .context("when attempting to connect to distribution port of target")?;

    let out_send_name = dist::SendName {
        //flags: in_send_name.flags | NAME_ME_MASK,
        flags: in_send_name.flags & !dist_flags::DFLAG_FRAGMENTS,
        creation: in_send_name.creation,
        name: in_send_name.name.clone(),
    };
    log::info!(
        "   P->B SendName: name: {} flags: {} creation: {}",
        out_send_name.name.as_str().unwrap(),
        out_send_name.flags,
        out_send_name.creation
    );
    let out_send_name_data: Vec<u8> = out_send_name.try_into()?;
    dist::write_framed(&mut target_dist_sock, &out_send_name_data).await?;

    let status_bytes = dist::read_framed(&mut target_dist_sock).await?;
    let (_, _recv_status) = dist::RecvStatus::from_bytes((&status_bytes, 0)).unwrap();
    log::info!(
        //"   P<-B RecvStatusNamed: name: {} creation: {}",
        "   P<-B RecvStatusNamed: ok",
    );

    //let status_str = std::str::from_utf8(&status_bytes).unwrap();
    //if !status_str.starts_with("sok") {
    //    bail!("unsupported status: {}", status_str);
    //}

    dist::write_framed(&mut stream, &status_bytes).await?;
    log::info!(
        "A<-P    RecvStatusNamed: ok",
        //recv_status.name.as_str().unwrap(),
        //recv_status.creation
    );

    // Challenge exchange with peer B
    // Receive challenge
    let recv_challenge_data = dist::read_framed(&mut target_dist_sock).await?;
    let (_, recv_challenge) = dist::RecvChallenge::from_bytes((&recv_challenge_data, 0)).unwrap();
    log::info!(
        "   P<-B RecvChallenge: name: {} challenge: {} creation: {} flags: {}",
        recv_challenge.name.as_str().unwrap(),
        recv_challenge.challenge,
        recv_challenge.creation,
        recv_challenge.flags
    );

    let remote_long_name = recv_challenge.name.clone();

    {
        // Digest
        let b_digest_pt = format!("{}{}", target_cookie, recv_challenge.challenge);
        let b_digest_digest = md5::compute(b_digest_pt.as_bytes());

        // Send challenge reply
        let msg = dist::SendChallengeReply {
            challenge: recv_challenge.challenge,
            digest: b_digest_digest.0,
        };
        log::info!(
            "   P->B SendChallengeReply: challenge: {} digest: {:?}",
            msg.challenge,
            msg.digest
        );
        let msg_bin: Vec<u8> = msg.try_into()?;
        dist::write_framed(&mut target_dist_sock, &msg_bin).await?;

        let data = dist::read_framed(&mut target_dist_sock).await?;
        let (_, recv_challenge_ack) = dist::RecvChallengeAck::from_bytes((&data, 0)).unwrap();
        log::info!(
            "   P<-B RecvChallengeAck: digest: {:?}",
            recv_challenge_ack.digest
        );
    }

    log::info!("finished distribution handshake with peer B");

    // Challenge exchange with peer A
    {
        let challenge = dist::RecvChallenge {
            flags: recv_challenge.flags & !dist_flags::DFLAG_FRAGMENTS,
            challenge: recv_challenge.challenge,
            creation: recv_challenge.creation,
            //name: PString::new(&full_pod_name),
            //name: PString::new("partners-api-gql@127.0.0.1"),
            name: PString::new(&virtual_long_name),
        };
        // Send challenge
        let challenge_bin: Vec<u8> = challenge.clone().try_into()?;
        dist::write_framed(&mut stream, &challenge_bin).await?;
        log::info!(
            "A<-P    RecvChallenge: name: {} challenge: {} creation: {} flags: {}",
            challenge.name.as_str().unwrap(),
            challenge.challenge,
            challenge.creation,
            challenge.flags
        );

        // Digest
        let b_digest_pt = format!("{}{}", in_cookie, recv_challenge.challenge);
        let b_digest_digest = md5::compute(b_digest_pt.as_bytes());

        // Receive challenge reply
        let data = dist::read_framed(&mut stream).await?;
        let (_, send_challenge_reply) = dist::SendChallengeReply::from_bytes((&data, 0)).unwrap();
        log::info!(
            "A->P    SendChallengeReply: challenge: {} digest: {:?}",
            send_challenge_reply.challenge,
            send_challenge_reply.digest,
        );

        // Validate
        if b_digest_digest.0 != send_challenge_reply.digest {
            bail!("mismatching digest! was iex started with the cookie flag?");
        }

        // Response digest
        let b_digest_pt = format!("{}{}", in_cookie, send_challenge_reply.challenge);
        let b_digest_digest = md5::compute(b_digest_pt.as_bytes());

        let challenge_ack = dist::RecvChallengeAck {
            digest: b_digest_digest.0,
        };
        let challenge_ack_bin: Vec<u8> = challenge_ack.clone().try_into()?;
        dist::write_framed(&mut stream, &challenge_ack_bin)
            .await
            .unwrap();
        log::info!(
            "A<-P    RecvChallengeAck: digest: {:?}",
            challenge_ack.digest
        );
    }

    println!("{}", format!("! forwarding traffic").blue());
    //transfer(&mut target_dist_sock, &mut stream).await.unwrap();

    let mut local_transport = Framed::new(stream, LengthDelimitedCodec::new());
    let mut remote_transport = Framed::new(target_dist_sock, LengthDelimitedCodec::new());

    let mut local_atom_cache = AtomCache::new();
    let mut remote_atom_cache = AtomCache::new();

    let map_control_message = |msg: &mut EtfTerm, direction: bool| {
        msg.map_term(&mut |term| match term {
            EtfTerm::Atom(atom, cache_num) => {
                if direction {
                    if atom.0 == remote_long_name.data.as_bytes() {
                        *atom = Atom(virtual_long_name.clone().into());
                        *cache_num = None;
                    }
                } else {
                    if atom.0 == virtual_long_name.as_bytes() {
                        *atom = Atom(remote_long_name.data.clone());
                        *cache_num = None;
                    }
                }
            }
            _ => (),
        });
    };

    loop {
        tokio::select! {
            msg = local_transport.next() => {
                let bytes = msg.unwrap()?.freeze();
                if bytes.len() > 0 {
                    //println!("A->B: RAW {:?}", bytes);
                    let (_, mut message) = p_dist_message(&*bytes, &mut local_atom_cache).unwrap();
                    //log::info!("A->B: MSG BEFOR {:?} {:?} {:?}", message.control_typ, message.control, message.arg);

                    //let header = DistributionHeader::decode(&*bytes).unwrap();
                    //if ret.is_err() {
                    //    File::create_new("dump.bin").unwrap().write_all(&*bytes).unwrap();
                    //}

                    let mut control_buf = Vec::new();

                    map_control_message(&mut message.control, false);
                    message.control.write(&mut control_buf).unwrap();

                    if let Some(arg) = &mut message.arg {
                        map_control_message(arg, false);
                        arg.write(&mut control_buf).unwrap();
                    }

                    log::info!("A->B: HDR AFTER {:?} {:?} {:?}", message.control_typ, message.control, message.arg);

                    let mut buf = BytesMut::new();
                    buf.extend_from_slice(message.header_slice);
                    buf.extend_from_slice(&control_buf);

                    remote_transport.send(buf.into()).await.unwrap();
                } else {
                    remote_transport.send(bytes).await.unwrap();
                }
            }
            msg = remote_transport.next() => {
                let bytes = msg.unwrap()?.freeze();
                if bytes.len() > 0 {
                    //println!("A<-B: RAW {:?}", bytes);
                    let (_, mut message) = p_dist_message(&*bytes, &mut remote_atom_cache).unwrap();
                    log::info!("A<-B: MSG BEFOR {:?} {:?} {:?}", message.control_typ, message.control, message.arg);

                    if true {
                        let mut control_buf = Vec::new();

                        map_control_message(&mut message.control, true);
                        message.control.write(&mut control_buf).unwrap();

                        if let Some(arg) = &mut message.arg {
                            map_control_message(arg, true);
                            arg.write(&mut control_buf).unwrap();
                        }

                        log::info!("A<-B: HDR AFTER {:?} {:?} {:?}", message.control_typ, message.control, message.arg);

                        let mut buf = BytesMut::new();
                        buf.extend_from_slice(message.header_slice);
                        buf.extend_from_slice(&control_buf);

                        local_transport.send(buf.into()).await.unwrap();
                    } else {
                        local_transport.send(bytes).await.unwrap();
                    }
                } else {
                    local_transport.send(bytes).await.unwrap();
                }
            }
        }
    }

    //Ok(())
}

const DEFAULT_EPMD_PORT: u16 = 4369;

async fn get_port_forward(
    apis: &Apis,
    pod_name: &PodName,
    port: u16,
) -> anyhow::Result<impl AsyncRead + AsyncWrite + Unpin> {
    let mut port_forwarder = apis
        .pod
        .portforward(&pod_name.name, &[port])
        .await
        .context("when forwarding port")?;

    let epmd_port_forward = port_forwarder
        .take_stream(port)
        .context("could not get port of target")?;

    Ok(epmd_port_forward)
}
