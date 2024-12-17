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
use k8s_openapi::api::core::v1::EnvVar;
use k8s_util::{
    apis::Apis,
    build_env::{deployment_get_pod_spec, pod_spec_get_env},
    kube_types::{PodName, Target, TargetPod, TargetSelector},
};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

mod dist;
mod epmd;

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
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    env_logger::Builder::new()
        .filter_level(args.verbose.log_level_filter())
        .init();

    let namespace = args.namespace;

    let hostname_os = hostname::get().context("when getting hostname")?;
    let hostname = hostname_os.into_string().ok().unwrap();

    let context;
    if let Some(ctx) = args.context {
        context = ctx;
    } else {
        let (_, _, contexts) =
            k8s_util::client::create_client_with_specific_context(None, None).await?;
        if contexts.len() == 1 {
            context = contexts[0].clone();
        } else {
            bail!("please choose a k8s context: {:?}", contexts);
        }
    }

    let (client, _config, _contexts) =
        k8s_util::client::create_client_with_specific_context(None, Some(&context))
            .await
            .unwrap();
    let client = client.context("attempting to get client")?;
    let apis = Arc::new(Apis::namespaced(&client, &namespace));

    match args.command {
        Commands::ProxyDeployment {
            deployment_name,
            epmd_port,
        } => {
            let mut rng = thread_rng();
            let in_cookie: String = (0..24).map(|_| rng.sample(Alphanumeric) as char).collect();

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

            let target_cookie = env.get("RELEASE_COOKIE").or_else(|| env.get("ERLANG_COOKIE")).or_else(|| env.get("K8S_NODE_COOKIE")).context("Could not find cookie in pod environment. Either `RELEASE_COOKIE` or `ERLANG_COOKIE` must be set.")?;

            let short_name = format!("{}--{}--k8s", deployment_name, namespace);
            let long_name = format!(
                "{}{}{}{}{}",
                deployment_name, "--", namespace, "--k8s@", hostname
            );

            let target_selector = TargetSelector::DeploymentName(deployment_name.clone());

            //let deployments = apis
            //    .deployment
            //    .list(&kube::api::ListParams {
            //        label_selector: None,
            //        field_selector: None,
            //        timeout: None,
            //        limit: None,
            //        continue_token: None,
            //        version_match: None,
            //        resource_version: None,
            //    })
            //    .await
            //    .context("listing deployments")?;

            //// Identify all deployments which have the required RELEASE_NODE and RELEASE_COOKIE
            //// environment variables set
            //let mut connectable = Vec::new();
            //for depl in deployments.items.iter() {
            //    if let Some(env) = deployment_get_pod_spec(&depl).and_then(pod_spec_get_env) {
            //        //let node_opt = env.iter().find(|v| v.name == "RELEASE_NODE");
            //        let cookie_opt = env
            //            .iter()
            //            .find(|v| v.name == "RELEASE_COOKIE")
            //            .or_else(|| env.iter().find(|v| v.name == "ERLANG_COOKIE"));
            //        if let Some(_node) = cookie_opt {
            //            //let target: Target = ::new(pod_name, port_number)
            //            connectable.push((
            //                depl.metadata.name.clone().unwrap(),
            //                env.clone(),
            //                TargetSelector::DeploymentName(
            //                    depl.metadata
            //                        .name
            //                        .clone()
            //                        .context("deployment has no name")?,
            //                ),
            //            ));
            //        }
            //    }
            //}

            //println!(
            //    "{}",
            //    format!("! queried deployments in namespace {}", namespace).blue()
            //);

            //if connectable.len() == 0 {
            //    println!(
            //        "{} ({} total)",
            //        "No connectable deployments found!".red(),
            //        deployments.items.len()
            //    );
            //    return Ok(());
            //} else {
            //    println!(
            //        "Found {} connectable deployments in namespace:",
            //        connectable.len()
            //    );
            //}
            //for (name, _, _) in &connectable {
            //    println!(
            //        "- {}{}{}{}{}",
            //        name.green(),
            //        "--",
            //        namespace.green(),
            //        "--k8s@",
            //        hostname
            //    );
            //}
            //println!("HINT: Connectable deployments must have RELEASE_NODE and RELEASE_COOKIE environment variables set, which enable Erlang distribution");
            //println!("");

            println!(
                "1. Start IEX with `iex --sname undefined --cookie {} --hidden`",
                in_cookie
            );
            println!("2. Run `Node.connect(:\"{}\")`", short_name);
            println!("");

            let epmd_addr: SocketAddr = ("localhost", epmd_port).to_socket_addrs()?.next().unwrap();

            //let mut epmd_registrations = Vec::new();
            //for (name, env, target_selector) in &connectable {
            let dist_bind = TcpListener::bind(("0.0.0.0", 0))
                .await
                .context("when binding local dist port")?;
            let dist_port = dist_bind.local_addr().unwrap().port();

            let arc_env = Arc::new(env.clone());

            let handle = tokio::spawn(dist_accept_loop(
                dist_bind,
                apis.clone(),
                short_name.clone(),
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

            //epmd_registrations.push((registration, handle));
            //}

            //println!(
            //    "{}",
            //    format!(
            //        "! registered {} name(s) in EPMD, ready to forward",
            //        epmd_registrations.len()
            //    )
            //    .blue()
            //);

            //let nodes = epmd::read_all_nodes(&epmd_addr).await?;
            //println!("{:#?}", nodes);

            //epmd::get_dist_port(&epmd_addr, "iex2").await?;
            //epmd::get_dist_port(&epmd_addr, "pricetag-deployment--pricetag-prod--k8s").await?;

            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }

    //Ok(())
}

async fn dist_accept_loop(
    dist_bind: TcpListener,
    apis: Arc<Apis>,
    full_pod_name: String,
    in_cookie: String,
    target_cookie: String,
    target_selector: TargetSelector,
) -> anyhow::Result<()> {
    loop {
        let (stream, addr) = dist_bind.accept().await?;

        let apis = apis.clone();
        let full_pod_name = full_pod_name.clone();
        let in_cookie = in_cookie.clone();
        let target_cookie = target_cookie.clone();
        let target_selector = target_selector.clone();
        tokio::spawn(async move {
            let result = handle_erl_fun(
                stream,
                apis,
                full_pod_name,
                in_cookie,
                target_cookie,
                target_selector,
            )
            .await;
            println!("{:?}", result);
        });
    }
}

const NAME_ME_MASK: u64 = 1 << 33;

async fn handle_erl_fun(
    mut stream: TcpStream,
    apis: Arc<Apis>,
    full_pod_name: String,
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

    //let node_name = apis
    //    .resolve_env_var(&*env, "RELEASE_NODE")
    //    .await
    //    .context("while resolving RELEASE_NODE env var")?;

    let data = dist::read_framed(&mut stream)
        .await
        .context("when reading dist message")?;
    let (_, in_send_name) =
        dist::SendName::from_bytes((&data, 0)).context("when parsing dist message")?;

    log::info!(
        "A-> SendName: name: {} flags: {} creation: {}",
        in_send_name.name.as_str().unwrap(),
        in_send_name.flags,
        in_send_name.creation
    );

    let in_name_me = in_send_name.flags & NAME_ME_MASK != 0;
    if !in_name_me {
        bail!("connecting node was named!");
    }

    let target_pod_name = apis
        .find_pod(&target_selector)
        .await
        .context("when finding pod")?;
    println!(
        "{}",
        format!("! selected pod `{}`", target_pod_name.0).blue()
    );

    let remote_epmd_socket = get_port_forward(&apis, &target_pod_name, DEFAULT_EPMD_PORT)
        .await
        .context("when getting EPMD port forward")?;
    let all_nodes = epmd::read_all_nodes_sock(remote_epmd_socket).await?;

    if all_nodes.len() != 1 {
        log::warn!(
            "Found multiple nodes registered in EPMD in {} pod: {:?}",
            target_pod_name.0,
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
            node_details.name, node_details.port, target_pod_name.0
        )
        .blue()
    );

    let mut target_dist_sock = get_port_forward(&apis, &target_pod_name, node_details.port)
        .await
        .context("when attempting to connect to distribution port of target")?;

    let out_send_name = dist::SendName {
        flags: in_send_name.flags | NAME_ME_MASK,
        creation: in_send_name.creation,
        name: in_send_name.name.clone(),
    };
    log::info!(
        "->B SendName: name: {} flags: {} creation: {}",
        out_send_name.name.as_str().unwrap(),
        out_send_name.flags,
        out_send_name.creation
    );
    let out_send_name_data: Vec<u8> = out_send_name.try_into()?;
    dist::write_framed(&mut target_dist_sock, &out_send_name_data).await?;

    let status_bytes = dist::read_framed(&mut target_dist_sock).await?;
    let (_, recv_status_named) = dist::RecvStatusNamed::from_bytes((&status_bytes, 0)).unwrap();
    log::info!(
        "B-> RecvStatusNamed: name: {} creation: {}",
        recv_status_named.name.as_str().unwrap(),
        recv_status_named.creation
    );

    //let status_str = std::str::from_utf8(&status_bytes).unwrap();
    //if !status_str.starts_with("sok") {
    //    bail!("unsupported status: {}", status_str);
    //}

    dist::write_framed(&mut stream, &status_bytes).await?;
    log::info!(
        "->A RecvStatusNamed: name: {} creation: {}",
        recv_status_named.name.as_str().unwrap(),
        recv_status_named.creation
    );

    // Challenge exchange with peer B
    // Receive challenge
    let recv_challenge_data = dist::read_framed(&mut target_dist_sock).await?;
    let (_, recv_challenge) = dist::RecvChallenge::from_bytes((&recv_challenge_data, 0)).unwrap();
    log::info!(
        "B-> RecvChallenge: name: {} challenge: {} creation: {} flags: {}",
        recv_challenge.name.as_str().unwrap(),
        recv_challenge.challenge,
        recv_challenge.creation,
        recv_challenge.flags
    );

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
            "->B SendChallengeReply: challenge: {} digest: {:?}",
            msg.challenge,
            msg.digest
        );
        let msg_bin: Vec<u8> = msg.try_into()?;
        dist::write_framed(&mut target_dist_sock, &msg_bin).await?;

        let data = dist::read_framed(&mut target_dist_sock).await?;
        let (_, recv_challenge_ack) = dist::RecvChallengeAck::from_bytes((&data, 0)).unwrap();
        log::info!(
            "B-> RecvChallengeAck: digest: {:?}",
            recv_challenge_ack.digest
        );
    }

    log::info!("finished distribution handshake with peer B");

    // Challenge exchange with peer A
    {
        let challenge = dist::RecvChallenge {
            flags: recv_challenge.flags,
            challenge: recv_challenge.challenge,
            creation: recv_challenge.creation,
            name: PString::new(&full_pod_name),
        };
        // Send challenge
        let challenge_bin: Vec<u8> = recv_challenge_data.try_into()?;
        dist::write_framed(&mut stream, &challenge_bin).await?;
        log::info!(
            "->A RecvChallenge: name: {} challenge: {} creation: {} flags: {}",
            challenge.name.as_str().unwrap(),
            challenge.challenge,
            challenge.creation,
            challenge.flags
        );

        // Digest
        let b_digest_pt = format!("{}{}", in_cookie, recv_challenge.challenge);
        let b_digest_digest = md5::compute(b_digest_pt.as_bytes());

        println!("wrote peer A challenge, waiting for response..");

        // Receive challenge reply
        let data = dist::read_framed(&mut stream).await?;
        let recv_challenge = dist::SendChallengeReply::from_bytes((&data, 0)).unwrap();

        // Validate
        if b_digest_digest.0 != recv_challenge.1.digest {
            bail!("mismatching digest! was iex started with the cookie flag?");
        }
    }

    Ok(())
}

const DEFAULT_EPMD_PORT: u16 = 4369;

async fn get_port_forward(
    apis: &Apis,
    pod_name: &PodName,
    port: u16,
) -> anyhow::Result<impl AsyncRead + AsyncWrite + Unpin> {
    let mut port_forwarder = apis
        .pod
        .portforward(&pod_name.0, &[port])
        .await
        .context("when forwarding port")?;

    let epmd_port_forward = port_forwarder
        .take_stream(port)
        .context("could not get port of target")?;

    Ok(epmd_port_forward)
}
