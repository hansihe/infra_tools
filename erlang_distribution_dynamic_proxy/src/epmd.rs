use anyhow::{bail, Context, Result};
use std::{
    io::{BufWriter, Write},
    net::{Ipv4Addr, SocketAddr, ToSocketAddrs},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{TcpSocket, TcpStream},
    pin,
};

#[derive(Debug)]
pub struct NodeDetails {
    pub name: String,
    pub port: u16,
}

pub async fn read_all_nodes(epmd_addr: &SocketAddr) -> Result<Vec<NodeDetails>> {
    let socket = TcpStream::connect(epmd_addr)
        .await
        .context("when establishing connection to EPMD")?;

    read_all_nodes_sock(socket).await
}

pub async fn read_all_nodes_sock(socket: impl AsyncRead + AsyncWrite) -> Result<Vec<NodeDetails>> {
    pin!(socket);

    socket.write_u16(1).await?;
    socket.write_u8(110).await?;

    let _epmd_port_no = socket.read_u32().await?;

    let mut nodes = Vec::new();

    let mut buf = String::new();
    let mut reader = BufReader::new(socket);
    while let Ok(count) = reader.read_line(&mut buf).await {
        if count == 0 {
            break;
        }

        let (a, b) = buf
            .split_once(" at port ")
            .context("malformed epmd response")?;
        let node_port: u16 = b.trim().parse().context("malformed epmd response")?;
        if !a.starts_with("name ") {
            bail!("malformed epmd response");
        }
        let name = &a[5..];

        nodes.push(NodeDetails {
            name: name.to_owned(),
            port: node_port,
        });

        buf.clear();
    }

    Ok(nodes)
}

pub struct NodeRegistrationHandle {
    socket: TcpStream,
    pub creation: u32,
}

impl NodeRegistrationHandle {
    pub async fn unregister(&mut self) {
        self.socket.shutdown().await.unwrap();
    }
}

pub async fn register_node(
    epmd_addr: &SocketAddr,
    details: &NodeDetails,
) -> Result<NodeRegistrationHandle> {
    let mut buf = std::io::BufWriter::new(Vec::new());
    buf.write_all(&[120]).unwrap();
    buf.write_all(&details.port.to_be_bytes()).unwrap();
    buf.write_all(&[77, 0, 0, 6, 0, 5]).unwrap();
    buf.write_all(&(details.name.len() as u16).to_be_bytes())
        .unwrap();
    buf.write_all(details.name.as_bytes()).unwrap();
    buf.write_all(&0u16.to_be_bytes()).unwrap();
    let data = buf.into_inner().unwrap();

    let mut socket = TcpStream::connect(epmd_addr)
        .await
        .context("when establishing connection to EPMD")?;
    socket
        .write_u16(data.len() as u16)
        .await
        .context("when writing length in EPMD registration request")?;
    socket
        .write_all(&data)
        .await
        .context("when writing EPMD registration request")?;

    let typ = socket
        .read_u8()
        .await
        .context("when reading response type")?;
    match typ {
        121 => bail!("ancient EPMD version"),
        118 => (),
        _ => bail!("unknown reply from EPMD"),
    }

    let result = socket.read_u8().await?;
    let creation = socket.read_u32().await?;

    if result > 0 {
        bail!("failed to register node in local EPMD (error {})", result);
    }

    Ok(NodeRegistrationHandle { socket, creation })
}

pub async fn get_dist_port(epmd_addr: &SocketAddr, name: &str) -> Result<()> {
    let socket = TcpStream::connect(epmd_addr)
        .await
        .context("when establishing connection to EPMD")?;

    get_dist_port_stream(socket, name).await
}

pub async fn get_dist_port_stream(socket: impl AsyncRead + AsyncWrite, name: &str) -> Result<()> {
    pin!(socket);

    socket.write_u16(1 + name.len() as u16).await?;
    socket.write_u8(122).await?;
    socket.write_all(name.as_bytes()).await?;

    let typ = socket.read_u8().await?;
    if typ != 119 {
        bail!("received bad reply from EPMD (typ: {})", typ);
    }
    let result = socket.read_u8().await?;
    if result > 0 {
        bail!("bad result to EPMD port request (result: {})", result);
    }

    let port_no = socket.read_u16().await.context("while reading port_no")?;
    let node_type = socket.read_u8().await.context("while reading node_type")?;
    let protocol = socket.read_u8().await.context("while reading protocol")?;
    let highest_version = socket
        .read_u16()
        .await
        .context("while reading highest_version")?;
    let lowest_version = socket
        .read_u16()
        .await
        .context("while reading lowest_version")?;

    println!("port no: {}", port_no);
    println!("node type: {}", node_type);
    println!("protocol: {}", protocol);
    println!("highest version: {}", highest_version);
    println!("lowest version: {}", lowest_version);

    let node_name_len = socket.read_u16().await?;
    let mut node_name = vec![0u8; node_name_len as usize];
    socket.read_exact(&mut node_name).await?;

    let extra_len = socket.read_u16().await?;
    let mut extra = vec![0u8; extra_len as usize];
    socket.read_exact(&mut extra).await?;

    println!("node name: {:?}", std::str::from_utf8(&node_name).unwrap());
    println!("extra: {:?}", std::str::from_utf8(&extra).unwrap());

    Ok(())
}

pub async fn epmd_testing(epmd_port: u16) -> Result<()> {
    let addr: SocketAddr = ("localhost", epmd_port).to_socket_addrs()?.next().unwrap();

    let nodes = read_all_nodes(&addr).await?;
    println!("nodes: {:?}", nodes);

    register_node(
        &addr,
        &NodeDetails {
            name: "testing@foobar".into(),
            port: 11123,
        },
    )
    .await?;

    Ok(())
}
