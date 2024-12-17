use std::{fmt::format, net::Ipv4Addr, str::FromStr, sync::Arc};

use anyhow::Context;
use hickory_client::{
    client::{Client, ClientHandle},
    proto::{
        rr::{DNSClass, RecordType},
        xfer::DnsResponse,
    },
};
use hickory_resolver::{
    proto::{
        runtime::iocompat::AsyncIoTokioAsStd,
        tcp::{TcpClientStream, TcpStream},
    },
    Name,
};
use tokio::sync::Mutex;

use crate::kube_types::AnyReady;

use super::{Apis, Cluster};

async fn connect_client(kube_system_apis: Arc<Apis>) -> anyhow::Result<Client> {
    let dns_pod = kube_system_apis
        .find_pod_by_service_name("kube-dns", &AnyReady)
        .await
        .unwrap();

    let mut port_forward = kube_system_apis
        .pod
        .portforward(&dns_pod.0, &[53])
        .await
        .unwrap();
    // AsyncRead + AsyncWrite + Unpin
    let stream = port_forward.take_stream(53).unwrap();

    let conv_stream = AsyncIoTokioAsStd(stream);
    let dummy_addr = std::net::SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), 53);
    let (tcp_stream, stream_handle) = TcpStream::from_stream(conv_stream, dummy_addr);
    let client_stream = TcpClientStream::from_stream(tcp_stream);

    let (client, bg) = Client::new(
        Box::pin(async move { Ok(client_stream) }),
        stream_handle,
        None,
    )
    .await
    .context("connection failed")?;

    tokio::spawn(bg);

    Ok(client)
}

struct Inner {
    client: Option<Client>,
}

pub struct DNSResolver {
    system_ns: Arc<Apis>,
    inner: Mutex<Inner>,
}

impl DNSResolver {
    pub fn from_cluster(cluster: &Cluster) -> Self {
        let apis = cluster.get_namespace("kube-system");
        DNSResolver {
            system_ns: apis,
            inner: Mutex::new(Inner { client: None }),
        }
    }

    pub async fn query(
        &self,
        name: Name,
        query_class: DNSClass,
        query_type: RecordType,
    ) -> anyhow::Result<DnsResponse> {
        let mut guard = self.inner.lock().await;
        if guard.client.is_none() {
            guard.client = Some(connect_client(self.system_ns.clone()).await?);
        }
        let client = guard.client.as_mut().unwrap();
        let resp = client.query(name, query_class, query_type).await?;
        Ok(resp)
    }
}

impl Cluster {
    pub async fn lookup(&self, domain: &str, namespace: &str) -> anyhow::Result<()> {
        let cluster_local_zone = Name::from_str("cluster.local.").unwrap();

        let resolver = self.get_dns_resolver();

        let mut name = Name::from_str(domain).unwrap();

        // If ending with `.cluster.local`, this is a FQDN for the
        // k8s cluster.
        if !name.is_fqdn() && name.zone_of(&cluster_local_zone) {
            name.set_fqdn(true);
        }

        if name.is_fqdn() {
            let resp = resolver.query(name, DNSClass::IN, RecordType::A).await?;
            panic!("fqdn answer: {:?}", resp);
        }

        let resp = resolver.query(name, DNSClass::IN, RecordType::A).await?;
        panic!("base answer: {:?}", resp);

        // Search domain setup for k8s cluster
        //let search_1 = format!("{}.{}.svc.cluster.local", domain, namespace);
        //let search_2 = format!("{}.svc.cluster.local", domain);
        //let search_3 = format!("{}.cluster.local", domain);
        //let search_4 = domain;

        //let resolver = Resolver::new_with_conn(
        //    ResolverConfig::new(),
        //    ResolverOpts::default(),
        //    KubeDnsConnectionProvider(kube_system_api),
        //);

        todo!()
    }
}

//#[derive(Clone)]
//struct KubeDnsConnectionProvider(Arc<Apis>);
//
//impl ConnectionProvider for KubeDnsConnectionProvider {
//    type Conn = GenericConnection;
//    type FutureConn = Pin<Box<dyn Future<Output = Result<GenericConnection, ProtoError>> + Send>>;
//    type RuntimeProvider = TokioRuntimeProvider;
//
//    fn new_connection(
//        &self,
//        config: &hickory_resolver::config::NameServerConfig,
//        options: &ResolverOpts,
//    ) -> Result<Self::FutureConn, std::io::Error> {
//        let apis = self.0.clone();
//        match config.protocol {
//            Protocol::Tcp => Ok(Box::pin(async move {
//                let dns_pod = apis
//                    .find_pod_by_service_name("kube-dns", &AnyReady)
//                    .await
//                    .unwrap();
//
//                let mut port_forward = apis.pod.portforward(&dns_pod.0, &[53]).await.unwrap();
//                let stream = port_forward.take_stream(53).unwrap();
//
//                let conv_stream = AsyncIoTokioAsStd(stream);
//                let dummy_addr =
//                    std::net::SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), 53);
//                let (tcp_stream, stream_handle) = TcpStream::from_stream(conv_stream, dummy_addr);
//                let client_stream = TcpClientStream::from_stream(tcp_stream);
//                //let multiplexer = DnsMultiplexer::new(client_stream, stream_handle, None);
//
//                //let (exchange, background) = DnsExchange::from_stream(stream);
//
//                //ConnectionConnect::Tcp(exchange)
//                todo!()
//            })),
//            Protocol::Udp => Err(std::io::Error::new(
//                std::io::ErrorKind::Unsupported,
//                anyhow::anyhow!("udp not supported"),
//            )),
//            _ => Err(std::io::Error::new(
//                std::io::ErrorKind::Unsupported,
//                anyhow::anyhow!("protocol not supported"),
//            )),
//        }
//    }
//}

//#[derive(Clone)]
//struct KubeDnsRuntimeProvider;
//
//impl RuntimeProvider for KubeDnsRuntimeProvider {
//    type Handle = TokioHandle;
//    type Timer = TokioTime;
//    type Udp = UdpSocket;
//    type Tcp = AsyncIoTokioAsStd<TcpStream>;
//
//    fn create_handle(&self) -> Self::Handle {
//        todo!()
//    }
//
//    fn connect_tcp(
//        &self,
//        server_addr: std::net::SocketAddr,
//        bind_addr: Option<std::net::SocketAddr>,
//        timeout: Option<std::time::Duration>,
//    ) -> std::pin::Pin<Box<dyn Send + std::future::Future<Output = std::io::Result<Self::Tcp>>>>
//    {
//        todo!()
//    }
//
//    fn bind_udp(
//        &self,
//        local_addr: std::net::SocketAddr,
//        server_addr: std::net::SocketAddr,
//    ) -> std::pin::Pin<Box<dyn Send + std::future::Future<Output = std::io::Result<Self::Udp>>>>
//    {
//        todo!()
//    }
//}
