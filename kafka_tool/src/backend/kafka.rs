use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use k8s_util::apis::Cluster;
use lasso::Spur;
use rskafka::{
    client::{Client, ClientBuilder},
    BackoffConfig,
};
use rustls::ClientConfig;

use crate::lua::ConnectStrategy;

use super::State;

use rustls::{
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    SignatureScheme,
};

#[derive(Debug)]
pub struct NoVerification;

impl ServerCertVerifier for NoVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            SignatureScheme::RSA_PKCS1_SHA1,
            SignatureScheme::ECDSA_SHA1_Legacy,
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::ED25519,
            SignatureScheme::ED448,
        ]
    }
}

pub async fn kafka_connect(
    connect_strategy: ConnectStrategy,
    _state: &State,
) -> anyhow::Result<Client> {
    match connect_strategy {
        ConnectStrategy::KubernetesService {
            namespace,
            service,
            service_port,
        } => {
            let client = k8s_util::create_client(None).await.unwrap();
            let cluster = Cluster::new(client);

            log::info!("got k8s client");

            let cluster_inner = cluster.clone();
            let (_join_handle, local_addr) = k8s_util::socks5::listen_socks5_forward(
                cluster_inner,
                namespace.to_string(),
                "localhost:0",
            )
            .await
            .unwrap();
            log::info!(
                "bound k8s forward socks5 proxy to {}",
                local_addr.to_string()
            );

            let service_url = format!("{}:{}", service, service_port);
            let brokers_vec = vec![service_url];

            let tls_client_config = ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoVerification))
                .with_no_client_auth();

            let mut backoff_config = BackoffConfig::default();
            backoff_config.deadline = Some(Duration::from_secs(2));

            log::info!("Connecting to broker...");

            let kafka_client = ClientBuilder::new(brokers_vec)
                .tls_config(Arc::new(tls_client_config))
                .socks5_proxy(local_addr.to_string())
                .backoff_config(backoff_config)
                .build()
                .await
                .unwrap();

            log::info!("Connected!");

            Ok(kafka_client)
        }
    }
}

pub async fn list_topics(
    state: &State,
    client: &Client,
) -> anyhow::Result<(BTreeMap<Spur, BTreeSet<i32>>, Vec<Spur>)> {
    let topics_raw = client.list_topics().await?;

    let topic_partitions = topics_raw
        .iter()
        .map(|v| {
            let name = state.rodeo.get_or_intern(v.name.clone());
            (name, v.partitions.clone())
        })
        .collect::<BTreeMap<_, _>>();

    let mut sorted_topics = topic_partitions
        .iter()
        .map(|(k, _v)| *k)
        .collect::<Vec<_>>();
    sorted_topics.sort_by_key(|v| &state.rodeo[*v]);

    Ok((topic_partitions, sorted_topics))
}
