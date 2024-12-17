use std::borrow::Borrow;

use anyhow::bail;

use crate::apis::Cluster;

#[derive(Debug)]
pub enum ParsedDomainName {
    Service {
        service: String,
        namespace: Option<String>,
    },
    Pod {
        pod: String,
        namespace: Option<String>,
    },
    PodInService {
        pod: String,
        service: String,
        namespace: Option<String>,
    },
    /// One of the forms:
    /// * `<arbitrary-name>.<service-name>`
    /// * `<service-name>.<namespace>`
    ///
    /// It is not possible to tell these apart as is.
    Ambiguous { part_a: String, part_b: String },
}

impl ParsedDomainName {
    pub fn parse(string: &str) -> anyhow::Result<Self> {
        let parts: Vec<_> = string.split(".").collect();
        assert!(parts.len() > 0);

        let res = match parts.len() {
            1 => ParsedDomainName::Service {
                service: parts[0].to_string(),
                namespace: None,
            },
            2 => ParsedDomainName::Ambiguous {
                part_a: parts[0].to_string(),
                part_b: parts[1].to_string(),
            },
            n if n >= 3 && n <= 6 && parts[2] == "svc" => ParsedDomainName::Service {
                service: parts[0].to_string(),
                namespace: Some(parts[1].to_string()),
            },
            n if n >= 3 && n <= 6 && parts[2] == "pod" => ParsedDomainName::Pod {
                pod: parts[0].to_string(),
                namespace: Some(parts[1].to_string()),
            },
            n if n >= 3 && n < 7 && parts.get(3).map(|p| *p) == Some("svc") => {
                ParsedDomainName::PodInService {
                    pod: parts[0].to_string(),
                    service: parts[1].to_string(),
                    namespace: Some(parts[2].to_string()),
                }
            }
            _ => bail!("badly formatted domain name"),
        };

        Ok(res)
    }

    pub fn namespace(&self) -> Option<&str> {
        match self {
            ParsedDomainName::Service { namespace, .. } => namespace.as_deref(),
            ParsedDomainName::Pod { namespace, .. } => namespace.as_deref(),
            ParsedDomainName::PodInService { namespace, .. } => namespace.as_deref(),
            ParsedDomainName::Ambiguous { .. } => None,
        }
    }
}

pub enum ResolvedEntity {
    Service(String),
}

impl Cluster {
    pub async fn resolve_domain_name(
        &self,
        domain_name: &str,
        default_ns: &str,
    ) -> anyhow::Result<ResolvedEntity> {
        let parsed = ParsedDomainName::parse(domain_name)?;

        let namespace = parsed.namespace().unwrap_or(default_ns);
        let apis = self.get_namespace(namespace);

        match parsed {
            ParsedDomainName::Service { service, .. } => {
                match apis.pod.get(&service).await {
                    Ok(_) => Ok(ResolvedEntity::Service(service.clone())),
                    // TODO retry
                    Err(err) => Err(err)?,
                }
            }
            ParsedDomainName::Pod { pod, .. } => todo!(),
            ParsedDomainName::PodInService { pod, service, .. } => todo!(),
            ParsedDomainName::Ambiguous { part_a, part_b } => todo!(),
        }
    }
}
