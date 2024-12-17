//! ## DNS configuration in a k8s cluster
//! My understanding of DNS config in k8s is as follows:
//!
//! Search domains are configured as:
//! ```
//! search <ns>.svc.cluster.local svc.cluster.local cluster.local
//! ```
//! `ndots` is 5 in k8s by default
//!
//! When a name like `mysvc.default` is looked up:
//! * `ndots` is set to above 1, so `mysvc.default` is not looked up directly
//!   initially
//! * Search domains are applied and tried in order, first match is returned
//! * Fall back to look up `mysvc` directly
//!
//! ## Replicating the k8s name lookup behaviour with API
//! This boils down to that for a lookup A, we try:
//! 1. `<A>.<ns>.svc.cluster.local`
//! 2. `<A>.svc.cluster.local`
//! 3. `<A>.cluster.local`
//! 4. `<A>`
//!
//! We need to consider the following patterns:
//! * A service in format `<svc>.<ns>.svc.cluster.local`
//! * A pod in format `<pod>.<svc>.<ns>.svc.cluster.local`
//! * A pod in format `<pod>.<ns>.pod.cluster.local`
//! * A statefulset pod in format `<ordinal>.<statefulset>.<ns>.svc.cluster.local`
//!
//! Peripherally relevant are SRV records:
//! * `_<port-name>._<protocol>.<svc>.<ns>.svc.cluster.local`
//!
//! I think there might also be custom domain stuff, but fuck that for now.

use super::Cluster;

fn expand_name_candidates_str(name: &str, default_ns: &str) -> Vec<String> {
    // If it is already fully qualified, only one candidate.
    if name.ends_with("cluster.local") {
        vec![name.into()]
    } else {
        vec![
            format!("{}.{}.svc.cluster.local", name, default_ns),
            format!("{}.svc.cluster.local", name),
            format!("{}.cluster.local", name),
            format!("{}", name),
        ]
    }
}

#[derive(Debug)]
pub enum ResolvedEntity {
    Pod {
        pod: String,
        namespace: String,
    },
    Svc {
        service: String,
        namespace: String,
    },
    /// Pod in `service` or `statefulset`
    PodInSvc {
        pod: String,
        service: String,
        namespace: String,
    },
}

impl ResolvedEntity {
    pub fn namespace(&self) -> &str {
        match self {
            ResolvedEntity::Pod { namespace, .. } => namespace.as_ref(),
            ResolvedEntity::Svc { namespace, .. } => namespace.as_ref(),
            ResolvedEntity::PodInSvc { namespace, .. } => namespace.as_ref(),
        }
    }
}

fn parse_candidate(domain: &str) -> Option<ResolvedEntity> {
    let items: Vec<_> = domain.split(".").collect();

    // < 5 never valid
    if items.len() < 5 {
        None?;
    }

    // two last always `.cluster.local`
    if items[items.len() - 2] != "cluster" || items[items.len() - 1] != "local" {
        None?;
    }

    match items[items.len() - 3] {
        "svc" if items.len() == 5 => Some(ResolvedEntity::Svc {
            service: items[0].into(),
            namespace: items[1].into(),
        }),
        "svc" if items.len() == 6 => Some(ResolvedEntity::PodInSvc {
            pod: items[0].into(),
            service: items[1].into(),
            namespace: items[2].into(),
        }),
        "pod" if items.len() == 5 => Some(ResolvedEntity::Pod {
            pod: items[0].into(),
            namespace: items[1].into(),
        }),
        _ => None,
    }
}

fn expand_name_candiates(domain: &str, default_ns: &str) -> Vec<ResolvedEntity> {
    let candidate_strs = expand_name_candidates_str(domain, default_ns);
    candidate_strs
        .iter()
        .flat_map(|v| parse_candidate(v))
        .collect()
}

impl Cluster {
    pub async fn resolve_domain_name(
        &self,
        domain: &str,
        default_ns: &str,
    ) -> anyhow::Result<Option<ResolvedEntity>> {
        let mut candidates = expand_name_candiates(domain, default_ns);
        log::info!("domain {} => candidates: {:?}", domain, candidates);
        let mut result = None;
        for candidate in candidates.drain(..) {
            let apis = self.get_namespace(candidate.namespace());
            match &candidate {
                ResolvedEntity::Pod { pod, .. } => {
                    let _result = apis.pod.get(pod).await.unwrap();
                    result = Some(candidate);
                    break;
                }
                ResolvedEntity::Svc { service, .. } => {
                    let _result = apis.service.get(service).await.unwrap();
                    result = Some(candidate);
                    break;
                }
                ResolvedEntity::PodInSvc {
                    pod, service: _svc, ..
                } => {
                    let _result = apis.pod.get(pod).await.unwrap();
                    result = Some(candidate);
                    break;
                }
            }
        }
        log::info!("resolved domain name {} => {:?}", domain, result);
        Ok(result)
    }
}
