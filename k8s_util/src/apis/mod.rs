use std::{
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    ops::Deref,
    sync::{Arc, Mutex, RwLock},
};

//mod dns_svc_resolver;
mod dns_resolver;
mod resolve_env;

use anyhow::{bail, Context};
use dns_resolver::ResolvedEntity;
use k8s_openapi::api::{
    apps::v1::{Deployment, ReplicaSet},
    core::v1::{ConfigMap, Pod, Secret, Service},
};
use kube::{api::ListParams, Api, Client};
use log::{debug, info};
use serde::de::DeserializeOwned;

use crate::kube_types::PodSelection;

use super::kube_types::{AnyReady, PodName, TargetSelector};

struct CacheData<K> {
    by_name: HashMap<String, K>,
}
impl<K> Default for CacheData<K> {
    fn default() -> Self {
        Self {
            by_name: HashMap::new(),
        }
    }
}

pub struct CachedApi<K> {
    pub api: Api<K>,
    cache: RwLock<CacheData<K>>,
}

impl<K> CachedApi<K> {
    fn new(api: Api<K>) -> Self {
        Self {
            api,
            cache: Default::default(),
        }
    }
}

impl<K> Deref for CachedApi<K> {
    type Target = Api<K>;
    fn deref(&self) -> &Api<K> {
        &self.api
    }
}

impl<K: Clone + DeserializeOwned + Debug> CachedApi<K> {
    pub async fn get_cached(&self, name: &str) -> kube::Result<K> {
        if let Some(val) = self.cache.read().unwrap().by_name.get(name).cloned() {
            return Ok(val);
        }
        let val = self.api.get(name).await?;
        self.cache
            .write()
            .unwrap()
            .by_name
            .insert(name.to_owned(), val.clone());
        Ok(val)
    }
}

pub struct Cluster {
    pub client: Client,
    pub namespaces: Mutex<BTreeMap<String, Arc<Apis>>>,
    //dns_resolver: OnceLock<Arc<dns::DNSResolver>>,
}

impl Cluster {
    pub fn new(client: Client) -> Arc<Self> {
        Arc::new(Cluster {
            client,
            namespaces: Mutex::new(BTreeMap::new()),
            //dns_resolver: OnceLock::new(),
        })
    }

    pub fn get_namespace(&self, ns: &str) -> Arc<Apis> {
        let mut nss = self.namespaces.lock().unwrap();
        if let Some(apis) = nss.get(ns).cloned() {
            apis
        } else {
            let apis = Arc::new(Apis::namespaced(&self.client, ns));
            nss.insert(ns.to_string(), apis.clone());
            apis
        }
    }

    pub async fn find_pod_by_resolved_entity(
        &self,
        entity: &ResolvedEntity,
        pod_selector: &dyn PodSelection,
    ) -> anyhow::Result<PodName> {
        let apis = self.get_namespace(entity.namespace());
        match entity {
            ResolvedEntity::Pod { pod, .. } => todo!("pod {}", pod),
            ResolvedEntity::Svc { service, .. } => {
                apis.find_pod_by_service_name(service, pod_selector).await
            }
            ResolvedEntity::PodInSvc {
                pod, service: _svc, ..
            } => {
                apis.pod.get(pod).await.unwrap();
                Ok(PodName {
                    namespace: entity.namespace().into(),
                    name: pod.into(),
                })
            }
        }
    }

    //fn get_dns_resolver(&self) -> &dns::DNSResolver {
    //    self.dns_resolver
    //        .get_or_init(|| Arc::new(dns::DNSResolver::from_cluster(self)))
    //}
}

pub struct Apis {
    pub namespace: String,
    pub deployment: Api<Deployment>,
    pub replica_set: Api<ReplicaSet>,
    pub service: Api<Service>,
    pub pod: Api<Pod>,
    pub config_map: CachedApi<ConfigMap>,
    pub secret: CachedApi<Secret>,
}

impl Apis {
    pub fn namespaced(client: &Client, namespace: &str) -> Self {
        Apis {
            namespace: namespace.to_string(),
            deployment: Api::namespaced(client.clone(), namespace),
            replica_set: Api::namespaced(client.clone(), namespace),
            service: Api::namespaced(client.clone(), namespace),
            pod: Api::namespaced(client.clone(), namespace),
            config_map: CachedApi::new(Api::namespaced(client.clone(), namespace)),
            secret: CachedApi::new(Api::namespaced(client.clone(), namespace)),
        }
    }
}

/// Pod finder
impl Apis {
    pub async fn find_pod(&self, target: &TargetSelector) -> anyhow::Result<PodName> {
        let ready_pod = AnyReady {};

        match target {
            TargetSelector::ServiceName(name) => {
                self.find_pod_by_service_name(name, &ready_pod).await
            }
            TargetSelector::PodLabel(label) => self.find_pod_by_label(label, &ready_pod).await,
            TargetSelector::DeploymentName(name) => {
                self.find_pod_by_deployment_name(name, &ready_pod).await
            }
        }
    }

    pub async fn find_pod_by_service_name(
        &self,
        name: &str,
        pod_selector: &dyn PodSelection,
    ) -> anyhow::Result<PodName> {
        match self.service.get(name).await {
            Ok(service) => {
                if let Some(selector) = service.spec.and_then(|spec| spec.selector) {
                    let label_selector_str = selector
                        .iter()
                        .map(|(key, value)| format!("{}={}", key, value))
                        .collect::<Vec<_>>()
                        .join(",");

                    debug!("Selector for service '{}': {}", name, label_selector_str);

                    let pods = self
                        .pod
                        .list(&ListParams::default().labels(&label_selector_str))
                        .await?;

                    debug!(
                        "Pods found for selector '{}': {:?}",
                        label_selector_str,
                        pods.items
                            .iter()
                            .map(|v| &v.metadata.name)
                            .collect::<Vec<_>>()
                    );

                    let pod = pod_selector.select(&pods.items, &label_selector_str)?;
                    info!(
                        "Pod found for service '{}': {:?} in {:?}",
                        name, pod.metadata.name, pod.metadata.namespace
                    );
                    let pod_name = pod.metadata.name.clone().context("Pod Name is None")?;
                    Ok(PodName {
                        namespace: self.namespace.clone(),
                        name: pod_name,
                    })
                } else {
                    Err(anyhow::anyhow!("No selector found for service '{}'", name))
                }
            }
            Err(kube::Error::Api(kube::error::ErrorResponse { code: 404, .. })) => {
                let label_selector_str = format!("app={}", name);

                debug!(
                    "Using service name as label selector: {}",
                    label_selector_str
                );

                let pods = self
                    .pod
                    .list(&ListParams::default().labels(&label_selector_str))
                    .await?;

                debug!(
                    "Pods found for label '{}': {:?}",
                    label_selector_str, pods.items
                );

                let pod = pod_selector.select(&pods.items, &label_selector_str)?;
                let pod_name = pod.metadata.name.clone().context("Pod Name is None")?;
                Ok(PodName {
                    namespace: self.namespace.clone(),
                    name: pod_name,
                })
            }
            Err(e) => Err(anyhow::anyhow!("Error finding service '{}': {}", name, e)),
        }
    }

    pub async fn find_pod_by_label(
        &self,
        label: &str,
        ready_pod: &AnyReady,
    ) -> anyhow::Result<PodName> {
        let label_selector_str = label.to_string();
        let pods = self
            .pod
            .list(&ListParams::default().labels(&label_selector_str))
            .await?;

        let pod = ready_pod.select(&pods.items, &label_selector_str)?;

        let pod_name = pod.metadata.name.clone().context("Pod Name is None")?;
        Ok(PodName {
            namespace: self.namespace.clone(),
            name: pod_name,
        })
    }

    pub async fn find_pod_by_deployment_name(
        &self,
        name: &str,
        ready_pod: &AnyReady,
    ) -> anyhow::Result<PodName> {
        let deployment = self
            .deployment
            .get(name)
            .await
            .context("when getting deployment")?;

        let selector = &deployment.spec.as_ref().unwrap().selector;

        //let app_label = deployment
        //    .metadata
        //    .labels
        //    .as_ref()
        //    .unwrap()
        //    .get("app")
        //    .unwrap();

        assert!(selector.match_expressions.is_none());

        let rs_label_string = selector
            .match_labels
            .iter()
            .flat_map(|v| v.iter())
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join(",");

        let replica_sets = self
            .replica_set
            .list(&ListParams::default().labels(&rs_label_string))
            .await
            .context("when getting replicaset")?;
        if replica_sets.items.len() == 0 {
            bail!("no replicasets found for deployment");
        }

        let latest_rs = replica_sets
            .items
            .iter()
            .max_by_key(|v| v.metadata.creation_timestamp.as_ref().unwrap())
            .unwrap();

        let rs_labels = latest_rs.metadata.labels.as_ref().unwrap();
        let pod_template_hash = rs_labels.get("pod-template-hash").unwrap();

        let pod_label_string = format!(
            "{},pod-template-hash={}",
            rs_label_string, pod_template_hash
        );
        let pods = self
            .pod
            .list(&ListParams::default().labels(&pod_label_string))
            .await
            .context("when listing pods")?;

        let pod = ready_pod.select(&pods.items, &pod_label_string)?;
        let pod_name = pod.metadata.name.clone().context("Pod Name is None")?;
        Ok(PodName {
            namespace: self.namespace.clone(),
            name: pod_name,
        })
    }
}
