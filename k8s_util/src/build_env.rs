use std::collections::HashMap;

use k8s_openapi::api::{
    apps::v1::Deployment,
    core::v1::{ConfigMap, EnvVar, PodSpec, Secret},
};
use kube::Api;

pub fn deployment_get_pod_spec(deployment: &Deployment) -> Option<&PodSpec> {
    deployment.spec.as_ref()?.template.spec.as_ref()
}

pub fn pod_spec_get_env(pod_spec: &PodSpec) -> Option<&Vec<EnvVar>> {
    pod_spec.containers.first()?.env.as_ref()
}

pub struct EnvScope {
    configmap_api: Api<ConfigMap>,
    secret_api: Api<Secret>,

    configmaps: HashMap<String, ConfigMap>,
    secrets: HashMap<String, Secret>,
}
