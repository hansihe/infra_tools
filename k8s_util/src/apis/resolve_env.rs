use std::collections::HashMap;

use anyhow::{bail, Context};
use k8s_openapi::api::core::v1::{EnvFromSource, EnvVar, EnvVarSource};

use super::Apis;

/// Resolve env var
impl Apis {
    pub async fn resolve_env(
        &self,
        env: &Option<Vec<EnvVar>>,
        env_from: &Option<Vec<EnvFromSource>>,
        out: &mut HashMap<String, String>,
    ) -> anyhow::Result<()> {
        for source in env_from.iter().flat_map(|v| v.iter()) {
            let add_prefix = |n: &str| {
                if let Some(prefix) = &source.prefix {
                    format!("{}{}", prefix, n)
                } else {
                    n.to_owned()
                }
            };

            if let Some(cm_ref) = &source.config_map_ref {
                let cm = self
                    .config_map
                    .get_cached(&cm_ref.name)
                    .await
                    .context("TODO 404 not handled")?;

                let data = cm
                    .data
                    .context("expected configmap with text data for env vars")?;

                for (k, v) in data.iter() {
                    out.insert(add_prefix(k), v.to_owned());
                }
            }

            if let Some(sec_ref) = &source.secret_ref {
                let sec = self
                    .secret
                    .get_cached(&sec_ref.name)
                    .await
                    .context("TODO 404 not handled")?;

                let data = sec.data.context("expected secret with data for env vars")?;

                for (k, v) in data.iter() {
                    if let Ok(v) = std::str::from_utf8(&v.0) {
                        out.insert(add_prefix(k), v.to_owned());
                    } else {
                        bail!("secret value was not valid string! (key: {})", k);
                    }
                }
            }
        }

        for var in env.iter().flat_map(|v| v.iter()) {
            let value = match var {
                EnvVar {
                    value: Some(val), ..
                } => self.resolve_env_var_string(&val, &*out).await?,
                EnvVar {
                    value_from: Some(from),
                    ..
                } => self.resolve_env_var_from(&from).await?,
                _ => bail!("unhandled env var reference ({:?})", var),
            };
            out.insert(var.name.clone(), value);
        }

        Ok(())
    }

    async fn resolve_env_var_string(
        &self,
        val: &str,
        previous: &HashMap<String, String>,
    ) -> anyhow::Result<String> {
        let mut parsed_str = String::with_capacity(val.len());
        let mut n = 0;
        let tn = parsed_str.len();
        while n < tn {
            if val.as_bytes()[n] == b'$' {
                match val.as_bytes().get(n + 1) {
                    Some(b'$') => {
                        parsed_str.push('$');
                        n += 2;
                    }
                    Some(b'(') => {
                        let (end_idx, _) = val
                            .as_bytes()
                            .iter()
                            .enumerate()
                            .skip(n + 2)
                            .find(|(_i, val)| **val == b')')
                            .context("escape value not closed")?;
                        let var_name =
                            std::str::from_utf8(&val.as_bytes()[n + 2..end_idx]).unwrap();
                        let value = previous
                            .get(var_name)
                            .context("could not resolve env var reference")?;
                        parsed_str.push_str(&value);
                        n += 3 + value.len();
                    }
                    Some(val) => bail!("unexpected escape char: {}", val),
                    None => bail!("invalid env value!"),
                }
            }
        }
        Ok(parsed_str)
    }

    async fn resolve_env_var_from(&self, val: &EnvVarSource) -> anyhow::Result<String> {
        match val {
            EnvVarSource {
                config_map_key_ref: Some(cm_ref),
                ..
            } => {
                let cm = self
                    .config_map
                    .get_cached(&cm_ref.name)
                    .await
                    .context("could not find referred configmap")?;
                let value = cm.data.as_ref().unwrap().get(&cm_ref.key).unwrap();
                Ok(value.to_owned())
            }
            EnvVarSource {
                secret_key_ref: Some(s_ref),
                ..
            } => {
                let sec = self
                    .secret
                    .get_cached(&s_ref.name)
                    .await
                    .context("could not find referred secret")?;
                let value_bin = sec.data.as_ref().unwrap().get(&s_ref.key).unwrap();
                let value =
                    std::str::from_utf8(&value_bin.0).context("secret value was not valid utf8")?;
                Ok(value.to_owned())
            }
            // TODO
            EnvVarSource {
                field_ref: Some(_f_ref),
                ..
            } => Ok("".into()),
            _ => bail!("unhandled env var reference ({:?})", val),
        }
    }
}
