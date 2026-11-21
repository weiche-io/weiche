//! resolve listener certificates and Service TLS policies against the local stores.
//! HTTPS uses one kubernetes.io/tls Secret with tls.crt and tls.key. cross-namespace
//! references require a ReferenceGrant from the Secret namespace.
//! a BackendTLSPolicy selects a Service or named port, its DNS identity and either
//! one ConfigMap containing ca.crt or the process's system trust store.
use super::{
    super::ir::{BackendTls, Protocol, RefError, ServerTls},
    GatewayStores,
};
use compio::tls::rustls::pki_types::{CertificateDer, DnsName, pem::PemObject};
use k8s_openapi::api::core::v1::Service;
use kube::{ResourceExt, runtime::reflector::ObjectRef};
use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
};
use weiche_gtwapi::v1::{
    backendtlspolicies::BackendTlsPolicy,
    gateways::{GatewayListeners, GatewayListenersTlsMode},
};

/// condition reason and message, safe to publish in resource status.
#[derive(Clone, Debug)]
pub struct TlsError {
    pub reason: &'static str,
    pub message: String,
}

impl TlsError {
    fn new(reason: &'static str, message: impl Into<String>) -> Self {
        Self { reason, message: message.into() }
    }
}

/// validate listener settings and resolve the single core certificate reference.
pub fn listener_tls(
    stores: &GatewayStores,
    namespace: &str,
    listener: &GatewayListeners,
) -> Result<Option<Arc<ServerTls>>, TlsError> {
    let invalid = |message| TlsError::new("InvalidCertificateRef", message);
    let tls = match (listener.protocol.as_str(), &listener.tls) {
        ("HTTP", None) => return Ok(None),
        ("HTTPS", Some(tls)) => tls,
        ("HTTPS", None) => return Err(invalid("HTTPS requires a certificate reference")),
        ("HTTP", Some(_)) => {
            return Err(TlsError::new("Invalid", "HTTP does not accept TLS settings"));
        }
        _ => {
            return Err(TlsError::new(
                "UnsupportedProtocol",
                "supported listener protocols are HTTP and HTTPS",
            ));
        }
    };
    if matches!(tls.mode, Some(GatewayListenersTlsMode::Passthrough))
        || tls.options.as_ref().is_some_and(|options| !options.is_empty())
    {
        return Err(TlsError::new(
            "Invalid",
            "HTTPS requires TLS termination with default options",
        ));
    }
    let [reference] = tls.certificate_refs.as_deref().unwrap_or_default() else {
        return Err(invalid("HTTPS requires one TLS Secret reference"));
    };
    let group = reference.group.as_deref().unwrap_or("");
    let kind = reference.kind.as_deref().unwrap_or("Secret");
    let target = reference.namespace.as_deref().unwrap_or(namespace);
    if !group.is_empty() || kind != "Secret" {
        return Err(invalid("certificate reference must name a core Secret"));
    }
    if !stores.permits(namespace, "Gateway", target, kind, &reference.name) {
        return Err(TlsError::new(
            "RefNotPermitted",
            "certificate reference requires a ReferenceGrant in the Secret namespace",
        ));
    }
    let secret = stores
        .secrets
        .get(&ObjectRef::new(&reference.name).within(target))
        .filter(|s| {
            s.metadata.deletion_timestamp.is_none()
                && s.type_.as_deref() == Some("kubernetes.io/tls")
        })
        .ok_or_else(|| {
            invalid("referenced TLS Secret is missing or has the wrong type")
        })?;
    let field = |name| {
        secret
            .data
            .as_ref()
            .and_then(|data| data.get(name))
            .map(|bytes| bytes.0.as_slice())
    };
    let cert =
        field("tls.crt").ok_or_else(|| invalid("TLS Secret is missing tls.crt"))?;
    let key = field("tls.key").ok_or_else(|| invalid("TLS Secret is missing tls.key"))?;
    ServerTls::from_pem(listener.hostname.as_deref().unwrap_or(""), cert, key)
        .map(|tls| Some(Arc::new(tls)))
        .map_err(|_| invalid("invalid certificate chain or mismatched private key"))
}

/// policy decisions for Service ports reached during this compilation.
#[derive(Default)]
pub struct Policies {
    pub reports: HashMap<ObjectRef<BackendTlsPolicy>, Result<(), TlsError>>,
    configs: HashMap<
        (ObjectRef<BackendTlsPolicy>, Protocol),
        Result<Arc<BackendTls>, TlsError>,
    >,
}

impl Policies {
    /// prefer an explicit port binding, then the oldest policy and lexical name.
    pub fn backend(
        &mut self,
        stores: &GatewayStores,
        namespace: &str,
        service: &Service,
        port: Option<&str>,
        protocol: Protocol,
    ) -> Result<Option<Arc<BackendTls>>, RefError> {
        let name = service.name_any();
        let ports = service
            .spec
            .as_ref()
            .and_then(|spec| spec.ports.as_deref())
            .unwrap_or_default();
        let mut candidates = Vec::new();
        for policy in stores.tls_policies.state() {
            if policy.metadata.namespace.as_deref() != Some(namespace)
                || policy.metadata.deletion_timestamp.is_some()
            {
                continue;
            }
            let targets = policy.spec.target_refs.iter().filter(|target| {
                target.group.is_empty() && target.kind == "Service" && target.name == name
            });
            for target in targets {
                if let Some(section) = target.section_name.as_deref()
                    && Some(section) != port
                {
                    if !ports.iter().any(|p| p.name.as_deref() == Some(section)) {
                        let message = format!(
                            "Service {namespace}/{name} has no port named {section}"
                        );
                        self.reports.insert(
                            ObjectRef::from_obj(policy.as_ref()),
                            Err(TlsError::new("TargetNotFound", message)),
                        );
                    }
                    continue;
                }
                candidates.push((target.section_name.is_none(), policy.clone()));
                break;
            }
        }
        candidates.sort_by_key(|(general, p)| {
            (*general, p.metadata.creation_timestamp.clone(), p.name_any())
        });
        let Some(((_, policy), others)) = candidates.split_first() else {
            return Ok(None);
        };
        for (_, other) in others {
            self.reports
                .entry(ObjectRef::from_obj(other.as_ref()))
                .or_insert_with(|| {
                    Err(TlsError::new(
                        "Conflicted",
                        "another policy takes precedence on this Service port",
                    ))
                });
        }
        let key = ObjectRef::from_obj(policy.as_ref());
        let tls = self.configs.entry((key.clone(), protocol)).or_insert_with(|| {
            policy_tls(stores, namespace, policy, protocol).map(Arc::new)
        });
        self.reports
            .insert(key, tls.as_ref().map(|_| ()).map_err(Clone::clone));
        tls.clone().map(Some).map_err(|_| RefError::InvalidTlsPolicy)
    }
}

/// construct a verifier only from the trust source selected by the policy.
fn policy_tls(
    stores: &GatewayStores,
    namespace: &str,
    policy: &BackendTlsPolicy,
    protocol: Protocol,
) -> Result<BackendTls, TlsError> {
    let validation = &policy.spec.validation;
    if policy.spec.target_refs.len() != 1
        || policy.spec.options.as_ref().is_some_and(|opts| !opts.is_empty())
        || validation
            .subject_alt_names
            .as_ref()
            .is_some_and(|names| !names.is_empty())
    {
        return Err(TlsError::new(
            "Invalid",
            "supported policies have one Service target, hostname validation and default options",
        ));
    }
    let hostname = DnsName::try_from(validation.hostname.as_str()).map_err(|_| {
        TlsError::new("Invalid", "validation.hostname must be a DNS hostname")
    })?;
    let refs = validation.ca_certificate_refs.as_deref().unwrap_or_default();
    match (refs, validation.well_known_ca_certificates.as_deref().unwrap_or("")) {
        ([reference], "") => {
            if !reference.group.is_empty() || reference.kind != "ConfigMap" {
                return Err(TlsError::new(
                    "InvalidKind",
                    format!("CA reference {} must be a core ConfigMap", reference.name),
                ));
            }
            let invalid = |why: &str| {
                TlsError::new(
                    "InvalidCACertificateRef",
                    format!("ConfigMap {namespace}/{}: {why}", reference.name),
                )
            };
            let map = stores
                .config_maps
                .get(&ObjectRef::new(&reference.name).within(namespace))
                .filter(|map| map.metadata.deletion_timestamp.is_none())
                .ok_or_else(|| invalid("not found"))?;
            let pem = map
                .data
                .as_ref()
                .and_then(|data| data.get("ca.crt"))
                .map(String::as_bytes)
                .or_else(|| {
                    map.binary_data
                        .as_ref()?
                        .get("ca.crt")
                        .map(|value| value.0.as_slice())
                })
                .ok_or_else(|| invalid("ca.crt is missing"))?;
            let roots = CertificateDer::pem_slice_iter(pem)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| invalid("malformed PEM"))?;
            BackendTls::new(hostname, &roots, protocol)
                .map_err(|e| invalid(&e.to_string()))
        }
        ([], "System") => {
            static ROOTS: OnceLock<Vec<CertificateDer<'static>>> = OnceLock::new();
            let roots =
                ROOTS.get_or_init(|| rustls_native_certs::load_native_certs().certs);
            BackendTls::new(hostname, roots, protocol)
                .map_err(|e| TlsError::new("Invalid", e.to_string()))
        }
        _ => Err(TlsError::new(
            "Invalid",
            "choose one CA ConfigMap or wellKnownCACertificates: System",
        )),
    }
}
