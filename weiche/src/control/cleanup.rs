//! retire only resources whose recorded owner and identity still match the observed Gateway.
use super::{GatewayStores, service::Plan, workload::Settings};
use k8s_openapi::api::{
    apps::v1::Deployment,
    core::v1::{ConfigMap, Secret, Service, ServiceAccount},
    rbac::v1::{Role, RoleBinding},
};
use kube::{
    Api, Client, ResourceExt,
    api::{ApiResource, DeleteParams, DynamicObject, Preconditions},
};

async fn remove(
    client: &Client,
    kind: ApiResource,
    ns: &str,
    name: &str,
    owner_uid: &str,
) -> anyhow::Result<()> {
    let api = Api::<DynamicObject>::namespaced_with(client.clone(), ns, &kind);
    let Some(obj) = api.get_opt(name).await? else { return Ok(()) };
    if obj
        .owner_references()
        .iter()
        .any(|r| r.controller == Some(true) && r.uid == owner_uid)
    {
        let params = DeleteParams {
            preconditions: Some(Preconditions {
                uid: obj.uid(),
                resource_version: obj.resource_version(),
            }),
            ..Default::default()
        };
        match api.delete(name, &params).await {
            Err(kube::Error::Api(e)) if e.code == 404 => {}
            result => {
                result?;
            }
        }
    }
    Ok(())
}

pub async fn retired(
    client: &Client,
    stores: &GatewayStores,
    settings: &Settings,
    plans: &[Plan],
) -> anyhow::Result<()> {
    for cm in stores.config_maps.state() {
        if cm
            .labels()
            .get("app.kubernetes.io/managed-by")
            .is_none_or(|v| v != "weiche")
        {
            continue;
        }
        let instance = cm.labels().get("app.kubernetes.io/instance").map(String::as_str);
        if instance != Some(settings.release.as_str())
            && !(instance.is_none() && settings.release == "weiche")
        {
            continue;
        }
        let Some(owner) = cm.owner_references().iter().find(|r| {
            r.controller == Some(true)
                && r.kind == "Gateway"
                && r.api_version == "gateway.networking.k8s.io/v1"
        }) else {
            continue;
        };
        let name = cm.name_any();
        let ns = cm.namespace().unwrap_or_default();
        if !settings.namespaces.contains(&ns)
            || name != format!("weiche-gw-{}", owner.uid)
            || plans.iter().any(|p| p.gateway.uid().as_deref() == Some(&owner.uid))
        {
            continue;
        }
        // handle a live Gateway whose class was withdrawn. Kubernetes collects deleted Gateways.
        if !stores.gateways.state().iter().any(|g| {
            g.uid().as_deref() == Some(&owner.uid)
                && g.metadata.deletion_timestamp.is_none()
        }) {
            continue;
        }
        for kind in [
            ApiResource::erase::<Secret>(&()),
            ApiResource::erase::<Deployment>(&()),
            ApiResource::erase::<Service>(&()),
            ApiResource::erase::<RoleBinding>(&()),
            ApiResource::erase::<Role>(&()),
            ApiResource::erase::<ServiceAccount>(&()),
            ApiResource::erase::<ConfigMap>(&()),
        ] {
            remove(client, kind, &ns, &name, &owner.uid).await?;
        }
    }
    Ok(())
}
