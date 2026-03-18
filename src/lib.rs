//! Mapky Nexus Plugin — indexes `/pub/mapky.app/` events into the shared
//! Nexus Neo4j graph and Redis cache.
//!
//! Ported from `mapky-indexer`. Uses the `NexusPlugin` trait so the watcher
//! dispatcher can route events here before `pubky-app-specs` URI parsing.

mod api;
pub mod handlers;
pub mod models;
pub mod queries;

use axum::Router;
use mapky_app_specs::MapkyAppObject;
use utoipa::OpenApi;
use nexus_common::db::get_neo4j_graph;
use nexus_common::plugin::{NexusPlugin, PluginContext, PluginManifest};
use nexus_common::types::DynError;
use tracing::{debug, warn};

pub struct MapkyPlugin;

impl MapkyPlugin {
    pub fn new() -> Self {
        MapkyPlugin
    }

    pub fn openapi_docs(&self) -> utoipa::openapi::OpenApi {
        api::MapkyApiDoc::openapi()
    }
}

impl Default for MapkyPlugin {
    fn default() -> Self {
        Self::new()
    }
}

/// Split `/pub/mapky.app/posts/0034TK01CC73G` into `("posts", "0034TK01CC73G")`.
fn split_resource(path: &str) -> Option<(&str, &str)> {
    let after_app = path.strip_prefix("/pub/mapky.app/")?;
    let slash = after_app.find('/')?;
    let resource_type = &after_app[..slash];  // e.g. "posts"
    let resource_id = &after_app[slash + 1..]; // e.g. "0034TK01CC73G"
    if resource_id.is_empty() {
        return None;
    }
    Some((resource_type, resource_id))
}

/// Extract `/pub/mapky.app/...` from `pubky://{user_id}/pub/mapky.app/...`.
fn extract_pub_path(uri: &str) -> Option<&str> {
    let without_scheme = uri.strip_prefix("pubky://")?;
    let slash = without_scheme.find('/')?;
    let path = &without_scheme[slash..];
    if path.starts_with("/pub/") {
        Some(path)
    } else {
        None
    }
}

#[async_trait::async_trait]
impl NexusPlugin for MapkyPlugin {
    fn manifest(&self) -> PluginManifest {
        PluginManifest {
            name: "mapky",
            namespace: "/pub/mapky.app/",
        }
    }

    async fn handle_put(
        &self,
        uri: &str,
        data: &[u8],
        user_id: &str,
        _ctx: &PluginContext,
    ) -> Result<(), DynError> {
        let path = extract_pub_path(uri)
            .ok_or_else(|| format!("Cannot extract path from URI: {uri}"))?;
        let (resource_type, resource_id) = split_resource(path)
            .ok_or_else(|| format!("Cannot split resource from path: {path}"))?;

        let object = MapkyAppObject::from_path(resource_type, data, resource_id)
            .map_err(|e| format!("Failed to parse {uri}: {e}"))?;

        match object {
            MapkyAppObject::Post(post) => {
                handlers::post::sync_put(&post, user_id, resource_id).await?;
            }
            MapkyAppObject::LocationTag(_) => {
                debug!("LocationTag handler not yet implemented, skipping {uri}");
            }
            MapkyAppObject::Collection(_) => {
                debug!("Collection handler not yet implemented, skipping {uri}");
            }
            MapkyAppObject::Incident(_) => {
                debug!("Incident handler not yet implemented, skipping {uri}");
            }
            MapkyAppObject::GeoCapture(_) => {
                debug!("GeoCapture handler not yet implemented, skipping {uri}");
            }
            MapkyAppObject::Route(_) => {
                debug!("Route handler not yet implemented, skipping {uri}");
            }
        }

        Ok(())
    }

    async fn handle_del(
        &self,
        uri: &str,
        user_id: &str,
        _ctx: &PluginContext,
    ) -> Result<(), DynError> {
        let path = extract_pub_path(uri)
            .ok_or_else(|| format!("Cannot extract path from URI: {uri}"))?;
        let (resource_type, resource_id) = split_resource(path)
            .ok_or_else(|| format!("Cannot split resource from path: {path}"))?;

        match resource_type {
            "posts" => {
                handlers::post::del(user_id, resource_id).await?;
            }
            other => {
                debug!("DEL handler for '{other}' not yet implemented, skipping {uri}");
            }
        }

        Ok(())
    }

    fn routes(&self, ctx: PluginContext) -> Router {
        api::routes(ctx)
    }

    async fn setup_schema(&self, _ctx: &PluginContext) -> Result<(), DynError> {
        let graph = get_neo4j_graph()?;

        let ddl_statements: &[(&str, &str)] = &[
            // Spatial index on Place.location — required for viewport bbox queries
            (
                "mapky_schema_place_unique",
                "CREATE CONSTRAINT mapky_place_unique IF NOT EXISTS \
                 FOR (p:Place) REQUIRE p.osm_canonical IS UNIQUE",
            ),
            (
                "mapky_schema_place_location",
                "CREATE POINT INDEX mapky_place_location IF NOT EXISTS \
                 FOR (p:Place) ON (p.location)",
            ),
            // MapkyPost uniqueness constraint (:MapkyPost avoids nexus's :Post collision)
            (
                "mapky_schema_post_unique",
                "CREATE CONSTRAINT mapky_post_unique IF NOT EXISTS \
                 FOR (p:MapkyPost) REQUIRE p.id IS UNIQUE",
            ),
        ];

        for (name, ddl) in ddl_statements {
            if let Err(e) = graph
                .run(nexus_common::db::graph::Query::new(name, *ddl))
                .await
            {
                if e.to_string().contains("EquivalentSchemaRuleAlreadyExists") {
                    warn!("Mapky schema rule already exists (concurrent init), ignoring: {ddl}");
                } else {
                    return Err(e.into());
                }
            }
        }

        Ok(())
    }
}
