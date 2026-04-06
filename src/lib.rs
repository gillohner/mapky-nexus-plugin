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
use futures::TryStreamExt;
use mapky_app_specs::MapkyAppObject;
use utoipa::OpenApi;
use nexus_common::db::get_neo4j_graph;
use nexus_common::plugin::{GraphNodeRef, NexusPlugin, PluginContext, PluginManifest};
use nexus_common::types::DynError;
use tracing::{debug, warn};

pub struct MapkyPlugin;

impl MapkyPlugin {
    pub fn new() -> Self {
        MapkyPlugin
    }
}

impl Default for MapkyPlugin {
    fn default() -> Self {
        Self::new()
    }
}

/// Split `/pub/mapky.app/posts/0034TK01CC73G` into `("posts", "0034TK01CC73G")`.
pub(crate) fn split_resource(path: &str) -> Option<(&str, &str)> {
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
pub(crate) fn extract_pub_path(uri: &str) -> Option<&str> {
    let without_scheme = uri.strip_prefix("pubky://")?;
    let slash = without_scheme.find('/')?;
    let path = &without_scheme[slash..];
    if path.starts_with("/pub/") {
        Some(path)
    } else {
        None
    }
}

/// Extract `user_id` from `pubky://{user_id}/pub/...`.
pub(crate) fn extract_user_id(uri: &str) -> Option<&str> {
    let without_scheme = uri.strip_prefix("pubky://")?;
    let slash = without_scheme.find('/')?;
    Some(&without_scheme[..slash])
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

        // Infrastructure types — handle before MapkyAppObject dispatch.
        match resource_type {
            "tags" => {
                handlers::tag::sync_put(data, user_id, resource_id).await?;
                return Ok(());
            }
            "files" => {
                handlers::file::sync_put(data, uri, user_id).await?;
                return Ok(());
            }
            "blobs" => return Ok(()), // raw binary, fetched on-demand via file.src
            _ => {}
        }

        // Skip unrecognized resource types.
        let object = match MapkyAppObject::from_path(resource_type, data, resource_id) {
            Ok(obj) => obj,
            Err(_) => {
                debug!("Skipping unrecognized resource type '{resource_type}' at {uri}");
                return Ok(());
            }
        };

        match object {
            MapkyAppObject::Post(post) => {
                handlers::post::sync_put(&post, user_id, resource_id).await?;
            }
            MapkyAppObject::Collection(collection) => {
                handlers::collection::sync_put(&collection, user_id, resource_id).await?;
            }
            MapkyAppObject::Incident(incident) => {
                handlers::incident::sync_put(&incident, user_id, resource_id).await?;
            }
            MapkyAppObject::GeoCapture(geo_capture) => {
                handlers::geo_capture::sync_put(&geo_capture, user_id, resource_id).await?;
            }
            MapkyAppObject::Route(route) => {
                handlers::route::sync_put(&route, user_id, resource_id).await?;
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
            "tags" => {
                handlers::tag::del(user_id, resource_id).await?;
            }
            "files" => {
                handlers::file::del(uri, user_id).await?;
            }
            "blobs" => {} // raw binary, not indexed
            "posts" => {
                handlers::post::del(user_id, resource_id).await?;
            }
            "incidents" => {
                handlers::incident::del(user_id, resource_id).await?;
            }
            "geo_captures" => {
                handlers::geo_capture::del(user_id, resource_id).await?;
            }
            "collections" => {
                handlers::collection::del(user_id, resource_id).await?;
            }
            "routes" => {
                handlers::route::del(user_id, resource_id).await?;
            }
            _ => {
                debug!("Skipping DEL for '{resource_type}' at {uri}");
            }
        }

        Ok(())
    }

    fn routes(&self, ctx: PluginContext) -> Router {
        api::routes(ctx)
    }

    fn openapi_docs(&self) -> Option<utoipa::openapi::OpenApi> {
        Some(api::MapkyApiDoc::openapi())
    }

    async fn resolve_graph_node(
        &self,
        resource_type: &str,
        resource_id: &str,
        uri_owner_id: &str,
        _ctx: &PluginContext,
    ) -> Result<Option<GraphNodeRef>, DynError> {
        let compound_id = format!("{uri_owner_id}:{resource_id}");
        let graph = get_neo4j_graph()?;

        let (label, query) = match resource_type {
            "posts" => (
                "MapkyAppPost",
                queries::get::mapky_post_exists(&compound_id),
            ),
            "incidents" => (
                "MapkyAppIncident",
                queries::get::mapky_incident_exists(&compound_id),
            ),
            "geo_captures" => (
                "MapkyAppGeoCapture",
                queries::get::mapky_geo_capture_exists(&compound_id),
            ),
            "collections" => (
                "MapkyAppCollection",
                queries::get::mapky_collection_exists(&compound_id),
            ),
            "routes" => (
                "MapkyAppRoute",
                queries::get::mapky_route_exists(&compound_id),
            ),
            _ => return Ok(None),
        };

        let mut stream = graph.execute(query).await?;
        let exists: bool = stream
            .try_next()
            .await?
            .and_then(|row| row.get("exists").ok())
            .unwrap_or(false);

        if exists {
            Ok(Some(GraphNodeRef {
                label: label.to_string(),
                property: "id".to_string(),
                id: compound_id,
            }))
        } else {
            Ok(None)
        }
    }

    async fn setup_schema(&self, _ctx: &PluginContext) -> Result<(), DynError> {
        let graph = get_neo4j_graph()?;

        let ddl_statements: &[(&str, &str)] = &[
            // ── Place ──
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
            // ── MapkyAppPost ──
            (
                "mapky_schema_post_unique",
                "CREATE CONSTRAINT mapky_post_unique IF NOT EXISTS \
                 FOR (p:MapkyAppPost) REQUIRE p.id IS UNIQUE",
            ),
            // ── MapkyAppIncident ──
            (
                "mapky_schema_incident_unique",
                "CREATE CONSTRAINT mapky_incident_unique IF NOT EXISTS \
                 FOR (i:MapkyAppIncident) REQUIRE i.id IS UNIQUE",
            ),
            (
                "mapky_schema_incident_location",
                "CREATE POINT INDEX mapky_incident_location IF NOT EXISTS \
                 FOR (i:MapkyAppIncident) ON (i.location)",
            ),
            // ── MapkyAppGeoCapture ──
            (
                "mapky_schema_geo_capture_unique",
                "CREATE CONSTRAINT mapky_geo_capture_unique IF NOT EXISTS \
                 FOR (g:MapkyAppGeoCapture) REQUIRE g.id IS UNIQUE",
            ),
            (
                "mapky_schema_geo_capture_location",
                "CREATE POINT INDEX mapky_geo_capture_location IF NOT EXISTS \
                 FOR (g:MapkyAppGeoCapture) ON (g.location)",
            ),
            // ── MapkyAppCollection ──
            (
                "mapky_schema_collection_unique",
                "CREATE CONSTRAINT mapky_collection_unique IF NOT EXISTS \
                 FOR (c:MapkyAppCollection) REQUIRE c.id IS UNIQUE",
            ),
            // ── MapkyAppRoute ──
            (
                "mapky_schema_route_unique",
                "CREATE CONSTRAINT mapky_route_unique IF NOT EXISTS \
                 FOR (r:MapkyAppRoute) REQUIRE r.id IS UNIQUE",
            ),
            (
                "mapky_schema_route_start",
                "CREATE POINT INDEX mapky_route_start IF NOT EXISTS \
                 FOR (r:MapkyAppRoute) ON (r.start_point)",
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
