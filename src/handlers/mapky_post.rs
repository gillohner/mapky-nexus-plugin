//! Cross-namespace post handler — indexes `PubkyAppPost` blobs stored under
//! `/pub/mapky.app/posts/{id}` (rather than `/pub/pubky.app/posts/{id}`).
//!
//! These posts carry the dual label `:Post:MapkyAppPost` in Neo4j and serve
//! as threaded replies on any MapKy resource (review, route, collection,
//! geo-capture, sequence, incident, or another mapky-namespaced post) **or**
//! a place-level comment when the parent URI is an OSM URL.
//!
//! Parent resolution:
//! - OSM URL (`https://www.openstreetmap.org/{type}/{id}`) → ensure `:Place`
//!   exists (geocode via `PlaceDetails::from_osm_url` on first sight) and
//!   create `(:MapkyAppPost)-[:ABOUT]->(:Place)` — symmetric with reviews.
//! - Pubky URI under `/pub/mapky.app/{segment}/{id}` → create `[:REPLY_TO]`
//!   to that resource.
//! - Any other URI → store `parent_uri` as a property only, no edge (covers
//!   cross-domain parents like `/pub/pubky.app/posts/`).

use futures::TryStreamExt;
use mapky_app_specs::traits::Validatable;
use mapky_app_specs::{PubkyAppPost, PubkyAppPostKind};
use nexus_common::db::get_neo4j_graph;
use nexus_common::db::graph::GraphOps;
use nexus_common::types::DynError;
use std::sync::Arc;
use tracing::debug;

use crate::models::mapky_post::MapkyPostDetails;
use crate::models::place::{osm_canonical_from_url, PlaceDetails};
use crate::queries;

const OSM_URL_PREFIX: &str = "https://www.openstreetmap.org/";

enum ParentRef {
    /// Parent is a MapKy resource (review, route, collection, geo_capture,
    /// sequence, incident, or another mapky-namespaced post).
    Mapky {
        label: &'static str,
        compound_id: String,
    },
    /// Parent is an OSM URL — the post is anchored directly to a place.
    OsmPlace { osm_url: String },
}

pub async fn sync_put(data: &[u8], user_id: &str, post_id: &str) -> Result<(), DynError> {
    let post = <PubkyAppPost as Validatable>::try_from(data, post_id)
        .map_err(|e| format!("Failed to deserialize PubkyAppPost: {e}"))?;

    debug!("Indexing mapky-namespaced post: {user_id}/{post_id}");

    let graph = get_neo4j_graph()?;
    let post_details = MapkyPostDetails::from_pubky_post(&post, user_id, post_id);

    graph
        .run(queries::put::create_user(user_id, post_details.indexed_at))
        .await?;

    graph
        .run(queries::put::create_mapky_post(&post_details))
        .await?;

    if matches!(post.kind, PubkyAppPostKind::Collection) {
        crate::handlers::collection::sync_put(&post, user_id, post_id).await?;
    }

    if let Some(ref parent_uri) = post_details.parent_uri {
        match resolve_parent(&graph, parent_uri).await? {
            Some(ParentRef::OsmPlace { osm_url }) => {
                let osm_canonical = osm_canonical_from_url(&osm_url);

                // Ensure the Place node exists — geocode via Nominatim on
                // first sight, exactly like the review handler does.
                let already_exists: bool = {
                    let mut stream = graph
                        .execute(queries::get::place_exists(&osm_canonical))
                        .await?;
                    stream
                        .try_next()
                        .await?
                        .and_then(|row| row.get("exists").ok())
                        .unwrap_or(false)
                };
                if !already_exists {
                    let place = PlaceDetails::from_osm_url(&osm_url).await;
                    graph.run(queries::put::create_place(&place)).await?;
                }

                graph
                    .run(queries::put::link_post_to_place(
                        &post_details.id,
                        &osm_canonical,
                    ))
                    .await?;
            }
            Some(ParentRef::Mapky { label, compound_id }) => {
                graph
                    .run(queries::put::link_mapky_post_reply(
                        &post_details.id,
                        label,
                        &compound_id,
                    ))
                    .await?;
            }
            None => {
                // Cross-domain or unrecognised parent — keep the property,
                // skip the edge.
            }
        }
    }

    Ok(())
}

pub async fn del(user_id: &str, post_id: &str) -> Result<(), DynError> {
    debug!("Deleting mapky-namespaced post: {user_id}/{post_id}");

    let compound_id = format!("{user_id}:{post_id}");
    let graph = get_neo4j_graph()?;

    graph
        .run(queries::del::delete_mapky_post(user_id, &compound_id))
        .await?;
    graph
        .run(queries::del::delete_collection(user_id, &compound_id))
        .await?;

    Ok(())
}

async fn resolve_parent(
    graph: &Arc<dyn GraphOps>,
    parent_uri: &str,
) -> Result<Option<ParentRef>, DynError> {
    if parent_uri.starts_with(OSM_URL_PREFIX) {
        return Ok(Some(ParentRef::OsmPlace {
            osm_url: parent_uri.to_string(),
        }));
    }

    let path = match crate::extract_pub_path(parent_uri) {
        Some(v) => v,
        None => return Ok(None),
    };
    let (resource_type, resource_id) = match crate::split_resource(path) {
        Some(v) => v,
        None => return Ok(None),
    };
    let author_id = match crate::extract_user_id(parent_uri) {
        Some(v) => v,
        None => return Ok(None),
    };
    let compound_id = format!("{author_id}:{resource_id}");
    let label = mapky_resource_label(graph, resource_type, &compound_id).await?;
    let Some(label) = label else {
        return Ok(None);
    };
    Ok(Some(ParentRef::Mapky {
        label,
        compound_id,
    }))
}

/// Map a `/pub/mapky.app/{segment}/` segment to its Neo4j node label.
/// Kept in lockstep with `handlers::tag::neo4j_label_for` so the same set of
/// resources can be tagged AND replied to.
pub(crate) async fn mapky_resource_label(
    graph: &Arc<dyn GraphOps>,
    resource_type: &str,
    compound_id: &str,
) -> Result<Option<&'static str>, DynError> {
    Ok(match resource_type {
        "reviews" => Some("MapkyAppReview"),
        "posts" => {
            let exists: bool = graph
                .execute(queries::get::mapky_collection_exists(compound_id))
                .await?
                .try_next()
                .await?
                .and_then(|row| row.get("exists").ok())
                .unwrap_or(false);
            if exists {
                Some("MapkyAppCollection")
            } else {
                Some("MapkyAppPost")
            }
        }
        "collections" => Some("MapkyAppCollection"),
        "incidents" => Some("MapkyAppIncident"),
        "geo_captures" => Some("MapkyAppGeoCapture"),
        "routes" => Some("MapkyAppRoute"),
        "sequences" => Some("MapkyAppSequence"),
        _ => None,
    })
}
