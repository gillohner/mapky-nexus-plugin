//! Tag event handler — processes `PubkyAppTag` blobs stored under
//! `/pub/mapky.app/tags/`.
//!
//! Inspects `tag.uri` to determine the target:
//! - OSM URL → `(User)-[:TAGGED]->(Place)` + increment `tag_count`
//! - Mapky resource URI → `(User)-[:TAGGED]->(MapkyApp*)` via plugin-local query

use chrono::Utc;
use futures::TryStreamExt;
use nexus_common::db::get_neo4j_graph;
use nexus_common::types::DynError;
use serde::Deserialize;
use tracing::debug;

/// Lightweight deserialization target — avoids depending on `pubky-app-specs`
/// types which may conflict with the version used by `nexus-common`.
#[derive(Deserialize)]
struct PubkyTag {
    uri: String,
    label: String,
}

use crate::models::place::{osm_canonical_from_url, PlaceDetails};
use crate::queries;

const OSM_URL_PREFIX: &str = "https://www.openstreetmap.org/";

/// Map a mapky resource type to its Neo4j node label.
/// Kept in lockstep with `handlers::mapky_post::mapky_resource_label`.
fn neo4j_label_for(resource_type: &str) -> Option<&'static str> {
    match resource_type {
        "reviews" => Some("MapkyAppReview"),
        "posts" => Some("MapkyAppPost"),
        "collections" => Some("MapkyAppCollection"),
        "incidents" => Some("MapkyAppIncident"),
        "geo_captures" => Some("MapkyAppGeoCapture"),
        "routes" => Some("MapkyAppRoute"),
        "sequences" => Some("MapkyAppSequence"),
        _ => None,
    }
}

pub async fn sync_put(data: &[u8], tagger_user_id: &str, tag_id: &str) -> Result<(), DynError> {
    let tag: PubkyTag = serde_json::from_slice(data)
        .map_err(|e| format!("Failed to deserialize PubkyAppTag: {e}"))?;

    debug!(
        "Indexing mapky tag {tag_id}: user={tagger_user_id} uri={} label={}",
        tag.uri, tag.label
    );

    let indexed_at = Utc::now().timestamp_millis();
    let graph = get_neo4j_graph()?;

    if tag.uri.starts_with(OSM_URL_PREFIX) {
        // ── Tag targets an OSM place ────────────────────────────────────
        let osm_canonical = osm_canonical_from_url(&tag.uri);

        // Ensure the Place node exists (geocode if new).
        let exists_query = queries::get::place_exists(&osm_canonical);
        let exists: bool = graph
            .execute(exists_query)
            .await?
            .try_next()
            .await?
            .and_then(|row| row.get("exists").ok())
            .unwrap_or(false);

        if !exists {
            let place = PlaceDetails::from_osm_url(&tag.uri).await;
            graph.run(queries::put::create_place(&place)).await?;
        }

        // Ensure tagger user exists.
        graph
            .run(queries::put::create_user(tagger_user_id, indexed_at))
            .await?;

        // Create the TAGGED relationship + increment tag_count.
        graph
            .run(queries::put::create_place_tag(
                tagger_user_id,
                &osm_canonical,
                tag_id,
                &tag.label,
                indexed_at,
            ))
            .await?;
    } else if tag.uri.starts_with("pubky://") {
        // ── Tag targets a mapky resource (post, collection, etc.) ───────
        let path = crate::extract_pub_path(&tag.uri)
            .ok_or_else(|| format!("Cannot extract path from tag URI: {}", tag.uri))?;
        let (resource_type, resource_id) = crate::split_resource(path)
            .ok_or_else(|| format!("Cannot split resource from tag URI path: {path}"))?;
        let uri_owner_id = crate::extract_user_id(&tag.uri)
            .ok_or_else(|| format!("Cannot extract user_id from tag URI: {}", tag.uri))?;

        let node_label = neo4j_label_for(resource_type).ok_or_else(|| {
            format!("Unknown mapky resource type for tagging: {resource_type}")
        })?;

        let compound_id = format!("{uri_owner_id}:{resource_id}");

        // Ensure tagger user exists.
        graph
            .run(queries::put::create_user(tagger_user_id, indexed_at))
            .await?;

        // Create TAGGED relationship from user to the mapky resource node.
        let query = queries::put::create_resource_tag(
            tagger_user_id,
            node_label,
            &compound_id,
            tag_id,
            &tag.label,
            indexed_at,
        );
        graph.run(query).await?;
    } else {
        debug!("Skipping tag with unrecognized URI scheme: {}", tag.uri);
    }

    Ok(())
}

pub async fn del(tagger_user_id: &str, tag_id: &str) -> Result<(), DynError> {
    debug!("Deleting mapky tag {tag_id} for user {tagger_user_id}");

    let graph = get_neo4j_graph()?;
    let query = queries::del::delete_tag(tagger_user_id, tag_id);
    graph.run(query).await?;

    Ok(())
}
