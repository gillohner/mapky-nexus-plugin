//! Neo4j write queries for the mapky plugin.
//!
//! Two distinct post-like node families:
//! - `:MapkyAppReview` — rating-mandatory, place-anchored, never a reply.
//! - `:Post:MapkyAppPost` — generic comments and threaded replies.
//!   Dual-labeled so reply chains share core's `:Post` shape, while the
//!   `:MapkyAppPost` co-label scopes plugin queries.
//!
//! Identity for both follows the same compound-key convention as nexus:
//! `author_id:resource_id`. Compound IDs always contain a `:` so they can't
//! collide with bare timestamp IDs that core's `:Post.id` constraint enforces.

use crate::models::collection::CollectionDetails;
use crate::models::geo_capture::GeoCaptureDetails;
use crate::models::incident::IncidentDetails;
use crate::models::mapky_post::MapkyPostDetails;
use crate::models::place::PlaceDetails;
use crate::models::review::ReviewDetails;
use crate::models::route::RouteDetails;
use crate::models::sequence::SequenceDetails;
use nexus_common::db::graph::Query;

/// Create or update a User node (minimal — no profile data from post events).
pub fn create_user(user_id: &str, indexed_at: i64) -> Query {
    Query::new(
        "mapky_create_user",
        "MERGE (u:User {id: $id})
         ON CREATE SET u.indexed_at = $indexed_at",
    )
    .param("id", user_id)
    .param("indexed_at", indexed_at)
}

/// MERGE a Place node with spatial index point.
pub fn create_place(place: &PlaceDetails) -> Query {
    Query::new(
        "mapky_create_place",
        "MERGE (p:Place {osm_canonical: $osm_canonical})
         ON CREATE SET
             p.osm_type = $osm_type,
             p.osm_id = $osm_id,
             p.location = point({latitude: $lat, longitude: $lon}),
             p.lat = $lat,
             p.lon = $lon,
             p.geocoded = $geocoded,
             p.review_count = 0,
             p.avg_rating = 0.0,
             p.tag_count = 0,
             p.photo_count = 0,
             p.indexed_at = $indexed_at",
    )
    .param("osm_canonical", place.osm_canonical.clone())
    .param("osm_type", place.osm_type.clone())
    .param("osm_id", place.osm_id)
    .param("lat", place.lat)
    .param("lon", place.lon)
    .param("geocoded", place.geocoded)
    .param("indexed_at", place.indexed_at)
}

/// Create a `:MapkyAppReview` node with AUTHORED and ABOUT relationships.
/// `review.id` is the compound key `author_id:review_id`.
pub fn create_review(review: &ReviewDetails) -> Query {
    Query::new(
        "mapky_create_review",
        "MATCH (author:User {id: $author_id})
         MATCH (place:Place {osm_canonical: $osm_canonical})
         MERGE (author)-[:AUTHORED]->(r:MapkyAppReview {id: $review_id})
         MERGE (r)-[:ABOUT]->(place)
         ON CREATE SET r.indexed_at = $indexed_at
         SET r.content = $content,
             r.rating = $rating,
             r.attachments = $attachments",
    )
    .param("author_id", review.author_id.clone())
    .param("review_id", review.id.clone())
    .param("osm_canonical", review.osm_canonical.clone())
    .param("content", review.content.clone().unwrap_or_default())
    .param("rating", review.rating as i64)
    .param("attachments", review.attachments.clone())
    .param("indexed_at", review.indexed_at)
}

/// Create a dual-labeled `:Post:MapkyAppPost` node with an AUTHORED edge.
/// `post.id` is the compound key `author_id:post_id`. The compound colon
/// ensures these IDs don't collide with core's bare-id `:Post.id` uniqueness
/// constraint (we deliberately do not add our own constraint on `:Post`).
pub fn create_mapky_post(post: &MapkyPostDetails) -> Query {
    Query::new(
        "mapky_create_mapky_post",
        "MERGE (u:User {id: $author_id})
         MERGE (u)-[:AUTHORED]->(p:Post:MapkyAppPost {id: $post_id})
         ON CREATE SET p.indexed_at = $indexed_at
         SET p.content = $content,
             p.kind = $kind,
             p.parent_uri = $parent_uri,
             p.attachments = $attachments,
             p.embed_uri = $embed_uri,
             p.embed_kind = $embed_kind,
             p.namespace = 'mapky.app'",
    )
    .param("author_id", post.author_id.clone())
    .param("post_id", post.id.clone())
    .param("content", post.content.clone())
    .param("kind", post.kind.clone())
    .param("parent_uri", post.parent_uri.clone())
    .param("attachments", post.attachments.clone())
    .param("embed_uri", post.embed_uri.clone())
    .param("embed_kind", post.embed_kind.clone())
    .param("indexed_at", post.indexed_at)
}

/// Create an `[:ABOUT]` edge from a `:MapkyAppPost` (cross-namespace comment)
/// to a `:Place` — used when the post's parent is an OSM URL. Symmetric with
/// the review handler's anchor.
pub fn link_post_to_place(post_id: &str, osm_canonical: &str) -> Query {
    Query::new(
        "mapky_link_post_to_place",
        "MATCH (p:MapkyAppPost {id: $post_id})
         MATCH (place:Place {osm_canonical: $osm_canonical})
         MERGE (p)-[:ABOUT]->(place)",
    )
    .param("post_id", post_id)
    .param("osm_canonical", osm_canonical)
}

/// Create a `[:REPLY_TO]` edge from a `:MapkyAppPost` child to a parent
/// MapKy resource (any of the labels in `mapky_resource_label`).
///
/// Uses MATCH for the parent — if it doesn't exist yet (out-of-order delivery),
/// the query silently does nothing. `parent_label` is plugin-controlled (not
/// user input) so format-interpolating it into the cypher string is safe.
pub fn link_mapky_post_reply(child_id: &str, parent_label: &str, parent_id: &str) -> Query {
    let cypher = format!(
        "MATCH (child:MapkyAppPost {{id: $child_id}})
         MATCH (parent:{parent_label} {{id: $parent_id}})
         MERGE (child)-[:REPLY_TO]->(parent)"
    );
    Query::new("mapky_link_mapky_post_reply", &cypher)
        .param("child_id", child_id)
        .param("parent_id", parent_id)
}

/// Increment the running average rating on a Place after a new review.
pub fn increment_place_rating(osm_canonical: &str, rating: u8) -> Query {
    Query::new(
        "mapky_increment_rating",
        "MATCH (p:Place {osm_canonical: $osm_canonical})
         SET p.avg_rating = (p.avg_rating * p.review_count + $rating) / (p.review_count + 1),
             p.review_count = p.review_count + 1",
    )
    .param("osm_canonical", osm_canonical)
    .param("rating", rating as i64)
}

/// Create a TAGGED relationship from a User to a Place, and increment tag_count.
pub fn create_place_tag(
    tagger_user_id: &str,
    osm_canonical: &str,
    tag_id: &str,
    label: &str,
    indexed_at: i64,
) -> Query {
    Query::new(
        "mapky_create_place_tag",
        "MATCH (user:User {id: $user_id})
         MATCH (place:Place {osm_canonical: $osm_canonical})
         MERGE (user)-[t:TAGGED {label: $label}]->(place)
         ON CREATE SET t.indexed_at = $indexed_at,
                       t.id = $tag_id,
                       place.tag_count = place.tag_count + 1",
    )
    .param("user_id", tagger_user_id)
    .param("osm_canonical", osm_canonical)
    .param("tag_id", tag_id)
    .param("label", label)
    .param("indexed_at", indexed_at)
}

/// Roll back the running average rating after a review is deleted.
pub fn decrement_place_rating(osm_canonical: &str, rating: u8) -> Query {
    Query::new(
        "mapky_decrement_rating",
        "MATCH (p:Place {osm_canonical: $osm_canonical})
         WITH p,
              CASE WHEN p.review_count > 1
                   THEN (p.avg_rating * p.review_count - $rating) / (p.review_count - 1)
                   ELSE 0.0 END AS new_avg
         SET p.review_count = CASE WHEN p.review_count > 0 THEN p.review_count - 1 ELSE 0 END,
             p.avg_rating = new_avg",
    )
    .param("osm_canonical", osm_canonical)
    .param("rating", rating as i64)
}

// ── Incident ────────────────────────────────────────────────────────────

/// MERGE a MapkyAppIncident node with spatial point and REPORTED edge.
pub fn create_incident(incident: &IncidentDetails) -> Query {
    Query::new(
        "mapky_create_incident",
        "MATCH (author:User {id: $author_id})
         MERGE (author)-[:REPORTED]->(i:MapkyAppIncident {id: $id})
         ON CREATE SET i.indexed_at = $indexed_at
         SET i.incident_type = $incident_type,
             i.severity = $severity,
             i.location = point({latitude: $lat, longitude: $lon}),
             i.lat = $lat,
             i.lon = $lon,
             i.heading = $heading,
             i.description = $description,
             i.attachments = $attachments,
             i.expires_at = $expires_at",
    )
    .param("author_id", incident.author_id.clone())
    .param("id", incident.id.clone())
    .param("incident_type", incident.incident_type.clone())
    .param("severity", incident.severity.clone())
    .param("lat", incident.lat)
    .param("lon", incident.lon)
    .param("heading", incident.heading)
    .param("description", incident.description.clone())
    .param("attachments", incident.attachments.clone())
    .param("expires_at", incident.expires_at)
    .param("indexed_at", incident.indexed_at)
}

// ── GeoCapture ──────────────────────────────────────────────────────────

/// MERGE a MapkyAppGeoCapture node with spatial point and CAPTURED edge.
pub fn create_geo_capture(capture: &GeoCaptureDetails) -> Query {
    Query::new(
        "mapky_create_geo_capture",
        "MATCH (author:User {id: $author_id})
         MERGE (author)-[:CAPTURED]->(g:MapkyAppGeoCapture {id: $id})
         ON CREATE SET g.indexed_at = $indexed_at
         SET g.file_uri = $file_uri,
             g.kind = $kind,
             g.location = point({latitude: $lat, longitude: $lon}),
             g.lat = $lat,
             g.lon = $lon,
             g.ele = $ele,
             g.heading = $heading,
             g.pitch = $pitch,
             g.fov = $fov,
             g.caption = $caption,
             g.sequence_uri = $sequence_uri,
             g.sequence_index = $sequence_index,
             g.captured_at = $captured_at",
    )
    .param("author_id", capture.author_id.clone())
    .param("id", capture.id.clone())
    .param("file_uri", capture.file_uri.clone())
    .param("kind", capture.kind.clone())
    .param("lat", capture.lat)
    .param("lon", capture.lon)
    .param("ele", capture.ele)
    .param("heading", capture.heading)
    .param("pitch", capture.pitch)
    .param("fov", capture.fov)
    .param("caption", capture.caption.clone())
    .param("sequence_uri", capture.sequence_uri.clone())
    .param("sequence_index", capture.sequence_index)
    .param("captured_at", capture.captured_at)
    .param("indexed_at", capture.indexed_at)
}

// ── Collection ──────────────────────────────────────────────────────────

/// MERGE a MapkyAppCollection node with CREATED edge.
pub fn create_collection(collection: &CollectionDetails) -> Query {
    Query::new(
        "mapky_create_collection",
        "MATCH (author:User {id: $author_id})
         MERGE (author)-[:CREATED]->(c:MapkyAppCollection {id: $id})
         ON CREATE SET c.indexed_at = $indexed_at
         SET c.name = $name,
             c.description = $description,
             c.image_uri = $image_uri,
             c.color = $color",
    )
    .param("author_id", collection.author_id.clone())
    .param("id", collection.id.clone())
    .param("name", collection.name.clone())
    .param("description", collection.description.clone())
    .param("image_uri", collection.image_uri.clone())
    .param("color", collection.color.clone())
    .param("indexed_at", collection.indexed_at)
}

/// MERGE a CONTAINS edge from a Collection to a Place.
pub fn link_collection_place(collection_id: &str, osm_canonical: &str) -> Query {
    Query::new(
        "mapky_link_collection_place",
        "MATCH (c:MapkyAppCollection {id: $collection_id})
         MATCH (p:Place {osm_canonical: $osm_canonical})
         MERGE (c)-[:CONTAINS]->(p)",
    )
    .param("collection_id", collection_id)
    .param("osm_canonical", osm_canonical)
}

/// Remove CONTAINS edges to Places no longer in the collection's items list.
pub fn cleanup_collection_places(collection_id: &str, current_canonicals: &[String]) -> Query {
    Query::new(
        "mapky_cleanup_collection_places",
        "MATCH (c:MapkyAppCollection {id: $collection_id})-[r:CONTAINS]->(p:Place)
         WHERE NOT p.osm_canonical IN $current_canonicals
         DELETE r",
    )
    .param("collection_id", collection_id)
    .param("current_canonicals", current_canonicals.to_vec())
}

// ── Route ───────────────────────────────────────────────────────────────

/// MERGE a MapkyAppRoute node with bounding box and CREATED edge.
/// Full waypoint data stays on the homeserver — only metadata is indexed.
pub fn create_route(route: &RouteDetails) -> Query {
    Query::new(
        "mapky_create_route",
        "MATCH (author:User {id: $author_id})
         MERGE (author)-[:CREATED]->(r:MapkyAppRoute {id: $id})
         ON CREATE SET r.indexed_at = $indexed_at
         SET r.name = $name,
             r.description = $description,
             r.activity = $activity,
             r.distance_m = $distance_m,
             r.elevation_gain_m = $elevation_gain_m,
             r.elevation_loss_m = $elevation_loss_m,
             r.estimated_duration_s = $estimated_duration_s,
             r.image_uri = $image_uri,
             r.start_point = point({latitude: $start_lat, longitude: $start_lon}),
             r.start_lat = $start_lat,
             r.start_lon = $start_lon,
             r.min_lat = $min_lat,
             r.min_lon = $min_lon,
             r.max_lat = $max_lat,
             r.max_lon = $max_lon,
             r.waypoint_count = $waypoint_count",
    )
    .param("author_id", route.author_id.clone())
    .param("id", route.id.clone())
    .param("name", route.name.clone())
    .param("description", route.description.clone())
    .param("activity", route.activity.clone())
    .param("distance_m", route.distance_m)
    .param("elevation_gain_m", route.elevation_gain_m)
    .param("elevation_loss_m", route.elevation_loss_m)
    .param("estimated_duration_s", route.estimated_duration_s)
    .param("image_uri", route.image_uri.clone())
    .param("start_lat", route.start_lat)
    .param("start_lon", route.start_lon)
    .param("min_lat", route.min_lat)
    .param("min_lon", route.min_lon)
    .param("max_lat", route.max_lat)
    .param("max_lon", route.max_lon)
    .param("waypoint_count", route.waypoint_count)
    .param("indexed_at", route.indexed_at)
}

// ── Sequence ────────────────────────────────────────────────────────────

/// MERGE a MapkyAppSequence node with CAPTURED edge.
pub fn create_sequence(sequence: &SequenceDetails) -> Query {
    Query::new(
        "mapky_create_sequence",
        "MATCH (author:User {id: $author_id})
         MERGE (author)-[:CAPTURED]->(s:MapkyAppSequence {id: $id})
         ON CREATE SET s.indexed_at = $indexed_at
         SET s.name = $name,
             s.description = $description,
             s.kind = $kind,
             s.captured_at_start = $captured_at_start,
             s.captured_at_end = $captured_at_end,
             s.capture_count = $capture_count,
             s.min_lat = $min_lat,
             s.min_lon = $min_lon,
             s.max_lat = $max_lat,
             s.max_lon = $max_lon,
             s.device = $device",
    )
    .param("author_id", sequence.author_id.clone())
    .param("id", sequence.id.clone())
    .param("name", sequence.name.clone())
    .param("description", sequence.description.clone())
    .param("kind", sequence.kind.clone())
    .param("captured_at_start", sequence.captured_at_start)
    .param("captured_at_end", sequence.captured_at_end)
    .param("capture_count", sequence.capture_count)
    .param("min_lat", sequence.min_lat)
    .param("min_lon", sequence.min_lon)
    .param("max_lat", sequence.max_lat)
    .param("max_lon", sequence.max_lon)
    .param("device", sequence.device.clone())
    .param("indexed_at", sequence.indexed_at)
}

/// Create a TAGGED relationship from a user to a mapky resource node.
///
/// `node_label` and `node_property` come from plugin code (not user input),
/// so interpolating them into the query string is safe.
pub fn create_resource_tag(
    tagger_user_id: &str,
    node_label: &str,
    node_id: &str,
    tag_id: &str,
    label: &str,
    indexed_at: i64,
) -> Query {
    let cypher = format!(
        "MATCH (user:User {{id: $user_id}})
         MATCH (target:{node_label} {{id: $target_id}})
         OPTIONAL MATCH (user)-[existing:TAGGED {{label: $label}}]->(target)
         MERGE (user)-[t:TAGGED {{label: $label}}]->(target)
         ON CREATE SET t.indexed_at = $indexed_at,
                       t.id = $tag_id
         RETURN existing IS NOT NULL AS flag"
    );
    Query::new("mapky_create_resource_tag", &cypher)
        .param("user_id", tagger_user_id)
        .param("target_id", node_id)
        .param("tag_id", tag_id)
        .param("label", label)
        .param("indexed_at", indexed_at)
}

// ── BTCMap sync ─────────────────────────────────────────────────────────

/// Batch-upsert Bitcoin-accepting OSM places from BTCMap.
///
/// `rows` is a pre-built `BoltType::List` of maps; each map carries:
/// `osm_canonical`, `osm_type`, `osm_id`, `lat`, `lon`, `name`,
/// `btc_onchain`, `btc_lightning`, `btc_lightning_contactless`.
/// Construction lives in `btcmap_sync.rs` so this module stays Cypher-only.
///
/// Behavior:
/// - `MERGE` on `osm_canonical` so we adopt user-driven places that
///   already exist (created from posts/collections/tags).
/// - `ON CREATE SET p.source = 'btcmap'` distinguishes BTCMap-origin
///   nodes from user-driven ones; we never overwrite an existing
///   `source`. Newly-created BTCMap nodes get sensible defaults for
///   the social counters (review_count, avg_rating, tag_count, photo_count).
/// - Always sets `accepts_bitcoin` + the sub-flags + `btc_synced_at`,
///   matching the post-sync cleanup query that clears stale flags.
pub fn upsert_btcmap_places(rows: neo4rs::BoltType, synced_at: i64) -> Query {
    Query::new(
        "mapky_upsert_btcmap_places",
        "UNWIND $rows AS row
         MERGE (p:Place {osm_canonical: row.osm_canonical})
         ON CREATE SET
             p.source = 'btcmap',
             p.osm_type = row.osm_type,
             p.osm_id = row.osm_id,
             p.location = point({latitude: row.lat, longitude: row.lon}),
             p.lat = row.lat,
             p.lon = row.lon,
             p.geocoded = true,
             p.review_count = 0,
             p.avg_rating = 0.0,
             p.tag_count = 0,
             p.photo_count = 0,
             p.indexed_at = $synced_at,
             p.name = row.name
         SET p.accepts_bitcoin = true,
             p.btc_onchain = row.btc_onchain,
             p.btc_lightning = row.btc_lightning,
             p.btc_lightning_contactless = row.btc_lightning_contactless,
             p.btc_synced_at = $synced_at",
    )
    .param("rows", rows)
    .param("synced_at", synced_at)
}

/// Clear BTC flags from places that fell out of the latest BTCMap sync.
///
/// Run after `upsert_btcmap_places` for the full set: any place still
/// flagged `accepts_bitcoin = true` whose `btc_synced_at` is older than
/// the current sync timestamp lost its BTCMap entry. We don't delete
/// the node (it might be tagged/bookmarked/posted-about) — just clear
/// the BTC properties so the BTC viewport stops returning it.
pub fn clear_stale_btcmap_flags(synced_at: i64) -> Query {
    Query::new(
        "mapky_clear_stale_btcmap_flags",
        "MATCH (p:Place)
         WHERE p.accepts_bitcoin = true
           AND coalesce(p.btc_synced_at, 0) < $synced_at
         REMOVE p.accepts_bitcoin,
                p.btc_onchain,
                p.btc_lightning,
                p.btc_lightning_contactless,
                p.btc_synced_at",
    )
    .param("synced_at", synced_at)
}
