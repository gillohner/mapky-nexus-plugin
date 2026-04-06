//! Neo4j read queries for the mapky plugin.

use nexus_common::db::graph::Query;

/// Fetch Place nodes within a lat/lon bounding box.
pub fn get_places_in_viewport(
    min_lat: f64,
    min_lon: f64,
    max_lat: f64,
    max_lon: f64,
    limit: i64,
) -> Query {
    Query::new(
        "mapky_viewport",
        "MATCH (p:Place)
         WHERE point.withinBBox(
             p.location,
             point({latitude: $min_lat, longitude: $min_lon}),
             point({latitude: $max_lat, longitude: $max_lon})
         )
         RETURN p.osm_canonical AS osm_canonical,
                p.osm_type AS osm_type,
                p.osm_id AS osm_id,
                p.lat AS lat,
                p.lon AS lon,
                p.geocoded AS geocoded,
                p.review_count AS review_count,
                p.avg_rating AS avg_rating,
                p.tag_count AS tag_count,
                p.photo_count AS photo_count,
                p.indexed_at AS indexed_at
         LIMIT $limit",
    )
    .param("min_lat", min_lat)
    .param("min_lon", min_lon)
    .param("max_lat", max_lat)
    .param("max_lon", max_lon)
    .param("limit", limit)
}

/// Fetch a single Place by its osm_canonical key.
pub fn get_place_by_canonical(osm_canonical: &str) -> Query {
    Query::new(
        "mapky_get_place",
        "MATCH (p:Place {osm_canonical: $osm_canonical})
         RETURN p.osm_canonical AS osm_canonical,
                p.osm_type AS osm_type,
                p.osm_id AS osm_id,
                p.lat AS lat,
                p.lon AS lon,
                p.geocoded AS geocoded,
                p.review_count AS review_count,
                p.avg_rating AS avg_rating,
                p.tag_count AS tag_count,
                p.photo_count AS photo_count,
                p.indexed_at AS indexed_at",
    )
    .param("osm_canonical", osm_canonical)
}

/// Fetch MapkyPost nodes for a place, most recent first.
pub fn get_posts_for_place(osm_canonical: &str, skip: i64, limit: i64) -> Query {
    Query::new(
        "mapky_place_posts",
        "MATCH (u:User)-[:AUTHORED]->(p:MapkyAppPost)-[:ABOUT]->(:Place {osm_canonical: $osm_canonical})
         RETURN p.id AS id,
                u.id AS author_id,
                $osm_canonical AS osm_canonical,
                p.content AS content,
                p.rating AS rating,
                p.kind AS kind,
                p.parent_uri AS parent_uri,
                p.attachments AS attachments,
                p.indexed_at AS indexed_at
         ORDER BY p.indexed_at DESC
         SKIP $skip LIMIT $limit",
    )
    .param("osm_canonical", osm_canonical)
    .param("skip", skip)
    .param("limit", limit)
}

/// Check if a Place node exists by its osm_canonical key (lightweight — no data returned).
pub fn place_exists(osm_canonical: &str) -> Query {
    Query::new(
        "mapky_place_exists",
        "MATCH (p:Place {osm_canonical: $osm_canonical}) RETURN count(p) > 0 AS exists",
    )
    .param("osm_canonical", osm_canonical)
}

/// Fetch all TAGGED relationships targeting a MapkyPost.
/// Returns one row per (tagger, label) pair, plus an `exists` flag.
/// If the post does not exist, the stream will be empty.
pub fn get_tags_for_mapky_post(post_id: &str) -> Query {
    Query::new(
        "mapky_post_tags",
        "MATCH (p:MapkyAppPost {id: $post_id})
         OPTIONAL MATCH (tagger:User)-[tag:TAGGED]->(p)
         RETURN true AS exists, tag.label AS label, tagger.id AS tagger_id",
    )
    .param("post_id", post_id)
}

/// Check if a `MapkyPost` node exists by id.
/// Used for cross-domain tag/bookmark resolution.
pub fn mapky_post_exists(post_id: &str) -> Query {
    Query::new(
        "mapky_post_exists",
        "MATCH (p:MapkyAppPost {id: $post_id}) RETURN count(p) > 0 AS exists",
    )
    .param("post_id", post_id)
}

/// Fetch tags on a Place node, aggregated by label.
pub fn get_tags_for_place(osm_canonical: &str) -> Query {
    Query::new(
        "mapky_place_tags",
        "MATCH (p:Place {osm_canonical: $osm_canonical})
         OPTIONAL MATCH (tagger:User)-[tag:TAGGED]->(p)
         RETURN true AS exists, tag.label AS label, tagger.id AS tagger_id",
    )
    .param("osm_canonical", osm_canonical)
}

// ── Incident queries ────────────────────────────────────────────────────

/// Fetch MapkyAppIncident nodes within a bounding box.
pub fn get_incidents_in_viewport(
    min_lat: f64,
    min_lon: f64,
    max_lat: f64,
    max_lon: f64,
    limit: i64,
) -> Query {
    Query::new(
        "mapky_incidents_viewport",
        "MATCH (u:User)-[:REPORTED]->(i:MapkyAppIncident)
         WHERE point.withinBBox(
             i.location,
             point({latitude: $min_lat, longitude: $min_lon}),
             point({latitude: $max_lat, longitude: $max_lon})
         )
         RETURN i.id AS id,
                u.id AS author_id,
                i.incident_type AS incident_type,
                i.severity AS severity,
                i.lat AS lat, i.lon AS lon,
                i.heading AS heading,
                i.description AS description,
                i.attachments AS attachments,
                i.expires_at AS expires_at,
                i.indexed_at AS indexed_at
         ORDER BY i.indexed_at DESC
         LIMIT $limit",
    )
    .param("min_lat", min_lat)
    .param("min_lon", min_lon)
    .param("max_lat", max_lat)
    .param("max_lon", max_lon)
    .param("limit", limit)
}

/// Fetch a single MapkyAppIncident by compound ID.
pub fn get_incident_by_id(compound_id: &str) -> Query {
    Query::new(
        "mapky_get_incident",
        "MATCH (u:User)-[:REPORTED]->(i:MapkyAppIncident {id: $id})
         RETURN i.id AS id,
                u.id AS author_id,
                i.incident_type AS incident_type,
                i.severity AS severity,
                i.lat AS lat, i.lon AS lon,
                i.heading AS heading,
                i.description AS description,
                i.attachments AS attachments,
                i.expires_at AS expires_at,
                i.indexed_at AS indexed_at",
    )
    .param("id", compound_id)
}

/// Check if a MapkyAppIncident exists (for resolve_graph_node).
pub fn mapky_incident_exists(compound_id: &str) -> Query {
    Query::new(
        "mapky_incident_exists",
        "MATCH (i:MapkyAppIncident {id: $id}) RETURN count(i) > 0 AS exists",
    )
    .param("id", compound_id)
}

// ── GeoCapture queries ──────────────────────────────────────────────────

/// Fetch MapkyAppGeoCapture nodes within a bounding box.
pub fn get_geo_captures_in_viewport(
    min_lat: f64,
    min_lon: f64,
    max_lat: f64,
    max_lon: f64,
    limit: i64,
) -> Query {
    Query::new(
        "mapky_geo_captures_viewport",
        "MATCH (u:User)-[:CAPTURED]->(g:MapkyAppGeoCapture)
         WHERE point.withinBBox(
             g.location,
             point({latitude: $min_lat, longitude: $min_lon}),
             point({latitude: $max_lat, longitude: $max_lon})
         )
         RETURN g.id AS id,
                u.id AS author_id,
                g.file_uri AS file_uri,
                g.kind AS kind,
                g.lat AS lat, g.lon AS lon,
                g.ele AS ele,
                g.heading AS heading,
                g.pitch AS pitch,
                g.fov AS fov,
                g.caption AS caption,
                g.sequence_uri AS sequence_uri,
                g.sequence_index AS sequence_index,
                g.indexed_at AS indexed_at
         ORDER BY g.indexed_at DESC
         LIMIT $limit",
    )
    .param("min_lat", min_lat)
    .param("min_lon", min_lon)
    .param("max_lat", max_lat)
    .param("max_lon", max_lon)
    .param("limit", limit)
}

/// Fetch a single MapkyAppGeoCapture by compound ID.
pub fn get_geo_capture_by_id(compound_id: &str) -> Query {
    Query::new(
        "mapky_get_geo_capture",
        "MATCH (u:User)-[:CAPTURED]->(g:MapkyAppGeoCapture {id: $id})
         RETURN g.id AS id,
                u.id AS author_id,
                g.file_uri AS file_uri,
                g.kind AS kind,
                g.lat AS lat, g.lon AS lon,
                g.ele AS ele,
                g.heading AS heading,
                g.pitch AS pitch,
                g.fov AS fov,
                g.caption AS caption,
                g.sequence_uri AS sequence_uri,
                g.sequence_index AS sequence_index,
                g.indexed_at AS indexed_at",
    )
    .param("id", compound_id)
}

/// Check if a MapkyAppGeoCapture exists (for resolve_graph_node).
pub fn mapky_geo_capture_exists(compound_id: &str) -> Query {
    Query::new(
        "mapky_geo_capture_exists",
        "MATCH (g:MapkyAppGeoCapture {id: $id}) RETURN count(g) > 0 AS exists",
    )
    .param("id", compound_id)
}

// ── Collection queries ──────────────────────────────────────────────────

/// Fetch a single MapkyAppCollection by compound ID, including its items.
pub fn get_collection_by_id(compound_id: &str) -> Query {
    Query::new(
        "mapky_get_collection",
        "MATCH (u:User)-[:CREATED]->(c:MapkyAppCollection {id: $id})
         OPTIONAL MATCH (c)-[:CONTAINS]->(p:Place)
         WITH u, c, collect(p.osm_canonical) AS items
         RETURN c.id AS id,
                u.id AS author_id,
                c.name AS name,
                c.description AS description,
                items,
                c.image_uri AS image_uri,
                c.indexed_at AS indexed_at",
    )
    .param("id", compound_id)
}

/// Fetch a user's collections, most recent first.
pub fn get_user_collections(user_id: &str, skip: i64, limit: i64) -> Query {
    Query::new(
        "mapky_user_collections",
        "MATCH (u:User {id: $user_id})-[:CREATED]->(c:MapkyAppCollection)
         OPTIONAL MATCH (c)-[:CONTAINS]->(p:Place)
         WITH u, c, collect(p.osm_canonical) AS items
         RETURN c.id AS id,
                u.id AS author_id,
                c.name AS name,
                c.description AS description,
                items,
                c.image_uri AS image_uri,
                c.indexed_at AS indexed_at
         ORDER BY c.indexed_at DESC
         SKIP $skip LIMIT $limit",
    )
    .param("user_id", user_id)
    .param("skip", skip)
    .param("limit", limit)
}

/// Fetch collections that contain a specific place.
pub fn get_collections_containing_place(osm_canonical: &str) -> Query {
    Query::new(
        "mapky_collections_for_place",
        "MATCH (u:User)-[:CREATED]->(c:MapkyAppCollection)-[:CONTAINS]->(p:Place {osm_canonical: $osm_canonical})
         OPTIONAL MATCH (c)-[:CONTAINS]->(all_p:Place)
         WITH u, c, collect(all_p.osm_canonical) AS items
         RETURN c.id AS id,
                u.id AS author_id,
                c.name AS name,
                c.description AS description,
                items,
                c.image_uri AS image_uri,
                c.indexed_at AS indexed_at
         ORDER BY c.indexed_at DESC",
    )
    .param("osm_canonical", osm_canonical)
}

/// Check if a MapkyAppCollection exists (for resolve_graph_node).
pub fn mapky_collection_exists(compound_id: &str) -> Query {
    Query::new(
        "mapky_collection_exists",
        "MATCH (c:MapkyAppCollection {id: $id}) RETURN count(c) > 0 AS exists",
    )
    .param("id", compound_id)
}

// ── Route queries ───────────────────────────────────────────────────────

/// Fetch MapkyAppRoute nodes whose bounding box overlaps the viewport.
pub fn get_routes_in_viewport(
    min_lat: f64,
    min_lon: f64,
    max_lat: f64,
    max_lon: f64,
    limit: i64,
) -> Query {
    Query::new(
        "mapky_routes_viewport",
        "MATCH (u:User)-[:CREATED]->(r:MapkyAppRoute)
         WHERE r.max_lat >= $min_lat AND r.min_lat <= $max_lat
           AND r.max_lon >= $min_lon AND r.min_lon <= $max_lon
         RETURN r.id AS id,
                u.id AS author_id,
                r.name AS name,
                r.description AS description,
                r.activity AS activity,
                r.difficulty AS difficulty,
                r.distance_m AS distance_m,
                r.elevation_gain_m AS elevation_gain_m,
                r.elevation_loss_m AS elevation_loss_m,
                r.estimated_duration_s AS estimated_duration_s,
                r.image_uri AS image_uri,
                r.min_lat AS min_lat, r.min_lon AS min_lon,
                r.max_lat AS max_lat, r.max_lon AS max_lon,
                r.waypoint_count AS waypoint_count,
                r.indexed_at AS indexed_at
         ORDER BY r.indexed_at DESC
         LIMIT $limit",
    )
    .param("min_lat", min_lat)
    .param("min_lon", min_lon)
    .param("max_lat", max_lat)
    .param("max_lon", max_lon)
    .param("limit", limit)
}

/// Fetch a single MapkyAppRoute by compound ID.
pub fn get_route_by_id(compound_id: &str) -> Query {
    Query::new(
        "mapky_get_route",
        "MATCH (u:User)-[:CREATED]->(r:MapkyAppRoute {id: $id})
         RETURN r.id AS id,
                u.id AS author_id,
                r.name AS name,
                r.description AS description,
                r.activity AS activity,
                r.difficulty AS difficulty,
                r.distance_m AS distance_m,
                r.elevation_gain_m AS elevation_gain_m,
                r.elevation_loss_m AS elevation_loss_m,
                r.estimated_duration_s AS estimated_duration_s,
                r.image_uri AS image_uri,
                r.min_lat AS min_lat, r.min_lon AS min_lon,
                r.max_lat AS max_lat, r.max_lon AS max_lon,
                r.waypoint_count AS waypoint_count,
                r.indexed_at AS indexed_at",
    )
    .param("id", compound_id)
}

/// Fetch a user's routes, most recent first.
pub fn get_user_routes(user_id: &str, skip: i64, limit: i64) -> Query {
    Query::new(
        "mapky_user_routes",
        "MATCH (u:User {id: $user_id})-[:CREATED]->(r:MapkyAppRoute)
         RETURN r.id AS id,
                u.id AS author_id,
                r.name AS name,
                r.description AS description,
                r.activity AS activity,
                r.difficulty AS difficulty,
                r.distance_m AS distance_m,
                r.elevation_gain_m AS elevation_gain_m,
                r.elevation_loss_m AS elevation_loss_m,
                r.estimated_duration_s AS estimated_duration_s,
                r.image_uri AS image_uri,
                r.min_lat AS min_lat, r.min_lon AS min_lon,
                r.max_lat AS max_lat, r.max_lon AS max_lon,
                r.waypoint_count AS waypoint_count,
                r.indexed_at AS indexed_at
         ORDER BY r.indexed_at DESC
         SKIP $skip LIMIT $limit",
    )
    .param("user_id", user_id)
    .param("skip", skip)
    .param("limit", limit)
}

/// Check if a MapkyAppRoute exists (for resolve_graph_node).
pub fn mapky_route_exists(compound_id: &str) -> Query {
    Query::new(
        "mapky_route_exists",
        "MATCH (r:MapkyAppRoute {id: $id}) RETURN count(r) > 0 AS exists",
    )
    .param("id", compound_id)
}

/// Fetch only review posts (rating > 0) for a place, most recent first.
pub fn get_reviews_for_place(osm_canonical: &str, skip: i64, limit: i64) -> Query {
    Query::new(
        "mapky_place_reviews",
        "MATCH (u:User)-[:AUTHORED]->(p:MapkyAppPost)-[:ABOUT]->(:Place {osm_canonical: $osm_canonical})
         WHERE p.rating IS NOT NULL AND p.rating > 0
         RETURN p.id AS id,
                u.id AS author_id,
                $osm_canonical AS osm_canonical,
                p.content AS content,
                p.rating AS rating,
                p.kind AS kind,
                p.parent_uri AS parent_uri,
                p.attachments AS attachments,
                p.indexed_at AS indexed_at
         ORDER BY p.indexed_at DESC
         SKIP $skip LIMIT $limit",
    )
    .param("osm_canonical", osm_canonical)
    .param("skip", skip)
    .param("limit", limit)
}
