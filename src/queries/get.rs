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
         WHERE p.geocoded = true
           AND point.withinBBox(
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

/// Fetch a user's posts, most recent first.
pub fn get_user_posts(user_id: &str, skip: i64, limit: i64) -> Query {
    Query::new(
        "mapky_user_posts",
        "MATCH (u:User {id: $user_id})-[:AUTHORED]->(p:MapkyAppPost)-[:ABOUT]->(place:Place)
         RETURN p.id AS id,
                u.id AS author_id,
                place.osm_canonical AS osm_canonical,
                p.content AS content,
                p.rating AS rating,
                p.kind AS kind,
                p.parent_uri AS parent_uri,
                p.attachments AS attachments,
                p.indexed_at AS indexed_at
         ORDER BY p.indexed_at DESC
         SKIP $skip LIMIT $limit",
    )
    .param("user_id", user_id)
    .param("skip", skip)
    .param("limit", limit)
}

/// Fetch a user's incidents, most recent first.
pub fn get_user_incidents(user_id: &str, skip: i64, limit: i64) -> Query {
    Query::new(
        "mapky_user_incidents",
        "MATCH (u:User {id: $user_id})-[:REPORTED]->(i:MapkyAppIncident)
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
         SKIP $skip LIMIT $limit",
    )
    .param("user_id", user_id)
    .param("skip", skip)
    .param("limit", limit)
}

/// Fetch a user's geo captures, most recent first.
pub fn get_user_geo_captures(user_id: &str, skip: i64, limit: i64) -> Query {
    Query::new(
        "mapky_user_geo_captures",
        "MATCH (u:User {id: $user_id})-[:CAPTURED]->(g:MapkyAppGeoCapture)
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
                g.captured_at AS captured_at,
                g.indexed_at AS indexed_at
         ORDER BY g.indexed_at DESC
         SKIP $skip LIMIT $limit",
    )
    .param("user_id", user_id)
    .param("skip", skip)
    .param("limit", limit)
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
                g.captured_at AS captured_at,
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
                g.captured_at AS captured_at,
                g.indexed_at AS indexed_at",
    )
    .param("id", compound_id)
}

/// Fetch all captures belonging to a sequence (matched by `sequence_uri`), ordered
/// by `sequence_index` ascending.
pub fn get_captures_in_sequence(sequence_uri: &str, skip: i64, limit: i64) -> Query {
    Query::new(
        "mapky_captures_in_sequence",
        "MATCH (u:User)-[:CAPTURED]->(g:MapkyAppGeoCapture)
         WHERE g.sequence_uri = $sequence_uri
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
                g.captured_at AS captured_at,
                g.indexed_at AS indexed_at
         ORDER BY g.sequence_index ASC
         SKIP $skip LIMIT $limit",
    )
    .param("sequence_uri", sequence_uri)
    .param("skip", skip)
    .param("limit", limit)
}

/// Fetch all TAGGED relationships targeting a MapkyAppGeoCapture, aggregated
/// via `OPTIONAL MATCH`. Returns one row per (tagger, label) pair plus an
/// `exists` flag. If the capture does not exist, the stream is empty.
pub fn get_tags_for_geo_capture(compound_id: &str) -> Query {
    Query::new(
        "mapky_geo_capture_tags",
        "MATCH (g:MapkyAppGeoCapture {id: $id})
         OPTIONAL MATCH (tagger:User)-[tag:TAGGED]->(g)
         RETURN true AS exists, tag.label AS label, tagger.id AS tagger_id",
    )
    .param("id", compound_id)
}

/// Fetch MapkyAppGeoCapture nodes within a radius of a point, optionally
/// excluding captures that belong to a specific sequence. Used for
/// cross-sequence "nearby" navigation in the street-view experience.
pub fn get_nearby_captures(
    lat: f64,
    lon: f64,
    radius_meters: f64,
    exclude_sequence_uri: Option<&str>,
    limit: i64,
) -> Query {
    let seq_filter = match exclude_sequence_uri {
        Some(_) => "AND (g.sequence_uri IS NULL OR g.sequence_uri <> $exclude_seq)",
        None => "",
    };

    let cypher = format!(
        "MATCH (u:User)-[:CAPTURED]->(g:MapkyAppGeoCapture)
         WHERE point.distance(
             g.location,
             point({{latitude: $lat, longitude: $lon}})
         ) < $radius
         {seq_filter}
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
                g.captured_at AS captured_at,
                g.indexed_at AS indexed_at,
                point.distance(g.location, point({{latitude: $lat, longitude: $lon}})) AS distance
         ORDER BY distance ASC
         LIMIT $limit"
    );

    let mut q = Query::new("mapky_nearby_captures", cypher)
        .param("lat", lat)
        .param("lon", lon)
        .param("radius", radius_meters)
        .param("limit", limit);

    if let Some(seq_uri) = exclude_sequence_uri {
        q = q.param("exclude_seq", seq_uri.to_string());
    }

    q
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
         WITH u, c, collect('https://www.openstreetmap.org/' + p.osm_canonical) AS items
         RETURN c.id AS id,
                u.id AS author_id,
                c.name AS name,
                c.description AS description,
                items,
                c.image_uri AS image_uri,
                c.color AS color,
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
         WITH u, c, collect('https://www.openstreetmap.org/' + p.osm_canonical) AS items
         RETURN c.id AS id,
                u.id AS author_id,
                c.name AS name,
                c.description AS description,
                items,
                c.image_uri AS image_uri,
                c.color AS color,
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
         WITH u, c, collect('https://www.openstreetmap.org/' + all_p.osm_canonical) AS items
         RETURN c.id AS id,
                u.id AS author_id,
                c.name AS name,
                c.description AS description,
                items,
                c.image_uri AS image_uri,
                c.color AS color,
                c.indexed_at AS indexed_at
         ORDER BY c.indexed_at DESC",
    )
    .param("osm_canonical", osm_canonical)
}

/// Fetch public collections with at least one Place inside the given
/// bounding box. Used by the discover sidebar's "In this area" tab so
/// users can browse what others have curated near where they're
/// looking, without needing an account.
pub fn get_collections_in_viewport(
    min_lat: f64,
    min_lon: f64,
    max_lat: f64,
    max_lon: f64,
    limit: i64,
) -> Query {
    Query::new(
        "mapky_collections_viewport",
        "MATCH (u:User)-[:CREATED]->(c:MapkyAppCollection)-[:CONTAINS]->(p:Place)
         WHERE p.geocoded = true
           AND point.withinBBox(
             p.location,
             point({latitude: $min_lat, longitude: $min_lon}),
             point({latitude: $max_lat, longitude: $max_lon})
         )
         WITH DISTINCT c, u
         OPTIONAL MATCH (c)-[:CONTAINS]->(all_p:Place)
         WITH u, c, collect('https://www.openstreetmap.org/' + all_p.osm_canonical) AS items
         RETURN c.id AS id,
                u.id AS author_id,
                c.name AS name,
                c.description AS description,
                items,
                c.image_uri AS image_uri,
                c.color AS color,
                c.indexed_at AS indexed_at
         ORDER BY c.indexed_at DESC
         LIMIT $limit",
    )
    .param("min_lat", min_lat)
    .param("min_lon", min_lon)
    .param("max_lat", max_lat)
    .param("max_lon", max_lon)
    .param("limit", limit)
}

/// Fetch tags on a collection, aggregated by label.
pub fn get_tags_for_collection(compound_id: &str) -> Query {
    Query::new(
        "mapky_collection_tags",
        "MATCH (c:MapkyAppCollection {id: $id})
         OPTIONAL MATCH (tagger:User)-[tag:TAGGED]->(c)
         RETURN true AS exists, tag.label AS label, tagger.id AS tagger_id",
    )
    .param("id", compound_id)
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
                r.distance_m AS distance_m,
                r.elevation_gain_m AS elevation_gain_m,
                r.elevation_loss_m AS elevation_loss_m,
                r.estimated_duration_s AS estimated_duration_s,
                r.image_uri AS image_uri,
                r.min_lat AS min_lat, r.min_lon AS min_lon,
                r.max_lat AS max_lat, r.max_lon AS max_lon,
                r.start_lat AS start_lat, r.start_lon AS start_lon,
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
                r.distance_m AS distance_m,
                r.elevation_gain_m AS elevation_gain_m,
                r.elevation_loss_m AS elevation_loss_m,
                r.estimated_duration_s AS estimated_duration_s,
                r.image_uri AS image_uri,
                r.min_lat AS min_lat, r.min_lon AS min_lon,
                r.max_lat AS max_lat, r.max_lon AS max_lon,
                r.start_lat AS start_lat, r.start_lon AS start_lon,
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
                r.distance_m AS distance_m,
                r.elevation_gain_m AS elevation_gain_m,
                r.elevation_loss_m AS elevation_loss_m,
                r.estimated_duration_s AS estimated_duration_s,
                r.image_uri AS image_uri,
                r.min_lat AS min_lat, r.min_lon AS min_lon,
                r.max_lat AS max_lat, r.max_lon AS max_lon,
                r.start_lat AS start_lat, r.start_lon AS start_lon,
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

/// Fetch all TAGGED relationships targeting a MapkyAppRoute. Returns one
/// row per (tagger, label); `exists` is true if the route node was found
/// even when no tags are attached.
pub fn get_tags_for_mapky_route(compound_id: &str) -> Query {
    Query::new(
        "mapky_route_tags",
        "MATCH (r:MapkyAppRoute {id: $id})
         OPTIONAL MATCH (tagger:User)-[tag:TAGGED]->(r)
         RETURN true AS exists, tag.label AS label, tagger.id AS tagger_id",
    )
    .param("id", compound_id)
}

/// Fetch routes whose bounding box contains the given point, ordered by
/// distance from the route's start. Used by the place detail panel to
/// surface "routes that pass through here".
pub fn get_routes_near_point(lat: f64, lon: f64, limit: i64) -> Query {
    Query::new(
        "mapky_routes_near_point",
        "MATCH (u:User)-[:CREATED]->(r:MapkyAppRoute)
         WHERE r.min_lat <= $lat AND r.max_lat >= $lat
           AND r.min_lon <= $lon AND r.max_lon >= $lon
         RETURN r.id AS id,
                u.id AS author_id,
                r.name AS name,
                r.description AS description,
                r.activity AS activity,
                r.distance_m AS distance_m,
                r.elevation_gain_m AS elevation_gain_m,
                r.elevation_loss_m AS elevation_loss_m,
                r.estimated_duration_s AS estimated_duration_s,
                r.image_uri AS image_uri,
                r.min_lat AS min_lat, r.min_lon AS min_lon,
                r.max_lat AS max_lat, r.max_lon AS max_lon,
                r.start_lat AS start_lat, r.start_lon AS start_lon,
                r.waypoint_count AS waypoint_count,
                r.indexed_at AS indexed_at
         ORDER BY point.distance(
             r.start_point,
             point({latitude: $lat, longitude: $lon})
         ) ASC
         LIMIT $limit",
    )
    .param("lat", lat)
    .param("lon", lon)
    .param("limit", limit)
}

// ── Tag search queries ─────────────────────────────────────────────────

/// Search for Places tagged with a label that contains the query string.
pub fn search_places_by_tag(query_str: &str, limit: i64) -> Query {
    Query::new(
        "mapky_search_places_by_tag",
        "MATCH (tagger:User)-[t:TAGGED]->(p:Place)
         WHERE t.label CONTAINS $query
         WITH p, count(DISTINCT tagger) AS tagger_count
         ORDER BY tagger_count DESC
         LIMIT $limit
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
    .param("query", query_str)
    .param("limit", limit)
}

/// Search for Collections tagged with a label that contains the query string.
pub fn search_collections_by_tag(query_str: &str, limit: i64) -> Query {
    Query::new(
        "mapky_search_collections_by_tag",
        "MATCH (tagger:User)-[t:TAGGED]->(c:MapkyAppCollection)
         WHERE t.label CONTAINS $query
         WITH c, count(DISTINCT tagger) AS tagger_count
         ORDER BY tagger_count DESC
         LIMIT $limit
         OPTIONAL MATCH (u:User)-[:CREATED]->(c)
         OPTIONAL MATCH (c)-[:CONTAINS]->(p:Place)
         WITH u, c, tagger_count, collect(p.osm_canonical) AS items
         RETURN c.id AS id,
                u.id AS author_id,
                c.name AS name,
                c.description AS description,
                items,
                c.image_uri AS image_uri,
                c.color AS color,
                c.indexed_at AS indexed_at",
    )
    .param("query", query_str)
    .param("limit", limit)
}

/// Search for MapkyAppPosts tagged with a label that contains the query string.
pub fn search_posts_by_tag(query_str: &str, limit: i64) -> Query {
    Query::new(
        "mapky_search_posts_by_tag",
        "MATCH (tagger:User)-[t:TAGGED]->(post:MapkyAppPost)
         WHERE t.label CONTAINS $query
         WITH post, count(DISTINCT tagger) AS tagger_count
         ORDER BY tagger_count DESC
         LIMIT $limit
         MATCH (author:User)-[:AUTHORED]->(post)-[:ABOUT]->(place:Place)
         RETURN post.id AS id,
                author.id AS author_id,
                place.osm_canonical AS osm_canonical,
                post.content AS content,
                post.rating AS rating,
                post.kind AS kind,
                post.parent_uri AS parent_uri,
                post.attachments AS attachments,
                post.indexed_at AS indexed_at",
    )
    .param("query", query_str)
    .param("limit", limit)
}

/// Search for MapkyAppRoutes tagged with a label that contains the query string.
pub fn search_routes_by_tag(query_str: &str, limit: i64) -> Query {
    Query::new(
        "mapky_search_routes_by_tag",
        "MATCH (tagger:User)-[t:TAGGED]->(r:MapkyAppRoute)
         WHERE t.label CONTAINS $query
         WITH r, count(DISTINCT tagger) AS tagger_count
         ORDER BY tagger_count DESC
         LIMIT $limit
         MATCH (u:User)-[:CREATED]->(r)
         RETURN r.id AS id,
                u.id AS author_id,
                r.name AS name,
                r.description AS description,
                r.activity AS activity,
                r.distance_m AS distance_m,
                r.elevation_gain_m AS elevation_gain_m,
                r.elevation_loss_m AS elevation_loss_m,
                r.estimated_duration_s AS estimated_duration_s,
                r.image_uri AS image_uri,
                r.min_lat AS min_lat, r.min_lon AS min_lon,
                r.max_lat AS max_lat, r.max_lon AS max_lon,
                r.start_lat AS start_lat, r.start_lon AS start_lon,
                r.waypoint_count AS waypoint_count,
                r.indexed_at AS indexed_at",
    )
    .param("query", query_str)
    .param("limit", limit)
}

// ── Sequence queries ────────────────────────────────────────────────────

const SEQUENCE_FIELDS: &str = "s.id AS id,
        u.id AS author_id,
        s.name AS name,
        s.description AS description,
        s.kind AS kind,
        s.captured_at_start AS captured_at_start,
        s.captured_at_end AS captured_at_end,
        s.capture_count AS capture_count,
        s.min_lat AS min_lat, s.min_lon AS min_lon,
        s.max_lat AS max_lat, s.max_lon AS max_lon,
        s.device AS device,
        s.indexed_at AS indexed_at";

/// Fetch a single MapkyAppSequence by compound ID.
pub fn get_sequence_by_id(compound_id: &str) -> Query {
    let cypher = format!(
        "MATCH (u:User)-[:CAPTURED]->(s:MapkyAppSequence {{id: $id}})
         RETURN {SEQUENCE_FIELDS}"
    );
    Query::new("mapky_get_sequence", &cypher).param("id", compound_id)
}

/// Fetch a user's sequences, most recent first.
pub fn get_user_sequences(user_id: &str, skip: i64, limit: i64) -> Query {
    let cypher = format!(
        "MATCH (u:User {{id: $user_id}})-[:CAPTURED]->(s:MapkyAppSequence)
         RETURN {SEQUENCE_FIELDS}
         ORDER BY s.indexed_at DESC
         SKIP $skip LIMIT $limit"
    );
    Query::new("mapky_user_sequences", &cypher)
        .param("user_id", user_id)
        .param("skip", skip)
        .param("limit", limit)
}

/// Fetch tags on a MapkyAppSequence, aggregated by label.
pub fn get_tags_for_sequence(compound_id: &str) -> Query {
    Query::new(
        "mapky_sequence_tags",
        "MATCH (s:MapkyAppSequence {id: $id})
         OPTIONAL MATCH (tagger:User)-[tag:TAGGED]->(s)
         RETURN true AS exists, tag.label AS label, tagger.id AS tagger_id",
    )
    .param("id", compound_id)
}

/// Check if a MapkyAppSequence exists (for cross-domain resolution).
pub fn mapky_sequence_exists(compound_id: &str) -> Query {
    Query::new(
        "mapky_sequence_exists",
        "MATCH (s:MapkyAppSequence {id: $id}) RETURN count(s) > 0 AS exists",
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
