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
        "MATCH (u:User)-[:AUTHORED]->(p:MapkyPost)-[:ABOUT]->(:Place {osm_canonical: $osm_canonical})
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

/// Fetch only review posts (rating > 0) for a place, most recent first.
pub fn get_reviews_for_place(osm_canonical: &str, skip: i64, limit: i64) -> Query {
    Query::new(
        "mapky_place_reviews",
        "MATCH (u:User)-[:AUTHORED]->(p:MapkyPost)-[:ABOUT]->(:Place {osm_canonical: $osm_canonical})
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
