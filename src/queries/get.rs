//! Neo4j read queries for the mapky plugin.

use nexus_common::db::graph::Query;

/// Activity dimensions a place can satisfy. Used as a multi-select OR
/// set: a place matches the filter if it satisfies ANY of the selected
/// activities. Replaces the old AND-of-three-booleans pattern, which
/// hit an impossible-intersection trap whenever the user wanted "any
/// place that has Mapky engagement" (a place with only posts, no
/// reviews/tags, was unreachable through the old filter).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PlaceActivity {
    /// `:User-[:TAGGED]->Place` exists (the place has at least one tag).
    Tagged,
    /// `:MapkyAppReview-[:ABOUT]->Place` exists.
    Reviewed,
    /// `:MapkyAppPost-[:ABOUT]->Place` exists (non-review post, comment, or media).
    Posted,
    /// `:MapkyAppCollection-[:CONTAINS]->Place` exists.
    Collected,
}

impl PlaceActivity {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "tagged" => Some(Self::Tagged),
            "reviewed" => Some(Self::Reviewed),
            "posted" => Some(Self::Posted),
            "collected" => Some(Self::Collected),
            _ => None,
        }
    }

    /// Cypher predicate against the bound `p:Place` row. Tagged and
    /// Reviewed read the denormalized counters on the Place node (cheap);
    /// Posted and Collected probe edges via `EXISTS { ... }` (one
    /// expand each — Neo4j short-circuits on first hit).
    fn cypher_predicate(self) -> &'static str {
        match self {
            Self::Tagged => "p.tag_count > 0",
            Self::Reviewed => "p.review_count > 0",
            Self::Posted => "EXISTS { (post:MapkyAppPost)-[:ABOUT]->(p) }",
            Self::Collected => "EXISTS { (:MapkyAppCollection)-[:CONTAINS]->(p) }",
        }
    }
}

/// Filters layered on top of the bbox.
///
/// `activities` — multi-select OR. Empty defaults to "any Mapky
/// engagement" (OR of all four activities). Selecting one or more
/// pills narrows further. This default exists because the BTCMap
/// sync floods Neo4j with `:Place` nodes that have no Mapky data
/// (no reviews/tags/posts/collections); without this default, every
/// /viewport call would return every BTC merchant in the bbox and
/// the place layer would be unfilterable. BTC merchants surface via
/// the dedicated `/btc/viewport` overlay instead.
///
/// `include_unengaged` — escape hatch that bypasses the
/// "any-Mapky-engagement" default and returns every Place node in
/// the bbox (the old behavior). Useful for admin / "show me the raw
/// graph" UIs; off by default.
///
/// `min_rating` — optional 0–10 floor (avg_rating is stored on the
/// 0–10 scale so half-stars stay precise; the API layer multiplies a
/// 0–5 user input by 2 before passing it through).
#[derive(Debug, Clone, Default)]
pub struct PlaceFilters {
    pub activities: Vec<PlaceActivity>,
    pub include_unengaged: bool,
    pub min_rating: Option<f64>,
}

impl PlaceFilters {
    /// Build the Cypher fragment that AND's onto the base bbox WHERE.
    /// Empty when `include_unengaged` is on AND no other filter is set
    /// — preserves the unfiltered raw-graph fast path.
    fn cypher_clause(&self) -> String {
        let mut out = String::new();
        // De-dup activities while preserving order — repeated tokens
        // shouldn't OR the same predicate twice.
        let mut seen = std::collections::HashSet::new();
        let active: Vec<&PlaceActivity> = self
            .activities
            .iter()
            .filter(|a| seen.insert(**a))
            .collect();

        let activity_clause: Option<String> = if !active.is_empty() {
            // Explicit activities: narrow to those.
            Some(
                active
                    .iter()
                    .map(|a| a.cypher_predicate())
                    .collect::<Vec<_>>()
                    .join(" OR "),
            )
        } else if !self.include_unengaged {
            // Empty + no opt-out: default to "any Mapky engagement".
            Some(
                [
                    PlaceActivity::Tagged,
                    PlaceActivity::Reviewed,
                    PlaceActivity::Posted,
                    PlaceActivity::Collected,
                ]
                .iter()
                .map(|a| a.cypher_predicate())
                .collect::<Vec<_>>()
                .join(" OR "),
            )
        } else {
            // include_unengaged + no explicit activities: no narrowing.
            None
        };
        if let Some(or_chain) = activity_clause {
            out.push_str(" AND (");
            out.push_str(&or_chain);
            out.push(')');
        }
        if let Some(min) = self.min_rating {
            // avg_rating stored 0–10; caller is responsible for the
            // 0–5 → 0–10 conversion.
            use std::fmt::Write;
            // f64 formatting via write!/format! is locale-independent and
            // emits a `.` decimal — safe to inline directly into Cypher.
            let _ = write!(&mut out, " AND p.avg_rating >= {min}");
        }
        out
    }
}

/// Fetch individual Place nodes within a lat/lon bounding box.
/// High-zoom path: the frontend is showing balloons, not clusters.
pub fn get_places_in_viewport(
    min_lat: f64,
    min_lon: f64,
    max_lat: f64,
    max_lon: f64,
    filters: &PlaceFilters,
    limit: i64,
) -> Query {
    let cypher = format!(
        "MATCH (p:Place)
         WHERE p.geocoded = true
           AND point.withinBBox(
             p.location,
             point({{latitude: $min_lat, longitude: $min_lon}}),
             point({{latitude: $max_lat, longitude: $max_lon}})
         ){filters}
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
                p.indexed_at AS indexed_at,
                p.name AS name,
                coalesce(p.accepts_bitcoin, false) AS accepts_bitcoin,
                coalesce(p.btc_onchain, false) AS btc_onchain,
                coalesce(p.btc_lightning, false) AS btc_lightning,
                coalesce(p.btc_lightning_contactless, false) AS btc_lightning_contactless
         LIMIT $limit",
        filters = filters.cypher_clause()
    );
    Query::new("mapky_viewport", &cypher)
        .param("min_lat", min_lat)
        .param("min_lon", min_lon)
        .param("max_lat", max_lat)
        .param("max_lon", max_lon)
        .param("limit", limit)
}

/// Cluster Place nodes into a `cell`-sized lat/lon grid and aggregate.
///
/// `cell` is in degrees; the frontend picks a value based on the
/// current zoom (smaller cells at higher zoom). Each cluster carries
/// total count + per-filter sub-counts so the UI can render the BTC /
/// reviewed / tagged ratios inside the bubble without a second query.
///
/// Returns at most `limit` clusters ordered by `total` desc, so dense
/// areas (cities) are guaranteed to appear before empty cells.
pub fn get_place_clusters_in_viewport(
    min_lat: f64,
    min_lon: f64,
    max_lat: f64,
    max_lon: f64,
    cell: f64,
    filters: &PlaceFilters,
    limit: i64,
) -> Query {
    // Cluster lat/lon = centroid of the cell's actual places (geo
    // mean), not the cell midpoint. Midpoint snapping looked rigid
    // (clusters sat on a perfect grid across the viewport) and made
    // the BTC + Mapky cluster sets land on top of each other in
    // every shared cell. With centroids the bubbles trace the actual
    // density pattern of the data and rarely overlap exactly between
    // layers; the BTC overlay layer also applies a small marker
    // offset client-side as a safety net so even when two centroids
    // do match, the bubbles sit side-by-side rather than stacked.
    let cypher = format!(
        "MATCH (p:Place)
         WHERE p.geocoded = true
           AND point.withinBBox(
             p.location,
             point({{latitude: $min_lat, longitude: $min_lon}}),
             point({{latitude: $max_lat, longitude: $max_lon}})
         ){filters}
         WITH p,
              floor(p.lat / $cell) AS lat_idx,
              floor(p.lon / $cell) AS lon_idx
         WITH lat_idx, lon_idx,
              count(p) AS total,
              sum(CASE WHEN p.review_count > 0 THEN 1 ELSE 0 END) AS reviewed,
              avg(p.lat) AS lat,
              avg(p.lon) AS lon
         RETURN lat, lon, total, reviewed
         ORDER BY total DESC
         LIMIT $limit",
        filters = filters.cypher_clause()
    );
    Query::new("mapky_viewport_clusters", &cypher)
        .param("min_lat", min_lat)
        .param("min_lon", min_lon)
        .param("max_lat", max_lat)
        .param("max_lon", max_lon)
        .param("cell", cell)
        .param("limit", limit)
}

/// Fetch Bitcoin-accepting Place nodes within a lat/lon bounding box.
/// Filters on `accepts_bitcoin = true`; relies on the same `Place.location`
/// point index as the generic viewport query.
pub fn get_btc_places_in_viewport(
    min_lat: f64,
    min_lon: f64,
    max_lat: f64,
    max_lon: f64,
    limit: i64,
) -> Query {
    Query::new(
        "mapky_btc_viewport",
        "MATCH (p:Place)
         WHERE p.accepts_bitcoin = true
           AND p.geocoded = true
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
                p.name AS name,
                coalesce(p.btc_onchain, false) AS btc_onchain,
                coalesce(p.btc_lightning, false) AS btc_lightning,
                coalesce(p.btc_lightning_contactless, false) AS btc_lightning_contactless
         LIMIT $limit",
    )
    .param("min_lat", min_lat)
    .param("min_lon", min_lon)
    .param("max_lat", max_lat)
    .param("max_lon", max_lon)
    .param("limit", limit)
}

/// Cluster BTC-accepting Place nodes into a `cell`-sized lat/lon
/// grid. Mirrors `get_place_clusters_in_viewport` but filters on
/// `accepts_bitcoin = true` and returns just `(lat, lon, total)` —
/// the BTC overlay's cluster bubbles don't carry sub-counts since
/// the overlay's only signal IS "BTC merchant".
///
/// Places that are both Mapky-engaged AND BTC are intentionally in
/// both cluster sets — once here, once in `get_place_clusters_in_viewport`.
/// The two clusters render in different colors (orange vs teal) so
/// the overlap is visible to the user.
pub fn get_btc_place_clusters_in_viewport(
    min_lat: f64,
    min_lon: f64,
    max_lat: f64,
    max_lon: f64,
    cell: f64,
    limit: i64,
) -> Query {
    Query::new(
        "mapky_btc_viewport_clusters",
        "MATCH (p:Place)
         WHERE p.accepts_bitcoin = true
           AND p.geocoded = true
           AND point.withinBBox(
             p.location,
             point({latitude: $min_lat, longitude: $min_lon}),
             point({latitude: $max_lat, longitude: $max_lon})
         )
         WITH p,
              floor(p.lat / $cell) AS lat_idx,
              floor(p.lon / $cell) AS lon_idx
         WITH lat_idx, lon_idx,
              count(p) AS total,
              avg(p.lat) AS lat,
              avg(p.lon) AS lon
         RETURN lat, lon, total
         ORDER BY total DESC
         LIMIT $limit",
    )
    .param("min_lat", min_lat)
    .param("min_lon", min_lon)
    .param("max_lat", max_lat)
    .param("max_lon", max_lon)
    .param("cell", cell)
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
                p.indexed_at AS indexed_at,
                p.name AS name,
                coalesce(p.accepts_bitcoin, false) AS accepts_bitcoin,
                coalesce(p.btc_onchain, false) AS btc_onchain,
                coalesce(p.btc_lightning, false) AS btc_lightning,
                coalesce(p.btc_lightning_contactless, false) AS btc_lightning_contactless",
    )
    .param("osm_canonical", osm_canonical)
}

/// Fetch `:MapkyAppPost` (cross-namespace comments) directly anchored to a
/// place via the `[:ABOUT]` edge — the symmetric anchor to reviews. Replies
/// to specific resources (reviews, routes, …) are fetched separately via the
/// `/v0/mapky/{resource_type}/{author}/{id}/posts` endpoint.
pub fn get_mapky_posts_for_place(osm_canonical: &str, skip: i64, limit: i64) -> Query {
    Query::new(
        "mapky_place_posts",
        "MATCH (u:User)-[:AUTHORED]->(p:MapkyAppPost)-[:ABOUT]->(:Place {osm_canonical: $osm_canonical})
         RETURN p.id AS id,
                u.id AS author_id,
                p.content AS content,
                p.kind AS kind,
                p.parent_uri AS parent_uri,
                p.attachments AS attachments,
                p.embed_uri AS embed_uri,
                p.embed_kind AS embed_kind,
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

/// Fetch all TAGGED relationships targeting a `:MapkyAppReview` or `:MapkyAppPost`.
/// `compound_id` is `author_id:resource_id`. `node_label` must be either
/// `"MapkyAppReview"` or `"MapkyAppPost"` and is plugin-controlled (safe to
/// interpolate). Returns one row per (tagger, label) pair, plus an `exists`
/// flag. If the target does not exist, the stream will be empty.
pub fn get_tags_for_mapky_resource(node_label: &str, compound_id: &str) -> Query {
    let cypher = format!(
        "MATCH (n:{node_label} {{id: $id}})
         OPTIONAL MATCH (tagger:User)-[tag:TAGGED]->(n)
         RETURN true AS exists, tag.label AS label, tagger.id AS tagger_id"
    );
    Query::new("mapky_resource_tags", &cypher).param("id", compound_id)
}

/// Check if a node with the given label and id exists.
/// Used for cross-domain tag/bookmark resolution.
pub fn mapky_node_exists(node_label: &str, compound_id: &str) -> Query {
    let cypher = format!("MATCH (n:{node_label} {{id: $id}}) RETURN count(n) > 0 AS exists");
    Query::new("mapky_node_exists", &cypher).param("id", compound_id)
}

/// Fetch a user's `:MapkyAppPost` (cross-namespace comments), most recent first.
pub fn get_user_mapky_posts(user_id: &str, skip: i64, limit: i64) -> Query {
    Query::new(
        "mapky_user_posts",
        "MATCH (u:User {id: $user_id})-[:AUTHORED]->(p:MapkyAppPost)
         RETURN p.id AS id,
                u.id AS author_id,
                p.content AS content,
                p.kind AS kind,
                p.parent_uri AS parent_uri,
                p.attachments AS attachments,
                p.embed_uri AS embed_uri,
                p.embed_kind AS embed_kind,
                p.indexed_at AS indexed_at
         ORDER BY p.indexed_at DESC
         SKIP $skip LIMIT $limit",
    )
    .param("user_id", user_id)
    .param("skip", skip)
    .param("limit", limit)
}

/// Fetch a user's `:MapkyAppReview` rows, most recent first.
pub fn get_user_reviews(user_id: &str, skip: i64, limit: i64) -> Query {
    Query::new(
        "mapky_user_reviews",
        "MATCH (u:User {id: $user_id})-[:AUTHORED]->(r:MapkyAppReview)-[:ABOUT]->(place:Place)
         RETURN r.id AS id,
                u.id AS author_id,
                place.osm_canonical AS osm_canonical,
                r.content AS content,
                r.rating AS rating,
                r.attachments AS attachments,
                r.indexed_at AS indexed_at
         ORDER BY r.indexed_at DESC
         SKIP $skip LIMIT $limit",
    )
    .param("user_id", user_id)
    .param("skip", skip)
    .param("limit", limit)
}

/// Fetch the entire `:MapkyAppPost` descendant tree under any MapKy resource.
/// Uses a variable-length `[:REPLY_TO*]` pattern so the endpoint returns ALL
/// nested replies in one round-trip — the frontend tree builder then nests
/// them via each post's `parent_uri` property.
///
/// Capped at depth 10 to bound the traversal; in practice threads are far
/// shallower. `parent_label` and `parent_compound_id` come from plugin code
/// (path-segment lookup), so interpolating the label into the cypher string
/// is safe.
pub fn get_replies_for_resource(
    parent_label: &str,
    parent_compound_id: &str,
    skip: i64,
    limit: i64,
) -> Query {
    let cypher = format!(
        "MATCH (reply:MapkyAppPost)-[:REPLY_TO*1..10]->(target:{parent_label} {{id: $parent_id}})
         WITH DISTINCT reply
         MATCH (u:User)-[:AUTHORED]->(reply)
         RETURN reply.id AS id,
                u.id AS author_id,
                reply.content AS content,
                reply.kind AS kind,
                reply.parent_uri AS parent_uri,
                reply.attachments AS attachments,
                reply.embed_uri AS embed_uri,
                reply.embed_kind AS embed_kind,
                reply.indexed_at AS indexed_at
         ORDER BY reply.indexed_at DESC
         SKIP $skip LIMIT $limit"
    );
    Query::new("mapky_resource_replies", &cypher)
        .param("parent_id", parent_compound_id)
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
                p.indexed_at AS indexed_at,
                p.name AS name,
                coalesce(p.accepts_bitcoin, false) AS accepts_bitcoin,
                coalesce(p.btc_onchain, false) AS btc_onchain,
                coalesce(p.btc_lightning, false) AS btc_lightning,
                coalesce(p.btc_lightning_contactless, false) AS btc_lightning_contactless",
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

/// Search for `:MapkyAppReview` rows tagged with a label that contains the query string.
pub fn search_reviews_by_tag(query_str: &str, limit: i64) -> Query {
    Query::new(
        "mapky_search_reviews_by_tag",
        "MATCH (tagger:User)-[t:TAGGED]->(review:MapkyAppReview)
         WHERE t.label CONTAINS $query
         WITH review, count(DISTINCT tagger) AS tagger_count
         ORDER BY tagger_count DESC
         LIMIT $limit
         MATCH (author:User)-[:AUTHORED]->(review)-[:ABOUT]->(place:Place)
         RETURN review.id AS id,
                author.id AS author_id,
                place.osm_canonical AS osm_canonical,
                review.content AS content,
                review.rating AS rating,
                review.attachments AS attachments,
                review.indexed_at AS indexed_at",
    )
    .param("query", query_str)
    .param("limit", limit)
}

/// Search for `:MapkyAppPost` (cross-namespace comments) tagged with a label.
pub fn search_posts_by_tag(query_str: &str, limit: i64) -> Query {
    Query::new(
        "mapky_search_posts_by_tag",
        "MATCH (tagger:User)-[t:TAGGED]->(post:MapkyAppPost)
         WHERE t.label CONTAINS $query
         WITH post, count(DISTINCT tagger) AS tagger_count
         ORDER BY tagger_count DESC
         LIMIT $limit
         MATCH (author:User)-[:AUTHORED]->(post)
         RETURN post.id AS id,
                author.id AS author_id,
                post.content AS content,
                post.kind AS kind,
                post.parent_uri AS parent_uri,
                post.attachments AS attachments,
                post.embed_uri AS embed_uri,
                post.embed_kind AS embed_kind,
                post.indexed_at AS indexed_at",
    )
    .param("query", query_str)
    .param("limit", limit)
}

/// Search for `:MapkyAppGeoCapture` rows tagged with a label.
pub fn search_geo_captures_by_tag(query_str: &str, limit: i64) -> Query {
    Query::new(
        "mapky_search_geo_captures_by_tag",
        "MATCH (tagger:User)-[t:TAGGED]->(g:MapkyAppGeoCapture)
         WHERE t.label CONTAINS $query
         WITH g, count(DISTINCT tagger) AS tagger_count
         ORDER BY tagger_count DESC
         LIMIT $limit
         MATCH (u:User)-[:AUTHORED]->(g)
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
    .param("query", query_str)
    .param("limit", limit)
}

/// Search for `:MapkyAppSequence` rows tagged with a label.
pub fn search_sequences_by_tag(query_str: &str, limit: i64) -> Query {
    let cypher = format!(
        "MATCH (tagger:User)-[t:TAGGED]->(s:MapkyAppSequence)
         WHERE t.label CONTAINS $query
         WITH s, count(DISTINCT tagger) AS tagger_count
         ORDER BY tagger_count DESC
         LIMIT $limit
         MATCH (u:User)-[:CAPTURED]->(s)
         RETURN {SEQUENCE_FIELDS}"
    );
    Query::new("mapky_search_sequences_by_tag", &cypher)
        .param("query", query_str)
        .param("limit", limit)
}

/// Search for `:MapkyAppIncident` rows tagged with a label.
pub fn search_incidents_by_tag(query_str: &str, limit: i64) -> Query {
    Query::new(
        "mapky_search_incidents_by_tag",
        "MATCH (tagger:User)-[t:TAGGED]->(i:MapkyAppIncident)
         WHERE t.label CONTAINS $query
         WITH i, count(DISTINCT tagger) AS tagger_count
         ORDER BY tagger_count DESC
         LIMIT $limit
         MATCH (u:User)-[:REPORTED]->(i)
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

/// Fetch `:MapkyAppReview` rows for a place, most recent first.
pub fn get_reviews_for_place(osm_canonical: &str, skip: i64, limit: i64) -> Query {
    Query::new(
        "mapky_place_reviews",
        "MATCH (u:User)-[:AUTHORED]->(r:MapkyAppReview)-[:ABOUT]->(:Place {osm_canonical: $osm_canonical})
         RETURN r.id AS id,
                u.id AS author_id,
                $osm_canonical AS osm_canonical,
                r.content AS content,
                r.rating AS rating,
                r.attachments AS attachments,
                r.indexed_at AS indexed_at
         ORDER BY r.indexed_at DESC
         SKIP $skip LIMIT $limit",
    )
    .param("osm_canonical", osm_canonical)
    .param("skip", skip)
    .param("limit", limit)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_filter_narrows_to_any_mapky_engagement() {
        let f = PlaceFilters::default();
        assert_eq!(
            f.cypher_clause(),
            " AND (p.tag_count > 0 OR p.review_count > 0 OR EXISTS { (post:MapkyAppPost)-[:ABOUT]->(p) } OR EXISTS { (:MapkyAppCollection)-[:CONTAINS]->(p) })"
        );
    }

    #[test]
    fn include_unengaged_emits_no_clause() {
        let f = PlaceFilters {
            include_unengaged: true,
            ..Default::default()
        };
        assert_eq!(f.cypher_clause(), "");
    }

    #[test]
    fn single_activity_wraps_in_parens() {
        let f = PlaceFilters {
            activities: vec![PlaceActivity::Tagged],
            include_unengaged: false,
            min_rating: None,
        };
        assert_eq!(f.cypher_clause(), " AND (p.tag_count > 0)");
    }

    #[test]
    fn multiple_activities_or_chain() {
        let f = PlaceFilters {
            activities: vec![
                PlaceActivity::Tagged,
                PlaceActivity::Reviewed,
                PlaceActivity::Posted,
            ],
            include_unengaged: false,
            min_rating: None,
        };
        assert_eq!(
            f.cypher_clause(),
            " AND (p.tag_count > 0 OR p.review_count > 0 OR EXISTS { (post:MapkyAppPost)-[:ABOUT]->(p) })"
        );
    }

    #[test]
    fn duplicate_activities_dedup() {
        let f = PlaceFilters {
            activities: vec![
                PlaceActivity::Tagged,
                PlaceActivity::Tagged,
                PlaceActivity::Reviewed,
            ],
            include_unengaged: false,
            min_rating: None,
        };
        assert_eq!(
            f.cypher_clause(),
            " AND (p.tag_count > 0 OR p.review_count > 0)"
        );
    }

    #[test]
    fn min_rating_alone_pairs_with_engagement_default() {
        // No activities + no include_unengaged → engagement OR is added.
        // min_rating is added on top so this combines correctly.
        let f = PlaceFilters {
            activities: vec![],
            include_unengaged: false,
            min_rating: Some(7.5),
        };
        assert!(f.cypher_clause().starts_with(" AND ("));
        assert!(f.cypher_clause().ends_with(" AND p.avg_rating >= 7.5"));
    }

    #[test]
    fn min_rating_with_include_unengaged_emits_only_rating() {
        let f = PlaceFilters {
            activities: vec![],
            include_unengaged: true,
            min_rating: Some(7.5),
        };
        assert_eq!(f.cypher_clause(), " AND p.avg_rating >= 7.5");
    }

    #[test]
    fn activity_and_min_rating_combine() {
        let f = PlaceFilters {
            activities: vec![PlaceActivity::Reviewed],
            include_unengaged: false,
            min_rating: Some(8.0),
        };
        assert_eq!(
            f.cypher_clause(),
            " AND (p.review_count > 0) AND p.avg_rating >= 8"
        );
    }

    #[test]
    fn parse_recognized_activities() {
        assert_eq!(PlaceActivity::parse("tagged"), Some(PlaceActivity::Tagged));
        assert_eq!(
            PlaceActivity::parse("reviewed"),
            Some(PlaceActivity::Reviewed)
        );
        assert_eq!(PlaceActivity::parse("posted"), Some(PlaceActivity::Posted));
        assert_eq!(
            PlaceActivity::parse("collected"),
            Some(PlaceActivity::Collected)
        );
        assert_eq!(PlaceActivity::parse("nonsense"), None);
        assert_eq!(PlaceActivity::parse(""), None);
    }
}
