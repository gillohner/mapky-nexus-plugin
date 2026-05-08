//! Axum API routes for the mapky plugin.
//! Mounted at `/v0/mapky/` by nexusd.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use futures::TryStreamExt;
use nexus_common::db::get_neo4j_graph;
use nexus_common::plugin::PluginContext;
use serde::{Deserialize, Serialize};
use utoipa::OpenApi;

use crate::btcmap_sync::{self, SyncStatus};
use crate::handlers::mapky_post::mapky_resource_label;
use crate::models::collection::CollectionDetails;
use crate::models::geo_capture::GeoCaptureDetails;
use crate::models::incident::IncidentDetails;
use crate::models::mapky_post::MapkyPostDetails;
use crate::models::place::PlaceDetails;
use crate::models::review::ReviewDetails;
use crate::models::route::RouteDetails;
use crate::models::sequence::SequenceDetails;
use crate::models::tag::PostTagDetails;
use crate::osm::{
    batch_lookup_cached, reverse_cached, search_cached, NominatimLookup, SearchParams,
};
use crate::queries;
use crate::routing::{self as routing_proxy, RouteOutcome};

pub fn routes(ctx: PluginContext) -> Router {
    Router::new()
        // ── Place ──
        .route("/viewport", get(viewport))
        // Composite map-viewport: one request, up to four parallel Neo4j
        // queries (places + collections + captures + routes), narrowed by
        // `include`. See `viewport_multi`.
        .route("/viewport/all", get(viewport_multi))
        .route("/place/{osm_type}/{osm_id}", get(place_detail))
        // Composite place-detail: detail + reviews + posts + tags +
        // collections + routes in one request. See `place_detail_full`.
        .route("/place/{osm_type}/{osm_id}/full", get(place_detail_full))
        .route("/place/{osm_type}/{osm_id}/reviews", get(place_reviews))
        .route("/place/{osm_type}/{osm_id}/posts", get(place_posts))
        .route("/place/{osm_type}/{osm_id}/tags", get(place_tags))
        .route("/place/{osm_type}/{osm_id}/routes", get(place_routes))
        // ── Review ──
        .route("/reviews/{author_id}/{review_id}/tags", get(review_tags))
        .route("/reviews/user/{user_id}", get(user_reviews))
        // ── Post (cross-namespace PubkyAppPost stored at /pub/mapky.app/posts/) ──
        .route("/posts/{author_id}/{post_id}/tags", get(post_tags))
        .route("/posts/user/{user_id}", get(user_posts))
        // ── Replies — `:MapkyAppPost` nodes whose `[:REPLY_TO]` targets the resource ──
        .route(
            "/{resource_type}/{author_id}/{resource_id}/posts",
            get(resource_replies),
        )
        // ── Incident ──
        .route("/incidents/viewport", get(incidents_viewport))
        .route("/incidents/{author_id}/{incident_id}", get(incident_detail))
        .route("/incidents/user/{user_id}", get(user_incidents))
        // ── GeoCapture ──
        .route("/geo_captures/viewport", get(geo_captures_viewport))
        .route(
            "/geo_captures/{author_id}/{capture_id}",
            get(geo_capture_detail),
        )
        .route(
            "/geo_captures/{author_id}/{capture_id}/tags",
            get(geo_capture_tags),
        )
        .route("/geo_captures/user/{user_id}", get(user_geo_captures))
        .route("/geo_captures/nearby", get(nearby_geo_captures))
        // ── Sequence ──
        .route("/sequences/{author_id}/{sequence_id}", get(sequence_detail))
        .route(
            "/sequences/{author_id}/{sequence_id}/tags",
            get(sequence_tags),
        )
        .route(
            "/sequences/{author_id}/{sequence_id}/captures",
            get(sequence_captures),
        )
        .route("/sequences/user/{user_id}", get(user_sequences))
        // ── Collection ──
        .route("/collections/viewport", get(collections_viewport))
        .route(
            "/collections/{author_id}/{collection_id}",
            get(collection_detail),
        )
        .route("/collections/user/{user_id}", get(user_collections))
        .route(
            "/collections/{author_id}/{collection_id}/tags",
            get(collection_tags),
        )
        .route(
            "/collections/place/{osm_type}/{osm_id}",
            get(collections_for_place),
        )
        // ── Route ──
        .route("/routes/viewport", get(routes_viewport))
        .route("/routes/{author_id}/{route_id}", get(route_detail))
        .route("/routes/{author_id}/{route_id}/tags", get(route_tags))
        .route("/routes/user/{user_id}", get(user_routes))
        // ── Search ──
        .route("/search/tags", get(search_tags))
        // ── OSM auxiliary services ──
        .route("/osm/lookup", get(osm_lookup))
        .route("/osm/search", get(osm_search))
        .route("/osm/reverse", get(osm_reverse))
        // ── BTC POI overlay (BTCMap-sourced) ──
        .route("/btc/viewport", get(btc_viewport))
        .route("/btc/status", get(btc_status))
        // ── Cached routing proxy ──
        .route("/routing/valhalla", post(routing_valhalla))
        .with_state(ctx)
}

#[derive(OpenApi)]
#[openapi(
    tags(
        (name = "Place", description = "Places (OSM nodes/ways) and spatial queries"),
        (name = "Post", description = "Posts and reviews on places"),
        (name = "Incident", description = "Geo-located incidents"),
        (name = "GeoCapture", description = "Geo-located captures (photos, audio, video)"),
        (name = "Sequence", description = "Capture sessions grouping multiple GeoCaptures"),
        (name = "Collection", description = "Curated collections of places"),
        (name = "Route", description = "Routes and trails"),
        (name = "Search", description = "Cross-resource search"),
        (name = "OSM", description = "Cached Nominatim lookups (rate-limited proxy)"),
        (name = "BTC", description = "BTCMap-derived Bitcoin-accepting places"),
        (name = "Routing", description = "Cached proxy for Valhalla routing"),
    ),
    paths(
        viewport, viewport_multi,
        place_detail, place_detail_full,
        place_reviews, place_posts, place_tags, place_routes,
        review_tags, user_reviews,
        post_tags, user_posts, resource_replies,
        incidents_viewport, incident_detail, user_incidents,
        geo_captures_viewport, geo_capture_detail, geo_capture_tags, user_geo_captures, nearby_geo_captures,
        sequence_detail, sequence_tags, sequence_captures, user_sequences,
        collections_viewport, collection_detail, user_collections, collections_for_place, collection_tags,
        routes_viewport, route_detail, route_tags, user_routes,
        search_tags,
        osm_lookup,
        osm_search,
        osm_reverse,
        btc_viewport,
        btc_status,
        routing_valhalla,
    ),
    components(schemas(
        PlaceDetails, ReviewDetails, MapkyPostDetails, PostTagDetails,
        IncidentDetails, GeoCaptureDetails, SequenceDetails, CollectionDetails, RouteDetails,
        ViewportQuery, PlaceViewportQuery, MultiViewportQuery, MultiViewportResponse,
        PlaceFullQuery, PlaceFullResponse,
        PostsQuery, PaginationQuery, NearbyQuery,
        TagSearchQuery, TagSearchResponse,
        OsmLookupQuery, OsmSearchQuery, OsmReverseQuery, NominatimLookup,
        BitcoinPoi, SyncStatus,
        PlaceCluster, ViewportResponse,
    ))
)]
pub struct MapkyApiDoc;

// ── Query param structs ──────────────────────────────────────────────────────

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct ViewportQuery {
    pub min_lat: f64,
    pub min_lon: f64,
    pub max_lat: f64,
    pub max_lon: f64,
    #[serde(default = "default_limit")]
    pub limit: i64,
}

/// Place-viewport query — adds zoom + optional filter dimensions so
/// the handler can decide between cluster and individual rendering
/// and narrow the result without a second round-trip.
///
/// Filters:
///   `activity` — comma-separated multi-select OR (`tagged,reviewed,posted,collected`).
///   `min_rating` — 0.0–5.0 floor on the place's average rating.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct PlaceViewportQuery {
    pub min_lat: f64,
    pub min_lon: f64,
    pub max_lat: f64,
    pub max_lon: f64,
    /// Current MapLibre zoom (0-22). Below `CLUSTER_ZOOM_THRESHOLD`
    /// the response is `{kind:"clusters"}`; at or above, individual
    /// places. Defaults to a high zoom when omitted so legacy callers
    /// without a `zoom` param keep getting individual places.
    #[serde(default = "default_viewport_zoom")]
    pub zoom: u8,
    #[serde(default = "default_limit")]
    pub limit: i64,
    /// Comma-separated activity OR set. Recognized: `tagged`, `reviewed`,
    /// `posted`, `collected`. Unknown tokens are silently ignored.
    #[serde(default)]
    pub activity: Option<String>,
    /// Minimum average rating on the user-facing 0–5 scale. Translated
    /// to the internal 0–10 storage scale before hitting Cypher.
    #[serde(default)]
    pub min_rating: Option<f64>,
    /// When true AND no explicit activities are selected, return every
    /// Place node in the bbox (including BTCMap-synced merchants with
    /// no Mapky engagement). Off by default — the place layer hides
    /// unengaged BTC merchants so they only surface via the dedicated
    /// `/btc/viewport` overlay.
    #[serde(default)]
    pub include_unengaged: bool,
}

fn default_viewport_zoom() -> u8 {
    13
}

/// Composite-viewport query — superset of `PlaceViewportQuery` with an
/// `include` selector. The handler runs one Neo4j query per requested
/// layer in parallel and returns a single envelope, eliminating the four
/// independent fetches the frontend used to fan out per pan.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct MultiViewportQuery {
    pub min_lat: f64,
    pub min_lon: f64,
    pub max_lat: f64,
    pub max_lon: f64,
    /// Zoom for the place layer's cluster/individual decision. Ignored
    /// by the other layers.
    #[serde(default = "default_viewport_zoom")]
    pub zoom: u8,
    #[serde(default = "default_limit")]
    pub limit: i64,
    /// Comma-separated layer names. Recognized: `places`, `collections`,
    /// `captures`, `routes`. Unknown tokens are silently ignored. When
    /// omitted, defaults to `places` so the endpoint is useful as a
    /// drop-in replacement for `/viewport`.
    #[serde(default)]
    pub include: Option<String>,
    /// Comma-separated activity OR set, only consulted when
    /// `include` selects `places`. See `PlaceViewportQuery::activity`.
    #[serde(default)]
    pub activity: Option<String>,
    /// 0.0–5.0 minimum-rating filter, only consulted when `include`
    /// selects `places`.
    #[serde(default)]
    pub min_rating: Option<f64>,
    /// Opt-out for the place layer's "any-Mapky-engagement" default,
    /// only consulted when `include` selects `places`. See
    /// `PlaceViewportQuery::include_unengaged`.
    #[serde(default)]
    pub include_unengaged: bool,
}

/// Composite-viewport response. Each field is `Some` only when the
/// matching layer was selected via `include`; absent fields are omitted
/// from the JSON entirely (`skip_serializing_if`) so a `?include=places`
/// caller gets the same wire bytes whether the other layers exist or not.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct MultiViewportResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub places: Option<ViewportResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collections: Option<Vec<CollectionDetails>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub captures: Option<Vec<GeoCaptureDetails>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub routes: Option<Vec<RouteDetails>>,
}

/// Composite envelope returned by `/v0/mapky/place/{type}/{id}/full`.
/// All six slices are computed in one request via parallel Neo4j
/// queries; the frontend's PlacePanel reads the whole struct in one
/// shot instead of mounting six independent `useQuery` hooks.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct PlaceFullResponse {
    pub detail: PlaceDetails,
    pub reviews: Vec<ReviewDetails>,
    pub posts: Vec<MapkyPostDetails>,
    pub tags: Vec<PostTagDetails>,
    pub collections: Vec<CollectionDetails>,
    pub routes: Vec<RouteDetails>,
}

/// Pagination knobs for the composite place-detail endpoint. Each
/// limit applies to the matching slice; defaults match the
/// single-purpose endpoints (reviews/posts: 100; routes: 50).
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct PlaceFullQuery {
    #[serde(default = "default_limit")]
    pub reviews_limit: i64,
    #[serde(default = "default_limit")]
    pub posts_limit: i64,
    #[serde(default = "default_routes_near_limit")]
    pub routes_limit: i64,
}

fn default_routes_near_limit() -> i64 {
    50
}

#[derive(Debug, Default, Clone, Copy)]
struct IncludeSet {
    places: bool,
    collections: bool,
    captures: bool,
    routes: bool,
}

fn parse_include(raw: Option<&str>) -> IncludeSet {
    let Some(raw) = raw else {
        // Default: place layer only — same surface as `/viewport`.
        return IncludeSet {
            places: true,
            ..Default::default()
        };
    };
    let mut set = IncludeSet::default();
    for tok in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        match tok {
            "places" => set.places = true,
            "collections" => set.collections = true,
            "captures" => set.captures = true,
            "routes" => set.routes = true,
            _ => {} // unknown — ignore, forward-compat
        }
    }
    // Defensive: an `include=` with only unknown tokens would yield an
    // all-false set and an empty response. Treat that as "default" so
    // misuse degrades to the legacy place-only behavior rather than
    // silently returning {}.
    if !(set.places || set.collections || set.captures || set.routes) {
        set.places = true;
    }
    set
}

/// Parse the place-viewport filter query params into a `PlaceFilters`.
///
/// - `activity` is comma-separated; unknown tokens are ignored.
/// - `min_rating` is on the user-facing 0–5 scale and gets doubled to
///   match the 0–10 storage scale before reaching Cypher.
/// - `include_unengaged` opts out of the "any-Mapky-engagement"
///   default — when on AND no explicit activities are given, the
///   query returns every Place node in the bbox (BTCMap-flooded
///   merchants included).
fn parse_place_filters(
    activity: Option<&str>,
    min_rating: Option<f64>,
    include_unengaged: bool,
) -> queries::get::PlaceFilters {
    let activities = activity
        .map(|raw| {
            raw.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .filter_map(queries::get::PlaceActivity::parse)
                .collect()
        })
        .unwrap_or_default();
    let min_rating = min_rating
        // Out-of-range values are clamped silently; the alternative is
        // a 400 that's unhelpful for an exploratory filter UI.
        .filter(|r| r.is_finite() && *r > 0.0)
        .map(|r| (r * 2.0).clamp(0.0, 10.0));
    queries::get::PlaceFilters {
        activities,
        include_unengaged,
        min_rating,
    }
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct PostsQuery {
    #[serde(default)]
    pub skip: i64,
    #[serde(default = "default_limit")]
    pub limit: i64,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct PaginationQuery {
    #[serde(default)]
    pub skip: i64,
    #[serde(default = "default_limit")]
    pub limit: i64,
}

fn default_limit() -> i64 {
    100
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct NearbyQuery {
    pub lat: f64,
    pub lon: f64,
    #[serde(default = "default_nearby_radius")]
    pub radius: f64,
    pub exclude_sequence: Option<String>,
    #[serde(default = "default_nearby_limit")]
    pub limit: i64,
}

fn default_nearby_radius() -> f64 {
    80.0
}

fn default_nearby_limit() -> i64 {
    8
}

fn default_tag_search_limit() -> i64 {
    20
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct TagSearchQuery {
    pub q: String,
    #[serde(default = "default_tag_search_limit")]
    pub limit: i64,
}

/// Query for the cached OSM lookup. Comma-separated list of OSM IDs
/// in the standard `N123,W456,R789` form (Nominatim's input format).
/// Up to ~50 per call — beyond that the handler chunks internally.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct OsmLookupQuery {
    pub osm_ids: String,
}

/// Query for the cached `/osm/search` proxy. Subset of Nominatim's
/// `/search` params — only the knobs the frontend uses.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct OsmSearchQuery {
    pub q: String,
    /// `west,north,east,south`. Optional viewport bias / restriction.
    #[serde(default)]
    pub viewbox: Option<String>,
    /// Combined with `viewbox`, restricts results to the box.
    #[serde(default)]
    pub bounded: bool,
    #[serde(default = "default_search_limit")]
    pub limit: u32,
    #[serde(default = "default_true")]
    pub dedupe: bool,
    #[serde(default)]
    pub addressdetails: bool,
}

fn default_search_limit() -> u32 {
    8
}

fn default_true() -> bool {
    true
}

/// Query for the cached `/osm/reverse` proxy. `zoom` defaults to
/// Nominatim's standard 18 (street-level).
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct OsmReverseQuery {
    pub lat: f64,
    pub lon: f64,
    #[serde(default = "default_reverse_zoom")]
    pub zoom: u32,
}

fn default_reverse_zoom() -> u32 {
    18
}

/// One row from the cluster aggregation. Carries enough sub-counts
/// for the frontend to draw a per-cluster ratio bar (BTC / reviewed /
/// tagged) without a second query.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct PlaceCluster {
    pub lat: f64,
    pub lon: f64,
    pub total: i64,
    pub btc: i64,
    pub reviewed: i64,
    pub tagged: i64,
}

/// Discriminated envelope for the place-viewport endpoint. The frontend
/// switches between cluster bubbles (low zoom) and individual balloons
/// (high zoom) based on `kind`. One shape, one query key, no client-
/// side guesswork about which mode the server picked.
#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum ViewportResponse {
    /// `zoom < cluster_zoom_threshold` — a grid-aggregated overview.
    Clusters {
        clusters: Vec<PlaceCluster>,
        /// Cell size (degrees) used for binning, echoed back so the
        /// client can render bin-aware tooltips ("places in this 1°×1° area").
        cell: f64,
    },
    /// `zoom >= cluster_zoom_threshold` — individual place markers.
    Places { places: Vec<PlaceDetails> },
}

/// MapLibre zoom at or above which we switch from cluster bubbles to
/// individual place balloons. Below this, viewports are wide enough
/// that hundreds of POIs would clutter the map; above, a metro/city
/// fits in the viewport and the user wants per-place detail.
///
/// 11 is roughly "showing the whole city" — at z=11 a viewport spans
/// ~20km, comfortably small enough to render every BTC POI as an
/// individual balloon while still letting the user see the layout.
/// Below z=11 we cluster to keep continent/country views readable.
const CLUSTER_ZOOM_THRESHOLD: u8 = 11;

/// Cluster cell size (degrees of lat/lon) for the given MapLibre
/// zoom. Continuous formula (`45 / 2^zoom`) keyed to viewport width:
/// at any zoom, ~8 cells fit across the visible viewport, so the
/// frontend's ClusterBubble grid stays evenly spaced regardless of
/// how zoomed-out the user is.
///
/// Spot values:
///   z=0  → 45°    (world view: ~6-8 populated cells globally)
///   z=4  → 2.8°
///   z=8  → 0.18°
///   z=10 → 0.044°
fn cluster_cell_for_zoom(zoom: u8) -> f64 {
    // Cap the shift so well-behaved math even at unrealistic zooms.
    let z = zoom.min(20);
    45.0 / ((1u32 << z) as f64)
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct TagSearchResponse {
    pub places: Vec<PlaceDetails>,
    pub collections: Vec<CollectionDetails>,
    pub reviews: Vec<ReviewDetails>,
    pub posts: Vec<MapkyPostDetails>,
    pub routes: Vec<RouteDetails>,
    pub geo_captures: Vec<GeoCaptureDetails>,
    pub sequences: Vec<SequenceDetails>,
    pub incidents: Vec<IncidentDetails>,
}

/// Strip the `author_id:` prefix from a compound Neo4j post id, returning just the short post_id.
fn short_post_id(compound: &str) -> String {
    compound
        .split_once(':')
        .map(|(_, post_id)| post_id.to_string())
        .unwrap_or_else(|| compound.to_string())
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
struct ApiError {
    error: String,
}

type ApiResult<T> = Result<Json<T>, (StatusCode, Json<ApiError>)>;

fn graph_err(e: impl ToString) -> (StatusCode, Json<ApiError>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ApiError {
            error: e.to_string(),
        }),
    )
}

/// Read a `:Place` row produced by any of the Place-returning queries
/// (viewport, get-by-id, search). All such queries share the same
/// projection — defined in `queries::get` and kept in sync with the
/// `PlaceDetails` struct shape.
fn place_details_from_row(row: &neo4rs::Row) -> PlaceDetails {
    PlaceDetails {
        osm_canonical: row.get("osm_canonical").unwrap_or_default(),
        osm_type: row.get("osm_type").unwrap_or_default(),
        osm_id: row.get("osm_id").unwrap_or(0),
        lat: row.get("lat").unwrap_or(0.0),
        lon: row.get("lon").unwrap_or(0.0),
        geocoded: row.get("geocoded").unwrap_or(false),
        review_count: row.get("review_count").unwrap_or(0),
        avg_rating: row.get("avg_rating").unwrap_or(0.0),
        tag_count: row.get("tag_count").unwrap_or(0),
        photo_count: row.get("photo_count").unwrap_or(0),
        indexed_at: row.get("indexed_at").unwrap_or(0),
        name: row.get("name").ok(),
        accepts_bitcoin: row.get("accepts_bitcoin").unwrap_or(false),
        btc_onchain: row.get("btc_onchain").unwrap_or(false),
        btc_lightning: row.get("btc_lightning").unwrap_or(false),
        btc_lightning_contactless: row.get("btc_lightning_contactless").unwrap_or(false),
    }
}

/// Execute a `get_tags_for_*` query and aggregate rows into `(found, Vec<PostTagDetails>)`.
/// The underlying queries use `OPTIONAL MATCH`, so a target with zero tags emits one
/// row with NULL label/tagger (found=true, empty vec); a nonexistent target emits
/// zero rows (found=false). Callers that want 404 semantics check `found`; callers
/// embedding tags in a detail response can ignore it.
async fn fetch_tags(
    query: nexus_common::db::graph::Query,
) -> Result<(bool, Vec<PostTagDetails>), (StatusCode, Json<ApiError>)> {
    use std::collections::HashMap;

    let graph = get_neo4j_graph().map_err(graph_err)?;
    let mut stream = graph.execute(query).await.map_err(graph_err)?;

    let mut found = false;
    let mut tag_map: HashMap<String, Vec<String>> = HashMap::new();
    while let Some(row) = stream.try_next().await.map_err(graph_err)? {
        found = true;
        let label: Option<String> = row.get("label").ok();
        let tagger_id: Option<String> = row.get("tagger_id").ok();
        if let (Some(l), Some(t)) = (label, tagger_id) {
            tag_map.entry(l).or_default().push(t);
        }
    }

    let tags: Vec<PostTagDetails> = tag_map
        .into_iter()
        .map(|(label, taggers)| {
            let taggers_count = taggers.len();
            PostTagDetails {
                label,
                taggers,
                taggers_count,
            }
        })
        .collect();

    Ok((found, tags))
}

// ── Viewport sub-query helpers ──────────────────────────────────────────────
//
// Each helper returns the success payload (or an `ApiError` tuple) for one of
// the four map-viewport layers. They are called both by the single-purpose
// handlers (`viewport`, `collections_viewport`, `geo_captures_viewport`,
// `routes_viewport`) and by the composite `viewport_multi` handler, which
// runs them in parallel via `tokio::try_join!`.

async fn fetch_places_in_viewport(
    min_lat: f64,
    min_lon: f64,
    max_lat: f64,
    max_lon: f64,
    zoom: u8,
    limit: i64,
    filters: &queries::get::PlaceFilters,
) -> Result<ViewportResponse, (StatusCode, Json<ApiError>)> {
    let graph = get_neo4j_graph().map_err(graph_err)?;

    if zoom >= CLUSTER_ZOOM_THRESHOLD {
        let mut stream = graph
            .execute(queries::get::get_places_in_viewport(
                min_lat, min_lon, max_lat, max_lon, filters, limit,
            ))
            .await
            .map_err(graph_err)?;
        let mut places = Vec::new();
        while let Some(row) = stream.try_next().await.map_err(graph_err)? {
            places.push(place_details_from_row(&row));
        }
        return Ok(ViewportResponse::Places { places });
    }

    let cell = cluster_cell_for_zoom(zoom);
    let mut stream = graph
        .execute(queries::get::get_place_clusters_in_viewport(
            min_lat, min_lon, max_lat, max_lon, cell, filters, limit,
        ))
        .await
        .map_err(graph_err)?;
    let mut clusters = Vec::new();
    while let Some(row) = stream.try_next().await.map_err(graph_err)? {
        clusters.push(PlaceCluster {
            lat: row.get("lat").unwrap_or(0.0),
            lon: row.get("lon").unwrap_or(0.0),
            total: row.get("total").unwrap_or(0),
            btc: row.get("btc").unwrap_or(0),
            reviewed: row.get("reviewed").unwrap_or(0),
            tagged: row.get("tagged").unwrap_or(0),
        });
    }
    Ok(ViewportResponse::Clusters { clusters, cell })
}

async fn fetch_collections_in_viewport(
    min_lat: f64,
    min_lon: f64,
    max_lat: f64,
    max_lon: f64,
    limit: i64,
) -> Result<Vec<CollectionDetails>, (StatusCode, Json<ApiError>)> {
    let graph = get_neo4j_graph().map_err(graph_err)?;
    let mut stream = graph
        .execute(queries::get::get_collections_in_viewport(
            min_lat, min_lon, max_lat, max_lon, limit,
        ))
        .await
        .map_err(graph_err)?;
    let mut collections = Vec::new();
    while let Some(row) = stream.try_next().await.map_err(graph_err)? {
        collections.push(CollectionDetails {
            id: row.get("id").unwrap_or_default(),
            author_id: row.get("author_id").unwrap_or_default(),
            name: row.get("name").unwrap_or_default(),
            description: row.get("description").ok(),
            items: row.get::<Vec<String>>("items").unwrap_or_default(),
            image_uri: row.get("image_uri").ok(),
            color: row.get("color").ok(),
            indexed_at: row.get("indexed_at").unwrap_or(0),
        });
    }
    Ok(collections)
}

async fn fetch_geo_captures_in_viewport(
    min_lat: f64,
    min_lon: f64,
    max_lat: f64,
    max_lon: f64,
    limit: i64,
) -> Result<Vec<GeoCaptureDetails>, (StatusCode, Json<ApiError>)> {
    let graph = get_neo4j_graph().map_err(graph_err)?;
    let mut stream = graph
        .execute(queries::get::get_geo_captures_in_viewport(
            min_lat, min_lon, max_lat, max_lon, limit,
        ))
        .await
        .map_err(graph_err)?;
    let mut captures = Vec::new();
    while let Some(row) = stream.try_next().await.map_err(graph_err)? {
        captures.push(geo_capture_from_row(&row));
    }
    Ok(captures)
}

async fn fetch_routes_in_viewport(
    min_lat: f64,
    min_lon: f64,
    max_lat: f64,
    max_lon: f64,
    limit: i64,
) -> Result<Vec<RouteDetails>, (StatusCode, Json<ApiError>)> {
    let graph = get_neo4j_graph().map_err(graph_err)?;
    let mut stream = graph
        .execute(queries::get::get_routes_in_viewport(
            min_lat, min_lon, max_lat, max_lon, limit,
        ))
        .await
        .map_err(graph_err)?;
    let mut routes = Vec::new();
    while let Some(row) = stream.try_next().await.map_err(graph_err)? {
        routes.push(route_from_row(&row));
    }
    Ok(routes)
}

// ── Place sub-query helpers ─────────────────────────────────────────────────
//
// Used by the per-slice handlers (`place_detail`, `place_reviews`,
// `place_posts`, `place_tags`, `collections_for_place`, `place_routes`) and
// by the composite `place_detail_full` handler, which runs them in parallel
// after resolving the place's lat/lon.

async fn fetch_place_detail_by_canonical(
    osm_canonical: &str,
) -> Result<Option<PlaceDetails>, (StatusCode, Json<ApiError>)> {
    let graph = get_neo4j_graph().map_err(graph_err)?;
    let mut stream = graph
        .execute(queries::get::get_place_by_canonical(osm_canonical))
        .await
        .map_err(graph_err)?;
    Ok(stream
        .try_next()
        .await
        .map_err(graph_err)?
        .map(|row| place_details_from_row(&row)))
}

async fn fetch_reviews_for_place(
    osm_canonical: &str,
    skip: i64,
    limit: i64,
) -> Result<Vec<ReviewDetails>, (StatusCode, Json<ApiError>)> {
    let graph = get_neo4j_graph().map_err(graph_err)?;
    let mut stream = graph
        .execute(queries::get::get_reviews_for_place(
            osm_canonical,
            skip,
            limit,
        ))
        .await
        .map_err(graph_err)?;
    let mut reviews = Vec::new();
    while let Some(row) = stream.try_next().await.map_err(graph_err)? {
        reviews.push(review_from_row(&row));
    }
    Ok(reviews)
}

async fn fetch_mapky_posts_for_place(
    osm_canonical: &str,
    skip: i64,
    limit: i64,
) -> Result<Vec<MapkyPostDetails>, (StatusCode, Json<ApiError>)> {
    let graph = get_neo4j_graph().map_err(graph_err)?;
    let mut stream = graph
        .execute(queries::get::get_mapky_posts_for_place(
            osm_canonical,
            skip,
            limit,
        ))
        .await
        .map_err(graph_err)?;
    let mut posts = Vec::new();
    while let Some(row) = stream.try_next().await.map_err(graph_err)? {
        posts.push(mapky_post_from_row(&row));
    }
    Ok(posts)
}

/// Fetch tags for a place. Returns `(found, tags)` so callers can
/// distinguish "place doesn't exist" (`found=false`) from "place exists
/// but has no tags" (`found=true, tags=[]`). The single-purpose handler
/// turns `found=false` into a 404; the composite handler ignores it
/// because we've already validated the place via `fetch_place_detail`.
async fn fetch_tags_for_place(
    osm_canonical: &str,
) -> Result<(bool, Vec<PostTagDetails>), (StatusCode, Json<ApiError>)> {
    use std::collections::HashMap;

    let graph = get_neo4j_graph().map_err(graph_err)?;
    let mut stream = graph
        .execute(queries::get::get_tags_for_place(osm_canonical))
        .await
        .map_err(graph_err)?;

    let mut found = false;
    let mut tag_map: HashMap<String, Vec<String>> = HashMap::new();
    while let Some(row) = stream.try_next().await.map_err(graph_err)? {
        found = true;
        let label: Option<String> = row.get("label").ok();
        let tagger_id: Option<String> = row.get("tagger_id").ok();
        if let (Some(l), Some(t)) = (label, tagger_id) {
            tag_map.entry(l).or_default().push(t);
        }
    }

    let tags: Vec<PostTagDetails> = tag_map
        .into_iter()
        .map(|(label, taggers)| {
            let taggers_count = taggers.len();
            PostTagDetails {
                label,
                taggers,
                taggers_count,
            }
        })
        .collect();

    Ok((found, tags))
}

async fn fetch_collections_for_place(
    osm_canonical: &str,
) -> Result<Vec<CollectionDetails>, (StatusCode, Json<ApiError>)> {
    let graph = get_neo4j_graph().map_err(graph_err)?;
    let mut stream = graph
        .execute(queries::get::get_collections_containing_place(
            osm_canonical,
        ))
        .await
        .map_err(graph_err)?;
    let mut collections = Vec::new();
    while let Some(row) = stream.try_next().await.map_err(graph_err)? {
        collections.push(CollectionDetails {
            id: row.get("id").unwrap_or_default(),
            author_id: row.get("author_id").unwrap_or_default(),
            name: row.get("name").unwrap_or_default(),
            description: row.get("description").ok(),
            items: row.get::<Vec<String>>("items").unwrap_or_default(),
            image_uri: row.get("image_uri").ok(),
            color: row.get("color").ok(),
            indexed_at: row.get("indexed_at").unwrap_or(0),
        });
    }
    Ok(collections)
}

async fn fetch_routes_near_point(
    lat: f64,
    lon: f64,
    limit: i64,
) -> Result<Vec<RouteDetails>, (StatusCode, Json<ApiError>)> {
    let graph = get_neo4j_graph().map_err(graph_err)?;
    let mut stream = graph
        .execute(queries::get::get_routes_near_point(lat, lon, limit))
        .await
        .map_err(graph_err)?;
    let mut routes = Vec::new();
    while let Some(row) = stream.try_next().await.map_err(graph_err)? {
        routes.push(route_from_row(&row));
    }
    Ok(routes)
}

// ── Handlers ────────────────────────────────────────────────────────────────

/// Places within a geographic bounding box.
///
/// Returns a discriminated envelope: cluster bubbles when the zoom is
/// wide enough that individual balloons would clutter the map, and
/// individual `PlaceDetails` once the user is zoomed in. The frontend
/// switches rendering modes off the `kind` discriminator, so the
/// transition between zoom levels is seamless.
///
/// Optional `bitcoin` / `reviewed` / `tagged` flags narrow the result
/// in both modes — same Cypher predicates either way.
#[utoipa::path(
    get,
    path = "/v0/mapky/viewport",
    tag = "Place",
    params(
        ("min_lat" = f64, Query, description = "Minimum latitude"),
        ("min_lon" = f64, Query, description = "Minimum longitude"),
        ("max_lat" = f64, Query, description = "Maximum latitude"),
        ("max_lon" = f64, Query, description = "Maximum longitude"),
        ("zoom" = Option<u8>, Query, description = "Current MapLibre zoom (0-22). Switches to cluster mode below 13."),
        ("limit" = Option<i64>, Query, description = "Max rows (clusters or places); default 100"),
        ("activity" = Option<String>, Query, description = "Comma-separated activity OR set: tagged,reviewed,posted,collected"),
        ("min_rating" = Option<f64>, Query, description = "Minimum average rating (0.0–5.0)"),
        ("include_unengaged" = Option<bool>, Query, description = "Opt out of the 'any-Mapky-engagement' default (off by default)"),
    ),
    responses(
        (status = 200, description = "Cluster summary or individual places", body = ViewportResponse),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
async fn viewport(
    State(_ctx): State<PluginContext>,
    Query(params): Query<PlaceViewportQuery>,
) -> ApiResult<ViewportResponse> {
    let filters = parse_place_filters(
        params.activity.as_deref(),
        params.min_rating,
        params.include_unengaged,
    );
    let resp = fetch_places_in_viewport(
        params.min_lat,
        params.min_lon,
        params.max_lat,
        params.max_lon,
        params.zoom,
        params.limit,
        &filters,
    )
    .await?;
    Ok(Json(resp))
}

/// Composite map-viewport: one request returns up to four layers.
///
/// `include` selects which layers to compute (`places,collections,captures,routes`).
/// Sub-queries run in parallel via `tokio::try_join!` so latency stays at
/// max(t_layers) rather than sum(t_layers). Layers not selected are omitted
/// from the response (`skip_serializing_if = "Option::is_none"`).
///
/// Replaces the four-fetch fan-out the frontend used to do per pan
/// (`useViewportPlaces` + `useViewportCollections` + `useViewportCaptures`
/// + `useViewportRoutes`). The single-purpose endpoints stay mounted for
/// backwards compat during rollout.
#[utoipa::path(
    get,
    path = "/v0/mapky/viewport/all",
    tag = "Place",
    params(
        ("min_lat" = f64, Query, description = "Minimum latitude"),
        ("min_lon" = f64, Query, description = "Minimum longitude"),
        ("max_lat" = f64, Query, description = "Maximum latitude"),
        ("max_lon" = f64, Query, description = "Maximum longitude"),
        ("zoom" = Option<u8>, Query, description = "Zoom for the place layer's cluster decision (defaults to 13)"),
        ("limit" = Option<i64>, Query, description = "Max rows per layer; default 100"),
        ("include" = Option<String>, Query, description = "Comma-separated layer names: places,collections,captures,routes. Default: places."),
        ("activity" = Option<String>, Query, description = "Place filter: comma-separated activity OR set (tagged,reviewed,posted,collected)"),
        ("min_rating" = Option<f64>, Query, description = "Place filter: minimum average rating (0.0–5.0)"),
        ("include_unengaged" = Option<bool>, Query, description = "Place filter: opt out of the 'any-Mapky-engagement' default"),
    ),
    responses(
        (status = 200, description = "Composite envelope with one branch per requested layer", body = MultiViewportResponse),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
async fn viewport_multi(
    State(_ctx): State<PluginContext>,
    Query(params): Query<MultiViewportQuery>,
) -> ApiResult<MultiViewportResponse> {
    let inc = parse_include(params.include.as_deref());
    let filters = parse_place_filters(
        params.activity.as_deref(),
        params.min_rating,
        params.include_unengaged,
    );

    // Build a future per layer. When the layer is not requested, the
    // future short-circuits to `Ok(None)` — skipped by `try_join!` at
    // ~zero cost, no Neo4j round-trip.
    let places_fut = async {
        if inc.places {
            fetch_places_in_viewport(
                params.min_lat,
                params.min_lon,
                params.max_lat,
                params.max_lon,
                params.zoom,
                params.limit,
                &filters,
            )
            .await
            .map(Some)
        } else {
            Ok(None)
        }
    };
    let collections_fut = async {
        if inc.collections {
            fetch_collections_in_viewport(
                params.min_lat,
                params.min_lon,
                params.max_lat,
                params.max_lon,
                params.limit,
            )
            .await
            .map(Some)
        } else {
            Ok(None)
        }
    };
    let captures_fut = async {
        if inc.captures {
            fetch_geo_captures_in_viewport(
                params.min_lat,
                params.min_lon,
                params.max_lat,
                params.max_lon,
                params.limit,
            )
            .await
            .map(Some)
        } else {
            Ok(None)
        }
    };
    let routes_fut = async {
        if inc.routes {
            fetch_routes_in_viewport(
                params.min_lat,
                params.min_lon,
                params.max_lat,
                params.max_lon,
                params.limit,
            )
            .await
            .map(Some)
        } else {
            Ok(None)
        }
    };

    let (places, collections, captures, routes) =
        tokio::try_join!(places_fut, collections_fut, captures_fut, routes_fut)?;

    Ok(Json(MultiViewportResponse {
        places,
        collections,
        captures,
        routes,
    }))
}

/// Get a single place by OSM type and ID
#[utoipa::path(
    get,
    path = "/v0/mapky/place/{osm_type}/{osm_id}",
    tag = "Place",
    params(
        ("osm_type" = String, Path, description = "OSM element type: node, way, or relation"),
        ("osm_id" = i64, Path, description = "OSM element ID")
    ),
    responses(
        (status = 200, description = "Place details", body = PlaceDetails),
        (status = 404, description = "Place not found", body = ApiError),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
async fn place_detail(
    State(_ctx): State<PluginContext>,
    Path((osm_type, osm_id)): Path<(String, i64)>,
) -> ApiResult<PlaceDetails> {
    let osm_canonical = format!("{osm_type}/{osm_id}");
    match fetch_place_detail_by_canonical(&osm_canonical).await? {
        Some(detail) => Ok(Json(detail)),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ApiError {
                error: format!("Place {osm_canonical} not found"),
            }),
        )),
    }
}

/// Composite place-detail: detail + reviews + posts + tags + collections + routes
/// in one request.
///
/// Replaces the six-fetch fan-out PlacePanel used to do on every place open.
/// Detail is fetched first (we need lat/lon for the routes-near-point query
/// and a 404 short-circuits the rest); the remaining five sub-queries then
/// run in parallel via `tokio::try_join!`. Wall-clock cost is
/// `t_detail + max(t_others)` instead of `sum(t_all)`.
#[utoipa::path(
    get,
    path = "/v0/mapky/place/{osm_type}/{osm_id}/full",
    tag = "Place",
    params(
        ("osm_type" = String, Path, description = "OSM element type: node, way, or relation"),
        ("osm_id" = i64, Path, description = "OSM element ID"),
        ("reviews_limit" = Option<i64>, Query, description = "Max reviews (default 100)"),
        ("posts_limit" = Option<i64>, Query, description = "Max posts (default 100)"),
        ("routes_limit" = Option<i64>, Query, description = "Max routes near the place (default 50)"),
    ),
    responses(
        (status = 200, description = "Place with all related slices", body = PlaceFullResponse),
        (status = 404, description = "Place not found", body = ApiError),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
async fn place_detail_full(
    State(_ctx): State<PluginContext>,
    Path((osm_type, osm_id)): Path<(String, i64)>,
    Query(params): Query<PlaceFullQuery>,
) -> ApiResult<PlaceFullResponse> {
    let osm_canonical = format!("{osm_type}/{osm_id}");

    let detail = fetch_place_detail_by_canonical(&osm_canonical)
        .await?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ApiError {
                    error: format!("Place {osm_canonical} not found"),
                }),
            )
        })?;

    // Fan out the five remaining slices in parallel. We discard the
    // `found` flag from `fetch_tags_for_place` because we already
    // proved the place exists by reading `detail`; tags-empty is just
    // an empty Vec, not a 404.
    let (reviews, posts, tags, collections, routes) = tokio::try_join!(
        fetch_reviews_for_place(&osm_canonical, 0, params.reviews_limit),
        fetch_mapky_posts_for_place(&osm_canonical, 0, params.posts_limit),
        async {
            fetch_tags_for_place(&osm_canonical)
                .await
                .map(|(_, tags)| tags)
        },
        fetch_collections_for_place(&osm_canonical),
        fetch_routes_near_point(detail.lat, detail.lon, params.routes_limit),
    )?;

    Ok(Json(PlaceFullResponse {
        detail,
        reviews,
        posts,
        tags,
        collections,
        routes,
    }))
}

/// Read a `:MapkyAppPost` (cross-namespace comment) row produced by any of the
/// post-returning queries. All such queries share the same projection — kept
/// in sync with the `MapkyPostDetails` struct shape.
fn mapky_post_from_row(row: &neo4rs::Row) -> MapkyPostDetails {
    let compound_id: String = row.get("id").unwrap_or_default();
    MapkyPostDetails {
        id: short_post_id(&compound_id),
        author_id: row.get("author_id").unwrap_or_default(),
        content: row.get("content").unwrap_or_default(),
        kind: row.get("kind").unwrap_or_else(|_| "short".to_string()),
        parent_uri: row.get("parent_uri").ok(),
        attachments: row.get::<Vec<String>>("attachments").unwrap_or_default(),
        embed_uri: row.get("embed_uri").ok(),
        embed_kind: row.get("embed_kind").ok(),
        indexed_at: row.get("indexed_at").unwrap_or(0),
    }
}

/// Read a `:MapkyAppReview` row produced by any of the review-returning
/// queries.
fn review_from_row(row: &neo4rs::Row) -> ReviewDetails {
    let compound_id: String = row.get("id").unwrap_or_default();
    let rating_raw: i64 = row.get("rating").unwrap_or(0);
    ReviewDetails {
        id: short_post_id(&compound_id),
        author_id: row.get("author_id").unwrap_or_default(),
        osm_canonical: row.get("osm_canonical").unwrap_or_default(),
        content: row.get("content").ok(),
        rating: rating_raw.clamp(0, u8::MAX as i64) as u8,
        attachments: row.get::<Vec<String>>("attachments").unwrap_or_default(),
        indexed_at: row.get("indexed_at").unwrap_or(0),
    }
}

/// Aggregate `(label, tagger_id)` rows into deduplicated `PostTagDetails`.
async fn collect_resource_tags(
    node_label: &str,
    compound_id: &str,
) -> Result<Vec<PostTagDetails>, (StatusCode, Json<ApiError>)> {
    use std::collections::HashMap;

    let graph = get_neo4j_graph().map_err(graph_err)?;
    let mut stream = graph
        .execute(queries::get::get_tags_for_mapky_resource(
            node_label,
            compound_id,
        ))
        .await
        .map_err(graph_err)?;

    let mut found = false;
    let mut tag_map: HashMap<String, Vec<String>> = HashMap::new();
    while let Some(row) = stream.try_next().await.map_err(graph_err)? {
        found = true;
        let label: Option<String> = row.get("label").ok();
        let tagger_id: Option<String> = row.get("tagger_id").ok();
        if let (Some(l), Some(t)) = (label, tagger_id) {
            tag_map.entry(l).or_default().push(t);
        }
    }

    if !found {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ApiError {
                error: format!("{node_label} {compound_id} not found"),
            }),
        ));
    }

    Ok(tag_map
        .into_iter()
        .map(|(label, taggers)| {
            let taggers_count = taggers.len();
            PostTagDetails {
                label,
                taggers,
                taggers_count,
            }
        })
        .collect())
}

/// Tags on a cross-namespace MapkyAppPost (`/pub/mapky.app/posts/{id}`)
#[utoipa::path(
    get,
    path = "/v0/mapky/posts/{author_id}/{post_id}/tags",
    tag = "Post",
    params(
        ("author_id" = String, Path, description = "Author's pubky ID"),
        ("post_id" = String, Path, description = "MapkyAppPost ID"),
    ),
    responses(
        (status = 200, description = "Tags for a MapkyAppPost", body = Vec<PostTagDetails>),
        (status = 404, description = "Post not found"),
        (status = 500, description = "Internal server error", body = ApiError),
    )
)]
async fn post_tags(
    State(_ctx): State<PluginContext>,
    Path((author_id, post_id)): Path<(String, String)>,
) -> ApiResult<Vec<PostTagDetails>> {
    let compound_id = format!("{author_id}:{post_id}");
    let tags = collect_resource_tags("MapkyAppPost", &compound_id).await?;
    Ok(Json(tags))
}

/// Tags on a `:MapkyAppReview`
#[utoipa::path(
    get,
    path = "/v0/mapky/reviews/{author_id}/{review_id}/tags",
    tag = "Review",
    params(
        ("author_id" = String, Path, description = "Author's pubky ID"),
        ("review_id" = String, Path, description = "MapkyAppReview ID"),
    ),
    responses(
        (status = 200, description = "Tags for a MapkyAppReview", body = Vec<PostTagDetails>),
        (status = 404, description = "Review not found"),
        (status = 500, description = "Internal server error", body = ApiError),
    )
)]
async fn review_tags(
    State(_ctx): State<PluginContext>,
    Path((author_id, review_id)): Path<(String, String)>,
) -> ApiResult<Vec<PostTagDetails>> {
    let compound_id = format!("{author_id}:{review_id}");
    let tags = collect_resource_tags("MapkyAppReview", &compound_id).await?;
    Ok(Json(tags))
}

/// List a user's cross-namespace posts (`MapkyAppPost`).
#[utoipa::path(
    get,
    path = "/v0/mapky/posts/user/{user_id}",
    tag = "Post",
    params(
        ("user_id" = String, Path, description = "User's pubky ID"),
        ("skip" = Option<i64>, Query, description = "Pagination offset (default 0)"),
        ("limit" = Option<i64>, Query, description = "Max results (default 100)"),
    ),
    responses(
        (status = 200, description = "User's posts", body = Vec<MapkyPostDetails>),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
async fn user_posts(
    State(_ctx): State<PluginContext>,
    Path(user_id): Path<String>,
    Query(params): Query<PaginationQuery>,
) -> ApiResult<Vec<MapkyPostDetails>> {
    let graph = get_neo4j_graph().map_err(graph_err)?;
    let mut stream = graph
        .execute(queries::get::get_user_mapky_posts(
            &user_id,
            params.skip,
            params.limit,
        ))
        .await
        .map_err(graph_err)?;

    let mut posts = Vec::new();
    while let Some(row) = stream.try_next().await.map_err(graph_err)? {
        posts.push(mapky_post_from_row(&row));
    }
    Ok(Json(posts))
}

/// List a user's reviews
#[utoipa::path(
    get,
    path = "/v0/mapky/reviews/user/{user_id}",
    tag = "Review",
    params(
        ("user_id" = String, Path, description = "User's pubky ID"),
        ("skip" = Option<i64>, Query, description = "Pagination offset (default 0)"),
        ("limit" = Option<i64>, Query, description = "Max results (default 100)"),
    ),
    responses(
        (status = 200, description = "User's reviews", body = Vec<ReviewDetails>),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
async fn user_reviews(
    State(_ctx): State<PluginContext>,
    Path(user_id): Path<String>,
    Query(params): Query<PaginationQuery>,
) -> ApiResult<Vec<ReviewDetails>> {
    let graph = get_neo4j_graph().map_err(graph_err)?;
    let mut stream = graph
        .execute(queries::get::get_user_reviews(
            &user_id,
            params.skip,
            params.limit,
        ))
        .await
        .map_err(graph_err)?;

    let mut reviews = Vec::new();
    while let Some(row) = stream.try_next().await.map_err(graph_err)? {
        reviews.push(review_from_row(&row));
    }
    Ok(Json(reviews))
}

/// List `:MapkyAppPost` replies anchored to any MapKy resource. The
/// `resource_type` segment is mapped to a Neo4j label (reviews, routes,
/// collections, geo_captures, sequences, incidents, posts).
#[utoipa::path(
    get,
    path = "/v0/mapky/{resource_type}/{author_id}/{resource_id}/posts",
    tag = "Post",
    params(
        ("resource_type" = String, Path, description = "MapKy resource type: reviews, routes, collections, geo_captures, sequences, incidents, posts"),
        ("author_id" = String, Path, description = "Author of the resource being replied to"),
        ("resource_id" = String, Path, description = "ID of the resource being replied to"),
        ("skip" = Option<i64>, Query, description = "Pagination offset (default 0)"),
        ("limit" = Option<i64>, Query, description = "Max results (default 100)"),
    ),
    responses(
        (status = 200, description = "Replies to the resource", body = Vec<MapkyPostDetails>),
        (status = 400, description = "Unknown resource_type"),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
async fn resource_replies(
    State(_ctx): State<PluginContext>,
    Path((resource_type, author_id, resource_id)): Path<(String, String, String)>,
    Query(params): Query<PaginationQuery>,
) -> ApiResult<Vec<MapkyPostDetails>> {
    let label = mapky_resource_label(&resource_type).ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError {
                error: format!("Unknown resource_type: {resource_type}"),
            }),
        )
    })?;
    let compound_id = format!("{author_id}:{resource_id}");
    let graph = get_neo4j_graph().map_err(graph_err)?;
    let mut stream = graph
        .execute(queries::get::get_replies_for_resource(
            label,
            &compound_id,
            params.skip,
            params.limit,
        ))
        .await
        .map_err(graph_err)?;

    let mut replies = Vec::new();
    while let Some(row) = stream.try_next().await.map_err(graph_err)? {
        replies.push(mapky_post_from_row(&row));
    }
    Ok(Json(replies))
}

/// List reviews for a place
#[utoipa::path(
    get,
    path = "/v0/mapky/place/{osm_type}/{osm_id}/reviews",
    tag = "Place",
    params(
        ("osm_type" = String, Path, description = "OSM element type: node, way, or relation"),
        ("osm_id" = i64, Path, description = "OSM element ID"),
        ("skip" = Option<i64>, Query, description = "Pagination offset (default 0)"),
        ("limit" = Option<i64>, Query, description = "Max results (default 100)"),
    ),
    responses(
        (status = 200, description = "List of reviews for the place", body = Vec<ReviewDetails>),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
async fn place_reviews(
    State(_ctx): State<PluginContext>,
    Path((osm_type, osm_id)): Path<(String, i64)>,
    Query(params): Query<PostsQuery>,
) -> ApiResult<Vec<ReviewDetails>> {
    let osm_canonical = format!("{osm_type}/{osm_id}");
    let reviews = fetch_reviews_for_place(&osm_canonical, params.skip, params.limit).await?;
    Ok(Json(reviews))
}

/// List cross-namespace `:MapkyAppPost` (comments) anchored to a place — i.e.
/// posts whose `[:REPLY_TO]` points at a `:MapkyAppReview` for this place.
#[utoipa::path(
    get,
    path = "/v0/mapky/place/{osm_type}/{osm_id}/posts",
    tag = "Place",
    params(
        ("osm_type" = String, Path, description = "OSM element type: node, way, or relation"),
        ("osm_id" = i64, Path, description = "OSM element ID"),
        ("skip" = Option<i64>, Query, description = "Pagination offset (default 0)"),
        ("limit" = Option<i64>, Query, description = "Max results (default 100)"),
    ),
    responses(
        (status = 200, description = "List of posts for the place", body = Vec<MapkyPostDetails>),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
async fn place_posts(
    State(_ctx): State<PluginContext>,
    Path((osm_type, osm_id)): Path<(String, i64)>,
    Query(params): Query<PostsQuery>,
) -> ApiResult<Vec<MapkyPostDetails>> {
    let osm_canonical = format!("{osm_type}/{osm_id}");
    let posts = fetch_mapky_posts_for_place(&osm_canonical, params.skip, params.limit).await?;
    Ok(Json(posts))
}

// ── Place tags ──────────────────────────────────────────────────────────────

/// Get tags on a Place
#[utoipa::path(
    get,
    path = "/v0/mapky/place/{osm_type}/{osm_id}/tags",
    tag = "Place",
    params(
        ("osm_type" = String, Path, description = "OSM element type"),
        ("osm_id" = i64, Path, description = "OSM element ID"),
    ),
    responses(
        (status = 200, description = "Tags on a place", body = Vec<PostTagDetails>),
        (status = 404, description = "Place not found"),
        (status = 500, description = "Internal server error", body = ApiError),
    )
)]
async fn place_tags(
    State(_ctx): State<PluginContext>,
    Path((osm_type, osm_id)): Path<(String, i64)>,
) -> ApiResult<Vec<PostTagDetails>> {
    let osm_canonical = format!("{osm_type}/{osm_id}");
    let (found, tags) = fetch_tags_for_place(&osm_canonical).await?;
    if !found {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ApiError {
                error: format!("Place {osm_canonical} not found"),
            }),
        ));
    }
    Ok(Json(tags))
}

// ── Incidents ───────────────────────────────────────────────────────────────

/// List incidents within a geographic bounding box
#[utoipa::path(
    get,
    path = "/v0/mapky/incidents/viewport",
    tag = "Incident",
    params(
        ("min_lat" = f64, Query, description = "Minimum latitude"),
        ("min_lon" = f64, Query, description = "Minimum longitude"),
        ("max_lat" = f64, Query, description = "Maximum latitude"),
        ("max_lon" = f64, Query, description = "Maximum longitude"),
        ("limit" = Option<i64>, Query, description = "Max results (default 100)")
    ),
    responses(
        (status = 200, description = "Incidents in viewport", body = Vec<IncidentDetails>),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
async fn incidents_viewport(
    State(_ctx): State<PluginContext>,
    Query(params): Query<ViewportQuery>,
) -> ApiResult<Vec<IncidentDetails>> {
    let graph = get_neo4j_graph().map_err(graph_err)?;
    let mut stream = graph
        .execute(queries::get::get_incidents_in_viewport(
            params.min_lat,
            params.min_lon,
            params.max_lat,
            params.max_lon,
            params.limit,
        ))
        .await
        .map_err(graph_err)?;

    let mut incidents = Vec::new();
    while let Some(row) = stream.try_next().await.map_err(graph_err)? {
        incidents.push(IncidentDetails {
            id: row.get("id").unwrap_or_default(),
            author_id: row.get("author_id").unwrap_or_default(),
            incident_type: row.get("incident_type").unwrap_or_default(),
            severity: row.get("severity").unwrap_or_default(),
            lat: row.get("lat").unwrap_or(0.0),
            lon: row.get("lon").unwrap_or(0.0),
            heading: row.get("heading").ok(),
            description: row.get("description").ok(),
            attachments: row.get::<Vec<String>>("attachments").unwrap_or_default(),
            expires_at: row.get("expires_at").ok(),
            indexed_at: row.get("indexed_at").unwrap_or(0),
        });
    }

    Ok(Json(incidents))
}

/// Get a single incident by author and ID
#[utoipa::path(
    get,
    path = "/v0/mapky/incidents/{author_id}/{incident_id}",
    tag = "Incident",
    params(
        ("author_id" = String, Path, description = "Author's pubky ID"),
        ("incident_id" = String, Path, description = "Incident ID"),
    ),
    responses(
        (status = 200, description = "Incident details", body = IncidentDetails),
        (status = 404, description = "Incident not found"),
        (status = 500, description = "Internal server error", body = ApiError),
    )
)]
async fn incident_detail(
    State(_ctx): State<PluginContext>,
    Path((author_id, incident_id)): Path<(String, String)>,
) -> ApiResult<IncidentDetails> {
    let compound_id = format!("{author_id}:{incident_id}");
    let graph = get_neo4j_graph().map_err(graph_err)?;
    let mut stream = graph
        .execute(queries::get::get_incident_by_id(&compound_id))
        .await
        .map_err(graph_err)?;

    match stream.try_next().await.map_err(graph_err)? {
        Some(row) => Ok(Json(IncidentDetails {
            id: row.get("id").unwrap_or_default(),
            author_id: row.get("author_id").unwrap_or_default(),
            incident_type: row.get("incident_type").unwrap_or_default(),
            severity: row.get("severity").unwrap_or_default(),
            lat: row.get("lat").unwrap_or(0.0),
            lon: row.get("lon").unwrap_or(0.0),
            heading: row.get("heading").ok(),
            description: row.get("description").ok(),
            attachments: row.get::<Vec<String>>("attachments").unwrap_or_default(),
            expires_at: row.get("expires_at").ok(),
            indexed_at: row.get("indexed_at").unwrap_or(0),
        })),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ApiError {
                error: format!("Incident {compound_id} not found"),
            }),
        )),
    }
}

/// List a user's incidents
#[utoipa::path(
    get,
    path = "/v0/mapky/incidents/user/{user_id}",
    tag = "Incident",
    params(
        ("user_id" = String, Path, description = "User's pubky ID"),
        ("skip" = Option<i64>, Query, description = "Pagination offset (default 0)"),
        ("limit" = Option<i64>, Query, description = "Max results (default 100)"),
    ),
    responses(
        (status = 200, description = "User's incidents", body = Vec<IncidentDetails>),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
async fn user_incidents(
    State(_ctx): State<PluginContext>,
    Path(user_id): Path<String>,
    Query(params): Query<PaginationQuery>,
) -> ApiResult<Vec<IncidentDetails>> {
    let graph = get_neo4j_graph().map_err(graph_err)?;
    let mut stream = graph
        .execute(queries::get::get_user_incidents(
            &user_id,
            params.skip,
            params.limit,
        ))
        .await
        .map_err(graph_err)?;

    let mut incidents = Vec::new();
    while let Some(row) = stream.try_next().await.map_err(graph_err)? {
        incidents.push(IncidentDetails {
            id: row.get("id").unwrap_or_default(),
            author_id: row.get("author_id").unwrap_or_default(),
            incident_type: row.get("incident_type").unwrap_or_default(),
            severity: row.get("severity").unwrap_or_default(),
            lat: row.get("lat").unwrap_or(0.0),
            lon: row.get("lon").unwrap_or(0.0),
            heading: row.get("heading").ok(),
            description: row.get("description").ok(),
            attachments: row.get::<Vec<String>>("attachments").unwrap_or_default(),
            expires_at: row.get("expires_at").ok(),
            indexed_at: row.get("indexed_at").unwrap_or(0),
        });
    }

    Ok(Json(incidents))
}

// ── GeoCaptures ─────────────────────────────────────────────────────────────

/// List geo captures within a geographic bounding box
#[utoipa::path(
    get,
    path = "/v0/mapky/geo_captures/viewport",
    tag = "GeoCapture",
    params(
        ("min_lat" = f64, Query, description = "Minimum latitude"),
        ("min_lon" = f64, Query, description = "Minimum longitude"),
        ("max_lat" = f64, Query, description = "Maximum latitude"),
        ("max_lon" = f64, Query, description = "Maximum longitude"),
        ("limit" = Option<i64>, Query, description = "Max results (default 100)")
    ),
    responses(
        (status = 200, description = "Geo captures in viewport", body = Vec<GeoCaptureDetails>),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
async fn geo_captures_viewport(
    State(_ctx): State<PluginContext>,
    Query(params): Query<ViewportQuery>,
) -> ApiResult<Vec<GeoCaptureDetails>> {
    let captures = fetch_geo_captures_in_viewport(
        params.min_lat,
        params.min_lon,
        params.max_lat,
        params.max_lon,
        params.limit,
    )
    .await?;
    Ok(Json(captures))
}

/// Get a single geo capture by author and ID
#[utoipa::path(
    get,
    path = "/v0/mapky/geo_captures/{author_id}/{capture_id}",
    tag = "GeoCapture",
    params(
        ("author_id" = String, Path, description = "Author's pubky ID"),
        ("capture_id" = String, Path, description = "GeoCapture ID"),
    ),
    responses(
        (status = 200, description = "GeoCapture details", body = GeoCaptureDetails),
        (status = 404, description = "GeoCapture not found"),
        (status = 500, description = "Internal server error", body = ApiError),
    )
)]
async fn geo_capture_detail(
    State(_ctx): State<PluginContext>,
    Path((author_id, capture_id)): Path<(String, String)>,
) -> ApiResult<GeoCaptureDetails> {
    let compound_id = format!("{author_id}:{capture_id}");
    let graph = get_neo4j_graph().map_err(graph_err)?;
    let mut stream = graph
        .execute(queries::get::get_geo_capture_by_id(&compound_id))
        .await
        .map_err(graph_err)?;

    let mut capture = match stream.try_next().await.map_err(graph_err)? {
        Some(row) => GeoCaptureDetails {
            id: row.get("id").unwrap_or_default(),
            author_id: row.get("author_id").unwrap_or_default(),
            file_uri: row.get("file_uri").unwrap_or_default(),
            kind: row.get("kind").unwrap_or_default(),
            lat: row.get("lat").unwrap_or(0.0),
            lon: row.get("lon").unwrap_or(0.0),
            ele: row.get("ele").ok(),
            heading: row.get("heading").ok(),
            pitch: row.get("pitch").ok(),
            fov: row.get("fov").ok(),
            caption: row.get("caption").ok(),
            sequence_uri: row.get("sequence_uri").ok(),
            sequence_index: row.get("sequence_index").ok(),
            captured_at: row.get("captured_at").ok(),
            indexed_at: row.get("indexed_at").unwrap_or(0),
            tags: None,
        },
        None => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(ApiError {
                    error: format!("GeoCapture {compound_id} not found"),
                }),
            ))
        }
    };

    let (_found, tags) = fetch_tags(queries::get::get_tags_for_geo_capture(&compound_id)).await?;
    capture.tags = Some(tags);
    Ok(Json(capture))
}

/// List a user's geo captures
#[utoipa::path(
    get,
    path = "/v0/mapky/geo_captures/user/{user_id}",
    tag = "GeoCapture",
    params(
        ("user_id" = String, Path, description = "User's pubky ID"),
        ("skip" = Option<i64>, Query, description = "Pagination offset (default 0)"),
        ("limit" = Option<i64>, Query, description = "Max results (default 100)"),
    ),
    responses(
        (status = 200, description = "User's geo captures", body = Vec<GeoCaptureDetails>),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
async fn user_geo_captures(
    State(_ctx): State<PluginContext>,
    Path(user_id): Path<String>,
    Query(params): Query<PaginationQuery>,
) -> ApiResult<Vec<GeoCaptureDetails>> {
    let graph = get_neo4j_graph().map_err(graph_err)?;
    let mut stream = graph
        .execute(queries::get::get_user_geo_captures(
            &user_id,
            params.skip,
            params.limit,
        ))
        .await
        .map_err(graph_err)?;

    let mut captures = Vec::new();
    while let Some(row) = stream.try_next().await.map_err(graph_err)? {
        captures.push(geo_capture_from_row(&row));
    }

    Ok(Json(captures))
}

/// Find GeoCaptures near a GPS point (for cross-sequence street-view navigation)
#[utoipa::path(
    get,
    path = "/v0/mapky/geo_captures/nearby",
    tag = "GeoCapture",
    params(
        ("lat" = f64, Query, description = "Latitude"),
        ("lon" = f64, Query, description = "Longitude"),
        ("radius" = Option<f64>, Query, description = "Search radius in meters (default 80)"),
        ("exclude_sequence" = Option<String>, Query, description = "Exclude captures from this sequence URI"),
        ("limit" = Option<i64>, Query, description = "Max results (default 8)"),
    ),
    responses(
        (status = 200, description = "Nearby geo captures sorted by distance", body = Vec<GeoCaptureDetails>),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
async fn nearby_geo_captures(
    State(_ctx): State<PluginContext>,
    axum::extract::Query(params): axum::extract::Query<NearbyQuery>,
) -> ApiResult<Vec<GeoCaptureDetails>> {
    let graph = get_neo4j_graph().map_err(graph_err)?;
    let mut stream = graph
        .execute(queries::get::get_nearby_captures(
            params.lat,
            params.lon,
            params.radius,
            params.exclude_sequence.as_deref(),
            params.limit,
        ))
        .await
        .map_err(graph_err)?;

    let mut captures = Vec::new();
    while let Some(row) = stream.try_next().await.map_err(graph_err)? {
        captures.push(geo_capture_from_row(&row));
    }

    Ok(Json(captures))
}

/// Get tags for a MapkyAppGeoCapture
#[utoipa::path(
    get,
    path = "/v0/mapky/geo_captures/{author_id}/{capture_id}/tags",
    tag = "GeoCapture",
    params(
        ("author_id" = String, Path, description = "Author's pubky ID"),
        ("capture_id" = String, Path, description = "GeoCapture ID"),
    ),
    responses(
        (status = 200, description = "Tags on the GeoCapture", body = Vec<PostTagDetails>),
        (status = 404, description = "GeoCapture not found", body = ApiError),
        (status = 500, description = "Internal server error", body = ApiError),
    )
)]
async fn geo_capture_tags(
    State(_ctx): State<PluginContext>,
    Path((author_id, capture_id)): Path<(String, String)>,
) -> ApiResult<Vec<PostTagDetails>> {
    let compound_id = format!("{author_id}:{capture_id}");
    let (found, tags) = fetch_tags(queries::get::get_tags_for_geo_capture(&compound_id)).await?;
    if !found {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ApiError {
                error: format!("GeoCapture {compound_id} not found"),
            }),
        ));
    }
    Ok(Json(tags))
}

/// Helper to parse a Neo4j row into `GeoCaptureDetails` (no tags).
fn geo_capture_from_row(row: &neo4rs::Row) -> GeoCaptureDetails {
    GeoCaptureDetails {
        id: row.get("id").unwrap_or_default(),
        author_id: row.get("author_id").unwrap_or_default(),
        file_uri: row.get("file_uri").unwrap_or_default(),
        kind: row.get("kind").unwrap_or_default(),
        lat: row.get("lat").unwrap_or(0.0),
        lon: row.get("lon").unwrap_or(0.0),
        ele: row.get("ele").ok(),
        heading: row.get("heading").ok(),
        pitch: row.get("pitch").ok(),
        fov: row.get("fov").ok(),
        caption: row.get("caption").ok(),
        sequence_uri: row.get("sequence_uri").ok(),
        sequence_index: row.get("sequence_index").ok(),
        captured_at: row.get("captured_at").ok(),
        indexed_at: row.get("indexed_at").unwrap_or(0),
        tags: None,
    }
}

// ── Sequences ───────────────────────────────────────────────────────────────

/// Get a single sequence by author and ID (tags embedded)
#[utoipa::path(
    get,
    path = "/v0/mapky/sequences/{author_id}/{sequence_id}",
    tag = "Sequence",
    params(
        ("author_id" = String, Path, description = "Author's pubky ID"),
        ("sequence_id" = String, Path, description = "Sequence ID"),
    ),
    responses(
        (status = 200, description = "Sequence details", body = SequenceDetails),
        (status = 404, description = "Sequence not found"),
        (status = 500, description = "Internal server error", body = ApiError),
    )
)]
async fn sequence_detail(
    State(_ctx): State<PluginContext>,
    Path((author_id, sequence_id)): Path<(String, String)>,
) -> ApiResult<SequenceDetails> {
    let compound_id = format!("{author_id}:{sequence_id}");
    let graph = get_neo4j_graph().map_err(graph_err)?;
    let mut stream = graph
        .execute(queries::get::get_sequence_by_id(&compound_id))
        .await
        .map_err(graph_err)?;

    let mut sequence = match stream.try_next().await.map_err(graph_err)? {
        Some(row) => sequence_from_row(&row),
        None => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(ApiError {
                    error: format!("Sequence {compound_id} not found"),
                }),
            ))
        }
    };

    let (_found, tags) = fetch_tags(queries::get::get_tags_for_sequence(&compound_id)).await?;
    sequence.tags = Some(tags);
    Ok(Json(sequence))
}

/// List a user's sequences
#[utoipa::path(
    get,
    path = "/v0/mapky/sequences/user/{user_id}",
    tag = "Sequence",
    params(
        ("user_id" = String, Path, description = "User's pubky ID"),
        ("skip" = Option<i64>, Query, description = "Pagination offset (default 0)"),
        ("limit" = Option<i64>, Query, description = "Max results (default 100)"),
    ),
    responses(
        (status = 200, description = "User's sequences", body = Vec<SequenceDetails>),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
async fn user_sequences(
    State(_ctx): State<PluginContext>,
    Path(user_id): Path<String>,
    Query(params): Query<PaginationQuery>,
) -> ApiResult<Vec<SequenceDetails>> {
    let graph = get_neo4j_graph().map_err(graph_err)?;
    let mut stream = graph
        .execute(queries::get::get_user_sequences(
            &user_id,
            params.skip,
            params.limit,
        ))
        .await
        .map_err(graph_err)?;

    let mut sequences = Vec::new();
    while let Some(row) = stream.try_next().await.map_err(graph_err)? {
        sequences.push(sequence_from_row(&row));
    }
    Ok(Json(sequences))
}

/// Get tags for a MapkyAppSequence
#[utoipa::path(
    get,
    path = "/v0/mapky/sequences/{author_id}/{sequence_id}/tags",
    tag = "Sequence",
    params(
        ("author_id" = String, Path, description = "Author's pubky ID"),
        ("sequence_id" = String, Path, description = "Sequence ID"),
    ),
    responses(
        (status = 200, description = "Tags on the sequence", body = Vec<PostTagDetails>),
        (status = 404, description = "Sequence not found", body = ApiError),
        (status = 500, description = "Internal server error", body = ApiError),
    )
)]
async fn sequence_tags(
    State(_ctx): State<PluginContext>,
    Path((author_id, sequence_id)): Path<(String, String)>,
) -> ApiResult<Vec<PostTagDetails>> {
    let compound_id = format!("{author_id}:{sequence_id}");
    let (found, tags) = fetch_tags(queries::get::get_tags_for_sequence(&compound_id)).await?;
    if !found {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ApiError {
                error: format!("Sequence {compound_id} not found"),
            }),
        ));
    }
    Ok(Json(tags))
}

/// List all captures in a sequence, ordered by `sequence_index` ascending.
#[utoipa::path(
    get,
    path = "/v0/mapky/sequences/{author_id}/{sequence_id}/captures",
    tag = "Sequence",
    params(
        ("author_id" = String, Path, description = "Sequence author's pubky ID"),
        ("sequence_id" = String, Path, description = "Sequence ID"),
        ("skip" = Option<i64>, Query, description = "Pagination offset (default 0)"),
        ("limit" = Option<i64>, Query, description = "Max results (default 100)"),
    ),
    responses(
        (status = 200, description = "Captures in the sequence", body = Vec<GeoCaptureDetails>),
        (status = 500, description = "Internal server error", body = ApiError),
    )
)]
async fn sequence_captures(
    State(_ctx): State<PluginContext>,
    Path((author_id, sequence_id)): Path<(String, String)>,
    Query(params): Query<PaginationQuery>,
) -> ApiResult<Vec<GeoCaptureDetails>> {
    let sequence_uri = format!("pubky://{author_id}/pub/mapky.app/sequences/{sequence_id}");
    let graph = get_neo4j_graph().map_err(graph_err)?;
    let mut stream = graph
        .execute(queries::get::get_captures_in_sequence(
            &sequence_uri,
            params.skip,
            params.limit,
        ))
        .await
        .map_err(graph_err)?;

    let mut captures = Vec::new();
    while let Some(row) = stream.try_next().await.map_err(graph_err)? {
        captures.push(geo_capture_from_row(&row));
    }
    Ok(Json(captures))
}

/// Helper to parse a Neo4j row into `SequenceDetails` (no tags).
fn sequence_from_row(row: &neo4rs::Row) -> SequenceDetails {
    SequenceDetails {
        id: row.get("id").unwrap_or_default(),
        author_id: row.get("author_id").unwrap_or_default(),
        name: row.get("name").ok(),
        description: row.get("description").ok(),
        kind: row.get("kind").unwrap_or_default(),
        captured_at_start: row.get("captured_at_start").unwrap_or(0),
        captured_at_end: row.get("captured_at_end").unwrap_or(0),
        capture_count: row.get("capture_count").unwrap_or(0),
        min_lat: row.get("min_lat").ok(),
        min_lon: row.get("min_lon").ok(),
        max_lat: row.get("max_lat").ok(),
        max_lon: row.get("max_lon").ok(),
        device: row.get("device").ok(),
        indexed_at: row.get("indexed_at").unwrap_or(0),
        tags: None,
    }
}

// ── Collections ─────────────────────────────────────────────────────────────

/// Get a single collection by author and ID
#[utoipa::path(
    get,
    path = "/v0/mapky/collections/{author_id}/{collection_id}",
    tag = "Collection",
    params(
        ("author_id" = String, Path, description = "Author's pubky ID"),
        ("collection_id" = String, Path, description = "Collection ID"),
    ),
    responses(
        (status = 200, description = "Collection details with items", body = CollectionDetails),
        (status = 404, description = "Collection not found"),
        (status = 500, description = "Internal server error", body = ApiError),
    )
)]
async fn collection_detail(
    State(_ctx): State<PluginContext>,
    Path((author_id, collection_id)): Path<(String, String)>,
) -> ApiResult<CollectionDetails> {
    let compound_id = format!("{author_id}:{collection_id}");
    let graph = get_neo4j_graph().map_err(graph_err)?;
    let mut stream = graph
        .execute(queries::get::get_collection_by_id(&compound_id))
        .await
        .map_err(graph_err)?;

    match stream.try_next().await.map_err(graph_err)? {
        Some(row) => Ok(Json(CollectionDetails {
            id: row.get("id").unwrap_or_default(),
            author_id: row.get("author_id").unwrap_or_default(),
            name: row.get("name").unwrap_or_default(),
            description: row.get("description").ok(),
            items: row.get::<Vec<String>>("items").unwrap_or_default(),
            image_uri: row.get("image_uri").ok(),
            color: row.get("color").ok(),
            indexed_at: row.get("indexed_at").unwrap_or(0),
        })),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ApiError {
                error: format!("Collection {compound_id} not found"),
            }),
        )),
    }
}

/// List collections that contain at least one place inside the viewport
#[utoipa::path(
    get,
    path = "/v0/mapky/collections/viewport",
    tag = "Collection",
    params(
        ("min_lat" = f64, Query, description = "Minimum latitude"),
        ("min_lon" = f64, Query, description = "Minimum longitude"),
        ("max_lat" = f64, Query, description = "Maximum latitude"),
        ("max_lon" = f64, Query, description = "Maximum longitude"),
        ("limit" = Option<i64>, Query, description = "Max results (default 100)")
    ),
    responses(
        (status = 200, description = "Collections with places in viewport", body = Vec<CollectionDetails>),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
async fn collections_viewport(
    State(_ctx): State<PluginContext>,
    Query(params): Query<ViewportQuery>,
) -> ApiResult<Vec<CollectionDetails>> {
    let collections = fetch_collections_in_viewport(
        params.min_lat,
        params.min_lon,
        params.max_lat,
        params.max_lon,
        params.limit,
    )
    .await?;
    Ok(Json(collections))
}

/// List collections for a user
#[utoipa::path(
    get,
    path = "/v0/mapky/collections/user/{user_id}",
    tag = "Collection",
    params(
        ("user_id" = String, Path, description = "User's pubky ID"),
        ("skip" = Option<i64>, Query, description = "Pagination offset (default 0)"),
        ("limit" = Option<i64>, Query, description = "Max results (default 100)"),
    ),
    responses(
        (status = 200, description = "User's collections", body = Vec<CollectionDetails>),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
async fn user_collections(
    State(_ctx): State<PluginContext>,
    Path(user_id): Path<String>,
    Query(params): Query<PaginationQuery>,
) -> ApiResult<Vec<CollectionDetails>> {
    let graph = get_neo4j_graph().map_err(graph_err)?;
    let mut stream = graph
        .execute(queries::get::get_user_collections(
            &user_id,
            params.skip,
            params.limit,
        ))
        .await
        .map_err(graph_err)?;

    let mut collections = Vec::new();
    while let Some(row) = stream.try_next().await.map_err(graph_err)? {
        collections.push(CollectionDetails {
            id: row.get("id").unwrap_or_default(),
            author_id: row.get("author_id").unwrap_or_default(),
            name: row.get("name").unwrap_or_default(),
            description: row.get("description").ok(),
            items: row.get::<Vec<String>>("items").unwrap_or_default(),
            image_uri: row.get("image_uri").ok(),
            color: row.get("color").ok(),
            indexed_at: row.get("indexed_at").unwrap_or(0),
        });
    }

    Ok(Json(collections))
}

/// List collections that contain a specific place
#[utoipa::path(
    get,
    path = "/v0/mapky/collections/place/{osm_type}/{osm_id}",
    tag = "Collection",
    params(
        ("osm_type" = String, Path, description = "OSM element type"),
        ("osm_id" = i64, Path, description = "OSM element ID"),
    ),
    responses(
        (status = 200, description = "Collections containing this place", body = Vec<CollectionDetails>),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
async fn collections_for_place(
    State(_ctx): State<PluginContext>,
    Path((osm_type, osm_id)): Path<(String, i64)>,
) -> ApiResult<Vec<CollectionDetails>> {
    let osm_canonical = format!("{osm_type}/{osm_id}");
    let collections = fetch_collections_for_place(&osm_canonical).await?;
    Ok(Json(collections))
}

/// Get tags on a collection
#[utoipa::path(
    get,
    path = "/v0/mapky/collections/{author_id}/{collection_id}/tags",
    tag = "Collection",
    params(
        ("author_id" = String, Path, description = "Collection author's pubky ID"),
        ("collection_id" = String, Path, description = "Collection ID"),
    ),
    responses(
        (status = 200, description = "Tags on the collection", body = Vec<PostTagDetails>),
        (status = 404, description = "Collection not found", body = ApiError),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
async fn collection_tags(
    State(_ctx): State<PluginContext>,
    Path((author_id, collection_id)): Path<(String, String)>,
) -> ApiResult<Vec<PostTagDetails>> {
    use std::collections::HashMap;

    let compound_id = format!("{author_id}:{collection_id}");
    let graph = get_neo4j_graph().map_err(graph_err)?;
    let mut stream = graph
        .execute(queries::get::get_tags_for_collection(&compound_id))
        .await
        .map_err(graph_err)?;

    let mut found = false;
    let mut tag_map: HashMap<String, Vec<String>> = HashMap::new();

    while let Some(row) = stream.try_next().await.map_err(graph_err)? {
        found = true;
        let label: Option<String> = row.get("label").ok();
        let tagger_id: Option<String> = row.get("tagger_id").ok();
        if let (Some(l), Some(t)) = (label, tagger_id) {
            tag_map.entry(l).or_default().push(t);
        }
    }

    if !found {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ApiError {
                error: format!("Collection {compound_id} not found"),
            }),
        ));
    }

    let tags: Vec<PostTagDetails> = tag_map
        .into_iter()
        .map(|(label, taggers)| {
            let taggers_count = taggers.len();
            PostTagDetails {
                label,
                taggers,
                taggers_count,
            }
        })
        .collect();

    Ok(Json(tags))
}

// ── Routes ──────────────────────────────────────────────────────────────────

/// List routes whose bounding box overlaps the viewport
#[utoipa::path(
    get,
    path = "/v0/mapky/routes/viewport",
    tag = "Route",
    params(
        ("min_lat" = f64, Query, description = "Minimum latitude"),
        ("min_lon" = f64, Query, description = "Minimum longitude"),
        ("max_lat" = f64, Query, description = "Maximum latitude"),
        ("max_lon" = f64, Query, description = "Maximum longitude"),
        ("limit" = Option<i64>, Query, description = "Max results (default 100)")
    ),
    responses(
        (status = 200, description = "Routes overlapping viewport", body = Vec<RouteDetails>),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
async fn routes_viewport(
    State(_ctx): State<PluginContext>,
    Query(params): Query<ViewportQuery>,
) -> ApiResult<Vec<RouteDetails>> {
    let routes = fetch_routes_in_viewport(
        params.min_lat,
        params.min_lon,
        params.max_lat,
        params.max_lon,
        params.limit,
    )
    .await?;
    Ok(Json(routes))
}

/// Get a single route by author and ID
#[utoipa::path(
    get,
    path = "/v0/mapky/routes/{author_id}/{route_id}",
    tag = "Route",
    params(
        ("author_id" = String, Path, description = "Author's pubky ID"),
        ("route_id" = String, Path, description = "Route ID"),
    ),
    responses(
        (status = 200, description = "Route details (metadata, no waypoints)", body = RouteDetails),
        (status = 404, description = "Route not found"),
        (status = 500, description = "Internal server error", body = ApiError),
    )
)]
async fn route_detail(
    State(_ctx): State<PluginContext>,
    Path((author_id, route_id)): Path<(String, String)>,
) -> ApiResult<RouteDetails> {
    let compound_id = format!("{author_id}:{route_id}");
    let graph = get_neo4j_graph().map_err(graph_err)?;
    let mut stream = graph
        .execute(queries::get::get_route_by_id(&compound_id))
        .await
        .map_err(graph_err)?;

    match stream.try_next().await.map_err(graph_err)? {
        Some(row) => Ok(Json(route_from_row(&row))),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ApiError {
                error: format!("Route {compound_id} not found"),
            }),
        )),
    }
}

/// List routes for a user
#[utoipa::path(
    get,
    path = "/v0/mapky/routes/user/{user_id}",
    tag = "Route",
    params(
        ("user_id" = String, Path, description = "User's pubky ID"),
        ("skip" = Option<i64>, Query, description = "Pagination offset (default 0)"),
        ("limit" = Option<i64>, Query, description = "Max results (default 100)"),
    ),
    responses(
        (status = 200, description = "User's routes", body = Vec<RouteDetails>),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
async fn user_routes(
    State(_ctx): State<PluginContext>,
    Path(user_id): Path<String>,
    Query(params): Query<PaginationQuery>,
) -> ApiResult<Vec<RouteDetails>> {
    let graph = get_neo4j_graph().map_err(graph_err)?;
    let mut stream = graph
        .execute(queries::get::get_user_routes(
            &user_id,
            params.skip,
            params.limit,
        ))
        .await
        .map_err(graph_err)?;

    let mut routes = Vec::new();
    while let Some(row) = stream.try_next().await.map_err(graph_err)? {
        routes.push(route_from_row(&row));
    }

    Ok(Json(routes))
}

/// Get tags for a MapkyAppRoute
#[utoipa::path(
    get,
    path = "/v0/mapky/routes/{author_id}/{route_id}/tags",
    tag = "Route",
    params(
        ("author_id" = String, Path, description = "Author's pubky ID"),
        ("route_id" = String, Path, description = "MapkyAppRoute ID"),
    ),
    responses(
        (status = 200, description = "Tags for a MapkyAppRoute", body = Vec<PostTagDetails>),
        (status = 404, description = "Route not found"),
        (status = 500, description = "Internal server error", body = ApiError),
    )
)]
async fn route_tags(
    State(_ctx): State<PluginContext>,
    Path((author_id, route_id)): Path<(String, String)>,
) -> ApiResult<Vec<PostTagDetails>> {
    use std::collections::HashMap;

    let compound_id = format!("{author_id}:{route_id}");
    let graph = get_neo4j_graph().map_err(graph_err)?;

    let mut stream = graph
        .execute(queries::get::get_tags_for_mapky_route(&compound_id))
        .await
        .map_err(graph_err)?;

    let mut found = false;
    let mut tag_map: HashMap<String, Vec<String>> = HashMap::new();

    while let Some(row) = stream.try_next().await.map_err(graph_err)? {
        found = true;
        let label: Option<String> = row.get("label").ok();
        let tagger_id: Option<String> = row.get("tagger_id").ok();
        if let (Some(l), Some(t)) = (label, tagger_id) {
            tag_map.entry(l).or_default().push(t);
        }
    }

    if !found {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ApiError {
                error: format!("Route {compound_id} not found"),
            }),
        ));
    }

    let tags: Vec<PostTagDetails> = tag_map
        .into_iter()
        .map(|(label, taggers)| {
            let taggers_count = taggers.len();
            PostTagDetails {
                label,
                taggers,
                taggers_count,
            }
        })
        .collect();

    Ok(Json(tags))
}

/// List routes whose bounding box covers a given OSM place. Used by the
/// place detail panel to show "routes that pass through here".
#[utoipa::path(
    get,
    path = "/v0/mapky/place/{osm_type}/{osm_id}/routes",
    tag = "Place",
    params(
        ("osm_type" = String, Path, description = "OSM element type: node, way, or relation"),
        ("osm_id" = i64, Path, description = "OSM element ID"),
        ("limit" = Option<i64>, Query, description = "Max results (default 50)"),
    ),
    responses(
        (status = 200, description = "Routes near the place", body = Vec<RouteDetails>),
        (status = 404, description = "Place not found"),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
async fn place_routes(
    State(_ctx): State<PluginContext>,
    Path((osm_type, osm_id)): Path<(String, i64)>,
    Query(params): Query<PaginationQuery>,
) -> ApiResult<Vec<RouteDetails>> {
    let osm_canonical = format!("{osm_type}/{osm_id}");
    let detail = fetch_place_detail_by_canonical(&osm_canonical)
        .await?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ApiError {
                    error: format!("Place {osm_canonical} not found"),
                }),
            )
        })?;
    let routes = fetch_routes_near_point(detail.lat, detail.lon, params.limit).await?;
    Ok(Json(routes))
}

/// Helper to parse a Neo4j row into RouteDetails.
fn route_from_row(row: &neo4rs::Row) -> RouteDetails {
    RouteDetails {
        id: row.get("id").unwrap_or_default(),
        author_id: row.get("author_id").unwrap_or_default(),
        name: row.get("name").unwrap_or_default(),
        description: row.get("description").ok(),
        activity: row.get("activity").unwrap_or_default(),
        distance_m: row.get("distance_m").ok(),
        elevation_gain_m: row.get("elevation_gain_m").ok(),
        elevation_loss_m: row.get("elevation_loss_m").ok(),
        estimated_duration_s: row.get("estimated_duration_s").ok(),
        image_uri: row.get("image_uri").ok(),
        min_lat: row.get("min_lat").unwrap_or(0.0),
        min_lon: row.get("min_lon").unwrap_or(0.0),
        max_lat: row.get("max_lat").unwrap_or(0.0),
        max_lon: row.get("max_lon").unwrap_or(0.0),
        start_lat: row.get("start_lat").unwrap_or(0.0),
        start_lon: row.get("start_lon").unwrap_or(0.0),
        waypoint_count: row.get("waypoint_count").unwrap_or(0),
        indexed_at: row.get("indexed_at").unwrap_or(0),
    }
}

// ── Search ──────────────────────────────────────────────────────────────────

/// Search places and collections by tag label
#[utoipa::path(
    get,
    path = "/v0/mapky/search/tags",
    tag = "Search",
    params(
        ("q" = String, Query, description = "Tag label substring to search for"),
        ("limit" = Option<i64>, Query, description = "Max results per type (default 20)")
    ),
    responses(
        (status = 200, description = "Tagged places and collections", body = TagSearchResponse),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
async fn search_tags(
    State(_ctx): State<PluginContext>,
    Query(params): Query<TagSearchQuery>,
) -> ApiResult<TagSearchResponse> {
    let query_str = params.q.trim().to_lowercase();
    if query_str.is_empty() {
        return Ok(Json(TagSearchResponse {
            places: Vec::new(),
            collections: Vec::new(),
            reviews: Vec::new(),
            posts: Vec::new(),
            routes: Vec::new(),
            geo_captures: Vec::new(),
            sequences: Vec::new(),
            incidents: Vec::new(),
        }));
    }

    let graph = get_neo4j_graph().map_err(graph_err)?;

    // Search places by tag
    let mut places = Vec::new();
    let mut stream = graph
        .execute(queries::get::search_places_by_tag(&query_str, params.limit))
        .await
        .map_err(graph_err)?;
    while let Some(row) = stream.try_next().await.map_err(graph_err)? {
        places.push(place_details_from_row(&row));
    }

    // Search collections by tag
    let mut collections = Vec::new();
    let mut stream = graph
        .execute(queries::get::search_collections_by_tag(
            &query_str,
            params.limit,
        ))
        .await
        .map_err(graph_err)?;
    while let Some(row) = stream.try_next().await.map_err(graph_err)? {
        collections.push(CollectionDetails {
            id: row.get("id").unwrap_or_default(),
            author_id: row.get("author_id").unwrap_or_default(),
            name: row.get("name").unwrap_or_default(),
            description: row.get("description").ok(),
            items: row.get::<Vec<String>>("items").unwrap_or_default(),
            image_uri: row.get("image_uri").ok(),
            color: row.get("color").ok(),
            indexed_at: row.get("indexed_at").unwrap_or(0),
        });
    }

    // Search reviews by tag
    let mut reviews = Vec::new();
    let mut stream = graph
        .execute(queries::get::search_reviews_by_tag(
            &query_str,
            params.limit,
        ))
        .await
        .map_err(graph_err)?;
    while let Some(row) = stream.try_next().await.map_err(graph_err)? {
        reviews.push(review_from_row(&row));
    }

    // Search routes by tag
    let mut routes = Vec::new();
    let mut stream = graph
        .execute(queries::get::search_routes_by_tag(&query_str, params.limit))
        .await
        .map_err(graph_err)?;
    while let Some(row) = stream.try_next().await.map_err(graph_err)? {
        routes.push(route_from_row(&row));
    }

    // Search cross-namespace posts by tag
    let mut posts = Vec::new();
    let mut stream = graph
        .execute(queries::get::search_posts_by_tag(&query_str, params.limit))
        .await
        .map_err(graph_err)?;
    while let Some(row) = stream.try_next().await.map_err(graph_err)? {
        posts.push(mapky_post_from_row(&row));
    }

    // Search geo-captures by tag
    let mut geo_captures = Vec::new();
    let mut stream = graph
        .execute(queries::get::search_geo_captures_by_tag(
            &query_str,
            params.limit,
        ))
        .await
        .map_err(graph_err)?;
    while let Some(row) = stream.try_next().await.map_err(graph_err)? {
        geo_captures.push(geo_capture_from_row(&row));
    }

    // Search sequences by tag
    let mut sequences = Vec::new();
    let mut stream = graph
        .execute(queries::get::search_sequences_by_tag(
            &query_str,
            params.limit,
        ))
        .await
        .map_err(graph_err)?;
    while let Some(row) = stream.try_next().await.map_err(graph_err)? {
        sequences.push(sequence_from_row(&row));
    }

    // Search incidents by tag
    let mut incidents = Vec::new();
    let mut stream = graph
        .execute(queries::get::search_incidents_by_tag(
            &query_str,
            params.limit,
        ))
        .await
        .map_err(graph_err)?;
    while let Some(row) = stream.try_next().await.map_err(graph_err)? {
        incidents.push(IncidentDetails {
            id: row.get("id").unwrap_or_default(),
            author_id: row.get("author_id").unwrap_or_default(),
            incident_type: row.get("incident_type").unwrap_or_default(),
            severity: row.get("severity").unwrap_or_default(),
            lat: row.get("lat").unwrap_or(0.0),
            lon: row.get("lon").unwrap_or(0.0),
            heading: row.get("heading").ok(),
            description: row.get("description").ok(),
            attachments: row.get::<Vec<String>>("attachments").unwrap_or_default(),
            expires_at: row.get("expires_at").ok(),
            indexed_at: row.get("indexed_at").unwrap_or(0),
        });
    }

    Ok(Json(TagSearchResponse {
        places,
        collections,
        reviews,
        posts,
        routes,
        geo_captures,
        sequences,
        incidents,
    }))
}

/// Cached batched OSM lookup. Frontend hits this instead of public
/// Nominatim — Redis-backed (30 d TTL) and rate-limited to Nominatim's
/// 1 req/s policy on the backend, so multiple users sharing a place
/// only cost one upstream request.
#[utoipa::path(
    get,
    path = "/v0/mapky/osm/lookup",
    tag = "OSM",
    params(
        ("osm_ids" = String, Query, description = "Comma-separated OSM IDs in `N123,W456,R789` form"),
    ),
    responses(
        (status = 200, description = "Resolved Nominatim entries (one per input ref, in input order)", body = Vec<NominatimLookup>),
        (status = 400, description = "Malformed osm_ids", body = ApiError)
    )
)]
async fn osm_lookup(
    State(_ctx): State<PluginContext>,
    Query(params): Query<OsmLookupQuery>,
) -> ApiResult<Vec<NominatimLookup>> {
    let raw = params.osm_ids.trim();
    if raw.is_empty() {
        return Ok(Json(Vec::new()));
    }

    // Parse `N123,W456,R789` into (osm_type, osm_id) pairs. Bad refs
    // (anything that doesn't start with N/W/R or has a non-numeric
    // id) get dropped — the response stays tied to the parsed list,
    // not the raw query, so callers should match by the result's
    // `osm_type` + `osm_id` fields.
    let mut refs: Vec<(String, i64)> = Vec::new();
    for chunk in raw.split(',') {
        let chunk = chunk.trim();
        if chunk.is_empty() {
            continue;
        }
        let (prefix, rest) = chunk.split_at(1);
        let osm_type = match prefix {
            "N" => "node",
            "W" => "way",
            "R" => "relation",
            _ => continue,
        };
        let Ok(id) = rest.parse::<i64>() else {
            continue;
        };
        refs.push((osm_type.to_string(), id));
    }

    Ok(Json(batch_lookup_cached(&refs).await))
}

/// Cached free-text search proxy.
///
/// Returns the same shape as `/osm/lookup` (a `NominatimLookup` list),
/// minus `extratags` for the search shape. Cached in Redis with
/// versioned keys; the upstream call shares the same 1 req/s gate as
/// the lookup pipeline so search and lookup never collide.
#[utoipa::path(
    get,
    path = "/v0/mapky/osm/search",
    tag = "OSM",
    params(
        ("q" = String, Query, description = "Free-text query"),
        ("viewbox" = Option<String>, Query, description = "`west,north,east,south` bias / restriction"),
        ("bounded" = Option<bool>, Query, description = "Restrict to viewbox (requires viewbox)"),
        ("limit" = Option<u32>, Query, description = "Max results, default 8"),
        ("dedupe" = Option<bool>, Query, description = "Drop near-duplicate results, default true"),
        ("addressdetails" = Option<bool>, Query, description = "Include `address` map, default false"),
    ),
    responses(
        (status = 200, description = "Matching Nominatim entries", body = Vec<NominatimLookup>)
    )
)]
async fn osm_search(
    State(_ctx): State<PluginContext>,
    Query(params): Query<OsmSearchQuery>,
) -> ApiResult<Vec<NominatimLookup>> {
    let q = params.q.trim();
    if q.is_empty() {
        return Ok(Json(Vec::new()));
    }
    let result = search_cached(&SearchParams {
        q: q.to_string(),
        viewbox: params.viewbox,
        bounded: params.bounded,
        limit: params.limit.clamp(1, 50),
        dedupe: params.dedupe,
        addressdetails: params.addressdetails,
    })
    .await;
    Ok(Json(result))
}

/// Cached reverse-geocode proxy.
///
/// Returns the place at `(lat, lon)` or 404 if Nominatim has nothing.
/// Coordinates are quantized to ~1 m precision for cache key building
/// so callers within the same metre share a Redis slot — matches the
/// frontend's `makeReverseKey`.
#[utoipa::path(
    get,
    path = "/v0/mapky/osm/reverse",
    tag = "OSM",
    params(
        ("lat" = f64, Query, description = "Latitude"),
        ("lon" = f64, Query, description = "Longitude"),
        ("zoom" = Option<u32>, Query, description = "Nominatim zoom (1-18, default 18)"),
    ),
    responses(
        (status = 200, description = "Place at coordinate", body = NominatimLookup),
        (status = 404, description = "No place at coordinate", body = ApiError)
    )
)]
async fn osm_reverse(
    State(_ctx): State<PluginContext>,
    Query(params): Query<OsmReverseQuery>,
) -> ApiResult<NominatimLookup> {
    let zoom = params.zoom.clamp(1, 18);
    match reverse_cached(params.lat, params.lon, zoom).await {
        Some(hit) => Ok(Json(hit)),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ApiError {
                error: format!("No Nominatim entry at {},{}", params.lat, params.lon),
            }),
        )),
    }
}

// ── BTC overlay ─────────────────────────────────────────────────────────

/// Bitcoin-accepting POI as served to the frontend BTC overlay.
///
/// Mirrors the frontend's `BitcoinPoi` (`mapky-app/src/lib/btcmap/overpass.ts`)
/// so the type can be consumed unchanged after the network call swaps
/// from public Overpass to this endpoint.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct BitcoinPoi {
    pub osm_type: String,
    pub osm_id: i64,
    pub lat: f64,
    pub lon: f64,
    pub name: Option<String>,
    pub onchain: bool,
    pub lightning: bool,
    pub lightning_contactless: bool,
}

/// List Bitcoin-accepting places within a geographic bounding box.
///
/// Served from `:Place` nodes flagged `accepts_bitcoin = true` by the
/// periodic BTCMap sync (see `btcmap_sync.rs`). No upstream Overpass
/// call on the request path — sub-100 ms for any user, any region.
#[utoipa::path(
    get,
    path = "/v0/mapky/btc/viewport",
    tag = "BTC",
    params(
        ("min_lat" = f64, Query, description = "Minimum latitude"),
        ("min_lon" = f64, Query, description = "Minimum longitude"),
        ("max_lat" = f64, Query, description = "Maximum latitude"),
        ("max_lon" = f64, Query, description = "Maximum longitude"),
        ("limit" = Option<i64>, Query, description = "Max results (default 100)")
    ),
    responses(
        (status = 200, description = "Bitcoin-accepting POIs in viewport", body = Vec<BitcoinPoi>),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
async fn btc_viewport(
    State(_ctx): State<PluginContext>,
    Query(params): Query<ViewportQuery>,
) -> ApiResult<Vec<BitcoinPoi>> {
    let graph = get_neo4j_graph().map_err(graph_err)?;

    let mut stream = graph
        .execute(queries::get::get_btc_places_in_viewport(
            params.min_lat,
            params.min_lon,
            params.max_lat,
            params.max_lon,
            params.limit,
        ))
        .await
        .map_err(graph_err)?;

    let mut places = Vec::new();
    while let Some(row) = stream.try_next().await.map_err(graph_err)? {
        places.push(BitcoinPoi {
            osm_type: row.get("osm_type").unwrap_or_default(),
            osm_id: row.get("osm_id").unwrap_or(0),
            lat: row.get("lat").unwrap_or(0.0),
            lon: row.get("lon").unwrap_or(0.0),
            name: row.get("name").ok(),
            onchain: row.get("btc_onchain").unwrap_or(false),
            lightning: row.get("btc_lightning").unwrap_or(false),
            lightning_contactless: row.get("btc_lightning_contactless").unwrap_or(false),
        });
    }

    Ok(Json(places))
}

/// Inspect BTCMap sync state: configured upstream URL, refresh
/// interval, last successful sync timestamp, whether a sync is
/// currently running. Cheap call, two Redis GETs.
#[utoipa::path(
    get,
    path = "/v0/mapky/btc/status",
    tag = "BTC",
    responses(
        (status = 200, description = "Current BTCMap sync state", body = SyncStatus)
    )
)]
async fn btc_status(State(_ctx): State<PluginContext>) -> ApiResult<SyncStatus> {
    Ok(Json(btcmap_sync::read_status().await))
}

// ── Cached Valhalla routing ─────────────────────────────────────────────

/// Cached Valhalla `/route` proxy.
///
/// Body is the standard Valhalla request envelope (locations, costing,
/// costing_options, alternates, directions_options). Response is
/// passthrough — same JSON the upstream returned, but cached in Redis
/// keyed by a content hash of the request.
///
/// The hash is canonical (object key order doesn't matter), so two
/// frontends serializing the same request differently still hit the
/// same cache entry.
#[utoipa::path(
    post,
    path = "/v0/mapky/routing/valhalla",
    tag = "Routing",
    request_body(
        description = "Valhalla `/route` request",
        content_type = "application/json"
    ),
    responses(
        (status = 200, description = "Snapped route — passthrough Valhalla response"),
        (status = 429, description = "Upstream rate-limited", body = ApiError),
        (status = 502, description = "Upstream error or unreachable", body = ApiError)
    )
)]
async fn routing_valhalla(
    State(_ctx): State<PluginContext>,
    Json(body): Json<serde_json::Value>,
) -> ApiResult<serde_json::Value> {
    match routing_proxy::route(body).await {
        RouteOutcome::Ok(value) => Ok(Json(value)),
        RouteOutcome::Upstream { status, body } => {
            // Forward the upstream status when we recognize it, so the
            // frontend's existing 429 handling (friendly rate-limit
            // message) stays intact.
            let mapped = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
            Err((mapped, Json(ApiError { error: body })))
        }
        RouteOutcome::Network(msg) => Err((
            StatusCode::BAD_GATEWAY,
            Json(ApiError {
                // Most operational hits here are the public FOSSGIS
                // instance going down. The env-var hint nudges the
                // operator at a self-hosted Valhalla without making
                // them grep the code.
                error: format!(
                    "Valhalla upstream unreachable: {msg}. \
                     Set MAPKY_VALHALLA_URL to a self-hosted /route endpoint."
                ),
            }),
        )),
    }
}
