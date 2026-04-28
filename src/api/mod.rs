//! Axum API routes for the mapky plugin.
//! Mounted at `/v0/mapky/` by nexusd.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use futures::TryStreamExt;
use nexus_common::db::get_neo4j_graph;
use nexus_common::plugin::PluginContext;
use serde::{Deserialize, Serialize};
use utoipa::OpenApi;

use crate::models::collection::CollectionDetails;
use crate::models::geo_capture::GeoCaptureDetails;
use crate::models::incident::IncidentDetails;
use crate::models::place::PlaceDetails;
use crate::models::post::PostDetails;
use crate::models::route::RouteDetails;
use crate::models::sequence::SequenceDetails;
use crate::models::tag::PostTagDetails;
use crate::queries;

pub fn routes(ctx: PluginContext) -> Router {
    Router::new()
        // ── Place ──
        .route("/viewport", get(viewport))
        .route("/place/{osm_type}/{osm_id}", get(place_detail))
        .route("/place/{osm_type}/{osm_id}/posts", get(place_posts))
        .route("/place/{osm_type}/{osm_id}/tags", get(place_tags))
        .route("/place/{osm_type}/{osm_id}/routes", get(place_routes))
        // ── Post ──
        .route("/posts/{author_id}/{post_id}/tags", get(post_tags))
        .route("/posts/user/{user_id}", get(user_posts))
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
        .route(
            "/sequences/{author_id}/{sequence_id}",
            get(sequence_detail),
        )
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
    ),
    paths(
        viewport, place_detail, place_posts, place_tags, place_routes,
        post_tags, user_posts,
        incidents_viewport, incident_detail, user_incidents,
        geo_captures_viewport, geo_capture_detail, geo_capture_tags, user_geo_captures, nearby_geo_captures,
        sequence_detail, sequence_tags, sequence_captures, user_sequences,
        collection_detail, user_collections, collections_for_place, collection_tags,
        routes_viewport, route_detail, route_tags, user_routes,
        search_tags,
    ),
    components(schemas(
        PlaceDetails, PostDetails, PostTagDetails,
        IncidentDetails, GeoCaptureDetails, SequenceDetails, CollectionDetails, RouteDetails,
        ViewportQuery, PostsQuery, PaginationQuery, NearbyQuery,
        TagSearchQuery, TagSearchResponse,
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

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct PostsQuery {
    #[serde(default)]
    pub skip: i64,
    #[serde(default = "default_limit")]
    pub limit: i64,
    #[serde(default)]
    pub reviews_only: bool,
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

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct TagSearchResponse {
    pub places: Vec<PlaceDetails>,
    pub collections: Vec<CollectionDetails>,
    pub posts: Vec<PostDetails>,
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

// ── Handlers ────────────────────────────────────────────────────────────────

/// List places within a geographic bounding box
#[utoipa::path(
    get,
    path = "/v0/mapky/viewport",
    tag = "Place",
    params(
        ("min_lat" = f64, Query, description = "Minimum latitude"),
        ("min_lon" = f64, Query, description = "Minimum longitude"),
        ("max_lat" = f64, Query, description = "Maximum latitude"),
        ("max_lon" = f64, Query, description = "Maximum longitude"),
        ("limit" = Option<i64>, Query, description = "Max results (default 100)")
    ),
    responses(
        (status = 200, description = "List of places in viewport", body = Vec<PlaceDetails>),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
async fn viewport(
    State(_ctx): State<PluginContext>,
    Query(params): Query<ViewportQuery>,
) -> ApiResult<Vec<PlaceDetails>> {
    let graph = get_neo4j_graph().map_err(graph_err)?;

    let mut stream = graph
        .execute(queries::get::get_places_in_viewport(
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
        let place = PlaceDetails {
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
        };
        places.push(place);
    }

    Ok(Json(places))
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
    let graph = get_neo4j_graph().map_err(graph_err)?;

    let mut stream = graph
        .execute(queries::get::get_place_by_canonical(&osm_canonical))
        .await
        .map_err(graph_err)?;

    match stream.try_next().await.map_err(graph_err)? {
        Some(row) => {
            let place = PlaceDetails {
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
            };
            Ok(Json(place))
        }
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ApiError {
                error: format!("Place {osm_canonical} not found"),
            }),
        )),
    }
}

/// Get tags for a MapkyAppPost
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
    use std::collections::HashMap;

    let compound_id = format!("{author_id}:{post_id}");
    let graph = get_neo4j_graph().map_err(graph_err)?;

    let mut stream = graph
        .execute(queries::get::get_tags_for_mapky_post(&compound_id))
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
                error: format!("Post {compound_id} not found"),
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

/// List a user's posts
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
        (status = 200, description = "User's posts", body = Vec<PostDetails>),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
async fn user_posts(
    State(_ctx): State<PluginContext>,
    Path(user_id): Path<String>,
    Query(params): Query<PaginationQuery>,
) -> ApiResult<Vec<PostDetails>> {
    let graph = get_neo4j_graph().map_err(graph_err)?;
    let mut stream = graph
        .execute(queries::get::get_user_posts(&user_id, params.skip, params.limit))
        .await
        .map_err(graph_err)?;

    let mut posts = Vec::new();
    while let Some(row) = stream.try_next().await.map_err(graph_err)? {
        let rating_raw: Option<i64> = row.get("rating").ok();
        let rating = rating_raw.and_then(|r| if r > 0 { Some(r as u8) } else { None });
        let compound_id: String = row.get("id").unwrap_or_default();
        posts.push(PostDetails {
            id: short_post_id(&compound_id),
            author_id: row.get("author_id").unwrap_or_default(),
            osm_canonical: row.get("osm_canonical").unwrap_or_default(),
            content: row.get("content").ok(),
            rating,
            kind: row.get("kind").unwrap_or_else(|_| "post".to_string()),
            parent_uri: row.get("parent_uri").ok(),
            attachments: row.get::<Vec<String>>("attachments").unwrap_or_default(),
            indexed_at: row.get("indexed_at").unwrap_or(0),
        });
    }

    Ok(Json(posts))
}

/// List posts for a place, optionally filtered to reviews only
#[utoipa::path(
    get,
    path = "/v0/mapky/place/{osm_type}/{osm_id}/posts",
    tag = "Place",
    params(
        ("osm_type" = String, Path, description = "OSM element type: node, way, or relation"),
        ("osm_id" = i64, Path, description = "OSM element ID"),
        ("skip" = Option<i64>, Query, description = "Pagination offset (default 0)"),
        ("limit" = Option<i64>, Query, description = "Max results (default 100)"),
        ("reviews_only" = Option<bool>, Query, description = "Return only rated reviews")
    ),
    responses(
        (status = 200, description = "List of posts for the place", body = Vec<PostDetails>),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
async fn place_posts(
    State(_ctx): State<PluginContext>,
    Path((osm_type, osm_id)): Path<(String, i64)>,
    Query(params): Query<PostsQuery>,
) -> ApiResult<Vec<PostDetails>> {
    let osm_canonical = format!("{osm_type}/{osm_id}");
    let graph = get_neo4j_graph().map_err(graph_err)?;

    let q = if params.reviews_only {
        queries::get::get_reviews_for_place(&osm_canonical, params.skip, params.limit)
    } else {
        queries::get::get_posts_for_place(&osm_canonical, params.skip, params.limit)
    };

    let mut stream = graph.execute(q).await.map_err(graph_err)?;

    let mut posts = Vec::new();
    while let Some(row) = stream.try_next().await.map_err(graph_err)? {
        let rating_raw: Option<i64> = row.get("rating").ok();
        let rating = rating_raw.and_then(|r| if r > 0 { Some(r as u8) } else { None });

        let compound_id: String = row.get("id").unwrap_or_default();
        let post = PostDetails {
            id: short_post_id(&compound_id),
            author_id: row.get("author_id").unwrap_or_default(),
            osm_canonical: row.get("osm_canonical").unwrap_or_default(),
            content: row.get("content").ok(),
            rating,
            kind: row.get("kind").unwrap_or_else(|_| "post".to_string()),
            parent_uri: row.get("parent_uri").ok(),
            attachments: row.get::<Vec<String>>("attachments").unwrap_or_default(),
            indexed_at: row.get("indexed_at").unwrap_or(0),
        };
        posts.push(post);
    }

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
    use std::collections::HashMap;

    let osm_canonical = format!("{osm_type}/{osm_id}");
    let graph = get_neo4j_graph().map_err(graph_err)?;
    let mut stream = graph
        .execute(queries::get::get_tags_for_place(&osm_canonical))
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
                error: format!("Place {osm_canonical} not found"),
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
        .execute(queries::get::get_user_incidents(&user_id, params.skip, params.limit))
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
    let graph = get_neo4j_graph().map_err(graph_err)?;
    let mut stream = graph
        .execute(queries::get::get_geo_captures_in_viewport(
            params.min_lat,
            params.min_lon,
            params.max_lat,
            params.max_lon,
            params.limit,
        ))
        .await
        .map_err(graph_err)?;

    let mut captures = Vec::new();
    while let Some(row) = stream.try_next().await.map_err(graph_err)? {
        captures.push(GeoCaptureDetails {
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
        });
    }

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

    let (_found, tags) =
        fetch_tags(queries::get::get_tags_for_geo_capture(&compound_id)).await?;
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
    let (found, tags) =
        fetch_tags(queries::get::get_tags_for_geo_capture(&compound_id)).await?;
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

    let (_found, tags) =
        fetch_tags(queries::get::get_tags_for_sequence(&compound_id)).await?;
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
    let (found, tags) =
        fetch_tags(queries::get::get_tags_for_sequence(&compound_id)).await?;
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
    let graph = get_neo4j_graph().map_err(graph_err)?;
    let mut stream = graph
        .execute(queries::get::get_collections_containing_place(&osm_canonical))
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
    let graph = get_neo4j_graph().map_err(graph_err)?;
    let mut stream = graph
        .execute(queries::get::get_routes_in_viewport(
            params.min_lat,
            params.min_lon,
            params.max_lat,
            params.max_lon,
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
    let graph = get_neo4j_graph().map_err(graph_err)?;

    // Resolve the place's lat/lon first — bounding-box-contains needs the
    // point to test against. 404 if the place isn't indexed yet.
    let mut place_stream = graph
        .execute(queries::get::get_place_by_canonical(&osm_canonical))
        .await
        .map_err(graph_err)?;
    let place_row = place_stream.try_next().await.map_err(graph_err)?;
    let (lat, lon): (f64, f64) = match place_row {
        Some(r) => (
            r.get("lat").unwrap_or(0.0),
            r.get("lon").unwrap_or(0.0),
        ),
        None => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(ApiError {
                    error: format!("Place {osm_canonical} not found"),
                }),
            ));
        }
    };

    let mut stream = graph
        .execute(queries::get::get_routes_near_point(lat, lon, params.limit))
        .await
        .map_err(graph_err)?;

    let mut routes = Vec::new();
    while let Some(row) = stream.try_next().await.map_err(graph_err)? {
        routes.push(route_from_row(&row));
    }

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
            posts: Vec::new(),
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
        places.push(PlaceDetails {
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
        });
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

    // Search posts by tag
    let mut posts = Vec::new();
    let mut stream = graph
        .execute(queries::get::search_posts_by_tag(&query_str, params.limit))
        .await
        .map_err(graph_err)?;
    while let Some(row) = stream.try_next().await.map_err(graph_err)? {
        let compound_id: String = row.get("id").unwrap_or_default();
        posts.push(PostDetails {
            id: short_post_id(&compound_id),
            author_id: row.get("author_id").unwrap_or_default(),
            osm_canonical: row.get("osm_canonical").unwrap_or_default(),
            content: row.get("content").ok(),
            rating: row.get::<i64>("rating").ok().map(|r| r as u8),
            kind: row.get("kind").unwrap_or_default(),
            parent_uri: row.get("parent_uri").ok(),
            attachments: row.get::<Vec<String>>("attachments").unwrap_or_default(),
            indexed_at: row.get("indexed_at").unwrap_or(0),
        });
    }

    Ok(Json(TagSearchResponse {
        places,
        collections,
        posts,
    }))
}
