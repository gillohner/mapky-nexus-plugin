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

use crate::models::place::PlaceDetails;
use crate::models::post::PostDetails;
use crate::models::tag::PostTagDetails;
use crate::queries;

pub fn routes(ctx: PluginContext) -> Router {
    Router::new()
        .route("/viewport", get(viewport))
        .route("/place/{osm_type}/{osm_id}", get(place_detail))
        .route("/place/{osm_type}/{osm_id}/posts", get(place_posts))
        .route("/posts/{author_id}/{post_id}/tags", get(post_tags))
        .with_state(ctx)
}

#[derive(OpenApi)]
#[openapi(
    paths(viewport, place_detail, place_posts, post_tags),
    components(schemas(PlaceDetails, PostDetails, PostTagDetails, ViewportQuery, PostsQuery))
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

fn default_limit() -> i64 {
    100
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

// ── Handlers ────────────────────────────────────────────────────────────────

/// List places within a geographic bounding box
#[utoipa::path(
    get,
    path = "/v0/mapky/viewport",
    tag = "Mapky",
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
    tag = "Mapky",
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
    tag = "Mapky",
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

/// List posts for a place, optionally filtered to reviews only
#[utoipa::path(
    get,
    path = "/v0/mapky/place/{osm_type}/{osm_id}/posts",
    tag = "Mapky",
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
