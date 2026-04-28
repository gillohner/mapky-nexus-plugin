//! Route model — a user-created route stored as a Neo4j `:MapkyAppRoute`
//! node with bounding box for spatial viewport queries. Full waypoint data
//! stays on the homeserver — only metadata is indexed.

use mapky_app_specs::MapkyAppRoute;
use serde::{Deserialize, Serialize};

use chrono::Utc;

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct RouteDetails {
    pub id: String, // compound: "user_id:route_id"
    pub author_id: String,
    pub name: String,
    pub description: Option<String>,
    pub activity: String,
    pub distance_m: Option<f64>,
    pub elevation_gain_m: Option<f64>,
    pub elevation_loss_m: Option<f64>,
    pub estimated_duration_s: Option<i64>,
    pub image_uri: Option<String>,
    /// Bounding box computed from waypoints.
    pub min_lat: f64,
    pub min_lon: f64,
    pub max_lat: f64,
    pub max_lon: f64,
    /// First waypoint — used for spatial point index.
    pub start_lat: f64,
    pub start_lon: f64,
    pub waypoint_count: i64,
    pub indexed_at: i64,
}

impl RouteDetails {
    pub fn from_mapky_route(route: &MapkyAppRoute, user_id: &str, route_id: &str) -> Self {
        // Compute bounding box from waypoints.
        let min_lat = route
            .waypoints
            .iter()
            .map(|w| w.lat)
            .fold(f64::INFINITY, f64::min);
        let max_lat = route
            .waypoints
            .iter()
            .map(|w| w.lat)
            .fold(f64::NEG_INFINITY, f64::max);
        let min_lon = route
            .waypoints
            .iter()
            .map(|w| w.lon)
            .fold(f64::INFINITY, f64::min);
        let max_lon = route
            .waypoints
            .iter()
            .map(|w| w.lon)
            .fold(f64::NEG_INFINITY, f64::max);

        let start_lat = route.waypoints[0].lat;
        let start_lon = route.waypoints[0].lon;

        Self {
            id: format!("{user_id}:{route_id}"),
            author_id: user_id.to_string(),
            name: route.name.clone(),
            description: route.description.clone(),
            activity: serde_json::to_value(&route.activity)
                .ok()
                .and_then(|v| v.as_str().map(String::from))
                .unwrap_or_default(),
            distance_m: route.distance_m,
            elevation_gain_m: route.elevation_gain_m,
            elevation_loss_m: route.elevation_loss_m,
            estimated_duration_s: route.estimated_duration_s,
            image_uri: route.image_uri.clone(),
            min_lat,
            min_lon,
            max_lat,
            max_lon,
            start_lat,
            start_lon,
            waypoint_count: route.waypoints.len() as i64,
            indexed_at: Utc::now().timestamp_millis(),
        }
    }
}
