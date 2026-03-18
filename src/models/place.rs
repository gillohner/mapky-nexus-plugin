//! Place model — an OSM element stored as a Neo4j `:Place` node.

use std::time::Duration;

use chrono::Utc;
use mapky_app_specs::OsmRef;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::warn;

/// Public Nominatim API (1 req/s rate limit)
const NOMINATIM_URL: &str = "https://nominatim.openstreetmap.org";

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct PlaceDetails {
    pub osm_canonical: String, // "node/123" — primary key across stores
    pub osm_type: String,      // "node" | "way" | "relation"
    pub osm_id: i64,
    pub lat: f64,
    pub lon: f64,
    pub review_count: i64,
    pub avg_rating: f64,
    pub tag_count: i64,
    pub photo_count: i64,
    pub indexed_at: i64,
}

impl PlaceDetails {
    pub fn new(osm_type: &str, osm_id: i64, lat: f64, lon: f64) -> Self {
        Self {
            osm_canonical: format!("{osm_type}/{osm_id}"),
            osm_type: osm_type.to_string(),
            osm_id,
            lat,
            lon,
            review_count: 0,
            avg_rating: 0.0,
            tag_count: 0,
            photo_count: 0,
            indexed_at: Utc::now().timestamp_millis(),
        }
    }

    /// Resolve OSM coordinates via Nominatim. Falls back to (0.0, 0.0) on failure.
    pub async fn from_osm_ref(osm_ref: &OsmRef) -> Self {
        let osm_type = osm_ref.osm_type.to_string();
        let osm_id = osm_ref.osm_id;

        match resolve_osm_coords(&osm_type, osm_id).await {
            Some((lat, lon)) => Self::new(&osm_type, osm_id, lat, lon),
            None => {
                warn!("Could not resolve coordinates for {osm_type}/{osm_id}, defaulting to (0,0)");
                Self::new(&osm_type, osm_id, 0.0, 0.0)
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct NominatimResult {
    lat: String,
    lon: String,
}

async fn resolve_osm_coords(osm_type: &str, osm_id: i64) -> Option<(f64, f64)> {
    let char = match osm_type {
        "node" => 'N',
        "way" => 'W',
        "relation" => 'R',
        _ => return None,
    };

    let client = Client::builder()
        .user_agent("mapky-nexus-plugin/0.1 (+https://github.com/gillohner/mapky-indexer)")
        .timeout(Duration::from_secs(5))
        .build()
        .ok()?;

    let url = format!("{NOMINATIM_URL}/lookup?osm_ids={char}{osm_id}&format=json");
    let results: Vec<NominatimResult> = client.get(&url).send().await.ok()?.json().await.ok()?;

    let first = results.into_iter().next()?;
    let lat = first.lat.parse::<f64>().ok()?;
    let lon = first.lon.parse::<f64>().ok()?;
    Some((lat, lon))
}
