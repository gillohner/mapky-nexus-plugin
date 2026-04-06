//! Place model — an OSM element stored as a Neo4j `:Place` node.

use std::sync::OnceLock;
use std::time::{Duration, Instant};

use chrono::Utc;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::warn;

/// Public Nominatim API — 1 request/second hard limit per usage policy.
const NOMINATIM_URL: &str = "https://nominatim.openstreetmap.org";
const NOMINATIM_MIN_INTERVAL: Duration = Duration::from_secs(1);

/// Shared HTTP client — built once, reused across all geocoding calls.
fn nominatim_client() -> &'static Client {
    static CLIENT: OnceLock<Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        Client::builder()
            .user_agent("mapky-nexus-plugin/0.1 (+https://github.com/gillohner/mapky)")
            .timeout(Duration::from_secs(10))
            .build()
            .expect("failed to build Nominatim HTTP client")
    })
}

/// Global rate-limit gate. All geocoding calls serialize through this mutex
/// and sleep until 1 s has elapsed since the previous request, ensuring we
/// never exceed Nominatim's 1 req/s policy even under concurrent event load.
fn nominatim_rate_limiter() -> &'static Mutex<Instant> {
    static LAST_REQUEST: OnceLock<Mutex<Instant>> = OnceLock::new();
    LAST_REQUEST.get_or_init(|| {
        // Subtract the interval so the first request fires immediately.
        Mutex::new(Instant::now() - NOMINATIM_MIN_INTERVAL)
    })
}

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct PlaceDetails {
    pub osm_canonical: String, // "node/123" — primary key across stores
    pub osm_type: String,      // "node" | "way" | "relation"
    pub osm_id: i64,
    pub lat: f64,
    pub lon: f64,
    /// `true` when coordinates were successfully resolved via Nominatim.
    /// `false` means the place is stored at (0, 0) and should be backfilled
    /// once geocoding succeeds (query: `MATCH (p:Place {geocoded: false})`).
    pub geocoded: bool,
    pub review_count: i64,
    pub avg_rating: f64,
    pub tag_count: i64,
    pub photo_count: i64,
    pub indexed_at: i64,
}

impl PlaceDetails {
    pub fn new(osm_type: &str, osm_id: i64, lat: f64, lon: f64, geocoded: bool) -> Self {
        Self {
            osm_canonical: format!("{osm_type}/{osm_id}"),
            osm_type: osm_type.to_string(),
            osm_id,
            lat,
            lon,
            geocoded,
            review_count: 0,
            avg_rating: 0.0,
            tag_count: 0,
            photo_count: 0,
            indexed_at: Utc::now().timestamp_millis(),
        }
    }

    /// Resolve OSM coordinates via Nominatim (rate-limited to 1 req/s).
    ///
    /// `osm_url` is an OpenStreetMap URL like `https://www.openstreetmap.org/node/123`.
    /// On failure the place is stored at (0, 0) with `geocoded: false` so it
    /// can be identified and backfilled later.
    pub async fn from_osm_url(osm_url: &str) -> Self {
        let (osm_type, osm_id) = parse_osm_url(osm_url);

        match resolve_osm_coords(&osm_type, osm_id).await {
            Some((lat, lon)) => Self::new(&osm_type, osm_id, lat, lon, true),
            None => {
                warn!(
                    "Could not resolve coordinates for {osm_type}/{osm_id}, \
                     storing at (0,0) with geocoded=false for later backfill"
                );
                Self::new(&osm_type, osm_id, 0.0, 0.0, false)
            }
        }
    }
}

/// Extract `"node/123"` from `"https://www.openstreetmap.org/node/123"`.
/// The URL is already validated by mapky-app-specs.
pub fn osm_canonical_from_url(url: &str) -> String {
    url.strip_prefix("https://www.openstreetmap.org/")
        .unwrap_or(url)
        .to_string()
}

/// Parse an OSM URL into `(osm_type, osm_id)`.
/// The URL is already validated by mapky-app-specs.
pub fn parse_osm_url(url: &str) -> (String, i64) {
    let canonical = osm_canonical_from_url(url);
    let (osm_type, osm_id_str) = canonical.split_once('/').expect("validated OSM URL");
    (osm_type.to_string(), osm_id_str.parse().expect("validated OSM ID"))
}

#[derive(Debug, Deserialize)]
struct NominatimResult {
    lat: String,
    lon: String,
}

/// Fetch coordinates from Nominatim, blocking behind the global rate limiter.
async fn resolve_osm_coords(osm_type: &str, osm_id: i64) -> Option<(f64, f64)> {
    let type_char = match osm_type {
        "node" => 'N',
        "way" => 'W',
        "relation" => 'R',
        _ => return None,
    };

    // Enforce the 1 req/s limit. All callers queue here; each holds the lock
    // only long enough to update the timestamp, so no request is skipped.
    {
        let mut last = nominatim_rate_limiter().lock().await;
        let elapsed = last.elapsed();
        if elapsed < NOMINATIM_MIN_INTERVAL {
            tokio::time::sleep(NOMINATIM_MIN_INTERVAL - elapsed).await;
        }
        *last = Instant::now();
    }

    let url = format!("{NOMINATIM_URL}/lookup?osm_ids={type_char}{osm_id}&format=json");
    let results: Vec<NominatimResult> = nominatim_client()
        .get(&url)
        .send()
        .await
        .ok()?
        .json()
        .await
        .ok()?;

    let first = results.into_iter().next()?;
    let lat = first.lat.parse::<f64>().ok()?;
    let lon = first.lon.parse::<f64>().ok()?;
    Some((lat, lon))
}
