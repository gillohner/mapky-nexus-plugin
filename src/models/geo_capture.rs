//! GeoCapture model — a geo-located media capture stored as a Neo4j
//! `:MapkyAppGeoCapture` node with a spatial point index.

use mapky_app_specs::MapkyAppGeoCapture;
use serde::{Deserialize, Serialize};

use chrono::Utc;

use crate::models::tag::PostTagDetails;

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct GeoCaptureDetails {
    pub id: String, // compound: "user_id:capture_id"
    pub author_id: String,
    pub file_uri: String,
    pub kind: String,
    pub lat: f64,
    pub lon: f64,
    pub ele: Option<f64>,
    pub heading: Option<f64>,
    pub pitch: Option<f64>,
    pub fov: Option<f64>,
    pub caption: Option<String>,
    pub sequence_uri: Option<String>,
    pub sequence_index: Option<i64>,
    /// UNIX microseconds — moment the media was captured. Distinct from `indexed_at`.
    pub captured_at: Option<i64>,
    pub indexed_at: i64,
    /// Tags targeting this capture, aggregated by label. Only populated by
    /// single-item detail endpoints; list endpoints leave this `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<PostTagDetails>>,
}

impl GeoCaptureDetails {
    pub fn from_mapky_geo_capture(
        capture: &MapkyAppGeoCapture,
        user_id: &str,
        capture_id: &str,
    ) -> Self {
        Self {
            id: format!("{user_id}:{capture_id}"),
            author_id: user_id.to_string(),
            file_uri: capture.file_uri.clone(),
            kind: serde_json::to_value(&capture.kind)
                .ok()
                .and_then(|v| v.as_str().map(String::from))
                .unwrap_or_default(),
            lat: capture.lat,
            lon: capture.lon,
            ele: capture.ele,
            heading: capture.heading,
            pitch: capture.pitch,
            fov: capture.fov,
            caption: capture.caption.clone(),
            sequence_uri: capture.sequence_uri.clone(),
            sequence_index: capture.sequence_index.map(|i| i as i64),
            captured_at: capture.captured_at,
            indexed_at: Utc::now().timestamp_millis(),
            tags: None,
        }
    }
}
