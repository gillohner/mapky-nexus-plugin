//! Sequence model — a capture session grouping multiple `:MapkyAppGeoCapture`
//! nodes under a `:MapkyAppSequence` node in Neo4j.

use chrono::Utc;
use mapky_app_specs::MapkyAppSequence;
use serde::{Deserialize, Serialize};

use crate::models::tag::PostTagDetails;

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct SequenceDetails {
    pub id: String, // compound: "user_id:sequence_id"
    pub author_id: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub kind: String,
    pub captured_at_start: i64,
    pub captured_at_end: i64,
    pub capture_count: i64,
    pub min_lat: Option<f64>,
    pub min_lon: Option<f64>,
    pub max_lat: Option<f64>,
    pub max_lon: Option<f64>,
    pub device: Option<String>,
    pub indexed_at: i64,
    /// Tags targeting this sequence, aggregated by label. Only populated by
    /// single-item detail endpoints.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<PostTagDetails>>,
}

impl SequenceDetails {
    pub fn from_mapky_sequence(
        sequence: &MapkyAppSequence,
        user_id: &str,
        sequence_id: &str,
    ) -> Self {
        let (min_lat, min_lon, max_lat, max_lon) = match &sequence.bbox {
            Some(b) => (
                Some(b.min_lat),
                Some(b.min_lon),
                Some(b.max_lat),
                Some(b.max_lon),
            ),
            None => (None, None, None, None),
        };

        Self {
            id: format!("{user_id}:{sequence_id}"),
            author_id: user_id.to_string(),
            name: sequence.name.clone(),
            description: sequence.description.clone(),
            kind: serde_json::to_value(&sequence.kind)
                .ok()
                .and_then(|v| v.as_str().map(String::from))
                .unwrap_or_default(),
            captured_at_start: sequence.captured_at_start,
            captured_at_end: sequence.captured_at_end,
            capture_count: sequence.capture_count as i64,
            min_lat,
            min_lon,
            max_lat,
            max_lon,
            device: sequence.device.clone(),
            indexed_at: Utc::now().timestamp_millis(),
            tags: None,
        }
    }
}
