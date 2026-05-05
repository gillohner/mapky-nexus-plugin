//! Review model — a rating-mandatory review of an OSM place.

use chrono::Utc;
use mapky_app_specs::MapkyAppReview;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ReviewDetails {
    pub id: String,
    pub author_id: String,
    pub osm_canonical: String,
    pub content: Option<String>,
    pub rating: u8,
    pub attachments: Vec<String>,
    pub indexed_at: i64,
}

impl ReviewDetails {
    pub fn from_mapky_review(review: &MapkyAppReview, user_id: &str, review_id: &str) -> Self {
        Self {
            id: format!("{user_id}:{review_id}"),
            author_id: user_id.to_string(),
            osm_canonical: crate::models::place::osm_canonical_from_url(&review.place),
            content: review.content.clone(),
            rating: review.rating,
            attachments: review.attachments.clone().unwrap_or_default(),
            indexed_at: Utc::now().timestamp_millis(),
        }
    }
}
