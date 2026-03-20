use serde::{Deserialize, Serialize};

/// Per-label tag summary for a MapkyPost.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct PostTagDetails {
    pub label: String,
    pub taggers: Vec<String>,
    pub taggers_count: usize,
}
