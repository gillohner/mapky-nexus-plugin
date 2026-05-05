//! Cached Valhalla routing proxy.
//!
//! `POST /v0/mapky/routing/valhalla` accepts the same JSON body the
//! frontend used to send to public Valhalla, forwards it to the
//! configured upstream, and caches the response in Redis keyed by
//! a content hash of the request.
//!
//! Why cache: identical waypoint/costing pairs come up often (popular
//! routes between cities, tag-aligned trail segments). With a server-
//! side cache, the second user planning Bahnhof Bern → Reichenbachfälle
//! gets an instant snap; without one, every user pays Valhalla's full
//! latency.
//!
//! Why not cache: the upstream rate limit response (429) is a transient
//! signal; we surface it to the caller and never persist it.
//!
//! ## Configuration (environment variables)
//!
//! | Variable | Default | Effect |
//! |---|---|---|
//! | `MAPKY_VALHALLA_URL` | `https://valhalla1.openstreetmap.de/route` | Upstream `/route` endpoint. Point at a self-hosted Valhalla in production. |
//! | `MAPKY_VALHALLA_CACHE_TTL_SECS` | `86400` (24 h) | TTL for cached snaps. The shape of the road network changes slowly. |
//! | `MAPKY_VALHALLA_TIMEOUT_SECS` | `30` | HTTP timeout for the upstream call. |

use std::sync::OnceLock;
use std::time::Duration;

use deadpool_redis::redis::AsyncCommands;
use nexus_common::db::get_redis_conn;
use reqwest::Client;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tracing::warn;

const REDIS_KEY_PREFIX: &str = "mapky:routing:v1:valhalla:";
const DEFAULT_VALHALLA_URL: &str = "https://valhalla1.openstreetmap.de/route";
const DEFAULT_CACHE_TTL_SECS: u64 = 24 * 60 * 60;
const DEFAULT_TIMEOUT_SECS: u64 = 30;

#[derive(Debug, Clone)]
struct RoutingConfig {
    upstream_url: String,
    cache_ttl_secs: u64,
    timeout: Duration,
}

fn config() -> &'static RoutingConfig {
    static CFG: OnceLock<RoutingConfig> = OnceLock::new();
    CFG.get_or_init(|| {
        let upstream_url = std::env::var("MAPKY_VALHALLA_URL")
            .unwrap_or_else(|_| DEFAULT_VALHALLA_URL.to_string());
        let cache_ttl_secs = std::env::var("MAPKY_VALHALLA_CACHE_TTL_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(DEFAULT_CACHE_TTL_SECS);
        let timeout_secs = std::env::var("MAPKY_VALHALLA_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(DEFAULT_TIMEOUT_SECS);
        RoutingConfig {
            upstream_url,
            cache_ttl_secs,
            timeout: Duration::from_secs(timeout_secs),
        }
    })
}

fn http_client() -> &'static Client {
    static CLIENT: OnceLock<Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        Client::builder()
            .user_agent("mapky-nexus-plugin/0.1 (+https://github.com/gillohner/mapky)")
            .timeout(config().timeout)
            .build()
            .expect("failed to build Valhalla HTTP client")
    })
}

/// Outcome of a `route()` call. We pass the upstream HTTP status all
/// the way through so the API handler can map 429 → friendly message
/// (matching the previous frontend behavior).
#[derive(Debug)]
pub enum RouteOutcome {
    /// Upstream returned 200 OK. Body is the JSON response (cached
    /// when this branch fires fresh; cache-hit also returns this).
    Ok(Value),
    /// Upstream returned a non-2xx status. Body is the (possibly
    /// non-JSON) error text from upstream so the caller can forward
    /// it to the client.
    Upstream { status: u16, body: String },
    /// Network error / timeout reaching upstream. No body to forward.
    Network(String),
}

/// Forward a Valhalla `/route` request through the cache.
///
/// `body` is the parsed JSON request — kept as `serde_json::Value` so
/// the canonical-JSON cache key generation is deterministic regardless
/// of frontend key ordering.
pub async fn route(body: Value) -> RouteOutcome {
    let key = cache_key(&body);

    if let Some(hit) = read_cache(&key).await {
        return RouteOutcome::Ok(hit);
    }

    let cfg = config();
    let response = match http_client()
        .post(&cfg.upstream_url)
        .json(&body)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            warn!("Valhalla request failed: {e}");
            return RouteOutcome::Network(e.to_string());
        }
    };

    let status = response.status();
    if !status.is_success() {
        let text = response.text().await.unwrap_or_default();
        return RouteOutcome::Upstream {
            status: status.as_u16(),
            body: text,
        };
    }

    let parsed: Value = match response.json().await {
        Ok(v) => v,
        Err(e) => {
            warn!("Valhalla response parse failed: {e}");
            return RouteOutcome::Network(e.to_string());
        }
    };

    if let Ok(json) = serde_json::to_string(&parsed) {
        if let Ok(mut conn) = get_redis_conn().await {
            let _: Result<(), _> = conn.set_ex(&key, json, cfg.cache_ttl_secs).await;
        }
    }

    RouteOutcome::Ok(parsed)
}

/// Build a cache key that's stable across processes: SHA-256 of the
/// canonicalized JSON body, hex-encoded. Two semantically identical
/// requests with different key ordering hash to the same slot.
fn cache_key(body: &Value) -> String {
    let canonical = canonical_json(body);
    let mut hasher = Sha256::new();
    hasher.update(canonical.as_bytes());
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(REDIS_KEY_PREFIX.len() + 64);
    hex.push_str(REDIS_KEY_PREFIX);
    for b in digest {
        use std::fmt::Write;
        let _ = write!(&mut hex, "{b:02x}");
    }
    hex
}

/// Render a `serde_json::Value` with object keys sorted, so the same
/// logical request always serializes to the same string.
fn canonical_json(v: &Value) -> String {
    match v {
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => v.to_string(),
        Value::Array(arr) => {
            let mut out = String::from("[");
            for (i, item) in arr.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&canonical_json(item));
            }
            out.push(']');
            out
        }
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let mut out = String::from("{");
            for (i, k) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                let key_json = serde_json::to_string(k).unwrap_or_else(|_| String::from("\"\""));
                out.push_str(&key_json);
                out.push(':');
                out.push_str(&canonical_json(&map[*k]));
            }
            out.push('}');
            out
        }
    }
}

async fn read_cache(key: &str) -> Option<Value> {
    let mut conn = get_redis_conn().await.ok()?;
    let raw: Option<String> = conn.get(key).await.ok()?;
    serde_json::from_str(&raw?).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn canonical_json_sorts_object_keys() {
        let a = json!({ "b": 1, "a": 2 });
        let b = json!({ "a": 2, "b": 1 });
        assert_eq!(canonical_json(&a), canonical_json(&b));
    }

    #[test]
    fn canonical_json_recurses_into_arrays_and_objects() {
        let a = json!({"locations": [{"lat": 1.0, "lon": 2.0}, {"lon": 4.0, "lat": 3.0}]});
        let expected = r#"{"locations":[{"lat":1.0,"lon":2.0},{"lat":3.0,"lon":4.0}]}"#;
        assert_eq!(canonical_json(&a), expected);
    }

    #[test]
    fn cache_key_stable_across_key_orders() {
        let a = json!({"costing": "auto", "locations": []});
        let b = json!({"locations": [], "costing": "auto"});
        assert_eq!(cache_key(&a), cache_key(&b));
    }
}
