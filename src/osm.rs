//! OSM auxiliary services — Nominatim lookup with Redis caching.
//!
//! The `/v0/mapky/osm/lookup` endpoint sits in front of the public
//! (or self-hosted) Nominatim instance so the browser never has to
//! call it directly. Without this, every viewport pan would fan out
//! one `/lookup` per visible POI per user and trip Nominatim's
//! per-IP rate limit.
//!
//! ## Behavior
//!
//! - **Cache**: Redis, key `mapky:osm:lookup:{N|W|R}{id}`, configurable
//!   TTL (default 30 d). Cached entries — including empty ones for IDs
//!   Nominatim couldn't resolve — short-circuit future requests.
//! - **Rate limit**: a shared `Mutex<Instant>` enforces a configurable
//!   minimum interval between upstream calls (default 1000 ms, matching
//!   public Nominatim's policy). The same gate is shared with
//!   `models/place.rs::resolve_osm_coords`, so event-time geocoding
//!   and API-time lookup never double up.
//! - **Batching**: up to 50 `osm_ids` per upstream request (Nominatim's
//!   `/lookup` cap), so a 30-place viewport opens with one round-trip.
//!   Configurable via `MAPKY_OSM_BATCH_SIZE` if you self-host with a
//!   different limit.
//!
//! ## Configuration (environment variables)
//!
//! | Variable | Default | Effect |
//! |---|---|---|
//! | `MAPKY_NOMINATIM_URL` | `https://nominatim.openstreetmap.org` | Upstream base URL — point at a self-hosted Nominatim mirror in production. |
//! | `MAPKY_OVERPASS_URL` | `https://overpass-api.de/api/interpreter` | Overpass interpreter URL — used as a fallback when Nominatim returns empty for an OSM ref that does exist (e.g. a recently-edited unnamed building with full `addr:*` tags). Point at a self-hosted Overpass for production. |
//! | `MAPKY_NOMINATIM_MIN_INTERVAL_MS` | `1000` | Floor on inter-request spacing for both Nominatim and the Overpass fallback. Self-hosted instances typically allow much faster traffic; lower for higher throughput. |
//! | `MAPKY_NOMINATIM_USER_AGENT` | `mapky-nexus-plugin/0.1 (+repo)` | Sent on every upstream call. Operators sometimes use this to identify offending clients. |
//! | `MAPKY_OSM_CACHE_TTL_SECS` | `2592000` (30 d) | TTL for cached **resolved** lookups. Lower if you expect frequent OSM renames; higher for stable datasets. |
//! | `MAPKY_OSM_EMPTY_CACHE_TTL_SECS` | `21600` (6 h) | TTL for cached **empty** placeholders — refs Nominatim and Overpass both returned no data for. Short by design so a recent OSM edit catches up quickly. |
//! | `MAPKY_OSM_BATCH_SIZE` | `50` | IDs per upstream batch. Public Nominatim caps at 50; self-hosted may allow more. |
//!
//! All values are read once on first call (via `OnceLock`); restart
//! `nexusd` for changes to take effect.

use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use deadpool_redis::redis::AsyncCommands;
use nexus_common::db::get_redis_conn;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::warn;

/// Cache key prefix. Bump the version segment (`v2` → `v3`) when the
/// cached payload shape changes, OR when an upstream behavior change
/// could leave bad placeholders in Redis (e.g. the failure-cache bug
/// fixed alongside this version). Old keys time out naturally.
const REDIS_KEY_PREFIX: &str = "mapky:osm:v2:lookup:";
const DEFAULT_NOMINATIM_URL: &str = "https://nominatim.openstreetmap.org";
const DEFAULT_OVERPASS_URL: &str = "https://overpass-api.de/api/interpreter";
const DEFAULT_USER_AGENT: &str = "mapky-nexus-plugin/0.1 (+https://github.com/gillohner/mapky)";
const DEFAULT_MIN_INTERVAL_MS: u64 = 1000;
const DEFAULT_CACHE_TTL_SECS: u64 = 30 * 24 * 60 * 60;
/// Empty placeholders cache for a much shorter window than hits — they
/// represent "Nominatim doesn't know this ID yet". Six hours self-heals
/// fast when Nominatim's index catches up to a recent OSM edit, while
/// still suppressing pathological "ID truly doesn't exist" loops.
const DEFAULT_EMPTY_CACHE_TTL_SECS: u64 = 6 * 60 * 60;
const DEFAULT_BATCH_SIZE: usize = 50;

/// Resolved configuration, populated lazily from env on first use.
#[derive(Debug, Clone)]
struct OsmConfig {
    nominatim_url: String,
    overpass_url: String,
    user_agent: String,
    min_interval: Duration,
    cache_ttl_secs: u64,
    empty_cache_ttl_secs: u64,
    batch_size: usize,
}

fn config() -> &'static OsmConfig {
    static CFG: OnceLock<OsmConfig> = OnceLock::new();
    CFG.get_or_init(|| {
        let nominatim_url = std::env::var("MAPKY_NOMINATIM_URL")
            .unwrap_or_else(|_| DEFAULT_NOMINATIM_URL.to_string())
            .trim_end_matches('/')
            .to_string();
        let overpass_url = std::env::var("MAPKY_OVERPASS_URL")
            .unwrap_or_else(|_| DEFAULT_OVERPASS_URL.to_string());
        let user_agent = std::env::var("MAPKY_NOMINATIM_USER_AGENT")
            .unwrap_or_else(|_| DEFAULT_USER_AGENT.to_string());
        let min_interval_ms = std::env::var("MAPKY_NOMINATIM_MIN_INTERVAL_MS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(DEFAULT_MIN_INTERVAL_MS);
        let cache_ttl_secs = std::env::var("MAPKY_OSM_CACHE_TTL_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(DEFAULT_CACHE_TTL_SECS);
        let empty_cache_ttl_secs = std::env::var("MAPKY_OSM_EMPTY_CACHE_TTL_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(DEFAULT_EMPTY_CACHE_TTL_SECS);
        let batch_size = std::env::var("MAPKY_OSM_BATCH_SIZE")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(DEFAULT_BATCH_SIZE);
        OsmConfig {
            nominatim_url,
            overpass_url,
            user_agent,
            min_interval: Duration::from_millis(min_interval_ms),
            cache_ttl_secs,
            empty_cache_ttl_secs,
            batch_size,
        }
    })
}

/// Shared HTTP client. Single user-agent identifies the plugin to
/// Nominatim's operators and reuses the connection pool.
pub(crate) fn nominatim_client() -> &'static Client {
    static CLIENT: OnceLock<Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        Client::builder()
            .user_agent(config().user_agent.clone())
            .timeout(Duration::from_secs(10))
            .build()
            .expect("failed to build Nominatim HTTP client")
    })
}

/// Global rate-limit gate. Shared with `place.rs::resolve_osm_coords`
/// so event-time ingestion and API-time lookups never exceed the
/// configured `MAPKY_NOMINATIM_MIN_INTERVAL_MS`.
pub(crate) fn nominatim_rate_limiter() -> &'static Mutex<Instant> {
    static LAST_REQUEST: OnceLock<Mutex<Instant>> = OnceLock::new();
    LAST_REQUEST.get_or_init(|| Mutex::new(Instant::now() - config().min_interval))
}

/// Block until the configured min-interval has elapsed since the last
/// call. Holding the lock through the wait serialises concurrent
/// callers — every request waits its turn, none are dropped.
pub(crate) async fn wait_for_rate_limit() {
    let interval = config().min_interval;
    let mut last = nominatim_rate_limiter().lock().await;
    let elapsed = last.elapsed();
    if elapsed < interval {
        tokio::time::sleep(interval - elapsed).await;
    }
    *last = Instant::now();
}

/// Configured upstream Nominatim base URL (`MAPKY_NOMINATIM_URL` or
/// the public default). Exposed for callers that build their own
/// request URLs (e.g. `models/place.rs::resolve_osm_coords`).
pub(crate) fn nominatim_url() -> &'static str {
    &config().nominatim_url
}

/// Public-facing lookup result. Mirrors the frontend's
/// `NominatimResult` so the API can pass through unchanged.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct NominatimLookup {
    pub osm_type: Option<String>,
    pub osm_id: Option<i64>,
    pub name: Option<String>,
    pub display_name: String,
    #[serde(rename = "type")]
    pub kind: Option<String>,
    pub category: Option<String>,
    #[serde(default)]
    pub address: HashMap<String, String>,
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    /// Raw OSM tags beyond the structured fields — `currency:XBT`,
    /// `payment:*`, hours, etc. Frontend reads `BitcoinAcceptance`
    /// signals straight off this.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub extratags: HashMap<String, String>,
}

/// Raw response shape from Nominatim. Slightly looser than the
/// public version because Nominatim sends string lat/lon and may
/// omit fields entirely.
#[derive(Debug, Deserialize)]
struct NominatimRaw {
    osm_type: Option<String>,
    osm_id: Option<i64>,
    name: Option<String>,
    display_name: Option<String>,
    #[serde(rename = "type")]
    kind: Option<String>,
    category: Option<String>,
    class: Option<String>,
    #[serde(default)]
    address: HashMap<String, String>,
    lat: Option<String>,
    lon: Option<String>,
    #[serde(default)]
    extratags: HashMap<String, String>,
}

impl NominatimRaw {
    fn into_lookup(self) -> NominatimLookup {
        NominatimLookup {
            osm_type: self.osm_type,
            osm_id: self.osm_id,
            name: self.name,
            display_name: self.display_name.unwrap_or_default(),
            kind: self.kind,
            // Nominatim returns `category` on /search and `class` on
            // /lookup — accept either; the frontend doesn't care
            // which one it came from.
            category: self.category.or(self.class),
            address: self.address,
            lat: self.lat.and_then(|s| s.parse().ok()),
            lon: self.lon.and_then(|s| s.parse().ok()),
            extratags: self.extratags,
        }
    }
}

/// Stable Redis key per OSM ref.
fn cache_key(osm_type: &str, osm_id: i64) -> String {
    let prefix = match osm_type {
        "node" => 'N',
        "way" => 'W',
        "relation" => 'R',
        _ => '?',
    };
    format!("{REDIS_KEY_PREFIX}{prefix}{osm_id}")
}

/// Seed the lookup cache with a pre-built `NominatimLookup` — used by
/// the BTCMap sync to populate `mapky:osm:v2:lookup:` directly from
/// the dump's tag map, so place panels for BTC POIs never have to
/// queue behind the 1 req/s Nominatim gate. Uses the regular hit TTL.
///
/// Fire-and-forget: returns `true` only when the write reached Redis,
/// but a Redis hiccup just means the next `/osm/lookup` will fetch
/// upstream (correct behavior, no stale data persisted).
pub async fn seed_lookup_cache(osm_type: &str, osm_id: i64, lookup: &NominatimLookup) -> bool {
    let cfg = config();
    let key = cache_key(osm_type, osm_id);
    let Ok(json) = serde_json::to_string(lookup) else {
        return false;
    };
    let Ok(mut conn) = get_redis_conn().await else {
        return false;
    };
    conn.set_ex::<_, _, ()>(&key, json, cfg.cache_ttl_secs)
        .await
        .is_ok()
}

fn type_char(osm_type: &str) -> Option<char> {
    match osm_type {
        "node" => Some('N'),
        "way" => Some('W'),
        "relation" => Some('R'),
        _ => None,
    }
}

/// Empty placeholder for refs Nominatim couldn't resolve — keeps the
/// response array aligned with the input refs and short-circuits
/// future requests for the same ID until the (shorter) empty TTL.
fn empty_lookup(osm_type: &str, osm_id: i64) -> NominatimLookup {
    NominatimLookup {
        osm_type: Some(osm_type.to_string()),
        osm_id: Some(osm_id),
        name: None,
        display_name: String::new(),
        kind: None,
        category: None,
        address: HashMap::new(),
        lat: None,
        lon: None,
        extratags: HashMap::new(),
    }
}

/// True when a lookup carries no useful identification — happens when
/// Nominatim returned 200 OK without an entry for the requested ID
/// (recently-added OSM element, intentionally excluded from the index,
/// etc.). Drives the shorter cache TTL + the Overpass fallback below.
fn is_empty(l: &NominatimLookup) -> bool {
    l.name.is_none()
        && l.display_name.is_empty()
        && l.address.is_empty()
        && l.lat.is_none()
        && l.lon.is_none()
}

/// Raw Overpass response. We only care about `id`, optional `center`
/// (returned by `out center`), and the OSM tags map.
#[derive(Debug, Deserialize)]
struct OverpassElement {
    #[serde(rename = "type")]
    elem_type: String,
    id: i64,
    #[serde(default)]
    lat: Option<f64>,
    #[serde(default)]
    lon: Option<f64>,
    #[serde(default)]
    center: Option<OverpassCenter>,
    #[serde(default)]
    tags: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct OverpassCenter {
    lat: f64,
    lon: f64,
}

#[derive(Debug, Default, Deserialize)]
struct OverpassResponse {
    #[serde(default)]
    elements: Vec<OverpassElement>,
}

/// Build a Nominatim-shaped result from Overpass tags. Used as a
/// fallback when Nominatim's index doesn't have a recently-edited OSM
/// element but Overpass does. Address fields land in the `address`
/// map under Nominatim's keys (`road`, `house_number`, …) so the
/// frontend's existing address-name builder works unchanged.
fn lookup_from_overpass(elem: OverpassElement) -> NominatimLookup {
    let mut address: HashMap<String, String> = HashMap::new();
    if let Some(v) = elem.tags.get("addr:housenumber") {
        address.insert("house_number".into(), v.clone());
    }
    if let Some(v) = elem.tags.get("addr:street") {
        address.insert("road".into(), v.clone());
    }
    if let Some(v) = elem.tags.get("addr:city") {
        address.insert("city".into(), v.clone());
    }
    if let Some(v) = elem.tags.get("addr:postcode") {
        address.insert("postcode".into(), v.clone());
    }
    if let Some(v) = elem.tags.get("addr:country") {
        address.insert("country".into(), v.clone());
    }
    if let Some(v) = elem.tags.get("addr:suburb") {
        address.insert("suburb".into(), v.clone());
    }

    // Choose the most descriptive `kind` Overpass tags imply. Building
    // tag wins (matches Nominatim's `type=building`), then amenity, etc.
    let kind = elem
        .tags
        .get("amenity")
        .or_else(|| elem.tags.get("shop"))
        .or_else(|| elem.tags.get("tourism"))
        .or_else(|| elem.tags.get("leisure"))
        .or_else(|| elem.tags.get("building"))
        .cloned();

    // Synthesize a display_name from address tags for consumers that
    // read display_name's first segment. Mirrors how Nominatim formats
    // the same fields.
    let display_name = match (
        elem.tags.get("addr:housenumber"),
        elem.tags.get("addr:street"),
        elem.tags.get("addr:postcode"),
        elem.tags.get("addr:city"),
    ) {
        (Some(num), Some(street), pc, city) => {
            let mut s = format!("{street} {num}");
            if let (Some(pc), Some(city)) = (pc, city) {
                s.push_str(&format!(", {pc} {city}"));
            } else if let Some(city) = city {
                s.push_str(&format!(", {city}"));
            }
            s
        }
        (None, Some(street), pc, city) => {
            let mut s = street.clone();
            if let (Some(pc), Some(city)) = (pc, city) {
                s.push_str(&format!(", {pc} {city}"));
            } else if let Some(city) = city {
                s.push_str(&format!(", {city}"));
            }
            s
        }
        _ => elem.tags.get("name").cloned().unwrap_or_default(),
    };

    let (lat, lon) = match (elem.lat, elem.lon, elem.center) {
        (Some(la), Some(lo), _) => (Some(la), Some(lo)),
        (_, _, Some(c)) => (Some(c.lat), Some(c.lon)),
        _ => (None, None),
    };

    NominatimLookup {
        osm_type: Some(elem.elem_type),
        osm_id: Some(elem.id),
        name: elem.tags.get("name").cloned(),
        display_name,
        kind,
        // Overpass doesn't return Nominatim's `category` — mirror the
        // `kind` so the frontend's category icon still resolves.
        category: elem
            .tags
            .get("amenity")
            .or_else(|| elem.tags.get("shop"))
            .or_else(|| elem.tags.get("tourism"))
            .or_else(|| elem.tags.get("leisure"))
            .or_else(|| elem.tags.get("building"))
            .map(|_| {
                if elem.tags.contains_key("amenity") {
                    "amenity".to_string()
                } else if elem.tags.contains_key("shop") {
                    "shop".to_string()
                } else if elem.tags.contains_key("tourism") {
                    "tourism".to_string()
                } else if elem.tags.contains_key("leisure") {
                    "leisure".to_string()
                } else {
                    "building".to_string()
                }
            }),
        address,
        lat,
        lon,
        // Pass-through every tag — frontend reads `currency:XBT`,
        // `payment:*`, `opening_hours`, etc. straight off this map.
        extratags: elem.tags,
    }
}

/// Best-effort Overpass fetch for a list of (osm_type, osm_id) refs.
/// Returns a partial `osm_type:osm_id → lookup` map. Failures are
/// logged and silently produce an empty map — callers fall back to
/// the Nominatim placeholder. Same rate-limit gate as Nominatim so
/// fallbacks don't squeeze the public Overpass instance any harder
/// than the primary path.
async fn overpass_fallback(refs: &[(String, i64)]) -> HashMap<String, NominatimLookup> {
    let mut out: HashMap<String, NominatimLookup> = HashMap::new();
    if refs.is_empty() {
        return out;
    }

    // Group by element type so one Overpass query can pull all three.
    let mut nodes: Vec<i64> = Vec::new();
    let mut ways: Vec<i64> = Vec::new();
    let mut relations: Vec<i64> = Vec::new();
    for (t, id) in refs {
        match t.as_str() {
            "node" => nodes.push(*id),
            "way" => ways.push(*id),
            "relation" => relations.push(*id),
            _ => {}
        }
    }

    let mut parts: Vec<String> = Vec::new();
    if !nodes.is_empty() {
        parts.push(format!(
            "node(id:{});",
            nodes
                .iter()
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if !ways.is_empty() {
        parts.push(format!(
            "way(id:{});",
            ways.iter()
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if !relations.is_empty() {
        parts.push(format!(
            "relation(id:{});",
            relations
                .iter()
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if parts.is_empty() {
        return out;
    }
    let query = format!(
        "[out:json][timeout:25];({});out tags center;",
        parts.join(""),
    );

    let cfg = config();
    wait_for_rate_limit().await;

    let resp = nominatim_client()
        .post(&cfg.overpass_url)
        .body(format!("data={}", urlencoding::encode(&query)))
        .header(
            reqwest::header::CONTENT_TYPE,
            "application/x-www-form-urlencoded",
        )
        .send()
        .await;

    let parsed: OverpassResponse = match resp {
        Ok(r) if r.status().is_success() => r.json().await.unwrap_or_default(),
        Ok(r) => {
            warn!(status = ?r.status(), "Overpass fallback non-2xx");
            return out;
        }
        Err(e) => {
            warn!(error = ?e, "Overpass fallback failed");
            return out;
        }
    };

    for elem in parsed.elements {
        let key = cache_key(&elem.elem_type, elem.id);
        out.insert(key, lookup_from_overpass(elem));
    }
    out
}

/// Cached, batched OSM lookup. `refs` is `(osm_type, osm_id)` pairs;
/// the returned vec has one entry per input in the same order, with
/// `display_name == ""` indicating Nominatim didn't return that ref.
///
/// Lookup order:
///  1. Redis (`mapky:osm:lookup:{prefix}{id}`)
///  2. Public Nominatim, batched up to 50 per request, gated by the
///     1 req/s mutex
///  3. Cache miss results back into Redis with a 30 d TTL
pub async fn batch_lookup_cached(refs: &[(String, i64)]) -> Vec<NominatimLookup> {
    if refs.is_empty() {
        return Vec::new();
    }

    let mut out: Vec<Option<NominatimLookup>> = vec![None; refs.len()];

    // ── 1. Redis cache pass ──────────────────────────────────────
    // MGET in one round-trip. Index alignment relies on the keys
    // being in the same order as `refs`.
    let keys: Vec<String> = refs.iter().map(|(t, id)| cache_key(t, *id)).collect();
    if let Ok(mut conn) = get_redis_conn().await {
        let raw: Result<Vec<Option<String>>, _> = conn.mget(&keys).await;
        if let Ok(values) = raw {
            for (i, v) in values.into_iter().enumerate() {
                if let Some(json) = v {
                    if let Ok(parsed) = serde_json::from_str::<NominatimLookup>(&json) {
                        out[i] = Some(parsed);
                    }
                }
            }
        }
    }

    // ── 2. Collect misses + dedupe ──────────────────────────────
    // Same ref might appear twice in the input; we resolve once and
    // copy the result back to every position.
    let mut todo: Vec<usize> = Vec::new();
    let mut seen_keys: HashMap<String, usize> = HashMap::new();
    for (i, (osm_type, osm_id)) in refs.iter().enumerate() {
        if out[i].is_some() {
            continue;
        }
        let dedup_key = cache_key(osm_type, *osm_id);
        if let Some(&first) = seen_keys.get(&dedup_key) {
            // Will copy from `first` after fetch.
            todo.push(i);
            // Dedup tracking already covers this ref — leave as is.
            let _ = first;
        } else {
            seen_keys.insert(dedup_key, i);
            todo.push(i);
        }
    }
    if todo.is_empty() {
        // Everything cached — short-circuit before opening a Redis
        // pipeline for nothing.
        return out
            .into_iter()
            .enumerate()
            .map(|(i, opt)| opt.unwrap_or_else(|| empty_lookup(&refs[i].0, refs[i].1)))
            .collect();
    }

    // Build the unique fetch list (dedup on `prefix{id}`).
    let mut to_fetch: Vec<(usize, char, i64)> = Vec::new();
    let mut fetched_seen: HashMap<String, ()> = HashMap::new();
    for &i in &todo {
        let (osm_type, osm_id) = &refs[i];
        let Some(prefix) = type_char(osm_type) else {
            continue;
        };
        let key = format!("{prefix}{osm_id}");
        if fetched_seen.contains_key(&key) {
            continue;
        }
        fetched_seen.insert(key, ());
        to_fetch.push((i, prefix, *osm_id));
    }

    // ── 3. Batched Nominatim fetches ────────────────────────────
    // Each chunk is one HTTP call gated by the rate limiter. We track
    // which chunks succeeded so we can avoid caching empty results
    // for refs whose chunk failed — a transient 429 used to poison
    // the cache for the full TTL window, making the entire batch
    // permanently empty until manual flush.
    let mut by_ref: HashMap<String, NominatimLookup> = HashMap::new();
    let mut chunk_ok: HashMap<String, bool> = HashMap::new();
    let cfg = config();
    for chunk in to_fetch.chunks(cfg.batch_size) {
        let ids: String = chunk
            .iter()
            .map(|(_, p, id)| format!("{p}{id}"))
            .collect::<Vec<_>>()
            .join(",");

        wait_for_rate_limit().await;

        let url = format!(
            "{}/lookup?osm_ids={ids}&format=json&addressdetails=1&extratags=1",
            cfg.nominatim_url,
        );
        let resp = nominatim_client().get(&url).send().await;
        let (raw, ok) = match resp {
            Ok(r) if r.status().is_success() => (
                r.json::<Vec<NominatimRaw>>().await.unwrap_or_default(),
                true,
            ),
            Ok(r) => {
                warn!(status = ?r.status(), "Nominatim batch lookup non-2xx");
                (Vec::new(), false)
            }
            Err(e) => {
                warn!(error = ?e, "Nominatim batch lookup failed");
                (Vec::new(), false)
            }
        };

        // Mark every ref in this chunk so the cache step below knows
        // whether to write an empty placeholder (Nominatim returned
        // 200 without that ref → genuine miss, cache it) or to skip
        // (upstream errored → don't poison the cache).
        for (_, prefix, id) in chunk {
            chunk_ok.insert(format!("{prefix}{id}"), ok);
        }

        for raw_item in raw {
            let lookup = raw_item.into_lookup();
            if let (Some(t), Some(id)) = (&lookup.osm_type, lookup.osm_id) {
                by_ref.insert(cache_key(t, id), lookup);
            }
        }
    }

    // ── 4. Overpass fallback for refs Nominatim returned empty ──
    // Common case: an OSM way with full `addr:*` tags but no `name`
    // — Nominatim's named-entity index doesn't have it, but Overpass
    // does (it serves raw OSM tags). We synthesize a Nominatim-shaped
    // lookup from those tags so the place still has a usable title
    // ("48 Hirschengraben, Luzern") instead of falling through to
    // "Area #95985723".
    let mut overpass_refs: Vec<(String, i64)> = Vec::new();
    for &i in &todo {
        let (osm_type, osm_id) = &refs[i];
        let prefix_key = type_char(osm_type)
            .map(|c| format!("{c}{osm_id}"))
            .unwrap_or_default();
        if !chunk_ok.get(&prefix_key).copied().unwrap_or(false) {
            continue; // upstream Nominatim failed — don't pile on
        }
        let key = cache_key(osm_type, *osm_id);
        let nom = by_ref.get(&key);
        if nom.map(is_empty).unwrap_or(true) {
            overpass_refs.push((osm_type.clone(), *osm_id));
        }
    }
    if !overpass_refs.is_empty() {
        let overpass_results = overpass_fallback(&overpass_refs).await;
        for (k, v) in overpass_results {
            // Only overwrite empty Nominatim placeholders. Real
            // Nominatim entries (when Nominatim DID know but the
            // ref was a no-tags way) keep their fields.
            let existing_empty = by_ref.get(&k).map(is_empty).unwrap_or(true);
            if existing_empty {
                by_ref.insert(k, v);
            }
        }
    }

    // ── 5. Stitch results back into output + cache misses ───────
    // Only cache when the upstream chunk succeeded. Hits AND empty
    // misses both go in; empty placeholders use the shorter
    // empty-cache TTL so a recent OSM edit Nominatim hasn't indexed
    // yet self-heals quickly. Upstream failures aren't cached at all.
    let mut to_cache: Vec<(String, String, u64)> = Vec::new();
    for &i in &todo {
        let (osm_type, osm_id) = &refs[i];
        let key = cache_key(osm_type, *osm_id);
        let prefix_key = type_char(osm_type)
            .map(|c| format!("{c}{osm_id}"))
            .unwrap_or_default();
        let upstream_ok = chunk_ok.get(&prefix_key).copied().unwrap_or(false);
        let result = by_ref
            .get(&key)
            .cloned()
            .unwrap_or_else(|| empty_lookup(osm_type, *osm_id));
        if upstream_ok {
            if let Ok(json) = serde_json::to_string(&result) {
                let ttl = if is_empty(&result) {
                    cfg.empty_cache_ttl_secs
                } else {
                    cfg.cache_ttl_secs
                };
                to_cache.push((key, json, ttl));
            }
        }
        out[i] = Some(result);
    }

    // ── 6. Write back to Redis. Fire-and-mostly-forget — a Redis
    // outage here means we re-fetch next time, no correctness loss.
    if !to_cache.is_empty() {
        if let Ok(mut conn) = get_redis_conn().await {
            for (key, value, ttl) in to_cache {
                let _: Result<(), _> = conn.set_ex(&key, value, ttl).await;
            }
        }
    }

    out.into_iter()
        .enumerate()
        .map(|(i, opt)| opt.unwrap_or_else(|| empty_lookup(&refs[i].0, refs[i].1)))
        .collect()
}

// ── /search and /reverse — cached free-form queries ─────────────────────

/// Cache key prefix for `/search` results. Versioned alongside the
/// lookup prefix so a payload-shape bump rolls both at once.
const REDIS_SEARCH_PREFIX: &str = "mapky:osm:v2:search:";
/// Cache key prefix for `/reverse` results. Same versioning scheme.
const REDIS_REVERSE_PREFIX: &str = "mapky:osm:v2:reverse:";
/// Search results live shorter than `/lookup`: a place that didn't
/// match yesterday might match today (new tags, new index pass).
/// 24 h is the comfort floor for pure free-text queries.
const SEARCH_CACHE_TTL_SECS: u64 = 24 * 60 * 60;
/// Reverse-geocode by quantized coordinates is effectively immutable
/// (the place at lat/lon doesn't move). Same TTL as `/lookup` hits.
const REVERSE_CACHE_TTL_SECS: u64 = 30 * 24 * 60 * 60;

/// Parameters for the cached `/search` proxy. Mirrors the public
/// Nominatim subset the frontend actually uses — see
/// `mapky-app/src/lib/api/nominatim.ts`.
///
/// Free-text inputs are lower-cased + trimmed when building the
/// cache key so callers don't need to canonicalize.
#[derive(Debug, Clone)]
pub struct SearchParams {
    pub q: String,
    /// `west,north,east,south` — passes straight through to Nominatim.
    /// `None` is a global-scope query.
    pub viewbox: Option<String>,
    /// `bounded=1` restricts results to the viewbox. Falsy = scoped
    /// hint (scoring boost).
    pub bounded: bool,
    /// Default 8 — Nominatim caps responses at 50.
    pub limit: u32,
    /// Drop spatial near-duplicates with the same display name.
    pub dedupe: bool,
    /// Include the `address` map in responses. Set false for the
    /// search-bar autocomplete (smaller payload).
    pub addressdetails: bool,
}

/// Run a `/search` query against the configured Nominatim, caching
/// the resolved list in Redis.
///
/// Reuses the global rate-limit gate so search calls and `/lookup`
/// calls never collide against Nominatim's 1 req/s policy.
pub async fn search_cached(params: &SearchParams) -> Vec<NominatimLookup> {
    let cfg = config();
    let key = build_search_key(params);

    if let Some(cached) = read_cached_list(&key).await {
        return cached;
    }

    wait_for_rate_limit().await;

    let mut request = nominatim_client()
        .get(format!("{}/search", cfg.nominatim_url))
        .query(&[("q", params.q.as_str()), ("format", "json")])
        .query(&[("limit", params.limit.to_string())])
        .query(&[("dedupe", if params.dedupe { "1" } else { "0" })])
        .query(&[(
            "addressdetails",
            if params.addressdetails { "1" } else { "0" },
        )]);
    if let Some(vb) = &params.viewbox {
        request = request.query(&[("viewbox", vb.as_str())]);
        if params.bounded {
            request = request.query(&[("bounded", "1")]);
        }
    }

    let raw: Vec<NominatimRaw> = match request.send().await {
        Ok(resp) => match resp.error_for_status() {
            Ok(ok) => ok.json().await.unwrap_or_default(),
            Err(e) => {
                warn!("Nominatim /search upstream status: {e}");
                // Don't cache upstream errors — they're transient.
                return Vec::new();
            }
        },
        Err(e) => {
            warn!("Nominatim /search request failed: {e}");
            return Vec::new();
        }
    };
    let results: Vec<NominatimLookup> = raw.into_iter().map(NominatimRaw::into_lookup).collect();

    // Cache (including empty results — short TTL keeps pathological
    // "no match yet" loops from hammering Nominatim).
    if let Ok(json) = serde_json::to_string(&results) {
        let ttl = if results.is_empty() {
            cfg.empty_cache_ttl_secs
        } else {
            SEARCH_CACHE_TTL_SECS
        };
        if let Ok(mut conn) = get_redis_conn().await {
            let _: Result<(), _> = conn.set_ex(&key, json, ttl).await;
        }
    }

    results
}

/// Reverse-geocode a single coordinate. Returns `None` when Nominatim
/// has nothing for that point — the empty case is cached short so a
/// recently-named place self-heals.
pub async fn reverse_cached(lat: f64, lon: f64, zoom: u32) -> Option<NominatimLookup> {
    let cfg = config();
    // Quantize to ~1 m precision so every call within a 1 m bin shares
    // the same cache slot — matches the frontend's localStorage key
    // shape (`makeReverseKey`) so the two layers don't fragment.
    let key = format!(
        "{REDIS_REVERSE_PREFIX}{lat:.5},{lon:.5}:z{zoom}",
        lat = lat,
        lon = lon,
        zoom = zoom
    );

    match read_cached_optional(&key).await {
        Some(Some(hit)) => return Some(hit),
        Some(None) => return None,
        None => {}
    }

    wait_for_rate_limit().await;

    let request = nominatim_client()
        .get(format!("{}/reverse", cfg.nominatim_url))
        .query(&[
            ("lat", format!("{lat:.6}")),
            ("lon", format!("{lon:.6}")),
            ("format", "json".to_string()),
            ("zoom", zoom.to_string()),
        ]);

    let raw: NominatimRaw = match request.send().await {
        Ok(resp) => match resp.error_for_status() {
            Ok(ok) => match ok.json().await {
                Ok(parsed) => parsed,
                Err(e) => {
                    warn!("Nominatim /reverse parse failed: {e}");
                    return None;
                }
            },
            Err(e) => {
                warn!("Nominatim /reverse upstream status: {e}");
                return None;
            }
        },
        Err(e) => {
            warn!("Nominatim /reverse request failed: {e}");
            return None;
        }
    };
    let result = raw.into_lookup();
    let empty = is_empty(&result);

    // Cache the result (or the empty marker). `null` JSON = empty.
    let payload = if empty {
        "null".to_string()
    } else {
        match serde_json::to_string(&result) {
            Ok(s) => s,
            Err(_) => return Some(result),
        }
    };
    let ttl = if empty {
        cfg.empty_cache_ttl_secs
    } else {
        REVERSE_CACHE_TTL_SECS
    };
    if let Ok(mut conn) = get_redis_conn().await {
        let _: Result<(), _> = conn.set_ex(&key, payload, ttl).await;
    }

    if empty {
        None
    } else {
        Some(result)
    }
}

/// Build a stable cache key for a `/search` call. The key folds in
/// every parameter that affects the upstream response so two calls
/// with different limits/viewboxes don't collide.
fn build_search_key(p: &SearchParams) -> String {
    let q = p.q.trim().to_lowercase();
    let vb = p.viewbox.as_deref().unwrap_or("");
    format!(
        "{REDIS_SEARCH_PREFIX}q={q}|vb={vb}|b={}|l={}|d={}|a={}",
        if p.bounded { 1 } else { 0 },
        p.limit,
        if p.dedupe { 1 } else { 0 },
        if p.addressdetails { 1 } else { 0 }
    )
}

/// Read a cached `Vec<NominatimLookup>` if present.
async fn read_cached_list(key: &str) -> Option<Vec<NominatimLookup>> {
    let mut conn = get_redis_conn().await.ok()?;
    let raw: Option<String> = conn.get(key).await.ok()?;
    let json = raw?;
    serde_json::from_str(&json).ok()
}

/// Read a cached optional `NominatimLookup`. Inner `Some/None` follows
/// the cache convention: outer `None` = miss, `Some(None)` = cached
/// empty (don't re-fetch), `Some(Some(_))` = cached hit.
async fn read_cached_optional(key: &str) -> Option<Option<NominatimLookup>> {
    let mut conn = get_redis_conn().await.ok()?;
    let raw: Option<String> = conn.get(key).await.ok()?;
    let json = raw?;
    if json == "null" {
        return Some(None);
    }
    serde_json::from_str::<NominatimLookup>(&json)
        .ok()
        .map(Some)
}
