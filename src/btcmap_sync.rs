//! BTCMap → Neo4j sync job.
//!
//! Pulls the global Bitcoin-accepting OSM POI dataset from BTCMap.org
//! (or a configured mirror) on a recurring schedule and materializes it
//! as `:Place` nodes with `accepts_bitcoin = true` plus the BTCMap
//! sub-flags (`btc_onchain`, `btc_lightning`, `btc_lightning_contactless`).
//!
//! The `/v0/mapky/btc/viewport` endpoint then serves a normal Neo4j
//! spatial query — no upstream call on the request path. First-load
//! latency for any user, in any region, is the same as the existing
//! `/v0/mapky/viewport` (~sub-100 ms).
//!
//! ## Configuration
//!
//! | Variable | Default | Effect |
//! |---|---|---|
//! | `MAPKY_BTCMAP_URL` | `https://api.btcmap.org/v2/elements` | Upstream JSON endpoint. Self-host a mirror or point at a static dump for production. |
//! | `MAPKY_BTCMAP_REFRESH_SECS` | `21600` (6 h) | Interval between syncs. BTCMap edits land within hours; 6 h is the comfort floor. |
//! | `MAPKY_BTCMAP_REQUEST_TIMEOUT_SECS` | `120` | HTTP timeout for the (potentially large) JSON pull. |
//! | `MAPKY_BTCMAP_BATCH_SIZE` | `500` | Places per Neo4j MERGE transaction. Smaller batches keep Neo4j tx logs healthy. |
//! | `MAPKY_BTCMAP_DISABLED` | unset | Set to `1` / `true` to skip the sync entirely (useful for tests or downstream consumers that don't want the BTC layer). |
//!
//! ## Concurrency safety
//!
//! Multiple plugin replicas would otherwise race the sync. We acquire a
//! Redis lock (`SET NX EX`) before running and release it on completion;
//! a TTL of 30 minutes keeps a crashed worker from blocking the next
//! sync forever.

use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Duration;

use chrono::Utc;
use deadpool_redis::redis::AsyncCommands;
use neo4rs::{
    BoltBoolean, BoltFloat, BoltInteger, BoltList, BoltMap, BoltNull, BoltString, BoltType,
};
use nexus_common::db::{get_neo4j_graph, get_redis_conn};
use reqwest::Client;
use serde::Deserialize;
use tracing::{debug, info, warn};

use crate::osm::{seed_lookup_cache, NominatimLookup};
use crate::queries;

const DEFAULT_BTCMAP_URL: &str = "https://api.btcmap.org/v2/elements";
const DEFAULT_REFRESH_SECS: u64 = 6 * 60 * 60;
const DEFAULT_REQUEST_TIMEOUT_SECS: u64 = 120;
const DEFAULT_BATCH_SIZE: usize = 500;
/// Sync lock TTL — long enough to cover a full sync (a few minutes for
/// 30k records on Neo4j), short enough that a crashed worker doesn't
/// strand future syncs.
const SYNC_LOCK_TTL_SECS: u64 = 30 * 60;

const REDIS_LAST_SYNC_KEY: &str = "mapky:btc:last_sync";
const REDIS_SYNC_LOCK_KEY: &str = "mapky:btc:sync_lock";

#[derive(Debug, Clone)]
struct SyncConfig {
    url: String,
    refresh_interval: Duration,
    request_timeout: Duration,
    batch_size: usize,
    disabled: bool,
}

fn config() -> &'static SyncConfig {
    static CFG: OnceLock<SyncConfig> = OnceLock::new();
    CFG.get_or_init(|| {
        let url =
            std::env::var("MAPKY_BTCMAP_URL").unwrap_or_else(|_| DEFAULT_BTCMAP_URL.to_string());
        let refresh_secs = std::env::var("MAPKY_BTCMAP_REFRESH_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(DEFAULT_REFRESH_SECS);
        let request_timeout_secs = std::env::var("MAPKY_BTCMAP_REQUEST_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(DEFAULT_REQUEST_TIMEOUT_SECS);
        let batch_size = std::env::var("MAPKY_BTCMAP_BATCH_SIZE")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(DEFAULT_BATCH_SIZE);
        let disabled = matches!(
            std::env::var("MAPKY_BTCMAP_DISABLED").ok().as_deref(),
            Some("1") | Some("true") | Some("yes")
        );
        SyncConfig {
            url,
            refresh_interval: Duration::from_secs(refresh_secs),
            request_timeout: Duration::from_secs(request_timeout_secs),
            batch_size,
            disabled,
        }
    })
}

/// Snapshot of where the sync currently stands. Surfaced via the
/// `/v0/mapky/btc/status` endpoint so operators don't have to shell
/// into Redis to confirm the job is alive.
#[derive(Debug, Clone, serde::Serialize, utoipa::ToSchema)]
pub struct SyncStatus {
    /// Configured upstream URL — handy when troubleshooting why the
    /// public endpoint differs from a self-hosted mirror.
    pub upstream_url: String,
    /// Configured refresh interval.
    pub refresh_interval_secs: u64,
    /// `true` when `MAPKY_BTCMAP_DISABLED` is set — the sync task
    /// never spawns in that case.
    pub disabled: bool,
    /// Wall-clock timestamp (ms since epoch) of the last successful
    /// sync, as recorded in Redis. `None` means no sync has finished
    /// yet (still running, never ran, or Redis was unreachable).
    pub last_sync_ms: Option<i64>,
    /// `true` when another worker currently holds the sync lock.
    /// Useful in multi-replica deployments.
    pub sync_in_progress: bool,
}

/// Read sync status from Redis. Cheap — two GETs.
pub async fn read_status() -> SyncStatus {
    let cfg = config();
    let mut last_sync_ms: Option<i64> = None;
    let mut sync_in_progress = false;
    if let Ok(mut conn) = get_redis_conn().await {
        last_sync_ms = conn
            .get::<_, Option<i64>>(REDIS_LAST_SYNC_KEY)
            .await
            .ok()
            .flatten();
        sync_in_progress = conn
            .get::<_, Option<String>>(REDIS_SYNC_LOCK_KEY)
            .await
            .ok()
            .flatten()
            .is_some();
    }
    SyncStatus {
        upstream_url: cfg.url.clone(),
        refresh_interval_secs: cfg.refresh_interval.as_secs(),
        disabled: cfg.disabled,
        last_sync_ms,
        sync_in_progress,
    }
}

/// Spawn the recurring BTCMap sync as a tokio background task.
///
/// Called once during plugin init. The first sync runs immediately;
/// subsequent syncs run every `MAPKY_BTCMAP_REFRESH_SECS`. Errors are
/// logged and swallowed — a flaky upstream must not crash the plugin.
pub fn spawn() {
    let cfg = config();
    if cfg.disabled {
        info!("BTCMap sync disabled via MAPKY_BTCMAP_DISABLED");
        return;
    }

    let interval = cfg.refresh_interval;
    tokio::spawn(async move {
        loop {
            match run_once().await {
                Ok(stats) => info!(
                    "BTCMap sync ok: {} fetched, {} upserted, {} cleared",
                    stats.fetched, stats.upserted, stats.cleared
                ),
                Err(e) => warn!("BTCMap sync failed: {e}"),
            }
            tokio::time::sleep(interval).await;
        }
    });
}

/// Stats reported back from a single sync run.
#[derive(Debug, Default)]
struct SyncStats {
    fetched: usize,
    upserted: usize,
    cleared: usize,
}

/// Run one sync iteration: acquire the Redis lock, pull, upsert in
/// batches, clear stale flags, release the lock.
async fn run_once() -> Result<SyncStats, Box<dyn std::error::Error + Send + Sync>> {
    if !try_acquire_sync_lock().await? {
        debug!("BTCMap sync skipped (another worker holds the lock)");
        return Ok(SyncStats::default());
    }

    // Always release — even on error.
    let result = run_sync_inner().await;
    release_sync_lock().await;
    let stats = result?;
    record_last_sync().await;
    Ok(stats)
}

async fn run_sync_inner() -> Result<SyncStats, Box<dyn std::error::Error + Send + Sync>> {
    let cfg = config();
    let synced_at = Utc::now().timestamp_millis();
    info!("BTCMap sync starting; pulling {}", cfg.url);

    let client = Client::builder()
        .timeout(cfg.request_timeout)
        .user_agent("mapky-nexus-plugin/0.1 (+https://github.com/gillohner/mapky)")
        .build()?;

    let response = client.get(&cfg.url).send().await?.error_for_status()?;
    let bytes = response.bytes().await?;

    // Two-stage parse: first to `Vec<Value>` (so one malformed row
    // can't take down the whole sync), then per-row to `BtcMapElement`.
    // The per-row try-deserialize logs and skips offenders instead of
    // returning an error — BTCMap's schema drifts occasionally and we
    // want partial syncs to succeed.
    let raw_values: Vec<serde_json::Value> = serde_json::from_slice(&bytes)?;
    let raw = raw_values.len();
    let mut bad = 0usize;
    let mut alive = 0usize;
    let mut rows: Vec<PlaceRow> = Vec::with_capacity(raw / 2);
    for value in raw_values {
        let el: BtcMapElement = match serde_json::from_value(value) {
            Ok(el) => el,
            Err(e) => {
                bad += 1;
                if bad <= 5 {
                    warn!("BTCMap row deserialize failed (sample): {e}");
                }
                continue;
            }
        };
        // BTCMap emits `deleted_at: ""` for live entries and a real
        // timestamp for soft-deleted ones — NOT `null` / absent. Filter
        // on emptiness, not `Option::is_some()`.
        if !el.deleted_at.as_deref().unwrap_or("").is_empty() {
            continue;
        }
        alive += 1;
        if let Some(row) = PlaceRow::from_element(el) {
            rows.push(row);
        }
    }
    let fetched = rows.len();
    info!(
        "BTCMap fetched {raw} raw / {bad} unparseable / {alive} alive / {fetched} parsed elements",
    );
    if fetched == 0 && raw > 0 {
        // Zero-survival is almost always a schema-shape change in
        // BTCMap or a parsing bug — make it loud so it surfaces in
        // logs instead of looking like an empty viewport.
        warn!(
            "BTCMap sync produced 0 places from {raw} raw elements — \
             check the response shape against `BtcMapElement`/`BtcMapOsmJson`",
        );
    }

    let graph = get_neo4j_graph()?;
    let mut upserted = 0usize;
    for chunk in rows.chunks(cfg.batch_size) {
        let bolt_rows = chunk_to_bolt_list(chunk);
        graph
            .run(queries::put::upsert_btcmap_places(bolt_rows, synced_at))
            .await?;
        upserted += chunk.len();
    }

    // Pre-seed the Nominatim lookup cache from the same dump. Without
    // this, the first user clicking a fresh BTC POI queues behind the
    // 1 req/s upstream gate; with it, every BTC POI is an instant
    // Redis hit forever (or until the next sync refreshes it).
    let seeded = seed_lookup_cache_for_rows(&rows).await;
    if seeded > 0 {
        info!("BTCMap pre-seeded /osm/lookup cache for {seeded} POIs");
    }

    // Clear flags from places that fell out of the dump. Runs once
    // per sync (not per batch) so partial syncs don't drop coverage
    // before all batches are persisted.
    let cleared = run_cleanup(synced_at).await.unwrap_or_else(|e| {
        warn!("BTCMap stale-flag cleanup failed: {e}");
        0
    });

    Ok(SyncStats {
        fetched,
        upserted,
        cleared,
    })
}

/// Pre-seed Redis lookup cache. Returns count of successful writes.
/// Failures are silently skipped (Redis hiccups just mean the next
/// `/osm/lookup` will fetch upstream — correct fallback behavior).
async fn seed_lookup_cache_for_rows(rows: &[PlaceRow]) -> usize {
    let mut ok = 0usize;
    for row in rows {
        let lookup = nominatim_lookup_from_row(row);
        if seed_lookup_cache(&row.osm_type, row.osm_id, &lookup).await {
            ok += 1;
        }
    }
    ok
}

async fn run_cleanup(synced_at: i64) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    let graph = get_neo4j_graph()?;
    graph
        .run(queries::put::clear_stale_btcmap_flags(synced_at))
        .await?;
    // neo4rs's `run` doesn't return a count; the operational signal is
    // the lack of an error. Returning 0 here keeps the stats struct
    // stable — operators can verify via Cypher (`MATCH (p:Place
    // {accepts_bitcoin: true}) RETURN count(p)`).
    Ok(0)
}

async fn try_acquire_sync_lock() -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let mut conn = get_redis_conn().await?;
    // SET key value NX EX ttl — atomically set if not exists, with TTL.
    let result: Option<String> = deadpool_redis::redis::cmd("SET")
        .arg(REDIS_SYNC_LOCK_KEY)
        .arg("1")
        .arg("NX")
        .arg("EX")
        .arg(SYNC_LOCK_TTL_SECS as i64)
        .query_async(&mut conn)
        .await?;
    Ok(result.is_some())
}

async fn release_sync_lock() {
    if let Ok(mut conn) = get_redis_conn().await {
        let _: Result<i64, _> = conn.del(REDIS_SYNC_LOCK_KEY).await;
    }
}

async fn record_last_sync() {
    if let Ok(mut conn) = get_redis_conn().await {
        let now = Utc::now().timestamp_millis();
        let _: Result<(), _> = conn.set(REDIS_LAST_SYNC_KEY, now).await;
    }
}

// ── BTCMap response shape ───────────────────────────────────────────────

/// Single element from `https://api.btcmap.org/v2/elements`.
/// Schema is permissive — we only deserialize the fields we need and
/// ignore the rest so a BTCMap-side schema bump won't break ingest.
#[derive(Debug, Deserialize)]
struct BtcMapElement {
    osm_json: BtcMapOsmJson,
    /// Set when BTCMap soft-deleted the element. We skip these so the
    /// post-sync cleanup naturally clears their flags from Neo4j.
    #[serde(default)]
    deleted_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BtcMapOsmJson {
    #[serde(rename = "type")]
    osm_type: String,
    id: i64,
    /// Present for nodes; absent for ways/relations (those use `bounds`).
    #[serde(default)]
    lat: Option<f64>,
    #[serde(default)]
    lon: Option<f64>,
    #[serde(default)]
    bounds: Option<BtcMapBounds>,
    /// `#[serde(default)]` only fires for *missing* fields. BTCMap
    /// emits `tags: null` for soft-deleted entries, which would error
    /// out the bulk JSON parse. Accept `null` as the empty map so the
    /// parse completes and the entry can be filtered out by
    /// `deleted_at` further downstream.
    #[serde(default, deserialize_with = "deserialize_nullable_map")]
    tags: HashMap<String, String>,
}

fn deserialize_nullable_map<'de, D>(d: D) -> Result<HashMap<String, String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Option::<HashMap<String, String>>::deserialize(d)?.unwrap_or_default())
}

#[derive(Debug, Deserialize)]
struct BtcMapBounds {
    minlat: f64,
    minlon: f64,
    maxlat: f64,
    maxlon: f64,
}

/// Internal representation after parsing + filtering — what we hand to
/// the upsert query.
#[derive(Debug, Clone)]
struct PlaceRow {
    osm_canonical: String,
    osm_type: String,
    osm_id: i64,
    lat: f64,
    lon: f64,
    name: Option<String>,
    btc_onchain: bool,
    btc_lightning: bool,
    btc_lightning_contactless: bool,
    /// Full OSM tag map kept around so we can pre-seed the
    /// `mapky:osm:v2:lookup:` Redis cache during the same sync — that
    /// way `/v0/mapky/osm/lookup` for any BTC POI is an instant Redis
    /// hit and never queues behind the 1 req/s Nominatim gate.
    tags: HashMap<String, String>,
}

impl PlaceRow {
    fn from_element(el: BtcMapElement) -> Option<Self> {
        let osm = el.osm_json;
        let osm_type = match osm.osm_type.as_str() {
            "node" | "way" | "relation" => osm.osm_type.clone(),
            _ => return None,
        };
        let (lat, lon) = match (osm.lat, osm.lon, osm.bounds.as_ref()) {
            (Some(lat), Some(lon), _) => (lat, lon),
            (_, _, Some(b)) => ((b.minlat + b.maxlat) / 2.0, (b.minlon + b.maxlon) / 2.0),
            _ => return None,
        };
        // Defensive bounds check — placing (0, 0) entries pollutes
        // viewport queries near the equator/prime-meridian intersection.
        if !lat.is_finite() || !lon.is_finite() || (lat == 0.0 && lon == 0.0) {
            return None;
        }

        let name = osm.tags.get("name").cloned().filter(|n| !n.is_empty());
        let xbt = osm
            .tags
            .get("currency:XBT")
            .map(|s| s == "yes")
            .unwrap_or(false);
        let legacy_bitcoin = osm
            .tags
            .get("payment:bitcoin")
            .map(|s| s == "yes")
            .unwrap_or(false);
        // Filter: BTCMap occasionally surfaces elements that have lost
        // their Bitcoin tags between dumps. Skip them — the cleanup
        // sweep will retire any stale Neo4j flags on the next pass.
        if !xbt && !legacy_bitcoin {
            return None;
        }
        let onchain = osm
            .tags
            .get("payment:onchain")
            .map(|s| s == "yes")
            .unwrap_or(false)
            || legacy_bitcoin;
        let lightning = osm
            .tags
            .get("payment:lightning")
            .map(|s| s == "yes")
            .unwrap_or(false);
        let lightning_contactless = osm
            .tags
            .get("payment:lightning_contactless")
            .map(|s| s == "yes")
            .unwrap_or(false);

        Some(PlaceRow {
            osm_canonical: format!("{osm_type}/{}", osm.id),
            osm_type,
            osm_id: osm.id,
            lat,
            lon,
            name,
            btc_onchain: onchain,
            btc_lightning: lightning,
            btc_lightning_contactless: lightning_contactless,
            tags: osm.tags,
        })
    }
}

/// Build a `NominatimLookup` from a BTCMap row's tag map + coords.
/// Mirrors the shape `osm.rs::batch_lookup_cached` returns from a real
/// Nominatim hit, so seeding this into Redis makes `/osm/lookup` for
/// these POIs return instantly without ever hitting upstream.
fn nominatim_lookup_from_row(row: &PlaceRow) -> NominatimLookup {
    // Nominatim's `category` is the broad classification key
    // (`amenity`, `shop`, `tourism`, …) and `type` is the value
    // (`restaurant`, `cafe`, …). Mirror that mapping from OSM tags.
    let (category, kind) = ["amenity", "shop", "tourism", "leisure", "office", "craft"]
        .iter()
        .find_map(|k| {
            row.tags
                .get(*k)
                .map(|v| (Some(k.to_string()), Some(v.clone())))
        })
        .unwrap_or((None, None));

    // Strip the `addr:` prefix to match Nominatim's address-map shape
    // (`road`, `city`, `postcode`, `country`, …).
    let mut address: HashMap<String, String> = HashMap::new();
    for (k, v) in &row.tags {
        if let Some(stripped) = k.strip_prefix("addr:") {
            address.insert(stripped.to_string(), v.clone());
        }
    }

    // Synthesize a display_name from name + city + country when we
    // can't get the real geocoded one. Falls back gracefully when any
    // piece is missing.
    let display_name = {
        let mut parts: Vec<&str> = Vec::new();
        if let Some(n) = row.name.as_deref() {
            parts.push(n);
        }
        if let Some(city) = address.get("city").map(String::as_str) {
            parts.push(city);
        }
        if let Some(country) = address.get("country").map(String::as_str) {
            parts.push(country);
        }
        parts.join(", ")
    };

    NominatimLookup {
        osm_type: Some(row.osm_type.clone()),
        osm_id: Some(row.osm_id),
        name: row.name.clone(),
        display_name,
        kind,
        category,
        address,
        lat: Some(row.lat),
        lon: Some(row.lon),
        // The full OSM tag map IS what Nominatim returns as `extratags`
        // when called with `extratags=1`. The frontend's BitcoinAcceptance
        // component reads `currency:XBT` / `payment:*` straight off this.
        extratags: row.tags.clone(),
        // BTCMap rows have no admin boundary — point POIs only.
        boundingbox: None,
    }
}

// ── Bolt conversion ─────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// BTCMap emits `deleted_at: ""` for live entries (not absent /
    /// null). Lock that down — the original sync filtered on
    /// `is_none()`, dropped 100 % of the dump, and silently produced
    /// an empty viewport.
    #[test]
    fn live_entries_have_empty_string_deleted_at() {
        let raw = json!([
            {
                "id": "node:1",
                "osm_json": {
                    "type": "node", "id": 1, "lat": 47.0, "lon": 7.0,
                    "tags": { "currency:XBT": "yes", "name": "Alive" }
                },
                "deleted_at": ""
            },
            {
                "id": "node:2",
                "osm_json": {
                    "type": "node", "id": 2, "lat": 47.5, "lon": 7.5,
                    "tags": { "currency:XBT": "yes", "name": "Dead" }
                },
                "deleted_at": "2024-01-01T00:00:00Z"
            }
        ]);
        let elements: Vec<BtcMapElement> = serde_json::from_value(raw).unwrap();
        let alive: Vec<&BtcMapElement> = elements
            .iter()
            .filter(|el| el.deleted_at.as_deref().unwrap_or("").is_empty())
            .collect();
        assert_eq!(alive.len(), 1, "only the empty-string entry is alive");
        assert_eq!(alive[0].osm_json.id, 1);
    }

    /// BTCMap's `osm_json` for ways/relations carries a `bounds` block
    /// but no top-level lat/lon. We compute the centroid.
    #[test]
    fn way_uses_bounds_centroid() {
        let raw = json!({
            "id": "way:42",
            "osm_json": {
                "type": "way", "id": 42,
                "bounds": {
                    "minlat": 47.0, "maxlat": 47.2,
                    "minlon": 7.0, "maxlon": 7.4
                },
                "tags": { "currency:XBT": "yes", "name": "Polygon" }
            },
            "deleted_at": ""
        });
        let el: BtcMapElement = serde_json::from_value(raw).unwrap();
        let row = PlaceRow::from_element(el).expect("way should parse");
        assert!((row.lat - 47.1).abs() < 1e-9);
        assert!((row.lon - 7.2).abs() < 1e-9);
    }

    /// Soft-deleted BTCMap rows arrive with `osm_json.tags: null`
    /// (not absent). The original deserializer crashed the *entire*
    /// 36 MB bulk parse on the first such row, dropping the whole
    /// sync. Lock that down — accept null, end up with empty tags.
    #[test]
    fn null_tags_parses_as_empty_map() {
        let raw = json!({
            "id": "node:10069082977",
            "deleted_at": "2023-03-03T15:13:28Z",
            "osm_json": {
                "type": "node", "id": 10069082977_i64,
                "lat": 37.25, "lon": -8.34,
                "tags": null,
                "version": null,
                "user": null
            }
        });
        let el: BtcMapElement = serde_json::from_value(raw).expect("null tags must parse");
        assert!(el.osm_json.tags.is_empty());
    }

    /// The synthesized `NominatimLookup` is what the cache pre-seed
    /// writes to `mapky:osm:v2:lookup:`. Locks the contract: extratags
    /// pass through verbatim (BitcoinAcceptance reads from there),
    /// `addr:*` is stripped to match Nominatim's address shape, and
    /// `category`/`kind` come from the OSM type/value pair.
    #[test]
    fn nominatim_lookup_synthesis_matches_nominatim_shape() {
        let row = PlaceRow {
            osm_canonical: "node/42".into(),
            osm_type: "node".into(),
            osm_id: 42,
            lat: 47.5,
            lon: 7.5,
            name: Some("Bitcoin Cafe".into()),
            btc_onchain: true,
            btc_lightning: true,
            btc_lightning_contactless: false,
            tags: HashMap::from([
                ("name".into(), "Bitcoin Cafe".into()),
                ("amenity".into(), "cafe".into()),
                ("currency:XBT".into(), "yes".into()),
                ("payment:onchain".into(), "yes".into()),
                ("payment:lightning".into(), "yes".into()),
                ("addr:city".into(), "Bern".into()),
                ("addr:country".into(), "Switzerland".into()),
                ("addr:street".into(), "Bahnhofstrasse".into()),
            ]),
        };
        let lookup = nominatim_lookup_from_row(&row);
        assert_eq!(lookup.category.as_deref(), Some("amenity"));
        assert_eq!(lookup.kind.as_deref(), Some("cafe"));
        assert_eq!(lookup.address.get("city").map(String::as_str), Some("Bern"));
        assert_eq!(
            lookup.address.get("street").map(String::as_str),
            Some("Bahnhofstrasse")
        );
        assert!(
            !lookup.address.contains_key("addr:city"),
            "addr: prefix must be stripped"
        );
        // BitcoinAcceptance reads these straight off extratags.
        assert_eq!(
            lookup.extratags.get("currency:XBT").map(String::as_str),
            Some("yes")
        );
        assert_eq!(
            lookup
                .extratags
                .get("payment:lightning")
                .map(String::as_str),
            Some("yes")
        );
        assert!(lookup.display_name.contains("Bitcoin Cafe"));
        assert!(lookup.display_name.contains("Bern"));
    }

    /// Entries that lost their bitcoin tag between dumps must be
    /// dropped — the post-sync cleanup sweep then retires the stale
    /// flag on the Neo4j side.
    #[test]
    fn skips_elements_without_bitcoin_tag() {
        let raw = json!({
            "id": "node:99",
            "osm_json": {
                "type": "node", "id": 99, "lat": 1.0, "lon": 2.0,
                "tags": { "name": "Cash only" }
            },
            "deleted_at": ""
        });
        let el: BtcMapElement = serde_json::from_value(raw).unwrap();
        assert!(PlaceRow::from_element(el).is_none());
    }
}

/// Build a `BoltType::List` of `BoltType::Map` from a slice of rows.
/// Lives here (not in `queries/put.rs`) so the query module stays
/// Cypher-only and the sync module owns its own data shape.
fn chunk_to_bolt_list(rows: &[PlaceRow]) -> BoltType {
    let mut list = BoltList::default();
    for row in rows {
        let mut map = BoltMap::default();
        map.put(
            BoltString::from("osm_canonical"),
            BoltType::String(BoltString::from(row.osm_canonical.as_str())),
        );
        map.put(
            BoltString::from("osm_type"),
            BoltType::String(BoltString::from(row.osm_type.as_str())),
        );
        map.put(
            BoltString::from("osm_id"),
            BoltType::Integer(BoltInteger::new(row.osm_id)),
        );
        map.put(
            BoltString::from("lat"),
            BoltType::Float(BoltFloat::new(row.lat)),
        );
        map.put(
            BoltString::from("lon"),
            BoltType::Float(BoltFloat::new(row.lon)),
        );
        map.put(
            BoltString::from("name"),
            match &row.name {
                Some(n) => BoltType::String(BoltString::from(n.as_str())),
                None => BoltType::Null(BoltNull),
            },
        );
        map.put(
            BoltString::from("btc_onchain"),
            BoltType::Boolean(BoltBoolean::new(row.btc_onchain)),
        );
        map.put(
            BoltString::from("btc_lightning"),
            BoltType::Boolean(BoltBoolean::new(row.btc_lightning)),
        );
        map.put(
            BoltString::from("btc_lightning_contactless"),
            BoltType::Boolean(BoltBoolean::new(row.btc_lightning_contactless)),
        );
        list.push(BoltType::Map(map));
    }
    BoltType::List(list)
}
