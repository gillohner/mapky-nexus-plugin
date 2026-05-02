> **Pre-release (`0.1.0-alpha.1`).** APIs, schema, and routes may change without
> notice. Not published to crates.io — consume via git tag (see "Installation").

# mapky-nexus-plugin

A [Pubky Nexus](https://github.com/pubky/pubky-nexus) plugin that indexes
[MapKy](https://github.com/gillohner/mapky) geo-social content into the shared
Neo4j graph, exposing a REST API for place reviews, posts, and spatial queries.

## Installation

This crate is **not on crates.io** — the plugin system in `nexus-common` lives
on a local pubky-nexus fork that hasn't shipped upstream yet, so a published
copy would be unbuildable. Until upstream catches up, depend on this repo by
git tag:

```toml
[dependencies]
mapky-nexus-plugin = { git = "https://github.com/gillohner/mapky-nexus-plugin", tag = "v0.1.0-alpha.1" }
```

You also need a sibling checkout of `gillohner/pubky-nexus` (with the
plugin-system branch) at `../pubky-nexus/` for the path dep on `nexus-common`
to resolve.

## Overview

MapKy is a decentralized social layer on top of OpenStreetMap. Users write
reviews, posts, location tags, and routes anchored to OSM places — all stored
on Pubky homeservers. This plugin watches for `/pub/mapky.app/` events,
indexes them into Neo4j alongside the Nexus social graph, and serves them via
a spatial API.

```
Pubky homeserver
      │  PUT/DEL /pub/mapky.app/posts/<id>
      ▼
nexus-watcher dispatcher
      │  matched by namespace "/pub/mapky.app/"
      ▼
mapky-nexus-plugin
      ├── handle_put / handle_del  →  Neo4j (MapkyPost, Place, edges)
      └── /v0/mapky/               →  REST API (viewport, place detail, posts)
```

## Data Models

All models are defined in [`mapky-app-specs`](https://github.com/gillohner/mapky-app-specs).

| Model | Path | Indexed |
|---|---|---|
| `MapkyAppPost` | `/pub/mapky.app/posts/<id>` | Yes — nodes, edges, rating aggregates |
| `MapkyAppCollection` | `/pub/mapky.app/collections/<id>` | Yes — node + items list |
| `MapkyAppIncident` | `/pub/mapky.app/incidents/<id>` | Yes — node + spatial point |
| `MapkyAppGeoCapture` | `/pub/mapky.app/geo_captures/<id>` | Yes — node + spatial point + heading |
| `MapkyAppSequence` | `/pub/mapky.app/sequences/<id>` | Yes — ordered list of geo-captures |
| `MapkyAppRoute` | `/pub/mapky.app/routes/<id>` | Yes — metadata + bbox + start point (polyline stays on the homeserver) |

Tags on places, posts, routes, etc. use standard `PubkyAppTag` (universal tags)
stored at `/pub/mapky.app/tags/`. The plugin implements `resolve_graph_node()`
so nexus core can create `(User)-[:TAGGED {label}]->(MapkyApp*)` edges across
all MapKy resource types — that's what powers tag-based search of routes
without dedicated tag tables.

### Neo4j Graph Schema

```
(:User)-[:AUTHORED]->(:MapkyAppPost)-[:ABOUT]->(:Place)
(:MapkyAppPost)-[:REPLY_TO]->(:MapkyAppPost)         // threaded replies
(:User)-[:CREATED]->(:MapkyAppCollection)
(:User)-[:REPORTED]->(:MapkyAppIncident)
(:User)-[:AUTHORED]->(:MapkyAppGeoCapture)
(:User)-[:CREATED]->(:MapkyAppRoute)
(:User)-[:TAGGED {label}]->(<MapkyApp* | Place>)     // cross-domain

Place {
  osm_canonical, osm_type, osm_id,
  location: point,           // spatial index for bbox queries
  lat, lon,
  review_count, avg_rating,
  tag_count, photo_count
}

MapkyAppPost {
  id, content, rating,
  kind,          // "review" | "post"
  parent_uri,    // pubky:// URI of parent post (replies)
  attachments,   // list of pubky:// URIs
  indexed_at
}

MapkyAppRoute {
  id, name, description, activity,
  distance_m, estimated_duration_s,
  elevation_gain_m, elevation_loss_m,
  waypoint_count,
  start_point: point,                    // spatial index for nearby/viewport
  min_lat, min_lon, max_lat, max_lon,    // bbox-contains queries
  indexed_at
  // Note: waypoints + encoded polyline are NOT here. They live on
  // the author's homeserver and are fetched lazily by the frontend
  // when rendering a route detail.
}
```

Place coordinates are resolved via the [Nominatim](https://nominatim.org/)
geocoding API on first encounter. Subsequent posts to the same place reuse
the cached `Place` node.

## API Endpoints

Mounted at `/v0/mapky/` by nexusd. Full schema at
`/api-docs/mapky/openapi.json` (Swagger UI when running locally).

| Method | Path | Description |
|---|---|---|
| `GET` | `/viewport` | Places in a lat/lon bounding box |
| `GET` | `/place/{osm_type}/{osm_id}` | Single place detail |
| `GET` | `/place/{osm_type}/{osm_id}/posts` | Posts for a place (paginated, optional `reviews_only`) |
| `GET` | `/place/{osm_type}/{osm_id}/tags` | Tags on a place |
| `GET` | `/place/{osm_type}/{osm_id}/routes` | Routes passing near a place (bbox-contains) |
| `GET` | `/posts/{author_id}/{post_id}/tags` | Tags on a post |
| `GET` | `/posts/user/{user_id}` | A user's posts |
| `GET` | `/incidents/viewport` | Incidents in a bbox |
| `GET` | `/incidents/{author_id}/{incident_id}` | Incident detail |
| `GET` | `/incidents/user/{user_id}` | A user's incidents |
| `GET` | `/geo_captures/viewport` | Geo-captures in a bbox |
| `GET` | `/geo_captures/nearby` | Geo-captures near a point |
| `GET` | `/geo_captures/user/{user_id}` | A user's geo-captures |
| `GET` | `/sequences/user/{user_id}` | A user's capture sequences |
| `GET` | `/collections/user/{user_id}` | A user's collections |
| `GET` | `/routes/viewport` | Routes intersecting a bbox (metadata + start point only — no polyline) |
| `GET` | `/routes/{author_id}/{route_id}` | Route metadata |
| `GET` | `/routes/{author_id}/{route_id}/tags` | Tags on a route |
| `GET` | `/routes/user/{user_id}` | A user's routes |
| `GET` | `/search/tags?q=` | Tag search across places, collections, posts |
| `GET` | `/osm/lookup?osm_ids=N1,W2,R3` | Cached batched Nominatim lookup (proxy with Redis) |

### OSM lookup proxy (`/osm/lookup`)

Sits in front of public (or self-hosted) Nominatim so the browser
doesn't fan out per-POI `/lookup` calls and trip per-IP rate limits.
A 30-place viewport opens with one upstream request the first time;
every subsequent user sharing those places hits Redis instead, for
the configured TTL window.

- Accepts `?osm_ids=N1,W2,R3,…` (Nominatim's input format), up to 50
  per request — chunks larger inputs internally.
- Returns one result per input ref in the same order, with empty
  results (`display_name == ""`) for IDs Nominatim couldn't resolve.
  Empty results are also cached so a permanently-missing ID doesn't
  re-hit upstream every time someone asks.
- `addressdetails` and `extratags` are always included (`payment:*`
  / `currency:XBT` flags from the BTCMap schema, address fields used
  by the place card).

#### Configuration

All knobs are environment variables, read once on first call. Restart
`nexusd` to apply changes.

| Variable | Default | Purpose |
|---|---|---|
| `MAPKY_NOMINATIM_URL` | `https://nominatim.openstreetmap.org` | Upstream base URL. Point at a self-hosted mirror for production traffic; the public instance is per-IP rate-limited and not for sustained use per the [OSM usage policy](https://operations.osmfoundation.org/policies/nominatim/). |
| `MAPKY_OVERPASS_URL` | `https://overpass-api.de/api/interpreter` | Overpass `/interpreter` endpoint. Used as a fallback when Nominatim returns empty for an OSM ref that does exist (typical case: a recently-edited unnamed building with full `addr:*` tags — Nominatim's named-entity index doesn't have it, but Overpass serves the raw OSM tags). Point at a self-hosted instance in production. |
| `MAPKY_NOMINATIM_MIN_INTERVAL_MS` | `1000` | Floor on inter-request spacing (1 req/s matches Nominatim's policy). Shared with event-time geocoding and the Overpass fallback so all upstream calls compete fairly. |
| `MAPKY_NOMINATIM_USER_AGENT` | `mapky-nexus-plugin/0.1 (+repo)` | Sent on every upstream call. Set this to something operator-identifiable in production — Nominatim operators sometimes use it to follow up on traffic patterns. |
| `MAPKY_OSM_CACHE_TTL_SECS` | `2592000` (30 days) | Redis TTL for **resolved** lookups. Lower for datasets you expect to churn; higher for stable production OSM extracts. |
| `MAPKY_OSM_EMPTY_CACHE_TTL_SECS` | `21600` (6 hours) | Redis TTL for **empty placeholders** — refs neither Nominatim nor Overpass returned data for. Short by design so a recent OSM edit catches up quickly. |
| `MAPKY_OSM_BATCH_SIZE` | `50` | Max IDs per upstream request. Public Nominatim caps at 50; self-hosted may allow more. |

### Example

```bash
# Places in a viewport
curl 'localhost:8080/v0/mapky/viewport?min_lat=48.1&min_lon=16.3&max_lat=48.3&max_lon=16.5'

# Reviews for a specific OSM way
curl 'localhost:8080/v0/mapky/place/way/618456759/posts?reviews_only=true'

# Routes passing near a place
curl 'localhost:8080/v0/mapky/place/way/618456759/routes'

# Routes in a bbox (metadata only — fetch the body from the homeserver)
curl 'localhost:8080/v0/mapky/routes/viewport?min_lat=46.7&min_lon=6.1&max_lat=47.7&max_lon=9.6'
```

## Running with Nexus

This plugin is compiled into `nexusd` via the `mapky` feature flag. Without it,
`nexusd` builds normally with no MapKy dependency.

```bash
# From the pubky-nexus repo root, with this plugin next to it:
# pubky-nexus/
# mapky/mapky-nexus-plugin/   ← expected relative path

cargo run -p nexusd --features mapky
```

The plugin setup is idempotent — Neo4j constraints and spatial indexes are
created on startup if they don't already exist.

## Development

### Prerequisites

- Rust toolchain
- A running [Pubky Nexus](https://github.com/pubky/pubky-nexus) stack
  (Neo4j + Redis + homeserver via `docker compose`)

### Build & check

```bash
cd mapky/mapky-nexus-plugin

# Check (no nexusd needed)
cargo clippy -- -D warnings

# Check including nexusd integration
cd ../../pubky-nexus
cargo clippy --workspace --features mapky -- -D warnings
```

### Write test data

```bash
cd mapky/mapky-nexus-plugin
cargo run --example write_testnet
```

Then query the API:

```bash
curl -s 'localhost:8080/v0/mapky/place/way/618456759/posts' | jq '.[0]'
```

## Repository Layout

```
mapky-nexus-plugin/
├── src/
│   ├── lib.rs          # NexusPlugin impl for MapkyPlugin
│   ├── api/            # Axum handlers + OpenAPI docs
│   ├── handlers/       # put/del event handlers
│   ├── models/         # PostDetails, PlaceDetails
│   └── queries/        # Neo4j read/write/delete queries
└── examples/
    └── write_testnet.rs  # write sample data to local testnet
```

## Related

- [mapky-app-specs](https://github.com/gillohner/mapky-app-specs) — Rust/WASM data model definitions
- [pubky-nexus](https://github.com/pubky/pubky-nexus) — the host indexer
- [pubky-app-specs](https://github.com/pubky/pubky-app-specs) — base traits (`Validatable`, `TimestampId`, `HashId`)
