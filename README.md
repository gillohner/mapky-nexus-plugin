# mapky-nexus-plugin

A [Pubky Nexus](https://github.com/pubky/pubky-nexus) plugin that indexes
[MapKy](https://github.com/gillohner/mapky) geo-social content into the shared
Neo4j graph, exposing a REST API for place reviews, posts, and spatial queries.

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
| `MapkyAppLocationTag` | `/pub/mapky.app/location_tags/<id>` | Stub (v1) |
| `MapkyAppCollection` | `/pub/mapky.app/collections/<id>` | Stub (v1) |
| `MapkyAppIncident` | `/pub/mapky.app/incidents/<id>` | Stub (v1) |
| `MapkyAppGeoCapture` | `/pub/mapky.app/geo_captures/<id>` | Stub (v1) |
| `MapkyAppRoute` | `/pub/mapky.app/routes/<id>` | Stub (v1) |

### Neo4j Graph Schema

```
(:User)-[:AUTHORED]->(:MapkyPost)-[:ABOUT]->(:Place)
(:MapkyPost)-[:REPLY_TO]->(:MapkyPost)   // threaded replies

Place {
  osm_canonical, osm_type, osm_id,
  location: point,           // spatial index for bbox queries
  lat, lon,
  review_count, avg_rating,
  tag_count, photo_count
}

MapkyPost {
  id, content, rating,
  kind,          // "review" | "post"
  parent_uri,    // pubky:// URI of parent post (replies)
  attachments,   // list of pubky:// URIs
  indexed_at
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
| `GET` | `/v0/mapky/viewport` | Places in a lat/lon bounding box |
| `GET` | `/v0/mapky/place/{osm_type}/{osm_id}` | Single place detail |
| `GET` | `/v0/mapky/place/{osm_type}/{osm_id}/posts` | Posts for a place (paginated, optional `reviews_only`) |

### Example

```bash
# Places in a viewport
curl 'localhost:8080/v0/mapky/viewport?min_lat=48.1&min_lon=16.3&max_lat=48.3&max_lon=16.5'

# Reviews for a specific OSM way
curl 'localhost:8080/v0/mapky/place/way/618456759/posts?reviews_only=true'
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
