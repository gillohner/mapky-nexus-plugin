# Migrations

One-off Cypher scripts for backfilling existing `:MapkyAppRoute` /
`:MapkyAppPost` / `:Place` nodes when the plugin's PUT logic gains a new
indexed property.

These are idempotent and safe to re-run — each script's `WHERE` clause
gates on the absence of the new property.

## Running

```bash
# Local dev (against the docker neo4j with config-local credentials)
cypher-shell -a bolt://localhost:7687 -u neo4j -p 12345678 \
  -f migrations/2026-04-27-route-start-lat-lon.cypher

# Production — substitute prod auth + endpoint
cypher-shell -a $NEO4J_URI -u $NEO4J_USER -p $NEO4J_PASSWORD \
  -f migrations/2026-04-27-route-start-lat-lon.cypher
```

## Index

| Date | Script | What it does |
|---|---|---|
| 2026-04-27 | `2026-04-27-route-start-lat-lon.cypher` | Populates `r.start_lat / r.start_lon` scalars on routes that only have `r.start_point` (POINT). Required after the plugin started writing both. |
