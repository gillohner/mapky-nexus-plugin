// Backfill r.start_lat / r.start_lon scalar properties on existing
// :MapkyAppRoute nodes that were indexed before the plugin started writing
// them alongside r.start_point.
//
// Idempotent: only updates rows where start_lat is missing AND start_point
// is present. Safe to run multiple times.
//
// Run with cypher-shell or via Neo4j Browser:
//   cypher-shell -u neo4j -p <password> -f migrations/2026-04-27-route-start-lat-lon.cypher
//
// Or via HTTP API:
//   curl -u neo4j:<password> -H 'Content-Type: application/json' \
//     http://localhost:7474/db/neo4j/tx/commit \
//     -d "$(jq -n --arg q "$(cat migrations/2026-04-27-route-start-lat-lon.cypher)" \
//       '{statements:[{statement:$q}]}')"

MATCH (r:MapkyAppRoute)
WHERE r.start_lat IS NULL AND r.start_point IS NOT NULL
SET r.start_lat = r.start_point.y,
    r.start_lon = r.start_point.x
RETURN count(r) AS rows_updated;
