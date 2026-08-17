import test from "node:test";
import assert from "node:assert/strict";
import { buildGapDiscoveryQueue } from "../lib/research-engine-v2/independent-geographic-gap-discovery-wave-v1.js";

test("queue includes force-zero geos and Cuba first by score", () => {
  const queue = buildGapDiscoveryQueue({
    priorityDoc: {
      GEOGRAPHY_DISCOVERY_PRIORITY: [
        {
          geography: "Cuba",
          priority_score: 1425,
          gap_class: "ZERO_DEALALITY_BENCHMARK_NONZERO",
          BENCHMARK_COUNT: 295,
          dealality_census_count: 0,
        },
        {
          geography: "Belize",
          priority_score: 874,
          gap_class: "POSSIBLE_MAJOR_GAP",
          BENCHMARK_COUNT: 334,
          dealality_census_count: 12,
        },
      ],
      CITY_DESTINATION_DISCOVERY_PRIORITY: [
        { geography: "Cuba", city_or_destination: "Havana", priority_score: 900 },
      ],
    },
    matrixDoc: { matrix: [] },
  });
  const names = queue.map((q) => q.geography);
  assert.ok(names.includes("Cuba"));
  assert.ok(names.includes("Sint Eustatius"));
  assert.ok(names.includes("Saba"));
  assert.equal(queue[0].geography, "Cuba");
  assert.ok(queue.find((q) => q.geography === "Cuba").destinations.includes("Havana"));
  // No hotel names in queue artifact shape
  for (const q of queue) {
    assert.equal(typeof q.geography, "string");
    assert.ok(!("benchmark_hotel_names" in q));
  }
});
