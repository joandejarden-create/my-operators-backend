#!/usr/bin/env node
/**
 * Unit tests for hotel universe expansion (city infer + status mapping). No Airtable.
 */
import assert from "node:assert/strict";
import {
  inferCityFromCventUrl,
  resolveDiscoveryCity,
  mapIdentityToDiscoveryStatus,
  DISCOVERY_STATUS,
  buildCoverageScorecard,
  buildDiscoveryQueue,
} from "../lib/hotel-intelligence/universe-expansion/index.js";

assert.equal(
  inferCityFromCventUrl(
    "https://www.cvent.com/venues/rio-de-janeiro/hotel/grand-hyatt-rio-de-janeiro/venue-x"
  ).city,
  "Rio de Janeiro"
);
assert.equal(
  inferCityFromCventUrl(
    "https://www.cvent.com/venues/sao-paulo/hotel/unique/venue-x"
  ).city,
  "São Paulo"
);
assert.equal(inferCityFromCventUrl("https://example.com/nope").city, null);

const resolved = resolveDiscoveryCity({
  origin_city: null,
  origin_url:
    "https://www.cvent.com/venues/buenos-aires/hotel/foo/venue-1",
});
assert.equal(resolved.inferred, true);
assert.equal(resolved.city, "Buenos Aires");

assert.equal(
  mapIdentityToDiscoveryStatus(
    { match_status: "new" },
    { hasMinFields: true, inferredCity: true }
  ),
  DISCOVERY_STATUS.REVIEW_REQUIRED
);
assert.equal(
  mapIdentityToDiscoveryStatus(
    { match_status: "new" },
    { hasMinFields: true, inferredCity: false }
  ),
  DISCOVERY_STATUS.NEW_HOTEL
);
assert.equal(
  mapIdentityToDiscoveryStatus({ match_status: "exact" }, { hasMinFields: true }),
  DISCOVERY_STATUS.MATCHED
);

const scorecard = buildCoverageScorecard(
  { Brazil: 494, Mexico: 2181, Bermuda: 0 },
  {
    cventByCountry: { Brazil: 5165, Mexico: 3614 },
    hbxByCountry: { Mexico: 515 },
    holdsByCountry: { Brazil: 4842, Mexico: 1276 },
  }
);
assert.ok(scorecard.rows.length >= 3);
const br = scorecard.rows.find((r) => r.country === "Brazil");
assert.ok(br);
assert.ok(br.gap_estimate > 4000);
assert.equal(br.flag, "POOR");

const queue = buildDiscoveryQueue(scorecard);
assert.equal(queue.items[0].country, "Brazil");

console.log("test:hotel-intelligence-universe-expansion OK");
