#!/usr/bin/env node
/**
 * Discovery Factory unit tests — no Airtable.
 */
import assert from "node:assert/strict";
import {
  resolveDiscoveryCity,
  normalizeCityLabel,
  assignDiscoveryConfidence,
  scoreHotelNameStrength,
  DISCOVERY_TIER,
  STAGE_STATUS,
  scoreCountryPriority,
  buildPrioritizedQueue,
} from "../lib/hotel-intelligence/discovery-factory/index.js";

// Accents / aliases
assert.equal(normalizeCityLabel("Sao Paulo").city, "São Paulo");
assert.equal(normalizeCityLabel("sao paulo").city, "São Paulo");
assert.equal(normalizeCityLabel("Rio de Janeiro").city, "Rio de Janeiro");
assert.equal(normalizeCityLabel("san jose").city, "San José");
assert.equal(normalizeCityLabel("ciudad de panama").city, "Ciudad de Panamá");

// URL + name agree → high city confidence
const agree = resolveDiscoveryCity({
  property_name: "grand hyatt sao paulo",
  origin_url:
    "https://www.cvent.com/venues/sao-paulo/hotel/grand-hyatt-sao-paulo/venue-x",
  country: "Brazil",
});
assert.equal(agree.city, "São Paulo");
assert.equal(agree.method, "cvent_url_and_name_agree");
assert.ok(agree.confidence >= 0.9);

// Encoded slug
const enc = resolveDiscoveryCity({
  origin_url:
    "https://www.cvent.com/venues/s%C3%A3o-paulo/hotel/foo/venue-1",
});
assert.equal(enc.city, "São Paulo");

// Name strength
assert.ok(scoreHotelNameStrength("Grand Hyatt Sao Paulo") >= 0.7);
assert.ok(scoreHotelNameStrength("x") < 0.5);

// Tier A path
const tierA = assignDiscoveryConfidence({
  name: "Grand Hyatt Sao Paulo",
  country: "Brazil",
  cityResult: {
    city: "São Paulo",
    confidence: 0.95,
    method: "cvent_url_and_name_agree",
    known_city: true,
    inferred: true,
  },
  resolveResult: { match_status: "new", match_score: 0, matching_reasons: [] },
});
assert.equal(tierA.tier, DISCOVERY_TIER.A);
assert.equal(tierA.stage_status, STAGE_STATUS.READY_FOR_IMPORT);
assert.ok(tierA.identity_confidence >= 0.9);

// Matched → not import
const matched = assignDiscoveryConfidence({
  name: "Existing Hotel",
  country: "Brazil",
  cityResult: { city: "São Paulo", confidence: 0.9 },
  resolveResult: { match_status: "exact", match_score: 1 },
});
assert.equal(matched.stage_status, STAGE_STATUS.MATCHED_EXISTING);

// Priority: Brazil should beat tiny islands when gap is huge
const br = scoreCountryPriority({
  country: "Brazil",
  hotels_in_dealality: 494,
  expected_approximate_universe: 5336,
  sources: { weak_holds: 4842, cvent_candidates: 5165, hbx_candidates: 0 },
});
const bm = scoreCountryPriority({
  country: "Bermuda",
  hotels_in_dealality: 0,
  expected_approximate_universe: 0,
  sources: { weak_holds: 0, cvent_candidates: 0, hbx_candidates: 0 },
});
assert.ok(br.priority_score > bm.priority_score);

const queue = buildPrioritizedQueue([
  {
    country: "Brazil",
    hotels_in_dealality: 494,
    expected_approximate_universe: 5336,
    gap_estimate: 4842,
    flag: "POOR",
    sources: { weak_holds: 4842, cvent_candidates: 5165, hbx_candidates: 0 },
  },
  {
    country: "Paraguay",
    hotels_in_dealality: 0,
    expected_approximate_universe: 102,
    gap_estimate: 102,
    flag: "POOR",
    sources: { weak_holds: 61, cvent_candidates: 102, hbx_candidates: 0 },
  },
]);
assert.equal(queue.items[0].country, "Brazil");

// Territory aliases + island primary locality
const provo = resolveDiscoveryCity({
  origin_url:
    "https://www.cvent.com/venues/providenciales/hotel/andaz/venue-1",
  country: "Turks and Caicos",
});
assert.equal(provo.city, "Providenciales");
assert.ok(provo.confidence >= 0.85);

const bonairePrimary = resolveDiscoveryCity({
  city: "Bonaire",
  country: "Bonaire",
});
assert.equal(bonairePrimary.city, "Kralendijk");
assert.equal(bonairePrimary.method, "island_primary_locality");
assert.ok(bonairePrimary.confidence >= 0.85);

// Cross-island Cvent pollution must not clear Tier A city gate
const polluted = resolveDiscoveryCity({
  origin_url:
    "https://www.cvent.com/venues/willemstad/hotel/foo/venue-1",
  country: "Bonaire",
});
assert.equal(polluted.country_city_conflict, true);
assert.ok(polluted.confidence < 0.85);

// Soft duplicate pressure ignores weak pool noise
const noSoft = assignDiscoveryConfidence({
  name: "Wyndham Gramado Termas Resort Spa",
  country: "Brazil",
  cityResult: {
    city: "Gramado",
    confidence: 0.95,
    method: "cvent_url_venues_slug",
    known_city: true,
  },
  resolveResult: {
    match_status: "new",
    match_score: 0,
    candidate_matches: [
      { match_score: 0.2, match_status: "weak" },
      { match_score: 0.15, match_status: "weak" },
      { match_score: 0.1, match_status: "weak" },
    ],
  },
});
assert.ok(!noSoft.reasons.includes("soft_duplicate_pressure"));
assert.equal(noSoft.stage_status, STAGE_STATUS.READY_FOR_IMPORT);

const soft = assignDiscoveryConfidence({
  name: "Wyndham Gramado Termas Resort Spa",
  country: "Brazil",
  cityResult: {
    city: "Gramado",
    confidence: 0.95,
    method: "cvent_url_venues_slug",
    known_city: true,
  },
  resolveResult: {
    match_status: "new",
    match_score: 0,
    candidate_matches: [
      { match_score: 0.62, match_status: "weak" },
      { match_score: 0.58, match_status: "weak" },
    ],
  },
});
assert.ok(soft.reasons.includes("soft_duplicate_pressure"));

console.log("test:hotel-intelligence-discovery-factory OK");
