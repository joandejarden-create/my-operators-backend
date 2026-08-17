/**
 * Unit tests — independent census brand-exclusion (no Airtable).
 */
import test from "node:test";
import assert from "node:assert/strict";
import {
  ROUTE_BUCKETS,
  classifyCandidateBrandRoute,
} from "../lib/independent-census/brand-exclusion-audit.js";

test("Active brand name routes to branded Autopilot", () => {
  const r = classifyCandidateBrandRoute({
    rawHotelName: "Crowne Plaza Santo Domingo",
    rawBrand: "",
    rawWebsite: "",
    qualityScore: 80,
    missingFields: [],
  });
  assert.equal(r.route, ROUTE_BUCKETS.BRANDED_ACTIVE);
});

test("Hyatt Inclusive Secrets routes to known-chain hold (not independent)", () => {
  const r = classifyCandidateBrandRoute({
    rawHotelName: "Secrets Royal Beach Punta Cana",
    rawBrand: "",
    rawWebsite: "https://www.hyatt.com/en-US/hotel/dominican-republic/secrets-royal-beach/serpc",
    qualityScore: 100,
    missingFields: [],
  });
  assert.ok(
    r.route === ROUTE_BUCKETS.KNOWN_CHAIN_HOLD ||
      r.route === ROUTE_BUCKETS.BRANDED_DOMAIN ||
      r.route === ROUTE_BUCKETS.BRANDED_ACTIVE
  );
  assert.equal(r.independent_lane_eligible, false);
});

test("True independent boutique stays in independent lane", () => {
  const r = classifyCandidateBrandRoute({
    rawHotelName: "Hotel Atarazana",
    rawBrand: "",
    rawWebsite: "http://www.hotel-atarazana.com",
    qualityScore: 85,
    missingFields: ["missingCity"],
  });
  assert.equal(r.route, ROUTE_BUCKETS.INDEPENDENT_CANDIDATE);
  assert.equal(r.independent_lane_eligible, true);
});

test("RIU OSM brand tag is not independent", () => {
  const r = classifyCandidateBrandRoute({
    rawHotelName: "Hotel RIU Mambo",
    rawBrand: "RIU",
    rawWebsite: "",
    qualityScore: 70,
    missingFields: ["missingWebsite", "missingCity"],
  });
  assert.equal(r.independent_lane_eligible, false);
});
