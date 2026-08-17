import test from "node:test";
import assert from "node:assert/strict";
import {
  hasValidCoordinates,
  isInCountryBbox,
  haversineKm,
  isDemandAnchorMapDisplayReady,
  MAP_REQUIRE_VERIFIED_COUNTRIES,
} from "../lib/demand-anchors/coordinate-verification.js";

test("hasValidCoordinates rejects null and 0,0", () => {
  assert.equal(hasValidCoordinates(null, -69), false);
  assert.equal(hasValidCoordinates(18.4, null), false);
  assert.equal(hasValidCoordinates(0, 0), false);
  assert.equal(hasValidCoordinates(18.4, -69.9), true);
});

test("isInCountryBbox for Dominican Republic", () => {
  assert.equal(isInCountryBbox("Dominican Republic", 18.48, -69.9), true);
  assert.equal(isInCountryBbox("Dominican Republic", 25, -69.9), false);
});

test("isDemandAnchorMapDisplayReady gates DR on lastVerified", () => {
  assert.ok(MAP_REQUIRE_VERIFIED_COUNTRIES.has("Dominican Republic"));
  const base = {
    includeOnRadarMap: true,
    country: "Dominican Republic",
    latitude: 18.48,
    longitude: -69.9,
  };
  assert.equal(isDemandAnchorMapDisplayReady({ ...base, lastVerified: "" }), false);
  assert.equal(isDemandAnchorMapDisplayReady({ ...base, lastVerified: "2026-06-17" }), true);
  assert.equal(
    isDemandAnchorMapDisplayReady({
      ...base,
      lastVerified: "2026-06-17",
      includeOnRadarMap: false,
    }),
    false
  );
});

test("Puerto Rico not gated until lastVerified required for PR", () => {
  const pr = {
    includeOnRadarMap: true,
    country: "Puerto Rico",
    latitude: 18.4,
    longitude: -66.05,
    lastVerified: "",
  };
  if (!MAP_REQUIRE_VERIFIED_COUNTRIES.has("Puerto Rico")) {
    assert.equal(isDemandAnchorMapDisplayReady(pr), true);
  }
});

test("haversineKm sanity", () => {
  const km = haversineKm(18.48, -69.9, 18.49, -69.9);
  assert.ok(km > 1 && km < 2);
});
