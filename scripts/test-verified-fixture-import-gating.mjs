#!/usr/bin/env node
import test from "node:test";
import assert from "node:assert/strict";
import { previewDemandAnchorsImport } from "../lib/demand-anchors/import-commit.js";
import { previewTravelInfrastructureImport } from "../lib/travel-infrastructure/import-commit.js";
import { buildVerifiedCleanPoint } from "../lib/location-verification/google-places-verifier.js";
import { VERIFICATION_METHOD_LABEL } from "../lib/location-verification/verified-fixture-gating.js";

function point(overrides = {}) {
  return {
    name: "Corferias Bogotá Convention Center",
    pointType: "Convention Center",
    city: "Bogotá",
    country: "Colombia",
    region: "South America",
    submarket: "Bogotá",
    latitude: 4.6298,
    longitude: -74.0934,
    source: "Public Source",
    sourceReference: "https://corferias.com/",
    dataConfidence: "Medium",
    notes: "Candidate",
    ...overrides,
  };
}

const verificationMeta = {
  method: VERIFICATION_METHOD_LABEL,
  verifiedAt: "2026-06-23T12:00:00Z",
  verifiedRecords: 1,
  excludedRecords: 0,
  requirement: "High confidence Google match or explicit manual verification",
};

test("requireVerifiedFile blocks missing verification envelope", async () => {
  const res = await previewDemandAnchorsImport({
    market: "Colombia",
    country: "Colombia",
    region: "South America",
    requireVerifiedFile: true,
    points: [point()],
  });
  assert.equal(res.ok, false);
  assert.equal(res.error, "verified_fixture_required");
});

test("require-verified-fixture alias blocks missing verification envelope", async () => {
  const res = await previewDemandAnchorsImport({
    country: "Colombia",
    requireVerifiedFixture: true,
    points: [point()],
  });
  assert.equal(res.ok, false);
  assert.equal(res.error, "verified_fixture_required");
});

test("requireVerifiedFile blocks placeholder names", async () => {
  const res = await previewDemandAnchorsImport({
    market: "Colombia",
    country: "Colombia",
    region: "South America",
    requireVerifiedFile: true,
    verification: verificationMeta,
    points: [point({ name: "Placeholder Convention Center" })],
  });
  assert.equal(res.ok, false);
  assert.equal(res.error, "verified_fixture_required");
});

test("requireVerifiedFile blocks google fields on points", async () => {
  const res = await previewDemandAnchorsImport({
    country: "Colombia",
    requireVerifiedFile: true,
    verification: verificationMeta,
    points: [point({ googlePlaceId: "ChIJabc" })],
  });
  assert.equal(res.ok, false);
  assert.match(res.message, /Google-specific fields/);
});

test("requireVerifiedFile blocks unresolved verification notes", async () => {
  const res = await previewDemandAnchorsImport({
    country: "Colombia",
    requireVerifiedFile: true,
    verification: verificationMeta,
    points: [point({ notes: "Google verification ambiguous; unresolved." })],
  });
  assert.equal(res.ok, false);
  assert.match(res.message, /unresolved/);
});

test("requireVerifiedFile accepts fixture-level verification metadata", async () => {
  const res = await previewDemandAnchorsImport({
    market: "Colombia",
    country: "Colombia",
    region: "South America",
    requireVerifiedFile: true,
    verification: verificationMeta,
    points: [point()],
  });
  assert.equal(res.ok, true);
  assert.equal(res.summary.totalSubmitted, 1);
});

test("normal import preview still works without requireVerifiedFile", async () => {
  const res = await previewDemandAnchorsImport({
    market: "Colombia",
    country: "Colombia",
    region: "South America",
    points: [point()],
  });
  assert.equal(res.ok, true);
});

test("travel infrastructure honors require-verified-fixture gate", async () => {
  const res = await previewTravelInfrastructureImport({
    country: "Panama",
    requireVerifiedFile: true,
    points: [
      {
        name: "Tocumen International Airport",
        pointType: "International Airport",
        city: "Panama City",
        country: "Panama",
        latitude: 9.07,
        longitude: -79.38,
        sourceReference: "https://example.com",
      },
    ],
  });
  assert.equal(res.ok, false);
  assert.equal(res.error, "verified_fixture_required");
});

test("manual verified row can be promoted in clean output", () => {
  const candidate = point({
    dataConfidence: "High",
    notes: "Manually verified against official venue website.",
  });
  const verification = {
    verificationStatus: "Needs Review",
    matchConfidence: "Low",
    recommendedName: candidate.name,
    recommendedLatitude: candidate.latitude,
    recommendedLongitude: candidate.longitude,
  };
  const clean = buildVerifiedCleanPoint(candidate, verification, { allowMedium: false });
  assert.ok(clean);
  assert.match(clean.notes, /Location verified against Google Maps during pre-import QA/);
});

test("medium confidence excluded by default, included with allowMedium", () => {
  const candidate = point();
  const verification = {
    verificationStatus: "Needs Review",
    matchConfidence: "Medium",
    recommendedName: candidate.name,
    recommendedLatitude: candidate.latitude,
    recommendedLongitude: candidate.longitude,
  };
  const excluded = buildVerifiedCleanPoint(candidate, verification, { allowMedium: false });
  assert.equal(excluded, null);
  const included = buildVerifiedCleanPoint(candidate, verification, { allowMedium: true });
  assert.ok(included);
});
