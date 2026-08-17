#!/usr/bin/env node
import test from "node:test";
import assert from "node:assert/strict";
import { evaluatePlaceMatchConfidence } from "../lib/location-verification/place-match-confidence.js";
import {
  verifyCandidateWithGoogle,
  buildVerifiedCleanPoint,
} from "../lib/location-verification/google-places-verifier.js";
import {
  resolveGoogleApiKey,
  estimateVerificationApiRequests,
  parseGoogleVerificationCli,
} from "../lib/location-verification/google-api-config.js";
import { runGooglePlacesVerification } from "../lib/location-verification/run-google-places-verification.js";
import { buildVerificationReportPayload } from "../lib/location-verification/build-verification-report.js";

function mkPlace(overrides = {}) {
  return {
    googlePlaceId: "place-1",
    googleName: "Corferias Bogotá",
    googleLatitude: 4.6297,
    googleLongitude: -74.0935,
    googleFormattedAddress: "Corferias, Bogotá, Colombia",
    googleMapsUri: "https://maps.google.com/?q=Corferias",
    googleTypes: ["establishment", "convention_center"],
    ...overrides,
  };
}

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
    sourceReference: "https://corferias.com/",
    dataConfidence: "Medium",
    notes: "Submarket: Bogotá.",
    ...overrides,
  };
}

test("high confidence verification", () => {
  const res = evaluatePlaceMatchConfidence({
    candidateName: "Corferias Bogotá",
    candidateCity: "Bogotá",
    candidateCountry: "Colombia",
    candidateLatitude: 4.6298,
    candidateLongitude: -74.0934,
    candidatePointType: "Convention Center",
    result: mkPlace(),
    competingResults: [],
  });
  assert.equal(res.verificationStatus, "Verified");
  assert.equal(res.matchConfidence, "High");
});

test("medium confidence verification", () => {
  const res = evaluatePlaceMatchConfidence({
    candidateName: "Corferias Eventos Bogotá",
    candidateCity: "Bogotá",
    candidateCountry: "Colombia",
    candidateLatitude: 4.6298,
    candidateLongitude: -74.0934,
    candidatePointType: "Convention Center",
    result: mkPlace(),
    competingResults: [],
  });
  assert.equal(res.matchConfidence, "Medium");
  assert.equal(res.verificationStatus, "Needs Review");
});

test("low confidence / mismatched city", () => {
  const res = evaluatePlaceMatchConfidence({
    candidateName: "Random Site",
    candidateCity: "Cartagena",
    candidateCountry: "Colombia",
    candidateLatitude: 10.4,
    candidateLongitude: -75.5,
    candidatePointType: "Medical Campus",
    result: mkPlace(),
    competingResults: [],
  });
  assert.equal(res.matchConfidence, "Low");
  assert.equal(res.verificationStatus, "Needs Review");
});

test("ambiguous match detection", () => {
  const res = evaluatePlaceMatchConfidence({
    candidateName: "San Martin Plaza",
    candidateCity: "Cartagena",
    candidateCountry: "Colombia",
    candidatePointType: "Business District",
    result: mkPlace({ googleName: "San Martin Plaza Cartagena", googleFormattedAddress: "Cartagena, Colombia" }),
    competingResults: [
      mkPlace({ googleName: "Plaza San Martin Cartagena", googleFormattedAddress: "Cartagena, Colombia" }),
    ],
  });
  assert.equal(res.verificationStatus, "Ambiguous Match");
});

test("verifyCandidateWithGoogle no match", async () => {
  const result = await verifyCandidateWithGoogle(point({ name: "Unknown Place" }), {
    apiKey: "fake",
    searchTextFn: async () => [],
    placeDetailsFn: async () => null,
  });
  assert.equal(result.verificationStatus, "No Match");
});

test("prefer GOOGLE_PLACES_API_KEY when both set", () => {
  const prevPlaces = process.env.GOOGLE_PLACES_API_KEY;
  const prevMaps = process.env.GOOGLE_MAPS_API_KEY;
  process.env.GOOGLE_PLACES_API_KEY = "places-key";
  process.env.GOOGLE_MAPS_API_KEY = "maps-key";
  assert.equal(resolveGoogleApiKey(), "places-key");
  process.env.GOOGLE_PLACES_API_KEY = prevPlaces;
  process.env.GOOGLE_MAPS_API_KEY = prevMaps;
});

test("max request guard blocks oversized runs", async () => {
  const candidates = Array.from({ length: 60 }, (_, i) =>
    point({ name: `Place ${i}`, sourceReference: `https://example.com/${i}` })
  );
  const result = await runGooglePlacesVerification({
    payload: { points: candidates, country: "Colombia" },
    options: {
      apiKey: "fake",
      country: "Colombia",
      cacheEnabled: false,
      maxRequests: 150,
      delayMs: 0,
      maxResults: 5,
      output: "report.json",
      verifiedOutput: "clean.json",
      searchTextFn: async () => [],
    },
  });
  assert.equal(result.ok, false);
  assert.equal(result.error, "max_requests_exceeded");
});

test("estimateVerificationApiRequests worst case", () => {
  const est = estimateVerificationApiRequests(10, 10, { maxSearchQueriesPerCandidate: 3 });
  assert.equal(est.estimatedMaxTextSearch, 30);
  assert.equal(est.estimatedMaxPlaceDetails, 10);
  assert.equal(est.estimatedMaxTotal, 40);
});

test("high-confidence match goes to clean output without google fields", () => {
  const candidate = point();
  const verification = {
    verificationStatus: "Verified",
    matchConfidence: "High",
    recommendedName: "Corferias Bogotá",
    recommendedLatitude: 4.6297,
    recommendedLongitude: -74.0935,
    googlePlaceId: "abc",
    googleMapsUri: "https://maps.google.com/?q=abc",
  };
  const out = buildVerifiedCleanPoint(candidate, verification, { allowMedium: false });
  assert.ok(out);
  assert.equal(Object.hasOwn(out, "googlePlaceId"), false);
  assert.match(out.notes, /Location verified against Google Maps during pre-import QA/);
});

test("medium-confidence excluded by default", () => {
  const verification = {
    verificationStatus: "Needs Review",
    matchConfidence: "Medium",
    recommendedName: "Corferias Bogotá",
    recommendedLatitude: 4.6297,
    recommendedLongitude: -74.0935,
  };
  assert.equal(buildVerifiedCleanPoint(point(), verification, { allowMedium: false }), null);
});

test("medium-confidence included with allowMedium and medium note", () => {
  const verification = {
    verificationStatus: "Needs Review",
    matchConfidence: "Medium",
    recommendedName: "Corferias Bogotá",
    recommendedLatitude: 4.6297,
    recommendedLongitude: -74.0935,
  };
  const out = buildVerifiedCleanPoint(point(), verification, { allowMedium: true });
  assert.ok(out);
  assert.match(out.notes, /medium confidence; manually reviewed before import/);
});

test("low-confidence and ambiguous excluded", () => {
  const low = buildVerifiedCleanPoint(
    point(),
    { verificationStatus: "Needs Review", matchConfidence: "Low", recommendedName: "X" },
    { allowMedium: false }
  );
  assert.equal(low, null);
  const amb = buildVerifiedCleanPoint(
    point(),
    { verificationStatus: "Ambiguous Match", matchConfidence: "Low", recommendedName: "X" },
    { allowMedium: true }
  );
  assert.equal(amb, null);
});

test("verification report contains google metadata envelope", () => {
  const report = buildVerificationReportPayload({
    country: "Colombia",
    inputFile: "fixtures/test.json",
    candidateCount: 1,
    records: [
      {
        candidateName: "Corferias",
        googleName: "Corferias Bogotá",
        googleLatitude: 4.62,
        googleLongitude: -74.09,
        verificationStatus: "Verified",
        matchConfidence: "High",
      },
    ],
    excludedRecords: 0,
    cacheHits: 0,
    cacheMisses: 1,
    apiRequestsMade: 2,
  });
  assert.match(report.verification.method, /Google Maps \/ Google Places/);
  assert.equal(report.verification.verifiedHighConfidence, 1);
  assert.equal(report.records[0].googleName, "Corferias Bogotá");
});

test("missing api key returns graceful error from runner", async () => {
  const prevPlaces = process.env.GOOGLE_PLACES_API_KEY;
  const prevMaps = process.env.GOOGLE_MAPS_API_KEY;
  delete process.env.GOOGLE_PLACES_API_KEY;
  delete process.env.GOOGLE_MAPS_API_KEY;
  const result = await runGooglePlacesVerification({
    payload: { points: [point()] },
    options: { country: "Colombia", maxRequests: 150 },
  });
  assert.equal(result.ok, false);
  assert.equal(result.error, "missing_api_key");
  process.env.GOOGLE_PLACES_API_KEY = prevPlaces;
  process.env.GOOGLE_MAPS_API_KEY = prevMaps;
});

test("dry-run makes no API calls", async () => {
  let calls = 0;
  const result = await runGooglePlacesVerification({
    payload: { points: [point()] },
    options: {
      apiKey: "fake",
      country: "Colombia",
      dryRun: true,
      cacheEnabled: false,
      maxRequests: 150,
      output: "r.json",
      verifiedOutput: "c.json",
      searchTextFn: async () => {
        calls += 1;
        return [];
      },
    },
  });
  assert.equal(result.ok, true);
  assert.equal(result.dryRun, true);
  assert.equal(calls, 0);
  assert.equal(result.report, null);
});

test("parseGoogleVerificationCli reads guardrail flags", () => {
  const cli = parseGoogleVerificationCli([
    "--file",
    "x.json",
    "--max-requests",
    "50",
    "--delay-ms",
    "100",
    "--no-cache",
    "--dry-run",
  ]);
  assert.equal(cli.maxRequests, 50);
  assert.equal(cli.delayMs, 100);
  assert.equal(cli.cacheEnabled, false);
  assert.equal(cli.dryRun, true);
});
