#!/usr/bin/env node
import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, writeFileSync, rmSync } from "fs";
import { join } from "path";
import { tmpdir } from "os";
import {
  buildVerificationCacheKey,
  loadVerificationCache,
  saveVerificationCache,
  serializeCacheEntry,
  deserializeCacheEntry,
} from "../lib/location-verification/google-verification-cache.js";
import { runGooglePlacesVerification } from "../lib/location-verification/run-google-places-verification.js";

test("cache key normalizes country city name", () => {
  const key = buildVerificationCacheKey({
    name: "Corferias Bogotá",
    city: "Bogotá",
    country: "Colombia",
  });
  assert.equal(key, "colombia|bogota|corferias bogota");
});

test("cache round-trip stores verification without secrets", () => {
  const dir = mkdtempSync(join(tmpdir(), "google-cache-"));
  const cachePath = join(dir, "cache.json");
  const verification = {
    candidateName: "Corferias",
    candidateCity: "Bogotá",
    candidateCountry: "Colombia",
    googleName: "Corferias Bogotá",
    verificationStatus: "Verified",
    matchConfidence: "High",
    recommendedName: "Corferias Bogotá",
    recommendedLatitude: 4.62,
    recommendedLongitude: -74.09,
  };
  const doc = loadVerificationCache(cachePath);
  const key = buildVerificationCacheKey({
    name: "Corferias",
    city: "Bogotá",
    country: "Colombia",
  });
  doc.entries[key] = serializeCacheEntry(verification, "Corferias, Bogotá, Colombia");
  saveVerificationCache(doc, cachePath);

  const raw = JSON.parse(readFileSync(cachePath, "utf8"));
  assert.equal(Object.hasOwn(raw, "apiKey"), false);
  assert.ok(raw.entries[key]);

  const restored = deserializeCacheEntry(raw.entries[key]);
  assert.equal(restored.matchConfidence, "High");
  assert.equal(restored._cacheHit, true);
  rmSync(dir, { recursive: true, force: true });
});

test("cache hit avoids live verification calls", async () => {
  const dir = mkdtempSync(join(tmpdir(), "google-cache-"));
  const cachePath = join(dir, "cache.json");
  const candidate = {
    name: "Corferias Bogotá",
    city: "Bogotá",
    country: "Colombia",
    pointType: "Convention Center",
    latitude: 4.6298,
    longitude: -74.0934,
    sourceReference: "https://corferias.com/",
  };
  const key = buildVerificationCacheKey(candidate);
  const doc = loadVerificationCache(cachePath);
  doc.entries[key] = serializeCacheEntry(
    {
      candidateName: candidate.name,
      candidateCity: candidate.city,
      candidateCountry: candidate.country,
      candidateLatitude: candidate.latitude,
      candidateLongitude: candidate.longitude,
      candidatePointType: candidate.pointType,
      verificationStatus: "Verified",
      matchConfidence: "High",
      verificationNotes: "cached",
      recommendedName: candidate.name,
      recommendedLatitude: candidate.latitude,
      recommendedLongitude: candidate.longitude,
    },
    "Corferias, Bogotá, Colombia"
  );
  saveVerificationCache(doc, cachePath);

  let apiCalls = 0;
  const result = await runGooglePlacesVerification({
    payload: { points: [candidate], country: "Colombia" },
    options: {
      apiKey: "fake",
      country: "Colombia",
      cacheEnabled: true,
      cachePath,
      maxRequests: 150,
      delayMs: 0,
      maxResults: 5,
      output: "report.json",
      verifiedOutput: "clean.json",
      searchTextFn: async () => {
        apiCalls += 1;
        return [];
      },
    },
  });

  assert.equal(result.ok, true);
  assert.equal(apiCalls, 0);
  assert.equal(result.summary.cacheHits, 1);
  assert.equal(result.summary.verifiedRecords, 1);
  rmSync(dir, { recursive: true, force: true });
});
