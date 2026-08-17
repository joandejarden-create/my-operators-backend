#!/usr/bin/env node
/**
 * Build Saint Kitts and Nevis countrywide demand anchor candidate fixtures.
 */
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { BUILD_STRATEGY_TYPES } from "../lib/radar-buildout/country-build-strategies.js";
import { getCountryConfig } from "../lib/radar-buildout/country-configs.js";
import {
  getSaintKittsAndNevisCandidates,
  SAINT_KITTS_AND_NEVIS_SUBMARKETS,
} from "../lib/radar-buildout/saint-kitts-and-nevis-demand-anchors-candidates.js";
import { applySaintKittsAndNevisPlaceReviewCorrections } from "../lib/radar-buildout/saint-kitts-and-nevis-google-place-review-corrections.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const MARKET = "Saint Kitts and Nevis Countrywide";
const config = getCountryConfig("Saint Kitts and Nevis");

const points = applySaintKittsAndNevisPlaceReviewCorrections(getSaintKittsAndNevisCandidates());
const bySubmarket = {};
const byPointType = {};
const byCity = {};
for (const p of points) {
  bySubmarket[p.submarket] = (bySubmarket[p.submarket] || 0) + 1;
  byPointType[p.pointType] = (byPointType[p.pointType] || 0) + 1;
  byCity[p.city] = (byCity[p.city] || 0) + 1;
}

const fixture = {
  country: "Saint Kitts and Nevis",
  region: "Caribbean",
  market: MARKET,
  buildStrategy: BUILD_STRATEGY_TYPES.ISLAND_COUNTRYWIDE,
  submarkets: SAINT_KITTS_AND_NEVIS_SUBMARKETS,
  firstPassTargets: config?.targets || {},
  governanceRequired: true,
  googlePreImportVerificationRecommended: true,
  generatedAt: new Date().toISOString().slice(0, 10),
  status: "candidate_pre_verification",
  summary: { totalPoints: points.length, bySubmarket, byCity, byPointType },
  points,
};

for (const rel of [
  "fixtures/demand-anchors-saint-kitts-and-nevis-countrywide-candidates.json",
  "public/fixtures/demand-anchors-saint-kitts-and-nevis-countrywide-candidates.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Saint Kitts and Nevis countrywide candidates written:", points.length);
console.log("By submarket:", bySubmarket);
