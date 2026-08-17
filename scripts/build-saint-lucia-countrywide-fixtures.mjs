#!/usr/bin/env node
/**
 * Build Saint Lucia countrywide demand anchor candidate fixtures.
 */
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { BUILD_STRATEGY_TYPES } from "../lib/radar-buildout/country-build-strategies.js";
import { getCountryConfig } from "../lib/radar-buildout/country-configs.js";
import {
  getSaintLuciaCandidates,
  SAINT_LUCIA_SUBMARKETS,
} from "../lib/radar-buildout/saint-lucia-demand-anchors-candidates.js";
import { applySaintLuciaPlaceReviewCorrections } from "../lib/radar-buildout/saint-lucia-google-place-review-corrections.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const MARKET = "Saint Lucia Countrywide";
const config = getCountryConfig("Saint Lucia");

const points = applySaintLuciaPlaceReviewCorrections(getSaintLuciaCandidates());
const bySubmarket = {};
const byPointType = {};
const byCity = {};
for (const p of points) {
  bySubmarket[p.submarket] = (bySubmarket[p.submarket] || 0) + 1;
  byPointType[p.pointType] = (byPointType[p.pointType] || 0) + 1;
  byCity[p.city] = (byCity[p.city] || 0) + 1;
}

const fixture = {
  country: "Saint Lucia",
  region: "Caribbean",
  market: MARKET,
  buildStrategy: BUILD_STRATEGY_TYPES.ISLAND_COUNTRYWIDE,
  submarkets: SAINT_LUCIA_SUBMARKETS,
  firstPassTargets: config?.targets || {},
  governanceRequired: true,
  googlePreImportVerificationRecommended: true,
  generatedAt: new Date().toISOString().slice(0, 10),
  status: "candidate_pre_verification",
  summary: { totalPoints: points.length, bySubmarket, byCity, byPointType },
  points,
};

for (const rel of [
  "fixtures/demand-anchors-saint-lucia-countrywide-candidates.json",
  "public/fixtures/demand-anchors-saint-lucia-countrywide-candidates.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Saint Lucia countrywide candidates written:", points.length);
console.log("By submarket:", bySubmarket);
