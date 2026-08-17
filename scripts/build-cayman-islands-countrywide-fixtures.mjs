#!/usr/bin/env node
/**
 * Build Cayman Islands countrywide demand anchor candidate fixtures.
 */
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { BUILD_STRATEGY_TYPES } from "../lib/radar-buildout/country-build-strategies.js";
import { getCountryConfig } from "../lib/radar-buildout/country-configs.js";
import {
  getCaymanIslandsCandidates,
  CAYMAN_ISLANDS_SUBMARKETS,
} from "../lib/radar-buildout/cayman-islands-demand-anchors-candidates.js";
import { applyCaymanIslandsPlaceReviewCorrections } from "../lib/radar-buildout/cayman-islands-google-place-review-corrections.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const MARKET = "Cayman Islands Countrywide";
const config = getCountryConfig("Cayman Islands");

const points = applyCaymanIslandsPlaceReviewCorrections(getCaymanIslandsCandidates());
const bySubmarket = {};
const byPointType = {};
const byCity = {};
for (const p of points) {
  bySubmarket[p.submarket] = (bySubmarket[p.submarket] || 0) + 1;
  byPointType[p.pointType] = (byPointType[p.pointType] || 0) + 1;
  byCity[p.city] = (byCity[p.city] || 0) + 1;
}

const fixture = {
  country: "Cayman Islands",
  region: "Caribbean",
  market: MARKET,
  buildStrategy: BUILD_STRATEGY_TYPES.ISLAND_COUNTRYWIDE,
  submarkets: CAYMAN_ISLANDS_SUBMARKETS,
  firstPassTargets: config?.targets || {},
  governanceRequired: true,
  googlePreImportVerificationRecommended: true,
  generatedAt: new Date().toISOString().slice(0, 10),
  status: "candidate_pre_verification",
  summary: { totalPoints: points.length, bySubmarket, byCity, byPointType },
  points,
};

for (const rel of [
  "fixtures/demand-anchors-cayman-islands-countrywide-candidates.json",
  "public/fixtures/demand-anchors-cayman-islands-countrywide-candidates.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Cayman Islands countrywide candidates written:", points.length);
console.log("By submarket:", bySubmarket);
console.log("By point type:", byPointType);
