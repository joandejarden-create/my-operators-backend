#!/usr/bin/env node
/**
 * Build British Virgin Islands countrywide demand anchor candidate fixtures.
 */
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { BUILD_STRATEGY_TYPES } from "../lib/radar-buildout/country-build-strategies.js";
import { getCountryConfig } from "../lib/radar-buildout/country-configs.js";
import {
  getBritishVirginIslandsCandidates,
  BRITISH_VIRGIN_ISLANDS_SUBMARKETS,
} from "../lib/radar-buildout/british-virgin-islands-demand-anchors-candidates.js";
import { applyBritishVirginIslandsPlaceReviewCorrections } from "../lib/radar-buildout/british-virgin-islands-google-place-review-corrections.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const MARKET = "British Virgin Islands Countrywide";
const config = getCountryConfig("British Virgin Islands");

const points = applyBritishVirginIslandsPlaceReviewCorrections(getBritishVirginIslandsCandidates());
const bySubmarket = {};
const byPointType = {};
const byCity = {};
for (const p of points) {
  bySubmarket[p.submarket] = (bySubmarket[p.submarket] || 0) + 1;
  byPointType[p.pointType] = (byPointType[p.pointType] || 0) + 1;
  byCity[p.city] = (byCity[p.city] || 0) + 1;
}

const fixture = {
  country: "British Virgin Islands",
  region: "Caribbean",
  market: MARKET,
  buildStrategy: BUILD_STRATEGY_TYPES.ISLAND_COUNTRYWIDE,
  submarkets: BRITISH_VIRGIN_ISLANDS_SUBMARKETS,
  firstPassTargets: config?.targets || {},
  governanceRequired: true,
  googlePreImportVerificationRecommended: true,
  generatedAt: new Date().toISOString().slice(0, 10),
  status: "candidate_pre_verification",
  summary: { totalPoints: points.length, bySubmarket, byCity, byPointType },
  points,
};

for (const rel of [
  "fixtures/demand-anchors-british-virgin-islands-countrywide-candidates.json",
  "public/fixtures/demand-anchors-british-virgin-islands-countrywide-candidates.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("British Virgin Islands countrywide candidates written:", points.length);
console.log("By submarket:", bySubmarket);
