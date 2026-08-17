#!/usr/bin/env node
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { BUILD_STRATEGY_TYPES } from "../lib/radar-buildout/country-build-strategies.js";
import { getCountryConfig } from "../lib/radar-buildout/country-configs.js";
import {
  getMeridaYucatanCandidates,
  MERIDA_YUCATAN_SUBMARKETS,
} from "../lib/radar-buildout/merida-yucatan-demand-anchors-candidates.js";
import { applyMeridaYucatanPlaceReviewCorrections } from "../lib/radar-buildout/merida-yucatan-google-place-review-corrections.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const MARKET = "Mérida / Yucatán";
const config = getCountryConfig("Mexico");

const points = applyMeridaYucatanPlaceReviewCorrections(getMeridaYucatanCandidates());
const bySubmarket = {};
const byPointType = {};
const byCity = {};
for (const p of points) {
  bySubmarket[p.submarket] = (bySubmarket[p.submarket] || 0) + 1;
  byPointType[p.pointType] = (byPointType[p.pointType] || 0) + 1;
  byCity[p.city] = (byCity[p.city] || 0) + 1;
}

const strategyMap = {
  MARKET_BY_MARKET: BUILD_STRATEGY_TYPES.MARKET_BY_MARKET,
  ISLAND_COUNTRYWIDE: BUILD_STRATEGY_TYPES.ISLAND_COUNTRYWIDE,
  CORRIDOR_BASED: BUILD_STRATEGY_TYPES.CORRIDOR_BASED,
};

const fixture = {
  country: "Mexico",
  region: config?.region || "North America",
  market: MARKET,
  buildStrategy: strategyMap["MARKET_BY_MARKET"] || BUILD_STRATEGY_TYPES.MARKET_BY_MARKET,
  submarkets: MERIDA_YUCATAN_SUBMARKETS,
  firstPassTargets: config?.targets || {},
  governanceRequired: true,
  googlePreImportVerificationRecommended: true,
  generatedAt: new Date().toISOString().slice(0, 10),
  status: "candidate_pre_verification",
  summary: { totalPoints: points.length, bySubmarket, byCity, byPointType },
  points,
};

for (const rel of [
  "fixtures/demand-anchors-merida-yucatan-candidates.json",
  "public/fixtures/demand-anchors-merida-yucatan-candidates.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Mérida / Yucatán candidates written:", points.length);
