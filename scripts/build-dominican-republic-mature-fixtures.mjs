#!/usr/bin/env node
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { BUILD_STRATEGY_TYPES } from "../lib/radar-buildout/country-build-strategies.js";
import { getCountryConfig } from "../lib/radar-buildout/country-configs.js";
import {
  getDominicanRepublicMatureCandidates,
  DOMINICAN_REPUBLIC_MATURE_SUBMARKETS,
} from "../lib/radar-buildout/dominican-republic-mature-demand-anchors-candidates.js";
import { applyDominicanRepublicMaturePlaceReviewCorrections } from "../lib/radar-buildout/dominican-republic-mature-google-place-review-corrections.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const MARKET = "Dominican Republic Mature Pass";
const config = getCountryConfig("Dominican Republic");

const points = applyDominicanRepublicMaturePlaceReviewCorrections(getDominicanRepublicMatureCandidates());
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
  country: "Dominican Republic",
  region: "Caribbean",
  market: MARKET,
  buildStrategy: strategyMap["CORRIDOR_BASED"] || BUILD_STRATEGY_TYPES.MARKET_BY_MARKET,
  submarkets: DOMINICAN_REPUBLIC_MATURE_SUBMARKETS,
  firstPassTargets: config?.targets || {},
  governanceRequired: true,
  googlePreImportVerificationRecommended: true,
  generatedAt: new Date().toISOString().slice(0, 10),
  status: "candidate_pre_verification",
  summary: { totalPoints: points.length, bySubmarket, byCity, byPointType },
  points,
};

for (const rel of [
  "fixtures/demand-anchors-dominican-republic-mature-candidates.json",
  "public/fixtures/demand-anchors-dominican-republic-mature-candidates.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Dominican Republic Mature Pass candidates written:", points.length);
