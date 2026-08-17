#!/usr/bin/env node
/**
 * Build Curaçao countrywide demand anchor candidate fixtures.
 */
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { BUILD_STRATEGY_TYPES } from "../lib/radar-buildout/country-build-strategies.js";
import { getCountryConfig } from "../lib/radar-buildout/country-configs.js";
import {
  getCuracaoCandidates,
  CURACAO_SUBMARKETS,
} from "../lib/radar-buildout/curacao-demand-anchors-candidates.js";
import { applyCuracaoPlaceReviewCorrections } from "../lib/radar-buildout/curacao-google-place-review-corrections.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const MARKET = "Curaçao Countrywide";
const config = getCountryConfig("Curaçao");

const points = applyCuracaoPlaceReviewCorrections(getCuracaoCandidates());
const bySubmarket = {};
const byPointType = {};
const byCity = {};
for (const p of points) {
  bySubmarket[p.submarket] = (bySubmarket[p.submarket] || 0) + 1;
  byPointType[p.pointType] = (byPointType[p.pointType] || 0) + 1;
  byCity[p.city] = (byCity[p.city] || 0) + 1;
}

const fixture = {
  country: "Curaçao",
  region: "Caribbean",
  market: MARKET,
  buildStrategy: BUILD_STRATEGY_TYPES.ISLAND_COUNTRYWIDE,
  submarkets: CURACAO_SUBMARKETS,
  firstPassTargets: config?.targets || {},
  governanceRequired: true,
  googlePreImportVerificationRecommended: true,
  generatedAt: new Date().toISOString().slice(0, 10),
  status: "candidate_pre_verification",
  summary: {
    totalPoints: points.length,
    bySubmarket,
    byCity,
    byPointType,
  },
  points,
};

const paths = [
  "fixtures/demand-anchors-curacao-countrywide-candidates.json",
  "public/fixtures/demand-anchors-curacao-countrywide-candidates.json",
];

for (const rel of paths) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Curaçao countrywide candidates written:", points.length);
console.log("By submarket:", bySubmarket);
console.log("By point type:", byPointType);
