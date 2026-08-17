#!/usr/bin/env node
/**
 * Build Mexico — Cancún / Riviera Maya demand anchor candidate fixtures.
 *   node scripts/build-mexico-cancun-riviera-maya-fixtures.mjs
 *   node scripts/build-mexico-cancun-riviera-maya-fixtures.mjs --batch 1
 *   node scripts/build-mexico-cancun-riviera-maya-fixtures.mjs --batch all
 */
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { BUILD_STRATEGY_TYPES } from "../lib/radar-buildout/country-build-strategies.js";
import { getCountryConfig } from "../lib/radar-buildout/country-configs.js";
import {
  getMexicoCancunCandidates,
  MEXICO_CANCUN_SUBMARKETS,
} from "../lib/radar-buildout/mexico-cancun-demand-anchors-candidates.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const MARKET = "Cancún / Riviera Maya";
const config = getCountryConfig("Mexico");

const args = process.argv.slice(2);
const batchArg = args.find((a) => a.startsWith("--batch"));
const batch = batchArg
  ? batchArg.includes("=")
    ? batchArg.split("=")[1]
    : args[args.indexOf("--batch") + 1] || "1"
  : args.includes("--all")
    ? "all"
    : "1";

import { applyMexicoCancunPlaceReviewCorrections } from "../lib/radar-buildout/mexico-cancun-google-place-review-corrections.js";

const points = applyMexicoCancunPlaceReviewCorrections(getMexicoCancunCandidates(batch));
const bySubmarket = {};
const byPointType = {};
const byCity = {};
for (const p of points) {
  bySubmarket[p.submarket] = (bySubmarket[p.submarket] || 0) + 1;
  byPointType[p.pointType] = (byPointType[p.pointType] || 0) + 1;
  byCity[p.city] = (byCity[p.city] || 0) + 1;
}

const fixture = {
  country: "Mexico",
  region: config?.region || "North America",
  market: MARKET,
  buildStrategy: BUILD_STRATEGY_TYPES.MARKET_BY_MARKET,
  buildBatch: String(batch),
  submarkets: MEXICO_CANCUN_SUBMARKETS,
  firstPassTargets: config?.marketTargets?.[MARKET] || config?.targets || {},
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
  "fixtures/demand-anchors-mexico-cancun-riviera-maya-candidates.json",
  "public/fixtures/demand-anchors-mexico-cancun-riviera-maya-candidates.json",
];

for (const rel of paths) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Mexico Cancún / Riviera Maya candidates written:", points.length, `(batch ${batch})`);
console.log("By submarket:", bySubmarket);
console.log("By point type:", byPointType);
