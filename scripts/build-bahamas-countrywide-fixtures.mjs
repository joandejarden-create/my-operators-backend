#!/usr/bin/env node

/**

 * Build Bahamas countrywide demand anchor candidate fixtures.

 */

import { writeFileSync } from "fs";

import { join, dirname } from "path";

import { fileURLToPath } from "url";

import { BUILD_STRATEGY_TYPES } from "../lib/radar-buildout/country-build-strategies.js";

import { getCountryConfig } from "../lib/radar-buildout/country-configs.js";

import {

  getBahamasCandidates,

  BAHAMAS_SUBMARKETS,

} from "../lib/radar-buildout/bahamas-demand-anchors-candidates.js";

import { applyBahamasPlaceReviewCorrections } from "../lib/radar-buildout/bahamas-google-place-review-corrections.js";



const __dirname = dirname(fileURLToPath(import.meta.url));

const root = join(__dirname, "..");

const MARKET = "Bahamas Countrywide";

const config = getCountryConfig("Bahamas");



const points = applyBahamasPlaceReviewCorrections(getBahamasCandidates());

const bySubmarket = {};

const byPointType = {};

const byCity = {};

for (const p of points) {

  bySubmarket[p.submarket] = (bySubmarket[p.submarket] || 0) + 1;

  byPointType[p.pointType] = (byPointType[p.pointType] || 0) + 1;

  byCity[p.city] = (byCity[p.city] || 0) + 1;

}



const fixture = {

  country: "Bahamas",

  region: "Caribbean",

  market: MARKET,

  buildStrategy: BUILD_STRATEGY_TYPES.ISLAND_COUNTRYWIDE,

  submarkets: BAHAMAS_SUBMARKETS,

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

  "fixtures/demand-anchors-bahamas-countrywide-candidates.json",

  "public/fixtures/demand-anchors-bahamas-countrywide-candidates.json",

];



for (const rel of paths) {

  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");

}



console.log("Bahamas countrywide candidates written:", points.length);

console.log("By submarket:", bySubmarket);

console.log("By point type:", byPointType);

