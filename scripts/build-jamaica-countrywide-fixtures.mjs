#!/usr/bin/env node

/**

 * Build Jamaica countrywide demand anchor candidate fixtures.

 */

import { writeFileSync } from "fs";

import { join, dirname } from "path";

import { fileURLToPath } from "url";

import { BUILD_STRATEGY_TYPES } from "../lib/radar-buildout/country-build-strategies.js";

import { getCountryConfig } from "../lib/radar-buildout/country-configs.js";

import {

  getJamaicaCandidates,

  JAMAICA_SUBMARKETS,

} from "../lib/radar-buildout/jamaica-demand-anchors-candidates.js";

import { applyJamaicaPlaceReviewCorrections } from "../lib/radar-buildout/jamaica-google-place-review-corrections.js";



const __dirname = dirname(fileURLToPath(import.meta.url));

const root = join(__dirname, "..");

const MARKET = "Jamaica Countrywide";

const config = getCountryConfig("Jamaica");



const points = applyJamaicaPlaceReviewCorrections(getJamaicaCandidates());

const bySubmarket = {};

const byPointType = {};

const byCity = {};

for (const p of points) {

  bySubmarket[p.submarket] = (bySubmarket[p.submarket] || 0) + 1;

  byPointType[p.pointType] = (byPointType[p.pointType] || 0) + 1;

  byCity[p.city] = (byCity[p.city] || 0) + 1;

}



const fixture = {

  country: "Jamaica",

  region: "Caribbean",

  market: MARKET,

  buildStrategy: BUILD_STRATEGY_TYPES.CORRIDOR_BASED,

  submarkets: JAMAICA_SUBMARKETS,

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

  "fixtures/demand-anchors-jamaica-countrywide-candidates.json",

  "public/fixtures/demand-anchors-jamaica-countrywide-candidates.json",

];



for (const rel of paths) {

  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");

}



console.log("Jamaica countrywide candidates written:", points.length);

console.log("By submarket:", bySubmarket);

console.log("By point type:", byPointType);

