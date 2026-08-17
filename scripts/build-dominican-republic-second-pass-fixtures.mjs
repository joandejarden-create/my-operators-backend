#!/usr/bin/env node
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { DR_DEMAND_ANCHORS_SECOND_PASS } from "../lib/radar-buildout/dominican-republic-demand-anchors-second-pass.js";
import { DR_TRAVEL_INFRA_SECOND_PASS } from "../lib/radar-buildout/dominican-republic-travel-infra-second-pass.js";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");

const daFixture = {
  market: "Dominican Republic",
  country: "Dominican Republic",
  region: "Caribbean",
  pass: "second",
  points: DR_DEMAND_ANCHORS_SECOND_PASS,
};

const tiFixture = {
  market: "Dominican Republic",
  country: "Dominican Republic",
  region: "Caribbean",
  pass: "second",
  points: DR_TRAVEL_INFRA_SECOND_PASS,
};

const paths = [
  ["fixtures/demand-anchors-dominican-republic-second-pass-real.json", daFixture],
  ["public/fixtures/demand-anchors-dominican-republic-second-pass-real.json", daFixture],
  ["fixtures/travel-infrastructure-dominican-republic-second-pass-real.json", tiFixture],
  ["public/fixtures/travel-infrastructure-dominican-republic-second-pass-real.json", tiFixture],
];

for (const [rel, data] of paths) {
  writeFileSync(join(root, rel), JSON.stringify(data, null, 2) + "\n");
}
console.log("DR second-pass fixtures:", DR_DEMAND_ANCHORS_SECOND_PASS.length, "DA,", DR_TRAVEL_INFRA_SECOND_PASS.length, "TI");
