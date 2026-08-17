#!/usr/bin/env node
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildTrinidadAndTobagoTiDeltaFixture } from "../lib/radar-buildout/trinidad-and-tobago-travel-infrastructure-delta.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const fixture = buildTrinidadAndTobagoTiDeltaFixture();

for (const rel of [
  "fixtures/travel-infrastructure-trinidad-and-tobago-countrywide-real.json",
  "public/fixtures/travel-infrastructure-trinidad-and-tobago-countrywide-real.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Trinidad and Tobago TI delta:", fixture.points.length, "records");
