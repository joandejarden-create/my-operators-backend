#!/usr/bin/env node
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildGuatemalaTiDeltaFixture } from "../lib/radar-buildout/guatemala-travel-infrastructure-delta.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const fixture = buildGuatemalaTiDeltaFixture();

for (const rel of [
  "fixtures/travel-infrastructure-guatemala-countrywide-real.json",
  "public/fixtures/travel-infrastructure-guatemala-countrywide-real.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Guatemala Countrywide TI:", fixture.points.length, "records");
