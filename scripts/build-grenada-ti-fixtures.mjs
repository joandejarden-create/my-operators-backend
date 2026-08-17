#!/usr/bin/env node
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildGrenadaTiDeltaFixture } from "../lib/radar-buildout/grenada-travel-infrastructure-delta.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const fixture = buildGrenadaTiDeltaFixture();

for (const rel of [
  "fixtures/travel-infrastructure-grenada-countrywide-real.json",
  "public/fixtures/travel-infrastructure-grenada-countrywide-real.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Grenada TI delta:", fixture.points.length, "records");
