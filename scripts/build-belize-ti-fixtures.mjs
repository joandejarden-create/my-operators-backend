#!/usr/bin/env node
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildBelizeTiDeltaFixture } from "../lib/radar-buildout/belize-travel-infrastructure-delta.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const fixture = buildBelizeTiDeltaFixture();

for (const rel of [
  "fixtures/travel-infrastructure-belize-countrywide-real.json",
  "public/fixtures/travel-infrastructure-belize-countrywide-real.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Belize Countrywide TI:", fixture.points.length, "records");
