#!/usr/bin/env node
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildColombiaTiDeltaFixture } from "../lib/radar-buildout/colombia-travel-infrastructure-delta.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const fixture = buildColombiaTiDeltaFixture();

for (const rel of [
  "fixtures/travel-infrastructure-colombia-countrywide-real.json",
  "public/fixtures/travel-infrastructure-colombia-countrywide-real.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Colombia Countrywide TI:", fixture.points.length, "records");
