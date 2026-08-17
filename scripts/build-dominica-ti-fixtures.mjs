#!/usr/bin/env node
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildDominicaTiDeltaFixture } from "../lib/radar-buildout/dominica-travel-infrastructure-delta.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const fixture = buildDominicaTiDeltaFixture();

for (const rel of [
  "fixtures/travel-infrastructure-dominica-countrywide-real.json",
  "public/fixtures/travel-infrastructure-dominica-countrywide-real.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Dominica TI delta:", fixture.points.length, "records");
