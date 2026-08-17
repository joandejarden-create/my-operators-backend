#!/usr/bin/env node
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildUruguayTiDeltaFixture } from "../lib/radar-buildout/uruguay-travel-infrastructure-delta.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const fixture = buildUruguayTiDeltaFixture();

for (const rel of [
  "fixtures/travel-infrastructure-uruguay-countrywide-real.json",
  "public/fixtures/travel-infrastructure-uruguay-countrywide-real.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Uruguay Countrywide TI:", fixture.points.length, "records");
console.log("Region:", fixture.region);
console.log("By type:", fixture.summary.byPointType);
