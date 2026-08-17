#!/usr/bin/env node
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildNicaraguaTiDeltaFixture } from "../lib/radar-buildout/nicaragua-travel-infrastructure-delta.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const fixture = buildNicaraguaTiDeltaFixture();

for (const rel of [
  "fixtures/travel-infrastructure-nicaragua-countrywide-real.json",
  "public/fixtures/travel-infrastructure-nicaragua-countrywide-real.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Nicaragua Countrywide TI:", fixture.points.length, "records");
