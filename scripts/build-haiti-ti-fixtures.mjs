#!/usr/bin/env node
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildHaitiTiDeltaFixture } from "../lib/radar-buildout/haiti-travel-infrastructure-delta.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const fixture = buildHaitiTiDeltaFixture();

for (const rel of [
  "fixtures/travel-infrastructure-haiti-real.json",
  "public/fixtures/travel-infrastructure-haiti-real.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Haiti Countrywide TI:", fixture.points.length, "records");
