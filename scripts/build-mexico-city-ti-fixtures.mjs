#!/usr/bin/env node
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildMexicoCityTiDeltaFixture } from "../lib/radar-buildout/mexico-city-travel-infrastructure-delta.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const fixture = buildMexicoCityTiDeltaFixture();

for (const rel of [
  "fixtures/travel-infrastructure-mexico-city-real.json",
  "public/fixtures/travel-infrastructure-mexico-city-real.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Mexico City TI:", fixture.points.length, "records");
