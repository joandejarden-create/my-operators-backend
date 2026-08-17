#!/usr/bin/env node
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildCubaTiDeltaFixture } from "../lib/radar-buildout/cuba-travel-infrastructure-delta.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const fixture = buildCubaTiDeltaFixture();

for (const rel of [
  "fixtures/travel-infrastructure-cuba-real.json",
  "public/fixtures/travel-infrastructure-cuba-real.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Cuba Countrywide TI:", fixture.points.length, "records");
