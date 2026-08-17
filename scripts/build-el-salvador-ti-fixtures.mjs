#!/usr/bin/env node
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildElSalvadorTiDeltaFixture } from "../lib/radar-buildout/el-salvador-travel-infrastructure-delta.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const fixture = buildElSalvadorTiDeltaFixture();

for (const rel of [
  "fixtures/travel-infrastructure-el-salvador-countrywide-real.json",
  "public/fixtures/travel-infrastructure-el-salvador-countrywide-real.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("El Salvador Countrywide TI:", fixture.points.length, "records");
