#!/usr/bin/env node
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildHondurasTiDeltaFixture } from "../lib/radar-buildout/honduras-travel-infrastructure-delta.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const fixture = buildHondurasTiDeltaFixture();

for (const rel of [
  "fixtures/travel-infrastructure-honduras-countrywide-real.json",
  "public/fixtures/travel-infrastructure-honduras-countrywide-real.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Honduras Countrywide TI:", fixture.points.length, "records");
