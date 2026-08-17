#!/usr/bin/env node

import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildBarbadosTiDeltaFixture } from "../lib/radar-buildout/barbados-travel-infrastructure-delta.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const fixture = buildBarbadosTiDeltaFixture();

for (const rel of [
  "fixtures/travel-infrastructure-barbados-countrywide-real.json",
  "public/fixtures/travel-infrastructure-barbados-countrywide-real.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Barbados TI delta:", fixture.points.length, "records");
console.log("By type:", fixture.summary.byPointType);
