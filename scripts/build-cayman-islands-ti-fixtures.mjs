#!/usr/bin/env node

import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildCaymanIslandsTiDeltaFixture } from "../lib/radar-buildout/cayman-islands-travel-infrastructure-delta.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const fixture = buildCaymanIslandsTiDeltaFixture();

for (const rel of [
  "fixtures/travel-infrastructure-cayman-islands-countrywide-real.json",
  "public/fixtures/travel-infrastructure-cayman-islands-countrywide-real.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Cayman Islands TI delta:", fixture.points.length, "records");
console.log("By type:", fixture.summary.byPointType);
