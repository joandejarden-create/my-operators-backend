#!/usr/bin/env node
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildCostaRicaTiDeltaFixture } from "../lib/radar-buildout/costa-rica-travel-infrastructure-delta.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const fixture = buildCostaRicaTiDeltaFixture();

const paths = [
  "fixtures/travel-infrastructure-costa-rica-countrywide-real.json",
  "public/fixtures/travel-infrastructure-costa-rica-countrywide-real.json",
];

for (const rel of paths) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Costa Rica TI delta:", fixture.points.length, "records");
console.log("By type:", fixture.summary.byPointType);
