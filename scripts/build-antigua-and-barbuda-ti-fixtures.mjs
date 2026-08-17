#!/usr/bin/env node
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildAntiguaAndBarbudaTiDeltaFixture } from "../lib/radar-buildout/antigua-and-barbuda-travel-infrastructure-delta.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const fixture = buildAntiguaAndBarbudaTiDeltaFixture();

for (const rel of [
  "fixtures/travel-infrastructure-antigua-and-barbuda-countrywide-real.json",
  "public/fixtures/travel-infrastructure-antigua-and-barbuda-countrywide-real.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Antigua and Barbuda TI delta:", fixture.points.length, "records");
