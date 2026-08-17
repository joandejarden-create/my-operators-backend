#!/usr/bin/env node

import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildTurksAndCaicosTiDeltaFixture } from "../lib/radar-buildout/turks-and-caicos-travel-infrastructure-delta.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const fixture = buildTurksAndCaicosTiDeltaFixture();

for (const rel of [
  "fixtures/travel-infrastructure-turks-and-caicos-countrywide-real.json",
  "public/fixtures/travel-infrastructure-turks-and-caicos-countrywide-real.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Turks & Caicos TI delta:", fixture.points.length, "records");
console.log("By type:", fixture.summary.byPointType);
