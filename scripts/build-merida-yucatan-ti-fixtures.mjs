#!/usr/bin/env node
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildMeridaYucatanTiDeltaFixture } from "../lib/radar-buildout/merida-yucatan-travel-infrastructure-delta.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const fixture = buildMeridaYucatanTiDeltaFixture();

for (const rel of [
  "fixtures/travel-infrastructure-merida-yucatan-real.json",
  "public/fixtures/travel-infrastructure-merida-yucatan-real.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Mérida / Yucatán TI:", fixture.points.length, "records");
