#!/usr/bin/env node
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildDominicanRepublicMatureTiDeltaFixture } from "../lib/radar-buildout/dominican-republic-mature-travel-infrastructure-delta.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const fixture = buildDominicanRepublicMatureTiDeltaFixture();

for (const rel of [
  "fixtures/travel-infrastructure-dominican-republic-mature-real.json",
  "public/fixtures/travel-infrastructure-dominican-republic-mature-real.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Dominican Republic Mature Pass TI:", fixture.points.length, "records");
