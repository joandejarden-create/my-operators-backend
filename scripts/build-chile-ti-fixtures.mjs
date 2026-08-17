#!/usr/bin/env node
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildChileTiDeltaFixture } from "../lib/radar-buildout/chile-travel-infrastructure-delta.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const fixture = buildChileTiDeltaFixture();

const paths = [
  "fixtures/travel-infrastructure-chile-santiago-real.json",
  "public/fixtures/travel-infrastructure-chile-santiago-real.json",
];

for (const rel of paths) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Chile Santiago TI delta:", fixture.points.length, "records");
console.log("By type:", fixture.summary.byPointType);
