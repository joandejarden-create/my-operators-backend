#!/usr/bin/env node
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildMonterreyTiDeltaFixture } from "../lib/radar-buildout/monterrey-travel-infrastructure-delta.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const fixture = buildMonterreyTiDeltaFixture();

for (const rel of [
  "fixtures/travel-infrastructure-monterrey-real.json",
  "public/fixtures/travel-infrastructure-monterrey-real.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Monterrey TI:", fixture.points.length, "records");
