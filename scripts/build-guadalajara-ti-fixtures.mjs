#!/usr/bin/env node
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildGuadalajaraTiDeltaFixture } from "../lib/radar-buildout/guadalajara-travel-infrastructure-delta.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const fixture = buildGuadalajaraTiDeltaFixture();

for (const rel of [
  "fixtures/travel-infrastructure-guadalajara-real.json",
  "public/fixtures/travel-infrastructure-guadalajara-real.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Guadalajara TI:", fixture.points.length, "records");
