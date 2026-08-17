#!/usr/bin/env node
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildLosCabosTiDeltaFixture } from "../lib/radar-buildout/los-cabos-travel-infrastructure-delta.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const fixture = buildLosCabosTiDeltaFixture();

for (const rel of [
  "fixtures/travel-infrastructure-los-cabos-real.json",
  "public/fixtures/travel-infrastructure-los-cabos-real.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Los Cabos TI:", fixture.points.length, "records");
