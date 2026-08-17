#!/usr/bin/env node
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildPuertoVallartaRivieraNayaritTiDeltaFixture } from "../lib/radar-buildout/puerto-vallarta-riviera-nayarit-travel-infrastructure-delta.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const fixture = buildPuertoVallartaRivieraNayaritTiDeltaFixture();

for (const rel of [
  "fixtures/travel-infrastructure-puerto-vallarta-riviera-nayarit-real.json",
  "public/fixtures/travel-infrastructure-puerto-vallarta-riviera-nayarit-real.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
}

console.log("Puerto Vallarta / Riviera Nayarit TI:", fixture.points.length, "records");
