#!/usr/bin/env node
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildPanamaDemandAnchorDeltaFixture } from "../lib/radar-buildout/panama-demand-anchors-delta.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const fixture = buildPanamaDemandAnchorDeltaFixture();

const path = "fixtures/demand-anchors-panama-countrywide-delta-real.json";
writeFileSync(join(root, path), JSON.stringify(fixture, null, 2) + "\n");
writeFileSync(join(root, "public", path), JSON.stringify(fixture, null, 2) + "\n");

console.log("Panama DA delta:", fixture.points.length, "records");
