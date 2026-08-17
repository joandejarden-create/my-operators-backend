#!/usr/bin/env node
/**
 * Build Mexico Cancún delta demand anchor candidate fixtures.
 *   node scripts/build-mexico-cancun-delta-fixtures.mjs
 */
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  buildMexicoCancunDeltaCandidates,
  stripGoogleFieldsFromPoints,
} from "../lib/radar-buildout/mexico-cancun-delta-candidates.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

const fixture = buildMexicoCancunDeltaCandidates();
const candidateFixture = {
  ...fixture,
  points: fixture.points,
};

const paths = [
  "fixtures/demand-anchors-mexico-cancun-riviera-maya-delta-candidates.json",
  "public/fixtures/demand-anchors-mexico-cancun-riviera-maya-delta-candidates.json",
];

for (const rel of paths) {
  writeFileSync(join(root, rel), JSON.stringify(candidateFixture, null, 2) + "\n");
}

console.log("Mexico Cancún delta candidates:", fixture.summary.netNewCandidates);
console.log("Skipped (already imported):", fixture.summary.skippedAlreadyImported);
console.log("By submarket:", fixture.summary.bySubmarket);
console.log("By point type:", fixture.summary.byPointType);
