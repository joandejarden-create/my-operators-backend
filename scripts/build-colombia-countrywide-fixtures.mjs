#!/usr/bin/env node
/**
 * Build Colombia demand anchor candidate fixtures (market-by-market).
 *   node scripts/build-colombia-countrywide-fixtures.mjs           # Phase 1 only (default)
 *   node scripts/build-colombia-countrywide-fixtures.mjs --phase 2  # Phase 2 cities
 *   node scripts/build-colombia-countrywide-fixtures.mjs --all      # All Colombia candidates
 */
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  COLOMBIA_SUBMARKETS,
  COLOMBIA_DEMAND_ANCHOR_CANDIDATES,
  filterColombiaCandidatesByPhase,
} from "../lib/radar-buildout/colombia-demand-anchors-candidates.js";
import { COLOMBIA_PHASE_1_SUBMARKETS } from "../lib/radar-buildout/colombia-demand-anchor-governance.js";
import { applyColombiaPlaceReviewCorrections } from "../lib/radar-buildout/colombia-google-place-review-corrections.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

const args = process.argv.slice(2);
const phaseArg = args.find((a) => a.startsWith("--phase"));
const phase = phaseArg ? Number(phaseArg.split("=")[1] || args[args.indexOf(phaseArg) + 1]) : 1;
const includeAll = args.includes("--all");

let sourcePoints = COLOMBIA_DEMAND_ANCHOR_CANDIDATES;
let phaseLabel = "phase-1";
if (includeAll) {
  phaseLabel = "all-markets";
} else if (phase === 2) {
  sourcePoints = filterColombiaCandidatesByPhase(2);
  phaseLabel = "phase-2";
} else {
  sourcePoints = filterColombiaCandidatesByPhase(1);
  phaseLabel = "phase-1";
}

const reviewedPoints = applyColombiaPlaceReviewCorrections(sourcePoints);

const bySubmarket = {};
const byPointType = {};
const byCity = {};
for (const p of reviewedPoints) {
  bySubmarket[p.submarket] = (bySubmarket[p.submarket] || 0) + 1;
  byPointType[p.pointType] = (byPointType[p.pointType] || 0) + 1;
  byCity[p.city] = (byCity[p.city] || 0) + 1;
}

const fixture = {
  market: "Colombia",
  country: "Colombia",
  region: "South America",
  buildStrategy: "Large Country / Market-by-Market",
  buildPhase: phaseLabel,
  phase1Submarkets: COLOMBIA_PHASE_1_SUBMARKETS,
  generatedAt: new Date().toISOString().slice(0, 10),
  status: "candidate_pre_verification",
  submarkets: includeAll ? COLOMBIA_SUBMARKETS : Object.keys(bySubmarket).sort(),
  governanceRequired: true,
  summary: {
    totalPoints: reviewedPoints.length,
    bySubmarket,
    byCity,
    byPointType,
    reviewCorrectionsApplied: reviewedPoints.filter((p) =>
      String(p.notes || "").includes("Google Places review pass")
    ).length,
  },
  points: reviewedPoints,
};

const fixturePathA = "fixtures/demand-anchors-colombia-countrywide-candidates.json";
const fixturePathB = "public/fixtures/demand-anchors-colombia-countrywide-candidates.json";

writeFileSync(join(root, fixturePathA), JSON.stringify(fixture, null, 2) + "\n");
writeFileSync(join(root, fixturePathB), JSON.stringify(fixture, null, 2) + "\n");

console.log("Colombia candidate fixtures written:", reviewedPoints.length, "records", `(${phaseLabel})`);
console.log("By city:", byCity);
console.log("By submarket:", bySubmarket);
console.log("By point type:", byPointType);
