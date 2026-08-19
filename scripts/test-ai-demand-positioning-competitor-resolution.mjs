#!/usr/bin/env node
/**
 * Tests — competitor name resolution + downtown NYC scenario routing.
 *   npm run test:ai-demand-positioning-competitor-resolution
 */

import assert from "assert";
import { readFileSync } from "fs";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import {
  resolveCompetitorName,
  extractAndResolveCompetitors,
  matchesDeclaredComp,
  isNonHotelEntity,
} from "../lib/ai-demand-positioning/intelligence/competitor-name-resolution.js";
import {
  buildScenarioUniverse,
  resolveStandardScenarioMarket,
  isDowntownNycProfile,
} from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { parseObservation } from "../lib/ai-demand-positioning/execution/response-parser.js";
import { computeCompetitiveSet } from "../lib/ai-demand-positioning/intelligence/competitive-set.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const FIXTURES = join(__dirname, "../fixtures/ai-demand-positioning");

function loadProfile(name) {
  return JSON.parse(readFileSync(join(FIXTURES, name), "utf8"));
}

function main() {
  // --- Name resolution ---
  assert.equal(
    resolveCompetitorName("Luxury Collection Hotel"),
    "The Chatwal, a Luxury Collection Hotel",
  );
  assert.equal(
    resolveCompetitorName("Residence Inn"),
    "Residence Inn by Marriott New York Manhattan/Times Square",
  );
  assert.equal(resolveCompetitorName("Kimpton Muse Hotel"), "Kimpton Muse Hotel");
  assert.equal(resolveCompetitorName("Kimpton Hotel Eventi"), "Kimpton Hotel Eventi");
  assert.equal(resolveCompetitorName("Kimpton Hotel"), null, "generic Kimpton fragment dropped");
  assert.equal(isNonHotelEntity("The Lambs Club"), true);
  assert.equal(resolveCompetitorName("The Lambs Club"), null);

  const sampleResponse = [
    "1. **The Chatwal, a Luxury Collection Hotel** — luxury in Midtown",
    "2. **Kimpton Muse Hotel** — boutique style",
    "3. Residence Inn by Marriott New York Manhattan/Times Square",
    "Also consider The Lambs Club for dining (not a hotel).",
  ].join("\n");

  const nohoProfile = loadProfile("now-now-noho-property-profile.json");
  const resolved = extractAndResolveCompetitors(sampleResponse, nohoProfile);
  assert.ok(resolved.includes("The Chatwal, a Luxury Collection Hotel"));
  assert.ok(resolved.includes("Kimpton Muse Hotel"));
  assert.ok(!resolved.some((n) => /lambs club/i.test(n)), "non-hotel excluded");

  // --- Declared comp matching (stricter) ---
  assert.equal(matchesDeclaredComp("Crosby Street Hotel", "Crosby Street Hotel"), true);
  assert.equal(matchesDeclaredComp("The Knickerbocker Hotel", "The NoMad Hotel"), false);
  assert.equal(matchesDeclaredComp("The Bowery Hotel", "Ace Hotel New York"), false);

  // --- Scenario routing ---
  assert.equal(isDowntownNycProfile(nohoProfile), true);
  assert.equal(resolveStandardScenarioMarket(nohoProfile), "nyc_downtown");

  const renaissanceProfile = loadProfile("renaissance-times-square-property-profile.json");
  assert.equal(isDowntownNycProfile(renaissanceProfile), false);
  assert.equal(resolveStandardScenarioMarket(renaissanceProfile), "nyc_times_square");

  const nohoUniverse = buildScenarioUniverse(nohoProfile);
  const renaissanceUniverse = buildScenarioUniverse(renaissanceProfile);
  assert.ok(
    nohoUniverse.some((s) => s.scenarioId.startsWith("std_nyc_dt_")),
    "NoHo uses downtown standard scenarios",
  );
  assert.ok(
    !nohoUniverse.some((s) => s.scenarioId === "std_nyc_biz_01"),
    "NoHo does not use Times Square std_nyc_biz_01",
  );
  assert.ok(
    renaissanceUniverse.some((s) => s.scenarioId === "std_nyc_biz_01"),
    "Renaissance keeps Times Square scenarios",
  );
  assert.ok(nohoUniverse.length >= 60, "NoHo universe still includes property-specific prompts");

  // --- End-to-end parse + comp set ---
  const obs = parseObservation({
    scenarioId: "test_01",
    rawResponse: sampleResponse,
  }, nohoProfile);
  assert.ok(obs.competitorsMentioned.length >= 2);
  const compSet = computeCompetitiveSet([obs], nohoProfile);
  assert.ok(compSet.observed.every((c) => !/lambs club/i.test(c.name)));

  console.log("test:ai-demand-positioning-competitor-resolution — PASS");
}

main();
