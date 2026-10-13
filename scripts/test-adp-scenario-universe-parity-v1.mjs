#!/usr/bin/env node
/**
 * Guard: generic scenario parity pack replaces thin 20-row onboarding fallback.
 */
import assert from "node:assert/strict";
import { loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import {
  generateGenericProfileScenarios,
  buildGenericParityScenarioUniverse,
  GENERIC_PROFILE_SCENARIOS_VERSION,
} from "../lib/ai-demand-positioning/prompt-universe/generic-profile-scenarios.js";

const wRome = loadPropertyProfile("adp_w_rome");
const bethesda = loadPropertyProfile("adp_bethesda_marriott");
const renaissance = loadPropertyProfile("adp_renaissance_times_square");

const wU = buildScenarioUniverse(wRome);
const bU = buildScenarioUniverse(bethesda);
const rU = buildScenarioUniverse(renaissance);

assert.equal(wU.length, 63, `W Rome corrected universe expected 63, got ${wU.length}`);
assert.ok(bU.length >= 60, "Bethesda remains mature pack");
assert.ok(rU.length >= 60, "Renaissance remains mature pack");
assert.ok(
  wU.every((s) => !/rome\/italy|if\s*\(.*rome/i.test(s.query)),
  "no rome production branching in queries beyond profile city/market"
);

const core = generateGenericProfileScenarios(wRome);
assert.equal(core.length, 48);
assert.equal(core[0].scenarioId, "gen_adp_w_rome_01");
assert.equal(core[19].scenarioId, "gen_adp_w_rome_20");

const pack = buildGenericParityScenarioUniverse(wRome);
assert.equal(pack.version, GENERIC_PROFILE_SCENARIOS_VERSION);
assert.equal(pack.layers.total, 63);
assert.equal(pack.layers.property_capability, 15);

// Mature hotels must NOT fall into generic path
assert.ok(bU.every((s) => s.source === "standard" || s.source === "property_specific"));
assert.ok(rU.every((s) => s.source === "standard" || s.source === "property_specific"));

console.log("PASS test-adp-scenario-universe-parity-v1");
console.log(JSON.stringify({ wRome: wU.length, bethesda: bU.length, renaissance: rU.length }));
