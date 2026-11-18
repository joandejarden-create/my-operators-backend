#!/usr/bin/env node
/**
 * Contract presence gate for DEALALITY_RADAR_AUTHENTICATED_APP_ACCEPTANCE_GATE_V1.
 * Does NOT simulate a browser session and cannot award PASS for product behavior.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

function read(rel) {
  const abs = path.join(ROOT, rel);
  assert.ok(fs.existsSync(abs), `missing required file: ${rel}`);
  return fs.readFileSync(abs, "utf8");
}

const contract = read("docs/ai-build-system/RADAR_AUTHENTICATED_APP_ACCEPTANCE_GATE.md");
assert.match(contract, /DEALALITY_RADAR_AUTHENTICATED_APP_ACCEPTANCE_GATE_V1/);
assert.match(contract, /\/app#\/opportunity-radar/);
assert.match(contract, /localhost:8080/);
assert.match(contract, /\bPASS\b/);
assert.match(contract, /\bFAIL\b/);
assert.match(contract, /\bBLOCKED\b/);
assert.match(contract, /cannot.*upgrade.*BLOCKED.*PASS/i);
assert.match(contract, /without.*hotel-specific allowlists/i);
assert.match(contract, /Never.*authentication tokens|Never record authentication tokens/i);
console.log("PASS contract: RADAR_AUTHENTICATED_APP_ACCEPTANCE_GATE.md");

const rule = read(".cursor/rules/dealality-radar-authenticated-acceptance-gate.mdc");
assert.match(rule, /\/app#\/opportunity-radar/);
assert.match(rule, /BLOCKED/);
console.log("PASS cursor rule: dealality-radar-authenticated-acceptance-gate.mdc");

const qa = read("docs/ai-build-system/DEALALITY_QA_CHECKLIST.md");
assert.match(qa, /opportunity-radar/);
assert.match(qa, /RADAR_AUTHENTICATED_APP_ACCEPTANCE_GATE/);
console.log("PASS QA checklist references authenticated Radar gate");

const matrix = read("docs/dealality-pr-validation-matrix.md");
assert.match(matrix, /opportunity-radar/);
assert.match(matrix, /RADAR_AUTHENTICATED_APP_ACCEPTANCE_GATE/);
console.log("PASS PR validation matrix references authenticated Radar gate");

const startup = read("docs/ai-build-system/PRODUCT_CHAT_STARTUP_PROMPTS.md");
assert.match(startup, /opportunity-radar/);
assert.match(startup, /RADAR_AUTHENTICATED_APP_ACCEPTANCE_GATE|authenticated application acceptance/i);
console.log("PASS HE startup prompt references authenticated Radar gate");

const agents = read("AGENTS.md");
assert.match(agents, /Radar authenticated-app acceptance/);
assert.match(agents, /\/app#\/opportunity-radar/);
console.log("PASS AGENTS.md non-negotiable references authenticated Radar gate");

const mergeDiscipline = read(".cursor/rules/gdi-github-branch-merge-discipline.mdc");
assert.match(mergeDiscipline, /\/app#\/opportunity-radar/);
assert.match(mergeDiscipline, /RADAR_AUTHENTICATED_APP_ACCEPTANCE_GATE/);
console.log("PASS merge discipline references authenticated Radar gate");

const pkg = JSON.parse(read("package.json"));
assert.equal(
  pkg.scripts["test:radar-authenticated-app-acceptance-gate-v1"],
  "node scripts/test-radar-authenticated-app-acceptance-gate-v1.mjs",
);
console.log("PASS package.json script wiring");

console.log("PASS: radar authenticated app acceptance gate v1 (contract only; not a browser PASS)");
