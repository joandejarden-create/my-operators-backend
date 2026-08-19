#!/usr/bin/env node
/**
 * Narrative & Source Intelligence V1 contract tests.
 * No provider calls.
 */
import assert from "node:assert/strict";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  STAGE_B_AUTHORITATIVE_REPORT_REL_PATH,
  STAGE_B_AUTHORITATIVE_WAVE_ID,
} from "../lib/ai-visibility/stability-policy.js";
import {
  loadAuthoritativeStabilityReport,
  auditNarrativeInputReadiness,
  PORTFOLIO_BRANDS,
} from "../lib/ai-visibility/narrative-intelligence.js";
import {
  FORBIDDEN_SOURCE_LANGUAGE,
  mapAttributeToNarrativeFamily,
  NARRATIVE_FAMILIES,
} from "../lib/ai-visibility/narrative-taxonomy.js";
import { createBrandAiVisibilityReadStore } from "../lib/ai-visibility/storage/index.js";

const root = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
let passed = 0;
let failed = 0;

async function test(name, fn) {
  try {
    await fn();
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}

console.log("\nNarrative & Source Intelligence V1\n");

await test("authoritative stability report binds final wave", () => {
  const { report } = loadAuthoritativeStabilityReport();
  assert.equal(report.waveId, STAGE_B_AUTHORITATIVE_WAVE_ID);
  assert.equal(report.stageBEvidenceCount, 31);
});

await test("narrative taxonomy maps association attributes", () => {
  assert.equal(mapAttributeToNarrativeFamily("DISTRIBUTION"), "DISTRIBUTION_LOYALTY");
  assert.equal(mapAttributeToNarrativeFamily("DESIGN_INDIVIDUALITY"), "DESIGN_LOCAL_CHARACTER");
  assert.ok(NARRATIVE_FAMILIES.length >= 8);
});

await test("input readiness HIGH on federated baseline", async () => {
  const store = createBrandAiVisibilityReadStore();
  const evidence = await store.listEvidence({});
  const audit = auditNarrativeInputReadiness(evidence);
  assert.equal(audit.NARRATIVE_INPUT_READINESS, "HIGH");
  assert.ok(audit.RESPONSES_AVAILABLE >= 100);
});

await test("portfolio brand IDs present", () => {
  assert.equal(PORTFOLIO_BRANDS["Autograph Collection"], "recEJCTDj1zrsjPM6");
  assert.equal(PORTFOLIO_BRANDS["AC Hotels by Marriott"], "rec9aZp7GHtzUEg0c");
  assert.equal(PORTFOLIO_BRANDS.Westin, "recIPuBC50fv13zRR");
  assert.equal(PORTFOLIO_BRANDS["Design Hotels"], "rec02zPClpWUTCyXM");
});

await test("forbidden causal source language list is non-empty", () => {
  assert.ok(FORBIDDEN_SOURCE_LANGUAGE.includes("INFLUENCED"));
  assert.ok(FORBIDDEN_SOURCE_LANGUAGE.includes("CAUSED"));
});

await test("narrative v1 report artifact exists after run", () => {
  const reportPath = path.join(root, "reports", "ai-visibility", "narrative-source-v1-report.json");
  if (!fs.existsSync(reportPath)) {
    console.log("  SKIP narrative v1 report artifact (run npm run ai-visibility:narrative-source-v1 first)");
    return;
  }
  const report = JSON.parse(fs.readFileSync(reportPath, "utf8"));
  assert.equal(report.AuthoritativeInput.STABILITY_REPORT, STAGE_B_AUTHORITATIVE_REPORT_REL_PATH);
  assert.equal(report.AuthoritativeInput.ARCHIVED_WAVE_USED, "NO");
  assert.equal(report.guards.PROVIDER_CALLS, 0);
  assert.equal(report.Regression.PRESENCE_DIFF, 0);
});

await test("remediation report sealed holdout integrity", () => {
  const reportPath = path.join(root, "reports", "ai-visibility", "narrative-source-v1-remediation-report.json");
  if (!fs.existsSync(reportPath)) {
    console.log("  SKIP remediation report (run npm run ai-visibility:narrative-source-v1-remediation first)");
    return;
  }
  const report = JSON.parse(fs.readFileSync(reportPath, "utf8"));
  assert.equal(report.RemediationSet.HOLDOUT_SEALED_BEFORE_RULE_TUNING, "YES");
  assert.equal(report.RemediationSet.HOLDOUT_REUSED_FOR_ITERATIVE_TUNING, "NO");
  assert.ok(report.RemediationSet.TOTAL >= 60);
  assert.equal(report.guards.PROVIDER_CALLS, 0);
  assert.equal(report.Regression.DISTRIBUTION_ASSOCIATION_DIFF, 0);
});

console.log(`\nNarrative & Source Intelligence V1 tests: ${passed} passed, ${failed} failed\n`);
if (failed) process.exit(1);
