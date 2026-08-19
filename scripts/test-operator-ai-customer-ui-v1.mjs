#!/usr/bin/env node
/**
 * Operator AI Customer UI V1 — deterministic gates.
 * PROVIDER_CALLS = 0 · SPEND = $0 · No Brand regression.
 */

import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  OPERATOR_AI_UNIVERSE,
  PRIMARY_OPERATOR_COUNT,
  ARBOR_LODGING_ID,
  OPERATOR_AI_PRODUCT,
  buildOperatorCustomerPayload,
  buildOperatorCustomerUniversePayload,
  buildOperatorCustomerUiCertificationReport,
  buildClientPromotedGapIndex,
  extractOperatorCompetitiveGapCandidates,
  loadCertifiedOperatorPresenceCorpus,
  operatorProviderScopeToGapKey,
  auditOperatorCustomerPayload,
  OPERATOR_CUSTOMER_OWNER_INTENT,
} from "../lib/ai-visibility/operator-intelligence/index.js";
import {
  ALL_PROVIDERS_SELECTOR_ID,
  KNOWN_AI_VISIBILITY_PROVIDER_IDS,
} from "../lib/ai-visibility/provider-dimension.js";
import { BRAND_AI_VISIBILITY_EXPECTED_ROUTES } from "../lib/ai-visibility/route-registration-guard.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const OP_HTML = fs.readFileSync(path.join(ROOT, "public/operator-ai-intelligence.html"), "utf8");
const OP_JS = fs.readFileSync(path.join(ROOT, "public/js/ai-visibility/ai-visibility-operator.js"), "utf8");
const BRAND_JS = fs.readFileSync(path.join(ROOT, "public/js/ai-visibility/ai-visibility-brand.js"), "utf8");

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

function id(founder) {
  return OPERATOR_AI_UNIVERSE.find((o) => o.founderName === founder).canonicalId;
}

console.log("\nOperator AI Customer UI V1\n");

await test("route and entitlement contract", () => {
  assert.equal(OPERATOR_AI_PRODUCT.route, "/operator/ai-intelligence");
  assert.match(OP_HTML, /Operator AI Intelligence/);
  assert.match(OP_HTML, /Executive/);
  assert.match(OP_HTML, /Detailed/);
  assert.match(OP_JS, /competitiveGap/);
  assert.match(OP_JS, /aria-expanded/);
});

await test("universe payload: exactly 9 operators", () => {
  const universe = buildOperatorCustomerUniversePayload();
  assert.equal(universe.primaryOperatorCount, 9);
  assert.equal(universe.operators.length, 9);
  assert.equal(universe.providers.some((p) => p.id === "all" && p.derived === true), true);
  assert.equal(universe.blockedModules.operatorIndex, "BLOCKED");
  assert.equal(universe.blockedModules.recommendations, "BLOCKED");
});

await test("provider selector includes all five scopes", () => {
  const providers = ["all", "openai", "gemini", "perplexity", "claude"];
  for (const p of providers) {
    const payload = buildOperatorCustomerPayload(id("Hotel Equities CALA"), p);
    assert.equal(payload.ok, true);
    assert.ok(payload.providerLabel);
  }
});

await test("owner intent taxonomy: 12 governed labels", () => {
  assert.equal(Object.keys(OPERATOR_CUSTOMER_OWNER_INTENT).length, 12);
  const payload = buildOperatorCustomerPayload(id("Hotel Equities CALA"), "all");
  assert.equal(payload.detail.ownerIntentRows.length, 12);
  assert.equal(payload.detail.ownerIntentTaxonomy.length, 12);
});

await test("questions missing regression counts from corpus", () => {
  const cert = buildOperatorCustomerUiCertificationReport();
  assert.equal(cert.qmRegression.totalOperatorScenarioRows, 108);
  assert.equal(cert.qmRegression.diagnosticMissingRows, 36);
  assert.equal(cert.qmRegression.applicableMissingRows, 33);
});

await test("out-of-scope renders Not applicable not Missing weakness", () => {
  const payload = buildOperatorCustomerPayload(id("Brittain Resorts"), "all");
  const cala = payload.detail.ownerIntentRows.find(
    (r) => r.scenarioId === "op_scenario_cala_latam_regional_capability_v1"
  );
  assert.equal(cala.yourPresence.display, "Not applicable");
  assert.notEqual(cala.missing.display, "Missing");
});

await test("client-promoted gaps: exactly 8 and renderable at exact scope", () => {
  const corpus = loadCertifiedOperatorPresenceCorpus();
  const promoted = extractOperatorCompetitiveGapCandidates(corpus.extractions).filter(
    (c) => c.clientPromoted
  );
  assert.equal(promoted.length, 8);
  const cert = buildOperatorCustomerUiCertificationReport();
  assert.equal(cert.expectedClientPromoted, 8);
  assert.equal(cert.actualClientPromotedRenderable, 8);
  assert.equal(cert.unexpectedPromoted, 0);
});

await test("Aimbridge Claude full-service gap renders", () => {
  const payload = buildOperatorCustomerPayload(id("Aimbridge LATAM"), "claude");
  const row = payload.detail.ownerIntentRows.find(
    (r) => r.scenarioId === "op_scenario_full_service_uu_operator_selection_v1"
  );
  assert.equal(row.competitiveGap.clientPromoted, true);
  assert.equal(row.competitiveGap.display, "Certified Gap");
});

await test("Hotel Equities OpenAI owner-control + brand-agnostic gaps render", () => {
  const payload = buildOperatorCustomerPayload(id("Hotel Equities CALA"), "openai");
  const owner = payload.detail.ownerIntentRows.find(
    (r) => r.scenarioId === "op_scenario_owner_control_flexibility_v1"
  );
  const brand = payload.detail.ownerIntentRows.find(
    (r) => r.scenarioId === "op_scenario_brand_agnostic_operation_v1"
  );
  assert.equal(owner.competitiveGap.clientPromoted, true);
  assert.equal(brand.competiveGap?.clientPromoted ?? brand.competitiveGap.clientPromoted, true);
});

await test("Remington OpenAI gaps render", () => {
  const payload = buildOperatorCustomerPayload(id("Remington CALA"), "openai");
  const owner = payload.detail.ownerIntentRows.find(
    (r) => r.scenarioId === "op_scenario_owner_control_flexibility_v1"
  );
  const brand = payload.detail.ownerIntentRows.find(
    (r) => r.scenarioId === "op_scenario_brand_agnostic_operation_v1"
  );
  assert.equal(owner.competitiveGap.clientPromoted, true);
  assert.equal(brand.competitiveGap.clientPromoted, true);
});

await test("cross-provider stale paint blocked", () => {
  const cert = buildOperatorCustomerUiCertificationReport();
  assert.equal(cert.allProvidersStalePaint, 0);
  const heOpenai = buildOperatorCustomerPayload(id("Hotel Equities CALA"), "openai");
  const heGemini = buildOperatorCustomerPayload(id("Hotel Equities CALA"), "gemini");
  const heAll = buildOperatorCustomerPayload(id("Hotel Equities CALA"), "all");
  assert.equal(
    heOpenai.detail.ownerIntentRows.find(
      (r) => r.scenarioId === "op_scenario_owner_control_flexibility_v1"
    ).competitiveGap.clientPromoted,
    true
  );
  assert.equal(
    heGemini.detail.ownerIntentRows.find(
      (r) => r.scenarioId === "op_scenario_full_service_uu_operator_selection_v1"
    ).competitiveGap.clientPromoted,
    true
  );
  assert.equal(
    heAll.detail.ownerIntentRows.find(
      (r) => r.scenarioId === "op_scenario_owner_control_flexibility_v1"
    ).competitiveGap.clientPromoted,
    false
  );
});

await test("Marriott no false TPM gap", () => {
  const cert = buildOperatorCustomerUiCertificationReport();
  assert.equal(cert.marriottFalseTpmGaps, 0);
});

await test("Brittain no false CALA gap", () => {
  const cert = buildOperatorCustomerUiCertificationReport();
  assert.equal(cert.brittainFalseCalaGaps, 0);
});

await test("Arbor no competitive claims", () => {
  const cert = buildOperatorCustomerUiCertificationReport();
  assert.equal(cert.arborCompetitiveClaims, 0);
  const payload = buildOperatorCustomerPayload(id("Arbor Lodging"), "all");
  assert.equal(payload.operator.insufficientOperatorEvidence, true);
  assert.match(payload.kpis.aiPresence.display, /Insufficient/i);
  assert.ok(
    payload.detail.ownerIntentRows.every((r) => r.competitiveGap.clientPromoted !== true)
  );
});

await test("institutional + commercial scenarios: no certified gap", () => {
  const payload = buildOperatorCustomerPayload(id("Hotel Equities CALA"), "all");
  const institutional = payload.detail.ownerIntentRows.find(
    (r) => r.scenarioId === "op_scenario_institutional_platform_alignment_v1"
  );
  const commercial = payload.detail.ownerIntentRows.find(
    (r) => r.scenarioId === "op_scenario_commercial_revenue_capability_v1"
  );
  assert.notEqual(institutional.competitiveGap.display, "Certified Gap");
  assert.notEqual(commercial.competitiveGap.display, "Certified Gap");
});

await test("security: no prompt / matrix / gold / unpromoted gap leaks", () => {
  const cert = buildOperatorCustomerUiCertificationReport();
  assert.equal(cert.security.rawPrompt, 0);
  assert.equal(cert.security.comparabilityMatrix, 0);
  assert.equal(cert.security.goldLabel, 0);
  assert.equal(cert.security.unpromotedGap, 0);
});

await test("frontend does not hard-code promoted gaps", () => {
  assert.doesNotMatch(OP_JS, /Hotel Equities \(CALA\).*Certified Gap/s);
  assert.doesNotMatch(OP_JS, /TRUE_COMPETITIVE_GAP/);
  assert.doesNotMatch(OP_JS, /clientPromoted\s*=\s*true/);
  assert.match(OP_JS, /competitiveGap/);
});

await test("executive findings: no recommendation or win/loss language", () => {
  const banned = /\b(best operator|recommended|wins|loses|market leader|win rate|preferred over|lost to)\b/i;
  for (const op of OPERATOR_AI_UNIVERSE) {
    for (const provider of [ALL_PROVIDERS_SELECTOR_ID, ...KNOWN_AI_VISIBILITY_PROVIDER_IDS]) {
      const payload = buildOperatorCustomerPayload(op.canonicalId, provider);
      for (const f of payload.executive.findings || []) {
        assert.doesNotMatch(f.finding || "", banned);
      }
    }
  }
});

await test("Brand regression freeze", () => {
  assert.equal(BRAND_AI_VISIBILITY_EXPECTED_ROUTES.length, 10);
  const cert = buildOperatorCustomerUiCertificationReport();
  assert.equal(cert.brandDiff, 0);
  assert.equal(cert.operatorPresenceDiff, 0);
  assert.equal(cert.operatorCompetitiveCertificationDiff, 0);
  assert.doesNotMatch(BRAND_JS, /operator-customer-read-service/);
});

await test("no provider calls", () => {
  const cert = buildOperatorCustomerUiCertificationReport();
  assert.equal(cert.providerCalls, 0);
  assert.equal(cert.spend, "$0");
});

const certReport = buildOperatorCustomerUiCertificationReport();
const reportPath = path.join(ROOT, "reports/ai-visibility/operator-ai-customer-ui-v1.json");
fs.mkdirSync(path.dirname(reportPath), { recursive: true });
fs.writeFileSync(reportPath, JSON.stringify(certReport, null, 2));
console.log(`\nWrote ${reportPath}`);
console.log(`\nTOTAL: ${passed + failed}  PASS: ${passed}  FAIL: ${failed}\n`);
process.exit(failed > 0 ? 1 : 0);
