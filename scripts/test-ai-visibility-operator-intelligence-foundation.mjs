#!/usr/bin/env node
/**
 * Operator AI Intelligence Foundation V1 tests.
 * No provider calls. No Census. No DataForSEO. Brand files are not mutated.
 */

import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { PRODUCTION_SIGNALS } from "../lib/ai-visibility/signal-architecture/production-signals.js";
import { BRAND_AI_VISIBILITY_EXPECTED_ROUTES } from "../lib/ai-visibility/route-registration-guard.js";
import {
  OPERATOR_AI_UNIVERSE,
  PRIMARY_OPERATOR_COUNT,
  assertUniverseLock,
  isPrimaryMonitoredOperator,
  isBlockedUniverseExpansionName,
  classifyOperatorPresence,
  findOperatorSpans,
  OPERATOR_PROMPTS_V1,
  promptLibraryStats,
  OPERATOR_DECISION_SCENARIOS,
  scoreOperatorPresenceValidation,
  costOperatorFoundationWave,
  computeOperatorQuestionsMissing,
  computeOperatorAllProvidersPresence,
  interpretOperatorGap,
  GAP_INTERPRETATION,
  eligibilityFor,
  ELIGIBILITY,
  buildOperatorFoundationSnapshot,
  OPERATOR_AI_PRODUCT,
  OPERATOR_SIGNAL_PRESENCE,
} from "../lib/ai-visibility/operator-intelligence/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");

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

console.log("\nOperator AI Intelligence Foundation V1\n");

await test("primary operator universe exactly 9", () => {
  assertUniverseLock();
  assert.equal(OPERATOR_AI_UNIVERSE.length, 9);
  assert.equal(PRIMARY_OPERATOR_COUNT, 9);
});

await test("identity resolution — founder shorthand to Operator Master", () => {
  const expect = {
    "Marriott International": "recGmiPhRt6hiayd9",
    IHG: "rec7IXYQYpKMYsrDl",
    Hilton: "rec3Uwxe6ovpiokuN",
    "Aimbridge LATAM": "recGWxIJqnYHkJZFD",
    "Hotel Equities CALA": "recWPKu5laVZxsvpn",
    "Arbor Lodging": "recF5Z87OAqFgndoq",
    GHL: "reciI2tYQBfMoMK9G",
    "Brittain Resorts": "receHCdI6CEsJqdG4",
    "Remington CALA": "rec6UB6RpMKSs2tAo",
  };
  for (const [founder, rec] of Object.entries(expect)) {
    const row = OPERATOR_AI_UNIVERSE.find((o) => o.founderName === founder);
    assert.equal(row.canonicalId, rec, founder);
    assert.equal(row.identityStatus, "HIGH");
  }
});

await test("Aimbridge is regional scope of Aimbridge Hospitality, not a fake company", () => {
  const row = OPERATOR_AI_UNIVERSE.find((o) => o.founderName === "Aimbridge LATAM");
  assert.equal(row.parentPlatform, "Aimbridge Hospitality");
  assert.equal(row.monitoredScope, "LATAM");
  assert.match(row.canonicalName, /Aimbridge Hospitality/);
});

await test("Hotel Equities CALA is regional scope", () => {
  const row = OPERATOR_AI_UNIVERSE.find((o) => o.founderName === "Hotel Equities CALA");
  assert.equal(row.parentPlatform, "Hotel Equities");
  assert.equal(row.monitoredScope, "CALA");
});

await test("Remington CALA is regional scope of Remington Hospitality, not a fake company", () => {
  const row = OPERATOR_AI_UNIVERSE.find((o) => o.founderName === "Remington CALA");
  assert.equal(row.canonicalId, "rec6UB6RpMKSs2tAo");
  assert.equal(row.parentPlatform, "Remington Hospitality");
  assert.equal(row.monitoredScope, "CALA");
  assert.match(row.canonicalName, /Remington Hospitality/);
  assert.equal(row.domain, "remingtonhospitality.com");
  assert.equal(row.identityStatus, "HIGH");
});

await test("Remington clear operator mention = PRESENT", () => {
  const r = classifyOperatorPresence({
    text: "Remington Hospitality is a third-party management company owners consider in CALA.",
  });
  assert.ok(r.presentOperatorIds.includes(id("Remington CALA")));
});

await test("Remington source-domain-only = ABSENT", () => {
  const r = classifyOperatorPresence({
    text: "See https://www.remingtonhospitality.com/ for corporate information.",
    citations: [{ domain: "remingtonhospitality.com" }],
  });
  assert.equal(r.presentOperatorIds.includes(id("Remington CALA")), false);
});

await test("ambiguous Remington without operating context = ABSENT", () => {
  const r = classifyOperatorPresence({
    text: "Remington is a well-known brand in firearms and personal-care products.",
  });
  assert.equal(r.presentOperatorIds.length, 0);
});

await test("Remington observed competitor promotion — no duplicate entity", () => {
  const remingtonRows = OPERATOR_AI_UNIVERSE.filter((o) =>
    /remington/i.test(o.canonicalName)
  );
  assert.equal(remingtonRows.length, 1);
  assert.equal(isPrimaryMonitoredOperator("rec6UB6RpMKSs2tAo"), true);
  const r = classifyOperatorPresence({
    text: "Remington Hospitality is sometimes named as a third-party operator alongside other US platforms.",
  });
  assert.ok(r.presentOperatorIds.includes(id("Remington CALA")));
  assert.equal(r.observedCompetitors.some((c) => c.canonicalEntityId === id("Remington CALA")), false);
});

await test("no other operator promoted beyond Remington CALA", () => {
  assert.equal(isBlockedUniverseExpansionName("Highgate"), true);
  assert.equal(isBlockedUniverseExpansionName("Pyramid"), true);
  assert.equal(isBlockedUniverseExpansionName("Davidson"), true);
  assert.equal(isBlockedUniverseExpansionName("Remington"), false);
  assert.equal(OPERATOR_AI_UNIVERSE.length, 9);
});

await test("Marriott / Hilton / IHG use Managed operator lens", () => {
  for (const name of ["Marriott International", "Hilton", "IHG"]) {
    const row = OPERATOR_AI_UNIVERSE.find((o) => o.founderName === name);
    assert.equal(row.operatorLens, "BRAND_MANAGED_OPERATING_CAPABILITY");
    assert.match(row.canonicalName, /Managed/);
  }
});

await test("GHL and Brittain aliases resolve", () => {
  const ghl = classifyOperatorPresence({
    text: "GHL Hoteles is a management company owners consider in Latin America.",
  });
  assert.ok(ghl.presentOperatorIds.includes(id("GHL")));
  const br = classifyOperatorPresence({
    text: "Brittain Resorts & Hotels is a management company for resort hotels.",
  });
  assert.ok(br.presentOperatorIds.includes(id("Brittain Resorts")));
});

await test("Marriott company vs operator disambiguation", () => {
  const hotelOnly = classifyOperatorPresence({
    text: "The site is a Marriott hotel with a strong loyalty following among guests.",
  });
  assert.equal(hotelOnly.presentOperatorIds.includes(id("Marriott International")), false);
  const managed = classifyOperatorPresence({
    text: "Owners considering who should operate the hotel may choose Marriott International (Managed).",
  });
  assert.ok(managed.presentOperatorIds.includes(id("Marriott International")));
});

await test("Hilton company vs operator disambiguation", () => {
  const honors = classifyOperatorPresence({
    text: "Guests earn Hilton Honors points at this property.",
  });
  assert.equal(honors.presentOperatorIds.includes(id("Hilton")), false);
  const ops = classifyOperatorPresence({
    text: "Hilton Management Services is an operating option for branded full-service hotels.",
  });
  assert.ok(ops.presentOperatorIds.includes(id("Hilton")));
});

await test("IHG company vs operator disambiguation", () => {
  const guest = classifyOperatorPresence({
    text: "Stay at an IHG hotel and collect points.",
  });
  assert.equal(guest.presentOperatorIds.includes(id("IHG")), false);
  const ops = classifyOperatorPresence({
    text: "IHG Hotels & Resorts is considered as an operating partner on a brand-managed path.",
  });
  assert.ok(ops.presentOperatorIds.includes(id("IHG")));
});

await test("source-only URL is not Presence", () => {
  const r = classifyOperatorPresence({
    text: "See https://aimbridgehospitality.com/ for corporate information.",
    citations: [{ domain: "aimbridgehospitality.com" }],
  });
  assert.equal(r.presentOperatorIds.includes(id("Aimbridge LATAM")), false);
});

await test("competitor appearance does not promote competitor", () => {
  assert.equal(isBlockedUniverseExpansionName("Highgate"), true);
  assert.equal(isPrimaryMonitoredOperator("recHIGHGATEFAKE"), false);
  const r = classifyOperatorPresence({
    text: "Highgate is often mentioned as a third-party operator in owner conversations.",
  });
  assert.equal(r.presentOperatorIds.length, 0);
  assert.equal(OPERATOR_AI_UNIVERSE.some((o) => /highgate/i.test(o.canonicalName)), false);
});

await test("alias collision prefers longest operator name", () => {
  const spans = findOperatorSpans(
    "Aimbridge Hospitality (LATAM) is a third-party management company in Mexico."
  );
  assert.ok(spans.length >= 1);
  assert.equal(spans[0].entity.id, id("Aimbridge LATAM"));
});

await test("prompt library size and origin", () => {
  const stats = promptLibraryStats();
  assert.ok(stats.total >= 24 && stats.total <= 36);
  assert.equal(stats.observed, 0);
  assert.equal(stats.derived, 0);
  assert.equal(stats.core, 12);
  assert.equal(OPERATOR_DECISION_SCENARIOS.length, 12);
  assert.ok(OPERATOR_PROMPTS_V1.every((p) => p.origin === "SCENARIO"));
  assert.ok(OPERATOR_PROMPTS_V1.every((p) => p.seedOperatorNames === false));
  assert.ok(
    OPERATOR_PROMPTS_V1.every(
      (p) => !/why is aimbridge the best|rank marriott, hilton and ihg/i.test(p.text)
    )
  );
});

await test("shared execution does not scale by operator count", () => {
  const cost = costOperatorFoundationWave();
  assert.equal(cost.costScalesByOperator, false);
  assert.equal(cost.marginalCostAddOperator, 0);
  assert.equal(cost.executionGrain, "PROMPT_PROVIDER");
  assert.equal(cost.totalCalls, 84);
  assert.equal(cost.totalCalls, cost.corePrompts * 4 + cost.extendedPrompts * 2);
  assert.ok(cost.projectedConservativeCost <= 60);
  assert.equal(cost.marginalCostAddOperator, 0);
});

await test("Questions Missing denominator requires comparable provider observation", () => {
  const operatorId = id("Arbor Lodging");
  const r = computeOperatorQuestionsMissing({
    operatorId,
    promptIds: ["p1", "p2", "p3"],
    observations: [
      { promptId: "p1", provider: "openai", present: false },
      { promptId: "p1", provider: "perplexity", present: false },
      { promptId: "p2", provider: "openai", present: true },
    ],
  });
  assert.equal(r.denominator, 2);
  assert.equal(r.questionsMissingCount, 1);
  assert.equal(r.missingProviderEqualsZero, false);
});

await test("All Providers missing provider is not zero", () => {
  const r = computeOperatorAllProvidersPresence([
    { promptId: "p1", provider: "openai", present: true },
  ]);
  assert.equal(r.missingProviderEqualsZero, false);
  assert.equal(r.derived, true);
});

await test("expected positioning: Marriott missing from brand-agnostic third-party context", () => {
  const r = interpretOperatorGap({
    operatorId: id("Marriott International"),
    scenarioId: "op_scenario_brand_agnostic_operation_v1",
    operatorPresent: false,
    presentPeerOperatorIds: [id("Aimbridge LATAM"), id("Hotel Equities CALA")],
    observationCount: 4,
  });
  assert.equal(r.interpretation, GAP_INTERPRETATION.SCENARIO_OUT_OF_SCOPE);
  assert.equal(r.executiveActionPill, false);
});

await test("Brittain is OUT_OF_SCOPE for CALA regional scenario", () => {
  const elig = eligibilityFor(id("Brittain Resorts"), "op_scenario_cala_latam_regional_capability_v1");
  assert.equal(elig.status, ELIGIBILITY.OUT_OF_SCOPE);
});

await test("Presence constructed holdout has zero false positives", () => {
  const scored = scoreOperatorPresenceValidation();
  assert.equal(scored.holdout.fp, 0);
  assert.equal(scored.signal || OPERATOR_SIGNAL_PRESENCE, OPERATOR_SIGNAL_PRESENCE);
  assert.ok(["RESEARCH_ONLY", "PARTIAL"].includes(scored.status));
  assert.notEqual(scored.status, "PRODUCTION_ELIGIBLE");
});

await test("no Recommendation metric exposure", () => {
  const snap = buildOperatorFoundationSnapshot();
  assert.equal(snap.guards.RECOMMENDATION_METRICS, 0);
  assert.equal(snap.product.recommendationMetrics, 0);
  const { reuse, ...clientFacing } = snap;
  void reuse;
  const html = fs.readFileSync(path.join(root, "public", "operator-ai-intelligence.html"), "utf8");
  const js = fs.readFileSync(
    path.join(root, "public", "js", "ai-visibility", "ai-visibility-operator.js"),
    "utf8"
  );
  for (const surface of [JSON.stringify(clientFacing), html, js]) {
    assert.doesNotMatch(surface, /Recommendation Rate|Questions Won|Win Rate|Share of Voice/);
  }
  assert.equal(PRODUCTION_SIGNALS.AI_SIGNAL_RECOMMENDED, "AI_SIGNAL_RECOMMENDED");
});

await test("no Census dependency", () => {
  const snap = buildOperatorFoundationSnapshot();
  assert.equal(snap.guards.CENSUS_READS, 0);
  assert.equal(snap.truth.censusReads, 0);
});

await test("Brand route contract unchanged", () => {
  assert.equal(BRAND_AI_VISIBILITY_EXPECTED_ROUTES.length, 10);
  assert.ok(
    BRAND_AI_VISIBILITY_EXPECTED_ROUTES.every((r) => r.path.startsWith("/api/ai-visibility/brand"))
  );
});

await test("product route and entitlement", () => {
  assert.equal(OPERATOR_AI_PRODUCT.route, "/operator/ai-intelligence");
  assert.equal(OPERATOR_AI_PRODUCT.userType, "operator");
  assert.equal(OPERATOR_AI_PRODUCT.ownerVisibleByDefault, false);
});

const report = {
  snapshot: buildOperatorFoundationSnapshot(),
  tests: { total: passed + failed, pass: passed, fail: failed },
};
const reportPath = path.join(root, "reports", "ai-visibility", "operator-ai-intelligence-foundation.json");
fs.mkdirSync(path.dirname(reportPath), { recursive: true });
fs.writeFileSync(reportPath, JSON.stringify(report, null, 2));
console.log(`\nWrote ${reportPath}`);
console.log(`\nTOTAL: ${passed + failed}  PASS: ${passed}  FAIL: ${failed}\n`);
process.exit(failed > 0 ? 1 : 0);
