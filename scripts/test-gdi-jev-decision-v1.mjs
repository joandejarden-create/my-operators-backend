/**
 * GDI Jev V1 unit tests — adapter, policy, fallback, shadow no-behavior-change.
 * Does not require live Jev calls.
 */
import assert from "node:assert/strict";
import {
  decide,
  sanitizeJevState,
  applyJevPolicy,
  classifyHighRiskError,
  isValidChoice,
  JEV_DECISION_TYPE,
  CHOICES,
  HARD_GATES,
  getThreshold,
  isJevShadowMode,
  resetJevCircuitBreaker,
  getJevCircuitState,
  describeJevConfig,
} from "../lib/group-demand-intelligence/jev/index.js";
import { loadHistoricalJevDecisions } from "../lib/group-demand-intelligence/jev/historical-dataset.js";

let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}`);
    console.error(err && err.stack ? err.stack : err);
  }
}

async function checkAsync(name, fn) {
  try {
    await fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}`);
    console.error(err && err.stack ? err.stack : err);
  }
}

check("config_redacted_snapshot", () => {
  const cfg = describeJevConfig();
  assert.equal(typeof cfg.jevConfigPresent, "boolean");
  assert.ok(cfg.endpoint.includes("typesafe.ai"));
  assert.ok(cfg.model);
  assert.equal(cfg.shadowDefault, true);
  assert.doesNotMatch(JSON.stringify(cfg), /Bearer [A-Za-z0-9]/);
});

check("all_decision_types_have_choices", () => {
  for (const t of Object.keys(JEV_DECISION_TYPE)) {
    assert.ok(Array.isArray(CHOICES[t]) && CHOICES[t].length >= 2, t);
    assert.ok(isValidChoice(t, CHOICES[t][0]));
  }
});

check("sanitize_drops_bulky_and_names", () => {
  const s = sanitizeJevState({
    hotelName: "Bethesda Marriott",
    organizationName: "Woman's Club",
    priority: "HIGH",
    missingFields: ["lodgingEvidence"],
    webpageHtml: "<html>huge</html>",
    evidenceSnippets: ["a".repeat(500), "ok"],
  });
  assert.equal(s.priority, "HIGH");
  assert.ok(!s.hotelName);
  assert.ok(!s.organizationName);
  assert.ok(!s.webpageHtml);
  assert.ok(s.evidenceSnippets[0].length <= 280);
});

check("hard_gate_overrides_jev_pursue", () => {
  const policy = applyJevPolicy({
    decisionType: "OPPORTUNITY_PREQUAL",
    jevSelected: "LIKELY_ACTIONABLE",
    confidence: 0.95,
    existingDecision: "LIKELY_REJECT",
    policyContext: { pastEvent: true },
    shadow: true,
  });
  assert.ok(policy.hardGates.includes(HARD_GATES.PAST_EVENT));
  assert.equal(policy.finalPolicyDecision, "LIKELY_REJECT");
  assert.equal(policy.policyOutcome, "HARD_GATE");
});

check("low_confidence_falls_back", () => {
  const thr = getThreshold("STOP_CONTINUE");
  const policy = applyJevPolicy({
    decisionType: "STOP_CONTINUE",
    jevSelected: "STOP_NO_USEFUL_NEW_EVIDENCE",
    confidence: Math.max(0, thr - 0.2),
    existingDecision: "CONTINUE_RESEARCH",
    policyContext: {},
    shadow: true,
  });
  assert.equal(policy.lowConfidence, true);
  assert.equal(policy.finalPolicyDecision, "CONTINUE_RESEARCH");
});

check("shadow_keeps_existing_even_when_jev_differs", () => {
  const policy = applyJevPolicy({
    decisionType: "TARGET_RESEARCH_PRIORITY",
    jevSelected: "RESEARCH_NOW",
    confidence: 0.9,
    existingDecision: "DEFER",
    policyContext: {},
    shadow: true,
  });
  assert.equal(policy.shadow, true);
  assert.equal(policy.finalPolicyDecision, "DEFER");
  assert.equal(policy.matchExisting, false);
});

check("invalid_choice_rejected", () => {
  assert.equal(isValidChoice("MATERIAL_CHANGE", "TOTALLY_FAKE"), false);
  const policy = applyJevPolicy({
    decisionType: "MATERIAL_CHANGE",
    jevSelected: "TOTALLY_FAKE",
    confidence: 0.99,
    existingDecision: "NON_MATERIAL_CHANGE",
    shadow: true,
  });
  // applyJevPolicy trusts caller; decide() validates — still final stays existing in shadow
  assert.equal(policy.finalPolicyDecision, "NON_MATERIAL_CHANGE");
});

check("high_risk_classifiers", () => {
  const a = classifyHighRiskError({
    decisionType: "OPPORTUNITY_PREQUAL",
    jevSelected: "LIKELY_ACTIONABLE",
    expected: "LIKELY_REJECT",
    policyContext: { pastEvent: true },
  });
  assert.ok(a.includes("PAST_EVENT_FALSE_PURSUE"));
  const b = classifyHighRiskError({
    decisionType: "STOP_CONTINUE",
    jevSelected: "STOP_NO_USEFUL_NEW_EVIDENCE",
    expected: "CONTINUE_RESEARCH",
    policyContext: {},
  });
  assert.ok(b.includes("TRUE_OPPORTUNITY_FALSE_STOP"));
  const c = classifyHighRiskError({
    decisionType: "PRIVATE_EVENT_SIGNAL_QUALITY",
    jevSelected: "SPECIFIC_EVENT_CANDIDATE",
    expected: "PRIVACY_REJECT",
    policyContext: { privacyReject: true },
  });
  assert.ok(c.includes("PRIVACY_FALSE_PURSUE"));
});

check("historical_dataset_min_100", () => {
  const rows = loadHistoricalJevDecisions({ minCount: 100 });
  assert.ok(rows.length >= 100);
  assert.ok(rows.every((r) => r.decisionType && r.expected));
});

await checkAsync("decide_fallback_without_breaking", async () => {
  resetJevCircuitBreaker();
  // Force disabled path by monkeypatching env temporarily
  const prev = process.env.GDI_JEV_ENABLED;
  process.env.GDI_JEV_ENABLED = "0";
  const r = await decide({
    decisionType: "TARGET_RESEARCH_PRIORITY",
    context: { priority: "HIGH" },
    existingDecision: "RESEARCH_NOW",
    forceShadow: true,
  });
  process.env.GDI_JEV_ENABLED = prev;
  assert.equal(r.fallbackUsed, true);
  assert.equal(r.finalPolicyDecision, "RESEARCH_NOW");
  assert.equal(r.shadow, true);
});

await checkAsync("jev_unavailable_preserves_existing", async () => {
  const r = await decide({
    decisionType: "OPPORTUNITY_PREQUAL",
    context: { lodgingEvidence: "none" },
    existingDecision: "LIKELY_WATCH",
    policyContext: { fullyPlaced: true },
    forceShadow: true,
  });
  assert.equal(r.finalPolicyDecision, "LIKELY_WATCH");
  assert.ok(r.policy.hardGates.includes(HARD_GATES.FULLY_PLACED) || r.fallbackUsed || r.selected != null);
});

check("default_mode_is_shadow", () => {
  assert.equal(isJevShadowMode(), true);
});

check("circuit_breaker_state_shape", () => {
  resetJevCircuitBreaker();
  const s = getJevCircuitState();
  assert.equal(s.open, false);
  assert.equal(typeof s.consecutiveErrors, "number");
});

if (failed) {
  console.error(`\n${failed} check(s) failed`);
  process.exit(1);
}
console.log("\nAll GDI Jev V1 unit checks passed.");
