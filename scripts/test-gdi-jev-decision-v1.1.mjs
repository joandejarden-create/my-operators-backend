/**
 * GDI Jev V1.1 unit tests — fallback split, adjudication, choice filter, cohort, shadow.
 */
import assert from "node:assert/strict";
import {
  decide,
  sanitizeJevState,
  enrichDecisionContext,
  applyJevPolicy,
  filterChoicesForContext,
  adjudicateDisagreement,
  ADJUDICATION,
  buildHotelShadowCohort,
  HOTEL_IDS,
  buildJevQuestion,
  JEV_DECISION_TYPE,
  buildJevAuditCompact,
  jevAuditToAirtableFields,
  resetJevCircuitBreaker,
} from "../lib/group-demand-intelligence/jev/index.js";
import {
  loadHistoricalJevDecisions,
  countHistoricalSources,
} from "../lib/group-demand-intelligence/jev/historical-dataset.js";

let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}`, err && err.stack ? err.stack : err);
  }
}
async function checkAsync(name, fn) {
  try {
    await fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}`, err && err.stack ? err.stack : err);
  }
}

check("historical_dataset_min_200", () => {
  const rows = loadHistoricalJevDecisions({ minCount: 200 });
  assert.ok(rows.length >= 200, `got ${rows.length}`);
  const sources = countHistoricalSources(rows);
  assert.ok(sources.expanded_seed > 0 || sources.real_core > 0);
});

check("choice_filter_drops_private_for_dg", () => {
  const c = filterChoicesForContext("RESEARCH_PLAYBOOK", {
    targetType: "DEMAND_GENERATOR",
  });
  assert.ok(!c.includes("PRIVATE_EVENT_SIGNAL"));
  assert.ok(c.includes("DEMAND_GENERATOR"));
  assert.ok(c.includes("LODGING_HOUSING"));
});

check("choice_filter_keeps_private_for_venue", () => {
  const c = filterChoicesForContext("RESEARCH_PLAYBOOK", {
    targetType: "PRIVATE_EVENT_VENUE",
  });
  assert.ok(c.includes("PRIVATE_EVENT_SIGNAL"));
});

check("question_uses_gap_wording", () => {
  const q = buildJevQuestion("RESEARCH_PLAYBOOK");
  assert.match(
    q.RESEARCH_PLAYBOOK.instructions,
    /unresolved evidence gap/i
  );
});

check("enrich_adds_hard_policy_and_summary", () => {
  const e = enrichDecisionContext(
    { targetType: "PROGRAM", missingFields: ["futureCycle"] },
    { pastEvent: true }
  );
  assert.equal(e.hardPolicyContext.PAST_EVENT, true);
  assert.match(e.evidenceSummary, /targetType=PROGRAM/);
});

check("sanitize_keeps_evidence_summary_bounds", () => {
  const s = sanitizeJevState({
    evidenceSummary: "x".repeat(900),
    hotelName: "Bethesda Marriott",
    organizationName: "Woman's Club",
  });
  assert.ok(s.evidenceSummary.length <= 600);
  assert.ok(!s.hotelName);
  assert.ok(!s.organizationName);
});

check("adjudicate_forced_lodging_bias", () => {
  const r = adjudicateDisagreement({
    decisionType: "RESEARCH_PLAYBOOK",
    existingGdiDecision: "DEMAND_GENERATOR",
    jevDecision: "LODGING_HOUSING",
    jevConfidence: 0.95,
    context: { targetType: "DEMAND_GENERATOR", missingFields: ["lodgingEvidence"] },
    inputNotes: { forcedLodgingMissing: true },
  });
  assert.equal(r.adjudication, ADJUDICATION.INSUFFICIENT_EVIDENCE);
});

check("adjudicate_overflow_lodging_jev_correct", () => {
  const r = adjudicateDisagreement({
    decisionType: "RESEARCH_PLAYBOOK",
    existingGdiDecision: "DEMAND_GENERATOR",
    jevDecision: "LODGING_HOUSING",
    jevConfidence: 0.8,
    context: {
      opportunityType: "OVERFLOW_HOUSING",
      missingFields: ["lodgingEvidence"],
    },
  });
  assert.equal(r.adjudication, ADJUDICATION.JEV_CORRECT);
});

check("adjudicate_hard_gate_gdi_correct", () => {
  const r = adjudicateDisagreement({
    decisionType: "OPPORTUNITY_PREQUAL",
    existingGdiDecision: "LIKELY_REJECT",
    jevDecision: "LIKELY_ACTIONABLE",
    jevConfidence: 0.9,
    hardGates: ["PAST_EVENT"],
    policyContext: { pastEvent: true },
  });
  assert.equal(r.adjudication, ADJUDICATION.CURRENT_GDI_CORRECT);
});

check("hard_gate_still_wins_policy", () => {
  const p = applyJevPolicy({
    decisionType: "OPPORTUNITY_PREQUAL",
    jevSelected: "LIKELY_ACTIONABLE",
    confidence: 0.99,
    existingDecision: "LIKELY_REJECT",
    policyContext: { pastEvent: true },
    shadow: true,
  });
  assert.equal(p.finalPolicyDecision, "LIKELY_REJECT");
  assert.equal(p.policyOutcome, "HARD_GATE");
});

check("cohort_multi_hotel_nonzero", () => {
  for (const id of [HOTEL_IDS.BETHESDA, HOTEL_IDS.RENAISSANCE, HOTEL_IDS.CAMBRIDGE]) {
    const c = buildHotelShadowCohort(id, [], { minSize: 8, opportunityLimit: 25 });
    assert.ok(c.targets.length >= 8, `${id} got ${c.targets.length}`);
    for (const t of c.targets) {
      assert.ok(!JSON.stringify(t.context).includes("Bethesda"));
      assert.ok(!JSON.stringify(t.context).includes("Renaissance"));
      assert.ok(!JSON.stringify(t.context).includes("Cambridge"));
    }
  }
});

check("jev_audit_compact_no_raw", () => {
  const a = buildJevAuditCompact({
    decisionId: "x",
    decisionType: "RESEARCH_PLAYBOOK",
    selected: "LODGING_HOUSING",
    confidence: 0.8,
    model: "jev-1.13.0",
    shadow: true,
    inputHash: "abc",
    policy: { policyOutcome: "JEV_CANDIDATE" },
  });
  assert.ok(!("prompt" in a));
  assert.ok(!("raw" in a));
  const fields = jevAuditToAirtableFields(a, {
    availableFields: new Set(["jevDecisionId", "jevDecision"]),
  });
  assert.equal(fields.jevDecisionId, "x");
  assert.ok(!fields.jevConfidence);
});

await checkAsync("shadow_no_behavior_change_without_key", async () => {
  resetJevCircuitBreaker();
  const prev = process.env.JEZ_API_KEY;
  const prev2 = process.env.JEV_API_KEY;
  const prev3 = process.env.TYPESAFE_API_KEY;
  process.env.JEZ_API_KEY = "";
  process.env.JEV_API_KEY = "";
  process.env.TYPESAFE_API_KEY = "";
  try {
    const d = await decide({
      decisionType: JEV_DECISION_TYPE.RESEARCH_PLAYBOOK,
      context: { targetType: "PROGRAM", missingFields: ["futureCycle"] },
      existingDecision: "EVENT_FUTURE_CYCLE",
      forceShadow: true,
    });
    assert.equal(d.finalPolicyDecision, "EVENT_FUTURE_CYCLE");
    assert.equal(d.technicalFallback, true);
  } finally {
    if (prev != null) process.env.JEZ_API_KEY = prev;
    else delete process.env.JEZ_API_KEY;
    if (prev2 != null) process.env.JEV_API_KEY = prev2;
    if (prev3 != null) process.env.TYPESAFE_API_KEY = prev3;
  }
});

if (failed) {
  console.error(`FAILED ${failed}`);
  process.exit(1);
}
console.log("ALL_PASS");
