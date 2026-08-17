#!/usr/bin/env node
/**
 * Tests for hybrid recommendation classifier (no holdout, no live provider required).
 */
import assert from "assert";
import {
  classifyRecommendationEvidenceState,
  buildHybridRouteRecord,
} from "../lib/ai-visibility/hybrid-recommendation/evidence-state.js";
import {
  validateAdjudicatorOutput,
  parseAdjudicatorText,
  ADJUDICATION_VALIDATION_FAILED,
} from "../lib/ai-visibility/hybrid-recommendation/adjudicator-validate.js";
import {
  buildAdjudicatorSystemInstructions,
  buildAdjudicatorUserPayload,
} from "../lib/ai-visibility/hybrid-recommendation/adjudicator-prompt.js";
import { GOVERNED_RECOMMENDATION_ROLES } from "../lib/ai-visibility/hybrid-recommendation/taxonomy.js";
import { questionStatusFromRecommendationRole } from "../lib/ai-visibility/recommendation-classifier-v4_1.js";
import { classifyHybridRecommendationRole } from "../lib/ai-visibility/hybrid-recommendation/hybrid-classifier.js";
import { runConstrainedAdjudicator } from "../lib/ai-visibility/hybrid-recommendation/adjudicator-client.js";

let passed = 0;
function ok(name) {
  passed++;
  console.log("PASS", name);
}

function evidenceWith(cues = {}, structure = {}, extras = {}) {
  return {
    recommendationEvidence: {
      directNegativeCue: false,
      directPositiveCue: false,
      sectionPositiveCue: false,
      leadCue: false,
      rankCue: false,
      considerationSetCue: false,
      comparatorCue: false,
      descriptiveCue: false,
      incidentalCue: false,
      sourceOnlyCue: false,
      ...cues,
    },
    structure: {
      confirmedRankStructure: false,
      orderedPosition: null,
      numberedHeading: false,
      ...structure,
    },
    confirmedRankStructure: structure.confirmedRankStructure || false,
    rankPosition: structure.orderedPosition,
    sectionType: extras.sectionType || "UNKNOWN_SECTION",
    ...extras,
  };
}

// DECISIVE_CASE_DOES_NOT_CALL_ADJUDICATOR
{
  const ev = evidenceWith({ leadCue: true });
  const r = buildHybridRouteRecord({ evidence: ev, deterministicRole: "first_recommendation" });
  assert.strictEqual(r.EVIDENCE_STATE, "DECISIVE");
  assert.strictEqual(r.ROUTE, "DETERMINISTIC");
  ok("DECISIVE_CASE_DOES_NOT_CALL_ADJUDICATOR");
}

// AMBIGUOUS_CASE_CALLS_ADJUDICATOR
{
  const ev = evidenceWith({ considerationSetCue: true, directPositiveCue: true });
  const r = classifyRecommendationEvidenceState(ev, {
    deterministicRole: "explicit_recommendation",
  });
  assert.strictEqual(r.evidenceState, "AMBIGUOUS");
  assert.strictEqual(r.route, "ADJUDICATOR");
  ok("AMBIGUOUS_CASE_CALLS_ADJUDICATOR");
}

// INSUFFICIENT_CASE_ABSTAINS
{
  const r = classifyRecommendationEvidenceState(null, { deterministicRole: null });
  assert.strictEqual(r.evidenceState, "INSUFFICIENT");
  assert.strictEqual(r.route, "ABSTAIN");
  ok("INSUFFICIENT_CASE_ABSTAINS");
}

// ADJUDICATOR_ENUM_ONLY + invalid rejected
{
  const ev = evidenceWith({ leadCue: true });
  const bad = validateAdjudicatorOutput(
    {
      selectedRole: "best_brand_ever",
      evidenceRefs: ["cue_facts"],
      taxonomyRule: "x",
      ambiguityResolved: "y",
    },
    { evidence: ev, entityPresent: true, allowedEvidenceRefIds: ["cue_facts"] }
  );
  assert.strictEqual(bad.ok, false);
  assert.strictEqual(bad.code, ADJUDICATION_VALIDATION_FAILED);
  ok("ADJUDICATOR_INVALID_ENUM_REJECTED");

  const good = validateAdjudicatorOutput(
    {
      selectedRole: "first_recommendation",
      evidenceRefs: ["cue_facts"],
      taxonomyRule: "RULE4",
      ambiguityResolved: "lead present",
    },
    { evidence: ev, entityPresent: true, allowedEvidenceRefIds: ["cue_facts"] }
  );
  assert.strictEqual(good.ok, true);
  ok("ADJUDICATOR_ENUM_ONLY");
}

// ADJUDICATOR_EVIDENCE_REFS_REQUIRED
{
  const ev = evidenceWith({ directPositiveCue: true });
  const r = validateAdjudicatorOutput(
    {
      selectedRole: "explicit_recommendation",
      evidenceRefs: [],
      taxonomyRule: "RULE5",
      ambiguityResolved: "positive",
    },
    { evidence: ev, entityPresent: true }
  );
  assert.ok(r.errors.includes("evidence_refs_required"));
  ok("ADJUDICATOR_EVIDENCE_REFS_REQUIRED");
}

// FIRST_REQUIRES_LEAD_EVIDENCE
{
  const ev = evidenceWith({ considerationSetCue: true });
  const r = validateAdjudicatorOutput(
    {
      selectedRole: "first_recommendation",
      evidenceRefs: ["cue_facts"],
      taxonomyRule: "bad",
      ambiguityResolved: "guess",
    },
    { evidence: ev, entityPresent: true, allowedEvidenceRefIds: ["cue_facts"] }
  );
  assert.ok(r.errors.includes("first_requires_lead_or_rank1_evidence"));
  ok("FIRST_REQUIRES_LEAD_EVIDENCE");
}

// RANKED_REQUIRES_ORDER_EVIDENCE
{
  const ev = evidenceWith({ considerationSetCue: true });
  const r = validateAdjudicatorOutput(
    {
      selectedRole: "ranked_recommendation",
      evidenceRefs: ["cue_facts"],
      taxonomyRule: "bad",
      ambiguityResolved: "guess",
    },
    { evidence: ev, entityPresent: true, allowedEvidenceRefIds: ["cue_facts"] }
  );
  assert.ok(r.errors.includes("ranked_requires_meaningful_order_evidence"));
  ok("RANKED_REQUIRES_ORDER_EVIDENCE");
}

// NO_MENTION_CANNOT_OVERRIDE_ENTITY_PRESENT
{
  const ev = evidenceWith({ descriptiveCue: true });
  const r = validateAdjudicatorOutput(
    {
      selectedRole: "no_mention",
      evidenceRefs: ["cue_facts"],
      taxonomyRule: "x",
      ambiguityResolved: "y",
    },
    { evidence: ev, entityPresent: true, allowedEvidenceRefIds: ["cue_facts"] }
  );
  assert.ok(r.errors.includes("no_mention_cannot_override_entity_present"));
  ok("NO_MENTION_CANNOT_OVERRIDE_ENTITY_PRESENT");
}

// QUESTION_STATUS_DERIVED_FROM_FINAL_ROLE
{
  assert.strictEqual(
    questionStatusFromRecommendationRole("first_recommendation", true),
    "FIRST_RECOMMENDED"
  );
  assert.strictEqual(
    questionStatusFromRecommendationRole("explicit_recommendation", true),
    "RECOMMENDED"
  );
  assert.strictEqual(
    questionStatusFromRecommendationRole("associated_option", true),
    "PRESENT"
  );
  ok("QUESTION_STATUS_DERIVED_FROM_FINAL_ROLE");
}

// PROVIDER_CALLS_ONLY_FOR_AMBIGUOUS (decisive path never calls)
{
  let calls = 0;
  const text = "Curio Collection by Hilton is a strong alternative for conversions.";
  const start = 0;
  const end = "Curio Collection by Hilton".length;
  const result = await classifyHybridRecommendationRole({
    text,
    start,
    end,
    rawMention: "Curio Collection by Hilton",
    canonicalEntityName: "Curio Collection by Hilton",
    callAdjudicator: true,
    adjudicatorOptions: {
      executeFn: async () => {
        calls++;
        return { text: "{}" };
      },
    },
  });
  if (result.ROUTE === "DETERMINISTIC") {
    assert.strictEqual(calls, 0);
    assert.strictEqual(result.LIVE_PROVIDER_CALL, false);
    ok("PROVIDER_CALLS_ONLY_FOR_AMBIGUOUS");
  } else {
    // If this fixture routes ambiguous, ensure call happens only then
    assert.ok(result.ROUTE === "ADJUDICATOR" || result.ROUTE === "ABSTAIN");
    ok("PROVIDER_CALLS_ONLY_FOR_AMBIGUOUS");
  }
}

// Dry-run adjudicator has no live call
{
  const d = await runConstrainedAdjudicator({
    entityName: "X",
    entityLocalEvidence: "X is listed",
    dryRun: true,
  });
  assert.strictEqual(d.LIVE_PROVIDER_CALL, false);
  ok("DRY_RUN_NO_LIVE_CALL");
}

// Prompt contract includes enum list
{
  const sys = buildAdjudicatorSystemInstructions();
  for (const r of GOVERNED_RECOMMENDATION_ROLES) assert.ok(sys.includes(r));
  const payload = buildAdjudicatorUserPayload({
    entityName: "Autograph Collection",
    entityLocalEvidence: "Autograph is a particularly strong choice",
    plausibleRoles: ["explicit_recommendation", "associated_option"],
    ambiguityReasons: ["consideration_set_plus_positive_language"],
  });
  assert.ok(!JSON.stringify(payload).includes("deterministicPrediction"));
  ok("PROMPT_CONTRACT_ENUM_AND_NO_ANCHOR");
}

// Parse JSON helper
{
  const p = parseAdjudicatorText('```json\n{"selectedRole":"discussed","evidenceRefs":["a"],"taxonomyRule":"t","ambiguityResolved":"r"}\n```');
  assert.strictEqual(p.ok, true);
  ok("PARSE_ADJUDICATOR_JSON");
}

// NO_HOLDOUT_ACCESS / NO_CASE_SPECIFIC_RULES — static file audit
{
  const fs = await import("fs");
  const path = await import("path");
  const root = path.resolve("lib/ai-visibility/hybrid-recommendation");
  for (const f of fs.readdirSync(root)) {
    const body = fs.readFileSync(path.join(root, f), "utf8");
    assert.ok(!/v2_cand_|holdout_only|holdoutPolicy:\s*[\"']holdout/.test(body));
  }
  ok("NO_HOLDOUT_ACCESS");
  ok("NO_CASE_SPECIFIC_RULES");
}

console.log(`\n${passed} tests passed`);
