#!/usr/bin/env node
import assert from "assert";
import {
  deriveNodeLabelsFromHumanRole,
  composeRoleFromNodePath,
  decideNodeDeterministic,
  HIERARCHICAL_CLASSIFIER_VERSION,
} from "../lib/ai-visibility/hierarchical-recommendation/index.js";

let n = 0;
function ok(name) {
  n++;
  console.log("PASS", name);
}

{
  const labels = deriveNodeLabelsFromHumanRole("discussed");
  assert.strictEqual(labels.Q3, "NEUTRAL_DISCUSSION");
  assert.strictEqual(labels.Q4, null);
  ok("DERIVE_DISCUSSED");
}
{
  const labels = deriveNodeLabelsFromHumanRole("associated_option");
  assert.strictEqual(labels.Q3, "DECISION_OPTION");
  assert.strictEqual(labels.Q4, "CONSIDERATION_SET");
  ok("DERIVE_ASSOCIATED");
}
{
  const labels = deriveNodeLabelsFromHumanRole("first_recommendation");
  assert.strictEqual(labels.Q6, "LEAD");
  assert.strictEqual(labels.Q5, "MEANINGFUL_ORDER");
  ok("DERIVE_FIRST");
}
{
  const role = composeRoleFromNodePath({
    Q1: "SUBSTANTIVE",
    Q2: "NO_NEGATIVE",
    Q3: "DECISION_OPTION",
    Q4: "DIRECT_ENDORSEMENT",
    Q5: "NO_MEANINGFUL_ORDER",
  });
  assert.strictEqual(role, "explicit_recommendation");
  ok("COMPOSE_EXPLICIT");
}
{
  const role = composeRoleFromNodePath({
    Q1: "SUBSTANTIVE",
    Q2: "NO_NEGATIVE",
    Q3: "DECISION_OPTION",
    Q4: "DIRECT_ENDORSEMENT",
    Q5: "MEANINGFUL_ORDER",
    Q6: "LEAD",
  });
  assert.strictEqual(role, "first_recommendation");
  ok("COMPOSE_FIRST");
}
{
  const evidence = {
    recommendationEvidence: {
      leadCue: true,
      directPositiveCue: true,
      considerationSetCue: false,
      comparatorCue: false,
      descriptiveCue: false,
      incidentalCue: false,
      sourceOnlyCue: false,
      directNegativeCue: false,
      sectionPositiveCue: false,
      rankCue: false,
    },
    structure: { confirmedRankStructure: false },
  };
  const q6 = decideNodeDeterministic("Q6", evidence, { entityPresent: true });
  assert.strictEqual(q6.result, "LEAD");
  assert.strictEqual(q6.needsAdjudicator, false);
  ok("DET_Q6_LEAD");
}
{
  assert.ok(HIERARCHICAL_CLASSIFIER_VERSION.includes("v5_hierarchical"));
  ok("VERSION");
}

console.log(`\n${n} tests passed`);
