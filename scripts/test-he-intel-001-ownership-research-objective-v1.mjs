#!/usr/bin/env node
/**
 * Ownership research objective correction — legal-entity-only is partial.
 */
import assert from "node:assert/strict";
import {
  OWNERSHIP_RESEARCH_OBJECTIVE_ID,
  OWNERSHIP_DRAWER_QUESTIONS,
  OWNERSHIP_SUCCESS_TIER,
  classifyOwnershipResearchSuccess,
  scoreUltimateOwnershipDimension,
  writeOwnershipObjectiveGuarantees,
} from "../lib/hotel-intelligence/research-engine-v1/ownership-research-objective.js";
import {
  buildClaimEnvelope,
  CLAIM_TYPES,
} from "../lib/hotel-intelligence/research-engine-v1/benchmark-p3a/claim-schema.js";
import { scoreClaims } from "../lib/hotel-intelligence/research-engine-v1/benchmark-p3a/quality-rubric.js";
import { CLAIM_ACCEPTANCE, EVIDENCE_LAYER } from "../lib/hotel-intelligence/research-engine-v1/benchmark-p3a/contract.js";
import { projectHotelExplorerContactViewModel } from "../lib/hotel-intelligence/research-engine-v1/he-contact-projection.js";
import { PRIORITY_OBJECTIVES } from "../lib/hotel-intelligence/research-engine-v1/benchmark-p3b-m1/evidence.js";

let passed = 0;
function pass(name) {
  console.log(`PASS ${name}`);
  passed += 1;
}
function test(name, fn) {
  fn();
  pass(name);
}

test("objective_contract_and_drawer_questions", () => {
  assert.equal(OWNERSHIP_RESEARCH_OBJECTIVE_ID, "HE_INTEL_001_OWNERSHIP_RESEARCH_OBJECTIVE_V1");
  assert.equal(OWNERSHIP_DRAWER_QUESTIONS.length, 4);
  assert.ok(OWNERSHIP_DRAWER_QUESTIONS[0].includes("Who owns"));
  assert.equal(writeOwnershipObjectiveGuarantees().legal_entity_only_is_partial, true);
});

test("legal_entity_only_is_partial_not_full_success", () => {
  const outcome = classifyOwnershipResearchSuccess({
    acceptedClaimTypes: [CLAIM_TYPES.OWNER_ENTITY, CLAIM_TYPES.LEGAL_TITLEHOLDER],
  });
  assert.equal(outcome.tier, OWNERSHIP_SUCCESS_TIER.PARTIAL_LEGAL_ENTITY_ONLY);
  assert.equal(outcome.fullResearchSuccess, false);
  assert.equal(scoreUltimateOwnershipDimension(new Set(["OWNER_ENTITY"])), 1);
});

test("ultimate_owner_scores_higher_than_legal_only", () => {
  assert.equal(scoreUltimateOwnershipDimension(new Set(["ULTIMATE_OWNER"])), 3);
  assert.ok(
    scoreUltimateOwnershipDimension(new Set(["ULTIMATE_OWNER"])) >
      scoreUltimateOwnershipDimension(new Set(["LEGAL_TITLEHOLDER"]))
  );
});

test("full_actionable_requires_owner_dm_contact_structure", () => {
  const outcome = classifyOwnershipResearchSuccess({
    acceptedClaimTypes: [
      "ULTIMATE_OWNER",
      "OWNERSHIP_DECISION_MAKER",
      "PUBLIC_EMAIL",
      "OWNERSHIP_STRUCTURE_EDGE",
    ],
  });
  assert.equal(outcome.tier, OWNERSHIP_SUCCESS_TIER.FULL_ACTIONABLE);
  assert.equal(outcome.fullResearchSuccess, true);
});

test("unresolved_ultimate_with_representative_pathway", () => {
  const outcome = classifyOwnershipResearchSuccess({
    acceptedClaimTypes: ["OWNERSHIP_REPRESENTATIVE", "ALTERNATIVE_CONTACT_PATHWAY"],
    ultimateOwnerUnresolved: true,
  });
  assert.equal(outcome.tier, OWNERSHIP_SUCCESS_TIER.PARTIAL_REPRESENTATIVE_PATHWAY);
  assert.equal(outcome.unresolvedUltimateOwnership, true);
});

test("rubric_legal_only_not_full_research_success", () => {
  const claims = [
    buildClaimEnvelope({
      claimType: CLAIM_TYPES.OWNER_ENTITY,
      value: "Hotel Holdco LLC",
      supportingSource: { url: "https://example.com/llc" },
      independentVerification: { ok: true, method: "t" },
      acceptanceDecision: CLAIM_ACCEPTANCE.ACCEPTED,
    }),
  ];
  const s = scoreClaims(claims);
  assert.equal(s.scores.ultimate_ownership, 1);
  assert.equal(s.legalEntityOnlyIsPartial, true);
  assert.equal(s.fullResearchSuccess, false);
});

test("rubric_full_actionable_path", () => {
  const mk = (claimType, value) =>
    buildClaimEnvelope({
      claimType,
      value,
      supportingSource: { url: "https://example.com/" + claimType },
      independentVerification: { ok: true, method: "t" },
      acceptanceDecision: CLAIM_ACCEPTANCE.ACCEPTED,
    });
  const claims = [
    mk(CLAIM_TYPES.ULTIMATE_OWNER, "Controlling Family Office"),
    mk(CLAIM_TYPES.OWNERSHIP_DECISION_MAKER, "Jane Owner"),
    mk(CLAIM_TYPES.PUBLIC_EMAIL, "jane@example.com"),
    mk(CLAIM_TYPES.OWNERSHIP_STRUCTURE_EDGE, "Holdco → OpCo"),
    mk(CLAIM_TYPES.OWNER_VS_OPERATOR, "DISTINCT"),
  ];
  const s = scoreClaims(claims);
  assert.equal(s.ownershipOutcome.fullResearchSuccess, true);
  assert.equal(s.fullResearchSuccess, true);
  assert.equal(s.scores.ultimate_ownership, 3);
});

test("he_projection_drawer_answers_and_partial_legal", () => {
  const vm = projectHotelExplorerContactViewModel({
    hpc_id: "recLuxvwwxID7U2B8",
    hotel_name: "Bethesda Marriott",
    ownership: {
      owner_entity: { legal_name: "Bethesda PropCo LLC", org_id: "dle_x" },
      ultimate_owner_status: "UNRESOLVED",
    },
  });
  assert.equal(vm.ownershipSummary.successTier, OWNERSHIP_SUCCESS_TIER.PARTIAL_LEGAL_ENTITY_ONLY);
  assert.deepEqual(vm.ownershipSummary.drawerQuestions, [...OWNERSHIP_DRAWER_QUESTIONS]);
  assert.equal(vm.ownershipSummary.operator.notAnOwner, true);
  assert.equal(vm.ownershipSummary.brand.notAnOwner, true);
  assert.equal(vm.qualitySummary.fullResearchSuccess, false);
});

test("p3b_priority_objectives_updated", () => {
  assert.ok(PRIORITY_OBJECTIVES.includes("ULTIMATE_OWNER_INVESTOR_FAMILY_FUND_REIT_OR_CONTROLLING_ORG"));
  assert.ok(!PRIORITY_OBJECTIVES.includes("LEGAL_OR_ECONOMIC_OWNER"));
});

console.log(`\nOK he-intel-001-ownership-research-objective-v1 (${passed} assertions)`);
