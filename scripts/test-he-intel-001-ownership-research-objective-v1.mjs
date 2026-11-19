#!/usr/bin/env node
/**
 * Ownership research objective — evidence-grounded FULL_ACTIONABLE gates.
 */
import assert from "node:assert/strict";
import {
  OWNERSHIP_RESEARCH_OBJECTIVE_ID,
  OWNERSHIP_DRAWER_QUESTIONS,
  OWNERSHIP_SUCCESS_TIER,
  classifyOwnershipResearchSuccess,
  buildClassifierInputFromClaims,
  buildClassifierInputFromStaged,
  evaluateDualOrgRoles,
  scoreUltimateOwnershipDimension,
  writeOwnershipObjectiveGuarantees,
} from "../lib/hotel-intelligence/research-engine-v1/ownership-research-objective.js";
import {
  buildClaimEnvelope,
  CLAIM_TYPES,
} from "../lib/hotel-intelligence/research-engine-v1/benchmark-p3a/claim-schema.js";
import { scoreClaims } from "../lib/hotel-intelligence/research-engine-v1/benchmark-p3a/quality-rubric.js";
import { CLAIM_ACCEPTANCE } from "../lib/hotel-intelligence/research-engine-v1/benchmark-p3a/contract.js";
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

function verifiedOwner(extra = {}) {
  return {
    verified: true,
    name: "Controlling Family Office",
    evidenceUrl: "https://example.com/owner",
    linkedToHpc: true,
    ...extra,
  };
}
function verifiedPerson(extra = {}) {
  return {
    verified: true,
    name: "Jane Asset Manager",
    evidenceUrl: "https://example.com/person",
    roleRelevant: true,
    connectedToOwnerOrRepresentative: true,
    ...extra,
  };
}
function verifiedDirectContact(extra = {}) {
  return {
    directVerified: true,
    indirectInstitutionalPathway: false,
    inferred: false,
    evidenceUrl: "https://example.com/email",
    connectedToPersonOrOwner: true,
    ...extra,
  };
}
function verifiedStructure(extra = {}) {
  return {
    verified: true,
    evidenceUrl: "https://example.com/structure",
    relevant: true,
    ...extra,
  };
}

test("objective_contract", () => {
  assert.equal(OWNERSHIP_RESEARCH_OBJECTIVE_ID, "HE_INTEL_001_OWNERSHIP_RESEARCH_OBJECTIVE_V1");
  assert.equal(OWNERSHIP_DRAWER_QUESTIONS.length, 4);
  assert.equal(writeOwnershipObjectiveGuarantees().full_actionable_requires_evidence, true);
});

test("legal_llc_only_partial", () => {
  const o = classifyOwnershipResearchSuccess({
    legalTitleholder: {
      verified: true,
      name: "Hotel Holdco LLC",
      evidenceUrl: "https://example.com/llc",
      linkedToHpc: true,
    },
  });
  assert.equal(o.tier, OWNERSHIP_SUCCESS_TIER.PARTIAL_LEGAL_ENTITY_ONLY);
  assert.equal(o.fullResearchSuccess, false);
});

test("actual_owner_without_relevant_contact_partial", () => {
  const o = classifyOwnershipResearchSuccess({
    ultimateOwner: verifiedOwner(),
    ownershipPerson: verifiedPerson(),
    structureChain: verifiedStructure(),
    contact: { directVerified: false, indirectInstitutionalPathway: false },
  });
  assert.equal(o.tier, OWNERSHIP_SUCCESS_TIER.PARTIAL_OWNER_WITHOUT_CONTACT);
  assert.equal(o.fullResearchSuccess, false);
});

test("owner_plus_unrelated_hotel_salesperson_not_full", () => {
  const o = classifyOwnershipResearchSuccess({
    ultimateOwner: verifiedOwner(),
    ownershipPerson: verifiedPerson({
      unrelatedHotelSales: true,
      roleRelevant: false,
      name: "Front Desk Sales",
    }),
    contact: verifiedDirectContact(),
    structureChain: verifiedStructure(),
  });
  assert.notEqual(o.tier, OWNERSHIP_SUCCESS_TIER.FULL_ACTIONABLE);
  assert.equal(o.fullResearchSuccess, false);
});

test("owner_plus_unverified_email_not_full", () => {
  const o = classifyOwnershipResearchSuccess({
    ultimateOwner: verifiedOwner(),
    ownershipPerson: verifiedPerson(),
    structureChain: verifiedStructure(),
    contact: {
      directVerified: false,
      inferred: true,
      evidenceUrl: null,
      connectedToPersonOrOwner: false,
    },
  });
  assert.equal(o.fullResearchSuccess, false);
});

test("owner_plus_verified_rep_and_credible_route_actionable", () => {
  const o = classifyOwnershipResearchSuccess({
    ultimateOwner: verifiedOwner(),
    ownershipPerson: verifiedPerson(),
    contact: verifiedDirectContact(),
    structureChain: verifiedStructure(),
  });
  assert.equal(o.tier, OWNERSHIP_SUCCESS_TIER.FULL_ACTIONABLE);
  assert.equal(o.fullResearchSuccess, true);
  assert.equal(o.contactChannelClass, "VERIFIED_DIRECT");
  assert.equal(o.customerPublication, "BLOCKED");
});

test("indirect_institutional_pathway_separated_from_direct", () => {
  const o = classifyOwnershipResearchSuccess({
    ultimateOwner: verifiedOwner(),
    ownershipPerson: verifiedPerson(),
    contact: {
      directVerified: false,
      indirectInstitutionalPathway: true,
      inferred: false,
      evidenceUrl: "https://example.com/ir-page",
      connectedToPersonOrOwner: true,
    },
    structureChain: verifiedStructure(),
  });
  assert.equal(o.fullResearchSuccess, true);
  assert.equal(o.contactChannelClass, "INDIRECT_INSTITUTIONAL");
});

test("owner_operator_dual_roles_independently_verified", () => {
  const dual = evaluateDualOrgRoles([
    {
      orgId: "org_1",
      roles: ["OWNER", "OPERATOR"],
      ownershipVerifiedIndependently: true,
      operationVerifiedIndependently: true,
      ownershipInferredFromManagementOnly: false,
    },
  ]);
  assert.equal(dual.supported.length, 1);
  assert.equal(dual.rejected.length, 0);

  const inferred = evaluateDualOrgRoles([
    {
      orgId: "org_2",
      roles: ["OWNER", "OPERATOR"],
      ownershipVerifiedIndependently: false,
      operationVerifiedIndependently: true,
      ownershipInferredFromManagementOnly: true,
      claimsOwnership: true,
    },
  ]);
  assert.equal(inferred.rejected.length, 1);

  const o = classifyOwnershipResearchSuccess({
    ultimateOwner: verifiedOwner(),
    ownershipPerson: verifiedPerson(),
    contact: verifiedDirectContact(),
    structureChain: verifiedStructure(),
    orgRoles: [
      {
        orgId: "org_1",
        roles: ["OWNER", "OPERATOR"],
        ownershipVerifiedIndependently: true,
        operationVerifiedIndependently: true,
      },
    ],
  });
  assert.equal(o.fullResearchSuccess, true);
  assert.equal(o.dualOrgRoles.supported.length, 1);
});

test("staged_but_unsupported_owner_claims_not_full", () => {
  const o = classifyOwnershipResearchSuccess({
    stagedOnly: true,
    ultimateOwner: {
      verified: false,
      name: "Maybe Owner",
      evidenceUrl: null,
      linkedToHpc: false,
      unsupported: true,
    },
    legalTitleholder: {
      verified: false,
      name: "Staged LLC",
      evidenceUrl: null,
    },
  });
  assert.equal(o.fullResearchSuccess, false);
  assert.equal(o.provisionalStaged, true);
  assert.notEqual(o.tier, OWNERSHIP_SUCCESS_TIER.FULL_ACTIONABLE);
});

test("labels_alone_do_not_create_full_actionable_from_claims", () => {
  const mk = (claimType, value, extra = {}) =>
    buildClaimEnvelope({
      claimType,
      value,
      supportingSource: { url: extra.url || null },
      independentVerification: { ok: Boolean(extra.url), method: "t" },
      acceptanceDecision: CLAIM_ACCEPTANCE.ACCEPTED,
      ...extra,
    });
  // Labels with no hotel link / person connection flags
  const claims = [
    mk(CLAIM_TYPES.ULTIMATE_OWNER, "X", { url: "https://example.com/o" }),
    mk(CLAIM_TYPES.OWNERSHIP_DECISION_MAKER, "Y", { url: "https://example.com/p" }),
    mk(CLAIM_TYPES.PUBLIC_EMAIL, "a@b.com", { url: "https://example.com/e" }),
    mk(CLAIM_TYPES.OWNERSHIP_STRUCTURE_EDGE, "edge", { url: "https://example.com/s" }),
  ];
  const input = buildClassifierInputFromClaims(claims, { linkedToHpc: false });
  const o = classifyOwnershipResearchSuccess(input);
  assert.equal(o.fullResearchSuccess, false);
});

test("rubric_uses_evidence_classifier_not_labels_alone", () => {
  const claims = [
    buildClaimEnvelope({
      claimType: CLAIM_TYPES.OWNER_ENTITY,
      value: "LLC Only",
      supportingSource: { url: "https://example.com/llc" },
      independentVerification: { ok: true, method: "t" },
      acceptanceDecision: CLAIM_ACCEPTANCE.ACCEPTED,
      linkedToHpc: true,
    }),
  ];
  const s = scoreClaims(claims, { linkedToHpc: true });
  assert.equal(s.legalEntityOnlyIsPartial || s.ownershipOutcome.partial, true);
  assert.equal(s.fullResearchSuccess, false);
});

test("he_projection_matches_benchmark_classifier", () => {
  const stagedFull = {
    hpc_id: "recLuxvwwxID7U2B8",
    hotel_name: "Bethesda Marriott",
    ownership: {
      ultimate_owner: {
        legal_name: "Controlling Family Office",
        verified: true,
        evidence_url: "https://example.com/owner",
        linked_to_hpc: true,
      },
      primary_ownership_contact: {
        full_name: "Jane Asset Manager",
        verified: true,
        evidence_url: "https://example.com/person",
        role_relevant: true,
        connected_to_owner_or_representative: true,
      },
      primary_contact_channels: [
        {
          kind: "email",
          value: "jane@example.com",
          verified: true,
          evidence_url: "https://example.com/email",
          connected_to_person_or_owner: true,
        },
      ],
      structure_chain: {
        verified: true,
        evidence_url: "https://example.com/structure",
        relevant: true,
      },
    },
  };
  const vm = projectHotelExplorerContactViewModel(stagedFull);
  const direct = classifyOwnershipResearchSuccess(buildClassifierInputFromStaged(stagedFull));
  assert.equal(vm.ownershipOutcome.tier, direct.tier);
  assert.equal(vm.ownershipOutcome.fullResearchSuccess, direct.fullResearchSuccess);
  assert.equal(vm.qualitySummary.fullResearchSuccess, true);
  assert.equal(vm.qualitySummary.customerPublication, "BLOCKED");
  assert.equal(vm.ownershipSummary.verifiedDirectContact, true);
  assert.equal(vm.research_complete, false);
});

test("he_projection_populated_arrays_without_verification_not_full", () => {
  const vm = projectHotelExplorerContactViewModel({
    hpc_id: "recLuxvwwxID7U2B8",
    hotel_name: "Bethesda Marriott",
    ownership: {
      owner_entity: { legal_name: "Staged LLC" },
      ultimate_owner: { legal_name: "Unverified Owner" },
      primary_ownership_contact: { full_name: "Someone" },
      primary_contact_channels: [{ kind: "email", value: "x@y.com" }],
    },
    persons: [{ name: "Someone" }],
  });
  assert.equal(vm.qualitySummary.fullResearchSuccess, false);
  assert.equal(vm.qualitySummary.provisionalStaged, true);
  assert.notEqual(vm.ownershipSummary.successTier, OWNERSHIP_SUCCESS_TIER.FULL_ACTIONABLE);
});

test("he_projection_legal_only_partial", () => {
  const vm = projectHotelExplorerContactViewModel({
    hpc_id: "recLuxvwwxID7U2B8",
    hotel_name: "Bethesda Marriott",
    ownership: {
      legal_titleholder: {
        legal_name: "Bethesda PropCo LLC",
        verified: true,
        evidence_url: "https://example.com/llc",
        linked_to_hpc: true,
      },
      ultimate_owner_status: "UNRESOLVED",
    },
  });
  assert.equal(
    vm.ownershipSummary.successTier,
    OWNERSHIP_SUCCESS_TIER.PARTIAL_LEGAL_ENTITY_ONLY
  );
  assert.equal(vm.ownershipSummary.operator.notAnOwner, true);
  assert.equal(vm.ownershipSummary.brand.notAnOwner, true);
});

test("p3b_priorities_include_ultimate_owner", () => {
  assert.ok(
    PRIORITY_OBJECTIVES.includes(
      "ULTIMATE_OWNER_INVESTOR_FAMILY_FUND_REIT_OR_CONTROLLING_ORG"
    )
  );
});

test("score_dimension_evidence_object", () => {
  assert.equal(
    scoreUltimateOwnershipDimension({
      ultimateOwner: verifiedOwner(),
      ownershipPerson: verifiedPerson(),
      contact: verifiedDirectContact(),
      structureChain: verifiedStructure(),
    }),
    3
  );
  assert.equal(
    scoreUltimateOwnershipDimension({
      legalTitleholder: {
        verified: true,
        name: "LLC",
        evidenceUrl: "https://example.com/llc",
        linkedToHpc: true,
      },
    }),
    1
  );
});

console.log(`\nOK he-intel-001-ownership-research-objective-v1 (${passed} assertions)`);
