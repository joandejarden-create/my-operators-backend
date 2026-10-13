#!/usr/bin/env node
/**
 * Regressions — canonical merge review/apply path + phone pilot.
 */
import assert from "node:assert/strict";
import {
  IDENTITY_DECISION,
  EMAIL_OUTCOME,
  PHONE_OUTCOME,
} from "../lib/hotel-intelligence/contact-intelligence/surfe-identity-acceptance.js";
import {
  FIELD_MERGE_ACTION,
  MERGE_SAFETY_TIER,
  MERGE_WRITE_MODE,
  FIELD_SOURCE_CLASS,
  CANONICAL_FIELD_KIND,
  createCanonicalField,
  HOTEL_FEEDBACK_STATE,
  applyHotelFeedback,
} from "../lib/hotel-intelligence/contact-intelligence/canonical-merge-policy.js";
import {
  createReviewApplyStore,
  generateMergeProposalsFromReachability,
  approveCanonicalContactMergeProposal,
  rejectCanonicalContactMergeProposal,
  applyApprovedCanonicalContactMerge,
  rollbackCanonicalContactMerge,
  validateProposalForApply,
  evaluatePhoneAutoApplyEligibility,
  resolveCanonicalReuseAfterApply,
  fieldStateFingerprint,
  PROPOSAL_STATUS,
  APPLY_RESULT,
  APPROVAL_PATH,
} from "../lib/hotel-intelligence/contact-intelligence/canonical-merge-review-apply.js";
import { isPaidEnrichmentEnabled } from "../lib/hotel-intelligence/contact-intelligence/policy.js";

let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log("PASS", name);
  } catch (err) {
    failed += 1;
    console.error("FAIL", name, err.message);
  }
}

function makeDryRunRow(overrides = {}) {
  return {
    name: "Ada Lovelace",
    organization: "Example Org",
    opportunityId: "opp_1",
    identity: IDENTITY_DECISION.ACCEPTED,
    decisions: [
      {
        field: "EMAIL",
        action: FIELD_MERGE_ACTION.ACCEPT_NEW_FIELD,
        reasonCode: "ACCEPT_NEW_MISSING_FIELD",
        safetyTier: MERGE_SAFETY_TIER.TIER_1_SAFE,
        previousValue: null,
        proposedValue: "ada@example.org",
        explanation: "fill",
      },
    ],
    ...overrides,
  };
}

function makeReachRow(overrides = {}) {
  return {
    opportunityId: "opp_1",
    opportunityTitle: "Example Event",
    role: "Director",
    whoConfidence: "HIGH",
    identity: { decision: IDENTITY_DECISION.ACCEPTED },
    surfe: { emailOutcome: EMAIL_OUTCOME.NEW_DIRECT_WORK_EMAIL, phoneOutcome: PHONE_OUTCOME.NOT_FOUND },
    creditsSpent: 1,
    ...overrides,
  };
}

check("1. TIER_1_SAFE approved email → applies", () => {
  const store = createReviewApplyStore();
  const [p] = generateMergeProposalsFromReachability(store, {
    dryRunRow: makeDryRunRow(),
    reachabilityRow: makeReachRow(),
  });
  approveCanonicalContactMergeProposal(store, p.proposalId, { reviewer: "FOUNDER_REVIEW" });
  const r = applyApprovedCanonicalContactMerge(store, p.proposalId);
  assert.equal(r.ok, true);
  assert.equal(r.result, APPLY_RESULT.APPLIED);
  assert.equal(r.person.fields.EMAIL.value, "ada@example.org");
  assert.equal(r.person.fields.EMAIL.provenance.provider, "surfe");
});

check("2. TIER_1 unapproved email → blocked", () => {
  const store = createReviewApplyStore();
  const [p] = generateMergeProposalsFromReachability(store, {
    dryRunRow: makeDryRunRow(),
    reachabilityRow: makeReachRow(),
  });
  const r = applyApprovedCanonicalContactMerge(store, p.proposalId);
  assert.equal(r.ok, false);
  assert.equal(r.result, APPLY_RESULT.BLOCKED_NOT_APPROVED);
});

check("3. TIER_2 email → not generated for safe apply path", () => {
  const store = createReviewApplyStore();
  const props = generateMergeProposalsFromReachability(store, {
    dryRunRow: makeDryRunRow({
      decisions: [
        {
          field: "EMAIL",
          action: FIELD_MERGE_ACTION.ACCEPT_NEW_FIELD,
          safetyTier: MERGE_SAFETY_TIER.TIER_2_REVIEW,
          previousValue: null,
          proposedValue: "ada@example.org",
        },
      ],
    }),
    reachabilityRow: makeReachRow(),
  });
  assert.equal(props.length, 0);
});

check("4. TIER_3 → not generated", () => {
  const store = createReviewApplyStore();
  const props = generateMergeProposalsFromReachability(store, {
    dryRunRow: makeDryRunRow({
      identity: IDENTITY_DECISION.AMBIGUOUS,
      decisions: [
        {
          field: "EMAIL",
          action: FIELD_MERGE_ACTION.REJECT_FIELD,
          safetyTier: MERGE_SAFETY_TIER.TIER_3_BLOCK,
          previousValue: null,
          proposedValue: null,
        },
      ],
    }),
    reachabilityRow: makeReachRow(),
  });
  assert.equal(props.length, 0);
});

check("5. AMBIGUOUS identity phone → not auto-applied", () => {
  const store = createReviewApplyStore();
  const props = generateMergeProposalsFromReachability(store, {
    dryRunRow: makeDryRunRow({
      identity: IDENTITY_DECISION.AMBIGUOUS,
      decisions: [
        {
          field: "MOBILE",
          action: FIELD_MERGE_ACTION.ACCEPT_NEW_FIELD,
          safetyTier: MERGE_SAFETY_TIER.TIER_1_SAFE,
          previousValue: null,
          proposedValue: "+15550100",
        },
      ],
    }),
    reachabilityRow: makeReachRow({
      surfe: { phoneOutcome: PHONE_OUTCOME.NEW_MOBILE_PROFESSIONAL },
    }),
  });
  assert.equal(props.length, 0);
});

check("6. ownership collision → blocked", () => {
  const store = createReviewApplyStore();
  const proposal = {
    fieldType: CANONICAL_FIELD_KIND.MOBILE,
    mergeDecision: FIELD_MERGE_ACTION.ACCEPT_NEW_FIELD,
    safetyTier: MERGE_SAFETY_TIER.TIER_1_SAFE,
    identityDecision: IDENTITY_DECISION.ACCEPTED,
    fieldOwnershipDecision: "COLLISION",
    phoneOutcome: PHONE_OUTCOME.OTHER_PERSON_PHONE_COLLISION,
    proposedValue: "+13019754470",
  };
  const elig = evaluatePhoneAutoApplyEligibility(proposal, {});
  assert.equal(elig.eligible, false);
  assert.ok(elig.reasons.includes("OWNERSHIP_CONFLICT"));
});

check("7. approved stale proposal → STALE_PROPOSAL_REQUIRES_REEVALUATION", () => {
  const store = createReviewApplyStore();
  const [p] = generateMergeProposalsFromReachability(store, {
    dryRunRow: makeDryRunRow(),
    reachabilityRow: makeReachRow(),
  });
  approveCanonicalContactMergeProposal(store, p.proposalId);
  const person = store.registry.getByNameOrg("Ada Lovelace", "Example Org");
  store.registry.upsertPerson({
    ...person,
    fields: {
      EMAIL: createCanonicalField({
        value: "other@example.org",
        sourceClass: FIELD_SOURCE_CLASS.HOTEL_VALIDATED,
        canonicalStatus: "CANONICAL",
      }),
    },
    recordVersion: (person.recordVersion || 0) + 1,
  });
  const v = validateProposalForApply(store, p.proposalId);
  assert.equal(v.ok, false);
  assert.equal(v.result, APPLY_RESULT.STALE_PROPOSAL_REQUIRES_REEVALUATION);
});

check("8. stronger canonical before apply → no overwrite", () => {
  const store = createReviewApplyStore();
  store.registry.upsertPerson({
    displayName: "Ada Lovelace",
    organization: "Example Org",
    fields: {
      EMAIL: createCanonicalField({
        value: "official@example.org",
        sourceClass: FIELD_SOURCE_CLASS.HOTEL_VALIDATED,
        canonicalStatus: "CANONICAL",
      }),
    },
    recordVersion: 0,
  });
  const [p] = generateMergeProposalsFromReachability(store, {
    dryRunRow: makeDryRunRow({
      decisions: [
        {
          field: "EMAIL",
          action: FIELD_MERGE_ACTION.ACCEPT_NEW_FIELD,
          safetyTier: MERGE_SAFETY_TIER.TIER_1_SAFE,
          previousValue: "official@example.org",
          proposedValue: "weaker@example.org",
        },
      ],
    }),
    reachabilityRow: makeReachRow(),
  });
  if (p) {
    approveCanonicalContactMergeProposal(store, p.proposalId);
    const r = applyApprovedCanonicalContactMerge(store, p.proposalId);
    assert.equal(r.ok, false);
    assert.equal(r.result, APPLY_RESULT.BLOCKED_STRONGER_CANONICAL);
  }
});

check("9. provenance retained after apply", () => {
  const store = createReviewApplyStore();
  const [p] = generateMergeProposalsFromReachability(store, {
    dryRunRow: makeDryRunRow(),
    reachabilityRow: makeReachRow({ opportunityId: "opp_x" }),
  });
  approveCanonicalContactMergeProposal(store, p.proposalId);
  const r = applyApprovedCanonicalContactMerge(store, p.proposalId);
  assert.equal(r.person.fields.EMAIL.provenance.provider, "surfe");
  assert.equal(r.person.fields.EMAIL.provenance.opportunityId, "opp_x");
  assert.ok(r.person.fields.EMAIL.acceptedAt);
});

check("10. rollback restores prior state", () => {
  const store = createReviewApplyStore();
  const [p] = generateMergeProposalsFromReachability(store, {
    dryRunRow: makeDryRunRow(),
    reachabilityRow: makeReachRow(),
  });
  approveCanonicalContactMergeProposal(store, p.proposalId);
  applyApprovedCanonicalContactMerge(store, p.proposalId);
  const rb = rollbackCanonicalContactMerge(store, p.proposalId, { reason: "test" });
  assert.equal(rb.ok, true);
  const person = store.registry.getByNameOrg("Ada Lovelace", "Example Org");
  assert.equal(person.fields.EMAIL?.value ?? null, null);
});

check("11. rollback preserves audit history", () => {
  const store = createReviewApplyStore();
  const [p] = generateMergeProposalsFromReachability(store, {
    dryRunRow: makeDryRunRow(),
    reachabilityRow: makeReachRow(),
  });
  approveCanonicalContactMergeProposal(store, p.proposalId);
  applyApprovedCanonicalContactMerge(store, p.proposalId);
  const before = store.immutableAudit.length;
  rollbackCanonicalContactMerge(store, p.proposalId);
  assert.ok(store.immutableAudit.length > before);
  assert.ok(store.immutableAudit.some((a) => a.type === "FIELD_APPLIED"));
  assert.ok(store.immutableAudit.some((a) => a.type === "ROLLBACK"));
});

check("12. hotel feedback blocks reuse", () => {
  const store = createReviewApplyStore();
  let person = store.registry.upsertPerson({
    displayName: "Ada Lovelace",
    organization: "Example Org",
    fields: {
      EMAIL: createCanonicalField({ value: "ada@example.org" }),
    },
  });
  person = applyHotelFeedback(person, { state: HOTEL_FEEDBACK_STATE.CONFIRMED_WRONG });
  store.registry.upsertPerson(person);
  const reuse = resolveCanonicalReuseAfterApply(store, {
    name: "Ada Lovelace",
    organization: "Example Org",
    needEmail: true,
  });
  assert.equal(reuse.outcome, null);
});

check("13. applied field reused cross-hotel", () => {
  const store = createReviewApplyStore();
  store.registry.upsertPerson({
    displayName: "Casey Planner",
    organization: "Coastal Association",
    fields: {
      EMAIL: createCanonicalField({
        value: "casey@coastal.org",
        sourceClass: FIELD_SOURCE_CLASS.PROVIDER_ACCEPTED,
        canonicalStatus: "CANONICAL",
        hotelId: "hotel_a",
        provenance: { provider: "surfe", hotelId: "hotel_a" },
      }),
      MOBILE: createCanonicalField({
        kind: CANONICAL_FIELD_KIND.MOBILE,
        value: "3055550199",
        canonicalStatus: "CANONICAL",
        provenance: { provider: "surfe" },
      }),
    },
  });
  store.registry.addRelationship({
    hotelId: "hotel_b",
    opportunityId: "opp_b",
    eventRole: "Retreat planner",
  });
  const reuse = resolveCanonicalReuseAfterApply(store, {
    name: "Casey Planner",
    organization: "Coastal Association",
    needEmail: true,
    needPhone: true,
  });
  assert.equal(reuse.reuseEmail, true);
  assert.equal(reuse.reusePhone, true);
  assert.equal(reuse.provenance.provider, "surfe");
  assert.equal(store.registry.snapshot().relationships[0].hotelId, "hotel_b");
});

check("14. reuse avoids provider call", () => {
  const store = createReviewApplyStore();
  store.registry.upsertPerson({
    displayName: "Casey Planner",
    organization: "Coastal Association",
    fields: {
      MOBILE: createCanonicalField({
        kind: CANONICAL_FIELD_KIND.MOBILE,
        value: "3055550199",
        canonicalStatus: "CANONICAL",
      }),
    },
  });
  const before = store.phonePilotMetrics.providerCallsAvoided;
  const reuse = resolveCanonicalReuseAfterApply(store, {
    name: "Casey Planner",
    organization: "Coastal Association",
    needPhone: true,
  });
  assert.equal(reuse.providerCallsAvoided, 1);
  assert.ok(store.phonePilotMetrics.providerCallsAvoided > before);
});

check("15. event context not merged into person canonical data", () => {
  const store = createReviewApplyStore();
  store.registry.upsertPerson({
    displayName: "Casey Planner",
    organization: "Coastal Association",
    title: "Director of Meetings",
  });
  store.registry.addRelationship({
    hotelId: "hotel_resort",
    opportunityId: "gdi_opp_bay_cup",
    eventRole: "Housing desk only",
    opportunityTitle: "Bay Cup Overflow",
  });
  const p = store.registry.getByNameOrg("Casey Planner", "Coastal Association");
  assert.equal(p.title, "Director of Meetings");
  assert.ok(!JSON.stringify(p).includes("Bay Cup"));
});

check("16. DRY_RUN registry path never sets APPLIED via old merge helper", () => {
  const store = createReviewApplyStore();
  const [p] = generateMergeProposalsFromReachability(store, {
    dryRunRow: makeDryRunRow(),
    reachabilityRow: makeReachRow(),
  });
  assert.equal(p.status, PROPOSAL_STATUS.PENDING_REVIEW);
  assert.notEqual(store.getProposal(p.proposalId).status, PROPOSAL_STATUS.APPLIED);
});

check("17. REVIEW_REQUIRED requires explicit approval", () => {
  const store = createReviewApplyStore();
  const [p] = generateMergeProposalsFromReachability(store, {
    dryRunRow: makeDryRunRow(),
    reachabilityRow: makeReachRow(),
  });
  assert.equal(p.approvalPath, APPROVAL_PATH.REVIEW_REQUIRED);
  rejectCanonicalContactMergeProposal(store, p.proposalId);
  const r = applyApprovedCanonicalContactMerge(store, p.proposalId);
  assert.equal(r.ok, false);
});

check("18. AUTO_ACCEPT_SAFE / global Surfe remain disabled", () => {
  assert.equal(isPaidEnrichmentEnabled(), false);
  assert.equal(process.env.CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED || "0", "0");
});

check("sequential email then mobile same person → both apply", () => {
  const store = createReviewApplyStore();
  const props = generateMergeProposalsFromReachability(store, {
    dryRunRow: makeDryRunRow({
      decisions: [
        {
          field: "EMAIL",
          action: FIELD_MERGE_ACTION.ACCEPT_NEW_FIELD,
          safetyTier: MERGE_SAFETY_TIER.TIER_1_SAFE,
          previousValue: null,
          proposedValue: "ada@example.org",
        },
        {
          field: "MOBILE",
          action: FIELD_MERGE_ACTION.ACCEPT_NEW_FIELD,
          safetyTier: MERGE_SAFETY_TIER.TIER_1_SAFE,
          previousValue: null,
          proposedValue: "+15550199",
        },
      ],
    }),
    reachabilityRow: makeReachRow({
      surfe: { phoneOutcome: PHONE_OUTCOME.NEW_MOBILE_PROFESSIONAL },
    }),
  });
  const email = props.find((p) => p.fieldType === "EMAIL");
  const mobile = props.find((p) => p.fieldType === "MOBILE");
  approveCanonicalContactMergeProposal(store, email.proposalId);
  assert.equal(applyApprovedCanonicalContactMerge(store, email.proposalId).ok, true);
  assert.equal(applyApprovedCanonicalContactMerge(store, mobile.proposalId).ok, true);
});

check("phone ACCEPTED + TIER_1 mobile → AUTO_APPLY_PHONE_PILOT status", () => {
  const store = createReviewApplyStore();
  const props = generateMergeProposalsFromReachability(store, {
    dryRunRow: makeDryRunRow({
      decisions: [
        {
          field: "MOBILE",
          action: FIELD_MERGE_ACTION.ACCEPT_NEW_FIELD,
          safetyTier: MERGE_SAFETY_TIER.TIER_1_SAFE,
          previousValue: null,
          proposedValue: "+15550199",
        },
      ],
    }),
    reachabilityRow: makeReachRow({
      surfe: { phoneOutcome: PHONE_OUTCOME.NEW_MOBILE_PROFESSIONAL },
    }),
  });
  assert.equal(props.length, 1);
  assert.equal(props[0].status, PROPOSAL_STATUS.AUTO_APPLY_PHONE_PILOT);
  const r = applyApprovedCanonicalContactMerge(store, props[0].proposalId);
  assert.equal(r.ok, true);
});

if (failed) {
  console.error(`\n${failed} failing`);
  process.exit(1);
}
console.log("\nAll canonical merge review/apply tests passed.");
