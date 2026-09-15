#!/usr/bin/env node
/**
 * Regressions — canonical contact merge policy + cross-hotel reuse.
 * No network · no Surfe · no production writes.
 */
import assert from "node:assert/strict";
import {
  IDENTITY_DECISION,
  EMAIL_OUTCOME,
  PHONE_OUTCOME,
} from "../lib/hotel-intelligence/contact-intelligence/surfe-identity-acceptance.js";
import {
  evaluateContactFieldMerge,
  createCanonicalField,
  createCanonicalPerson,
  createCanonicalPersonRegistry,
  applyHotelFeedback,
  FIELD_SOURCE_CLASS,
  FIELD_MERGE_ACTION,
  MERGE_WRITE_MODE,
  MERGE_SAFETY_TIER,
  CANONICAL_FIELD_KIND,
  HOTEL_FEEDBACK_STATE,
  REUSE_OUTCOME,
  simulateReachabilityRowMerge,
} from "../lib/hotel-intelligence/contact-intelligence/canonical-merge-policy.js";

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

const acceptedId = IDENTITY_DECISION.ACCEPTED;

check("1. accepted missing email → ACCEPT_NEW_FIELD", () => {
  const d = evaluateContactFieldMerge({
    canonicalPerson: createCanonicalPerson({ displayName: "Ada", organization: "X" }),
    canonicalField: null,
    incomingField: createCanonicalField({
      kind: CANONICAL_FIELD_KIND.EMAIL,
      value: "ada@x.org",
      sourceClass: FIELD_SOURCE_CLASS.PROVIDER_ACCEPTED,
    }),
    identityDecision: acceptedId,
    fieldOutcome: EMAIL_OUTCOME.NEW_DIRECT_WORK_EMAIL,
  });
  assert.equal(d.action, FIELD_MERGE_ACTION.ACCEPT_NEW_FIELD);
  assert.equal(d.safetyTier, MERGE_SAFETY_TIER.TIER_1_SAFE);
});

check("2. accepted mobile → ACCEPT_NEW_FIELD", () => {
  const d = evaluateContactFieldMerge({
    canonicalPerson: createCanonicalPerson({ displayName: "Ada", organization: "X" }),
    canonicalField: null,
    incomingField: createCanonicalField({
      kind: CANONICAL_FIELD_KIND.MOBILE,
      value: "+15551212",
      sourceClass: FIELD_SOURCE_CLASS.PROVIDER_ACCEPTED,
    }),
    identityDecision: acceptedId,
    fieldOutcome: PHONE_OUTCOME.NEW_MOBILE_PROFESSIONAL,
  });
  assert.equal(d.action, FIELD_MERGE_ACTION.ACCEPT_NEW_FIELD);
});

check("3. provider same value → CORROBORATE_EXISTING", () => {
  const d = evaluateContactFieldMerge({
    canonicalPerson: createCanonicalPerson({ displayName: "Ada", organization: "X" }),
    canonicalField: createCanonicalField({
      kind: CANONICAL_FIELD_KIND.EMAIL,
      value: "ada@x.org",
      sourceClass: FIELD_SOURCE_CLASS.OFFICIAL_DIRECT,
    }),
    incomingField: createCanonicalField({
      kind: CANONICAL_FIELD_KIND.EMAIL,
      value: "ada@x.org",
      sourceClass: FIELD_SOURCE_CLASS.PROVIDER_ACCEPTED,
    }),
    identityDecision: acceptedId,
    fieldOutcome: EMAIL_OUTCOME.CORROBORATES_OFFICIAL_EMAIL,
  });
  assert.equal(d.action, FIELD_MERGE_ACTION.CORROBORATE_EXISTING);
});

check("4. provider weaker than official → NO_INCREMENTAL_VALUE", () => {
  const d = evaluateContactFieldMerge({
    canonicalPerson: createCanonicalPerson({ displayName: "Ada", organization: "X" }),
    canonicalField: createCanonicalField({
      kind: CANONICAL_FIELD_KIND.EMAIL,
      value: "ada@x.org",
      sourceClass: FIELD_SOURCE_CLASS.OFFICIAL_DIRECT,
    }),
    incomingField: createCanonicalField({
      kind: CANONICAL_FIELD_KIND.EMAIL,
      value: "other@x.org",
      sourceClass: FIELD_SOURCE_CLASS.PROVIDER_ACCEPTED,
    }),
    identityDecision: acceptedId,
    fieldOutcome: EMAIL_OUTCOME.DIFFERENT_BUT_PLAUSIBLE,
  });
  assert.equal(d.action, FIELD_MERGE_ACTION.NO_INCREMENTAL_VALUE);
  assert.equal(d.reasonCode, "WEAKER_THAN_CANONICAL");
});

check("5. role email → trusted direct → REPLACE_WEAKER_FIELD", () => {
  const d = evaluateContactFieldMerge({
    canonicalPerson: createCanonicalPerson({ displayName: "Ada", organization: "X" }),
    canonicalField: createCanonicalField({
      kind: CANONICAL_FIELD_KIND.ROLE_EMAIL,
      value: "events@x.org",
      sourceClass: FIELD_SOURCE_CLASS.OFFICIAL_FUNCTIONAL,
    }),
    incomingField: createCanonicalField({
      kind: CANONICAL_FIELD_KIND.EMAIL,
      value: "ada@x.org",
      sourceClass: FIELD_SOURCE_CLASS.PROVIDER_VERIFIED,
    }),
    identityDecision: acceptedId,
    fieldOutcome: EMAIL_OUTCOME.NEW_DIRECT_WORK_EMAIL,
  });
  assert.equal(d.action, FIELD_MERGE_ACTION.REPLACE_WEAKER_FIELD);
  assert.equal(d.safetyTier, MERGE_SAFETY_TIER.TIER_2_REVIEW);
});

check("6. ambiguous identity → BLOCK", () => {
  const d = evaluateContactFieldMerge({
    canonicalPerson: createCanonicalPerson({ displayName: "Brad", organization: "Club" }),
    canonicalField: null,
    incomingField: createCanonicalField({
      kind: CANONICAL_FIELD_KIND.MOBILE,
      value: "+1",
      sourceClass: FIELD_SOURCE_CLASS.PROVIDER_ACCEPTED,
    }),
    identityDecision: IDENTITY_DECISION.AMBIGUOUS,
    fieldOutcome: PHONE_OUTCOME.IDENTITY_REJECTED,
  });
  assert.equal(d.action, FIELD_MERGE_ACTION.REJECT_FIELD);
  assert.equal(d.safetyTier, MERGE_SAFETY_TIER.TIER_3_BLOCK);
});

check("7. field ownership conflict → BLOCK", () => {
  const d = evaluateContactFieldMerge({
    canonicalPerson: createCanonicalPerson({ displayName: "Danielle", organization: "NIST" }),
    canonicalField: null,
    incomingField: createCanonicalField({
      kind: CANONICAL_FIELD_KIND.PHONE,
      value: "3019754470",
      sourceClass: FIELD_SOURCE_CLASS.PROVIDER_ACCEPTED,
    }),
    identityDecision: acceptedId,
    fieldOutcome: PHONE_OUTCOME.OTHER_PERSON_PHONE_COLLISION,
    ownershipDecision: "COLLISION",
  });
  assert.equal(d.action, FIELD_MERGE_ACTION.REJECT_FIELD);
  assert.equal(d.reasonCode, "FIELD_OWNERSHIP_CONFLICT");
});

check("8. former employee → BLOCK", () => {
  const d = evaluateContactFieldMerge({
    canonicalPerson: createCanonicalPerson({
      displayName: "Ex",
      organization: "Y",
      formerAffiliation: true,
    }),
    canonicalField: null,
    incomingField: createCanonicalField({
      kind: CANONICAL_FIELD_KIND.EMAIL,
      value: "ex@y.org",
      sourceClass: FIELD_SOURCE_CLASS.PROVIDER_ACCEPTED,
    }),
    identityDecision: acceptedId,
  });
  assert.equal(d.action, FIELD_MERGE_ACTION.REJECT_FIELD);
  assert.equal(d.reasonCode, "FORMER_EMPLOYEE");
});

check("9. canonical contact reused across opportunities", () => {
  const reg = createCanonicalPersonRegistry();
  reg.upsertPerson({
    displayName: "Casey Planner",
    organization: "Coastal Association",
    fields: {
      EMAIL: createCanonicalField({
        kind: CANONICAL_FIELD_KIND.EMAIL,
        value: "casey@coastal.org",
        sourceClass: FIELD_SOURCE_CLASS.PROVIDER_ACCEPTED,
        canonicalStatus: "CANONICAL",
      }),
    },
  });
  reg.addRelationship({
    hotelId: "hotel_a",
    opportunityId: "opp_a",
    personId: "p1",
    eventRole: "Director of Meetings",
  });
  reg.addRelationship({
    hotelId: "hotel_a",
    opportunityId: "opp_b",
    personId: "p1",
    eventRole: "Annual chair",
  });
  const reuse = reg.resolveReuse({
    name: "Casey Planner",
    organization: "Coastal Association",
    needEmail: true,
    needPhone: false,
  });
  assert.equal(reuse.outcome, REUSE_OUTCOME.CONTACT_REUSED_FROM_CANONICAL);
  assert.equal(reuse.requestEmail, false);
  assert.equal(reg.snapshot().relationships.length, 2);
});

check("10. canonical contact reused across hotels", () => {
  const reg = createCanonicalPersonRegistry();
  reg.upsertPerson({
    displayName: "Casey Planner",
    organization: "Coastal Association",
    fields: {
      EMAIL: createCanonicalField({
        value: "casey@coastal.org",
        sourceClass: FIELD_SOURCE_CLASS.PROVIDER_ACCEPTED,
      }),
      MOBILE: createCanonicalField({
        kind: CANONICAL_FIELD_KIND.MOBILE,
        value: "3055550100",
        sourceClass: FIELD_SOURCE_CLASS.PROVIDER_ACCEPTED,
      }),
    },
  });
  reg.addRelationship({ hotelId: "hotel_miami", opportunityId: "m1", eventRole: "Meetings" });
  const reuse = reg.resolveReuse({
    name: "Casey Planner",
    organization: "Coastal Association",
    needEmail: true,
    needPhone: true,
  });
  assert.equal(reuse.reuseEmail, true);
  assert.equal(reuse.reusePhone, true);
  reg.addRelationship({
    hotelId: "hotel_tampa",
    opportunityId: "t1",
    eventRole: "Overflow planner",
  });
  const snap = reg.snapshot();
  assert.equal(snap.people.length, 1);
  assert.equal(snap.relationships.map((r) => r.hotelId).sort().join(","), "hotel_miami,hotel_tampa");
});

check("11. reuse prevents paid provider call", () => {
  const reg = createCanonicalPersonRegistry();
  reg.upsertPerson({
    displayName: "Casey Planner",
    organization: "Coastal Association",
    fields: {
      EMAIL: createCanonicalField({ value: "casey@coastal.org" }),
      MOBILE: createCanonicalField({
        kind: CANONICAL_FIELD_KIND.MOBILE,
        value: "3055550100",
      }),
    },
  });
  const before = reg.providerCallsAvoided;
  const reuse = reg.resolveReuse({
    name: "Casey Planner",
    organization: "Coastal Association",
    needEmail: true,
    needPhone: true,
  });
  assert.equal(reuse.providerCallsAvoided, 2);
  assert.equal(reg.providerCallsAvoided, before + 2);
  assert.equal(reuse.requestEmail, false);
  assert.equal(reuse.requestMobile, false);
});

check("12. event relationship stays opportunity-specific", () => {
  const reg = createCanonicalPersonRegistry();
  const person = reg.upsertPerson({
    displayName: "Casey Planner",
    organization: "Coastal Association",
    title: "Director of Meetings",
  });
  reg.addRelationship({
    hotelId: "h1",
    opportunityId: "opp1",
    personId: person.personId,
    eventRole: "2027 Annual Meetings lead",
    whyThisPerson: "Named on event page",
  });
  const p = reg.getByNameOrg("Casey Planner", "Coastal Association");
  assert.equal(p.title, "Director of Meetings");
  assert.ok(!("eventRole" in p) || p.eventRole == null);
  assert.equal(reg.snapshot().relationships[0].eventRole, "2027 Annual Meetings lead");
});

check("13. hotel-specific opportunity facts do not alter canonical person globally", () => {
  const reg = createCanonicalPersonRegistry();
  reg.upsertPerson({
    displayName: "Casey Planner",
    organization: "Coastal Association",
    fields: {
      EMAIL: createCanonicalField({
        value: "casey@coastal.org",
        sourceClass: FIELD_SOURCE_CLASS.OFFICIAL_DIRECT,
      }),
    },
  });
  reg.addRelationship({
    hotelId: "recFakeResort999",
    opportunityId: "gdi_opp_resort_cup",
    eventRole: "Housing desk",
    opportunityTitle: "Bay Cup Overflow",
  });
  const p = reg.getByNameOrg("Casey Planner", "Coastal Association");
  assert.equal(p.fields.EMAIL.value, "casey@coastal.org");
  assert.equal(p.fields.EMAIL.sourceClass, FIELD_SOURCE_CLASS.OFFICIAL_DIRECT);
  assert.ok(!String(JSON.stringify(p)).includes("Bay Cup"));
});

check("14. hotel-confirmed wrong field prevents reuse", () => {
  let person = createCanonicalPerson({
    displayName: "Casey Planner",
    organization: "Coastal Association",
    fields: {
      EMAIL: createCanonicalField({ value: "casey@coastal.org" }),
    },
  });
  person = applyHotelFeedback(person, {
    state: HOTEL_FEEDBACK_STATE.CONFIRMED_WRONG,
    field: "EMAIL",
  });
  const reg = createCanonicalPersonRegistry();
  reg.upsertPerson(person);
  const reuse = reg.resolveReuse({
    name: "Casey Planner",
    organization: "Coastal Association",
    needEmail: true,
  });
  assert.equal(reuse.outcome, null);
  assert.equal(reuse.requestEmail, true);
  const d = evaluateContactFieldMerge({
    canonicalPerson: person,
    canonicalField: null,
    incomingField: createCanonicalField({ value: "new@coastal.org" }),
    identityDecision: acceptedId,
  });
  assert.equal(d.action, FIELD_MERGE_ACTION.REJECT_FIELD);
});

check("15. merge dry-run does not mutate production status", () => {
  const reg = createCanonicalPersonRegistry();
  const row = {
    name: "Amy Example",
    organization: "Example Org",
    role: "Director",
    opportunityId: "opp_x",
    opportunityTitle: "Example Event",
    whoConfidence: "HIGH",
    before: { email: null, phone: null, grade: "C" },
    after: { acceptedEmail: "amy@example.org", acceptedPhone: null },
    identity: { decision: IDENTITY_DECISION.ACCEPTED },
    surfe: {
      email: "amy@example.org",
      emailOutcome: EMAIL_OUTCOME.NEW_DIRECT_WORK_EMAIL,
      phone: null,
      phoneOutcome: PHONE_OUTCOME.NOT_FOUND,
    },
    merge: { email: "ACCEPT_NEW_FIELD", phone: "REJECT_FIELD" },
  };
  const { decisions } = simulateReachabilityRowMerge(reg, row, {
    writeMode: MERGE_WRITE_MODE.DRY_RUN,
    hotelId: "hotel_synth",
  });
  assert.ok(decisions.some((d) => d.action === FIELD_MERGE_ACTION.ACCEPT_NEW_FIELD));
  const snap = reg.snapshot();
  assert.ok(snap.auditLog.every((a) => a.mutated === false));
  assert.ok(
    snap.people[0].fields.EMAIL.canonicalStatus === "PROPOSED_DRY_RUN" ||
      snap.people[0].fields.EMAIL.canonicalStatus === "PROPOSED"
  );
  assert.ok(snap.auditLog.every((a) => a.writeMode === MERGE_WRITE_MODE.DRY_RUN));
});

if (failed) {
  console.error(`\n${failed} failing`);
  process.exit(1);
}
console.log("\nAll canonical merge policy tests passed.");
