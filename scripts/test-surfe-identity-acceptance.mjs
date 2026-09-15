#!/usr/bin/env node
/**
 * Regression tests — Surfe identity acceptance contract (GDI Phase 2).
 * No network · no Surfe API · no production writes.
 */
import assert from "node:assert/strict";
import {
  IDENTITY_DECISION,
  EMAIL_OUTCOME,
  PHONE_OUTCOME,
  MERGE_SIMULATION,
  acceptSurfeIdentity,
  evaluateEmailLocalPartMatch,
  evaluateLinkedInTokenMatch,
  classifySurfeEmailOutcome,
  classifySurfePhoneOutcome,
  isMeaningfulSurfeImprovement,
  simulateProductionMergeDecision,
} from "../lib/hotel-intelligence/contact-intelligence/surfe-identity-acceptance.js";

let passed = 0;
function ok(name, fn) {
  try {
    fn();
    passed += 1;
    console.log(`PASS ${name}`);
  } catch (err) {
    console.error(`FAIL ${name}: ${err.message}`);
    process.exitCode = 1;
  }
}

ok("Fessler vs Kessler email local-part must reject", () => {
  const local = evaluateEmailLocalPartMatch("jkessler@guidehouse.com", "Justin Fessler");
  assert.equal(local.contradiction, true);
  const id = acceptSurfeIdentity({
    expectedFullName: "Justin Fessler",
    expectedOrganization: "Guidehouse",
    expectedDomain: "guidehouse.com",
    returnedFullName: "Justin Kessler",
    returnedOrganization: "Guidehouse",
    returnedDomain: "guidehouse.com",
    returnedEmail: "jkessler@guidehouse.com",
    returnedLinkedInUrl: "https://www.linkedin.com/in/justin-kessler",
  });
  assert.equal(id.decision, IDENTITY_DECISION.REJECTED);
  assert.ok(id.contradictions.length >= 1);
});

ok("Same name, wrong organization must reject", () => {
  const id = acceptSurfeIdentity({
    expectedFullName: "Jami Sims",
    expectedOrganization: "National Association of REALTORS",
    expectedDomain: "nar.realtor",
    returnedFullName: "Jami Sims",
    returnedOrganization: "Unrelated Corp",
    returnedDomain: "unrelated-corp.example",
    returnedEmail: "jami.sims@unrelated-corp.example",
    returnedLinkedInUrl: "https://www.linkedin.com/in/jami-sims-unrelated",
  });
  assert.equal(id.decision, IDENTITY_DECISION.REJECTED);
});

ok("Exact name + org + matching email local-part may accept", () => {
  const id = acceptSurfeIdentity({
    expectedFullName: "Jami Sims",
    expectedOrganization: "National Association of REALTORS",
    expectedDomain: "nar.realtor",
    returnedFullName: "Jami Sims",
    returnedOrganization: "National Association of REALTORS",
    returnedDomain: "nar.realtor",
    returnedEmail: "jsims@nar.realtor",
    returnedLinkedInUrl: "https://www.linkedin.com/in/jami-sims",
  });
  assert.equal(id.decision, IDENTITY_DECISION.ACCEPTED);
  assert.equal(id.wouldAcceptForMerge, true);
});

ok("Name + domain alone insufficient (ambiguous)", () => {
  const id = acceptSurfeIdentity({
    expectedFullName: "Andrea Snader",
    expectedOrganization: "LogicMonitor",
    expectedDomain: "logicmonitor.com",
    returnedFullName: "Andrea Snader",
    returnedOrganization: "LogicMonitor",
    returnedDomain: "logicmonitor.com",
  });
  assert.equal(id.decision, IDENTITY_DECISION.AMBIGUOUS);
  assert.ok(id.reasons.includes("name_plus_domain_alone_insufficient"));
});

ok("Exact name + org but contradictory LinkedIn slug must reject", () => {
  const li = evaluateLinkedInTokenMatch(
    "https://www.linkedin.com/in/andrea-otherperson",
    "Andrea Snader"
  );
  assert.equal(li.contradiction, true);
  const id = acceptSurfeIdentity({
    expectedFullName: "Andrea Snader",
    expectedOrganization: "LogicMonitor",
    expectedDomain: "logicmonitor.com",
    returnedFullName: "Andrea Snader",
    returnedOrganization: "LogicMonitor",
    returnedDomain: "logicmonitor.com",
    returnedEmail: "asnader@logicmonitor.com",
    returnedLinkedInUrl: "https://www.linkedin.com/in/andrea-otherperson",
  });
  assert.equal(id.decision, IDENTITY_DECISION.REJECTED);
});

ok("Provider email matches official → corroboration only", () => {
  const emailOutcome = classifySurfeEmailOutcome({
    identityDecision: IDENTITY_DECISION.ACCEPTED,
    surfeEmail: "Jessica.Mitchell@nih.gov",
    baselineEmail: "Jessica.Mitchell@nih.gov",
    baselineEmailType: "DIRECT_WORK",
    baselineEmailVerification: "OFFICIAL_SOURCE_VERIFIED",
  });
  assert.equal(emailOutcome, EMAIL_OUTCOME.CORROBORATES_OFFICIAL_EMAIL);
  const merge = simulateProductionMergeDecision({
    identityDecision: IDENTITY_DECISION.ACCEPTED,
    emailOutcome,
    phoneOutcome: PHONE_OUTCOME.NOT_FOUND,
    baselineEmailVerification: "OFFICIAL_SOURCE_VERIFIED",
  });
  assert.equal(merge, MERGE_SIMULATION.WOULD_ACCEPT_AS_CORROBORATION_ONLY);
});

ok("New direct email for accepted identity = meaningful improvement", () => {
  const emailOutcome = classifySurfeEmailOutcome({
    identityDecision: IDENTITY_DECISION.ACCEPTED,
    surfeEmail: "jsims@nar.realtor",
    baselineEmail: "GADInst@nar.realtor",
    baselineEmailType: "ROLE_BASED",
    baselineEmailVerification: "OFFICIAL_SOURCE_VERIFIED",
  });
  assert.equal(emailOutcome, EMAIL_OUTCOME.NEW_DIRECT_WORK_EMAIL);
  assert.equal(
    isMeaningfulSurfeImprovement({
      emailOutcome,
      phoneOutcome: PHONE_OUTCOME.NOT_FOUND,
    }),
    true
  );
});

ok("Same main office phone = no meaningful improvement", () => {
  const phoneOutcome = classifySurfePhoneOutcome({
    identityDecision: IDENTITY_DECISION.ACCEPTED,
    surfePhone: "571-323-2587",
    baselinePhone: "571-323-2587",
    baselinePhoneType: "MAIN_ORGANIZATION",
    providerPhoneField: "mobilePhones",
  });
  assert.equal(phoneOutcome, PHONE_OUTCOME.SAME_MAIN_LINE);
  assert.equal(
    isMeaningfulSurfeImprovement({
      emailOutcome: EMAIL_OUTCOME.NOT_FOUND,
      phoneOutcome,
    }),
    false
  );
});

ok("Danielle Santos → Prebil NIST line = OTHER_PERSON_PHONE_COLLISION (no improvement)", () => {
  const phoneOutcome = classifySurfePhoneOutcome({
    identityDecision: IDENTITY_DECISION.ACCEPTED,
    surfePhone: "301-975-4470",
    baselinePhone: null,
    knownOtherPersonPhones: [
      { name: "Michael Prebil", phone: "301-975-4470", org: "NIST" },
    ],
    providerPhoneField: "mobilePhones",
  });
  assert.equal(phoneOutcome, PHONE_OUTCOME.OTHER_PERSON_PHONE_COLLISION);
  assert.equal(
    isMeaningfulSurfeImprovement({
      emailOutcome: EMAIL_OUTCOME.NOT_FOUND,
      phoneOutcome,
    }),
    false
  );
});

ok("New mobile for accepted identity = meaningful improvement", () => {
  const phoneOutcome = classifySurfePhoneOutcome({
    identityDecision: IDENTITY_DECISION.ACCEPTED,
    surfePhone: "+1-301-555-0199",
    baselinePhone: null,
    baselinePhoneType: "UNKNOWN",
    providerPhoneField: "mobilePhones",
  });
  assert.equal(phoneOutcome, PHONE_OUTCOME.NEW_MOBILE_PROFESSIONAL);
  assert.equal(
    isMeaningfulSurfeImprovement({
      emailOutcome: EMAIL_OUTCOME.CORROBORATES_OFFICIAL_EMAIL,
      phoneOutcome,
    }),
    true
  );
});

ok("Surfe conflicts with official source → official wins / reject merge", () => {
  const emailOutcome = classifySurfeEmailOutcome({
    identityDecision: IDENTITY_DECISION.ACCEPTED,
    surfeEmail: "info@afceabethesda.org",
    baselineEmail: "registrar@afceabethesda.org",
    baselineEmailType: "ROLE_BASED",
    baselineEmailVerification: "OFFICIAL_SOURCE_VERIFIED",
  });
  // generic/role new email vs official role — NEW_ROLE_BASED (not overwrite) or CONFLICTS
  assert.ok(
    emailOutcome === EMAIL_OUTCOME.NEW_ROLE_BASED_EMAIL ||
      emailOutcome === EMAIL_OUTCOME.CONFLICTS_WITH_OFFICIAL
  );
  const conflict = classifySurfeEmailOutcome({
    identityDecision: IDENTITY_DECISION.ACCEPTED,
    surfeEmail: "info@other.org",
    baselineEmail: "Jessica.Mitchell@nih.gov",
    baselineEmailType: "DIRECT_WORK",
    baselineEmailVerification: "OFFICIAL_SOURCE_VERIFIED",
  });
  // different generic vs official direct → CONFLICTS when generic
  assert.equal(conflict, EMAIL_OUTCOME.CONFLICTS_WITH_OFFICIAL);
  const merge = simulateProductionMergeDecision({
    identityDecision: IDENTITY_DECISION.ACCEPTED,
    emailOutcome: conflict,
    phoneOutcome: PHONE_OUTCOME.NOT_FOUND,
    baselineEmailVerification: "OFFICIAL_SOURCE_VERIFIED",
  });
  assert.equal(merge, MERGE_SIMULATION.WOULD_REJECT);
});

ok("Rejected identity must not salvage fields", () => {
  assert.equal(
    classifySurfeEmailOutcome({
      identityDecision: IDENTITY_DECISION.REJECTED,
      surfeEmail: "jkessler@guidehouse.com",
      baselineEmail: "registrar@afceabethesda.org",
      baselineEmailType: "ROLE_BASED",
      baselineEmailVerification: "OFFICIAL_SOURCE_VERIFIED",
    }),
    EMAIL_OUTCOME.IDENTITY_REJECTED
  );
  assert.equal(
    classifySurfePhoneOutcome({
      identityDecision: IDENTITY_DECISION.REJECTED,
      surfePhone: "+1-202-555-0100",
      baselinePhone: null,
      baselinePhoneType: "UNKNOWN",
    }),
    PHONE_OUTCOME.IDENTITY_REJECTED
  );
});

console.log(`\n${passed} Surfe identity acceptance tests passed`);
if (process.exitCode) process.exit(process.exitCode);
