#!/usr/bin/env node
/**
 * Reusable regressions — GDI contact coverage, reachability eligibility, portability.
 */
import assert from "node:assert/strict";
import {
  classifyReachabilityNeed,
  calculateGdiContactCoverage,
  buildReachabilityCohortFromQueue,
  mapOutcomeToFieldMerge,
  simulateGradeAfterAcceptedFields,
  REACHABILITY_NEED,
  FIELD_MERGE_DECISION,
} from "../lib/group-demand-intelligence/contact-coverage.js";
import {
  PRIMARY_KIND,
  isSurfeEligiblePerson,
} from "../lib/group-demand-intelligence/contact-candidate/index.js";
import {
  IDENTITY_DECISION,
  acceptSurfeIdentity,
  classifySurfePhoneOutcome,
  PHONE_OUTCOME,
} from "../lib/hotel-intelligence/contact-intelligence/surfe-identity-acceptance.js";
import { scoreContactCandidate } from "../lib/group-demand-intelligence/contact-candidate/index.js";

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

check("1. known person + missing phone → eligible PHONE_ONLY", () => {
  const need = classifyReachabilityNeed({
    name: "Brad Roos",
    email: "brad@example.org",
    phone: null,
    candidateConfidence: "HIGH",
    primaryKind: PRIMARY_KIND.NAMED_PERSON,
    employmentStatus: "CURRENT_CONFIRMED",
    eventRelationship: "CURRENT_EVENT_CONTACT",
  });
  assert.equal(need, REACHABILITY_NEED.PHONE_ONLY);
});

check("2. Grade A full contact → NO_ENRICHMENT_NEEDED", () => {
  const need = classifyReachabilityNeed({
    name: "Jami Sims",
    email: "a@b.com",
    phone: "202-555-0100",
    candidateConfidence: "HIGH",
    primaryKind: PRIMARY_KIND.NAMED_PERSON,
    employmentStatus: "CURRENT_CONFIRMED",
    eventRelationship: "CURRENT_EVENT_CONTACT",
  });
  assert.equal(need, REACHABILITY_NEED.NO_ENRICHMENT_NEEDED);
});

check("3. LinkedIn-only / org-role-only → not Surfe eligible", () => {
  assert.equal(
    isSurfeEligiblePerson(
      {
        name: "Li Only",
        candidateConfidence: "MEDIUM",
        candidateScore: 70,
        eventRelationship: "ORGANIZATION_ROLE_ONLY",
        employmentStatus: "CURRENT_PROBABLE",
      },
      PRIMARY_KIND.NAMED_PERSON
    ),
    false
  );
});

check("4. functional entity → NOT_ELIGIBLE for person enrichment", () => {
  const need = classifyReachabilityNeed({
    name: "HBC Event Services",
    email: "support@hbc.example",
    phone: "505-555-0100",
    candidateConfidence: "HIGH",
    primaryKind: PRIMARY_KIND.FUNCTIONAL_ENTITY,
    functionalEntity: true,
  });
  assert.equal(need, REACHABILITY_NEED.NOT_ELIGIBLE);
});

check("5. Fessler→Kessler style surname mismatch → REJECTED", () => {
  const id = acceptSurfeIdentity({
    expectedFullName: "Justin Fessler",
    expectedOrganization: "Guidehouse",
    expectedDomain: "guidehouse.com",
    returnedFullName: "Justin Kessler",
    returnedOrganization: "Guidehouse",
    returnedDomain: "guidehouse.com",
    returnedEmail: "justin.kessler@guidehouse.com",
  });
  assert.equal(id.decision, IDENTITY_DECISION.REJECTED);
});

check("6. Danielle→Prebil phone collision → OTHER_PERSON_PHONE_COLLISION", () => {
  const outcome = classifySurfePhoneOutcome({
    identityDecision: IDENTITY_DECISION.ACCEPTED,
    surfePhone: "301-975-4470",
    baselinePhone: null,
    baselinePhoneType: null,
    knownOtherPersonPhones: [
      { name: "Michael Prebil", phone: "301-975-4470", org: "NIST" },
    ],
    providerPhoneField: "mobilePhones",
  });
  assert.equal(outcome, PHONE_OUTCOME.OTHER_PERSON_PHONE_COLLISION);
  assert.equal(
    mapOutcomeToFieldMerge({ outcome }),
    FIELD_MERGE_DECISION.HOLD_FOR_REVIEW
  );
});

check("7. role inbox → accepted direct email → ACCEPT_NEW_FIELD", () => {
  assert.equal(
    mapOutcomeToFieldMerge({ outcome: "NEW_DIRECT_WORK_EMAIL" }),
    FIELD_MERGE_DECISION.ACCEPT_NEW_FIELD
  );
});

check("8. same main line → no improvement", () => {
  assert.equal(
    mapOutcomeToFieldMerge({ outcome: "SAME_MAIN_LINE" }),
    FIELD_MERGE_DECISION.NO_INCREMENTAL_VALUE
  );
});

check("9. official email vs weaker provider → corroborate / no overwrite", () => {
  assert.equal(
    mapOutcomeToFieldMerge({ outcome: "CORROBORATES_OFFICIAL_EMAIL" }),
    FIELD_MERGE_DECISION.CORROBORATE_EXISTING
  );
  assert.equal(
    mapOutcomeToFieldMerge({ outcome: "CONFLICTS_WITH_OFFICIAL" }),
    FIELD_MERGE_DECISION.HOLD_FOR_REVIEW
  );
});

check("10. named person + accepted mobile may improve grade", () => {
  const after = simulateGradeAfterAcceptedFields({
    beforeGrade: "B",
    hasNamedPerson: true,
    acceptedDirectEmail: false,
    acceptedUsefulPhone: true,
    hadOfficialEmail: true,
  });
  assert.equal(after, "A");
});

check("11. hotel-specific event facts do not alter core scoring (hints only)", () => {
  const base = {
    name: "Casey Planner",
    role: "Director of Meetings",
    organization: "Coastal Association",
    claimKind: "FACT",
    sourceUrl: "https://coastal.example/staff",
    eventSpecificEvidence: true,
  };
  const miami = scoreContactCandidate(base, {
    title: "Coastal Annual",
    opportunityType: "PRIMARY_PURSUIT",
    segment: "Associations",
    organizationName: "Coastal Association",
    geographyHints: ["miami"],
  });
  const austin = scoreContactCandidate(base, {
    title: "Coastal Annual",
    opportunityType: "PRIMARY_PURSUIT",
    segment: "Associations",
    organizationName: "Coastal Association",
    geographyHints: ["austin"],
  });
  // Same role/evidence — scores close; geography component may differ but core role score identical
  assert.equal(miami.breakdown.roleRelevance, austin.breakdown.roleRelevance);
  assert.ok(Math.abs(miami.candidateScore - austin.candidateScore) <= 5);
});

check("12. synthetic non-Bethesda hotel coverage works", () => {
  const cov = calculateGdiContactCoverage({
    opportunities: [
      {
        id: "a",
        priority: "MEDIUM_PRIORITY",
        primaryContact: { name: "Ada Lovelace", email: "ada@x.org" },
      },
      {
        id: "b",
        priority: "HIGH_PRIORITY",
        opportunityType: "OVERFLOW_HOUSING",
        primaryContact: {
          name: "Harbor Housing",
          email: "h@h.org",
          targetRoleMatch: "HOUSING_SOURCING_CONTACT",
        },
      },
      { id: "c", priority: "WATCHLIST", primaryContact: null },
    ],
    discoveryRows: [
      {
        opportunityId: "a",
        primaryKind: PRIMARY_KIND.NAMED_PERSON,
        primaryCandidate: {
          name: "Ada Lovelace",
          email: "ada@x.org",
          candidateConfidence: "HIGH",
        },
      },
      {
        opportunityId: "b",
        primaryKind: PRIMARY_KIND.FUNCTIONAL_ENTITY,
        primaryCandidate: {
          name: "Harbor Housing",
          email: "h@h.org",
          functionalEntity: true,
          candidateConfidence: "HIGH",
        },
      },
      { opportunityId: "c", primaryKind: PRIMARY_KIND.UNRESOLVED, primaryCandidate: null },
    ],
  });
  assert.equal(cov.totalOpportunities, 3);
  assert.equal(cov.namedPersonPrimaries, 1);
  assert.equal(cov.functionalEntityPrimaries, 1);
  assert.equal(cov.unresolved, 1);
  assert.equal(cov.funnel.qualifiedOpportunities, 3);
});

check("cohort builder dedupes same person+org", () => {
  const cohort = buildReachabilityCohortFromQueue([
    {
      opportunityId: "o1",
      title: "Event 1",
      name: "Jamie McCormick",
      organization: "NADO",
      email: "jmccormick@nado.org",
      confidence: "HIGH",
      score: 89,
      gap: "PHONE_GAP",
      nextAction: "SURFE_PHONE",
    },
    {
      opportunityId: "o2",
      title: "Event 2",
      name: "Jamie McCormick",
      organization: "NADO",
      email: "jmccormick@nado.org",
      confidence: "HIGH",
      score: 89,
      gap: "PHONE_GAP",
      nextAction: "SURFE_PHONE",
    },
  ]);
  assert.equal(cohort.subjects.length, 1);
  assert.deepEqual(cohort.subjects[0].opportunityIds.sort(), ["o1", "o2"]);
  assert.equal(cohort.subjects[0].requestMobile, true);
  assert.equal(cohort.subjects[0].requestEmail, false);
});

check("official sourceUrl host becomes enrichment domain (reusable)", () => {
  const cohort = buildReachabilityCohortFromQueue([
    {
      opportunityId: "o9",
      title: "Advocacy Conf",
      name: "Amy Example",
      organization: "Example Org",
      confidence: "HIGH",
      score: 72,
      gap: "BOTH_MISSING",
      nextAction: "SURFE_BOTH",
      sourceUrl: "https://ndss.org/meet-our-staff",
    },
  ]);
  assert.equal(cohort.subjects.length, 1);
  assert.equal(cohort.subjects[0].enrichmentDomain, "ndss.org");
  assert.equal(cohort.subjects[0].requestEmail, true);
  assert.equal(cohort.subjects[0].requestMobile, true);
});

if (failed) {
  console.error(`\n${failed} failing`);
  process.exit(1);
}
console.log("\nAll contact coverage/portability tests passed.");
