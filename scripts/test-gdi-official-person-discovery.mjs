#!/usr/bin/env node
/**
 * Regression tests — GDI official-source person discovery gates.
 */

import assert from "node:assert/strict";
import {
  applyPersonDiscoveryGates,
  EMPLOYMENT_STATUS,
  EVENT_RELATIONSHIP,
  scoreContactCandidate,
  annotateCandidate,
  selectRankedCandidates,
  discoverContactCandidates,
  NO_PROBABLE,
  PRIMARY_KIND,
} from "../lib/group-demand-intelligence/contact-candidate/index.js";

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

check("historical contact still employed → boosted", () => {
  const opp = {
    id: "t_hist",
    title: "WashCon 2028",
    opportunityType: "OVERFLOW_HOUSING",
    segment: "Associations",
    organizationName: "NADO",
  };
  const gated = applyPersonDiscoveryGates(
    {
      name: "Jamie Current",
      role: "Events Manager",
      organization: "NADO",
      email: "jamie@nado.org",
      claimKind: "FACT",
      sourceUrl: "https://www.nado.org/2026washcon/",
      employmentStatus: EMPLOYMENT_STATUS.CURRENT_PROBABLE,
      eventRelationship: EVENT_RELATIONSHIP.HISTORICAL_EVENT_CONTACT,
      relationshipToEvent: "Hotel contact on prior WashCon page",
    },
    opp
  );
  assert.equal(gated.reactivationBoost, true);
  assert.equal(gated.stillEmployed, true);
  const scored = scoreContactCandidate(gated, opp);
  assert.ok(scored.candidateScore >= 55, `score ${scored.candidateScore}`);
});

check("historical contact former employee → rejected", () => {
  const gated = applyPersonDiscoveryGates({
    name: "Former Planner",
    role: "Director of Meetings",
    organization: "Example",
    employmentStatus: EMPLOYMENT_STATUS.FORMER_EMPLOYEE,
    eventRelationship: EVENT_RELATIONSHIP.HISTORICAL_EVENT_CONTACT,
    sourceUrl: "https://example.org/old",
  });
  assert.equal(gated.rejected, true);
  const opp = {
    id: "t",
    title: "Meeting",
    opportunityType: "PRIMARY_PURSUIT",
    segment: "Associations",
    organizationName: "Example",
  };
  const a = annotateCandidate(gated, opp);
  const { primary } = selectRankedCandidates([a], opp);
  assert.equal(primary, null);
});

check("successor in same meetings role → acceptable", () => {
  const opp = {
    id: "t_succ",
    title: "Annual Meeting 2027",
    opportunityType: "PRIMARY_PURSUIT",
    segment: "Associations",
    organizationName: "Example",
  };
  const gated = applyPersonDiscoveryGates({
    name: "Sam Successor",
    role: "Director of Meetings",
    organization: "Example",
    claimKind: "FACT",
    sourceUrl: "https://example.org/staff",
    employmentStatus: EMPLOYMENT_STATUS.CURRENT_CONFIRMED,
    eventRelationship: EVENT_RELATIONSHIP.CURRENT_SUCCESSOR_ROLE,
    relationshipToEvent: "Successor to prior meetings director",
  });
  assert.equal(gated.eventSpecificEvidence, true);
  const a = annotateCandidate(gated, opp);
  const { primary } = selectRankedCandidates([a], opp);
  assert.equal(primary?.name, "Sam Successor");
});

check("generic inbox → not a named person primary", () => {
  const opp = {
    id: "t_gen",
    title: "Mystery 2027",
    opportunityType: "PRIMARY_PURSUIT",
    segment: "Medical",
    organizationName: "Mystery",
    primaryContact: {
      name: null,
      email: "info@mystery.org",
      role: "General inbox",
    },
  };
  const result = discoverContactCandidates(opp, {
    seeds: { t_gen: [] },
    personDiscoveries: { t_gen: [] },
  });
  assert.equal(result.resolution, NO_PROBABLE);
  assert.equal(result.primaryKind, PRIMARY_KIND.UNRESOLVED);
});

check("meetings manager beats executive director", () => {
  const opp = {
    id: "t_ed",
    title: "Assoc Annual 2027",
    opportunityType: "PRIMARY_PURSUIT",
    segment: "Associations",
    organizationName: "Assoc",
  };
  const ed = annotateCandidate(
    {
      name: "Pat Executive",
      role: "Executive Director",
      organization: "Assoc",
      email: "pat@assoc.org",
      phone: "202-555-0100",
      claimKind: "FACT",
      sourceUrl: "https://assoc.org/staff",
    },
    opp
  );
  const mm = annotateCandidate(
    {
      name: "Morgan Meetings",
      role: "Meetings Manager",
      organization: "Assoc",
      claimKind: "FACT",
      sourceUrl: "https://assoc.org/annual/prospectus",
      relationshipToEvent: "Named meetings manager on prospectus",
      eventSpecificEvidence: true,
      employmentStatus: EMPLOYMENT_STATUS.CURRENT_CONFIRMED,
      eventRelationship: EVENT_RELATIONSHIP.CURRENT_EVENT_CONTACT,
    },
    opp
  );
  const { primary } = selectRankedCandidates([ed, mm], opp);
  assert.equal(primary.name, "Morgan Meetings");
});

check("current event contact beats historical event contact", () => {
  const opp = {
    id: "t_cur",
    title: "Summit 2027",
    opportunityType: "PRIMARY_PURSUIT",
    segment: "Associations",
    organizationName: "Org",
  };
  const hist = annotateCandidate(
    applyPersonDiscoveryGates({
      name: "Old Contact",
      role: "Conference Director",
      organization: "Org",
      claimKind: "FACT",
      sourceUrl: "https://org.org/2024-program.pdf",
      employmentStatus: EMPLOYMENT_STATUS.CURRENT_PROBABLE,
      eventRelationship: EVENT_RELATIONSHIP.HISTORICAL_EVENT_CONTACT,
      relationshipToEvent: "Named on 2024 program",
      stillEmployed: true,
    }),
    opp
  );
  const cur = annotateCandidate(
    applyPersonDiscoveryGates({
      name: "New Contact",
      role: "Conference Director",
      organization: "Org",
      claimKind: "FACT",
      sourceUrl: "https://org.org/2027-summit",
      employmentStatus: EMPLOYMENT_STATUS.CURRENT_CONFIRMED,
      eventRelationship: EVENT_RELATIONSHIP.CURRENT_EVENT_CONTACT,
      relationshipToEvent: "Named contact on 2027 summit page",
      stillEmployed: true,
    }),
    opp
  );
  assert.ok(cur.candidateScore >= hist.candidateScore);
  const { primary } = selectRankedCandidates([hist, cur], opp);
  assert.equal(primary.name, "New Contact");
});

check("missing email does not lower WHO score materially", () => {
  const opp = {
    id: "t_email",
    title: "Conf 2027",
    opportunityType: "PRIMARY_PURSUIT",
    segment: "Scientific",
    organizationName: "Sci",
  };
  const base = {
    name: "Alex Planner",
    role: "Director of Meetings",
    organization: "Sci",
    claimKind: "FACT",
    sourceUrl: "https://sci.org/staff",
    relationshipToEvent: "Named on staff directory as meetings director",
    eventSpecificEvidence: true,
    employmentStatus: EMPLOYMENT_STATUS.CURRENT_CONFIRMED,
    eventRelationship: EVENT_RELATIONSHIP.CURRENT_ROLE_LIKELY_OWNER,
  };
  const without = scoreContactCandidate(base, opp);
  const withEmail = scoreContactCandidate({ ...base, email: "alex@sci.org" }, opp);
  assert.ok(
    withEmail.candidateScore - without.candidateScore <= 12,
    `delta ${withEmail.candidateScore - without.candidateScore}`
  );
  assert.ok(without.candidateScore >= 55, `without email score ${without.candidateScore}`);
});

check("weak LinkedIn-only match does not establish event ownership", () => {
  const gated = applyPersonDiscoveryGates({
    name: "Li Only",
    role: "Director of Meetings",
    organization: "AHIMA",
    sourceUrl: "https://www.linkedin.com/in/example",
    employmentStatus: EMPLOYMENT_STATUS.CURRENT_PROBABLE,
    eventRelationship: EVENT_RELATIONSHIP.ORGANIZATION_ROLE_ONLY,
  });
  assert.equal(gated.rejected, true);
  assert.match(gated.rejectReason, /linkedin/i);
});

if (failed) {
  console.error(`\n${failed} failing test(s)`);
  process.exit(1);
}
console.log("\nAll official person-discovery tests passed.");
