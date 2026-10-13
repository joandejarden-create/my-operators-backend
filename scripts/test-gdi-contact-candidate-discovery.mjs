#!/usr/bin/env node
/**
 * Regression tests — GDI Contact Candidate Discovery (WHO ranking).
 * Paid enrichment / Surfe must remain unused.
 */

import assert from "node:assert/strict";
import {
  EVENT_FAMILY,
  GDI_CONTACT_ROLE,
  CANDIDATE_STATUS,
  classifyEventFamily,
  classifyGdiContactRole,
  scoreContactCandidate,
  annotateCandidate,
  selectRankedCandidates,
  discoverContactCandidates,
  NO_PROBABLE,
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

check("association: meetings director beats executive director", () => {
  const opp = {
    id: "t_assoc",
    title: "Example Association Annual Meeting 2027",
    opportunityType: "PRIMARY_PURSUIT",
    segment: "Associations",
    organizationName: "Example Association",
  };
  const ed = annotateCandidate(
    {
      name: "Pat Executive",
      role: "Executive Director",
      organization: "Example Association",
      email: "pat@example.org",
      phone: "202-555-0100",
      claimKind: "FACT",
      sourceUrl: "https://example.org/staff",
      relationshipToEvent: "Association executive",
    },
    opp
  );
  const meetings = annotateCandidate(
    {
      name: "Sam Meetings",
      role: "Director of Meetings & Events",
      organization: "Example Association",
      claimKind: "FACT",
      sourceUrl: "https://example.org/annual-meeting/prospectus",
      relationshipToEvent: "Named as contact in the 2026 annual meeting prospectus",
      eventSpecificEvidence: true,
      // Intentionally weaker reachability than ED
    },
    opp
  );
  const { primary } = selectRankedCandidates([ed, meetings], opp);
  assert.equal(primary.name, "Sam Meetings");
  assert.equal(primary.gdiContactRole, GDI_CONTACT_ROLE.MEETINGS_OWNER);
  assert.ok(primary.candidateScore > ed.candidateScore);
});

check("overflow housing: housing provider beats association executive", () => {
  const opp = {
    id: "t_overflow",
    title: "Example Cup 2027",
    opportunityType: "OVERFLOW_HOUSING",
    segment: "Sports",
    organizationName: "Example Soccer Club",
  };
  assert.equal(classifyEventFamily(opp), EVENT_FAMILY.HOUSING_OVERFLOW);
  const housing = annotateCandidate(
    {
      name: "HBC Event Services",
      role: "Official stay-to-play housing partner",
      organization: "HBC Event Services",
      email: "support@hbceventservices.com",
      claimKind: "FACT",
      sourceUrl: "https://example.org/hotels",
      relationshipToEvent: "Manages mandatory stay-to-play hotel program",
      targetRoleMatch: "HOUSING_SOURCING_CONTACT",
      eventSpecificEvidence: true,
    },
    opp
  );
  const ed = annotateCandidate(
    {
      name: "Chris Clubed",
      role: "Executive Director",
      organization: "Example Soccer Club",
      email: "chris@examplesoccer.org",
      phone: "301-555-0199",
      claimKind: "FACT",
      sourceUrl: "https://examplesoccer.org/staff",
      relationshipToEvent: "Club executive director",
    },
    opp
  );
  const { primary } = selectRankedCandidates([ed, housing], opp);
  assert.equal(primary.name, "HBC Event Services");
  assert.equal(primary.gdiContactRole, GDI_CONTACT_ROLE.HOUSING_OWNER);
});

check("stale former employee → reject/downgrade, not primary", () => {
  const opp = {
    id: "t_stale",
    title: "Science Meeting 2027",
    opportunityType: "PRIMARY_PURSUIT",
    segment: "Scientific",
    organizationName: "Science Org",
  };
  const stale = annotateCandidate(
    {
      name: "Old Employee",
      role: "Director of Meetings",
      organization: "Science Org",
      email: "old@science.org",
      stillEmployed: false,
      staleUnverified: true,
      historicalOnly: true,
      claimKind: "FACT",
      sourceUrl: "https://science.org/old-staff",
      relationshipToEvent: "Former meetings director (left 2023)",
    },
    opp
  );
  const current = annotateCandidate(
    {
      name: "New Planner",
      role: "Meetings Manager",
      organization: "Science Org",
      claimKind: "FACT",
      sourceUrl: "https://science.org/staff",
      relationshipToEvent: "Current meetings manager on staff page",
      eventSpecificEvidence: true,
      stillEmployed: true,
    },
    opp
  );
  assert.equal(stale.candidateStatus, CANDIDATE_STATUS.STALE);
  const { primary } = selectRankedCandidates([stale, current], opp);
  assert.equal(primary.name, "New Planner");
});

check("event-specific named planner beats generic org contact", () => {
  const opp = {
    id: "t_event",
    title: "Advocacy Summit 2027",
    opportunityType: "PRIMARY_PURSUIT",
    segment: "Associations",
    organizationName: "Advocacy Org",
  };
  const generic = annotateCandidate(
    {
      name: "Jordan Generic",
      role: "Membership Coordinator",
      organization: "Advocacy Org",
      email: "info@advocacy.org",
      claimKind: "FACT",
      sourceUrl: "https://advocacy.org/contact",
      relationshipToEvent: "General organization staff",
    },
    opp
  );
  const planner = annotateCandidate(
    {
      name: "Riley Planner",
      role: "Conference Director",
      organization: "Advocacy Org",
      claimKind: "FACT",
      sourceUrl: "https://advocacy.org/summit-2027",
      relationshipToEvent: "Named conference director on current event page",
      eventSpecificEvidence: true,
    },
    opp
  );
  const { primary } = selectRankedCandidates([generic, planner], opp);
  assert.equal(primary.name, "Riley Planner");
});

check("high reachability + weak role must not become primary over strong role", () => {
  const opp = {
    id: "t_reach",
    title: "Industry Conference 2027",
    opportunityType: "PRIMARY_PURSUIT",
    segment: "Associations",
    organizationName: "Industry Assoc",
  };
  const easyWeak = annotateCandidate(
    {
      name: "Easy Email",
      role: "Receptionist",
      organization: "Industry Assoc",
      email: "easy@industry.org",
      phone: "202-555-0111",
      claimKind: "FACT",
      sourceUrl: "https://industry.org/contact",
      relationshipToEvent: "Front desk",
      emailType: "DIRECT_WORK",
    },
    opp
  );
  const hardStrong = annotateCandidate(
    {
      name: "Hard Reach Meetings",
      role: "VP Meetings & Events",
      organization: "Industry Assoc",
      claimKind: "FACT",
      sourceUrl: "https://industry.org/events/team",
      relationshipToEvent: "Listed as VP Meetings on events team page",
      eventSpecificEvidence: true,
      // no email/phone
    },
    opp
  );
  assert.ok(
    (easyWeak.reachability?.gap === "REACHABLE" || easyWeak.email) &&
      hardStrong.reachability?.gap !== "REACHABLE"
  );
  const { primary } = selectRankedCandidates([easyWeak, hardStrong], opp);
  assert.equal(primary.name, "Hard Reach Meetings");
  assert.ok(primary.candidateScore > easyWeak.candidateScore);
});

check("no evidence → unresolved, do not fabricate", () => {
  const opp = {
    id: "t_none",
    title: "Mystery Meeting 2028",
    opportunityType: "FUTURE_CYCLE",
    segment: "Corporate",
    organizationName: "Mystery Corp",
    primaryContact: null,
    backupContacts: [],
    contacts: [],
  };
  const result = discoverContactCandidates(opp, { seeds: { t_none: [] } });
  assert.equal(result.resolution, NO_PROBABLE);
  assert.equal(result.primaryCandidate, null);
  assert.equal(result.metrics.hasPrimary, false);
});

check("prior event planner still employed → reactivation boost", () => {
  const opp = {
    id: "t_react",
    title: "Returning Conference 2027",
    opportunityType: "REACTIVATION",
    segment: "Associations",
    organizationName: "Return Assoc",
    reactivationSignal: "PRIOR_HOST",
  };
  const prior = {
    name: "Alex Priorplanner",
    role: "Director of Meetings",
    organization: "Return Assoc",
    claimKind: "FACT",
    sourceUrl: "https://return.org/staff",
    relationshipToEvent: "Prior-year meeting planner; still on staff",
    priorEventInvolvement: true,
    historicalOnly: true,
    stillEmployed: true,
    reactivationBoost: true,
    eventSpecificEvidence: true,
  };
  const scoredBoost = scoreContactCandidate(prior, opp);
  const scoredNoBoost = scoreContactCandidate(
    { ...prior, reactivationBoost: false, priorEventInvolvement: false, historicalOnly: false },
    { ...opp, opportunityType: "PRIMARY_PURSUIT", reactivationSignal: null }
  );
  assert.ok(
    scoredBoost.candidateScore >= scoredNoBoost.candidateScore,
    `boost ${scoredBoost.candidateScore} vs ${scoredNoBoost.candidateScore}`
  );
  const annotated = annotateCandidate(prior, opp);
  assert.ok(annotated.candidateScore >= 55);
  assert.ok(
    [CANDIDATE_STATUS.STRONG_PROBABLE, CANDIDATE_STATUS.VERIFIED_ROLE_MATCH, CANDIDATE_STATUS.VERIFIED_EVENT_OWNER, CANDIDATE_STATUS.PROBABLE].includes(
      annotated.candidateStatus
    )
  );
});

check("role classifier: housing vs meetings vs executive", () => {
  assert.equal(
    classifyGdiContactRole({ role: "Official housing partner", name: "HBC Event Services" }),
    GDI_CONTACT_ROLE.HOUSING_OWNER
  );
  assert.equal(
    classifyGdiContactRole({ role: "Director of Meetings & Events" }),
    GDI_CONTACT_ROLE.MEETINGS_OWNER
  );
  assert.equal(
    classifyGdiContactRole({ role: "Executive Director" }),
    GDI_CONTACT_ROLE.EXECUTIVE_SPONSOR
  );
});

check("discovery must not mark Surfe/paid as used", () => {
  const opp = {
    id: "gdi_opp_nar_gad_institute_2027",
    title: "NAR GAD Institute 2027",
    opportunityType: "PRIMARY_PURSUIT",
    segment: "Association / Advocacy",
    organizationName: "National Association of REALTORS",
    primaryContact: {
      name: "Jami Sims",
      role: "NAR contact — GAD Institute",
      organization: "National Association of REALTORS",
      email: "GADInst@nar.realtor",
      claimKind: "FACT",
      sourceUrl: "https://www.nar.realtor/events/2027-gad-institute",
      relationshipToEvent: "Named contact on official event page",
      targetRoleMatch: "EVENT_MEETINGS_OWNER",
    },
  };
  const result = discoverContactCandidates(opp);
  assert.equal(result.surfeUsed, false);
  assert.equal(result.paidEnrichmentUsed, false);
  assert.ok(result.primaryCandidate);
  assert.equal(result.primaryCandidate.name, "Jami Sims");
});

if (failed) {
  console.error(`\n${failed} failing test(s)`);
  process.exit(1);
}
console.log("\nAll GDI contact-candidate discovery tests passed.");
