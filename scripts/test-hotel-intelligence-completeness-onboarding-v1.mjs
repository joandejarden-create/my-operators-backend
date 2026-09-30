/**
 * Tests: Hotel Intelligence Completeness + Onboarding V1
 *   node scripts/test-hotel-intelligence-completeness-onboarding-v1.mjs
 */
import assert from "node:assert/strict";
import {
  HI_DOMAIN_STATUS,
  HI_DOMAIN,
  REQUIRED_HI_DOMAINS,
  deriveDomainStatus,
  summarizeOverallHiStatus,
  domainCoverageScore,
  isDomainResolved,
  OVERALL_HI_STATUS,
} from "../lib/hotel-intelligence/onboarding/domain-status-v1.js";
import {
  gdiEventSpaceCapabilitySemantics,
  assertNoNotResearchedForOnboard,
} from "../lib/hotel-intelligence/onboarding/hi-completeness-gate.js";
import { evaluateHotelDomainStatuses } from "../lib/hotel-intelligence/onboarding/domain-status-store.js";

// Enum + resolved semantics
{
  assert.equal(REQUIRED_HI_DOMAINS.length, 6);
  assert.equal(isDomainResolved(HI_DOMAIN_STATUS.POPULATED), true);
  assert.equal(isDomainResolved(HI_DOMAIN_STATUS.RESEARCHED_EMPTY), true);
  assert.equal(isDomainResolved(HI_DOMAIN_STATUS.PUBLIC_DATA_CEILING), true);
  assert.equal(isDomainResolved(HI_DOMAIN_STATUS.NOT_RESEARCHED), false);
}

// Zero rows without research = NOT_RESEARCHED (false completeness)
{
  const empty = deriveDomainStatus(HI_DOMAIN.EVENT_SPACES, { rowCount: 0 });
  assert.equal(empty.domainStatus, HI_DOMAIN_STATUS.NOT_RESEARCHED);
  assert.equal(empty.falseCompletenessFlag, "ROW_EMPTY_WITH_NO_RESEARCH_STATE");
}

// Zero rows after research = RESEARCHED_EMPTY
{
  const emptyOk = deriveDomainStatus(HI_DOMAIN.EVENT_SPACES, {
    rowCount: 0,
    researchAttempted: true,
    researchedEmpty: true,
  });
  assert.equal(emptyOk.domainStatus, HI_DOMAIN_STATUS.RESEARCHED_EMPTY);
}

// Public data ceiling
{
  const ceiling = deriveDomainStatus(HI_DOMAIN.EVENT_SPACES, {
    rowCount: 0,
    researchAttempted: true,
    researchExhausted: true,
  });
  assert.equal(ceiling.domainStatus, HI_DOMAIN_STATUS.PUBLIC_DATA_CEILING);
}

// Populated
{
  const pop = deriveDomainStatus(HI_DOMAIN.EVENT_SPACES, { rowCount: 3 });
  assert.equal(pop.domainStatus, HI_DOMAIN_STATUS.POPULATED);
}

// Completeness gate overall
{
  const domains = Object.fromEntries(
    REQUIRED_HI_DOMAINS.map((d) => [
      d,
      { domainStatus: HI_DOMAIN_STATUS.POPULATED },
    ])
  );
  domains[HI_DOMAIN.SEASONALITY_NEED_PERIODS] = {
    domainStatus: HI_DOMAIN_STATUS.RESEARCHED_EMPTY,
  };
  domains[HI_DOMAIN.EVENT_SPACES] = {
    domainStatus: HI_DOMAIN_STATUS.PUBLIC_DATA_CEILING,
  };
  assert.equal(summarizeOverallHiStatus(domains), OVERALL_HI_STATUS.HI_COMPLETE);
  assert.equal(domainCoverageScore(domains).label, "6/6");

  domains[HI_DOMAIN.EVENT_SPACES] = {
    domainStatus: HI_DOMAIN_STATUS.NOT_RESEARCHED,
  };
  assert.equal(summarizeOverallHiStatus(domains), OVERALL_HI_STATUS.HI_INCOMPLETE);
}

// GDI event-space semantics
{
  const unresearched = gdiEventSpaceCapabilitySemantics(HI_DOMAIN_STATUS.NOT_RESEARCHED);
  assert.equal(unresearched.mayTreatAsNoCapability, false);
  assert.equal(unresearched.meetingCapabilityState, "UNKNOWN_UNRESEARCHED");

  const researchedEmpty = gdiEventSpaceCapabilitySemantics(
    HI_DOMAIN_STATUS.RESEARCHED_EMPTY
  );
  assert.equal(researchedEmpty.mayTreatAsNoCapability, true);
  assert.equal(researchedEmpty.meetingCapabilityState, "KNOWN_EMPTY");

  const populated = gdiEventSpaceCapabilitySemantics(HI_DOMAIN_STATUS.POPULATED);
  assert.equal(populated.mayEnterFitWithKnownMeetingState, true);
}

// Onboarding invariant: cannot proceed with NOT_RESEARCHED
{
  const blocked = assertNoNotResearchedForOnboard({
    complete: false,
    blockingDomains: [
      { domain: HI_DOMAIN.EVENT_SPACES, status: HI_DOMAIN_STATUS.NOT_RESEARCHED },
    ],
  });
  assert.equal(blocked.allowed, false);

  const allowed = assertNoNotResearchedForOnboard({ complete: true, blockingDomains: [] });
  assert.equal(allowed.allowed, true);
}

// Demand from GDI-only does not count as HI populated
{
  const evaluated = evaluateHotelDomainStatuses("recTEST", {
    identity: { hpcHotelId: "recTEST", hotelName: "Test" },
    commercialProfile: { rooms: 100, fromHiAirtable: true },
    eventSpaces: [],
    demandNodes: [{ name: "Corp", fromHiAirtable: false }],
    seasonality: [],
    needPeriods: [],
    evidenceSummary: {
      commercialAirtable: true,
      demandNodeAirtable: false,
      eventSpaceAirtable: false,
      evidenceAirtable: 0,
      legacyJsonFallback: { demandFromGdi: true },
    },
  }, { adpAttributeActiveCount: 0 });
  assert.equal(
    evaluated.domains[HI_DOMAIN.DEMAND_NODES].domainStatus,
    HI_DOMAIN_STATUS.NOT_RESEARCHED
  );
}

console.log("test-hotel-intelligence-completeness-onboarding-v1: PASS");
