#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  classifyCommercialOpportunity,
  projectDualProspectLists,
  filterProspectList,
  OPPORTUNITY_TIER,
  NOTICE_STATUS,
  PROSPECT_LIST,
} from "../lib/hotel-intelligence/sec-edgar/commercial-target-discovery-v1.js";
import { AUDITED_DATE_MEANING, EVIDENCE_CONFIDENCE, DECISION_WINDOW_ELIGIBILITY } from "../lib/hotel-intelligence/sec-edgar/contract-date-meaning-audit-v1.js";

// Opening year must HOLD — never Tier A/B
const opening = classifyCommercialOpportunity({
  hotelName: "Courtyard Example City",
  hotelVerified: true,
  primaryDate: "2008-01-01",
  dateMeaning: "FRANCHISE_EXPIRATION",
  evidenceSnippet: "Select Service | 2008 | 2018 | 104 | Rooms",
  agreementType: "FRANCHISE",
  sourceUrl: "https://www.sec.gov/Archives/edgar/data/1/x.htm",
});
assert.equal(opening.tier, OPPORTUNITY_TIER.HOLD_QUARANTINE);

// Tier B — verified expiration, notice not disclosed
const tierB = classifyCommercialOpportunity({
  hotelName: "Hampton Inn Austin Downtown",
  hotelVerified: true,
  hotelAttributed: true,
  agreementType: "FRANCHISE",
  primaryDate: "2028-06-30",
  brand: "Hampton",
  owner: "Example Owner LLC",
  evidenceSnippet: "Franchise Expiration Date June 30, 2028 for Hampton Inn Austin Downtown",
  dateAudit: {
    auditedMeaning: AUDITED_DATE_MEANING.FRANCHISE_AGREEMENT_EXPIRATION,
    confidence: EVIDENCE_CONFIDENCE.HIGH,
    decisionWindowEligibility: DECISION_WINDOW_ELIGIBILITY.ELIGIBLE,
  },
  sourceUrl: "https://www.sec.gov/Archives/edgar/data/1/ex.htm",
});
assert.equal(tierB.tier, OPPORTUNITY_TIER.TIER_B_VERIFIED_CONTRACT_EXPIRATION);
assert.equal(tierB.noticeStatus, NOTICE_STATUS.NOTICE_NOT_DISCLOSED);
assert.equal(tierB.researchEligible, true);
assert.equal(tierB.nearTermOutreachPriority, true);
assert.equal(tierB.activeTargeting, true);

// Historical Tier B remains research-eligible (no horizon exclusion)
const tierBHistorical = classifyCommercialOpportunity({
  hotelName: "Hampton Inn Historical Example",
  hotelVerified: true,
  hotelAttributed: true,
  agreementType: "FRANCHISE",
  primaryDate: "2018-06-30",
  brand: "Hampton",
  evidenceSnippet: "Franchise Expiration Date June 30, 2018 for Hampton Inn Historical Example",
  dateAudit: {
    auditedMeaning: AUDITED_DATE_MEANING.FRANCHISE_AGREEMENT_EXPIRATION,
    confidence: EVIDENCE_CONFIDENCE.HIGH,
    decisionWindowEligibility: DECISION_WINDOW_ELIGIBILITY.ELIGIBLE,
  },
  sourceUrl: "https://www.sec.gov/Archives/edgar/data/1/exh.htm",
});
assert.equal(tierBHistorical.tier, OPPORTUNITY_TIER.TIER_B_VERIFIED_CONTRACT_EXPIRATION);
assert.equal(tierBHistorical.researchEligible, true);
assert.equal(tierBHistorical.nearTermOutreachPriority, false);
assert.ok(tierBHistorical.appDateFilterHints.includes("HISTORICAL_EXPIRATIONS"));
assert.ok(
  filterProspectList([tierBHistorical], { dateFilter: "HISTORICAL_EXPIRATIONS" }).length === 1,
);

// Tier A — expiration + notice days
const tierA = classifyCommercialOpportunity({
  hotelName: "Residence Inn Miami Airport",
  hotelVerified: true,
  hotelAttributed: true,
  agreementType: "FRANCHISE",
  primaryDate: "2027-12-31",
  renewalNoticeDays: 180,
  evidenceSnippet: "Franchise agreement expires December 31, 2027; 180 days prior written notice",
  dateAudit: {
    auditedMeaning: AUDITED_DATE_MEANING.FRANCHISE_AGREEMENT_EXPIRATION,
    confidence: EVIDENCE_CONFIDENCE.HIGH,
    decisionWindowEligibility: DECISION_WINDOW_ELIGIBILITY.ELIGIBLE,
  },
  sourceUrl: "https://www.sec.gov/Archives/edgar/data/1/ex2.htm",
});
assert.equal(tierA.tier, OPPORTUNITY_TIER.TIER_A_VERIFIED_DECISION_WINDOW);
assert.ok(tierA.noticeDeadline);

// Tier C — hotel + conversion signal, no date
const tierC = classifyCommercialOpportunity({
  hotelName: "Hilton Garden Inn Example",
  hotelVerified: true,
  agreementType: "UNKNOWN",
  brand: "Hilton",
  conversionSignals: { brand: true },
  evidenceSnippet: "Operator transition announced; franchise status unclear",
  sourceUrl: "https://www.sec.gov/x.htm",
});
assert.equal(tierC.tier, OPPORTUNITY_TIER.TIER_C_CONVERSION_RESEARCH_CANDIDATE);

// Dual projections share research identity
const lists = projectDualProspectLists([
  tierA,
  {
    ...tierC,
    listHints: [PROSPECT_LIST.BRAND_CONVERSION],
    conversionSignals: { brand: true },
  },
  {
    researchId: "ct_mgmt1",
    tier: OPPORTUNITY_TIER.TIER_B_VERIFIED_CONTRACT_EXPIRATION,
    agreementType: "MANAGEMENT",
    hotelName: "Independent Resort Managed",
    verifiedEndDate: "2029-01-01",
    noticeStatus: NOTICE_STATUS.NOTICE_NOT_DISCLOSED,
    activeTargeting: true,
    changeClass: "NEW",
    renewalStatusClass: "STATUS_UNCONFIRMED",
    historicalApplicability: "HISTORICALLY_DISCLOSED_ONLY",
    confidence: "MEDIUM",
    sourceUrl: "https://www.sec.gov/y.htm",
    recommendedNextAction: "ENRICH_OWNER_OPERATOR_CONTACTS",
  },
]);
assert.ok(lists.brandConversionTargets.length >= 1);
assert.ok(lists.managementContractTargets.length >= 1);
assert.equal(
  filterProspectList(lists.brandConversionTargets, { requireVerifiedDate: true }).every((r) => r.verifiedEndDate),
  true,
);

// LEASE: classify + retain incidentally; never a third list; never inflate Brand/Management
const lease = classifyCommercialOpportunity({
  hotelName: "Example Ground Lease Hotel",
  hotelVerified: true,
  hotelAttributed: true,
  agreementType: "LEASE",
  primaryDate: "2031-12-31",
  owner: "Example Lessor LLC",
  evidenceSnippet: "Ground lease expires December 31, 2031 for Example Ground Lease Hotel",
  dateAudit: {
    auditedMeaning: AUDITED_DATE_MEANING.LEASE_OR_LICENSE_EXPIRATION,
    confidence: EVIDENCE_CONFIDENCE.HIGH,
    decisionWindowEligibility: DECISION_WINDOW_ELIGIBILITY.ELIGIBLE,
  },
  sourceUrl: "https://www.sec.gov/Archives/edgar/data/1/lease.htm",
  brand: "WouldNotMatter",
  conversionSignals: { brand: true },
  listHints: [PROSPECT_LIST.BRAND_CONVERSION, PROSPECT_LIST.MANAGEMENT_CONTRACT],
});
assert.notEqual(lease.tier, OPPORTUNITY_TIER.HOLD_QUARANTINE);
assert.equal(lease.agreementType, "LEASE");
assert.equal(lease.verifiedEndDate, "2031-12-31");

const leaseProj = projectDualProspectLists([lease, tierA]);
assert.equal(leaseProj.incidentalLeaseRetained.length, 1);
assert.equal(leaseProj.incidentalLeaseRetained[0].retentionMode, "INCIDENTAL_EVIDENCE_ONLY");
assert.equal(leaseProj.deferredLeaseIntelligence?.thirdProspectList, false);
assert.equal(leaseProj.deferredLeaseIntelligence?.dedicatedDiscoveryAuthorized, false);
assert.equal(
  leaseProj.brandConversionTargets.filter((r) => r.agreementType === "LEASE").length,
  0,
);
assert.equal(
  leaseProj.managementContractTargets.filter((r) => r.agreementType === "LEASE").length,
  0,
);
assert.ok(!Object.prototype.hasOwnProperty.call(PROSPECT_LIST, "LEASE"));

console.log("test-commercial-target-discovery-v1: PASS");
