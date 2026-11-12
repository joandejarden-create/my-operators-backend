#!/usr/bin/env node
/**
 * Regression: Bethesda 88 Full E2E opportunity quality contract + orchestration.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_OPPORTUNITY_QUALITY_CONTRACT_V1,
  RESEARCH_INTEGRITY,
  COMMERCIAL_CERT,
  evaluateOpportunityQualityContract,
  evaluateOpportunityQualityGate,
  evaluateOpportunityQualityBatch,
} from "../lib/group-demand-intelligence/gdi-opportunity-quality-contract-v1.js";
import {
  runGdiOpportunityFullE2eQualityOrchestration,
  GDI_OPPORTUNITY_FULL_E2E_QUALITY_ORCHESTRATOR_V1,
  proposeOpportunityRemediation,
} from "../lib/group-demand-intelligence/gdi-opportunity-full-e2e-quality-orchestrator-v1.js";
import {
  loadHotelOpportunityDoc,
  BETHESDA_PROTECTED_SCALING_IDS,
  CANONICAL_12_CONTACT_PATCHED_59_ROW_SHA256,
  WAVE2_FOUR_CONTACT_PATCHED_59_ROW_SHA256,
  FINAL_FULL_E2E_REMEDIATED_59_ROW_SHA256,
  ACCEPTABLE_59_ROW_SHA256S,
  BETHESDA_HOTEL_ID,
} from "../lib/group-demand-intelligence/research/gdi-hotel-store-integrity-guard-v1.js";
import { BETHESDA_29_ATTESTED_IDS } from "../lib/group-demand-intelligence/research/bethesda-historical-attestation-bag-v1.js";
import { assertPublicationWriteReadiness } from "../lib/group-demand-intelligence/workflow/full-e2e-publication-quality-v1.js";
import { hasMeetingCredentialContamination } from "../lib/group-demand-intelligence/opportunity-title-quality-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

assert.equal(typeof GDI_OPPORTUNITY_QUALITY_CONTRACT_V1, "string");
assert.equal(typeof GDI_OPPORTUNITY_FULL_E2E_QUALITY_ORCHESTRATOR_V1, "string");

// --- Contract unit cases ---
const credentialOpp = {
  id: "test_quarantine_credential",
  organizationName: "Example Org",
  title: "Join Zoom Meeting ID 123 Password secret",
  whatIsHappening: "Virtual join instructions only — not a commercial lodging opportunity.",
  commercialEventFamilyRole: "QUARANTINED",
  titleSourceStatus: "TITLE_SOURCE_INVALID",
};
assert.equal(hasMeetingCredentialContamination(credentialOpp.title), true);
const qEval = evaluateOpportunityQualityContract(credentialOpp, {
  familyRole: "QUARANTINED",
});
assert.equal(qEval.researchIntegrity, RESEARCH_INTEGRITY.QUARANTINED);
assert.equal(qEval.commercialCertification, COMMERCIAL_CERT.QUARANTINED);
assert.equal(qEval.eligibleForCustomerPublicationReview, false);

const gateQ = evaluateOpportunityQualityGate(credentialOpp, {
  familyRole: "QUARANTINED",
});
assert.equal(gateQ.okForCustomerPublication, false);
assert.equal(gateQ.okForResearchContinuation, true);

const salesReadyShape = {
  id: "test_sr_shape",
  organizationName: "Demo Buyer Co",
  title: "Demo Buyer Co Annual Leadership Summit 2026",
  category: "Corporate Meetings",
  whatIsHappening:
    "Annual leadership summit with confirmed lodging and meeting demand near Bethesda for 2026.",
  hotelFitRationale:
    "Bethesda Marriott sits within the National Harbor / Bethesda corporate corridor for overflow and meeting packages.",
  whyThisHotel:
    "Walkable to metro and proximate to the buyer HQ for room blocks and catering.",
  recommendedNextAction:
    "Approach the named meeting planner to discuss room block and F&B packages for the 2026 summit.",
  primaryContact: {
    name: "Jane Example",
    title: "Director of Meetings",
    email: "jane.example@demobuyer.example",
    contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
  },
  contactRecovery: { namedPerson: true, usableRoute: true },
  demandStatus: "CONFIRMED_DEMAND",
  officialSource: "https://example.com/summit",
};
const srEval = evaluateOpportunityQualityContract(salesReadyShape);
assert.ok(srEval.titleOk);
assert.ok(srEval.namedRelevantPrimary);
assert.equal(typeof srEval.commercialCertification, "string");

const adVsSr = evaluateOpportunityQualityBatch(
  [
    {
      id: "linked_a",
      organizationName: "CTN",
      title: "CTN / NCI linked research row",
      commercialEventFamilyRole: "LINKED_RESEARCH",
      whatIsHappening: "Linked research observation for the primary CTN/NCI event family.",
    },
  ],
  {
    familyById: new Map([
      ["linked_a", { role: "LINKED_RESEARCH", familyId: "ctn_nci" }],
    ]),
  }
);
assert.equal(
  adVsSr.rows[0].commercialCertification,
  COMMERCIAL_CERT.LINKED_RESEARCH
);

// Anti-clobber remediation proposal (no invent)
const rem = proposeOpportunityRemediation(
  {
    id: "gdi_pe_781f12393f8117e7",
    organizationName: "Woman's Club of Chevy Chase",
    title: "Woman's Club of Chevy Chase — event hospitality",
    primaryContact: { name: "Diane Robertson", title: "Manager" },
  },
  { historical: false }
);
assert.equal(rem.evaluation.productStatusMutationAuthorized, false);
assert.equal(rem.proposed.salesReady, false);
assert.equal(rem.proposed.customerFacing, false);

// Publication gate blocks quarantine
const pub = assertPublicationWriteReadiness(credentialOpp, {
  requireStructuredHotelFit: false,
});
assert.equal(pub.ok, false);
assert.ok(
  (pub.blockers || []).some((b) => b.code === "gdi_opportunity_quality_contract_block")
);

// --- Full 88 orchestration (historical apply allowed; canonical dry-run) ---
const storeBefore = loadHotelOpportunityDoc(BETHESDA_HOTEL_ID, { root: ROOT });
assert.equal(storeBefore.count, 59);
assert.ok(ACCEPTABLE_59_ROW_SHA256S.includes(storeBefore.sha256));
assert.ok(
  [
    CANONICAL_12_CONTACT_PATCHED_59_ROW_SHA256,
    WAVE2_FOUR_CONTACT_PATCHED_59_ROW_SHA256,
  FINAL_FULL_E2E_REMEDIATED_59_ROW_SHA256,
  ].includes(storeBefore.sha256)
);
for (const id of BETHESDA_PROTECTED_SCALING_IDS) {
  assert.ok(storeBefore.ids.includes(id), `missing SC ${id}`);
}
assert.equal(BETHESDA_29_ATTESTED_IDS.length, 29);

const report = runGdiOpportunityFullE2eQualityOrchestration({
  root: ROOT,
  applyHistorical: true,
  applyCanonical: false,
  founderAuthorization: false,
  paidResearchAuthorized: false,
});

assert.equal(report.ok, true);
assert.equal(report.execution.processedCount, 88);
assert.equal(report.execution.allIdsProcessed, true);
assert.equal(report.safety.paidResearch, false);
assert.equal(report.safety.opportunitiesJsonWrites, false);
assert.equal(report.safety.customerPublication, false);
assert.equal(report.proposedPaidBatch.authorized, false);
assert.equal(report.canonicalDryRunManifest.requiresFounderApproval, true);
assert.equal(report.remediations.length, 88);

// Unique commercial ≤ raw 88; linked/quarantine accounted
assert.ok(report.execution.uniqueCommercialOpportunities <= 88);
assert.ok(report.execution.uniqueCommercialOpportunities >= 80);

// Store SHA unchanged (no canonical write)
const storeAfter = loadHotelOpportunityDoc(BETHESDA_HOTEL_ID, { root: ROOT });
assert.equal(storeAfter.sha256, storeBefore.sha256);
assert.equal(storeAfter.count, 59);

// Checkpoints for every ID
const checkpointDir = path.join(
  ROOT,
  "reports/gdi-bethesda-88-full-e2e-opportunity-quality-v1/checkpoints"
);
assert.ok(fs.existsSync(checkpointDir));
for (const id of report.execution.processedIds) {
  assert.ok(
    fs.existsSync(path.join(checkpointDir, `${id}.json`)),
    `missing checkpoint ${id}`
  );
}

// SC still present in remediations
for (const id of BETHESDA_PROTECTED_SCALING_IDS) {
  assert.ok(report.remediations.some((r) => r.id === id), `SC missing ${id}`);
}

// Other-hotel non-regression: contract accepts non-Bethesda ids
const other = evaluateOpportunityQualityContract({
  id: "gdi_other_hotel_sample",
  organizationName: "Other Hotel Buyer",
  title: "Regional Association Annual Meeting 2027",
  category: "Association Events",
  whatIsHappening:
    "Regional association annual meeting with lodging demand in another market.",
  recommendedNextAction: "Contact association meetings director for room-block discussion.",
});
assert.equal(other.version, GDI_OPPORTUNITY_QUALITY_CONTRACT_V1);
assert.ok(other.id);

// Named coverage must not regress in projected after vs before
assert.ok(
  report.afterProjected.namedRelevantPrimary >= report.before.namedRelevantPrimary,
  "named coverage must not regress in projected after"
);

console.log(
  JSON.stringify(
    {
      ok: true,
      test: "test-gdi-bethesda-88-full-e2e-opportunity-quality-v1",
      processed: 88,
      uniqueCommercial: report.execution.uniqueCommercialOpportunities,
      namedBefore: report.before.namedRelevantPrimary,
      namedAfterProjected: report.afterProjected.namedRelevantPrimary,
      titleOkAfter: report.afterProjected.titleOk,
      commercialCertification: report.afterProjected.commercialCertification,
      researchIntegrity: report.afterProjected.researchIntegrity,
      canonicalDryRunPatches: report.canonicalDryRunManifest.patchCount,
      historicalWriteApplied: report.historicalPersist?.writeApplied === true,
      storeShaUnchanged: true,
      paidAuthorized: false,
    },
    null,
    2
  )
);
