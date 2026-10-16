/**
 * Focused tests: reviewed prospect publication visibility + import idempotency helpers.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { isCustomerFacingOpportunity } from "../lib/group-demand-intelligence/customer-visibility.js";
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import {
  isReviewedProspectCustomerVisible,
  REVIEWED_PROSPECT_PUBLICATION_BASIS,
} from "../lib/group-demand-intelligence/reviewed-prospect-publication-v1.js";
import { importReviewedProspectsForHotel } from "../lib/group-demand-intelligence/reviewed-prospect-import-v1.js";
import { applyContactPackageToOpportunity } from "../lib/group-demand-intelligence/prospect-publication-eligibility-v1.js";
import {
  clearGdiRepositoryReadCache,
  loadOpportunities,
  saveOpportunities,
  saveHotelProfile,
} from "../lib/group-demand-intelligence/repository.js";

const packageDir = path.resolve(
  "reports/gdi-bethesda-process-replication/hotel-review"
);
const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-prospect-"));
process.env.GDI_DATA_ROOT = tmpRoot;
process.env.GDI_OPPORTUNITIES_PERSISTENCE = "filesystem";
process.env.GDI_REVIEWED_PROSPECT_PUBLICATION_V1 = "1";
clearGdiRepositoryReadCache();

const hotelId = "recLuxvwwxID7U2B8";
saveHotelProfile(hotelId, {
  hotelId,
  identity: { hotelName: "Bethesda Marriott" },
});
saveOpportunities(hotelId, {
  hotelId,
  opportunities: [
    {
      id: "gdi_opp_ndss_advocacy_2027",
      hotelId,
      title: "NDSS Down Syndrome Advocacy Conference 2027 — hotel block TBA",
      organizationName: "National Down Syndrome Society (NDSS)",
      salesOwner: "keep-me",
      notes: "pre-existing note",
    },
  ],
});

// Homepage-only must not become customer-facing via prospect path
{
  const bad = {
    hotelId,
    prospectPublicationBasis: REVIEWED_PROSPECT_PUBLICATION_BASIS,
    prospectHotelFacing: true,
    organizationName: "Example",
    title: "Example 2027",
    publicContactPath: "https://example.org/",
    fitExplanation: "fit",
    officialSource: "https://example.org/event",
  };
  assert.equal(isReviewedProspectCustomerVisible(bad), false);
}

// Functional contact + stamp → visible; Ready may still be false
{
  let opp = {
    hotelId,
    title: "Example Advocacy 2027",
    organizationName: "Example Association",
    fitExplanation: "Downtown fit for association lodging",
    hotelFitScore: 60,
    hotelFitScoreBasis: "gdi-hotel-fit-v1",
    hotelOpportunityThesis:
      "Downtown lodging for association attendees traveling for the multi-day conference",
    travelEvidenceNote:
      "Delegates travel overnight for the multi-day advocacy conference; room block expected",
    roomDemandStatus: "REQUIRED",
    officialSource: "https://example.org/events/2027",
    sources: [{ url: "https://example.org/events/2027" }],
    timing: "Verified: April 1–2, 2027",
    eventStartDate: "2027-04-01",
    prospectPublicationBasis: REVIEWED_PROSPECT_PUBLICATION_BASIS,
    prospectReviewSection: "PRIORITY",
    recommendedAction: "Call the published route",
  };
  opp = applyContactPackageToOpportunity(opp, {
    person: "Jane Smith",
    title: "Director of Events",
    contactMethod: "jane@example.org",
    businessPhone: "202-555-0100",
    phoneType: "direct",
    bookingResponsibility: "POTENTIAL_RELEVANT_CONTACT",
    sourceUrl: "https://example.org/staff",
    verificationDate: "2026-10-14",
  });
  opp.prospectHotelFacing = true;
  assert.equal(isReviewedProspectCustomerVisible(opp), true);
  assert.equal(isCustomerFacingOpportunity(opp), true);
  assert.equal(isGdiCustomerOpportunityReady(opp).ok, false);
}

// Import dry-run then apply twice — stable IDs + preserve notes
{
  const dry = await importReviewedProspectsForHotel({
    hotelId,
    packageDir,
    dryRun: true,
  });
  assert.equal(dry.ok, true);
  assert.ok(dry.counts.priorityHotelFacing >= 10);

  const a1 = await importReviewedProspectsForHotel({
    hotelId,
    packageDir,
    dryRun: false,
    backupDir: path.join(tmpRoot, "backups"),
  });
  assert.equal(a1.ok, true);
  const ndss1 = a1.manifest.find((m) => m.cardId === "beth-ndss-2027");
  assert.equal(ndss1.opportunityId, "gdi_opp_ndss_advocacy_2027");
  assert.equal(ndss1.action, "update");

  const after1 = loadOpportunities(hotelId);
  const ndssOpp = after1.opportunities.find((o) => o.id === "gdi_opp_ndss_advocacy_2027");
  assert.equal(ndssOpp.salesOwner, "keep-me");
  assert.equal(ndssOpp.notes, "pre-existing note");
  assert.equal(ndssOpp.prospectHotelFacing, true);
  assert.equal(ndssOpp.primaryContactPhoneType, "organization_helpline");
  assert.equal(isCustomerFacingOpportunity(ndssOpp), true);

  const a2 = await importReviewedProspectsForHotel({
    hotelId,
    packageDir,
    dryRun: false,
    backupDir: path.join(tmpRoot, "backups"),
  });
  assert.equal(a2.counts.create, 0);
  const ndss2 = a2.manifest.find((m) => m.cardId === "beth-ndss-2027");
  assert.equal(ndss2.opportunityId, ndss1.opportunityId);

  // CTN enrichment queue not hotel-facing
  const ctn = a2.manifest.find((m) => m.cardId === "beth-ctn-2027");
  assert.equal(ctn.hotelFacing, "NO");
  const ctnOpp = loadOpportunities(hotelId).opportunities.find(
    (o) => o.id === ctn.opportunityId
  );
  assert.equal(isReviewedProspectCustomerVisible(ctnOpp), false);
}

// Mayflower init + import
{
  const mayId = "rec0eTEnCqlsGJJt9";
  const may = await importReviewedProspectsForHotel({
    hotelId: mayId,
    packageDir,
    dryRun: false,
    backupDir: path.join(tmpRoot, "backups"),
  });
  assert.equal(may.ok, true);
  assert.equal(may.counts.priorityHotelFacing, 9);
  assert.ok(may.profileInit.created === true || may.profileInit.created === false);
  const mayDoc = loadOpportunities(mayId);
  assert.equal(mayDoc.opportunities.length, 15);
  assert.equal(may.counts.create, 15);
  const facing = mayDoc.opportunities.filter((o) =>
    isCustomerFacingOpportunity(o)
  );
  assert.equal(facing.length, 11);
  assert.equal(may.counts.priorityHotelFacing, 9);
  assert.equal(may.counts.exploratoryHotelFacing, 2);
}

console.log("test-gdi-reviewed-prospect-publication-v1: PASS");
console.log("tmpRoot", tmpRoot);
