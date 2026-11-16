/**
 * test:adp-ai-admin-lifecycle-separation-v1
 *
 * Measurement complete / PDF available / founder reviewed / customer auth
 * must remain four distinct concepts.
 */
import assert from "node:assert/strict";
import { resolveAvaAdminHotelRow } from "../lib/ai-visibility-audit-action-plan/ava-admin-hotel-row-v1.js";

const PROPERTY_ID = "adp_radisson_blu_toronto_downtown";
const CORE =
  "adp_period_adp_radisson_blu_toronto_downtown_20261104094215_c8bd8b";

const row = resolveAvaAdminHotelRow(PROPERTY_ID, {
  baseline: {
    ok: true,
    baseline: {
      name: "Radisson Blu Toronto Downtown",
      market: "Toronto",
      periodId: CORE,
    },
  },
  meta: {
    propertyId: PROPERTY_ID,
    status: "FOUNDER_REVIEWED_STAGED",
    pdfStatus: "STAGED",
    pdfRelativePath: "reports/x.pdf",
    versions: [{ versionId: "v1", actionCount: 5, pdfRelativePath: "reports/x.pdf" }],
    latestVersionId: "v1",
    founderApprovedForCustomer: false,
    publicationAuthorizationStatus: "NOT_AUTHORIZED",
    customerReadinessStatus: "NOT_CUSTOMER_READY",
  },
});

assert.equal(row.measurementComplete, true);
assert.equal(row.auditArtifactAvailable, true);
assert.equal(row.founderReviewed, true);
assert.equal(row.customerPublicationAuthorized, false);
assert.equal(row.publicationAuthorizationStatus, "NOT_AUTHORIZED");
assert.equal(row.auditStatus, "FOUNDER_REVIEWED_STAGED");

// Explicit founder approval flips authorization without requiring PUBLISHED conflation
const approved = resolveAvaAdminHotelRow(PROPERTY_ID, {
  baseline: {
    ok: true,
    baseline: { name: "Radisson Blu Toronto Downtown", market: "Toronto", periodId: CORE },
  },
  meta: {
    propertyId: PROPERTY_ID,
    status: "FOUNDER_APPROVED_FOR_CUSTOMER",
    pdfStatus: "STAGED",
    versions: [{ versionId: "v1", actionCount: 5, pdfRelativePath: "reports/x.pdf" }],
    latestVersionId: "v1",
    founderApprovedForCustomer: true,
    publicationAuthorizationStatus: "AUTHORIZED",
  },
});
assert.equal(approved.founderReviewed, true);
assert.equal(approved.customerPublicationAuthorized, true);
assert.notEqual(approved.auditStatus, "FOUNDER_REVIEWED_STAGED");

console.log("PASS test:adp-ai-admin-lifecycle-separation-v1");
