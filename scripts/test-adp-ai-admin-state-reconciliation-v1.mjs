/**
 * test:adp-ai-admin-state-reconciliation-v1
 */
import assert from "node:assert/strict";
import { resolveAvaAdminHotelRow } from "../lib/ai-visibility-audit-action-plan/ava-admin-hotel-row-v1.js";

const PROPERTY_ID = "adp_radisson_blu_toronto_downtown";
const CORE =
  "adp_period_adp_radisson_blu_toronto_downtown_20261104094215_c8bd8b";

function baselineOk() {
  return {
    ok: true,
    baseline: {
      name: "Radisson Blu Toronto Downtown",
      market: "Toronto",
      periodId: CORE,
    },
  };
}

// Synthetic BASELINE_READY must not be used when meta exists with founder-reviewed + PDF
{
  const row = resolveAvaAdminHotelRow(PROPERTY_ID, {
    baseline: baselineOk(),
    meta: {
      propertyId: PROPERTY_ID,
      status: "FOUNDER_REVIEWED_STAGED",
      pdfStatus: "STAGED",
      acceptedRecommendationCount: 0,
      founderApprovedForCustomer: false,
      publicationAuthorizationStatus: "NOT_AUTHORIZED",
      adpBaselinePeriodId: CORE,
      versions: [
        {
          versionId: "v_old",
          actionCount: 5,
          pdfRelativePath: "reports/x.pdf",
        },
        {
          versionId: "v_latest",
          actionCount: 0,
          pdfRelativePath: "reports/x.pdf",
        },
      ],
      latestVersionId: "v_latest",
    },
  });
  assert.equal(row.auditStatus, "FOUNDER_REVIEWED_STAGED");
  assert.equal(row.pdfStatus, "STAGED");
  assert.equal(row.recommendationCount, 5, "must not show 0 when prior version has actions");
  assert.equal(row.measurementComplete, true);
  assert.equal(row.founderReviewed, true);
  assert.equal(row.customerPublicationAuthorized, false);
  assert.equal(row.adpBaselinePeriodId, CORE);
}

// Explicit empty meta (no store row) → baseline ready (not silent published)
{
  const row = resolveAvaAdminHotelRow(PROPERTY_ID, {
    baseline: baselineOk(),
    getMeta: () => null,
  });
  assert.equal(row.auditStatus, "BASELINE_READY");
  assert.equal(row.measurementComplete, true);
  assert.equal(row.pdfStatus, "MISSING");
  assert.equal(row.customerPublicationAuthorized, false);
}

console.log("PASS test:adp-ai-admin-state-reconciliation-v1");
