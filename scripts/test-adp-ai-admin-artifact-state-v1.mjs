/**
 * test:adp-ai-admin-artifact-state-v1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  resolvePdfStatus,
  resolveRecommendationCount,
  resolveAvaAdminHotelRow,
} from "../lib/ai-visibility-audit-action-plan/ava-admin-hotel-row-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PROPERTY_ID = "adp_radisson_blu_toronto_downtown";

{
  assert.equal(
    resolvePdfStatus({ pdfStatus: "MISSING", pdfRelativePath: "reports/a.pdf" }),
    "STAGED"
  );
  assert.equal(
    resolvePdfStatus({
      pdfStatus: "MISSING",
      versions: [{ pdfRelativePath: "reports/a.pdf", actionCount: 3 }],
    }),
    "STAGED"
  );
  assert.equal(resolvePdfStatus({}), "MISSING");
  assert.equal(
    resolveRecommendationCount({
      versions: [
        { actionCount: 2 },
        { actionCount: 6 },
        { actionCount: 0 },
      ],
      latestVersionId: "x",
    }),
    6
  );
}

const currentPdf = path.join(
  ROOT,
  "reports/ai-visibility-audit-action-plan/archive",
  PROPERTY_ID,
  "current.pdf"
);
const namedPdf = path.join(
  ROOT,
  "reports/ai-visibility-audit-action-plan/archive",
  PROPERTY_ID,
  "Dealality - AI Visibility Audit & Action Plan - Radisson Blu Toronto Downtown - NOVEMBER2026.pdf"
);
const stdPdf = path.join(
  ROOT,
  "reports/ai-demand-positioning/current-report-pdf",
  PROPERTY_ID,
  "report.pdf"
);

assert.ok(
  fs.existsSync(currentPdf) || fs.existsSync(namedPdf),
  "Toronto AVA current PDF must exist"
);
assert.ok(fs.existsSync(stdPdf), "Toronto Standard ADP PDF must exist");

const row = resolveAvaAdminHotelRow(PROPERTY_ID, {
  baseline: {
    ok: true,
    baseline: {
      name: "Radisson Blu Toronto Downtown",
      market: "Toronto",
      periodId:
        "adp_period_adp_radisson_blu_toronto_downtown_20261104094215_c8bd8b",
    },
  },
  meta: {
    propertyId: PROPERTY_ID,
    status: "FOUNDER_REVIEWED_STAGED",
    pdfStatus: "STAGED",
    pdfRelativePath: fs.existsSync(currentPdf)
      ? path.relative(ROOT, currentPdf).replace(/\\/g, "/")
      : path.relative(ROOT, namedPdf).replace(/\\/g, "/"),
    versions: [
      {
        versionId: "v1",
        actionCount: 5,
        pdfRelativePath: "reports/ai-visibility-audit-action-plan/archive/adp_radisson_blu_toronto_downtown/current.pdf",
      },
    ],
    latestVersionId: "v1",
    founderApprovedForCustomer: false,
  },
});

assert.equal(row.pdfStatus, "STAGED");
assert.equal(row.auditArtifactAvailable, true);
assert.ok(row.recommendationCount >= 5);

console.log("PASS test:adp-ai-admin-artifact-state-v1");
