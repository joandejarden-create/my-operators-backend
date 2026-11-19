#!/usr/bin/env node
/**
 * Evidence-supported ADP Performance Review + PDF rebuild for founder-approved
 * recovery-wave hotels (REVIEW_REBUILDABLE / PDF_REBUILDABLE / REGISTRY_REPAIR).
 *
 * LIVE_PROVIDER_CALLS=0. Does not authorize customer publication or mint shares.
 */
import "dotenv/config";
import { generateNewDraft } from "../lib/ai-demand-positioning/monthly-review/admin/generate-review-v1.js";
import { renderAndAttachMonthlyReviewPdfV1 } from "../lib/ai-demand-positioning/monthly-review/admin/render-and-attach-monthly-review-pdf-v1.mjs";
import { buildPublishedAdpReviewCoverageV1 } from "../lib/ai-demand-positioning/monthly-review/admin/published-review-coverage-v1.js";
import { resolveAdpMonthlyReviewEligibilityV1 } from "../lib/ai-demand-positioning/monthly-review/resolve-adp-monthly-review-eligibility-v1.js";
import { assessOwnerAppReportArtifacts } from "../lib/ai-demand-positioning/share/adp-owner-app-admin-inventory-v1.js";

const BASE_URL = (process.env.ADP_PDF_BASE_URL || "http://localhost:8080").replace(
  /\/$/,
  ""
);

/** Founder Nov 19 recovery priority set — exclude synthetics. */
export const ADP_RECOVERY_WAVE_HOTELS_V1 = Object.freeze([
  {
    propertyId: "adp_mayflower_hotel_washington_dc",
    name: "The Mayflower Hotel, Autograph Collection",
  },
  {
    propertyId: "adp_charlotte_marriott_city_center",
    name: "Charlotte Marriott City Center",
  },
  {
    propertyId: "adp_falls_church_marriott_fairview_park",
    name: "Falls Church Marriott Fairview Park",
  },
  {
    propertyId: "adp_nobu_london_portman",
    name: "Nobu Hotel London Portman Square",
  },
  {
    propertyId: "adp_renaissance_arlington_capital_view",
    name: "Renaissance Arlington Capital View Hotel",
  },
  {
    propertyId: "adp_tampa_airport_marriott",
    name: "Tampa Airport Marriott",
  },
  {
    propertyId: "adp_fiesta_americana_condesa_cancun_ai",
    name: "Fiesta Americana Condesa Cancún All Inclusive",
  },
  {
    propertyId: "adp_grand_fiesta_americana_chapultepec",
    name: "Grand Fiesta Americana Chapultepec",
  },
  {
    propertyId: "adp_umusic_hotel_madrid",
    name: "UMusic Hotel Madrid",
  },
]);

const results = [];

for (const hotel of ADP_RECOVERY_WAVE_HOTELS_V1) {
  const propertyId = hotel.propertyId;
  const eligibility = resolveAdpMonthlyReviewEligibilityV1(propertyId);
  const artifacts = assessOwnerAppReportArtifacts(propertyId);
  const row = {
    propertyId,
    name: hotel.name,
    researchPeriodId:
      eligibility?.measurementPeriodId ||
      eligibility?.currentPeriodId ||
      artifacts.durablePeriodId ||
      null,
    eligible: eligibility?.eligible === true,
    draft: null,
    pdf: null,
    coverageAfter: null,
    error: null,
  };

  if (!row.eligible) {
    row.error = eligibility?.reason || eligibility?.blockReason || "not_eligible";
    results.push(row);
    continue;
  }

  try {
    process.stdout.write(`[adp-recovery] ${propertyId} draft… `);
    const draft = generateNewDraft(propertyId, {
      generatedBy: "adp_gdi_recovery_wave_v1",
      warnings: [
        "Rebuilt from durable published / validated baseline observations — no live provider calls",
        "Not customer-publication authorized by this rebuild alone",
        `researchPeriod=${row.researchPeriodId || "unknown"}`,
      ],
    });
    row.draft = {
      ok: draft.ok === true,
      reviewId: draft.newReviewId || null,
      error: draft.error || null,
      liveProviderCalls: draft.liveProviderCalls,
    };
    if (!draft.ok || !draft.newReviewId) {
      console.log(`FAIL ${draft.error?.message || "draft_failed"}`);
      results.push(row);
      continue;
    }
    console.log(`OK ${draft.newReviewId}`);

    process.stdout.write(`[adp-recovery] ${propertyId} pdf… `);
    const pdf = await renderAndAttachMonthlyReviewPdfV1({
      reviewId: draft.newReviewId,
      propertyId,
      baseUrl: BASE_URL,
    });
    row.pdf = {
      ok: pdf.ok === true,
      alreadyAttached: pdf.alreadyAttached === true,
      viewUrl: pdf.viewUrl || null,
      downloadUrl: pdf.downloadUrl || null,
      pdfBytes: pdf.pdfBytes || null,
      error: pdf.error || null,
    };
    console.log(
      pdf.ok
        ? `OK ${pdf.pdfBytes || "?"}b`
        : `FAIL ${pdf.error?.message || pdf.error || "pdf_failed"}`
    );
  } catch (err) {
    row.error = String(err?.message || err).slice(0, 400);
    console.log(`ERROR ${row.error}`);
  }
  results.push(row);
}

const cov = buildPublishedAdpReviewCoverageV1({});
const ids = new Set(ADP_RECOVERY_WAVE_HOTELS_V1.map((h) => h.propertyId));
for (const row of results) {
  const c = (cov.properties || []).find((p) => p.propertyId === row.propertyId);
  row.coverageAfter = c
    ? {
        status: c.coverageStatus,
        reason: c.coverageReason,
        pdfStatus: c.pdfStatus || null,
        reviewId: c.latestReviewId || c.reviewId || null,
      }
    : null;
}

const summary = {
  gate: "ADP_RECOVERY_WAVE_PERFORMANCE_REVIEW_REBUILD_V1",
  ok: results.every(
    (r) => r.draft?.ok && r.pdf?.ok && r.coverageAfter?.status === "READY"
  ),
  liveProviderCalls: 0,
  baseUrl: BASE_URL,
  rebuiltCount: results.filter((r) => r.draft?.ok && r.pdf?.ok).length,
  failedCount: results.filter((r) => !(r.draft?.ok && r.pdf?.ok)).length,
  publishedCount: cov.publishedCount,
  reviewReadyCount: cov.reviewReadyCount,
  hotels: results,
  remainingNeedsRebuild: (cov.properties || [])
    .filter((p) => p.coverageStatus === "NEEDS_REBUILD" && ids.has(p.propertyId))
    .map((p) => ({
      propertyId: p.propertyId,
      name: p.propertyName || p.name,
      reason: p.coverageReason,
    })),
};

console.log(JSON.stringify(summary, null, 2));
process.exit(summary.ok ? 0 : 1);
