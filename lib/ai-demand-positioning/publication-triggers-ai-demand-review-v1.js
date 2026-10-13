/**
 * Non-blocking enqueue after ADP publication.
 * Doctrine:
 *   ADP_PUBLICATION_TRIGGERS_AI_DEMAND_REVIEW_ARTIFACT
 *   AI_DEMAND_PERFORMANCE_REVIEW_AND_ACTION_PLAN_ATOMIC_GENERATION
 *
 * Creates/refreshes AI Demand Performance Review draft from current published ADP.
 * PDF render remains a separate step. Failures must not block publication.
 */

import { resolveAdpMonthlyReviewEligibilityV1 } from "./monthly-review/resolve-adp-monthly-review-eligibility-v1.js";
import { generateNewDraft } from "./monthly-review/admin/generate-review-v1.js";
import { buildPublishedAdpReviewCoverageV1 } from "./monthly-review/admin/published-review-coverage-v1.js";

export const ADP_PUBLICATION_TRIGGERS_AI_DEMAND_REVIEW_ARTIFACT =
  "ADP_PUBLICATION_TRIGGERS_AI_DEMAND_REVIEW_ARTIFACT";
export const AI_DEMAND_PERFORMANCE_REVIEW_AND_ACTION_PLAN_ATOMIC_GENERATION =
  "AI_DEMAND_PERFORMANCE_REVIEW_AND_ACTION_PLAN_ATOMIC_GENERATION";

/**
 * @param {string} propertyId
 * @param {{ generatedBy?: string }} [opts]
 */
export function enqueueAiDemandReviewAfterPublication(propertyId, opts = {}) {
  const startedAt = new Date().toISOString();
  try {
    const coverage = buildPublishedAdpReviewCoverageV1();
    const row = coverage.properties.find((p) => p.propertyId === propertyId);
    if (row?.coverageStatus === "READY" && row?.hasPdf) {
      return {
        ok: true,
        skipped: true,
        reason: "performance_review_already_ready",
        gate: ADP_PUBLICATION_TRIGGERS_AI_DEMAND_REVIEW_ARTIFACT,
        atomicGate: AI_DEMAND_PERFORMANCE_REVIEW_AND_ACTION_PLAN_ATOMIC_GENERATION,
        propertyId,
        startedAt,
        actionCount: row.actionCount || 0,
      };
    }
    const eligibility = resolveAdpMonthlyReviewEligibilityV1(propertyId);
    if (!eligibility.eligible) {
      return {
        ok: true,
        skipped: true,
        reason: "review_blocked",
        blockers: eligibility.blockers || [],
        coverageStatus: "BLOCKED",
        gate: ADP_PUBLICATION_TRIGGERS_AI_DEMAND_REVIEW_ARTIFACT,
        propertyId,
        startedAt,
      };
    }
    const gen = generateNewDraft(propertyId, {
      generatedBy: opts.generatedBy || "adp-publication-hook",
    });
    return {
      ok: Boolean(gen.ok),
      skipped: false,
      reviewId: gen.newReviewId || null,
      error: gen.error || null,
      coverageStatus: gen.ok ? "NEEDS_REBUILD" : "BLOCKED",
      note: gen.ok
        ? "Performance Review draft created with Management Action Agenda; PDF render is a separate admin step."
        : "Review generation failed — publication still succeeded.",
      gate: ADP_PUBLICATION_TRIGGERS_AI_DEMAND_REVIEW_ARTIFACT,
      atomicGate: AI_DEMAND_PERFORMANCE_REVIEW_AND_ACTION_PLAN_ATOMIC_GENERATION,
      propertyId,
      startedAt,
      liveProviderCalls: 0,
    };
  } catch (err) {
    console.error(
      "[ADP publication→Performance Review] non-blocking failure:",
      err && err.message ? err.message : err
    );
    return {
      ok: false,
      skipped: false,
      error: err.message || String(err),
      coverageStatus: "BLOCKED",
      gate: ADP_PUBLICATION_TRIGGERS_AI_DEMAND_REVIEW_ARTIFACT,
      propertyId,
      startedAt,
      liveProviderCalls: 0,
    };
  }
}
