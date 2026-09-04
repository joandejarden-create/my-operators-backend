/**
 * BAI customer longitudinal payload + promotion-preview helpers.
 *
 * Builds customer-safe Wave 3/4 longitudinal fields for:
 * - INTERNAL CUSTOMER_PROMOTION_PREVIEW (prep only)
 * - Future CUSTOMER_PUBLISHED when published current has a comparable prior
 *
 * Does not promote Period 2. Does not mutate publication pointers.
 */

import {
  BAI_VIEW_MODE,
  BAI_PERIOD_2_CANDIDATE_ID,
  BAI_PERIOD_2_CUSTOMER_CURRENT_DATE,
  BAI_P2_CUSTOMER_PRIOR_PERIOD_ID,
  BAI_P2_CUSTOMER_PRIOR_DATE,
  BAI_CUSTOMER_PUBLISHED_PERIOD_ID,
  BAI_CUSTOMER_PUBLISHED_DATE,
  BAI_CUSTOMER_ROLLBACK_PERIOD_ID,
  BAI_CUSTOMER_ROLLBACK_DATE,
  BAI_PERIOD_1_INTERNAL_HISTORY_DATE,
  resolveBaiPriorComparablePeriodV1,
  getBaiPeriodPublicationState,
  dryRunBaiP2PromotionMechanicsV1,
} from "./resolve-bai-prior-comparable-period-v1.js";
import { buildBaiWave4LongitudinalPresentationV1 } from "./bai-wave4-longitudinal-presentation-v1.js";
import { buildBaiWave3FullCohortReconciliationV1 } from "./bai-wave3-longitudinal-intelligence-v1.js";

export const BAI_CUSTOMER_LONGITUDINAL_PAYLOAD_READY =
  "BAI_CUSTOMER_LONGITUDINAL_PAYLOAD_READY";
export const BAI_CUSTOMER_TRENDS_SURFACE_READY =
  "BAI_CUSTOMER_TRENDS_SURFACE_READY";
export const BAI_CUSTOMER_ABSOLUTE_RELATIVE_SURFACE_READY =
  "BAI_CUSTOMER_ABSOLUTE_RELATIVE_SURFACE_READY";
export const BAI_P2_CUSTOMER_EVIDENCE_BIND_READY =
  "BAI_P2_CUSTOMER_EVIDENCE_BIND_READY";
export const BAI_PROMOTION_PREVIEW_INTERNAL_ONLY =
  "BAI_PROMOTION_PREVIEW_INTERNAL_ONLY";
export const BAI_PROMOTION_PREP_NO_CURRENT_PUBLICATION_MUTATION =
  "BAI_PROMOTION_PREP_NO_CURRENT_PUBLICATION_MUTATION";
export const BAI_P2_CUSTOMER_VISUAL_CONTRACT_READY =
  "BAI_P2_CUSTOMER_VISUAL_CONTRACT_READY";
export const BAI_P2_CUSTOMER_19_BRAND_RECONCILIATION =
  "BAI_P2_CUSTOMER_19_BRAND_RECONCILIATION";
export const BAI_P2_EXPECTED_CUSTOMER_FINGERPRINT_READY =
  "BAI_P2_EXPECTED_CUSTOMER_FINGERPRINT_READY";
export const BAI_P2_PROMOTION_ROLLBACK_READY =
  "BAI_P2_PROMOTION_ROLLBACK_READY";

const BAI_P2_NO_MIXED_PRIOR = "BAI_P2_NO_MIXED_PRIOR_DATE_IDENTITY";

const FORBIDDEN_CUSTOMER_TERMS = [
  /period\s*2/i,
  /\bcandidate\b/i,
  /internal\s*qa/i,
  /wave\s*[34]/i,
  /unpromoted/i,
  /2026-08-18/,
  /aiv_brand_longitudinal_period_20260818/,
];

function scrubCustomerText(text) {
  if (text == null) return text;
  let out = String(text);
  // Soft-scrub: remove internal labels if somehow present
  out = out.replace(/\bPeriod\s*2\b/gi, "this monitoring run");
  out = out.replace(/\bcandidate\b/gi, "monitoring");
  out = out.replace(/\binternal\s*QA\b/gi, "monitoring");
  return out;
}

function textIsCustomerSafe(text) {
  const s = String(text || "");
  return !FORBIDDEN_CUSTOMER_TERMS.some((re) => re.test(s));
}

/**
 * Evidence period binding for hypothetical / future P2 customer current.
 */
export function resolveBaiCustomerEvidencePeriodBindingV1(opts = {}) {
  const currentPeriodId =
    opts.currentPeriodId || BAI_PERIOD_2_CANDIDATE_ID;
  const priorPeriodId =
    opts.priorPeriodId || BAI_P2_CUSTOMER_PRIOR_PERIOD_ID;
  return {
    ok: true,
    gate: BAI_P2_CUSTOMER_EVIDENCE_BIND_READY,
    currentEvidencePeriodId: currentPeriodId,
    currentEvidenceDate:
      opts.currentPeriodDate || BAI_PERIOD_2_CUSTOMER_CURRENT_DATE,
    priorEvidencePeriodId: priorPeriodId,
    priorEvidenceDate: opts.priorPeriodDate || BAI_P2_CUSTOMER_PRIOR_DATE,
    forbidAug14AsCurrent: priorPeriodId === BAI_P2_CUSTOMER_PRIOR_PERIOD_ID,
    note:
      "When P2 is promoted, current evidence must resolve from the P2 period tree; prior evidence (if shown) from Aug 14 only.",
  };
}

/**
 * Attach longitudinal when published current has a comparable prior.
 * Today: CUSTOMER_PUBLISHED has no prior → null (production unchanged).
 */
export function buildBaiCustomerLongitudinalAttachmentIfReady(opts = {}) {
  const viewMode = opts.viewMode || BAI_VIEW_MODE.CUSTOMER_PUBLISHED;
  const periodResolve =
    opts.periodResolve ||
    resolveBaiPriorComparablePeriodV1({ viewMode, geography: opts.geography });

  if (
    viewMode === BAI_VIEW_MODE.CUSTOMER_PUBLISHED &&
    (!periodResolve.comparable || !periodResolve.priorPeriodId)
  ) {
    return {
      ok: true,
      attached: false,
      reason: "customer_published_has_no_promoted_prior",
      gate: BAI_CUSTOMER_LONGITUDINAL_PAYLOAD_READY,
      customerLongitudinal: null,
      PERIOD_2_PUBLICATION_STATE:
        BAI_CUSTOMER_PUBLISHED_PERIOD_ID === BAI_PERIOD_2_CANDIDATE_ID
          ? "PUBLISHED"
          : "UNPROMOTED",
      LIVE_MUTATION: false,
    };
  }

  return buildBaiCustomerLongitudinalPayloadV1({
    ...opts,
    viewMode:
      viewMode === BAI_VIEW_MODE.CUSTOMER_PUBLISHED
        ? BAI_VIEW_MODE.CUSTOMER_PROMOTION_PREVIEW
        : viewMode,
    periodResolve,
  });
}

/**
 * Customer-safe longitudinal payload from Wave 4 (preview or future publish).
 */
export function buildBaiCustomerLongitudinalPayloadV1(opts = {}) {
  const viewMode =
    opts.viewMode || BAI_VIEW_MODE.CUSTOMER_PROMOTION_PREVIEW;
  const parentRaw = String(opts.parentCompanyName || opts.parent || "all").trim();
  const wantsFull =
    opts.scope === "full_cohort" ||
    !parentRaw ||
    /^(all|\*|full|cohort)$/i.test(parentRaw);

  const wave4 = buildBaiWave4LongitudinalPresentationV1({
    viewMode:
      viewMode === BAI_VIEW_MODE.CUSTOMER_PUBLISHED
        ? BAI_VIEW_MODE.CUSTOMER_PROMOTION_PREVIEW
        : viewMode,
    scope: wantsFull ? "full_cohort" : "parent_filter",
    parentCompanyName: wantsFull ? "all" : parentRaw,
    geography: opts.geography || "CALA",
    storeRoot: opts.storeRoot,
  });

  if (!wave4.ok) {
    return {
      ok: false,
      attached: false,
      gate: BAI_CUSTOMER_LONGITUDINAL_PAYLOAD_READY,
      wave4,
      customerLongitudinal: null,
    };
  }

  const periodResolve = wave4.periodResolve;
  const evidenceBind = resolveBaiCustomerEvidencePeriodBindingV1(periodResolve);

  const parents = (wave4.parents || []).map((p) => {
    const execNarrative = scrubCustomerText(p.executiveRead?.narrative);
    const competitiveNarrative = scrubCustomerText(
      p.competitive?.story?.narrative
    );
    return {
      parentCompanyKey: p.parentCompanyKey,
      parentCompanyName: p.parentCompanyName,
      currentDate: periodResolve.currentPeriodDate,
      priorDate: periodResolve.priorPeriodDate,
      portfolio: p.portfolio,
      priorRun: {
        available: true,
        currentPresence: p.portfolio?.currentPresence,
        priorPresence: p.portfolio?.priorPresence,
        deltaDisplay: p.portfolio?.deltaDisplay,
        currentDate: periodResolve.currentPeriodDate,
        priorDate: periodResolve.priorPeriodDate,
        visualPriority: p.portfolio?.visualPriority,
      },
      trend: {
        ...p.trend,
        surfaceReady: true,
        gate: BAI_CUSTOMER_TRENDS_SURFACE_READY,
      },
      absoluteRelative: {
        ...p.absoluteRelativeVisual,
        surfaceReady: true,
        gate: BAI_CUSTOMER_ABSOLUTE_RELATIVE_SURFACE_READY,
      },
      brandMovement: p.brandMovement,
      competitive: {
        ...p.competitive,
        story: {
          ...p.competitive?.story,
          narrative: competitiveNarrative,
        },
      },
      provider: {
        ...p.provider,
        // Customer: current-only table + disclosure (no blank Δ columns)
        showPriorDeltaColumns: false,
        rows: (p.provider?.rows || []).map((r) => ({
          provider: r.provider,
          providerLabel: r.providerLabel,
          currentPresence: r.currentPresence,
          observationCount: r.observationCount,
        })),
      },
      ownerIntent: p.ownerIntent,
      disclosures: p.disclosures,
      executiveRead: {
        available: true,
        narrative: execNarrative,
        customerSafe: textIsCustomerSafe(execNarrative),
      },
      evidenceBind,
    };
  });

  const allSafe = parents.every(
    (p) =>
      p.executiveRead.customerSafe &&
      textIsCustomerSafe(p.competitive?.story?.narrative) &&
      !(p.trend?.points || []).some((pt) =>
        String(pt.label || "").includes(BAI_PERIOD_1_INTERNAL_HISTORY_DATE)
      )
  );

  return {
    ok: wave4.ok && allSafe,
    attached: true,
    gate: BAI_CUSTOMER_LONGITUDINAL_PAYLOAD_READY,
    previewMode: viewMode === BAI_VIEW_MODE.CUSTOMER_PROMOTION_PREVIEW,
    PERIOD_2_PUBLICATION_STATE:
      BAI_CUSTOMER_PUBLISHED_PERIOD_ID === BAI_PERIOD_2_CANDIDATE_ID
        ? "PUBLISHED"
        : "UNPROMOTED",
    LIVE_PROVIDER_CALLS: 0,
    LIVE_MUTATION: false,
    publicationState: getBaiPeriodPublicationState(
      periodResolve.currentPeriodId
    ),
    periodResolve: {
      currentPeriodId: periodResolve.currentPeriodId,
      currentPeriodDate: periodResolve.currentPeriodDate,
      priorPeriodId: periodResolve.priorPeriodId,
      priorPeriodDate: periodResolve.priorPeriodDate,
      comparable: periodResolve.comparable,
      methodologyLineage: periodResolve.methodologyLineage,
      cohortChanged: periodResolve.cohortChanged,
      cohortChangeState: periodResolve.cohortChangeState,
      cohortChangeDisclosure: periodResolve.cohortChangeDisclosure,
      publicationState: periodResolve.publicationState,
    },
    evidenceBind,
    parents,
    cohortBrandCount: wave4.cohortBrandCount,
    wave4Gates: wave4.gates,
    displayReconciliation: wave4.displayReconciliation,
    gates: {
      [BAI_CUSTOMER_LONGITUDINAL_PAYLOAD_READY]: wave4.ok && allSafe,
      [BAI_CUSTOMER_TRENDS_SURFACE_READY]: parents.every(
        (p) => p.trend?.chartMode === "LINE" && p.trend?.points?.length === 2
      ),
      [BAI_CUSTOMER_ABSOLUTE_RELATIVE_SURFACE_READY]: parents.every((p) =>
        (p.brandMovement?.rows || []).every(
          (r) => r.absoluteLabel && r.relativeLabel
        )
      ),
      [BAI_P2_CUSTOMER_EVIDENCE_BIND_READY]: evidenceBind.ok,
      [BAI_P2_CUSTOMER_VISUAL_CONTRACT_READY]: wave4.ok && allSafe,
      [BAI_P2_NO_MIXED_PRIOR]: parents.every((p) => {
        const labels = (p.trend?.points || []).map((pt) => String(pt.label));
        return (
          labels.includes("2026-08-14") &&
          labels.includes("2026-09-03") &&
          !labels.some((l) => l.includes("2026-08-18"))
        );
      }),
    },
    customerLongitudinal: {
      available: true,
      mode: viewMode,
      currentDate: periodResolve.currentPeriodDate,
      priorDate: periodResolve.priorPeriodDate,
      parents,
      evidenceBind,
    },
  };
}

/**
 * Full promotion-preview package for one parent or full cohort.
 */
export function buildBaiCustomerPromotionPreviewV1(opts = {}) {
  const payload = buildBaiCustomerLongitudinalPayloadV1({
    ...opts,
    viewMode: BAI_VIEW_MODE.CUSTOMER_PROMOTION_PREVIEW,
  });
  const wave3 = buildBaiWave3FullCohortReconciliationV1({
    viewMode: BAI_VIEW_MODE.CUSTOMER_PROMOTION_PREVIEW,
    geography: opts.geography || "CALA",
    storeRoot: opts.storeRoot,
  });
  const livePublished = resolveBaiPriorComparablePeriodV1({
    viewMode: BAI_VIEW_MODE.CUSTOMER_PUBLISHED,
  });

  return {
    ...payload,
    accessClass: "INTERNAL_CUSTOMER_PROMOTION_PREVIEW",
    SHARE_CAPABILITY_FORBIDDEN: true,
    gate: BAI_PROMOTION_PREVIEW_INTERNAL_ONLY,
    liveCustomerUnchanged: {
      gate: BAI_PROMOTION_PREP_NO_CURRENT_PUBLICATION_MUTATION,
      currentPeriodId: livePublished.currentPeriodId,
      currentPeriodDate: livePublished.currentPeriodDate,
      priorPeriodId: livePublished.priorPeriodId,
      period2Exposed: livePublished.period2Exposed === true,
      matchesBaseline:
        livePublished.currentPeriodId === BAI_CUSTOMER_ROLLBACK_PERIOD_ID &&
        livePublished.currentPeriodDate === BAI_CUSTOMER_ROLLBACK_DATE &&
        livePublished.priorPeriodId == null,
      matchesPublishedP2:
        livePublished.currentPeriodId === BAI_PERIOD_2_CANDIDATE_ID &&
        livePublished.currentPeriodDate === BAI_PERIOD_2_CUSTOMER_CURRENT_DATE &&
        livePublished.priorPeriodId === BAI_P2_CUSTOMER_PRIOR_PERIOD_ID,
    },
    reconciliation19: {
      gate: BAI_P2_CUSTOMER_19_BRAND_RECONCILIATION,
      ok: wave3.ok && wave3.matrix?.length === 19,
      brandCount: wave3.matrix?.length || 0,
      matrix: wave3.matrix || [],
    },
    promotionDryRun: dryRunBaiP2PromotionMechanicsV1(),
    rollback: {
      gate: BAI_P2_PROMOTION_ROLLBACK_READY,
      restoreCurrentPeriodId: BAI_CUSTOMER_ROLLBACK_PERIOD_ID,
      restoreCurrentDate: BAI_CUSTOMER_ROLLBACK_DATE,
      keepP2History: true,
      deleteP2Forbidden: true,
    },
  };
}

/**
 * Overlay published P2 longitudinal onto a customer executive-summary payload.
 * Parent-scoped when parentCompanyKey provided.
 */
export function applyBaiPublishedLongitudinalToCustomerPayload(payload, opts = {}) {
  const parentKey = String(opts.parentCompanyKey || opts.parent || "").trim().toLowerCase();
  const attach = buildBaiCustomerLongitudinalAttachmentIfReady({
    viewMode: BAI_VIEW_MODE.CUSTOMER_PUBLISHED,
    geography: opts.geography || "CALA",
    parentCompanyName: parentKey || "all",
    scope: parentKey ? "parent_filter" : "full_cohort",
    storeRoot: opts.storeRoot,
  });

  if (!attach.attached || !attach.customerLongitudinal) {
    return {
      ...payload,
      customerLongitudinal: null,
      customerLongitudinalAttached: false,
      customerLongitudinalAttachReason: attach.reason || attach.wave4?.reason || null,
      PERIOD_2_PUBLICATION_STATE: attach.PERIOD_2_PUBLICATION_STATE,
      evidenceBind: null,
    };
  }

  const parents = attach.customerLongitudinal.parents || [];
  const pv =
    (parentKey &&
      parents.find((p) => p.parentCompanyKey === parentKey)) ||
    parents[0] ||
    null;

  const next = { ...payload };
  const freshness = { ...(payload.monitoringFreshness || {}) };
  freshness.LAST_MONITORED_AT = `${BAI_PERIOD_2_CUSTOMER_CURRENT_DATE}T12:00:00.000Z`;
  freshness.LAST_MONITORED_DISPLAY = "Sep 3, 2026";
  freshness.PUBLISHED_MEASUREMENT_PERIOD_ID = BAI_PERIOD_2_CANDIDATE_ID;
  freshness.PRIOR_RUN_DATE = BAI_P2_CUSTOMER_PRIOR_DATE;
  next.monitoringFreshness = freshness;

  if (pv?.portfolio && next.currentPosition?.portfolioAiPresence) {
    const pct = pv.portfolio.currentPresence;
    next.currentPosition = {
      ...next.currentPosition,
      portfolioAiPresence: {
        ...next.currentPosition.portfolioAiPresence,
        display:
          pct != null && Number.isFinite(Number(pct))
            ? `${Number(pct).toFixed(1)}%`
            : next.currentPosition.portfolioAiPresence.display,
        value: pct != null ? Number(pct) / 100 : next.currentPosition.portfolioAiPresence.value,
        helper:
          "Portfolio AI Presence for the current published monitoring run (2026-09-03).",
      },
    };
  }

  // Prefer published longitudinal Executive Read narrative when available
  if (pv?.executiveRead?.narrative) {
    next.baiPublishedExecutiveLongitudinal = {
      narrative: pv.executiveRead.narrative,
      currentDate: BAI_PERIOD_2_CUSTOMER_CURRENT_DATE,
      priorDate: BAI_P2_CUSTOMER_PRIOR_DATE,
    };
  }

  next.customerLongitudinal = {
    ...attach.customerLongitudinal,
    parents: parentKey && pv ? [pv] : parents,
  };
  next.customerLongitudinalAttached = true;
  next.PERIOD_2_PUBLICATION_STATE = "PUBLISHED";
  next.evidenceBind = attach.evidenceBind || pv?.evidenceBind || null;
  next.publicationDates = {
    currentDate: BAI_PERIOD_2_CUSTOMER_CURRENT_DATE,
    priorDate: BAI_P2_CUSTOMER_PRIOR_DATE,
    currentPeriodId: BAI_PERIOD_2_CANDIDATE_ID,
    priorPeriodId: BAI_P2_CUSTOMER_PRIOR_PERIOD_ID,
  };
  return next;
}

export function assertBaiPromotionPreviewNotOnShare(reqLike = {}) {
  if (reqLike.baiShare || reqLike.baiShareAuth?.mode === "SHARE_CAPABILITY") {
    return {
      ok: false,
      gate: BAI_PROMOTION_PREVIEW_INTERNAL_ONLY,
      reason: "share_cannot_invoke_promotion_preview",
    };
  }
  if (reqLike.query?.baiPromotionPreview && reqLike.baiShare) {
    return {
      ok: false,
      gate: BAI_PROMOTION_PREVIEW_INTERNAL_ONLY,
      reason: "share_query_preview_forbidden",
    };
  }
  return { ok: true, gate: BAI_PROMOTION_PREVIEW_INTERNAL_ONLY };
}
