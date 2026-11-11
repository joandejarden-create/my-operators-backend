/**
 * ADP_EXECUTIVE_REPORT_PDF_BETHESDA_MASTER_V1
 *
 * Design authority: Bethesda Marriott 8-page AI Demand Performance Review
 * (canonical monthly-review PDF template / Dealality report family).
 * Data authority: certified staged Full ADP owner payload.
 *
 * Distinct from:
 * - Interactive owner ADP SPA (dark dashboard)
 * - Legacy Monthly Executive Review archive rows (historical dossier)
 */

import { buildMonthlyExecutiveReviewV1 } from "../monthly-review/build-monthly-executive-review-v1.js";
import { loadStagedOwnerPayload } from "../review/adp-staged-report-store-v1.js";
import {
  ADP_EXECUTIVE_PDF_FOUNDER_DESIGN_ACCEPTANCE_V1,
  PDF_FOUNDER_DESIGN_ACCEPTANCE_PENDING,
} from "./adp-executive-pdf-founder-design-acceptance-v1.js";

export const ADP_EXECUTIVE_REPORT_PDF_BETHESDA_MASTER_V1 =
  "ADP_EXECUTIVE_REPORT_PDF_BETHESDA_MASTER_V1";
export const ADP_EXECUTIVE_PDF_BETHESDA_MASTER_FIDELITY_V1 =
  "ADP_EXECUTIVE_PDF_BETHESDA_MASTER_FIDELITY_V1";

/**
 * @deprecated Do not use for new Full ADP E2E PDFs.
 * Founder directive: Full ADP hotels must use the same cover product label as the
 * original 31-hotel portfolio (MONTHLY EXECUTIVE REVIEW), not a parallel subtitle.
 */
export const FULL_ADP_EXECUTIVE_COVER_SUBTITLE = "FULL ADP EXECUTIVE REPORT";

/** Portfolio-canonical cover product label (Bethesda / Mayflower / Westin / …). */
export const PORTFOLIO_CANONICAL_COVER_SUBTITLE = "MONTHLY EXECUTIVE REVIEW";

/**
 * Prefer certified positive signals over generic "Still developing" for Full ADP PDFs.
 * Does not invent metrics — only re-ranks existing staged evidence.
 */
export function resolveFullAdpExecutiveStrengthCardV1(stagedPayload, currentCard) {
  const name =
    stagedPayload?.property?.name ||
    stagedPayload?.propertyProfile?.name ||
    "This hotel";
  const headline = String(currentCard?.headline || "").trim();
  const isWeak =
    !headline ||
    /^still developing$/i.test(headline) ||
    /does not yet show a single dominant strength/i.test(
      String(currentCard?.body || "")
    );
  if (!isWeak && currentCard) return currentCard;

  const bpp = stagedPayload?.brandPortfolioPosition || {};
  const rankKpi = (bpp.kpis || []).find(
    (k) => k.id === "portfolioRank" && k.available !== false
  );
  const presenceKpi = (bpp.kpis || []).find(
    (k) => k.id === "portfolioAiPresence" && k.available !== false
  );
  const rankRaw = Number(rankKpi?.valueRaw);
  const rankDisplay = String(rankKpi?.value || "").trim();
  const presenceDisplay = String(presenceKpi?.value || "").trim();

  if (rankRaw === 1 && rankDisplay) {
    return {
      label: "BIGGEST STRENGTH",
      sectionLabel: "BIGGEST STRENGTH",
      headline: "Leads its brand portfolio peer set in AI presence",
      body: [
        `${name} ranks ${rankDisplay}`,
        presenceDisplay
          ? `with portfolio AI presence of ${presenceDisplay}`
          : null,
        "among the certified brand-portfolio peers for this period.",
        "This is the clearest positive portfolio signal in the staged report — not a claim about market-wide competitive share.",
      ]
        .filter(Boolean)
        .join(" "),
      source: "full_adp_executive_strength_from_bpp_rank",
    };
  }

  const ranking = stagedPayload?.executiveMetrics?.ranking || {};
  const top3 = Number(ranking.topThreeAppearanceRate);
  if (Number.isFinite(top3) && top3 >= 50) {
    return {
      label: "BIGGEST STRENGTH",
      sectionLabel: "BIGGEST STRENGTH",
      headline: "Often placed near the top when AI ranks hotels",
      body: `Among AI answers where hotel ranking could be measured, ${name} appeared in the Top 3 ${top3}% of the time. This shows how prominently the hotel is positioned when AI includes it.`,
      source: "full_adp_executive_strength_from_top3",
    };
  }

  const consideration = Number(
    stagedPayload?.executiveMetrics?.considerationRate?.rate
  );
  const scenarioPresence = Number(
    stagedPayload?.executiveMetrics?.scenarioPresence?.rate
  );
  if (Number.isFinite(scenarioPresence) && scenarioPresence >= 30) {
    return {
      label: "BIGGEST STRENGTH",
      sectionLabel: "BIGGEST STRENGTH",
      headline: "Recognized across a meaningful share of traveler needs",
      body: [
        `${name} appeared in at least one AI answer for ${scenarioPresence}% of monitored traveler-need scenarios`,
        Number.isFinite(consideration)
          ? `(AI Consideration ${consideration}% across individual answers)`
          : null,
        ". That reach is a usable commercial starting point even while consistency and competitive displacement remain constraints.",
      ]
        .filter(Boolean)
        .join(""),
      source: "full_adp_executive_strength_from_scenario_presence",
    };
  }

  return (
    currentCard || {
      label: "BIGGEST STRENGTH",
      sectionLabel: "BIGGEST STRENGTH",
      headline: "Still developing",
      body: `${name} does not yet show a single dominant strength with enough comparable evidence in this period.`,
    }
  );
}

/**
 * Surface Core competitor count + competitor-present displacement fraction (e.g. 49/63).
 */
export function enrichFullAdpCompetitiveMovementV1(review, stagedPayload) {
  const comp = { ...(review.competitiveMovement || {}) };
  const observedCount =
    stagedPayload?.competitiveSet?.observedCount ??
    stagedPayload?.competitiveSet?.observed?.length ??
    null;
  const totalScenarios =
    stagedPayload?.demandCapture?.totalScenarios ??
    stagedPayload?.period?.scenarioCount ??
    stagedPayload?.executiveMetrics?.scenarioPresence?.eligibleScenarios ??
    null;
  const competitorPresent =
    stagedPayload?.executiveMetrics?.competitorPresentScenarios?.scenarioCount ??
    null;

  const contextLines = [];
  if (observedCount != null) {
    contextLines.push(
      `${observedCount} Core competitors identified in the certified competitive set for this period.`
    );
    comp.coreCompetitorCount = observedCount;
  }
  if (competitorPresent != null && totalScenarios != null) {
    contextLines.push(
      `Competitors appeared in ${competitorPresent} of ${totalScenarios} monitored scenarios when measuring competitor-present displacement (observational co-appearance / absence patterns — not causation).`
    );
    comp.competitorPresentScenarios = competitorPresent;
    comp.totalScenarios = totalScenarios;
    comp.competitiveDisplacementDisplay = `${competitorPresent}/${totalScenarios}`;
  }

  if (contextLines.length) {
    const prefix = contextLines.join(" ");
    comp.summary = comp.summary ? `${prefix} ${comp.summary}` : prefix;
    comp.executiveContext = prefix;
  }

  review.competitiveMovement = comp;

  const insights = { ...(review.sectionInsights || {}) };
  const existing = insights.competitiveMovement?.text || "";
  if (contextLines.length && !/Core competitors identified/i.test(existing)) {
    insights.competitiveMovement = {
      ...(insights.competitiveMovement || {}),
      text: [contextLines.join(" "), existing].filter(Boolean).join(" "),
      source: "full_adp_executive_competitive_context_v1",
    };
    review.sectionInsights = insights;
  }
  return review;
}

function applyFullAdpStrengthOverride(review, stagedPayload) {
  const expansion =
    review.executiveAssessment?.expansion ||
    review.executiveAssessment ||
    {};
  const cards = expansion.scanCards || review.scanCards || {};
  const next = resolveFullAdpExecutiveStrengthCardV1(
    stagedPayload,
    cards.biggestStrength
  );
  const scanCards = { ...cards, biggestStrength: next };
  if (review.executiveAssessment?.expansion) {
    review.executiveAssessment.expansion.scanCards = scanCards;
  } else if (review.executiveAssessment) {
    review.executiveAssessment.scanCards = scanCards;
  }
  review.scanCards = scanCards;
  if (review.executiveAssessment?.summary) {
    review.executiveAssessment.summary.biggestStrength = next;
  }
  return review;
}

/**
 * Build Bethesda-template executive review JSON from certified staged Full ADP.
 * Does not re-monitor. Does not invent MoM trends without comparable prior.
 *
 * @param {string} propertyId
 * @param {{ expectedStagedVersionId?: string|null }} [opts]
 */
export function buildFullAdpExecutiveReportFromStagedV1(propertyId, opts = {}) {
  const id = String(propertyId || "").trim();
  if (!id) throw new Error("propertyId_required");

  const staged = loadStagedOwnerPayload(id);
  if (!staged?.payload) throw new Error("staged_report_unavailable");
  const stagedVersionId = staged.versionId;
  if (
    opts.expectedStagedVersionId &&
    stagedVersionId !== opts.expectedStagedVersionId
  ) {
    throw new Error(
      `staged_version_mismatch:expected=${opts.expectedStagedVersionId} actual=${stagedVersionId}`
    );
  }

  let review = buildMonthlyExecutiveReviewV1(id, {
    sourceMode: "staged_founder_review",
    purpose: "founder_review",
    skipEligibility: true,
  });

  // Inject Reality Gap V2 into the executive KPI band (Bethesda slot uses coverage;
  // Full ADP E2E requires the certified weighted gap as a first-class KPI).
  const gapScore =
    staged.payload?.realityGap?.gapScore ??
    staged.payload?.realityGap?.v2?.gapScore ??
    null;
  if (gapScore != null && Array.isArray(review.kpis)) {
    const gapDisplay =
      typeof gapScore === "number"
        ? `${Number(gapScore).toFixed(1)}%`
        : String(gapScore);
    const existingIdx = review.kpis.findIndex((k) => k.id === "realityCoverage");
    const gapKpi = {
      id: "realityGapV2",
      label: "Reality Gap V2",
      currentDisplay: gapDisplay,
      currentValue:
        typeof gapScore === "number" ? gapScore : Number(gapScore) || null,
      priorDisplay: null,
      priorValue: null,
      deltaDisplay: null,
      available: true,
      source: "staged.realityGap.gapScore",
      meta: "Weighted gap (miss + 0.5×partial) — certified staged V2",
      family: "REALITY_GAP",
    };
    if (existingIdx >= 0) {
      review.kpis.splice(existingIdx, 1, gapKpi);
    } else {
      review.kpis.splice(Math.min(2, review.kpis.length), 0, gapKpi);
    }
    review.kpis = review.kpis.slice(0, 6);
  }

  // Same cover product label as original portfolio hotels (Bethesda master).
  // Do not introduce a parallel "FULL ADP EXECUTIVE REPORT" subtitle.
  review.coverSubtitle = PORTFOLIO_CANONICAL_COVER_SUBTITLE;
  review = applyFullAdpStrengthOverride(review, staged.payload);
  review = enrichFullAdpCompetitiveMovementV1(review, staged.payload);

  // Tag for artifact contract / visual fidelity gates (design = Bethesda master).
  review.gates = {
    ...(review.gates || {}),
    ADP_EXECUTIVE_REPORT_PDF_BETHESDA_MASTER_V1: true,
    ADP_FULL_ADP_E2E_EXECUTIVE_PDF: true,
    ADP_CANONICAL_PORTFOLIO_PDF_PROCESS_V1: true,
    NOT_INTERACTIVE_DASHBOARD_PRINT: true,
    [ADP_EXECUTIVE_PDF_FOUNDER_DESIGN_ACCEPTANCE_V1]:
      PDF_FOUNDER_DESIGN_ACCEPTANCE_PENDING,
  };
  review._adpExecutivePdf = {
    templateId: ADP_EXECUTIVE_REPORT_PDF_BETHESDA_MASTER_V1,
    fidelityGate: ADP_EXECUTIVE_PDF_BETHESDA_MASTER_FIDELITY_V1,
    stagedVersionId,
    artifactType: "FULL_ADP_REPORT_PDF",
    dataSource: "certified_staged_full_adp",
    designSource: "bethesda_marriott_performance_review_master",
    productionLine:
      "buildMonthlyExecutiveReviewV1 → archive persist → generateMonthlyReviewPdfV1",
    coverSubtitle: PORTFOLIO_CANONICAL_COVER_SUBTITLE,
    founderDesignAcceptance: PDF_FOUNDER_DESIGN_ACCEPTANCE_PENDING,
    founderDesignAcceptanceContract:
      ADP_EXECUTIVE_PDF_FOUNDER_DESIGN_ACCEPTANCE_V1,
  };
  review.methodologyNote = [
    review.methodologyNote || "",
    "This executive PDF uses the same Dealality AI Demand Performance Review",
    "production line as the original certified hotel portfolio (Bethesda master).",
    "Content is bound to the certified staged Full ADP report version",
    `(${stagedVersionId}); it is not a printout of the interactive owner dashboard.`,
  ]
    .filter(Boolean)
    .join(" ");

  return {
    ok: true,
    propertyId: id,
    stagedVersionId,
    periodId: staged.periodId || staged.payload?.period?.periodId || null,
    review,
    templateId: ADP_EXECUTIVE_REPORT_PDF_BETHESDA_MASTER_V1,
    liveProviderCalls: 0,
    founderDesignAcceptance: PDF_FOUNDER_DESIGN_ACCEPTANCE_PENDING,
  };
}

/**
 * Bethesda fidelity checks against rendered PDF text / DOM markers.
 * @param {{ text?: string, pages?: number, hasCover?: boolean, hasDrsKpi?: boolean, hasInteractiveControls?: boolean, meanLuminances?: number[] }} probe
 */
export function assessBethesdaMasterFidelityV1(probe = {}) {
  const failures = [];
  const text = String(probe.text || "");
  if (probe.hasCover === false) failures.push("MISSING_BRANDED_COVER");
  if (probe.hasDrsKpi === false) failures.push("MISSING_DRS_KPI_GRID");
  if (probe.hasInteractiveControls === true) {
    failures.push("INTERACTIVE_WEB_CONTROLS_IN_PDF");
  }
  if (/Load Report|Save hotel feedback|adpProperty/i.test(text)) {
    failures.push("INTERACTIVE_WEB_CONTROLS_IN_PDF_TEXT");
  }
  if (/Staged review requested but not authorized/i.test(text)) {
    failures.push("DASHBOARD_AUTH_SHELL_PRINTED");
  }
  if (!/AI Demand Performance Review|Executive Performance|Dealality/i.test(text)) {
    failures.push("MISSING_EXECUTIVE_REPORT_IDENTITY");
  }
  if (!/Confidential|FOR RECIPIENT ONLY/i.test(text)) {
    failures.push("MISSING_CONFIDENTIALITY_MARKER");
  }
  if ((probe.pages || 0) < 4) failures.push("TOO_FEW_PAGES_FOR_EXECUTIVE_REPORT");
  const darkPages = (probe.meanLuminances || []).filter((l) => l < 40);
  if (darkPages.length > 1) {
    // Cover is intentionally dark; fail if multiple near-black interior pages.
    failures.push("DARK_DASHBOARD_INTERIOR_PAGES");
  }
  return {
    ok: failures.length === 0,
    gate: ADP_EXECUTIVE_PDF_BETHESDA_MASTER_FIDELITY_V1,
    templateId: ADP_EXECUTIVE_REPORT_PDF_BETHESDA_MASTER_V1,
    failures,
  };
}
