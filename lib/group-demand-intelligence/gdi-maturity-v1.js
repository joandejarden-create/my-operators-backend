/**
 * GDI Discovery Maturity Funnel V1 — canonical source of truth.
 *
 * Discovery maturity (system-derived):
 *   SIGNAL → CANDIDATE → QUALIFIED → ACTIONABLE
 *
 * Sales workflow (user/action-derived) is separate — see SALES_WORKFLOW_STATE.
 *
 * ACTIONABLE ≡ isGdiCustomerOpportunityReady (strict Ready gate). Do not lower.
 */

import { isGdiCustomerOpportunityReady } from "./customer-readiness-gate-v1.js";
import {
  classifyAccountQuality,
  ACCOUNT_QUALITY_CLASS,
} from "./account-quality-taxonomy-v1.js";
import {
  GDI_CONFIDENCE,
  GDI_EVIDENCE_TYPE,
  GDI_MATURITY_CLAIM_KIND,
} from "./gdi-evidence-taxonomy-v1.js";
import {
  evaluateGdiMaturityQualified,
  TRAVELING_COHORT_TYPE,
  LODGING_CONTROL_HYPOTHESIS,
} from "./gdi-maturity-qualified-v1.js";

/** Local copy — avoid circular import with customer-visibility.js */
function isGeneratorOnlyCustomerRecord(opp = {}) {
  if (opp == null || typeof opp !== "object") return false;
  if (opp.isDemandGenerator === true || opp.demandGeneratorOnly === true) return true;
  const family = String(opp.demandFamily || "").toUpperCase();
  if (family === "DEMAND_GENERATOR" || family === "DEMAND_CAMPAIGN") return true;
  const type = String(opp.opportunityType || "").toUpperCase();
  if (type === "DEMAND_GENERATOR" || type === "DEMAND_CAMPAIGN") return true;
  if (
    opp.gdiCampaignShell === true &&
    !opp.childEntityId &&
    !opp.childAdmissionClass
  ) {
    return true;
  }
  return false;
}

export const GDI_MATURITY_STATE = Object.freeze({
  SIGNAL: "SIGNAL",
  CANDIDATE: "CANDIDATE",
  QUALIFIED: "QUALIFIED",
  ACTIONABLE: "ACTIONABLE",
});

export const SALES_WORKFLOW_STATE = Object.freeze({
  UNTOUCHED: "UNTOUCHED",
  RESEARCHING: "RESEARCHING",
  PURSUING: "PURSUING",
  PROPOSAL: "PROPOSAL",
  WON: "WON",
  LOST: "LOST",
  NO_DEMAND: "NO_DEMAND",
});

export { TRAVELING_COHORT_TYPE, LODGING_CONTROL_HYPOTHESIS };

/** Legacy funnelStage / opportunityQualification → maturity (compat only). */
export const LEGACY_TO_MATURITY = Object.freeze({
  READY: GDI_MATURITY_STATE.ACTIONABLE,
  CONTACT_NOW: GDI_MATURITY_STATE.ACTIONABLE,
  HIGH_PRIORITY: GDI_MATURITY_STATE.ACTIONABLE,
  QUALIFIED: GDI_MATURITY_STATE.QUALIFIED,
  QUALIFY_NOW: GDI_MATURITY_STATE.QUALIFIED,
  RESEARCH_FURTHER: GDI_MATURITY_STATE.CANDIDATE,
  WATCH: GDI_MATURITY_STATE.CANDIDATE,
  WATCHLIST: GDI_MATURITY_STATE.CANDIDATE,
  TOO_EARLY: GDI_MATURITY_STATE.CANDIDATE,
  FUTURE_WATCH: GDI_MATURITY_STATE.CANDIDATE,
  SIGNAL: GDI_MATURITY_STATE.SIGNAL,
  DEMAND_GENERATOR: GDI_MATURITY_STATE.SIGNAL,
  DISQUALIFIED: GDI_MATURITY_STATE.SIGNAL,
});

/**
 * ACTIONABLE parity wrapper — identical to strict Ready gate.
 * Keep isGdiCustomerOpportunityReady as the implementation.
 */
export function isGdiMaturityActionable(opp = {}, opts = {}) {
  return isGdiCustomerOpportunityReady(opp, opts);
}

export function normalizeSalesWorkflowState(raw) {
  const v = String(raw || "").trim().toUpperCase();
  if (Object.values(SALES_WORKFLOW_STATE).includes(v)) return v;
  return SALES_WORKFLOW_STATE.UNTOUCHED;
}

/**
 * Never infer PURSUING from maturity=ACTIONABLE.
 */
export function resolveSalesWorkflowState(opp = {}) {
  if (opp.salesWorkflowState) {
    return normalizeSalesWorkflowState(opp.salesWorkflowState);
  }
  return SALES_WORKFLOW_STATE.UNTOUCHED;
}

/**
 * Hotel-fit interface for maturity.
 * Phase 1: preserve current presence-style fit behavior.
 * Phase 3+: pass opts.hotelFitProfile.evaluate(opp, opts).
 */
export function evaluateHotelFitForMaturity(opp = {}, opts = {}) {
  if (opts.hotelFitProfile && typeof opts.hotelFitProfile.evaluate === "function") {
    return opts.hotelFitProfile.evaluate(opp, opts);
  }
  const hasFit =
    (opp.hotelFitScore != null && opp.hotelFitScore !== "") ||
    Boolean(opp.summaryWhyHotel) ||
    Boolean(opp.fitExplanation);
  return {
    ok: hasFit,
    pass: hasFit,
    reason: hasFit ? "fit_present_phase1" : "hotel_fit_missing",
    hotelFitScore: opp.hotelFitScore ?? null,
    profileId: opts.hotelFitProfile?.id || null,
  };
}

/**
 * Phase 4 measurement hooks (interfaces only — not scored in Phase 1).
 */
export const GDI_MATURITY_METRICS_V1 = Object.freeze({
  TOP_20_OPPORTUNITY_PRECISION: "TOP_20_OPPORTUNITY_PRECISION",
  OPPORTUNITY_USEFULNESS_AT_20: "OPPORTUNITY_USEFULNESS@20",
  OPPORTUNITY_NOVELTY_AT_20: "OPPORTUNITY_NOVELTY@20",
  FALSE_ACTIONABLE_RATE: "FALSE_ACTIONABLE_RATE",
  VENUE_OPERATOR_LEAKAGE: "VENUE_OPERATOR_LEAKAGE",
  SPECULATIVE_ROOM_CLAIM_RATE: "SPECULATIVE_ROOM_CLAIM_RATE",
});

export function createEmptyMaturityMetricsScaffold() {
  return {
    version: "gdi-maturity-metrics-scaffold-v1",
    implemented: false,
    metrics: Object.fromEntries(
      Object.values(GDI_MATURITY_METRICS_V1).map((k) => [
        k,
        { status: "DEFERRED_PHASE_4", value: null },
      ])
    ),
  };
}

function namedAccountPresent(opp = {}) {
  const org = String(opp.organizationName || opp.company || "").trim();
  return org.length >= 3;
}

function isShellOrGeneratorAccount(opp = {}) {
  if (isGeneratorOnlyCustomerRecord(opp)) return true;
  const aq = classifyAccountQuality(opp);
  return (
    aq.class === ACCOUNT_QUALITY_CLASS.VENUE_OPERATOR_PLACEHOLDER ||
    aq.class === ACCOUNT_QUALITY_CLASS.GENERATOR_WRAPPER ||
    aq.class === ACCOUNT_QUALITY_CLASS.CONTACT_PATH_SHELL
  );
}

/**
 * Assign canonical gdiMaturityState.
 * Does not mutate salesWorkflowState. Does not lower ACTIONABLE bar.
 */
export function assignGdiMaturityState(opp = {}, opts = {}) {
  const evaluatedAt = opts.nowIso || new Date().toISOString();
  const reasons = [];

  if (opp == null || typeof opp !== "object") {
    return buildResult(GDI_MATURITY_STATE.SIGNAL, ["null_opportunity"], evaluatedAt, opp);
  }

  if (isGeneratorOnlyCustomerRecord(opp)) {
    return buildResult(
      GDI_MATURITY_STATE.SIGNAL,
      ["generator_or_campaign_shell"],
      evaluatedAt,
      opp,
      { accountQualityClass: classifyAccountQuality(opp).class }
    );
  }

  if (!namedAccountPresent(opp)) {
    return buildResult(
      GDI_MATURITY_STATE.SIGNAL,
      ["no_named_account"],
      evaluatedAt,
      opp
    );
  }

  // ACTIONABLE first — strict Ready parity
  const actionable = isGdiMaturityActionable(opp, opts);
  if (actionable.ok === true) {
    return buildResult(
      GDI_MATURITY_STATE.ACTIONABLE,
      ["ready_gate_pass"],
      evaluatedAt,
      opp,
      {
        actionable,
        accountQualityClass: actionable.accountQualityClass,
      }
    );
  }

  // QUALIFIED — intermediate bar (does not grant ACTIONABLE)
  const qualified = evaluateGdiMaturityQualified(opp, {
    ...opts,
    actionableProbe: actionable,
  });
  if (qualified.ok === true) {
    return buildResult(
      GDI_MATURITY_STATE.QUALIFIED,
      qualified.reasons.length ? qualified.reasons : ["qualified_requirements_met"],
      evaluatedAt,
      opp,
      {
        actionable,
        qualified,
        accountQualityClass: qualified.accountQualityClass,
      }
    );
  }

  // Named account but not QUALIFIED → CANDIDATE (or SIGNAL if pure shell)
  if (isShellOrGeneratorAccount(opp) && !hasAnyAccountSignal(opp)) {
    reasons.push("shell_without_account_signal");
    return buildResult(GDI_MATURITY_STATE.SIGNAL, reasons, evaluatedAt, opp, {
      actionable,
      qualified,
      accountQualityClass: classifyAccountQuality(opp).class,
    });
  }

  reasons.push(...(qualified.failed || []).slice(0, 6));
  if (!reasons.length) reasons.push("named_account_not_yet_qualified");
  return buildResult(GDI_MATURITY_STATE.CANDIDATE, reasons, evaluatedAt, opp, {
    actionable,
    qualified,
    accountQualityClass: classifyAccountQuality(opp).class,
  });
}

function hasAnyAccountSignal(opp = {}) {
  return Boolean(
    opp.officialSource ||
      opp.discoverySource ||
      (Array.isArray(opp.evidenceItems) && opp.evidenceItems.length) ||
      (Array.isArray(opp.sources) && opp.sources.length) ||
      opp.parentDemandSignalId
  );
}

function buildResult(state, reasons, evaluatedAt, opp, extra = {}) {
  const salesWorkflowState = resolveSalesWorkflowState(opp);
  return {
    gdiMaturityState: state,
    gdiMaturityReason: Array.isArray(reasons) ? reasons.join("|") : String(reasons || ""),
    gdiMaturityReasons: Array.isArray(reasons) ? reasons : [String(reasons || "")],
    gdiMaturityEvaluatedAt: evaluatedAt,
    gdiMaturityEvidenceSummary: summarizeEvidence(opp),
    salesWorkflowState,
    salesWorkflowInferredFromMaturity: false,
    ...extra,
  };
}

function summarizeEvidence(opp = {}) {
  const parts = [];
  if (opp.parentDemandSignalLabel) parts.push(`signal:${opp.parentDemandSignalLabel}`);
  if (opp.travelingCohortType) parts.push(`cohort:${opp.travelingCohortType}`);
  if (opp.lodgingControlHypothesis) {
    parts.push(`lodging:${opp.lodgingControlHypothesis}`);
  }
  if (opp.lodgingVerified === true) parts.push("lodging_verified");
  else parts.push("lodging_unverified");
  return parts.join("; ") || null;
}

/**
 * Stamp maturity fields onto an opportunity payload (payload-first; no Airtable).
 * Does not set customerVisible. Does not mutate sales workflow from maturity.
 */
export function applyGdiMaturityStamp(opp = {}, opts = {}) {
  const result = assignGdiMaturityState(opp, opts);
  const next = {
    ...opp,
    gdiMaturityState: result.gdiMaturityState,
    gdiMaturityReason: result.gdiMaturityReason,
    gdiMaturityEvaluatedAt: result.gdiMaturityEvaluatedAt,
    gdiMaturityEvidenceSummary: result.gdiMaturityEvidenceSummary,
    salesWorkflowState: result.salesWorkflowState,
  };
  return { opportunity: next, evaluation: result };
}

/**
 * Validate maturity enum (no other values allowed).
 */
export function isValidGdiMaturityState(state) {
  return Object.values(GDI_MATURITY_STATE).includes(String(state || ""));
}

/**
 * Transition legality (forward-only for system promotion; demotions allowed).
 */
export function canTransitionMaturity(from, to) {
  const order = [
    GDI_MATURITY_STATE.SIGNAL,
    GDI_MATURITY_STATE.CANDIDATE,
    GDI_MATURITY_STATE.QUALIFIED,
    GDI_MATURITY_STATE.ACTIONABLE,
  ];
  const a = order.indexOf(String(from || ""));
  const b = order.indexOf(String(to || ""));
  if (a < 0 || b < 0) return false;
  return true; // demotion and promotion both allowed; no skip enforcement at storage
}

export function maturityRank(state) {
  const order = {
    [GDI_MATURITY_STATE.SIGNAL]: 0,
    [GDI_MATURITY_STATE.CANDIDATE]: 1,
    [GDI_MATURITY_STATE.QUALIFIED]: 2,
    [GDI_MATURITY_STATE.ACTIONABLE]: 3,
  };
  return order[String(state || "")] ?? -1;
}

/**
 * Map legacy customerFacingState / bookingWindow / priority → suggested maturity
 * for audit display only — not authoritative.
 */
export function mapLegacyFieldsToSuggestedMaturity(opp = {}) {
  const ready = isGdiMaturityActionable(opp);
  if (ready.ok) return GDI_MATURITY_STATE.ACTIONABLE;
  const keys = [
    opp.customerFacingState,
    opp.bookingWindowStatus,
    opp.opportunityQualification,
    opp.funnelStage,
    opp.priority,
  ]
    .map((x) => String(x || "").toUpperCase())
    .filter(Boolean);
  for (const k of keys) {
    if (LEGACY_TO_MATURITY[k]) return LEGACY_TO_MATURITY[k];
  }
  if (isGeneratorOnlyCustomerRecord(opp)) return GDI_MATURITY_STATE.SIGNAL;
  if (namedAccountPresent(opp)) return GDI_MATURITY_STATE.CANDIDATE;
  return GDI_MATURITY_STATE.SIGNAL;
}

export {
  GDI_CONFIDENCE,
  GDI_EVIDENCE_TYPE,
  GDI_MATURITY_CLAIM_KIND,
  ACCOUNT_QUALITY_CLASS,
};
