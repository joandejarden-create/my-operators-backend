/**
 * GDI Maturity — QUALIFIED evaluator V1.
 *
 * QUALIFIED = intermediate customer-interesting state, not Ready/ACTIONABLE.
 * Requires traveling cohort + lodging-control hypotheses (UNKNOWN allowed)
 * and honest missingValidation. Does not lower ACTIONABLE thresholds.
 */

import {
  ACCOUNT_QUALITY_CLASS,
  classifyAccountQuality,
  isTrueReadyAccountClass,
} from "./account-quality-taxonomy-v1.js";
import {
  GDI_CONFIDENCE,
  GDI_MATURITY_CLAIM_KIND,
  hasConfirmedAccountSignalEvidence,
  listEvidenceItems,
  normalizeEvidenceItem,
} from "./gdi-evidence-taxonomy-v1.js";
import { passesCustomerSummaryGate } from "./customer-readiness-gate-v1.js";
import { evaluateGdiSummaryQuality, SUMMARY_QUALITY } from "./opportunity-summary-v1.js";

export const QUALIFIED_ACCOUNT_CLASSES = Object.freeze(
  new Set([
    ACCOUNT_QUALITY_CLASS.TRUE_BUYER_ACCOUNT,
    ACCOUNT_QUALITY_CLASS.TRUE_PARTICIPATING_ACCOUNT,
    ACCOUNT_QUALITY_CLASS.TRUE_DELEGATION_ACCOUNT,
    ACCOUNT_QUALITY_CLASS.TRUE_VENDOR_CREW_ACCOUNT,
    ACCOUNT_QUALITY_CLASS.TRUE_ORGANIZER_HOUSING_ACCOUNT,
  ])
);

export const TRAVELING_COHORT_TYPE = Object.freeze({
  EXECUTIVE_TEAM: "EXECUTIVE_TEAM",
  VIP_HOSPITALITY: "VIP_HOSPITALITY",
  SPONSOR_ACTIVATION: "SPONSOR_ACTIVATION",
  EXHIBITOR_TEAM: "EXHIBITOR_TEAM",
  DELEGATION: "DELEGATION",
  SPEAKER_FACULTY: "SPEAKER_FACULTY",
  PRODUCTION_CREW: "PRODUCTION_CREW",
  MEDIA_TEAM: "MEDIA_TEAM",
  PROJECT_TEAM: "PROJECT_TEAM",
  TRAINING_COHORT: "TRAINING_COHORT",
  INVESTOR_DELEGATION: "INVESTOR_DELEGATION",
  SPORTS_TEAM: "SPORTS_TEAM",
  UNKNOWN: "UNKNOWN",
});

export const LODGING_CONTROL_HYPOTHESIS = Object.freeze({
  ACCOUNT_DIRECT: "ACCOUNT_DIRECT",
  EVENT_ORGANIZER: "EVENT_ORGANIZER",
  PCO: "PCO",
  DMC: "DMC",
  TMC: "TMC",
  ACTIVATION_AGENCY: "ACTIVATION_AGENCY",
  EXECUTIVE_ASSISTANT: "EXECUTIVE_ASSISTANT",
  PROCUREMENT: "PROCUREMENT",
  TEAM_MANAGER: "TEAM_MANAGER",
  UNKNOWN: "UNKNOWN",
});

const CONFIDENCE_RANK = Object.freeze({
  [GDI_CONFIDENCE.HIGH]: 3,
  [GDI_CONFIDENCE.MEDIUM]: 2,
  [GDI_CONFIDENCE.LOW]: 1,
  [GDI_CONFIDENCE.UNKNOWN]: 0,
});

const PLACEHOLDER_ORG_RE =
  /^(partner|partners|delegations?|sponsors?|exhibitors?|unknown|tbd|n\/?a|various)$/i;

const EVENT_AS_ACCOUNT_RE =
  /\b(congress|conference|summit|festival|fair|expo|championship|grand prix)\b/i;

function norm(s) {
  return String(s || "").trim();
}

function isPlaceholderOrg(name) {
  const n = norm(name);
  if (!n || n.length < 3) return true;
  return PLACEHOLDER_ORG_RE.test(n);
}

/**
 * Hotel-fit hook for QUALIFIED (Phase 1: presence check; profile override later).
 */
export function evaluateQualifiedHotelFit(opp = {}, opts = {}) {
  if (opts.hotelFitProfile && typeof opts.hotelFitProfile.evaluate === "function") {
    return opts.hotelFitProfile.evaluate(opp, opts);
  }
  const hasFit =
    (opp.hotelFitScore != null && opp.hotelFitScore !== "") ||
    Boolean(opp.summaryWhyHotel) ||
    Boolean(opp.fitExplanation);
  return {
    ok: hasFit,
    reason: hasFit ? "fit_present_phase1" : "hotel_fit_missing",
  };
}

function cohortTypeOk(opp = {}) {
  const type = String(opp.travelingCohortType || "").toUpperCase();
  const summary = norm(opp.travelingCohortSummary);
  if (type && type !== TRAVELING_COHORT_TYPE.UNKNOWN) {
    return { ok: true, type, summary };
  }
  // UNKNOWN type allowed only with defensible inferred summary
  if (summary.length >= 24) {
    return { ok: true, type: type || TRAVELING_COHORT_TYPE.UNKNOWN, summary };
  }
  return { ok: false, type, summary };
}

function cohortConfidenceOk(raw) {
  const c = String(raw || GDI_CONFIDENCE.UNKNOWN).toUpperCase();
  return (CONFIDENCE_RANK[c] ?? 0) >= CONFIDENCE_RANK[GDI_CONFIDENCE.LOW];
}

function listCohortEvidence(opp = {}) {
  const rows = Array.isArray(opp.travelingCohortEvidence)
    ? opp.travelingCohortEvidence
    : [];
  return rows.map(normalizeEvidenceItem).filter(Boolean);
}

function speculativeRoomsAsConfirmed(opp = {}) {
  // Modeled bands presented as verified lodging / confirmed rooms
  const lodgingVerified = opp.lodgingVerified === true;
  const hasModeled =
    opp.modeledRoomsMin != null ||
    opp.modeledRoomsMax != null ||
    String(opp.modeledDemandBasis || "").length > 0 ||
    String(opp.peakRoomsClaimKind || opp.estimatedPeakRoomsClaimKind || "")
      .toUpperCase()
      .includes("ESTIMAT") ||
    String(opp.peakRoomsClaimKind || "").toUpperCase() === "MODELED";

  if (!hasModeled && !lodgingVerified) return false;

  if (lodgingVerified && hasModeled && !hasConfirmedLodgingEvidence(opp)) {
    return true; // claimed verified without lodging evidence
  }

  const claim = String(
    opp.peakRoomsClaimKind || opp.estimatedPeakRoomsClaimKind || opp.roomsClaimKind || ""
  ).toUpperCase();
  if (
    (claim === "FACT" || claim === "VERIFIED" || claim === "CONFIRMED") &&
    hasModeled &&
    !hasConfirmedLodgingEvidence(opp)
  ) {
    return true;
  }

  const why = `${opp.whyNow || ""} ${opp.summaryWhat || ""} ${opp.cardWhyNowLine || ""}`;
  if (
    hasModeled &&
    !hasConfirmedLodgingEvidence(opp) &&
    /\b(needs|requires|confirmed)\s+\d+\s*rooms?\b/i.test(why) &&
    !/modeled|estimated|not verified/i.test(why)
  ) {
    return true;
  }
  return false;
}

function hasConfirmedLodgingEvidence(opp = {}) {
  if (opp.lodgingVerified !== true) return false;
  const items = listEvidenceItems(opp);
  return items.some(
    (e) =>
      e.evidenceType === "CONFIRMED_LODGING_CONTROL" ||
      e.evidenceType === "CONFIRMED_HOTEL_RFP" ||
      (e.claimKind === GDI_MATURITY_CLAIM_KIND.FACT &&
        /lodging|hotel|room\s*block|housing/i.test(String(e.excerpt || "")))
  );
}

function modeledDemandHonest(opp = {}) {
  const hasModeled =
    opp.modeledRoomsMin != null ||
    opp.modeledRoomsMax != null ||
    Boolean(opp.modeledDemandBasis) ||
    Boolean(opp.modeledDemandDisclaimer);
  if (!hasModeled) return { ok: true, reason: "no_modeled_band" };
  const basis = String(opp.modeledDemandBasis || opp.modeledDemandDisclaimer || "");
  const claim = String(
    opp.peakRoomsClaimKind || opp.estimatedPeakRoomsClaimKind || "MODELED"
  ).toUpperCase();
  const honestClaim =
    claim === "MODELED" ||
    claim === "ESTIMATED" ||
    claim === "INFERENCE" ||
    claim === "INFERRED" ||
    claim === "";
  const honestCopy =
    /model(ed|ling)|estimat|not verified|unverified/i.test(basis) ||
    /model(ed|ling)|not verified/i.test(String(opp.modeledDemandDisclaimer || ""));
  if (!honestClaim && !honestCopy) {
    return { ok: false, reason: "modeled_rooms_not_marked_modeled" };
  }
  if (speculativeRoomsAsConfirmed(opp)) {
    return { ok: false, reason: "speculative_rooms_stated_as_confirmed" };
  }
  return { ok: true, reason: "modeled_honest" };
}

function plausibleGroupMotion(opp = {}) {
  const blob = [
    opp.participationRole,
    opp.childEntityType,
    opp.demandType,
    opp.opportunityType,
    opp.segment,
    opp.travelingCohortType,
    opp.travelingCohortSummary,
    opp.summaryWhat,
    opp.whyNow,
  ]
    .map((x) => String(x || ""))
    .join(" ");
  if (
    /\b(sponsor|exhibitor|delegation|hospitality|activation|crew|team|speaker|vendor|buyer|meeting|overflow|housing|vip|executive)\b/i.test(
      blob
    )
  ) {
    return true;
  }
  if (Number(opp.hotelFitScore) >= 50) return true;
  return Boolean(norm(opp.travelingCohortSummary));
}

function buildMissingValidation(opp = {}, lodgingHypothesis) {
  const missing = new Set(
    Array.isArray(opp.missingValidation) ? opp.missingValidation.map(String) : []
  );
  if (opp.lodgingVerified !== true) missing.add("lodging_block_unverified");
  if (opp.headcountVerified !== true) missing.add("headcount_unverified");
  if (String(lodgingHypothesis || "").toUpperCase() === LODGING_CONTROL_HYPOTHESIS.UNKNOWN) {
    missing.add("lodging_controller_unknown");
  }
  if (!opp.primaryContact?.name && !opp.buyerPersonName) {
    missing.add("named_buyer_person_missing");
  }
  return [...missing];
}

/**
 * Account quality for QUALIFIED — TRUE_* classes only (no venue/generator shells).
 * Stamped accountQualityClass accepted when it is a TRUE_* enum value.
 */
export function meetsQualifiedAccountRequirement(opp = {}) {
  const stamped = String(opp.accountQualityClass || "").toUpperCase();
  if (QUALIFIED_ACCOUNT_CLASSES.has(stamped) || isTrueReadyAccountClass(stamped)) {
    return {
      ok: true,
      class: stamped,
      reason: "stamped_true_account_class",
    };
  }
  const c = classifyAccountQuality(opp);
  if (QUALIFIED_ACCOUNT_CLASSES.has(c.class)) {
    return { ok: true, class: c.class, reason: c.reason };
  }
  return {
    ok: false,
    class: c.class,
    reason: c.reason || "account_quality_not_qualified_eligible",
  };
}

/**
 * Full QUALIFIED evaluation.
 * @returns {{ ok: boolean, failed: string[], reasons: string[], ... }}
 */
export function evaluateGdiMaturityQualified(opp = {}, opts = {}) {
  const failed = [];
  const reasons = [];

  const org = norm(opp.organizationName || opp.company);
  if (isPlaceholderOrg(org)) {
    failed.push("named_account_missing_or_placeholder");
  }

  // Event masquerading as account
  if (
    org &&
    EVENT_AS_ACCOUNT_RE.test(org) &&
    !opp.parentDemandSignalId &&
    /^(the\s+)?/.test(org.toLowerCase())
  ) {
    // Soft: only fail when org looks like the event title and role is empty
    const role = norm(opp.participationRole || opp.childEntityType);
    if (!role && String(opp.title || "").toLowerCase().includes(org.toLowerCase())) {
      failed.push("event_masquerading_as_account");
    }
  }

  const accountReq = meetsQualifiedAccountRequirement(opp);
  if (!accountReq.ok) {
    failed.push("account_quality_insufficient");
    if (
      accountReq.class === ACCOUNT_QUALITY_CLASS.VENUE_OPERATOR_PLACEHOLDER ||
      accountReq.class === ACCOUNT_QUALITY_CLASS.GENERATOR_WRAPPER ||
      accountReq.class === ACCOUNT_QUALITY_CLASS.GENERIC_ORG_SHELL ||
      accountReq.class === ACCOUNT_QUALITY_CLASS.CONTACT_PATH_SHELL
    ) {
      failed.push(`shell:${accountReq.class}`);
    }
  } else {
    reasons.push(`account:${accountReq.class}`);
  }

  if (!hasConfirmedAccountSignalEvidence(opp)) {
    failed.push("confirmed_account_signal_evidence_missing");
  } else {
    reasons.push("confirmed_evidence");
  }

  if (!plausibleGroupMotion(opp)) {
    failed.push("plausible_group_motion_missing");
  }

  const cohort = cohortTypeOk(opp);
  if (!cohort.ok) {
    failed.push("traveling_cohort_missing");
  }

  if (!cohortConfidenceOk(opp.travelingCohortConfidence)) {
    failed.push("traveling_cohort_confidence_insufficient");
  }

  const cohortEvidence = listCohortEvidence(opp);
  if (!cohortEvidence.length) {
    // Allow a single synthetic evidence object derived from summary only when
    // travelingCohortEvidence was omitted but summary + type exist (legacy pilot rows).
    if (cohort.ok && norm(opp.travelingCohortSummary)) {
      // Still require at least one evidence object per Phase 1 contract —
      // fail closed unless explicitly provided.
      failed.push("traveling_cohort_evidence_missing");
    } else {
      failed.push("traveling_cohort_evidence_missing");
    }
  }

  const lodgingHyp = String(
    opp.lodgingControlHypothesis || ""
  ).toUpperCase();
  if (!lodgingHyp || !Object.values(LODGING_CONTROL_HYPOTHESIS).includes(lodgingHyp)) {
    failed.push("lodging_control_hypothesis_missing");
  }

  const fit = evaluateQualifiedHotelFit(opp, opts);
  if (!fit.ok) {
    failed.push("hotel_fit_failed");
  }

  const modeled = modeledDemandHonest(opp);
  if (!modeled.ok) {
    failed.push(modeled.reason);
  }

  // lodgingVerified defaults false unless confirmed evidence
  if (opp.lodgingVerified === true && !hasConfirmedLodgingEvidence(opp)) {
    failed.push("lodging_verified_without_confirmed_evidence");
  }

  const missingValidation = buildMissingValidation(opp, lodgingHyp);
  if (!missingValidation.length && opp.lodgingVerified !== true) {
    failed.push("missing_validation_required_for_unresolved_facts");
  }
  // Always require lodging/buyer honesty entries when not verified
  if (opp.lodgingVerified !== true && !missingValidation.includes("lodging_block_unverified")) {
    failed.push("missing_validation_lodging");
  }

  const summaryEval = evaluateGdiSummaryQuality(opp);
  if (
    summaryEval.quality === SUMMARY_QUALITY.THIN ||
    summaryEval.quality === SUMMARY_QUALITY.INVALID ||
    opp.summaryInsufficient === true
  ) {
    // Prefer passesCustomerSummaryGate for QUALIFIED copy bar
    if (!passesCustomerSummaryGate(opp)) {
      failed.push("customer_summary_quality_gate");
    }
  }

  // Mass attendee / local-only individual
  if (
    /\b(mass\s*attendee|general\s*admission|local[- ]only\s*individual)\b/i.test(
      `${opp.travelingCohortSummary || ""} ${opp.summaryWhat || ""}`
    )
  ) {
    failed.push("mass_attendee_or_local_individual");
  }

  const ok = failed.length === 0;
  if (ok) reasons.push("qualified_bar_pass");

  return {
    ok,
    failed,
    reasons,
    accountQualityClass: accountReq.class,
    accountQualityReason: accountReq.reason,
    missingValidation,
    lodgingControlHypothesis: lodgingHyp || null,
    travelingCohortType: cohort.type || null,
    summaryQuality: summaryEval.quality,
    hotelFit: fit,
    modeledHonesty: modeled,
  };
}
