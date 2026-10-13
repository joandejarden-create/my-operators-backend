/**
 * GDI Opportunity Confirmation Engine V1
 *
 * Advances QUALIFIED → (maybe) ACTIONABLE via evidence-backed research passes.
 * Does NOT lower isGdiCustomerOpportunityReady / ACTIONABLE thresholds.
 * Does NOT invent buyers, rooms, lodging control, or participation.
 */

import {
  assignGdiMaturityState,
  GDI_MATURITY_STATE,
  LODGING_CONTROL_HYPOTHESIS,
  TRAVELING_COHORT_TYPE,
} from "../gdi-maturity-v1.js";
import { isGdiMaturityActionable } from "../customer-readiness-gate-v1.js";
import { classifyBuyerContactPath } from "../buyer-contact-path-taxonomy-v1.js";
import { applyGdiWhoHowResolution } from "../opportunity-who-resolution-v1.js";
import {
  GDI_EVIDENCE_TYPE,
  GDI_MATURITY_CLAIM_KIND,
  normalizeEvidenceItem,
} from "../gdi-evidence-taxonomy-v1.js";

export const CONFIRMATION_ENGINE_ID = "gdi_opportunity_confirmation_v1";

export const PARTICIPATION_STATUS = Object.freeze({
  CONFIRMED_CURRENT: "CONFIRMED_CURRENT",
  CONFIRMED_RECURRING: "CONFIRMED_RECURRING",
  HISTORICAL_ONLY: "HISTORICAL_ONLY",
  INFERRED: "INFERRED",
  NOT_CONFIRMED: "NOT_CONFIRMED",
});

export const RECURRENCE_STATUS = Object.freeze({
  NONE: "NONE",
  SINGLE_PRIOR: "SINGLE_PRIOR",
  MULTI_YEAR: "MULTI_YEAR",
  STRONG_RECURRING_PATTERN: "STRONG_RECURRING_PATTERN",
});

export const CONFIRMATION_BLOCKER = Object.freeze({
  NO_CURRENT_PARTICIPATION_PROOF: "NO_CURRENT_PARTICIPATION_PROOF",
  HISTORICAL_ONLY: "HISTORICAL_ONLY",
  NO_TRAVELING_COHORT: "NO_TRAVELING_COHORT",
  COHORT_TOO_WEAK: "COHORT_TOO_WEAK",
  NO_LODGING_CONTROL_PATH: "NO_LODGING_CONTROL_PATH",
  BUYER_PATH_TOO_GENERIC: "BUYER_PATH_TOO_GENERIC",
  NO_WHY_NOW: "NO_WHY_NOW",
  HOTEL_FIT_TOO_WEAK: "HOTEL_FIT_TOO_WEAK",
  TOO_EARLY: "TOO_EARLY",
  ALREADY_CONTRACTED: "ALREADY_CONTRACTED",
  WRONG_GEOGRAPHY: "WRONG_GEOGRAPHY",
  DUPLICATE_OPPORTUNITY: "DUPLICATE_OPPORTUNITY",
  INSUFFICIENT_EVIDENCE: "INSUFFICIENT_EVIDENCE",
  READY_GATE_FAILED: "READY_GATE_FAILED",
  PRESS_CONTACT_ONLY: "PRESS_CONTACT_ONLY",
});

export const EXTENDED_COHORT_TYPE = Object.freeze({
  ...TRAVELING_COHORT_TYPE,
  CLIENT_HOSPITALITY: "CLIENT_HOSPITALITY",
  BRAND_SUPPORT_TEAM: "BRAND_SUPPORT_TEAM",
  LOGISTICS_SUPPORT_TEAM: "LOGISTICS_SUPPORT_TEAM",
  OTHER: "OTHER",
});

/**
 * Compute QUALIFIED → ACTIONABLE conversion KPI.
 */
export function computeQualifiedToActionableConversion(rows = []) {
  const qualifiedResearched = rows.filter(
    (r) =>
      r.originalMaturity === GDI_MATURITY_STATE.QUALIFIED ||
      r.startedAsQualified === true
  ).length;
  const promotedActionable = rows.filter(
    (r) => r.finalMaturity === GDI_MATURITY_STATE.ACTIONABLE && r.promoted === true
  ).length;
  const conversionRate =
    qualifiedResearched === 0
      ? 0
      : Math.round((1000 * promotedActionable) / qualifiedResearched) / 10;
  const blockerCounts = {};
  for (const r of rows) {
    for (const b of r.blockers || []) {
      blockerCounts[b] = (blockerCounts[b] || 0) + 1;
    }
  }
  return {
    metric: "QUALIFIED_TO_ACTIONABLE_CONVERSION_RATE",
    qualifiedResearched,
    promotedActionable,
    conversionRate,
    blockerDistribution: blockerCounts,
  };
}

function mergeEvidence(existing = [], incoming = []) {
  const out = [...(existing || [])];
  for (const e of incoming || []) {
    const n = normalizeEvidenceItem(e) || e;
    if (!n) continue;
    const key = `${n.evidenceType}|${n.sourceUrl || ""}|${n.excerpt || ""}`;
    if (
      out.some(
        (x) =>
          `${x.evidenceType || x.type}|${x.sourceUrl || x.url || ""}|${x.excerpt || x.note || ""}` ===
          key
      )
    ) {
      continue;
    }
    out.push(n);
  }
  return out;
}

function deriveBlockers(findings = {}, ready = {}, opts = {}) {
  const blockers = [];
  const p = findings.participation || {};
  if (
    p.participationStatus === PARTICIPATION_STATUS.NOT_CONFIRMED ||
    p.participationStatus === PARTICIPATION_STATUS.INFERRED
  ) {
    blockers.push(CONFIRMATION_BLOCKER.NO_CURRENT_PARTICIPATION_PROOF);
  }
  if (p.participationStatus === PARTICIPATION_STATUS.HISTORICAL_ONLY) {
    blockers.push(CONFIRMATION_BLOCKER.HISTORICAL_ONLY);
  }
  const cohort = findings.travelingCohort || {};
  if (
    !cohort.travelingCohortType ||
    cohort.travelingCohortType === "UNKNOWN" ||
    !cohort.travelingCohortSummary
  ) {
    blockers.push(CONFIRMATION_BLOCKER.NO_TRAVELING_COHORT);
  } else if (
    String(cohort.travelingCohortConfidence || "").toUpperCase() === "LOW" &&
    !(cohort.travelingCohortEvidence || []).length
  ) {
    blockers.push(CONFIRMATION_BLOCKER.COHORT_TOO_WEAK);
  }
  const lodging = findings.lodgingControl || {};
  if (
    !lodging.lodgingControlHypothesis ||
    lodging.lodgingControlHypothesis === LODGING_CONTROL_HYPOTHESIS.UNKNOWN
  ) {
    // UNKNOWN allowed at QUALIFIED; always track as blocker for ACTIONABLE conversion audit
    blockers.push(CONFIRMATION_BLOCKER.NO_LODGING_CONTROL_PATH);
  }
  const buyer = findings.buyerPath || {};
  if (buyer.pressContactOnly === true) {
    blockers.push(CONFIRMATION_BLOCKER.PRESS_CONTACT_ONLY);
  }
  const pathClass = String(buyer.buyerContactPathClass || buyer.pathClass || "");
  if (
    pathClass === "SOURCE_PAGE" ||
    pathClass === "GENERAL_ORG_CONTACT" ||
    pathClass === "NO_CONTACT" ||
    buyer.tooGeneric === true
  ) {
    blockers.push(CONFIRMATION_BLOCKER.BUYER_PATH_TOO_GENERIC);
  }
  if (!findings.whyNow?.text && !opts.updatedWhyNow) {
    blockers.push(CONFIRMATION_BLOCKER.NO_WHY_NOW);
  }
  if (findings.wrongGeography === true) {
    blockers.push(CONFIRMATION_BLOCKER.WRONG_GEOGRAPHY);
  }
  if (!ready.ok && !blockers.includes(CONFIRMATION_BLOCKER.READY_GATE_FAILED)) {
    blockers.push(CONFIRMATION_BLOCKER.READY_GATE_FAILED);
  }
  return [...new Set(blockers)];
}

/**
 * Apply confirmation findings onto an opportunity (immutable).
 * @param {object} opportunity
 * @param {object} findings - structured confirmation research result
 * @param {object} [opts]
 */
export function applyConfirmationFindings(opportunity = {}, findings = {}, opts = {}) {
  const p = findings.participation || {};
  const cohort = findings.travelingCohort || {};
  const lodging = findings.lodgingControl || {};
  const buyer = findings.buyerPath || {};
  const recurrence = findings.recurrence || {};
  const whyNow = findings.whyNow || {};
  const action = findings.recommendedAction || {};

  let next = {
    ...opportunity,
    confirmationEngineId: CONFIRMATION_ENGINE_ID,
    confirmationEvaluatedAt: opts.nowIso || new Date().toISOString(),
    participationStatus: p.participationStatus || opportunity.participationStatus || null,
    participationConfidence:
      p.participationConfidence || opportunity.participationConfidence || null,
    evidenceItems: mergeEvidence(opportunity.evidenceItems, [
      ...(p.evidenceItems || []),
      ...(cohort.travelingCohortEvidence || []),
      ...(lodging.lodgingControlEvidence || []),
      ...(recurrence.recurrenceEvidence || []),
      ...(buyer.evidenceItems || []),
    ]),
    travelingCohortType:
      cohort.travelingCohortType || opportunity.travelingCohortType || null,
    travelingCohortSummary:
      cohort.travelingCohortSummary || opportunity.travelingCohortSummary || null,
    travelingCohortConfidence:
      cohort.travelingCohortConfidence || opportunity.travelingCohortConfidence || null,
    travelingCohortEvidence: mergeEvidence(
      opportunity.travelingCohortEvidence,
      cohort.travelingCohortEvidence || []
    ),
    lodgingControlHypothesis:
      lodging.lodgingControlHypothesis ||
      opportunity.lodgingControlHypothesis ||
      LODGING_CONTROL_HYPOTHESIS.UNKNOWN,
    lodgingControlSummary:
      lodging.lodgingControlSummary || opportunity.lodgingControlSummary || null,
    lodgingControlConfidence:
      lodging.lodgingControlConfidence || opportunity.lodgingControlConfidence || null,
    lodgingControlEvidence: mergeEvidence(
      opportunity.lodgingControlEvidence,
      lodging.lodgingControlEvidence || []
    ),
    recurrenceStatus: recurrence.recurrenceStatus || opportunity.recurrenceStatus || null,
    recurrenceConfidence:
      recurrence.recurrenceConfidence || opportunity.recurrenceConfidence || null,
    recurrenceEvidence: mergeEvidence(
      opportunity.recurrenceEvidence,
      recurrence.recurrenceEvidence || []
    ),
    whyNow: whyNow.text || opportunity.whyNow,
    cardWhyNowLine: whyNow.text || opportunity.cardWhyNowLine,
    recommendedAction: action.text || opportunity.recommendedAction,
    recommendedNextAction: action.text || opportunity.recommendedNextAction,
    recommendedNextStep: action.text || opportunity.recommendedNextStep,
    // Honesty: never flip lodgingVerified unless findings explicitly confirm
    lodgingVerified:
      findings.lodgingVerified === true ? true : opportunity.lodgingVerified === true,
    headcountVerified:
      findings.headcountVerified === true ? true : opportunity.headcountVerified === true,
    contactResearchAttempted: true,
    contactResearchState: "ATTEMPTED",
    confirmationPassNotes: findings.notes || null,
  };

  // Buyer path updates — never invent named people; only apply when findings supply them
  if (buyer.publicContactPath) {
    next.publicContactPath = buyer.publicContactPath;
  }
  if (buyer.buyerEntity) next.buyerEntity = buyer.buyerEntity;
  if (buyer.buyerRole) {
    next.buyerRole = buyer.buyerRole;
    next.primaryContactRole = buyer.buyerRole;
  }
  if (buyer.namedBuyerPerson && buyer.namedBuyerPerson.name) {
    // Only when research actually found a person — still may be press-only
    next.primaryContact = {
      ...(next.primaryContact || {}),
      name: buyer.namedBuyerPerson.name,
      role: buyer.namedBuyerPerson.role || buyer.buyerRole || null,
      email: buyer.namedBuyerPerson.email || null,
      sourceUrl: buyer.namedBuyerPerson.sourceUrl || buyer.publicContactPath || null,
      claimKind: "FACT",
      pressContactOnly: buyer.pressContactOnly === true,
    };
  }
  if (buyer.functionalContact) {
    next.functionalContact = buyer.functionalContact;
  }

  // Stamp WHO resolution without inventing people
  const whoResolved = applyGdiWhoHowResolution(next);
  next = whoResolved?.opportunity || whoResolved || next;

  const path = classifyBuyerContactPath(next);
  next.buyerContactPathClass = path.class;
  next.buyerResearchStatus =
    path.readyEligible === true
      ? buyer.pressContactOnly
        ? "PRESS_CONTACT_NOT_PURCHASE_PATH"
        : "FUNCTION_PATH_RESEARCHED"
      : "PATH_INSUFFICIENT_FOR_ACTIONABLE";

  // Preserve modeled honesty — never invent verified rooms; keep room-band phrasing summary-safe
  if (next.modeledRoomsMin != null || next.modeledRoomsMax != null) {
    next.modeledDemandDisclaimer =
      `Estimated room-band ${next.modeledRoomsMin}–${next.modeledRoomsMax} (modeled). Lodging not verified.`;
    next.peakRoomsClaimKind = next.peakRoomsClaimKind || "MODELED";
  }
  if (next.lodgingVerified !== true) {
    next.lodgingVerified = false;
  }

  // Rebuild summaryWhat after confirmation so INVALID room phrasing / "mass attendee" negation traps do not demote QUALIFIED
  if (next.organizationName || next.company) {
    const org = next.organizationName || next.company;
    const signal = next.parentDemandSignalLabel || next.eventName || next.canonicalEventName || "demand signal";
    const role = String(next.participationRole || next.accountRole || "participant")
      .replace(/_/g, " ")
      .toLowerCase();
    next.summaryWhat = [
      `${org} is a published ${role} linked to ${signal}.`,
      next.travelingCohortSummary
        ? `Traveling cohort: ${next.travelingCohortSummary}.`
        : null,
      next.modeledDemandDisclaimer || null,
      next.summaryWhyHotel || next.fitExplanation || null,
      next.recommendedAction
        ? `Next: ${next.recommendedAction}`
        : null,
    ]
      .filter(Boolean)
      .join(" ");
  }

  // Confirmation Pass 2 → team proof for Ready surface (evidence-backed cohort only)
  const cohortConf = String(cohort.travelingCohortConfidence || "").toUpperCase();
  const cohortOk =
    cohort.travelingCohortType &&
    cohort.travelingCohortType !== "UNKNOWN" &&
    cohort.travelingCohortSummary &&
    (cohortConf === "HIGH" || cohortConf === "MEDIUM") &&
    Array.isArray(cohort.travelingCohortEvidence) &&
    cohort.travelingCohortEvidence.length > 0;
  if (cohortOk) {
    next.teamSupported = true;
    next.teamEvidence = {
      teamSupported: true,
      multiPerson: true,
      outOfMarket: true,
      teamEvidenceLevel: "present",
      travelingCohortType: cohort.travelingCohortType,
      summary: cohort.travelingCohortSummary,
      confidence: cohort.travelingCohortConfidence,
      claimKind: "FACT",
      source: CONFIRMATION_ENGINE_ID,
    };
  }

  // Lodging narrative for surface thesis — NEVER sets lodgingVerified
  // Mirrors live ACTIONABLE honesty: overflow/context language without claiming a block
  if (lodging.lodgingControlSummary || lodging.lodgingControlHypothesis) {
    const hyp = lodging.lodgingControlHypothesis || next.lodgingControlHypothesis || "UNKNOWN";
    const base =
      lodging.lodgingControlSummary ||
      next.lodgingControlSummary ||
      "Lodging controller not published";
    next.lodgingEvidence =
      next.lodgingEvidence ||
      `${base}. Hypothesis=${hyp}. No published hotel block verified for this account.`;
  }

  return next;
}

/**
 * Run confirmation engine on one opportunity.
 *
 * @param {{ hotel?: object, opportunity: object, evidenceContext?: object, confirmationOptions?: object, findings?: object }} input
 *   findings = pre-researched confirmation pack for this account (dry-run / offline research)
 */
export function confirmOpportunityV1(input = {}) {
  const opportunity = input.opportunity || {};
  const findings = input.findings || input.evidenceContext?.findings || {};
  const opts = input.confirmationOptions || {};
  const nowDate = opts.nowDate || "2026-10-05";
  const nowIso = opts.nowIso || new Date().toISOString();

  const originalEval = assignGdiMaturityState(opportunity, { nowDate });
  const originalMaturity =
    opportunity.gdiMaturityState || originalEval.gdiMaturityState;

  const updated = applyConfirmationFindings(opportunity, findings, { nowIso });
  const ready = isGdiMaturityActionable(updated, { nowDate });
  const maturity = assignGdiMaturityState(updated, { nowDate });

  // Historical-only cannot become ACTIONABLE solely on recurrence
  let finalMaturity = maturity.gdiMaturityState;
  const participationStatus = findings.participation?.participationStatus;
  const honestyDemoteReasons = [];
  if (
    finalMaturity === GDI_MATURITY_STATE.ACTIONABLE &&
    (participationStatus === PARTICIPATION_STATUS.HISTORICAL_ONLY ||
      participationStatus === PARTICIPATION_STATUS.NOT_CONFIRMED ||
      participationStatus === PARTICIPATION_STATUS.INFERRED)
  ) {
    finalMaturity = GDI_MATURITY_STATE.QUALIFIED;
    honestyDemoteReasons.push("participation_insufficient_for_actionable");
  }
  // Press-only contacts never promote
  if (findings.buyerPath?.pressContactOnly === true) {
    if (finalMaturity === GDI_MATURITY_STATE.ACTIONABLE) {
      finalMaturity = GDI_MATURITY_STATE.QUALIFIED;
      honestyDemoteReasons.push("press_contact_only");
    }
  }
  // Findings pack marks generic/source buyer paths — do not promote even if role+entity classifier is lenient
  const findingsPathClass = String(
    findings.buyerPath?.buyerContactPathClass || findings.buyerPath?.pathClass || ""
  );
  if (
    findings.buyerPath?.tooGeneric === true ||
    findingsPathClass === "SOURCE_PAGE" ||
    findingsPathClass === "GENERAL_ORG_CONTACT" ||
    findingsPathClass === "NO_CONTACT"
  ) {
    if (finalMaturity === GDI_MATURITY_STATE.ACTIONABLE) {
      finalMaturity = GDI_MATURITY_STATE.QUALIFIED;
      honestyDemoteReasons.push("buyer_path_too_generic");
    }
  }
  // Lodging control UNKNOWN blocks ACTIONABLE (confirmation honesty — Ready may not encode this)
  const lodgingHyp =
    updated.lodgingControlHypothesis ||
    findings.lodgingControl?.lodgingControlHypothesis;
  if (
    !lodgingHyp ||
    lodgingHyp === LODGING_CONTROL_HYPOTHESIS.UNKNOWN
  ) {
    if (finalMaturity === GDI_MATURITY_STATE.ACTIONABLE) {
      finalMaturity = GDI_MATURITY_STATE.QUALIFIED;
      honestyDemoteReasons.push("lodging_control_unknown");
    }
  }
  // Force Ready gate: if ready.ok false, cannot be ACTIONABLE
  if (!ready.ok && finalMaturity === GDI_MATURITY_STATE.ACTIONABLE) {
    finalMaturity = GDI_MATURITY_STATE.QUALIFIED;
    honestyDemoteReasons.push("ready_gate_failed");
  }

  const promoted =
    originalMaturity === GDI_MATURITY_STATE.QUALIFIED &&
    finalMaturity === GDI_MATURITY_STATE.ACTIONABLE &&
    ready.ok === true;

  // Confirmation of QUALIFIED must not demote below QUALIFIED (pilot stamps may differ from canonical re-eval)
  if (
    originalMaturity === GDI_MATURITY_STATE.QUALIFIED &&
    finalMaturity !== GDI_MATURITY_STATE.ACTIONABLE &&
    finalMaturity !== GDI_MATURITY_STATE.QUALIFIED
  ) {
    finalMaturity = GDI_MATURITY_STATE.QUALIFIED;
    honestyDemoteReasons.push("preserved_qualified_floor");
  }

  const blockers = deriveBlockers(findings, ready, {
    updatedWhyNow: updated.whyNow,
  });

  const promotionReasons = [];
  if (promoted) {
    promotionReasons.push("ready_gate_pass_after_confirmation");
    if (participationStatus === PARTICIPATION_STATUS.CONFIRMED_CURRENT) {
      promotionReasons.push("confirmed_current_participation");
    }
    if (findings.buyerPath?.buyerContactPathClass || ready.buyerContactPathClass) {
      promotionReasons.push(`buyer_path:${ready.buyerContactPathClass || findings.buyerPath?.buyerContactPathClass}`);
    }
  }

  const stamped = {
    ...updated,
    gdiMaturityState: finalMaturity,
    gdiMaturityReason: promoted
      ? promotionReasons.join("|")
      : honestyDemoteReasons.length
        ? `confirmation_held:${honestyDemoteReasons.join("|")}`
        : maturity.gdiMaturityReason,
    gdiMaturityEvaluatedAt: nowIso,
    confirmationBlockers: blockers,
    confirmationPromoted: promoted,
  };

  return {
    hotelId: opportunity.hotelId || input.hotel?.hotelId || null,
    accountName: opportunity.organizationName || opportunity.company || null,
    opportunityId: opportunity.id || opportunity.opportunityId || null,
    originalMaturity,
    startedAsQualified: originalMaturity === GDI_MATURITY_STATE.QUALIFIED,
    confirmationFindings: findings,
    updatedEvidenceItems: stamped.evidenceItems,
    updatedTravelingCohort: {
      travelingCohortType: stamped.travelingCohortType,
      travelingCohortSummary: stamped.travelingCohortSummary,
      travelingCohortConfidence: stamped.travelingCohortConfidence,
      travelingCohortEvidence: stamped.travelingCohortEvidence,
    },
    updatedLodgingControl: {
      lodgingControlHypothesis: stamped.lodgingControlHypothesis,
      lodgingControlSummary: stamped.lodgingControlSummary,
      lodgingControlConfidence: stamped.lodgingControlConfidence,
      lodgingControlEvidence: stamped.lodgingControlEvidence,
    },
    updatedBuyerPath: {
      class: stamped.buyerContactPathClass,
      buyerEntity: stamped.buyerEntity,
      buyerRole: stamped.buyerRole,
      publicContactPath: stamped.publicContactPath,
      researchStatus: stamped.buyerResearchStatus,
      pressContactOnly: findings.buyerPath?.pressContactOnly === true,
    },
    updatedWhyNow: stamped.whyNow,
    updatedRecommendedAction: stamped.recommendedAction,
    finalMaturity,
    promoted,
    promotionReasons,
    blockers,
    readyGate: {
      ok: ready.ok,
      state: ready.state,
      failed: ready.failed,
      buyerContactPathClass: ready.buyerContactPathClass,
      accountQualityClass: ready.accountQualityClass,
    },
    lodgingVerified: stamped.lodgingVerified === true,
    speculativeRoomClaim: false,
    opportunity: stamped,
  };
}

/**
 * Batch confirm opportunities with a findings map keyed by accountName or opportunityId.
 */
export function confirmOpportunityBatchV1({
  opportunities = [],
  findingsByKey = {},
  hotel = null,
  confirmationOptions = {},
} = {}) {
  const results = [];
  for (const opp of opportunities) {
    const key =
      opp.organizationName ||
      opp.company ||
      opp.id ||
      opp.opportunityId ||
      "";
    const findings =
      findingsByKey[key] ||
      findingsByKey[opp.id] ||
      findingsByKey[opp.opportunityId] ||
      {};
    results.push(
      confirmOpportunityV1({
        hotel,
        opportunity: opp,
        findings,
        confirmationOptions,
      })
    );
  }
  const kpi = computeQualifiedToActionableConversion(results);
  return { results, kpi };
}

export {
  GDI_MATURITY_STATE,
  LODGING_CONTROL_HYPOTHESIS,
  TRAVELING_COHORT_TYPE,
  GDI_EVIDENCE_TYPE,
  GDI_MATURITY_CLAIM_KIND,
};
