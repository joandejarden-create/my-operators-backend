/**
 * GDI Contact Completeness V1 — Jev-assisted contact research routing.
 *
 * LEVEL 1 SAFE APPLY (automatic influence with deterministic gates):
 *   CONTACT_SOURCE_PATH, CONTACT_FOLLOWUP_TYPE, STOP_CONTACT_RESEARCH
 *
 * LEVEL 2 SHADOW / ADVISORY ONLY:
 *   NAMED_PERSON_WORTH_PURSUING, FUNCTIONAL_PATH_SUFFICIENT
 *   (person selection / role relevance never auto-applies)
 *
 * Jev must not invent people, titles, emails, phones, or override identity.
 */

import { decide } from "./jev/jev-decision-service.js";
import { CHOICES, isValidChoice } from "./jev/jev-types.js";
import {
  CONTACT_SOURCE_PATH,
  CONTACT_FOLLOWUP_TYPE,
  NAMED_PERSON_WORTH,
  FUNCTIONAL_SUFFICIENCY,
  STOP_CONTACT_RESEARCH,
  computeGdiContactRouting,
} from "./contact-source-recovery-v1-1.js";
import {
  CONTACT_JEV_DECISION,
  buildContactJevContext,
} from "./contact-jev-shadow-v1-1.js";

export const JEV_CONTACT_APPLY_LEVEL = Object.freeze({
  SAFE_APPLY: "SAFE_APPLY",
  SHADOW: "SHADOW",
});

/** Decision types allowed to influence production routing. */
export const JEV_CONTACT_SAFE_APPLY_TYPES = Object.freeze([
  CONTACT_JEV_DECISION.CONTACT_SOURCE_PATH,
  CONTACT_JEV_DECISION.CONTACT_FOLLOWUP_TYPE,
  CONTACT_JEV_DECISION.STOP_CONTACT_RESEARCH,
]);

/** Always advisory — never select WHO. */
export const JEV_CONTACT_SHADOW_TYPES = Object.freeze([
  CONTACT_JEV_DECISION.NAMED_PERSON_WORTH_PURSUING,
  CONTACT_JEV_DECISION.FUNCTIONAL_PATH_SUFFICIENT,
]);

export const JEV_QUALITY = Object.freeze({
  SAME: "SAME",
  HELPFUL_DIFFERENT: "HELPFUL_DIFFERENT",
  WRONG: "WRONG",
  UNKNOWN: "UNKNOWN",
});

const MIN_SAFE_APPLY_CONFIDENCE = 0.55;

/**
 * Hard gates: never apply Jev when it invents / lacks support / contradicts identity.
 */
export function canSafeApplyJevDecision({
  decisionType,
  jevChoice,
  deterministicChoice,
  confidence,
  domainState,
  technicalFallback,
  policyFallback,
} = {}) {
  if (!JEV_CONTACT_SAFE_APPLY_TYPES.includes(decisionType)) {
    return { ok: false, reason: "NOT_SAFE_APPLY_TYPE" };
  }
  if (technicalFallback || policyFallback) {
    return { ok: false, reason: "FALLBACK" };
  }
  if (!isValidChoice(decisionType, jevChoice)) {
    return { ok: false, reason: "INVALID_CHOICE" };
  }
  if (confidence != null && Number(confidence) < MIN_SAFE_APPLY_CONFIDENCE) {
    return { ok: false, reason: "LOW_CONFIDENCE" };
  }
  // Never let Jev invent a domain path when we have no domain
  if (
    decisionType === CONTACT_JEV_DECISION.CONTACT_SOURCE_PATH &&
    jevChoice !== CONTACT_SOURCE_PATH.DOMAIN_RESOLUTION &&
    jevChoice !== CONTACT_SOURCE_PATH.NONE &&
    !domainState?.host &&
    domainState?.confidence === "NO_OFFICIAL_DOMAIN"
  ) {
    return { ok: false, reason: "NO_DOMAIN_FOR_SOURCE_PATH" };
  }
  // Never apply STOP_SUFFICIENT when deterministic says CONTINUE and tier is empty
  if (
    decisionType === CONTACT_JEV_DECISION.STOP_CONTACT_RESEARCH &&
    jevChoice === STOP_CONTACT_RESEARCH.STOP_SUFFICIENT_PATH &&
    deterministicChoice === STOP_CONTACT_RESEARCH.CONTINUE
  ) {
    return { ok: false, reason: "STOP_SUFFICIENT_WITHOUT_PATH" };
  }
  return { ok: true, reason: null };
}

/**
 * Evaluate Jev contact routing with SAFE APPLY for level-1 types.
 */
export async function evaluateContactJevRouting({
  opportunity,
  domainState,
  gdiRoute,
  enableSafeApply = true,
} = {}) {
  const route = gdiRoute || computeGdiContactRouting(opportunity, domainState);
  const context = buildContactJevContext(opportunity, domainState, route);

  const specs = [
    {
      type: CONTACT_JEV_DECISION.CONTACT_SOURCE_PATH,
      existing: route.sourcePath,
      fallback: route.sourcePath,
      level: JEV_CONTACT_APPLY_LEVEL.SAFE_APPLY,
    },
    {
      type: CONTACT_JEV_DECISION.CONTACT_FOLLOWUP_TYPE,
      existing: route.followupType,
      fallback: route.followupType,
      level: JEV_CONTACT_APPLY_LEVEL.SAFE_APPLY,
    },
    {
      type: CONTACT_JEV_DECISION.STOP_CONTACT_RESEARCH,
      existing: route.stopContactResearch,
      fallback: route.stopContactResearch,
      level: JEV_CONTACT_APPLY_LEVEL.SAFE_APPLY,
    },
    {
      type: CONTACT_JEV_DECISION.NAMED_PERSON_WORTH_PURSUING,
      existing: route.namedPersonWorthPursuing,
      fallback: route.namedPersonWorthPursuing,
      level: JEV_CONTACT_APPLY_LEVEL.SHADOW,
    },
    {
      type: CONTACT_JEV_DECISION.FUNCTIONAL_PATH_SUFFICIENT,
      existing: route.functionalPathSufficient,
      fallback: route.functionalPathSufficient,
      level: JEV_CONTACT_APPLY_LEVEL.SHADOW,
    },
  ];

  const decisions = [];
  let techFallbacks = 0;
  let policyFallbacks = 0;
  let calls = 0;
  let safeApplyCount = 0;
  let shadowCount = 0;

  const appliedRoute = { ...route };

  for (const spec of specs) {
    calls += 1;
    const forceShadow = spec.level === JEV_CONTACT_APPLY_LEVEL.SHADOW;
    const result = await decide({
      decisionType: spec.type,
      context,
      existingDecision: spec.existing,
      policyContext: {
        noValidSource: !domainState?.host && !opportunity.officialSource,
        whoHowSeparation: true,
        noInventPerson: true,
      },
      forceShadow,
      fallbackDecision: spec.fallback,
    });

    if (result.technicalFallback) techFallbacks += 1;
    if (result.policyFallback || result.lowConfidenceFallback) policyFallbacks += 1;

    const jevSelected = result.selected || result.effectiveDecision || spec.fallback;
    const agreement = String(jevSelected) === String(spec.existing);
    let applied = false;
    let applyGate = { ok: false, reason: "SHADOW_ONLY" };

    if (
      enableSafeApply &&
      spec.level === JEV_CONTACT_APPLY_LEVEL.SAFE_APPLY
    ) {
      applyGate = canSafeApplyJevDecision({
        decisionType: spec.type,
        jevChoice: jevSelected,
        deterministicChoice: spec.existing,
        confidence: result.confidence,
        domainState,
        technicalFallback: result.technicalFallback,
        policyFallback: result.policyFallback || result.lowConfidenceFallback,
      });
      if (applyGate.ok && !agreement) {
        applied = true;
        safeApplyCount += 1;
        if (spec.type === CONTACT_JEV_DECISION.CONTACT_SOURCE_PATH) {
          appliedRoute.sourcePath = jevSelected;
        } else if (spec.type === CONTACT_JEV_DECISION.CONTACT_FOLLOWUP_TYPE) {
          appliedRoute.followupType = jevSelected;
        } else if (spec.type === CONTACT_JEV_DECISION.STOP_CONTACT_RESEARCH) {
          appliedRoute.stopContactResearch = jevSelected;
        }
      }
    } else {
      shadowCount += 1;
    }

    decisions.push({
      decisionType: spec.type,
      level: spec.level,
      deterministicDecision: spec.existing,
      jevDecision: jevSelected,
      confidence: result.confidence ?? null,
      agreement,
      applied,
      applyGateReason: applyGate.reason,
      finalDecision: applied ? jevSelected : spec.existing,
      technicalFallback: Boolean(result.technicalFallback),
      policyFallback: Boolean(result.policyFallback || result.lowConfidenceFallback),
      decisionId: result.decisionId || null,
      evaluatedAt: new Date().toISOString(),
      quality: agreement
        ? JEV_QUALITY.SAME
        : result.technicalFallback
          ? JEV_QUALITY.UNKNOWN
          : JEV_QUALITY.UNKNOWN, // scored post-recovery
    });
  }

  return {
    calls,
    techFallbacks,
    policyFallbacks,
    safeApplyCount,
    shadowCount,
    decisions,
    gdiRoute: route,
    appliedRoute,
    productionRoute: enableSafeApply ? appliedRoute : route,
    personSelectionApply: false,
    safeApplyEnabled: enableSafeApply,
  };
}

/**
 * Score Jev decisions after recovery outcome is known.
 */
export function scoreJevContactQuality({
  routingEval,
  recoveryResult,
  improved,
  beforeGrade,
  afterGrade,
} = {}) {
  const tallies = {
    SAME: 0,
    HELPFUL_DIFFERENT: 0,
    WRONG: 0,
    UNKNOWN: 0,
    HIGH_CONFIDENCE_WRONG: 0,
  };
  const scored = [];

  for (const d of routingEval?.decisions || []) {
    let quality = JEV_QUALITY.UNKNOWN;
    if (d.agreement) {
      quality = JEV_QUALITY.SAME;
    } else if (d.technicalFallback || d.policyFallback) {
      quality = JEV_QUALITY.UNKNOWN;
    } else if (d.level === JEV_CONTACT_APPLY_LEVEL.SAFE_APPLY && d.applied) {
      if (improved || (afterGrade && beforeGrade && afterGrade < beforeGrade === false && afterGrade !== beforeGrade)) {
        // letter grades: A best — use rank
        const rank = { E: 0, D: 1, C: 2, B: 3, A: 4 };
        const uplift =
          (rank[afterGrade] ?? 0) > (rank[beforeGrade] ?? 0) || improved;
        quality = uplift ? JEV_QUALITY.HELPFUL_DIFFERENT : JEV_QUALITY.SAME;
      } else if (
        d.decisionType === CONTACT_JEV_DECISION.STOP_CONTACT_RESEARCH &&
        d.jevDecision === STOP_CONTACT_RESEARCH.CONTINUE &&
        !improved
      ) {
        quality = JEV_QUALITY.SAME; // continued but no uplift — not wrong
      } else {
        quality = JEV_QUALITY.UNKNOWN;
      }
    } else if (
      d.level === JEV_CONTACT_APPLY_LEVEL.SHADOW &&
      d.decisionType === CONTACT_JEV_DECISION.NAMED_PERSON_WORTH_PURSUING
    ) {
      // Advisory — never count as WRONG person selection (not applied)
      quality = d.agreement ? JEV_QUALITY.SAME : JEV_QUALITY.UNKNOWN;
    } else {
      quality = d.agreement ? JEV_QUALITY.SAME : JEV_QUALITY.UNKNOWN;
    }

    // High-confidence wrong: applied STOP_SUFFICIENT but still E with no path
    if (
      d.applied &&
      d.decisionType === CONTACT_JEV_DECISION.STOP_CONTACT_RESEARCH &&
      d.jevDecision === STOP_CONTACT_RESEARCH.STOP_SUFFICIENT_PATH &&
      afterGrade === "E" &&
      Number(d.confidence) >= 0.7
    ) {
      quality = JEV_QUALITY.WRONG;
      tallies.HIGH_CONFIDENCE_WRONG += 1;
    }

    tallies[quality] = (tallies[quality] || 0) + 1;
    scored.push({ ...d, quality, outcome: recoveryResult || null });
  }

  return { tallies, decisions: scored };
}

export {
  CONTACT_JEV_DECISION,
  CONTACT_SOURCE_PATH,
  CONTACT_FOLLOWUP_TYPE,
  NAMED_PERSON_WORTH,
  FUNCTIONAL_SUFFICIENCY,
  STOP_CONTACT_RESEARCH,
  CHOICES,
};
