/**
 * GDI Contact Intelligence Completeness V1 — end-to-end gap closure.
 *
 * Wraps V1.2 recovery with:
 *   - research population selection
 *   - commercial-motion role ladder metadata
 *   - Jev SAFE APPLY for source / followup / stop
 *   - Jev SHADOW for person-worth / functional sufficiency
 *   - public-data ceiling + nextContactResearchAt
 *   - customer drawer model
 *   - WHO/HOW separation; Surfe auto = 0
 */

import {
  CONTACT_TIER,
  classifyContactTier,
  stripSurfeProviderPii,
} from "./contact-tiers-v1-2.js";
import {
  recoverOfficialContactSources,
  resolveDomainFromOpportunity,
  RECOVERY_RESULT,
  STOP_CONTACT_RESEARCH,
  computeGdiContactRouting,
} from "./contact-source-recovery-v1-1.js";
import {
  CONTACT_GAP_REASON,
  CONTACT_RESEARCH_STATUS,
  shouldSkipDeepContactResearch,
  classifyContactGapReason,
} from "./contact-gap-classify-v1-2.js";
import { resolveOpportunityContact } from "./weekly-contact-resolution-v1-2.js";
import {
  CONTACT_GRADE,
  DEFAULT_CONTACT_RESEARCH_BUDGET,
  gradeContactCompleteness,
  selectContactResearchPopulation,
  summarizeContactBaseline,
  attachCompletenessFields,
  computeGradeUplift,
  mapGapToCeilingReason,
  computeNextContactResearchAt,
  splitWhoHow,
  PUBLIC_CONTACT_CEILING_REASON,
  preferredRolesForOpportunity,
} from "./contact-completeness-v1.js";
import {
  evaluateContactJevRouting,
  scoreJevContactQuality,
  JEV_CONTACT_SAFE_APPLY_TYPES,
} from "./contact-jev-routing-v1.js";

const TIER_RANK = Object.freeze({
  [CONTACT_TIER.NO_CONTACT]: 0,
  [CONTACT_TIER.GENERIC_ONLY]: 1,
  [CONTACT_TIER.ORGANIZATION_PATH]: 2,
  [CONTACT_TIER.FUNCTIONAL_CONTACT]: 3,
  [CONTACT_TIER.NAMED_PARTIAL]: 4,
  [CONTACT_TIER.NAMED_DIRECT]: 5,
});

function tierImproved(before, after) {
  return (TIER_RANK[after] ?? 0) > (TIER_RANK[before] ?? 0);
}

/**
 * Resolve one weak/empty opportunity with Completeness V1 policy.
 */
export async function resolveContactCompletenessV1(opportunity = {}, opts = {}) {
  const started = Date.now();
  const budget = {
    ...DEFAULT_CONTACT_RESEARCH_BUDGET,
    ...(opts.budget || {}),
  };
  const before = gradeContactCompleteness(opportunity);
  const domainBefore = resolveDomainFromOpportunity(opportunity);
  const gapBefore = classifyContactGapReason(opportunity, {
    domainState: domainBefore,
  });
  const skip = shouldSkipDeepContactResearch(opportunity);
  const roles = preferredRolesForOpportunity(opportunity);

  if (skip.skip) {
    const next = attachCompletenessFields(opportunity, {
      unresolvedContactReason: skip.reason,
      publicContactCeilingReason: mapGapToCeilingReason(skip.reason, opportunity),
      contactResearchStatus: CONTACT_RESEARCH_STATUS.SKIPPED,
      lastContactResearchAt: new Date().toISOString(),
    });
    return {
      opportunity: next,
      beforeGrade: before.grade,
      afterGrade: before.grade,
      beforeTier: before.tier,
      afterTier: before.tier,
      improved: false,
      skipped: true,
      softStopped: false,
      namedPersonFound: false,
      gapReason: skip.reason,
      stopReason: "SKIPPED_LOW_VALUE",
      jev: null,
      metrics: {
        searches: 0,
        fetches: 0,
        rendered: 0,
        runtimeMs: Date.now() - started,
      },
      surfeAuto: 0,
    };
  }

  if (skip.softStop) {
    const ceiling = mapGapToCeilingReason(skip.reason, opportunity);
    const next = attachCompletenessFields(opportunity, {
      unresolvedContactReason: skip.reason,
      publicContactCeilingReason: ceiling,
      nextContactResearchAt: computeNextContactResearchAt(opportunity, ceiling),
      contactResearchStatus: CONTACT_RESEARCH_STATUS.STOPPED_LOW_VALUE,
      lastContactResearchAt: new Date().toISOString(),
    });
    return {
      opportunity: next,
      beforeGrade: before.grade,
      afterGrade: before.grade,
      beforeTier: before.tier,
      afterTier: before.tier,
      improved: false,
      skipped: false,
      softStopped: true,
      namedPersonFound: false,
      gapReason: skip.reason,
      stopReason: "SOFT_STOP_PUBLIC_CEILING",
      jev: null,
      metrics: {
        searches: 0,
        fetches: 0,
        rendered: 0,
        runtimeMs: Date.now() - started,
      },
      surfeAuto: 0,
    };
  }

  // Respect nextContactResearchAt deferral unless force
  if (
    !opts.force &&
    opportunity.nextContactResearchAt &&
    Date.parse(opportunity.nextContactResearchAt) > Date.now() &&
    opportunity.publicContactCeilingReason
  ) {
    return {
      opportunity,
      beforeGrade: before.grade,
      afterGrade: before.grade,
      beforeTier: before.tier,
      afterTier: before.tier,
      improved: false,
      skipped: true,
      softStopped: false,
      namedPersonFound: Boolean(
        opportunity.primaryContactName || opportunity.primaryContact?.name
      ),
      gapReason: opportunity.unresolvedContactReason || gapBefore,
      stopReason: "DEFERRED_NEXT_RESEARCH_AT",
      jev: null,
      metrics: {
        searches: 0,
        fetches: 0,
        rendered: 0,
        runtimeMs: Date.now() - started,
      },
      surfeAuto: 0,
    };
  }

  const gdiRoute = computeGdiContactRouting(opportunity, domainBefore);

  // Jev routing: SAFE APPLY source/followup/stop; person ranking SHADOW
  let jevEval = null;
  let routeOverride = null;
  if (opts.skipJev !== true) {
    try {
      jevEval = await evaluateContactJevRouting({
        opportunity,
        domainState: domainBefore,
        gdiRoute,
        enableSafeApply: opts.enableSafeApply !== false,
      });
      routeOverride = {
        sourcePath: jevEval.productionRoute.sourcePath,
        followupType: jevEval.productionRoute.followupType,
        stopContactResearch: jevEval.productionRoute.stopContactResearch,
      };
    } catch (err) {
      jevEval = {
        calls: 0,
        error: err.message || String(err),
        decisions: [],
        productionRoute: gdiRoute,
        personSelectionApply: false,
      };
    }
  }

  // Runtime budget hard stop
  const remainingMs = budget.maxRuntimeMs - (Date.now() - started);
  if (remainingMs < 5000) {
    const ceiling = PUBLIC_CONTACT_CEILING_REASON.PUBLIC_DATA_CEILING;
    const next = attachCompletenessFields(opportunity, {
      unresolvedContactReason: gapBefore,
      publicContactCeilingReason: ceiling,
      nextContactResearchAt: computeNextContactResearchAt(opportunity, ceiling),
      contactResearchStatus: CONTACT_RESEARCH_STATUS.STOPPED_NO_EVIDENCE,
    });
    return {
      opportunity: next,
      beforeGrade: before.grade,
      afterGrade: before.grade,
      beforeTier: before.tier,
      afterTier: before.tier,
      improved: false,
      skipped: false,
      softStopped: false,
      namedPersonFound: false,
      gapReason: gapBefore,
      stopReason: "RUNTIME_BUDGET",
      jev: jevEval,
      metrics: {
        searches: 0,
        fetches: 0,
        rendered: 0,
        runtimeMs: Date.now() - started,
      },
      surfeAuto: 0,
    };
  }

  const recovery = await recoverOfficialContactSources(opportunity, {
    allowNetworkDomainResolution: opts.allowNetworkDomainResolution !== false,
    budget: {
      maxDomainQueries: budget.maxDomainQueries,
      maxAdditionalFetches: Math.min(
        budget.maxAdditionalFetches,
        opts.maxFetchesPerOpp || budget.maxAdditionalFetches
      ),
      fetchTimeoutMs: budget.fetchTimeoutMs,
    },
    fetchPage: opts.fetchPage,
    routeOverride,
  });

  let working = recovery.opportunity || opportunity;
  // Re-run weekly contact resolve for consistency (Surfe never auto)
  try {
    const resolved = await resolveOpportunityContact(working, {
      surfeAuto: false,
      allowSurfe: false,
    });
    working = resolved?.opportunity || working;
  } catch {
    /* keep recovery opportunity */
  }

  if (working.primaryContact) {
    working.primaryContact = stripSurfeProviderPii(working.primaryContact, {
      surfeUsed: false,
    });
  }

  const afterTier = classifyContactTier(working);
  const after = gradeContactCompleteness(working);
  const improved = tierImproved(before.tier, afterTier);
  const whoHow = splitWhoHow(working);

  let stopReason = "COMPLETED";
  let ceiling = null;
  let status = improved
    ? CONTACT_RESEARCH_STATUS.IMPROVED
    : CONTACT_RESEARCH_STATUS.UNCHANGED;

  if (
    after.grade === CONTACT_GRADE.A ||
    after.grade === CONTACT_GRADE.B
  ) {
    stopReason = "STOP_SUFFICIENT_NAMED";
    status = CONTACT_RESEARCH_STATUS.STOPPED_SUFFICIENT;
  } else if (after.grade === CONTACT_GRADE.C) {
    stopReason = "STOP_SUFFICIENT_FUNCTIONAL";
    status = CONTACT_RESEARCH_STATUS.STOPPED_SUFFICIENT;
    ceiling = PUBLIC_CONTACT_CEILING_REASON.PUBLIC_DATA_CEILING;
  } else if (after.grade === CONTACT_GRADE.D) {
    stopReason = "STOP_ORG_PATH_CEILING";
    ceiling = PUBLIC_CONTACT_CEILING_REASON.ORGANIZATION_PATH_ONLY;
    status = CONTACT_RESEARCH_STATUS.STOPPED_NO_EVIDENCE;
  } else {
    stopReason = "NO_CONTACT_AFTER_RESEARCH";
    ceiling = mapGapToCeilingReason(
      classifyContactGapReason(working, { domainState: recovery.domainState }),
      working
    );
    status = CONTACT_RESEARCH_STATUS.STOPPED_NO_EVIDENCE;
  }

  const unresolved =
    after.grade === CONTACT_GRADE.A ||
    after.grade === CONTACT_GRADE.B ||
    after.grade === CONTACT_GRADE.C
      ? null
      : ceiling ||
        classifyContactGapReason(working, { domainState: recovery.domainState });

  let next = attachCompletenessFields(working, {
    preferredContactRoles: roles.preferredRoles,
    commercialMotion: roles.commercialMotion,
    unresolvedContactReason: unresolved,
    publicContactCeilingReason: ceiling,
    nextContactResearchAt:
      after.grade === CONTACT_GRADE.A || after.grade === CONTACT_GRADE.B
        ? null
        : computeNextContactResearchAt(working, ceiling),
    contactResearchStatus: status,
    contactResearchResult: recovery.result,
    lastContactResearchAt: new Date().toISOString(),
    contactResearchAudit: {
      ...(working.contactResearchAudit || {}),
      version: "contact_completeness_v1",
      beforeGrade: before.grade,
      afterGrade: after.grade,
      beforeTier: before.tier,
      afterTier,
      gapReasonBefore: gapBefore,
      stopReason,
      ladderBudget: budget,
      gdiSourcePath: gdiRoute.sourcePath,
      appliedSourcePath: routeOverride?.sourcePath || gdiRoute.sourcePath,
      jev: jevEval
        ? {
            calls: jevEval.calls,
            safeApplyCount: jevEval.safeApplyCount || 0,
            shadowCount: jevEval.shadowCount || 0,
            personSelectionApply: false,
            safeApplyTypes: [...JEV_CONTACT_SAFE_APPLY_TYPES],
            decisions: (jevEval.decisions || []).map((d) => ({
              type: d.decisionType,
              level: d.level,
              deterministic: d.deterministicDecision,
              jev: d.jevDecision,
              final: d.finalDecision,
              applied: d.applied,
              confidence: d.confidence,
              agreement: d.agreement,
            })),
          }
        : null,
      surfeAuto: 0,
      at: new Date().toISOString(),
    },
  });

  // Score Jev quality post-outcome
  let jevQuality = null;
  if (jevEval?.decisions?.length) {
    jevQuality = scoreJevContactQuality({
      routingEval: jevEval,
      recoveryResult: recovery.result,
      improved,
      beforeGrade: before.grade,
      afterGrade: after.grade,
    });
    next.contactResearchAudit = {
      ...next.contactResearchAudit,
      jevQuality: jevQuality.tallies,
    };
  }

  return {
    opportunity: next,
    beforeGrade: before.grade,
    afterGrade: after.grade,
    beforeTier: before.tier,
    afterTier,
    improved,
    skipped: false,
    softStopped: false,
    namedPersonFound: whoHow.whoResolved,
    role: whoHow.who.title || whoHow.who.roleClass,
    contactPath:
      next.customerContactDrawer?.functionalPath ||
      next.commercialContactPathLabel ||
      null,
    gapReason: gapBefore,
    unresolvedReason: unresolved,
    stopReason,
    ceilingReason: ceiling,
    uplift: computeGradeUplift(before.grade, after.grade),
    whoHow,
    jev: jevEval,
    jevQuality,
    recovery,
    result: recovery.result,
    metrics: {
      searches: recovery.metrics?.domainQueries || 0,
      fetches: recovery.metrics?.additionalFetches || 0,
      rendered: 0,
      namedPeople: recovery.metrics?.namedPeople || 0,
      functionalContacts: recovery.metrics?.functionalContacts || 0,
      publicEmails: recovery.metrics?.publicEmails || 0,
      publicPhones: recovery.metrics?.publicPhones || 0,
      runtimeMs: Date.now() - started,
    },
    surfeAuto: 0,
    surfeEligible: whoHow.whoResolvedHowMissing,
  };
}

/**
 * Batch Completeness V1 over a research population.
 */
export async function resolveContactCompletenessBatch(
  opportunities = [],
  opts = {}
) {
  const population =
    opts.usePopulation !== false
      ? selectContactResearchPopulation(opportunities)
      : (opportunities || []).map((o) => ({ opportunity: o, id: o.id }));

  const rows = [];
  let upgraded = 0;
  let namedFound = 0;
  let functionalUplift = 0;
  let stillE = 0;
  let searches = 0;
  let fetches = 0;
  let jevCalls = 0;
  let jevSafeApply = 0;
  const jevQualityTotals = {
    SAME: 0,
    HELPFUL_DIFFERENT: 0,
    WRONG: 0,
    UNKNOWN: 0,
    HIGH_CONFIDENCE_WRONG: 0,
  };
  const upliftBuckets = {
    E_TO_AB: 0,
    E_TO_C: 0,
    E_TO_D: 0,
    D_TO_AB: 0,
    C_TO_AB: 0,
  };
  const ceilingReasons = {};
  let whoResolved = 0;
  let whoResolvedHowMissing = 0;
  let howComplete = 0;

  for (const entry of population) {
    const opp = entry.opportunity || entry;
    const r = await resolveContactCompletenessV1(opp, opts);
    rows.push({
      id: opp.id,
      title: (opp.title || "").slice(0, 80),
      beforeGrade: r.beforeGrade,
      afterGrade: r.afterGrade,
      beforeTier: r.beforeTier,
      afterTier: r.afterTier,
      improved: r.improved,
      namedPersonFound: r.namedPersonFound,
      role: r.role,
      contactPath: r.contactPath,
      stopReason: r.stopReason,
      ceilingReason: r.ceilingReason,
      nextContactResearchAt: r.opportunity?.nextContactResearchAt || null,
      metrics: r.metrics,
      jevCalls: r.jev?.calls || 0,
      jevSafeApply: r.jev?.safeApplyCount || 0,
      surfeEligible: r.surfeEligible,
      opportunity: r.opportunity,
    });

    if (r.improved) upgraded += 1;
    if (r.whoHow?.whoResolved) whoResolved += 1;
    if (r.whoHow?.whoResolvedHowMissing) whoResolvedHowMissing += 1;
    if (r.whoHow?.howComplete) howComplete += 1;

    const b = r.beforeGrade;
    const a = r.afterGrade;
    if (b === "E" && (a === "A" || a === "B")) upliftBuckets.E_TO_AB += 1;
    if (b === "E" && a === "C") {
      upliftBuckets.E_TO_C += 1;
      functionalUplift += 1;
    }
    if (b === "E" && a === "D") upliftBuckets.E_TO_D += 1;
    if (b === "D" && (a === "A" || a === "B")) upliftBuckets.D_TO_AB += 1;
    if (b === "C" && (a === "A" || a === "B")) upliftBuckets.C_TO_AB += 1;
    if (a === "E") {
      stillE += 1;
      const cr = r.ceilingReason || "OTHER";
      ceilingReasons[cr] = (ceilingReasons[cr] || 0) + 1;
    }

    searches += r.metrics?.searches || 0;
    fetches += r.metrics?.fetches || 0;
    jevCalls += r.jev?.calls || 0;
    jevSafeApply += r.jev?.safeApplyCount || 0;
    if (r.jevQuality?.tallies) {
      for (const [k, v] of Object.entries(r.jevQuality.tallies)) {
        jevQualityTotals[k] = (jevQualityTotals[k] || 0) + v;
      }
    }
  }

  // Fix double-count: recount named from rows
  namedFound = rows.filter((r) => r.namedPersonFound).length;

  return {
    rows,
    summary: {
      researched: rows.length,
      upgraded,
      namedPeopleFound: namedFound,
      functionalUplift,
      stillNoContact: stillE,
      upliftBuckets,
      ceilingReasons,
      searches,
      fetches,
      jevCalls,
      jevSafeApply,
      jevQuality: jevQualityTotals,
      whoResolved,
      whoResolvedHowMissing,
      howComplete,
      surfeAuto: 0,
      personSelectionApply: false,
      safeApplyTypes: [...JEV_CONTACT_SAFE_APPLY_TYPES],
    },
  };
}

export {
  selectContactResearchPopulation,
  summarizeContactBaseline,
  gradeContactCompleteness,
  attachCompletenessFields,
  CONTACT_GRADE,
  PUBLIC_CONTACT_CEILING_REASON,
  JEV_CONTACT_SAFE_APPLY_TYPES,
};
