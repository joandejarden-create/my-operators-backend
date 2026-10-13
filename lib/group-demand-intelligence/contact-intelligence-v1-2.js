/**
 * GDI Contact Intelligence V1.2 — bounded gap closure for remaining blanks.
 *
 * Flow:
 *   gap classify → skip low-value → reuse/recover official sources → dual extract
 *   → persist public contact + research outcome → Jev shadow (advisory only)
 *
 * Surfe stays on-demand / ephemeral. No invented people. WHO-first.
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
} from "./contact-source-recovery-v1-1.js";
import {
  evaluateContactJevShadow,
  scoreJevRoutingOutcome,
} from "./contact-jev-shadow-v1-1.js";
import {
  CONTACT_GAP_REASON,
  CONTACT_RESEARCH_STATUS,
  shouldSkipDeepContactResearch,
  classifyContactGapReason,
  buildContactGapAuditRow,
} from "./contact-gap-classify-v1-2.js";
import { resolveOpportunityContact } from "./weekly-contact-resolution-v1-2.js";

const TIER_RANK = Object.freeze({
  [CONTACT_TIER.NO_CONTACT]: 0,
  [CONTACT_TIER.GENERIC_ONLY]: 1,
  [CONTACT_TIER.ORGANIZATION_PATH]: 2,
  [CONTACT_TIER.FUNCTIONAL_CONTACT]: 3,
  [CONTACT_TIER.NAMED_PARTIAL]: 4,
  [CONTACT_TIER.NAMED_DIRECT]: 5,
});

export function tierRank(tier) {
  return TIER_RANK[tier] ?? 0;
}

function nextBestPathFromRoute(gdiRoute = {}, gapReason = null) {
  if (gapReason === CONTACT_GAP_REASON.ACCOUNT_LEVEL_EVIDENCE_REQUIRED) {
    return "ACCOUNT_LEVEL_PROVIDER_OR_CRM";
  }
  if (gapReason === CONTACT_GAP_REASON.LOW_VALUE_NO_DEEP_RESEARCH) {
    return "NONE_SKIPPED";
  }
  if (!gdiRoute) return "OFFICIAL_DOMAIN_OR_STAFF";
  if (gdiRoute.stopContactResearch === STOP_CONTACT_RESEARCH.STOP_SUFFICIENT_PATH) {
    return "PROVIDER_REVEAL_ON_DEMAND";
  }
  return gdiRoute.sourcePath || gdiRoute.followupType || "OFFICIAL_SOURCE_PATH";
}

function attachResearchOutcome(opportunity, {
  beforeTier,
  afterTier,
  gapReason,
  status,
  result,
  gdiRoute,
  domainState,
  jevShadow = null,
} = {}) {
  const next = { ...opportunity };
  next.contactTier = afterTier;
  next.lastContactResearchAt = new Date().toISOString();
  next.contactResearchStatus = status;
  next.contactResearchResult = result || null;
  next.unresolvedContactReason =
    afterTier === CONTACT_TIER.NAMED_DIRECT ||
    afterTier === CONTACT_TIER.NAMED_PARTIAL ||
    afterTier === CONTACT_TIER.FUNCTIONAL_CONTACT
      ? null
      : gapReason || classifyContactGapReason(next, { domainState });
  next.nextBestContactPath = nextBestPathFromRoute(gdiRoute, next.unresolvedContactReason);
  if (domainState?.host && !next.officialDomain) {
    next.officialDomain = domainState.host;
  }
  if (domainState?.seedUrl && !next.officialSource && domainState.confidence !== "AMBIGUOUS") {
    // Only set when recovery already accepted the URL
    if (next.officialSource || next.contactOfficialUrl) {
      /* keep existing */
    }
  }
  next.contactResearchAudit = {
    ...(next.contactResearchAudit || {}),
    version: "contact_intelligence_v1_2",
    beforeTier,
    afterTier,
    gapReasonBefore: gapReason,
    unresolvedReason: next.unresolvedContactReason,
    nextBestPath: next.nextBestContactPath,
    domainConfidence: domainState?.confidence || null,
    gdiSourcePath: gdiRoute?.sourcePath || null,
    gdiStop: gdiRoute?.stopContactResearch || null,
    jevShadow: jevShadow
      ? {
          callCount: jevShadow.calls || jevShadow.callCount || 0,
          technicalFallbacks: jevShadow.techFallbacks || 0,
          policyFallbacks: jevShadow.policyFallbacks || 0,
          decisions: (jevShadow.decisions || []).map((d) => ({
            type: d.decisionType,
            choice: d.jevRoute,
            confidence: d.jevConfidence,
            shadow: true,
          })),
        }
      : null,
    at: next.lastContactResearchAt,
  };
  if (next.primaryContact) {
    next.primaryContact = stripSurfeProviderPii(next.primaryContact, { surfeUsed: false });
  }
  return next;
}

/**
 * Close one contact gap with V1.2 policy (skip / soft-stop / recover / extract).
 */
export async function resolveContactGapV12(opportunity = {}, opts = {}) {
  const beforeTier = classifyContactTier(opportunity);
  const domainBefore = resolveDomainFromOpportunity(opportunity);
  const gapBefore = classifyContactGapReason(opportunity, { domainState: domainBefore });
  const skip = shouldSkipDeepContactResearch(opportunity);

  if (skip.skip) {
    const next = attachResearchOutcome(opportunity, {
      beforeTier,
      afterTier: beforeTier,
      gapReason: skip.reason,
      status: CONTACT_RESEARCH_STATUS.SKIPPED,
      result: RECOVERY_RESULT.STOPPED,
      gdiRoute: { sourcePath: "NONE", stopContactResearch: STOP_CONTACT_RESEARCH.STOP_LOW_VALUE },
      domainState: domainBefore,
    });
    return {
      opportunity: next,
      beforeTier,
      afterTier: beforeTier,
      improved: false,
      skipped: true,
      gapReason: skip.reason,
      result: RECOVERY_RESULT.STOPPED,
      pagesReused: 0,
      additionalFetches: 0,
      domainQueries: 0,
      jev: null,
      recovery: null,
      pageResolve: null,
    };
  }

  // Soft-stop category watches: optional single domain attempt only when configured
  if (skip.softStop && opts.forceAccountWatch !== true) {
    const next = attachResearchOutcome(opportunity, {
      beforeTier,
      afterTier: beforeTier,
      gapReason: skip.reason,
      status: CONTACT_RESEARCH_STATUS.STOPPED_LOW_VALUE,
      result: RECOVERY_RESULT.STOPPED,
      gdiRoute: {
        sourcePath: "NONE",
        stopContactResearch: STOP_CONTACT_RESEARCH.STOP_LOW_VALUE,
      },
      domainState: domainBefore,
    });
    return {
      opportunity: next,
      beforeTier,
      afterTier: beforeTier,
      improved: false,
      skipped: false,
      softStopped: true,
      gapReason: skip.reason,
      result: RECOVERY_RESULT.STOPPED,
      pagesReused: 0,
      additionalFetches: 0,
      domainQueries: 0,
      jev: null,
      recovery: null,
      pageResolve: null,
    };
  }

  // 1) Reuse existing official URLs via weekly dual extract when present
  let working = opportunity;
  let pageResolve = null;
  const knownUrls = [
    opportunity.officialSource,
    opportunity.contactOfficialUrl,
    opportunity.discoverySource,
  ].filter((u) => /^https?:\/\//i.test(String(u || "")));

  if (knownUrls.length && tierRank(beforeTier) < tierRank(CONTACT_TIER.FUNCTIONAL_CONTACT)) {
    pageResolve = await resolveOpportunityContact(opportunity, opts);
    working = pageResolve.opportunity || opportunity;
  }

  let midTier = classifyContactTier(working);

  // 2) Official source path recovery when still weak
  let recovery = null;
  if (
    tierRank(midTier) <= tierRank(CONTACT_TIER.ORGANIZATION_PATH) ||
    midTier === CONTACT_TIER.GENERIC_ONLY
  ) {
    recovery = await recoverOfficialContactSources(working, {
      allowNetworkDomainResolution: opts.allowNetworkDomainResolution !== false,
      budget: opts.budget,
      fetchPage: opts.fetchPage,
    });
    working = recovery.opportunity || working;
  }

  const afterTier = classifyContactTier(working);
  const domainAfter = recovery?.domainState || resolveDomainFromOpportunity(working);
  const gdiRoute = recovery?.gdiRoute || null;

  // 3) Jev shadow — advisory only
  let jev = null;
  if (opts.jevShadow !== false) {
    jev = await evaluateContactJevShadow({
      opportunity: working,
      domainState: domainAfter,
      gdiRoute,
      forceShadow: true,
    });
    if (jev && gdiRoute) {
      jev.outcomeScores = scoreJevRoutingOutcome({
        shadowEval: jev,
        recoveryResult: recovery?.result || RECOVERY_RESULT.NO_IMPROVEMENT,
        actualSourceTypes: (recovery?.sourceYield || []).map((s) => s.sourceType).filter(Boolean),
      });
    }
  }

  const improved = tierRank(afterTier) > tierRank(beforeTier);
  let status = CONTACT_RESEARCH_STATUS.UNCHANGED;
  let result = recovery?.result || RECOVERY_RESULT.NO_IMPROVEMENT;
  if (improved) {
    status = CONTACT_RESEARCH_STATUS.IMPROVED;
  } else if (
    afterTier === CONTACT_TIER.FUNCTIONAL_CONTACT ||
    afterTier === CONTACT_TIER.NAMED_PARTIAL ||
    afterTier === CONTACT_TIER.NAMED_DIRECT
  ) {
    status = CONTACT_RESEARCH_STATUS.STOPPED_SUFFICIENT;
    result = RECOVERY_RESULT.STOPPED;
  } else if (
    recovery?.result === RECOVERY_RESULT.STOPPED ||
    gapBefore === CONTACT_GAP_REASON.NO_CONTACT_ON_PUBLIC_WEB
  ) {
    status = CONTACT_RESEARCH_STATUS.STOPPED_NO_EVIDENCE;
  }

  const next = attachResearchOutcome(working, {
    beforeTier,
    afterTier,
    gapReason: gapBefore,
    status,
    result,
    gdiRoute,
    domainState: domainAfter,
    jevShadow: jev,
  });

  return {
    opportunity: next,
    beforeTier,
    afterTier,
    improved,
    skipped: false,
    softStopped: false,
    gapReason: gapBefore,
    unresolvedReason: next.unresolvedContactReason,
    nextBestPath: next.nextBestContactPath,
    result,
    pagesReused: knownUrls.length,
    additionalFetches: recovery?.metrics?.additionalFetches || 0,
    domainQueries: recovery?.metrics?.domainQueries || 0,
    namedPeople: recovery?.metrics?.namedPeople || 0,
    functionalContacts: recovery?.metrics?.functionalContacts || 0,
    publicEmails: recovery?.metrics?.publicEmails || 0,
    publicPhones: recovery?.metrics?.publicPhones || 0,
    jev,
    recovery,
    pageResolve,
  };
}

/**
 * Run V1.2 gap closure over a cohort (actionable weak contacts).
 */
export async function resolveContactGapsBatch(opportunities = [], opts = {}) {
  const rows = [];
  let improved = 0;
  let skipped = 0;
  let softStopped = 0;
  let fetches = 0;
  let queries = 0;
  let jevCalls = 0;
  let jevTech = 0;
  let jevPolicy = 0;
  let highConfWrong = 0;
  const byGap = {};

  for (const opp of opportunities) {
    const r = await resolveContactGapV12(opp, opts);
    rows.push({
      ...buildContactGapAuditRow(opp),
      beforeTier: r.beforeTier,
      afterTier: r.afterTier,
      improved: r.improved,
      skipped: r.skipped,
      softStopped: r.softStopped,
      gapReason: r.gapReason,
      unresolvedReason: r.unresolvedReason,
      nextBestPath: r.nextBestPath,
      result: r.result,
      pagesReused: r.pagesReused,
      additionalFetches: r.additionalFetches,
      namedPeople: r.namedPeople,
      functionalContacts: r.functionalContacts,
      publicEmails: r.publicEmails,
      publicPhones: r.publicPhones,
      opportunity: r.opportunity,
      jevOutcome: r.jev?.outcomeScores || null,
    });
    byGap[r.gapReason] = (byGap[r.gapReason] || 0) + 1;
    if (r.improved) improved += 1;
    if (r.skipped) skipped += 1;
    if (r.softStopped) softStopped += 1;
    fetches += r.additionalFetches || 0;
    queries += r.domainQueries || 0;
    if (r.jev) {
      jevCalls += r.jev.calls || r.jev.callCount || 0;
      jevTech += r.jev.techFallbacks || 0;
      jevPolicy += r.jev.policyFallbacks || 0;
      for (const s of Object.values(r.jev.outcomeScores || {})) {
        if (s?.highConfWrong) highConfWrong += s.highConfWrong;
      }
    }
  }

  return {
    rows,
    summary: {
      attempted: rows.length,
      improved,
      skipped,
      softStopped,
      stillWeak: rows.filter(
        (r) =>
          r.afterTier === CONTACT_TIER.NO_CONTACT ||
          r.afterTier === CONTACT_TIER.GENERIC_ONLY ||
          r.afterTier === CONTACT_TIER.ORGANIZATION_PATH
      ).length,
      fetches,
      queries,
      jevCalls,
      jevTech,
      jevPolicy,
      highConfWrong,
      byGapReason: byGap,
    },
  };
}

export {
  CONTACT_GAP_REASON,
  CONTACT_RESEARCH_STATUS,
  shouldSkipDeepContactResearch,
  classifyContactGapReason,
  buildContactGapAuditRow,
};
