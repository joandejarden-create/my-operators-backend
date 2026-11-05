/**
 * GDI 2.0 Phase 2E/2F — bounded adaptive PUBLIC research controller.
 *
 * Extends ORS / recovery / planner surfaces. Executes ONLY verified $0
 * direct_public / official_page fetches (PUBLIC_FETCH_COST_USD=0).
 * Hard-denies Exa, Firecrawl, SerpAPI, Surfe, PDL, Parallel.
 *
 * Phase 2F: buyer evidence gate, portfolio allocation, source efficiency,
 * adaptive next-action by commercial gap type.
 *
 * Does NOT mutate canonical opportunity storage, Airtable, or visibility.
 * Pilot ceilings: ≤5 actions/opportunity, ≤60 external retrievals total.
 */

import { evaluateSharedCommercialEligibility } from "./commercial-eligibility-contract-v1.js";
import { recoverCommercialEvidence } from "./commercial-evidence-recovery-v1.js";
import { planBuyerRecovery } from "./buyer-recovery-planner-v1.js";
import { buildAdaptiveResearchPreflight } from "./adaptive-research-preflight-v1.js";
import { classifyResearchContinuationDisposition } from "./research-continuation-disposition-v1.js";
import { evaluateOpportunity } from "./research/opportunity-research-supervisor-v1.js";
import { fetchOfficialPage } from "./official-page-fetch.js";
import {
  extractPublicEvidenceFromText,
  selectPreferredPublicEmail,
} from "./research/direct-public-executor-v1.js";
import { runPublicBuyerIntelligencePass } from "./research/commercial-depth-buyer-intelligence-v1.js";
import { verifiedProviderBilling } from "./workflow/gdi-provider-billing-guard-v1.js";
import { PUBLIC_FETCH_COST_USD } from "./workflow/gdi-evidence-acquisition-v2.js";
import { looksLikePersonName } from "./secondary-named-contact-v1.js";
import { redactOpportunityForShadowExport } from "./gdi2-shadow-publication-evaluation-v1.js";
import {
  evaluateBuyerEvidenceGate,
  shouldPromoteStakeholderAsBuyer,
  classifyQualificationIntegrity,
  BUYER_EVIDENCE_STAGE,
} from "./buyer-evidence-gate-v1.js";
import {
  PHASE2F_ALLOCATION_POLICY,
  buildResearchAllocationPlan,
  createAllocationState,
  selectNextAllocationSlot,
  recordAllocationOutcome,
  summarizeAllocationState,
} from "./research-allocation-scheduler-v1.js";
import {
  normalizeSourceUrl,
  selectResearchUrls,
  discoverOfficialNavLinks,
  updateSourceSignalMemory,
  recommendStrategyChange,
} from "./source-selection-efficiency-v1.js";
import {
  selectBuyerDiscoveryStrategies,
  discoveryStrategyToApproach,
} from "./buyer-discovery-strategies-v1.js";
import {
  buildDiscoveredFetchPlan,
  assessRetrievedPageQuality,
  extractAndCacheNav,
  assessEvidenceFreshnessAndContradiction,
} from "./source-discovery-v1.js";
import {
  classifyDemandMechanismEvidence,
  proposeDemandThesisUpdate,
  DEMAND_EVIDENCE_CLASS,
} from "./demand-mechanism-research-v1.js";

export const ADAPTIVE_PUBLIC_RESEARCH_CONTROLLER_V1 =
  "gdi-adaptive-public-research-controller-v1";

export const PHASE2E_PILOT_LIMITS = Object.freeze({
  maxActionsPerOpportunity: 5,
  maxExternalRetrievalsPilot: 60,
  maxUrlsPerAction: 3,
  allowRender: false,
  authorizePaidResearch: false,
});

export const PHASE2F_PILOT_LIMITS = Object.freeze({
  ...PHASE2E_PILOT_LIMITS,
  maxUrlsPerAction: 2,
  initialActionsPerOpportunity: PHASE2F_ALLOCATION_POLICY.initialActionsPerOpportunity,
  initialRetrievalsPerOpportunity: PHASE2F_ALLOCATION_POLICY.initialRetrievalsPerOpportunity,
  enforceBuyerEvidenceGate: true,
  usePortfolioAllocator: true,
  useSourceSelectionEfficiency: true,
});

export const PHASE2G_PILOT_LIMITS = Object.freeze({
  ...PHASE2F_PILOT_LIMITS,
  useBuyerDiscoveryStrategies: true,
  useSourceDiscoveryV1: true,
  useDemandMechanismResearch: true,
  hotelCommercialAlignment: true,
});

export const DENIED_METERED_PROVIDERS = Object.freeze([
  "exa",
  "firecrawl",
  "serpapi",
  "surfe",
  "pdl",
  "parallel",
  "jev",
]);

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function hostnameOf(url) {
  try {
    return new URL(url).hostname.replace(/^www\./i, "");
  } catch {
    return "";
  }
}

function originOf(url) {
  try {
    const u = new URL(url);
    return `${u.protocol}//${u.host}`;
  } catch {
    return "";
  }
}

/**
 * Verify Phase 2E may only use $0 official_page / direct_public.
 */
export function assertPhase2ePublicProviderAuthorization(env = process.env) {
  const billing = verifiedProviderBilling(env);
  const official = billing.official_page || {};
  const denied = {};
  for (const p of DENIED_METERED_PROVIDERS) {
    denied[p] = { authorized: false, reason: "phase2e_metered_provider_denied" };
  }
  const ok =
    official.enabledForCharge === true &&
    Number(official.verifiedChargeUsd) === 0 &&
    Number(PUBLIC_FETCH_COST_USD) === 0;
  return {
    ok,
    provider: "direct_public_official_page",
    verifiedChargeUsd: official.verifiedChargeUsd,
    publicFetchCostUsd: PUBLIC_FETCH_COST_USD,
    researchAuthorizationRequired: official.researchAuthorizationRequired === true,
    phase2ePilotAuthorization: true,
    authorizePaidResearch: false,
    deniedMeteredProviders: denied,
    note: ok
      ? "Official page fetch verified at $0. Metered providers hard-denied for Phase 2E."
      : "Refusing Phase 2E execution — official_page charge not verified as zero.",
  };
}

function collectSeedUrls(opportunity = {}) {
  const urls = [];
  const push = (u) => {
    const s = text(u);
    if (!/^https?:\/\//i.test(s)) return;
    if (/linkedin\.com|facebook\.com|twitter\.com|instagram\.com|youtube\.com/i.test(s)) {
      return;
    }
    urls.push(s);
  };
  for (const s of opportunity.sources || []) push(s?.url || s);
  for (const e of opportunity.evidence || []) push(e?.sourceUrl || e?.url);
  push(opportunity.officialSource);
  push(opportunity.sourceUrl);
  push(opportunity.primarySourceUrl);
  push(opportunity.contactUrl);
  push(opportunity.organizationContactUrl);
  push(opportunity.primaryContact?.sourceUrl);
  return [...new Set(urls)];
}

function deriveOfficialCandidateUrls(seedUrls = [], strategyId = "") {
  const out = [];
  const origins = [...new Set(seedUrls.map(originOf).filter(Boolean))];
  const pathHints = [];
  if (/LEADERSHIP|OFFICIAL/i.test(strategyId)) {
    pathHints.push("/about", "/about-us", "/team", "/leadership", "/our-team", "/company");
  }
  if (/MEETINGS|EVENT|HOUSING|DEMAND|CONTACT|ORG_ROUTE/i.test(strategyId) || !strategyId) {
    pathHints.push(
      "/contact",
      "/contact-us",
      "/contact.aspx",
      "/housing",
      "/hotel",
      "/hotels",
      "/accommodation",
      "/events",
      "/meetings",
    );
  }
  if (/WORKPLACE|FACILITIES|PEOPLE|HR/i.test(strategyId)) {
    pathHints.push("/about", "/team", "/careers", "/people");
  }
  for (const origin of origins.slice(0, 3)) {
    for (const path of pathHints) {
      out.push(`${origin}${path}`);
    }
  }
  // Prefer seeds first
  return [...new Set([...seedUrls, ...out])];
}

function extractTitleNearName(pageText, personName) {
  const name = text(personName);
  if (!name || name.length < 4) return "";
  const blob = String(pageText || "");
  const idx = blob.toLowerCase().indexOf(name.toLowerCase());
  if (idx < 0) return "";
  const window = blob.slice(Math.max(0, idx - 80), idx + name.length + 120);
  const titleMatch = window.match(
    /,\s*([A-Z][A-Za-z0-9&/’'\- ]{2,60}(?:Director|Manager|VP|Vice President|Officer|Lead|Chair|Coordinator|President)[A-Za-z0-9&/’'\- ]{0,40})/,
  );
  if (titleMatch) return text(titleMatch[1]).slice(0, 80);
  const roleMatch = window.match(
    /\b((?:Director|Manager|VP|Vice President|Head|Lead|Chair|Coordinator|President)[^.\n|]{0,50})/i,
  );
  return roleMatch ? text(roleMatch[1]).slice(0, 80) : "";
}

function cloneOpp(opportunity) {
  try {
    return structuredClone(opportunity);
  } catch {
    return JSON.parse(JSON.stringify(opportunity));
  }
}

function isNonCommercialOrgRoute(email) {
  return /^(recruiting|careers|jobs|talent|hr|press|media|pr|noreply|no-reply|support|helpdesk|billing)@/i.test(
    text(email),
  );
}

function applyExtractedEvidenceToClone(opportunity, extraction, buyerPass, meta = {}) {
  const next = cloneOpp(opportunity);
  const enforceGate = meta.enforceBuyerEvidenceGate !== false;
  next.gdi2Phase2eResearchV1 = next.gdi2Phase2eResearchV1 || {
    actions: [],
    sourcesAdded: [],
    evidenceAdded: [],
    candidateStakeholders: [],
    buyerGateDecisions: [],
  };
  next.gdi2Phase2fResearchV1 = next.gdi2Phase2fResearchV1 || {
    candidateStakeholders: [],
    buyerGateDecisions: [],
    nextActionLog: [],
  };

  const preferredEmail =
    extraction?.preferredEmail ||
    selectPreferredPublicEmail(extraction?.emails || []) ||
    null;

  // Org route only — never invent person-direct attribution for unrelated locals.
  // Phase 2F: reject recruiting/HR/press mailboxes as hotel-buyer routes.
  if (preferredEmail && !text(next.organizationContactEmail)) {
    const local = preferredEmail.split("@")[0] || "";
    const personName = text(next.primaryContact?.name || next.contactName || "");
    const tokens = personName
      .toLowerCase()
      .split(/\s+/)
      .map((t) => t.replace(/[^a-z]/g, ""))
      .filter((t) => t.length >= 3);
    const looksPersonLocal = tokens.some((t) => local.includes(t));
    if (!looksPersonLocal && !isNonCommercialOrgRoute(preferredEmail)) {
      next.organizationContactEmail = preferredEmail;
      next.functionalContactEmail = next.functionalContactEmail || preferredEmail;
    } else if (isNonCommercialOrgRoute(preferredEmail)) {
      next.gdi2Phase2fResearchV1.rejectedRoutes = [
        ...(next.gdi2Phase2fResearchV1.rejectedRoutes || []),
        {
          emailDomain: preferredEmail.split("@")[1] || null,
          localPrefix: local,
          reason: "non_commercial_hotel_route",
          sourceUrl: meta.url || null,
        },
      ];
    }
  }

  if (extraction?.hasHousingLanguage) {
    const thesisBits = [
      text(next.hotelDemandThesis),
      text(next.hotelOpportunityThesis),
      text(next.potentialHotelBusiness),
      "Official source references housing / hotel / accommodation for this activity.",
    ].filter(Boolean);
    if (!/(guestroom|lodging|housing|hotel block|accommodation|room block)/i.test(thesisBits.join(" "))) {
      next.hotelDemandThesis = thesisBits.join(" — ").slice(0, 500);
      next.proposedHotelUse =
        text(next.proposedHotelUse) ||
        "Overflow / housing participation supported by official event language";
    } else if (!text(next.hotelDemandThesis) && text(next.hotelOpportunityThesis)) {
      next.hotelDemandThesis = text(next.hotelOpportunityThesis).slice(0, 500);
    }
  }

  const existingName = text(next.primaryContact?.name || next.contactName || "");
  if (existingName && looksLikePersonName(existingName) && !text(next.primaryContact?.role || next.contactRole)) {
    const fromPage = extractTitleNearName(extraction?.excerpt || "", existingName);
    if (fromPage) {
      const gate = evaluateBuyerEvidenceGate(
        { name: existingName, role: fromPage, title: fromPage },
        next,
        { contactDataOrigin: "PUBLIC_SOURCE", affiliationVerified: true },
      );
      next.gdi2Phase2fResearchV1.buyerGateDecisions.push({
        name: existingName,
        role: fromPage,
        promote: gate.buyerEligible,
        stages: gate.stages,
        notes: gate.notes,
      });
      // Only attach role onto primary when commercially eligible or already primary
      if (!enforceGate || gate.stages[BUYER_EVIDENCE_STAGE.COMMERCIAL_RELEVANCE_VERIFIED]) {
        next.contactRole = fromPage;
        next.primaryContact = {
          ...(next.primaryContact || { name: existingName }),
          name: existingName,
          role: fromPage,
          title: fromPage,
        };
      } else {
        next.gdi2Phase2fResearchV1.candidateStakeholders.push({
          name: existingName,
          role: fromPage,
          status: "CANDIDATE_ROLE_OBSERVED_NOT_BUYER_ELIGIBLE",
          notes: gate.notes,
        });
      }
    }
  }

  // Phase 2F: preserve all named stakeholders as candidates; promote ONLY when BUYER_ELIGIBLE.
  const stakeholders = [
    ...(buyerPass?.roleRelevantStakeholders || []),
    ...(buyerPass?.namedStakeholders || []),
  ];
  const preserved = [];
  let promoted = null;
  for (const s of stakeholders) {
    if (!looksLikePersonName(s.name) || !text(s.title)) continue;
    const decision = shouldPromoteStakeholderAsBuyer(s, next);
    preserved.push({
      name: s.name,
      role: text(s.title),
      responsibilityClass: s.responsibilityClass || null,
      buyerClassification: s.buyerClassification || null,
      decisionMakerClass: s.decisionMakerClass || null,
      sourceUrl: s.sourceUrl || meta.url || null,
      promote: decision.promote,
      stages: decision.gate.stages,
      notes: decision.gate.notes,
    });
    next.gdi2Phase2fResearchV1.buyerGateDecisions.push({
      name: s.name,
      role: text(s.title),
      promote: decision.promote,
      stages: decision.gate.stages,
      notes: decision.gate.notes,
    });
    if (decision.promote && !promoted) promoted = { ...s, cleanedTitle: text(s.title) };
  }
  next.gdi2Phase2fResearchV1.candidateStakeholders.push(...preserved);
  next.gdi2Phase2eResearchV1.candidateStakeholders =
    next.gdi2Phase2fResearchV1.candidateStakeholders;

  if (enforceGate) {
    if (
      promoted &&
      (!existingName ||
        !looksLikePersonName(existingName) ||
        /unknown|general|office|inquiries/i.test(existingName))
    ) {
      next.primaryContact = {
        name: promoted.name,
        role: promoted.cleanedTitle,
        title: promoted.cleanedTitle,
        email: null,
        sourceUrl: promoted.sourceUrl || meta.url || null,
        buyerEvidenceGate: BUYER_EVIDENCE_STAGE.BUYER_ELIGIBLE,
      };
      next.contactName = promoted.name;
      next.contactRole = promoted.cleanedTitle;
    }
  } else {
    // Legacy Phase 2E path (tests may still exercise); keep for regression isolation
    const relevant = stakeholders.find(
      (s) =>
        looksLikePersonName(s.name) &&
        text(s.title) &&
        /BOOKING_DECISION|CONFIRMED_RELEVANT|POTENTIALLY_RELEVANT|PROBABLE_RELEVANT|POTENTIAL_DECISION/i.test(
          `${s.responsibilityClass || ""} ${s.buyerClassification || ""} ${s.decisionMakerClass || ""}`,
        ),
    );
    if (relevant && (!existingName || !looksLikePersonName(existingName) || /unknown|general|office|inquiries/i.test(existingName))) {
      next.primaryContact = {
        name: relevant.name,
        role: text(relevant.title),
        title: text(relevant.title),
        email: null,
        sourceUrl: relevant.sourceUrl || meta.url || null,
      };
      next.contactName = relevant.name;
      next.contactRole = text(relevant.title);
    } else if (relevant && existingName && looksLikePersonName(existingName) && !text(next.contactRole)) {
      if (text(relevant.title)) {
        next.contactRole = text(relevant.title);
        next.primaryContact = {
          ...(next.primaryContact || {}),
          name: existingName,
          role: text(relevant.title),
          title: text(relevant.title),
        };
      }
    }
  }

  if (meta.url) {
    const sources = Array.isArray(next.sources) ? [...next.sources] : [];
    if (!sources.some((s) => text(s?.url || s) === meta.url)) {
      sources.push({
        url: meta.url,
        name: meta.title || hostnameOf(meta.url),
        claimKind: "FACT",
        retrievedAt: meta.retrievedAt,
        researchMethodId: "direct_public_official_page",
        provider: "direct_public",
      });
      next.sources = sources;
      next.gdi2Phase2eResearchV1.sourcesAdded.push(meta.url);
    }
    next.evidence = [
      ...(next.evidence || []),
      {
        id: `phase2f_${Date.now()}`,
        field: "public_research",
        value: (extraction?.excerpt || "").slice(0, 280),
        claimKind: "FACT",
        sourceUrl: meta.url,
        sourceTitle: meta.title || hostnameOf(meta.url),
        accessDate: meta.retrievedAt,
        researchMethodId: "direct_public_official_page",
        researchProvider: "direct_public",
        supportsFact: meta.supports || "source",
      },
    ];
    next.gdi2Phase2eResearchV1.evidenceAdded.push(meta.url);
  }

  if (!text(next.evidenceQuote) && extraction?.excerpt) {
    next.evidenceQuote = extraction.excerpt.slice(0, 240);
  }

  return next;
}

/**
 * Differentiate commercial gaps for adaptive next-action selection.
 */
export function classifyCommercialGapType(gaps = []) {
  const g = gaps || [];
  if (g.includes("MISSING_NAMED_RELEVANT_BUYER")) return "BUYER_IDENTITY_GAP";
  if (g.includes("BUYER_RESPONSIBILITY_UNKNOWN") || g.includes("BUYER_ROLE_IRRELEVANT")) {
    return "BUYER_RESPONSIBILITY_GAP";
  }
  if (g.includes("MISSING_CONTACT_ROUTE") || g.includes("INVALID_BUYER_CONTACT_ATTRIBUTION")) {
    return "CONTACT_ROUTING_GAP";
  }
  if (g.includes("MISSING_HOTEL_DEMAND_THESIS")) return "HOTEL_DEMAND_GAP";
  if (g.includes("MISSING_CREDIBLE_ORG_ACTIVITY")) return "ORG_ACTIVITY_GAP";
  return g[0] || "NONE";
}

export function selectAdaptiveNextAction(opportunity = {}, opts = {}) {
  const shared = evaluateSharedCommercialEligibility(opportunity, {
    hotelCommercialAlignment: opts.hotelCommercialAlignment !== false,
  });
  const gapType = classifyCommercialGapType(shared.gaps || []);
  const attempts = opportunity.buyerResearchAttempts || [];
  const attemptedIds = new Set(attempts.map((a) => text(a.strategyId || a.id)).filter(Boolean));
  const plan = planBuyerRecovery(opportunity, {
    attempts,
    paidResearchAuthorized: false,
  });
  const recentFetches = opts.recentFetches || [];
  const strategySignal = recommendStrategyChange({
    recentFetches,
    currentStrategyId: attempts[attempts.length - 1]?.strategyId || "",
  });

  const segmentFamily =
    opportunity.researchSegment === "scaling_companies"
      ? "scaling_companies"
      : opportunity.associationEvents || /association/i.test(String(opportunity.researchSegment || ""))
        ? "associations_events"
        : opportunity.researchSegment || "scaling_companies";

  // Phase 2G: prefer reusable buyer-discovery strategies for the unresolved blocker
  let discoveryApproach = null;
  if (opts.useBuyerDiscoveryStrategies !== false) {
    const discovered = selectBuyerDiscoveryStrategies({
      gapType,
      segmentFamily,
      attemptedIds: [...attemptedIds],
      max: 3,
    });
    discoveryApproach = discoveryStrategyToApproach(discovered.strategies[0] || null);
  }

  const preferByGap = {
    BUYER_IDENTITY_GAP: [/LEADERSHIP|OFFICIAL|TEAM|WHO|PEOPLE|TRAVEL|PROCUREMENT|MEETINGS|ASSOCIATION|FACILITIES/i],
    BUYER_RESPONSIBILITY_GAP: [/WORKPLACE|FACILITIES|MEETINGS|EVENT|HOUSING|ROLE|TRAVEL/i],
    CONTACT_ROUTING_GAP: [/CONTACT|ORG_ROUTE|HOUSING|EVENT|PCO|HOUSING_AGENCIES/i],
    HOTEL_DEMAND_GAP: [/HOUSING|EVENT|MEETINGS|DEMAND|ANNOUNCE|DEPLOYMENT|TRAINING/i],
    ORG_ACTIVITY_GAP: [/OFFICIAL|ANNOUNCE|ABOUT|LEADERSHIP|SUPPLIER/i],
  };

  const approachPool = [
    discoveryApproach,
    plan.nextApproach,
    ...(plan.prioritizedApproaches || plan.approaches || plan.rankedApproaches || []),
  ].filter(Boolean);

  let strategy = discoveryApproach || plan.nextApproach || null;
  if (strategySignal.change && strategy) {
    // Skip repeating the same low-info strategy
    const alts = approachPool.filter(
      (a) => a?.id && a.id !== strategy.id && !attemptedIds.has(a.id),
    );
    if (alts.length) strategy = alts[0];
  }

  // Prefer strategies matching the unresolved gap type; do not blindly follow static order
  const patterns = preferByGap[gapType] || [];
  if (patterns.length && plan) {
    const match = approachPool.find(
      (a) =>
        a?.id &&
        !attemptedIds.has(a.id) &&
        !/SURFE|PDL|EXA|FIRECRAWL|SERP|PARALLEL/i.test(a.id) &&
        patterns.some((re) => re.test(a.id)),
    );
    if (match) strategy = match;
  }

  if (!strategy || /SURFE|PDL|EXA|FIRECRAWL|SERP|PARALLEL/i.test(strategy?.id || "")) {
    return {
      continue: false,
      decision: plan.recommendation || "STOP_NO_NOVEL_STRATEGY",
      gapType,
      reason: strategy
        ? "only_remaining_strategies_are_metered_denied"
        : plan.worthContinuing?.reason || "no_untried_public_strategy_remaining",
      strategy: null,
      sharedGaps: shared.gaps || [],
    };
  }

  // Avoid repeating a strategy solely because it is next in a static sequence
  const sameStrategyCount = attempts.filter((a) => a.strategyId === strategy.id).length;
  if (sameStrategyCount >= 1 && !strategySignal.change) {
    const alt = approachPool.find(
      (a) =>
        a?.id &&
        a.id !== strategy.id &&
        !attemptedIds.has(a.id) &&
        !/SURFE|PDL|EXA|FIRECRAWL|SERP|PARALLEL/i.test(a.id),
    );
    if (alt) {
      return {
        continue: true,
        decision: "SWITCH_STRATEGY",
        gapType,
        reason: `avoid_static_repeat_of_${strategy.id}_for_gap_${gapType}`,
        strategy: alt,
        previousStrategyId: strategy.id,
        sharedGaps: shared.gaps || [],
        expectedInformationGain: alt.expectedInformationGain ?? null,
      };
    }
  }

  if (plan.recommendation === "COMMERCIALLY_INELIGIBLE") {
    return {
      continue: false,
      decision: plan.recommendation,
      gapType,
      reason: plan.worthContinuing?.reason || plan.recommendation,
      strategy: null,
      sharedGaps: shared.gaps || [],
    };
  }

  // Phase 2G: when planner would stop for weak foundation, still allow a
  // discovery strategy that targets demand/activity/buyer gaps via public pages.
  if (plan.recommendation === "STOP_WITH_EVIDENCE") {
    if (
      discoveryApproach &&
      /HOTEL_DEMAND|ORG_ACTIVITY|BUYER_IDENTITY|BUYER_RESPONSIBILITY|CONTACT_ROUTING/i.test(gapType)
    ) {
      return {
        continue: true,
        decision: "CONTINUE_DISCOVERY_DESPITE_WEAK_FOUNDATION",
        gapType,
        reason: `public_discovery_${discoveryApproach.id}_may_strengthen_foundation`,
        strategy: discoveryApproach,
        sharedGaps: shared.gaps || [],
        expectedInformationGain: discoveryApproach.expectedInformationGain ?? null,
      };
    }
    return {
      continue: false,
      decision: plan.recommendation,
      gapType,
      reason: plan.worthContinuing?.reason || plan.recommendation,
      strategy: null,
      sharedGaps: shared.gaps || [],
    };
  }

  return {
    continue: true,
    decision: strategySignal.change ? "SWITCH_STRATEGY" : "CONTINUE",
    gapType,
    reason: strategySignal.change
      ? strategySignal.reason
      : `pursue_${strategy.id}_for_${gapType}`,
    strategy,
    sharedGaps: shared.gaps || [],
    expectedInformationGain: strategy.expectedInformationGain ?? null,
  };
}

function buildEvidenceRecord({
  url,
  title,
  retrievedAt,
  opportunity,
  extraction,
  supportsDemand,
  supportsBuyer,
  supportsRoute,
  method,
  freshnessNote,
}) {
  return {
    sourceUrl: url,
    sourceTitle: title || hostnameOf(url) || null,
    retrievedAt,
    publicationOrEffectiveDate: null,
    organizationName: text(opportunity.organizationName) || null,
    activity:
      text(opportunity.eventName) ||
      text(opportunity.title) ||
      text(opportunity.activityHeadline) ||
      null,
    extractedEvidence: {
      preferredEmail: extraction?.preferredEmail || null,
      emailCount: (extraction?.emails || []).length,
      hasHousingLanguage: extraction?.hasHousingLanguage === true,
      hasStaffRoleLanguage: extraction?.hasStaffRoleLanguage === true,
      excerpt: (extraction?.excerpt || "").slice(0, 400),
      // redact full emails in exports — keep domain only for ledger
      emailDomains: (extraction?.emails || [])
        .map((e) => String(e).split("@")[1] || "")
        .filter(Boolean)
        .slice(0, 5),
    },
    supportsDemandThesis: supportsDemand,
    supportsBuyerResponsibility: supportsBuyer,
    supportsContactRoute: supportsRoute,
    hotelSpecificRelevance:
      supportsDemand ||
      /hotel|housing|accommodation|lodging|meeting/i.test(extraction?.excerpt || ""),
    sourceReliability: "public_official_or_org_page_unverified_authority",
    freshness: freshnessNote || "retrieved_live_unknown_page_date",
    researchMethod: method,
    remainingUncertainties: [
      !supportsBuyer ? "buyer_responsibility_may_remain_unresolved" : null,
      !supportsRoute ? "contact_route_may_remain_missing" : null,
      !supportsDemand ? "demand_thesis_may_remain_incomplete" : null,
    ].filter(Boolean),
    category: "A_NEW_EVIDENCE_FROM_EXECUTED_RESEARCH",
  };
}

/**
 * Execute one bounded public research action for an opportunity clone.
 */
export async function executePublicResearchAction({
  opportunity,
  strategy,
  ledger,
  limits = PHASE2E_PILOT_LIMITS,
} = {}) {
  const auth = assertPhase2ePublicProviderAuthorization();
  if (!auth.ok) {
    return {
      ok: false,
      skipped: true,
      reason: "provider_authorization_failed",
      auth,
      opportunity,
      retrievals: 0,
    };
  }
  const retrievalCeiling =
    Number(limits.maxExternalRetrievalsThisAction != null
      ? limits.maxExternalRetrievalsPilot
      : limits.maxExternalRetrievalsPilot) || PHASE2E_PILOT_LIMITS.maxExternalRetrievalsPilot;
  if (ledger.externalRetrievals >= retrievalCeiling) {
    return {
      ok: false,
      skipped: true,
      reason: "pilot_retrieval_ceiling_reached",
      opportunity,
      retrievals: 0,
    };
  }

  const strategyId = strategy?.id || "OFFICIAL_LEADERSHIP_PAGES";
  const attempted = new Set(
    (opportunity.buyerResearchAttempts || [])
      .map((a) => text(a.strategyId || a.id))
      .filter(Boolean),
  );
  if (attempted.has(strategyId) && (opportunity.buyerResearchAttempts || []).filter((a) => a.strategyId === strategyId).length >= 2) {
    return {
      ok: false,
      skipped: true,
      reason: "strategy_already_exhausted_locally",
      strategyId,
      opportunity,
      retrievals: 0,
    };
  }

  const enforceGate = limits.enforceBuyerEvidenceGate === true;
  const useSourceEff = limits.useSourceSelectionEfficiency !== false;
  const useSourceDiscovery = limits.useSourceDiscoveryV1 !== false;
  const seeds = collectSeedUrls(opportunity);
  ledger.sourceSignalMemory = ledger.sourceSignalMemory || new Map();
  ledger.attemptedNormalized = ledger.attemptedNormalized || new Set(
    (ledger.attemptedUrls || []).map(normalizeSourceUrl).filter(Boolean),
  );
  ledger.discoveredNavByOrigin = ledger.discoveredNavByOrigin || new Map();
  ledger.crossOpportunityUrlCache = ledger.crossOpportunityUrlCache || [];

  const maxUrls = Math.min(
    limits.maxUrlsPerAction || 2,
    Number(limits.maxRetrievalsThisAction) || limits.maxUrlsPerAction || 2,
  );

  let toFetch = [];
  let selectionMeta = null;
  if (useSourceEff || useSourceDiscovery) {
    const originKeys = seeds.map((u) => {
      try {
        return new URL(u).origin;
      } catch {
        return "";
      }
    });
    const discovered = [];
    for (const o of originKeys) {
      if (o && ledger.discoveredNavByOrigin.has(o)) {
        discovered.push(...ledger.discoveredNavByOrigin.get(o));
      }
    }
    if (useSourceDiscovery) {
      selectionMeta = buildDiscoveredFetchPlan({
        opportunity,
        discoveryStrategyId: strategy?.discoveryStrategyId || strategyId,
        strategyId,
        attemptedNormalized: ledger.attemptedNormalized,
        signalMemory: ledger.sourceSignalMemory,
        discoveredNav: discovered,
        crossOpportunityCache: ledger.crossOpportunityUrlCache,
        maxUrls,
      });
      toFetch = selectionMeta.selected.map((s) => s.url);
    } else {
      selectionMeta = selectResearchUrls({
        seedUrls: seeds,
        discoveredNav: discovered,
        strategyId,
        attemptedNormalized: ledger.attemptedNormalized,
        signalMemory: ledger.sourceSignalMemory,
        maxUrls,
        crossOpportunityCache: ledger.crossOpportunityUrlCache,
      });
      toFetch = selectionMeta.selected.map((s) => s.url);
    }
  } else {
    const candidates = deriveOfficialCandidateUrls(seeds, strategyId).filter(
      (u) => !(ledger.attemptedUrls || []).includes(u),
    );
    toFetch = candidates.slice(0, maxUrls);
  }

  if (!toFetch.length) {
    return {
      ok: false,
      skipped: true,
      reason: "no_novel_public_urls",
      strategyId,
      opportunity,
      retrievals: 0,
      failedRequests: 0,
      duplicateRequests: 0,
    };
  }

  let working = cloneOpp(opportunity);
  const evidenceRecords = [];
  let retrievals = 0;
  let informationFound = false;
  let failedRequests = 0;
  let duplicateRequests = 0;
  let novelEvidenceFound = 0;
  let buyerGapClosed = false;
  let demandGapClosed = false;
  const usefulSourceUrls = [];
  const recentFetches = [];
  const actionCeiling =
    Number(limits.maxRetrievalsThisAction) ||
    Number(limits.maxUrlsPerAction) ||
    2;

  for (const url of toFetch) {
    if (ledger.externalRetrievals + retrievals >= retrievalCeiling) break;
    if (retrievals >= actionCeiling) break;
    const normalized = normalizeSourceUrl(url);
    if (normalized && ledger.attemptedNormalized.has(normalized) && (ledger.attemptedUrls || []).includes(url)) {
      duplicateRequests += 1;
      continue;
    }
    const started = Date.now();
    const page = await fetchOfficialPage(url, {
      allowRender: limits.allowRender === true,
      timeoutMs: 20000,
    });
    retrievals += 1;
    ledger.attemptedUrls.push(url);
    if (normalized) ledger.attemptedNormalized.add(normalized);
    const ok = page?.ok === true;
    ledger.requests.push({
      url,
      normalizedUrl: normalized || null,
      strategyId,
      ok,
      fetchMethod: page?.fetchMethod || null,
      status: ok ? "ok" : page?.error || "failed",
      latencyMs: Date.now() - started,
      costUsd: PUBLIC_FETCH_COST_USD,
      provider: "direct_public",
    });

    const quality = assessRetrievedPageQuality(page, url);
    if (!ok || !quality.usable) {
      failedRequests += 1;
      updateSourceSignalMemory(ledger.sourceSignalMemory, url, {
        status: quality.soft404 || /404/.test(String(page?.error || "")) ? 404 : 0,
        error: page?.error || (quality.soft404 ? "soft_404" : "unusable"),
        ok: false,
        irrelevant: quality.irrelevant || quality.directorySpam,
        noCommercialEvidence: !quality.commercialSignal,
      });
      recentFetches.push({
        failed: true,
        status: quality.soft404 ? "soft_404" : page?.error || "failed",
        changeStrategy: quality.changeStrategy,
        sourceType: quality.sourceType,
      });
      continue;
    }

    // Discover official nav for later actions / siblings
    try {
      const nav = extractAndCacheNav(
        page.text,
        page.finalUrl || url,
        ledger.discoveredNavByOrigin,
      );
      for (const n of nav.slice(0, 8)) {
        if (!ledger.crossOpportunityUrlCache.includes(n)) {
          ledger.crossOpportunityUrlCache.push(n);
        }
      }
    } catch {
      /* ignore nav discovery errors */
    }

    const extraction = extractPublicEvidenceFromText(page.text, page.finalUrl || url);
    const demandClass = limits.useDemandMechanismResearch !== false
      ? classifyDemandMechanismEvidence(working, extraction, {
          sourceUrl: page.finalUrl || url,
        })
      : null;
    const freshness = assessEvidenceFreshnessAndContradiction(page.text, working);
    const buyerPass = runPublicBuyerIntelligencePass({
      evidence: [
        {
          url: page.finalUrl || url,
          text: page.text,
          statement: page.text,
          retrievedAt: new Date().toISOString(),
          sourceFamily: "FREE_PUBLIC_FETCH",
        },
      ],
      organizationName: working.organizationName,
      existingContactEmail:
        working.contactEmail || working.organizationContactEmail || null,
      segmentId: working.researchSegment || "generic",
    });

    const before = evaluateSharedCommercialEligibility(working);
    // Only strengthen demand thesis from Class A/B evidence — never fabricate
    let extractionForApply = extraction;
    if (demandClass?.strengthensEligibility && extraction.hasHousingLanguage) {
      const proposal = proposeDemandThesisUpdate(working, extraction);
      if (proposal.update) {
        working.hotelDemandThesis = proposal.proposedHotelDemandThesis;
        working.proposedHotelUse = proposal.proposedHotelUse;
      }
    }

    working = applyExtractedEvidenceToClone(working, extractionForApply, buyerPass, {
      url: page.finalUrl || url,
      title: hostnameOf(page.finalUrl || url),
      retrievedAt: new Date().toISOString(),
      enforceBuyerEvidenceGate: enforceGate,
      supports:
        demandClass?.evidenceClass === DEMAND_EVIDENCE_CLASS.A_VERIFIED_HOTEL_OR_ACCOMMODATION_REQUIREMENT ||
        extraction.hasHousingLanguage
          ? "demand"
          : extraction.hasStaffRoleLanguage
            ? "buyer"
            : "source",
    });
    working.gdi2Phase2gResearchV1 = working.gdi2Phase2gResearchV1 || {
      demandClassifications: [],
      sourceQuality: [],
    };
    if (demandClass) {
      working.gdi2Phase2gResearchV1.demandClassifications.push(demandClass);
    }
    working.gdi2Phase2gResearchV1.sourceQuality.push({
      url: page.finalUrl || url,
      ...quality,
      freshness,
    });
    const after = evaluateSharedCommercialEligibility(working, {
      hotelCommercialAlignment: limits.hotelCommercialAlignment !== false,
    });

    // Phase 2F: shared sales-ready alone is insufficient — require buyer gate when enforced
    const buyerGateNow = evaluateBuyerEvidenceGate(
      {
        name: working.primaryContact?.name || working.contactName,
        role: working.primaryContact?.role || working.contactRole,
        title: working.primaryContact?.title || working.contactRole,
        email: working.organizationContactEmail || working.contactEmail,
      },
      working,
      { contactDataOrigin: "PUBLIC_SOURCE", affiliationVerified: true },
    );

    const supportsDemand =
      before.gaps?.includes("MISSING_HOTEL_DEMAND_THESIS") &&
      !after.gaps?.includes("MISSING_HOTEL_DEMAND_THESIS");
    const supportsBuyer =
      enforceGate
        ? buyerGateNow.buyerEligible === true &&
          (before.gaps?.includes("MISSING_NAMED_RELEVANT_BUYER") ||
            before.gaps?.includes("BUYER_RESPONSIBILITY_UNKNOWN"))
        : (before.gaps?.includes("BUYER_RESPONSIBILITY_UNKNOWN") &&
            !after.gaps?.includes("BUYER_RESPONSIBILITY_UNKNOWN")) ||
          (before.gaps?.includes("MISSING_NAMED_RELEVANT_BUYER") &&
            !after.gaps?.includes("MISSING_NAMED_RELEVANT_BUYER"));
    const supportsRoute =
      before.gaps?.includes("MISSING_CONTACT_ROUTE") &&
      !after.gaps?.includes("MISSING_CONTACT_ROUTE") &&
      !isNonCommercialOrgRoute(working.organizationContactEmail);

    const commercialSignal =
      supportsDemand ||
      supportsBuyer ||
      supportsRoute ||
      extraction.hasHousingLanguage ||
      (extraction.preferredEmail && !isNonCommercialOrgRoute(extraction.preferredEmail)) ||
      (working.gdi2Phase2fResearchV1?.candidateStakeholders || []).length > 0;

    const noCommercialEvidence =
      !supportsDemand &&
      !supportsBuyer &&
      !supportsRoute &&
      !extraction.hasHousingLanguage &&
      !(extraction.preferredEmail && !isNonCommercialOrgRoute(extraction.preferredEmail));

    updateSourceSignalMemory(ledger.sourceSignalMemory, url, {
      ok: true,
      noCommercialEvidence,
      useful: Boolean(supportsDemand || supportsBuyer || supportsRoute || extraction.hasHousingLanguage),
      irrelevant: noCommercialEvidence && !extraction.hasStaffRoleLanguage,
    });
    recentFetches.push({
      failed: false,
      noCommercialEvidence,
      irrelevant: noCommercialEvidence,
    });

    if (commercialSignal) {
      informationFound = true;
      usefulSourceUrls.push(page.finalUrl || url);
    }
    if (supportsDemand || supportsBuyer || supportsRoute) {
      novelEvidenceFound += 1;
    }
    if (supportsBuyer) buyerGapClosed = true;
    if (supportsDemand) demandGapClosed = true;

    evidenceRecords.push(
      buildEvidenceRecord({
        url: page.finalUrl || url,
        title: hostnameOf(page.finalUrl || url),
        retrievedAt: new Date().toISOString(),
        opportunity: working,
        extraction: {
          ...extraction,
          preferredEmail: isNonCommercialOrgRoute(extraction.preferredEmail)
            ? null
            : extraction.preferredEmail,
        },
        supportsDemand: supportsDemand || extraction.hasHousingLanguage,
        supportsBuyer: supportsBuyer,
        supportsRoute: supportsRoute,
        method: "direct_public_official_page",
      }),
    );
  }

  const attempts = [
    ...(working.buyerResearchAttempts || []),
    {
      strategyId,
      informationFound,
      retrievedAt: new Date().toISOString(),
      provider: "direct_public",
      urls: toFetch,
    },
  ];
  working.buyerResearchAttempts = attempts;
  working.gdi2Phase2eResearchV1 = working.gdi2Phase2eResearchV1 || { actions: [] };
  working.gdi2Phase2eResearchV1.actions.push({
    strategyId,
    informationFound,
    retrievals,
    at: new Date().toISOString(),
  });

  return {
    ok: true,
    skipped: false,
    strategyId,
    informationFound,
    retrievals,
    evidenceRecords,
    opportunity: working,
    costUsd: retrievals * PUBLIC_FETCH_COST_USD,
    failedRequests,
    duplicateRequests,
    novelEvidenceFound,
    buyerGapClosed,
    demandGapClosed,
    usefulSourceUrls,
    recentFetches,
    urlSelection: selectionMeta
      ? {
          selected: selectionMeta.selected.map((s) => ({
            url: s.url,
            score: s.score,
            reasons: s.reasons,
          })),
          skippedLowValue: selectionMeta.skippedLowValue,
        }
      : null,
  };
}

function evaluateSalesReadyClaim(working, { enforceBuyerEvidenceGate = false, hotelCommercialAlignment = true } = {}) {
  const shared = evaluateSharedCommercialEligibility(working, {
    hotelCommercialAlignment,
  });
  const buyerGate = evaluateBuyerEvidenceGate(
    {
      name: working.primaryContact?.name || working.contactName,
      role: working.primaryContact?.role || working.contactRole,
      title: working.primaryContact?.title || working.contactRole,
      email: working.organizationContactEmail || working.contactEmail,
    },
    working,
    { contactDataOrigin: "PUBLIC_SOURCE", affiliationVerified: true },
  );
  if (!enforceBuyerEvidenceGate) {
    return {
      salesReady: shared.salesReady === true,
      shared,
      buyerGate,
      integrity: null,
    };
  }
  const evidenceSupportsHotelBuyer = buyerGate.buyerEligible === true;
  const salesReady =
    shared.salesReady === true &&
    buyerGate.buyerEligible === true &&
    shared.gates?.realisticHotelDemand === true;
  const integrity = classifyQualificationIntegrity({
    beforeSalesReady: false,
    afterSalesReady: salesReady || shared.salesReady === true,
    buyerGate,
    demandOk: shared.gates?.realisticHotelDemand === true,
    evidenceSupportsHotelBuyer,
  });
  // If shared would say sales-ready but gate rejects → treat as RESEARCH_CONTINUATION
  const effectiveSalesReady = salesReady === true;
  return {
    salesReady: effectiveSalesReady,
    shared: {
      ...shared,
      salesReady: effectiveSalesReady,
      status: effectiveSalesReady
        ? "SALES_READY"
        : integrity.status === "UNSUPPORTED" || integrity.status === "PROVISIONAL"
          ? "RESEARCH_CONTINUATION"
          : shared.status,
      qualificationIntegrity: integrity,
      buyerEvidenceGate: {
        buyerEligible: buyerGate.buyerEligible,
        completedStages: buyerGate.completedStages,
        notes: buyerGate.notes,
      },
    },
    buyerGate,
    integrity,
  };
}

/**
 * Run bounded adaptive public research cycle for one opportunity.
 */
export async function runBoundedPublicResearchCycle(opportunity = {}, opts = {}) {
  const limits = { ...PHASE2E_PILOT_LIMITS, ...(opts.limits || {}) };
  const enforceGate = limits.enforceBuyerEvidenceGate === true;
  const phaseLabel = limits.useBuyerDiscoveryStrategies
    ? "2G"
    : enforceGate || limits.usePortfolioAllocator
      ? "2F"
      : "2E";
  const ledger = opts.ledger || {
    externalRetrievals: 0,
    attemptedUrls: [],
    requests: [],
    deniedPaidAttempts: 0,
  };
  const maxActionsThisCycle = Number(opts.maxActionsThisCycle) || limits.maxActionsPerOpportunity;
  const startedAt = new Date().toISOString();
  const t0 = Date.now();

  const frozenBaseline = {
    shared: evaluateSharedCommercialEligibility(opportunity),
    ref: redactOpportunityForShadowExport(opportunity),
  };

  // Start from recovery overlay clone (evaluation working copy only)
  const recovered = recoverCommercialEvidence(opportunity);
  let working = cloneOpp(recovered.recoveredOpportunity || opportunity);
  const actions = [];
  const evidenceRecords = [];
  const stopLog = [];
  const nextActionLog = [];
  let recentFetches = [];

  for (let i = 0; i < maxActionsThisCycle; i += 1) {
    if (ledger.externalRetrievals >= limits.maxExternalRetrievalsPilot) {
      stopLog.push({
        decision: "STOP",
        justification: "pilot_retrieval_ceiling_reached",
      });
      break;
    }

    const claim = evaluateSalesReadyClaim(working, {
      enforceBuyerEvidenceGate: enforceGate,
      hotelCommercialAlignment: limits.hotelCommercialAlignment !== false,
    });
    if (claim.salesReady) {
      stopLog.push({
        decision: "STOP_SALES_READY",
        justification: enforceGate
          ? "shared_contract_and_buyer_evidence_gate_passed"
          : "shared_contract_passed_after_public_research_or_recovery",
      });
      break;
    }

    const preflight = buildAdaptiveResearchPreflight(working, { skipRecovery: true });
    const next = selectAdaptiveNextAction(working, {
      recentFetches,
      useBuyerDiscoveryStrategies: limits.useBuyerDiscoveryStrategies !== false,
      hotelCommercialAlignment: limits.hotelCommercialAlignment !== false,
    });
    nextActionLog.push({
      index: i + 1,
      ...next,
      strategyId: next.strategy?.id || null,
    });

    if (!next.continue || !next.strategy) {
      stopLog.push({
        decision: next.decision || "STOP_NO_NOVEL_STRATEGY",
        justification: next.reason || "no_useful_strategy",
      });
      break;
    }

    const strategy = next.strategy;

    // Hard-deny any paid-looking strategy ids
    if (/SURFE|PDL|EXA|FIRECRAWL|SERP|PARALLEL/i.test(strategy.id)) {
      ledger.deniedPaidAttempts += 1;
      working.buyerResearchAttempts = [
        ...(working.buyerResearchAttempts || []),
        {
          strategyId: strategy.id,
          informationFound: false,
          skipped: true,
          reason: "metered_provider_denied_phase2e",
        },
      ];
      continue;
    }

    const actionLimits = {
      ...limits,
      maxRetrievalsThisAction:
        Number(opts.maxRetrievalsThisAction) || limits.maxUrlsPerAction || 2,
    };

    const beforeGaps = evaluateSharedCommercialEligibility(working).gaps || [];
    const action = await executePublicResearchAction({
      opportunity: working,
      strategy,
      ledger,
      limits: actionLimits,
    });
    ledger.externalRetrievals += action.retrievals || 0;
    recentFetches = action.recentFetches || [];
    actions.push({
      index: i + 1,
      strategyId: action.strategyId || strategy.id,
      skipped: action.skipped === true,
      reason: action.reason || null,
      informationFound: action.informationFound === true,
      retrievals: action.retrievals || 0,
      blocker: preflight.commercialBlocker,
      gapType: next.gapType,
      nextActionDecision: next.decision,
      nextActionReason: next.reason,
      expectedInformationGain: next.expectedInformationGain ?? strategy.expectedInformationGain ?? null,
      urlSelection: action.urlSelection || null,
    });
    if (action.evidenceRecords?.length) {
      evidenceRecords.push(...action.evidenceRecords);
    }
    if (action.opportunity) working = action.opportunity;

    const afterGaps = evaluateSharedCommercialEligibility(working).gaps || [];
    action.buyerGapClosed =
      action.buyerGapClosed ||
      (beforeGaps.includes("MISSING_NAMED_RELEVANT_BUYER") &&
        !afterGaps.includes("MISSING_NAMED_RELEVANT_BUYER"));
    action.demandGapClosed =
      action.demandGapClosed ||
      (beforeGaps.includes("MISSING_HOTEL_DEMAND_THESIS") &&
        !afterGaps.includes("MISSING_HOTEL_DEMAND_THESIS"));

    if (action.skipped && action.reason === "no_novel_public_urls") {
      stopLog.push({
        decision: "STOP",
        justification: "no_novel_public_urls_for_strategy",
      });
      break;
    }
    if (!action.informationFound && i >= 2) {
      // Low-information streak
      const recent = actions.slice(-3);
      if (recent.length >= 3 && recent.every((a) => !a.informationFound && !a.skipped)) {
        stopLog.push({
          decision: "STOP_LOW_INFORMATION",
          justification: "three_consecutive_public_actions_without_new_evidence",
        });
        break;
      }
    }

    // Single-action portfolio slots stop after one productive scheduling unit
    if (opts.stopAfterOneAction === true) {
      stopLog.push({
        decision: action.informationFound ? "YIELD_TO_PORTFOLIO" : "YIELD_TO_PORTFOLIO_LOW_INFO",
        justification: "portfolio_initial_or_reallocation_slot_complete",
      });
      break;
    }
  }

  if (!stopLog.length) {
    const endClaim = evaluateSalesReadyClaim(working, {
      enforceBuyerEvidenceGate: enforceGate,
      hotelCommercialAlignment: limits.hotelCommercialAlignment !== false,
    });
    stopLog.push({
      decision: endClaim.salesReady ? "STOP_SALES_READY" : "CONTINUE_OR_BUDGET",
      justification: endClaim.salesReady
        ? "buyer_gate_and_shared_contract_passed"
        : "action_or_retrieval_limit_reached",
    });
  }

  const afterClaim = evaluateSalesReadyClaim(working, {
    enforceBuyerEvidenceGate: enforceGate,
    hotelCommercialAlignment: limits.hotelCommercialAlignment !== false,
  });
  const afterShared = afterClaim.shared;
  const continuation = classifyResearchContinuationDisposition({
    opportunity: working,
    lodgingEvidenceIncomplete: afterShared.gaps?.includes("MISSING_HOTEL_DEMAND_THESIS"),
    contactNotYetVerified:
      afterShared.gaps?.includes("MISSING_NAMED_RELEVANT_BUYER") ||
      afterShared.gaps?.includes("BUYER_RESPONSIBILITY_UNKNOWN") ||
      (enforceGate && afterClaim.buyerGate?.buyerEligible !== true),
  });

  let orsEval = null;
  try {
    orsEval = evaluateOpportunity(working, {
      dryRun: true,
      paidResearchAuthorized: false,
    });
  } catch (err) {
    orsEval = { error: String(err?.message || err) };
  }

  const evidenceBackedNewSalesReady =
    frozenBaseline.shared.salesReady !== true && afterClaim.salesReady === true;

  working.gdi2Phase2fResearchV1 = working.gdi2Phase2fResearchV1 || {};
  working.gdi2Phase2fResearchV1.nextActionLog = nextActionLog;

  return {
    version: ADAPTIVE_PUBLIC_RESEARCH_CONTROLLER_V1,
    phase: phaseLabel,
    startedAt,
    finishedAt: new Date().toISOString(),
    elapsedMs: Date.now() - t0,
    doesNotMutateCanonicalStorage: true,
    doesNotChangeVisibility: true,
    authorizePaidResearch: false,
    providerAuthorization: assertPhase2ePublicProviderAuthorization(),
    opportunityRef: frozenBaseline.ref,
    researchObjective: buildAdaptiveResearchPreflight(opportunity).researchObjective,
    baseline: {
      status: frozenBaseline.shared.status,
      salesReady: frozenBaseline.shared.salesReady === true,
      gaps: frozenBaseline.shared.gaps || [],
    },
    after: {
      status: afterShared.status,
      salesReady: afterClaim.salesReady === true,
      gaps: afterShared.gaps || [],
      qualificationIntegrity: afterClaim.integrity,
      buyerEvidenceGate: afterShared.buyerEvidenceGate || null,
    },
    evidenceBackedNewSalesReady,
    mappingOnlyLift: recovered.category === "B_DETERMINISTIC_MAPPING_CORRECTED" && !evidenceBackedNewSalesReady,
    recoveryCategory: recovered.category,
    actions,
    evidenceRecords,
    stopLog,
    nextActionLog,
    continuationDisposition: {
      disposition:
        afterClaim.integrity?.shadowDisposition ||
        continuation.disposition,
      commerciallyExhausted: continuation.commerciallyExhausted === true,
      qualificationIntegrity: afterClaim.integrity?.status || null,
    },
    orsDryRunBlockers: orsEval?.blockers?.map((b) => b.key || b.code) || [],
    workingOpportunity: working,
    costUsd: actions.reduce((n, a) => n + (a.retrievals || 0) * PUBLIC_FETCH_COST_USD, 0),
    retrievalsUsed: actions.reduce((n, a) => n + (a.retrievals || 0), 0),
    lastActionDiagnostics: actions.length
      ? {
          failedRequests: actions.reduce((n, a) => n + (a.failedRequests || 0), 0),
          novelEvidenceFound: actions.reduce((n, a) => n + (a.novelEvidenceFound || 0), 0),
        }
      : null,
  };
}

export async function runPhase2ePublicResearchPilot(opportunities = [], opts = {}) {
  const limits = { ...PHASE2E_PILOT_LIMITS, ...(opts.limits || {}) };
  const ledger = {
    externalRetrievals: 0,
    attemptedUrls: [],
    requests: [],
    deniedPaidAttempts: 0,
  };
  const results = [];
  for (const opp of opportunities) {
    if (ledger.externalRetrievals >= limits.maxExternalRetrievalsPilot) {
      results.push({
        opportunityRef: redactOpportunityForShadowExport(opp),
        skipped: true,
        reason: "pilot_retrieval_ceiling_reached_before_start",
      });
      continue;
    }
    const cycle = await runBoundedPublicResearchCycle(opp, { ledger, limits });
    results.push(cycle);
  }
  return {
    version: ADAPTIVE_PUBLIC_RESEARCH_CONTROLLER_V1,
    generatedAt: new Date().toISOString(),
    limits,
    ledger: {
      externalRetrievals: ledger.externalRetrievals,
      uniqueUrlsAttempted: [...new Set(ledger.attemptedUrls)].length,
      requestCount: ledger.requests.length,
      deniedPaidAttempts: ledger.deniedPaidAttempts,
      totalCostUsd: ledger.requests.reduce((n, r) => n + Number(r.costUsd || 0), 0),
      requests: ledger.requests,
    },
    results,
  };
}

/**
 * Phase 2F portfolio pilot — fair allocation + buyer gate + source efficiency.
 */
export async function runPhase2gPublicResearchPilot(opportunities = [], opts = {}) {
  return runPhase2fPublicResearchPilot(opportunities, {
    ...opts,
    limits: { ...PHASE2G_PILOT_LIMITS, ...(opts.limits || {}) },
  });
}

export async function runPhase2fPublicResearchPilot(opportunities = [], opts = {}) {
  const limits = { ...PHASE2F_PILOT_LIMITS, ...(opts.limits || {}) };
  const plan = buildResearchAllocationPlan(opportunities, { policy: limits });
  const allocState = createAllocationState(plan);
  const ledger = {
    externalRetrievals: 0,
    attemptedUrls: [],
    attemptedNormalized: new Set(),
    requests: [],
    deniedPaidAttempts: 0,
    sourceSignalMemory: new Map(),
    discoveredNavByOrigin: new Map(),
    crossOpportunityUrlCache: [],
  };

  const byId = new Map(opportunities.map((o) => [o.id, o]));
  const workingById = new Map();
  const resultsById = new Map();
  let guard = 0;

  while (guard++ < 500) {
    const slot = selectNextAllocationSlot(allocState);
    if (slot.done) break;
    if (ledger.externalRetrievals >= limits.maxExternalRetrievalsPilot) {
      break;
    }

    const baseOpp = workingById.get(slot.opportunityId) || byId.get(slot.opportunityId);
    if (!baseOpp) {
      recordAllocationOutcome(allocState, slot.opportunityId, {
        retrievals: 0,
        stopReason: "STOP_MISSING_OPPORTUNITY",
        usefulStrategy: false,
      });
      continue;
    }

    const cycle = await runBoundedPublicResearchCycle(baseOpp, {
      ledger,
      limits: {
        ...limits,
        maxRetrievalsThisAction: slot.maxRetrievalsThisAction,
      },
      maxActionsThisCycle: 1,
      stopAfterOneAction: true,
      maxRetrievalsThisAction: slot.maxRetrievalsThisAction,
    });

    workingById.set(slot.opportunityId, cycle.workingOpportunity || baseOpp);
    const prev = resultsById.get(slot.opportunityId);
    if (prev) {
      resultsById.set(slot.opportunityId, {
        ...cycle,
        actions: [...(prev.actions || []), ...(cycle.actions || [])],
        evidenceRecords: [...(prev.evidenceRecords || []), ...(cycle.evidenceRecords || [])],
        nextActionLog: [...(prev.nextActionLog || []), ...(cycle.nextActionLog || [])],
        retrievalsUsed: (prev.retrievalsUsed || 0) + (cycle.retrievalsUsed || 0),
        stopLog: [...(prev.stopLog || []), ...(cycle.stopLog || [])],
        baseline: prev.baseline,
      });
    } else {
      resultsById.set(slot.opportunityId, cycle);
    }

    const lastAction = (cycle.actions || [])[cycle.actions.length - 1] || {};
    const stopFromCycle = (cycle.stopLog || []).map((s) => s.decision).join("|");
    let stopReason = null;
    if (cycle.after?.salesReady) stopReason = "STOP_SALES_READY";
    else if (/STOP_NO_NOVEL|STOP_NO_USEFUL|COMMERCIALLY_INELIGIBLE|STOP_WITH_EVIDENCE/.test(stopFromCycle)) {
      stopReason = cycle.stopLog[cycle.stopLog.length - 1]?.decision || "STOP_NO_USEFUL_STRATEGY";
    } else if (/STOP_LOW_INFORMATION/.test(stopFromCycle)) {
      stopReason = "STOP_LOW_INFORMATION";
    }

    // After initial budget exhausted, yield to reallocation without hard-stopping the opp
    const entry = allocState.byId.get(slot.opportunityId);
    const initialExhausted =
      entry &&
      entry.phase === "INITIAL" &&
      entry.used.retrievals + (cycle.retrievalsUsed || 0) >= entry.initialBudget.retrievals;

    recordAllocationOutcome(allocState, slot.opportunityId, {
      retrievals: cycle.retrievalsUsed || 0,
      informationFound: (cycle.actions || []).some((a) => a.informationFound),
      salesReady: cycle.after?.salesReady === true,
      stopReason: stopReason || (initialExhausted ? null : null),
      phase: slot.phase,
      nextActionReason: lastAction.nextActionReason || slot.reason,
      failedRequests: lastAction.failedRequests || 0,
      duplicateRequests: lastAction.duplicateRequests || 0,
      novelEvidenceFound: (cycle.evidenceRecords || []).filter(
        (e) => e.supportsBuyerResponsibility || e.supportsDemandThesis || e.supportsContactRoute,
      ).length,
      buyerGapClosed: Boolean(cycle.actions?.some((a) => a.buyerGapClosed)),
      demandGapClosed: Boolean(cycle.actions?.some((a) => a.demandGapClosed)),
      usefulSourceUrls: (cycle.evidenceRecords || [])
        .filter((e) => e.supportsBuyerResponsibility || e.supportsDemandThesis || e.supportsContactRoute)
        .map((e) => e.sourceUrl)
        .filter(Boolean),
      gapsAfter: cycle.after?.gaps || [],
      preservedCandidates: cycle.workingOpportunity?.gdi2Phase2fResearchV1?.candidateStakeholders || [],
      usefulStrategy: !/STOP_NO_NOVEL|STOP_NO_USEFUL|COMMERCIALLY_INELIGIBLE/.test(stopFromCycle),
    });

    // Sync ledger counter with alloc state (cycle already incremented ledger.externalRetrievals)
    allocState.deniedPaidAttempts = ledger.deniedPaidAttempts;

    const updated = allocState.byId.get(slot.opportunityId);
    if (
      updated &&
      updated.phase === "INITIAL" &&
      updated.used.retrievals >= updated.initialBudget.retrievals &&
      !updated.salesReady &&
      !updated.stopReason
    ) {
      updated.phase = "WAITING_REALLOCATION";
      allocState.byId.set(slot.opportunityId, updated);
    }
  }

  // Mark never-touched deferred as backlog
  for (const e of allocState.byId.values()) {
    if (e.used.actions === 0 && !e.stopReason) {
      e.stopReason = "BACKLOG_OR_RESERVE_UNSPENT";
      allocState.byId.set(e.id, e);
    }
  }

  const allocationSummary = summarizeAllocationState(allocState);
  const results = opportunities.map((opp) => {
    const cycle = resultsById.get(opp.id);
    if (cycle) return cycle;
    const entry = allocState.byId.get(opp.id);
    return {
      opportunityRef: redactOpportunityForShadowExport(opp),
      skipped: true,
      reason: entry?.stopReason || entry?.status || "not_scheduled",
      baseline: {
        status: evaluateSharedCommercialEligibility(opp).status,
        salesReady: false,
        gaps: evaluateSharedCommercialEligibility(opp).gaps || [],
      },
      after: {
        status: evaluateSharedCommercialEligibility(opp).status,
        salesReady: false,
        gaps: evaluateSharedCommercialEligibility(opp).gaps || [],
      },
      evidenceBackedNewSalesReady: false,
      actions: [],
      retrievalsUsed: 0,
    };
  });

  return {
    version: ADAPTIVE_PUBLIC_RESEARCH_CONTROLLER_V1,
    phase: "2F",
    generatedAt: new Date().toISOString(),
    limits,
    allocationPlan: {
      eligibleCount: plan.eligibleCount,
      initialRetrievalBudgetAssigned: plan.initialRetrievalBudgetAssigned,
      coverageReserveRetrievals: plan.coverageReserveRetrievals,
      reallocationPoolEstimate: plan.reallocationPoolEstimate,
    },
    allocationSummary,
    ledger: {
      externalRetrievals: ledger.externalRetrievals,
      uniqueUrlsAttempted: [...new Set(ledger.attemptedUrls.map(normalizeSourceUrl).filter(Boolean))].length,
      requestCount: ledger.requests.length,
      deniedPaidAttempts: ledger.deniedPaidAttempts,
      totalCostUsd: ledger.requests.reduce((n, r) => n + Number(r.costUsd || 0), 0),
      failedRequests: ledger.requests.filter((r) => !r.ok).length,
      duplicateNormalizedAvoided: allocationSummary.diagnostics.duplicateRequests,
      requests: ledger.requests.map((r) => ({
        url: r.url,
        normalizedUrl: r.normalizedUrl || normalizeSourceUrl(r.url),
        strategyId: r.strategyId,
        ok: r.ok,
        status: r.status,
        costUsd: r.costUsd,
        provider: r.provider,
      })),
    },
    results,
  };
}
