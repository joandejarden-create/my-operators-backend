/**
 * Wave 1 fair-parity live arms for the AI-agent incremental value experiment.
 *
 * Arm A — existing GDI: deterministic official_page seed/nav sequence (no adaptive planner).
 * Arm B — agent: runBoundedPublicResearchCycle adaptive planning + same $0 official_page path.
 *
 * Shared caps: maxFetches, $0 paid, same derived origin allowlist, denied metered providers.
 * Does not write Airtable / publish / authorize paid providers.
 */

import { fetchOfficialPage } from "../official-page-fetch.js";
import { extractPublicEvidenceFromText } from "../research/direct-public-executor-v1.js";
import { runPublicBuyerIntelligencePass } from "../research/commercial-depth-buyer-intelligence-v1.js";
import { classifyDemandMechanismEvidence } from "../demand-mechanism-research-v1.js";
import { assessRetrievedPageQuality, extractAndCacheNav } from "../source-discovery-v1.js";
import { recoverCommercialEvidence } from "../commercial-evidence-recovery-v1.js";
import { evaluateOpportunity } from "../research/opportunity-research-supervisor-v1.js";
import { evaluateSharedCommercialEligibility } from "../commercial-eligibility-contract-v1.js";
import {
  runBoundedPublicResearchCycle,
  PHASE2G_PILOT_LIMITS,
  DENIED_METERED_PROVIDERS,
} from "../adaptive-public-research-controller-v1.js";
import { PUBLIC_FETCH_COST_USD } from "../workflow/gdi-evidence-acquisition-v2.js";
import { runIndependentEvidenceChallenger } from "./gdi-20opp-live-research-prep-v1.js";
import {
  assessOpportunityDrawerReadiness,
  classifyCommercialDisposition,
} from "../research/gdi-discovery-intake-disposition-v1.js";

export const WAVE1_ARMS_VERSION = "gdi-ai-agent-wave1-fair-parity-arms-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function normalizeUrl(url) {
  try {
    const u = new URL(String(url || "").trim());
    u.hash = "";
    return u.toString().replace(/\/$/, "");
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

function cloneOpp(opp) {
  return JSON.parse(JSON.stringify(opp || {}));
}

export function assertZeroPaidBilling() {
  const ok = Number(PUBLIC_FETCH_COST_USD) === 0;
  return {
    ok,
    publicFetchCostUsd: PUBLIC_FETCH_COST_USD,
    deniedMetered: [...DENIED_METERED_PROVIDERS],
  };
}

/** Collect seed URLs already present on the frozen opportunity (existing GDI evidence bag). */
export function collectOpportunitySeedUrls(opportunity = {}) {
  const raw = [
    opportunity.website,
    opportunity.organizationWebsite,
    opportunity.sourceUrl,
    opportunity.officialSource,
    opportunity.eventUrl,
    opportunity.primaryContact?.sourceUrl,
    opportunity.contact?.sourceUrl,
    ...(Array.isArray(opportunity.sources) ? opportunity.sources.map((s) => s.url || s.sourceUrl) : []),
    ...(Array.isArray(opportunity.evidence) ? opportunity.evidence.map((e) => e.url || e.sourceUrl) : []),
    ...(Array.isArray(opportunity.evidenceUrls) ? opportunity.evidenceUrls : []),
  ];
  return [...new Set(raw.map(normalizeUrl).filter(Boolean))];
}

export function deriveAllowedOrigins(seedUrls = []) {
  return [...new Set(seedUrls.map(originOf).filter(Boolean))];
}

export function isOriginAllowed(url, allowedOrigins = []) {
  const origin = originOf(url);
  if (!origin) return false;
  if (!allowedOrigins.length) return false;
  return allowedOrigins.some(
    (o) => origin === o || origin === o.replace(/\/$/, "") || originOf(o) === origin,
  );
}

function deterministicRank(url) {
  try {
    const p = new URL(url).pathname + new URL(url).search;
    if (/housing|hotel|room.?block|accommodation|lodging/i.test(p)) return 0;
    if (/contact|board|staff|about|leadership|team/i.test(p)) return 1;
    if (/event|meeting|conference|summit|banquet|agenda/i.test(p)) return 2;
    if (/news|press|media/i.test(p)) return 4;
    return 5;
  } catch {
    return 9;
  }
}

function isNoiseUrl(url) {
  return (
    /\.(css|js|png|jpe?g|gif|svg|woff2?)(\?|$)/i.test(url) ||
    /\/wp-json\//i.test(url) ||
    /\/wp-content\/(plugins|themes)\//i.test(url)
  );
}

function applyPageToWorking(working, pageResult) {
  const next = cloneOpp(working);
  const sources = Array.isArray(next.sources) ? [...next.sources] : [];
  sources.push({
    url: pageResult.finalUrl,
    statement: text(pageResult.excerpt).slice(0, 400),
    retrievedAt: pageResult.retrievedAt,
    provider: "official_page",
    arm: "A_existing_gdi_deterministic",
  });
  next.sources = sources;

  if (pageResult.extraction?.preferredEmail || (pageResult.extraction?.emails || []).length) {
    const email =
      pageResult.extraction.preferredEmail || pageResult.extraction.emails[0];
    if (email && !next.primaryContactEmail) {
      next.primaryContactEmail = email;
    }
  }

  const named = (pageResult.stakeholders || []).find(
    (s) => text(s.name) && text(s.title || s.role),
  );
  if (named && !text(next.primaryContact?.name)) {
    next.primaryContact = {
      name: named.name,
      title: named.title || named.role || null,
      email: named.emailPubliclyPresent ? named.email : null,
      sourceUrl: named.sourceUrl || pageResult.finalUrl,
      contactClass: named.buyerClassification || "RELEVANT_COMMERCIAL_ENTRY_POINT",
    };
    next.hasVerifiedNamedRelevantContact = Boolean(named.name && named.title);
  }

  if (pageResult.lodging?.housingLanguage || pageResult.extraction?.hasHousingLanguage) {
    next.guestroomThesis =
      next.guestroomThesis ||
      "Public page language indicates lodging/housing interest — verify open sourcing vs competitor placement.";
    if (!next.whatIsHappening && pageResult.excerpt) {
      next.whatIsHappening = text(pageResult.excerpt).slice(0, 280);
    }
  }

  if (pageResult.demandClass?.evidenceClass) {
    next.demandEvidenceClass =
      next.demandEvidenceClass || pageResult.demandClass.evidenceClass;
  }

  const facts = Array.isArray(next.establishedFacts) ? [...next.establishedFacts] : [];
  if (pageResult.excerpt) {
    facts.push(text(pageResult.excerpt).slice(0, 240));
    next.establishedFacts = facts.slice(0, 40);
  }
  return next;
}

/**
 * Arm A — existing GDI research: deterministic official_page sequence under shared caps.
 */
export async function runExistingGdiActiveResearchArm(frozenOpportunity, opts = {}) {
  const t0 = Date.now();
  const billing = assertZeroPaidBilling();
  if (!billing.ok) {
    throw new Error("Refusing Wave 1 — PUBLIC_FETCH_COST_USD is not $0");
  }

  const maxFetches = Number(opts.maxFetchesPerCase) || 12;
  const recovery = recoverCommercialEvidence(frozenOpportunity);
  let working = cloneOpp(recovery.recoveredOpportunity || frozenOpportunity);

  const seeds = collectOpportunitySeedUrls(working);
  const allowedOrigins =
    opts.allowedOrigins?.length > 0
      ? [...opts.allowedOrigins]
      : deriveAllowedOrigins(seeds);

  const attempted = new Set();
  const queue = seeds
    .filter((u) => isOriginAllowed(u, allowedOrigins))
    .sort((a, b) => deterministicRank(a) - deterministicRank(b));

  const pages = [];
  const failures = [];
  const retrievalRecords = [];
  const navCache = new Map();
  let retrievals = 0;
  let actualPaidUsd = 0;

  while (queue.length && retrievals < maxFetches) {
    queue.sort((a, b) => deterministicRank(a) - deterministicRank(b));
    const url = queue.shift();
    const key = normalizeUrl(url);
    if (!key || attempted.has(key) || !isOriginAllowed(url, allowedOrigins)) continue;
    attempted.add(key);

    const started = Date.now();
    const page = await fetchOfficialPage(url, {
      allowRender: false,
      timeoutMs: 20000,
    });
    retrievals += 1;
    actualPaidUsd += Number(PUBLIC_FETCH_COST_USD) || 0;
    const latencyMs = Date.now() - started;

    if (!page?.ok) {
      failures.push({ url, error: page?.error || "fetch_failed", latencyMs });
      retrievalRecords.push({
        mocked: false,
        strategyId: "EXISTING_GDI_DETERMINISTIC_OFFICIAL_PAGE",
        informationFound: false,
        retrievals: 1,
        urls: [url],
        provenance: { provider: "official_page", arm: "A" },
      });
      continue;
    }

    const finalUrl = page.finalUrl || url;
    if (!isOriginAllowed(finalUrl, allowedOrigins)) {
      failures.push({ url, finalUrl, error: "redirected_off_allowlist", latencyMs });
      continue;
    }

    const bodyText = page.text || "";
    const quality = assessRetrievedPageQuality(page, finalUrl);
    const extraction = extractPublicEvidenceFromText(bodyText, finalUrl);
    const demandClass = classifyDemandMechanismEvidence(working, extraction, {
      sourceUrl: finalUrl,
    });
    const buyerPass = runPublicBuyerIntelligencePass({
      evidence: [
        {
          url: finalUrl,
          text: bodyText,
          statement: bodyText,
          retrievedAt: new Date().toISOString(),
          sourceFamily: "FREE_PUBLIC_FETCH",
        },
      ],
      organizationName: working.organizationName || working.company || metaOrg(working),
      existingContactEmail: working.primaryContact?.email || working.contact?.email || null,
      segmentId: working.researchSegment || working.demandSegmentId || "association_events",
    });

    const lodging = {
      housingLanguage: /housing|hotel block|room block|accommodation|official hotel|host hotel/i.test(
        bodyText,
      ),
    };
    const useful =
      !isNoiseUrl(finalUrl) &&
      quality.usable === true &&
      (extraction.hasHousingLanguage ||
        extraction.hasStaffRoleLanguage ||
        (extraction.emails || []).length > 0 ||
        (buyerPass.stakeholders || []).length > 0 ||
        lodging.housingLanguage ||
        (/20(2[6-9]|3\d)/.test(bodyText) && /conference|meeting|summit/i.test(finalUrl + bodyText.slice(0, 200))));

    const pageResult = {
      finalUrl,
      seedUrl: url,
      useful,
      latencyMs,
      retrievedAt: new Date().toISOString(),
      extraction,
      demandClass: demandClass
        ? {
            evidenceClass: demandClass.evidenceClass || demandClass.class || null,
            strengthensEligibility: demandClass.strengthensEligibility === true,
          }
        : null,
      stakeholders: (buyerPass.stakeholders || []).map((s) => ({
        name: s.name,
        title: s.title,
        email: s.email || null,
        buyerClassification: s.buyerClassification,
        sourceUrl: s.sourceUrl,
        emailPubliclyPresent:
          s.email &&
          (extraction.emails || []).some(
            (e) => e.toLowerCase() === String(s.email).toLowerCase(),
          ),
      })),
      lodging,
      excerpt: text(bodyText).slice(0, 500),
    };
    pages.push(pageResult);
    retrievalRecords.push({
      mocked: false,
      strategyId: "EXISTING_GDI_DETERMINISTIC_OFFICIAL_PAGE",
      informationFound: useful === true,
      retrievals: 1,
      urls: [finalUrl],
      provenance: { provider: "official_page", arm: "A", useful },
    });

    if (useful) working = applyPageToWorking(working, pageResult);

    try {
      const nav = extractAndCacheNav(bodyText, finalUrl, navCache) || [];
      for (const n of nav) {
        const nu = normalizeUrl(n);
        if (nu && isOriginAllowed(nu, allowedOrigins) && !attempted.has(nu)) queue.push(nu);
      }
    } catch {
      /* ignore */
    }

    const hrefRe = /href=["']([^"']+)["']/gi;
    let m;
    while ((m = hrefRe.exec(page.html || bodyText))) {
      try {
        const abs = new URL(m[1], finalUrl).toString();
        const nu = normalizeUrl(abs);
        if (
          nu &&
          isOriginAllowed(nu, allowedOrigins) &&
          !attempted.has(nu) &&
          deterministicRank(nu) <= 2
        ) {
          queue.push(nu);
        }
      } catch {
        /* ignore */
      }
    }
  }

  const independent = runIndependentEvidenceChallenger({
    baselineOpportunity: frozenOpportunity,
    workingOpportunity: working,
    recovery,
    retrievalRecords,
  });

  const supervisor = evaluateOpportunity(working, { requireFutureDemand: false });
  let shared = evaluateSharedCommercialEligibility(working);
  if (independent.salesReadyBlockedByChallenger && shared.salesReady === true) {
    shared = {
      ...shared,
      status: "RESEARCH_CONTINUATION",
      salesReady: false,
      commercialEligible: false,
      challengerOverride: "blocked_unsupported_or_unverified_buyer",
      gaps: [...(shared.gaps || []), "CHALLENGER_BLOCKED_SALES_READY"],
    };
  }

  const drawer = assessOpportunityDrawerReadiness(working);
  const disposition = classifyCommercialDisposition(working, {
    targetHotel: opts.targetHotel || working.hotelName || "",
  });

  return {
    arm: "A_existing_gdi",
    executionKind: "active_deterministic_official_page_sequence",
    adaptivePlanner: false,
    activeResearch: true,
    version: WAVE1_ARMS_VERSION,
    durationMs: Date.now() - t0,
    allowedOrigins,
    seedUrlCount: seeds.length,
    costs: {
      actualPaidUsd,
      fetchCount: retrievals,
      fetchBudgetCap: maxFetches,
      mode: "wave1_live_public_zero_cost",
      deniedMetered: billing.deniedMetered,
    },
    retrievalRecords,
    pages: pages.map((p) => ({
      url: p.finalUrl,
      useful: p.useful,
      latencyMs: p.latencyMs,
      excerpt: p.excerpt,
    })),
    failures,
    recovery: {
      applied:
        recovery?.overlaysApplied?.demand === true ||
        recovery?.overlaysApplied?.buyer === true,
      category: recovery?.category || null,
    },
    independentChallenger: independent,
    sharedEligibility: {
      status: shared.status,
      salesReady: shared.salesReady === true,
      gaps: shared.gaps || [],
      challengerOverride: shared.challengerOverride || null,
    },
    supervisor: {
      blockers: (supervisor.blockers || []).map((b) => b.key || b),
      publishable: supervisor.publishable === true,
      hardStopReason: supervisor.hardStop?.reason || null,
    },
    drawer,
    disposition,
    workingOpportunity: working,
    commercial: summarizeCommercial(working, independent, shared, drawer, disposition, {
      durationMs: Date.now() - t0,
      actualPaidUsd,
      fetchCount: retrievals,
    }),
  };
}

function metaOrg(working) {
  return text(working.organizationName || working.company || working.displayCompanyName) || "org";
}

function summarizeCommercial(working, independent, shared, drawer, disposition, costs) {
  return {
    eligibilityStatus: shared.status || null,
    salesReady: shared.salesReady === true,
    challengerBlocked: independent.salesReadyBlockedByChallenger === true,
    verifiedNamedCount: independent.contact?.verifiedNamedCount ?? independent.after?.contactRelevance?.verifiedNamedCount ?? 0,
    genuinelyNewVerifiedCount: (independent.buckets?.genuinelyNewVerifiedEvidence || []).length,
    drawerReady: drawer.ready === true,
    drawerGaps: drawer.gaps || [],
    disposition: disposition.commercialDisposition,
    whyActNow: Boolean(text(working.whyActNow)),
    recommendedNextStep: Boolean(
      text(working.recommendedNextStep || working.recommendedAction),
    ),
    durationMs: costs.durationMs,
    actualPaidUsd: costs.actualPaidUsd,
    fetchCount: costs.fetchCount,
  };
}

/**
 * Arm B — adaptive agent: bounded public research cycle under identical fetch cap.
 */
export async function runAgentAdaptiveResearchArm(frozenOpportunity, opts = {}) {
  const t0 = Date.now();
  const billing = assertZeroPaidBilling();
  if (!billing.ok) {
    throw new Error("Refusing Wave 1 — PUBLIC_FETCH_COST_USD is not $0");
  }

  const maxFetches = Number(opts.maxFetchesPerCase) || 12;
  const seeds = collectOpportunitySeedUrls(frozenOpportunity);
  const allowedOrigins =
    opts.allowedOrigins?.length > 0
      ? [...opts.allowedOrigins]
      : deriveAllowedOrigins(seeds);

  const recovery = recoverCommercialEvidence(frozenOpportunity);
  const starting = cloneOpp(recovery.recoveredOpportunity || frozenOpportunity);

  // Encode allowlist onto working copy so adaptive URL selection stays same-origin when possible.
  starting._wave1AllowedOrigins = allowedOrigins;

  const liveCycle = await runBoundedPublicResearchCycle(starting, {
    limits: {
      ...PHASE2G_PILOT_LIMITS,
      maxActionsPerOpportunity: Math.min(6, Math.ceil(maxFetches / 2)),
      maxUrlsPerAction: 2,
      maxExternalRetrievalsPilot: maxFetches,
      authorizePaidResearch: false,
    },
    maxActionsThisCycle: Math.min(6, Math.ceil(maxFetches / 2)),
  });

  const working = liveCycle.workingOpportunity || starting;
  const retrievalRecords = (liveCycle.actions || []).map((a) => ({
    mocked: false,
    strategyId: a.strategyId,
    informationFound: a.informationFound === true,
    retrievals: a.retrievals || 0,
    urls: a.urlSelection?.selected?.map((s) => s.url) || [],
    provenance: { provider: "official_page", arm: "B", phase: "2G" },
    skipped: a.skipped === true,
    reason: a.reason || null,
    gapType: a.gapType || null,
    nextActionDecision: a.nextActionDecision || null,
  }));

  const fetchCount = Number(liveCycle.retrievalsUsed || 0);
  const actualPaidUsd = fetchCount * (Number(PUBLIC_FETCH_COST_USD) || 0);

  const independent = runIndependentEvidenceChallenger({
    baselineOpportunity: frozenOpportunity,
    workingOpportunity: working,
    recovery,
    retrievalRecords,
  });

  const supervisor = evaluateOpportunity(working, { requireFutureDemand: false });
  let shared = evaluateSharedCommercialEligibility(working);
  if (independent.salesReadyBlockedByChallenger && shared.salesReady === true) {
    shared = {
      ...shared,
      status: "RESEARCH_CONTINUATION",
      salesReady: false,
      commercialEligible: false,
      challengerOverride: "blocked_unsupported_or_unverified_buyer",
      gaps: [...(shared.gaps || []), "CHALLENGER_BLOCKED_SALES_READY"],
    };
  }

  const drawer = assessOpportunityDrawerReadiness(working);
  const disposition = classifyCommercialDisposition(working, {
    targetHotel: opts.targetHotel || working.hotelName || "",
  });

  return {
    arm: "B_agent_assisted",
    executionKind: "active_adaptive_bounded_public_research_cycle",
    adaptivePlanner: true,
    activeResearch: true,
    version: WAVE1_ARMS_VERSION,
    durationMs: Date.now() - t0,
    allowedOrigins,
    seedUrlCount: seeds.length,
    costs: {
      actualPaidUsd,
      fetchCount,
      fetchBudgetCap: maxFetches,
      mode: "wave1_live_public_zero_cost",
      deniedMetered: billing.deniedMetered,
    },
    retrievalRecords,
    liveCycleSummary: {
      retrievalsUsed: liveCycle.retrievalsUsed || 0,
      actionCount: (liveCycle.actions || []).length,
      stopLog: liveCycle.stopLog || [],
      nextActionLog: (liveCycle.nextActionLog || []).slice(0, 12),
    },
    recovery: {
      applied:
        recovery?.overlaysApplied?.demand === true ||
        recovery?.overlaysApplied?.buyer === true,
      category: recovery?.category || null,
    },
    independentChallenger: independent,
    sharedEligibility: {
      status: shared.status,
      salesReady: shared.salesReady === true,
      gaps: shared.gaps || [],
      challengerOverride: shared.challengerOverride || null,
    },
    supervisor: {
      blockers: (supervisor.blockers || []).map((b) => b.key || b),
      publishable: supervisor.publishable === true,
      hardStopReason: supervisor.hardStop?.reason || null,
    },
    drawer,
    disposition,
    workingOpportunity: working,
    commercial: summarizeCommercial(working, independent, shared, drawer, disposition, {
      durationMs: Date.now() - t0,
      actualPaidUsd,
      fetchCount,
    }),
  };
}

export function buildSharedParityContext(frozenOpportunity, limits = {}) {
  const seeds = collectOpportunitySeedUrls(frozenOpportunity);
  const allowedOrigins = deriveAllowedOrigins(seeds);
  return {
    maxFetchesPerCase: Number(limits.maxFetchesPerCase) || 12,
    maxPaidUsd: 0,
    allowedOrigins,
    seedUrls: seeds,
    paidProviders: "denied",
    providerPath: "official_page_direct_public_only",
  };
}
