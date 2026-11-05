/**
 * External evidence acquisition for GDI_AUTONOMOUS_HOTEL_FULL_E2E_V2.
 *
 * Archive recovery stays first. Official pages already cited on the opportunity
 * can be re-read with the existing contact resolver. Search and identity
 * providers stay blocked unless a caller passes a fresh authorization.
 */

import { hasUsableContactRoute, evaluateProspectPublicationEligibility } from "../prospect-publication-eligibility-v1.js";
import { classifyDemandPosture } from "./gdi-three-hotel-full-e2e-v1.js";
import { detectSourceLanguage } from "../discovery/multilingual-evidence-normalize-v1.js";
import { extractContactIntelligenceFromSource } from "../extract-contact-intelligence-from-source.js";
import { extractCommercialEvidenceFromSource } from "./gdi-live-evidence-yield-v1.js";
import { resolveOpportunityContact } from "../weekly-contact-resolution-v1-2.js";
import { SURFE_EMAIL_CREDIT_USD_ESTIMATE } from "./paid-multisegment-research-v1.js";
import { researchLanguages, localLanguageQueries, canonicalEntityKey } from "./gdi-global-weekly-feedback-v2.js";
import { readBenchmarkFlags } from "../research/benchmark-provider-flags-v1.js";
import { quoteBenchmarkCall } from "../research/benchmark-provider-pricing-v1.js";

export const EVIDENCE_ACQUISITION_VERSION = "gdi-evidence-acquisition-v2.2";
export const PUBLIC_FETCH_COST_USD = 0;
export const SEARCH_QUERY_ESTIMATE_USD = 0.01;

const DECISION_ROLE = /housing|lodging|meeting|event manager|procurement|contrataci[oó]n|h[eé]bergement|宿泊/i;

export function citedOfficialUrls(opp = {}) {
  const urls = [];
  const push = (value) => {
    const url = String(value || "").trim();
    if (/^https?:\/\//i.test(url) && !urls.includes(url)) urls.push(url);
  };
  push(opp.officialSource);
  push(opp.discoverySource);
  push(opp.lodgingEvidenceUrl);
  push(opp.contactOfficialUrl);
  for (const source of opp.sources || []) push(source.url || source.sourceUrl || source);
  for (const item of opp.evidence || []) push(item.sourceUrl);
  return urls;
}

export function classifyEvidenceGap(opp = {}, context = {}) {
  const route = hasUsableContactRoute(opp);
  const urls = citedOfficialUrls(opp);
  let contactClass = "USABLE";
  if (!route.ok && route.reason === "homepage_or_vague_only") contactClass = "VAGUE_PAGE_NOT_A_DECISION_MAKER";
  else if (!route.ok && context.canonicalHit) contactClass = "PRESENT_IN_CANONICAL_ARCHIVE";
  else if (!route.ok && context.siblingRoute) contactClass = "PRESENT_ON_DUPLICATE_SURVIVOR";
  else if (!route.ok && urls.length) contactClass = "CITED_SOURCE_NOT_EXTRACTED";
  else if (!route.ok) contactClass = "ABSENT_FROM_ARCHIVE";
  const posture = classifyDemandPosture(opp);
  const historical = opp.demandStatus || null;
  const confirmedLabel = /CONFIRMED/i.test(String(historical || ""));
  return {
    opportunityId: opp.id || null,
    hotelId: context.hotelId || opp.hotelId || null,
    contactClass,
    contactReason: route.reason || null,
    officialUrlCount: urls.length,
    firstOfficialUrl: urls[0] || null,
    historicalDemandStatus: historical,
    currentPosture: posture.posture,
    currentOpenBooking: posture.posture === "CONFIRMED",
    confirmedEventIsNotOpenDemand: confirmedLabel && posture.posture !== "CONFIRMED",
    demandContradiction: confirmedLabel && posture.posture !== "CONFIRMED",
    publicationAffected: opp.customerVisible === true && evaluateProspectPublicationEligibility(opp).hotelFacing !== true,
  };
}

export function buildLocalResearchQueries(profile = {}, organizationName = "") {
  const org = String(organizationName || "").trim();
  const languages = researchLanguages(profile);
  return languages.map((language) => {
    const terms = localLanguageQueries({ primaryLanguage: language, secondaryLanguages: [] }, "congress")[0];
    return {
      language,
      query: [org, terms?.query].filter(Boolean).join(" "),
      executable: false,
      blocker: "SEARCH_PROVIDER_NOT_AUTHORIZED",
    };
  });
}

export function planExternalAction(opp = {}, gap, context = {}) {
  const fingerprint = `acquire:${opp.id}:${gap.contactClass}:${gap.firstOfficialUrl || "none"}`;
  if ((context.completedFingerprints || []).includes(fingerprint)) {
    return { action: "SKIP_ALREADY_EXECUTED", executableNow: false, fingerprint, costUsd: 0, reason: "idempotent replay" };
  }
  if (context.mode === "WEEKLY_INCREMENTAL" && context.unchangedIds?.has(opp.id) && gap.contactClass === "USABLE" && !gap.demandContradiction) {
    return { action: "SKIP_UNCHANGED", executableNow: false, fingerprint, costUsd: 0, reason: "No new gap on this weekly cycle." };
  }
  if (gap.contactClass === "USABLE" || gap.contactClass === "PRESENT_IN_CANONICAL_ARCHIVE" || gap.contactClass === "PRESENT_ON_DUPLICATE_SURVIVOR") {
    return { action: "ARCHIVE_SUFFICIENT", executableNow: false, fingerprint, costUsd: 0, reason: "Existing archive already answers the contact question." };
  }
  if (gap.contactClass === "VAGUE_PAGE_NOT_A_DECISION_MAKER") {
    return {
      action: "HOLD_VAGUE_CONTACT",
      executableNow: false,
      fingerprint,
      costUsd: 0,
      reason: "A general or homepage contact is already stored. It is not a verified decision-maker.",
    };
  }
  if (gap.contactClass === "CITED_SOURCE_NOT_EXTRACTED") {
    return {
      action: "READ_CITED_OFFICIAL_PAGE",
      executableNow: context.allowPublicFetch === true,
      fingerprint,
      costUsd: PUBLIC_FETCH_COST_USD,
      sourceTier: "OFFICIAL_ORGANIZER_OR_EVENT_PAGE",
      reason: "An official URL is already cited. Re-read that page. Do not run a paid search.",
    };
  }
  return {
    action: "PAID_SOURCE_SEARCH",
    executableNow: false,
    blocker: "BLOCKED_AUTHORIZATION",
    fingerprint,
    costUsd: null,
    estimatedSearchUsd: null,
    rejectedSearchUsd: SEARCH_QUERY_ESTIMATE_USD,
    estimatedIdentityUsd: SURFE_EMAIL_CREDIT_USD_ESTIMATE,
    reason: "No cited official URL remains. A search needs its own authorization. The $0.01 placeholder is not a billing rate.",
  };
}

/**
 * Benchmark-only choice of Exa or Firecrawl for one evidence gap.
 * Does not change the production action or mark a paid search executable.
 */
export function planBenchmarkAcquisition(opp = {}, context = {}) {
  const gap = context.gap || classifyEvidenceGap(opp, context);
  const production = planExternalAction(opp, gap, context);
  const closed = {
    ...production,
    benchmarkExecutable: false,
    benchmarkProvider: null,
    benchmarkOperation: null,
    benchmarkParams: null,
    benchmarkArm: null,
    benchmarkMaxUsd: null,
    benchmarkRefusal: null,
  };
  if (context.benchmarkEvaluation !== true) return closed;
  if (!readBenchmarkFlags(context.env || process.env).evaluationEnabled) {
    return { ...closed, benchmarkRefusal: "BENCHMARK_EVAL_DISABLED" };
  }

  const filePath = context.localDocumentPath || opp.localDocumentPath || null;
  const plannerAction = context.plannerAction;
  if (plannerAction?.fingerprint && plannerAction.strategy) {
    const operation = plannerAction.operation || "search";
    const provider = operation === "scrape" || operation === "parse" ? "firecrawl" : "exa";
    const params =
      operation === "contents"
        ? { urls: plannerAction.urls || [], text: true }
        : operation === "scrape"
          ? { url: plannerAction.urls?.[0], formats: ["markdown"] }
          : operation === "parse"
            ? { filePath: plannerAction.filePath, maxPages: plannerAction.maxPages, formats: ["markdown"] }
            : provider === "firecrawl"
              ? { query: plannerAction.queries?.[0] || context.query, limit: 5 }
              : { query: plannerAction.queries?.[0] || context.query, numResults: 5, type: "lite" };
    const priced = quoteBenchmarkCall({ provider, operation, params });
    return {
      ...closed,
      benchmarkExecutable: priced.bounded,
      benchmarkProvider: priced.bounded ? provider : null,
      benchmarkOperation: operation,
      benchmarkParams: params,
      benchmarkArm: provider === "firecrawl" ? "4_firecrawl" : "3_exa",
      benchmarkMaxUsd: priced.maxUsd,
      benchmarkRefusal: priced.bounded ? null : priced.reason,
      plannerFingerprint: plannerAction.fingerprint,
    };
  }

  if (filePath) {
    const params = {
      filePath,
      maxPages: context.maxPages,
      formats: ["markdown"],
    };
    const quote = quoteBenchmarkCall({ provider: "firecrawl", operation: "parse", params });
    return {
      ...closed,
      benchmarkExecutable: quote.bounded,
      benchmarkProvider: quote.bounded ? "firecrawl" : null,
      benchmarkOperation: "parse",
      benchmarkParams: params,
      benchmarkArm: "5_complementary",
      benchmarkMaxUsd: quote.maxUsd,
      benchmarkRefusal: quote.bounded ? null : quote.reason,
    };
  }

  const url = gap.firstOfficialUrl;
  const pdf = /\.pdf(?:$|\?)/i.test(String(url || "")) || context.sourceKind === "pdf";
  const thin =
    context.priorExaTextLength != null && Number(context.priorExaTextLength) < 200;
  if (url && /^https:\/\//i.test(url) && (pdf || thin || context.sourceKind === "javascript")) {
    const params = { url, formats: ["markdown"] };
    const quote = quoteBenchmarkCall({ provider: "firecrawl", operation: "scrape", params });
    return {
      ...closed,
      benchmarkExecutable: quote.bounded,
      benchmarkProvider: "firecrawl",
      benchmarkOperation: "scrape",
      benchmarkParams: params,
      benchmarkArm: "5_complementary",
      benchmarkMaxUsd: quote.maxUsd,
      benchmarkRefusal: quote.bounded ? null : quote.reason,
    };
  }
  if (url && /^https:\/\//i.test(url)) {
    const params = { urls: [url], text: true };
    const quote = quoteBenchmarkCall({ provider: "exa", operation: "contents", params });
    return {
      ...closed,
      benchmarkExecutable: quote.bounded,
      benchmarkProvider: "exa",
      benchmarkOperation: "contents",
      benchmarkParams: params,
      benchmarkArm: "3_exa",
      benchmarkMaxUsd: quote.maxUsd,
      benchmarkRefusal: quote.bounded ? null : quote.reason,
    };
  }

  const query = String(context.query || opp.organizationName || "").trim();
  if (!query) return { ...closed, benchmarkRefusal: "MISSING_RESEARCH_TARGET" };
  const provider = context.benchmarkProvider === "firecrawl" ? "firecrawl" : "exa";
  const params =
    provider === "firecrawl"
      ? { query, limit: 10 }
      : { query, numResults: 10, type: "lite" };
  const quote = quoteBenchmarkCall({ provider, operation: "search", params });
  return {
    ...closed,
    benchmarkExecutable: quote.bounded,
    benchmarkProvider: provider,
    benchmarkOperation: "search",
    benchmarkParams: params,
    benchmarkArm: provider === "firecrawl" ? "4_firecrawl" : "3_exa",
    benchmarkMaxUsd: quote.maxUsd,
    benchmarkRefusal: quote.bounded ? null : quote.reason,
  };
}

export function interpretRetrievedPage({ url, pageText, retrievedAt, language }) {
  const extracted = extractContactIntelligenceFromSource({
    source: { url, sourceType: "official_web" },
    pageContent: pageText || "",
  });
  const commercial = extractCommercialEvidenceFromSource({ url, pageHtml: pageText || "" });
  const people = extracted.people || [];
  const functional = extracted.functionalContacts || [];
  const named = people.filter((person) => person.personName || person.name);
  const decision = named.find((person) => DECISION_ROLE.test(`${person.role || ""} ${person.title || ""}`));
  return {
    url,
    retrievedAt: retrievedAt || null,
    language: language || detectSourceLanguage(pageText || ""),
    originalTextPreserved: true,
    functionalContactFound: named.length > 0 || functional.length > 0,
    verifiedDecisionMaker: Boolean(decision),
    confidence: decision ? "MEDIUM" : named.length || functional.length ? "LOW" : "NONE",
    costUsd: PUBLIC_FETCH_COST_USD,
    commercial,
  };
}

export async function executeOfficialPageRead(opp, context = {}) {
  const gap = classifyEvidenceGap(opp, context);
  const plan = planExternalAction(opp, gap, context);
  if (plan.action !== "READ_CITED_OFFICIAL_PAGE" || plan.executableNow !== true) {
    return { ok: false, plan, acquired: false, costUsd: 0 };
  }
  const url = gap.firstOfficialUrl;
  const working = {
    ...opp,
    sources: [{ url }],
    officialSource: url,
    discoverySource: null,
    lodgingEvidenceUrl: null,
    contactOfficialUrl: null,
  };
  let result;
  try {
    result = await resolveOpportunityContact(working, {
      allowSourceRecovery: false,
      allowNetworkDomainResolution: false,
      allowFollowupFetches: context.allowFollowupFetches !== false,
    });
  } catch (error) {
    return {
      ok: false,
      acquired: false,
      plan,
      fetchesUsed: 0,
      queriesUsed: 0,
      surfeUsed: false,
      costUsd: 0,
      blocked: String(error.message || error).slice(0, 180),
    };
  }
  const acquired = result.fetchesUsed > 0 && result.skipped !== true;
  return {
    ok: acquired && (result.functionalAdded === true || result.namedAdded === true || result.improved === true),
    acquired,
    plan,
    fetchesUsed: result.fetchesUsed || 0,
    fetchLedger: result.fetchLedger || [],
    queriesUsed: result.queriesUsed || 0,
    surfeUsed: result.surfeUsed === true,
    costUsd: 0,
    citation: {
      url,
      retrievedAt: new Date().toISOString(),
      language: detectSourceLanguage(`${opp.organizationName || ""} ${opp.title || ""}`),
      blankReason: result.blankReason || null,
      afterTier: result.afterTier || null,
      verifiedDecisionMaker: false,
    },
    researchHistoryEntry: {
      action: "READ_CITED_OFFICIAL_PAGE",
      fingerprint: plan.fingerprint,
      url,
      costUsd: 0,
      surfeUsed: false,
      yield: result.improved === true,
    },
  };
}

export function deriveCurrentCommercialConclusion(opp = {}) {
  const gap = classifyEvidenceGap(opp);
  return {
    historicalClassification: gap.historicalDemandStatus,
    currentPosture: gap.currentPosture,
    currentOpenBooking: gap.currentOpenBooking,
    confirmedEventIsNotOpenDemand: gap.confirmedEventIsNotOpenDemand,
    contradiction: gap.demandContradiction,
    publicationFlag: opp.customerVisible === true,
    publicationRewrite: false,
  };
}

export function buildPublicationDifference(opportunities = [], recoveryById = new Map()) {
  const buckets = { keep: [], remove: [], add: [], downgrade: [], superseded: [], revise: [] };
  for (const opp of opportunities) {
    let facing = false;
    try {
      facing = evaluateProspectPublicationEligibility(opp).hotelFacing === true;
    } catch {
      facing = false;
    }
    const visible = opp.customerVisible === true;
    const recovery = recoveryById.get(opp.id);
    const row = { id: opp.id, organization: opp.organizationName || null, storedVisible: visible, currentHotelFacing: facing };
    if (recovery?.reason === "event_concluded" || recovery?.lifecycle === "SUPERSEDED") buckets.superseded.push(row);
    else if (visible && facing) buckets.keep.push(row);
    else if (visible && !facing) buckets.remove.push(row);
    else if (!visible && facing) buckets.add.push(row);
    if (visible && (recovery?.qualificationProvenance || recovery?.whyNowStatus)) buckets.revise.push(row);
    if (visible && !facing) buckets.downgrade.push(row);
  }
  return {
    canonicalEligibility: "evaluateProspectPublicationEligibility",
    publicationChanged: false,
    counts: {
      keep: buckets.keep.length,
      remove: buckets.remove.length,
      add: buckets.add.length,
      downgrade: buckets.downgrade.length,
      superseded: buckets.superseded.length,
      revise: buckets.revise.length,
    },
    buckets,
  };
}

export function buildResearchQueue(opportunities, context = {}) {
  const seen = new Set();
  const items = [];
  for (const opp of opportunities) {
    const gap = classifyEvidenceGap(opp, context);
    const plan = planExternalAction(opp, gap, context);
    if (seen.has(plan.fingerprint)) continue;
    seen.add(plan.fingerprint);
    items.push({
      hotelId: context.hotelId || opp.hotelId,
      opportunityId: opp.id,
      question: gap.contactClass,
      action: plan.action,
      fingerprint: plan.fingerprint,
      attempts: 0,
      status: plan.executableNow ? "QUEUED" : plan.blocker || (plan.action === "READ_CITED_OFFICIAL_PAGE" ? "PUBLIC_FETCH_AVAILABLE" : "NOT_REQUIRED"),
      costUsd: plan.costUsd || 0,
      sourceTier: plan.sourceTier || null,
    });
  }
  return { version: EVIDENCE_ACQUISITION_VERSION, hotelId: context.hotelId || null, items };
}

export function resumeResearchQueue(queue, checkpoint = {}) {
  const done = new Set(checkpoint.completedFingerprints || []);
  return {
    ...queue,
    items: queue.items.map((item) => {
      if (done.has(item.fingerprint)) return { ...item, status: "RESUME_SKIPPED", attempts: item.attempts };
      if ((checkpoint.attempts || {})[item.fingerprint] >= 2) {
        if (item.status === "BLOCKED_AUTHORIZATION" || item.status === "PUBLIC_FETCH_AVAILABLE" || item.action === "PAID_SOURCE_SEARCH") {
          return {
            ...item,
            status: item.action === "READ_CITED_OFFICIAL_PAGE" ? "PUBLIC_FETCH_AVAILABLE" : "BLOCKED_AUTHORIZATION",
            attempts: checkpoint.attempts[item.fingerprint],
            reason: "authorization_blocker_is_not_exhaustion",
          };
        }
        return { ...item, status: "EXHAUSTED", attempts: 2, reason: "bounded_retries" };
      }
      return item;
    }),
  };
}

export function sameOrganizationAcrossLanguages(left, right) {
  const a = canonicalEntityKey(left);
  const b = canonicalEntityKey(right);
  return Boolean(a) && a === b;
}

export function summarizeAcquisitionPlan(opportunities = [], context = {}) {
  const queue = buildResearchQueue(opportunities, context);
  const counts = {};
  for (const item of queue.items) counts[item.action] = (counts[item.action] || 0) + 1;
  return {
    version: EVIDENCE_ACQUISITION_VERSION,
    hotelId: context.hotelId || null,
    counts,
    publicReadsAvailable: counts.READ_CITED_OFFICIAL_PAGE || 0,
    paidSearchBlocked: counts.PAID_SOURCE_SEARCH || 0,
    researchCostUsd: 0,
    publicationChanged: false,
  };
}

export function redactAcquisition(value) {
  return JSON.parse(JSON.stringify(value, (key, item) => {
    if (/email|phone|mobile/i.test(key)) return undefined;
    if (typeof item === "string" && item.includes("@")) return "[redacted]";
    return item;
  }));
}
