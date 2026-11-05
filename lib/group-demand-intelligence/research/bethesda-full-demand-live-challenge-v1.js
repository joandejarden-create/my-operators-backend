/**
 * Bethesda Marriott Full-Demand Challenge — authorized live net-new discovery + deep research.
 * Aggregate ≤ $9 · Discovery ≤ $1.50 · Deep ≤ $7.50 · Per-opp ≤ $0.55
 * Isolated ledgers under reports/gdi-bethesda-full-demand-challenge-v1/live-ledgers/
 * Surfe/PDL/Parallel OFF · Airtable OFF · publication OFF · weekly = 0
 */

import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import { SUPERVISOR_EXPECTED_SEGMENTS } from "./research-constants-v1.js";
import { RESEARCH_SEGMENT_LABEL } from "../workflow/hotel-segment-research-plan-v1.js";
import {
  generateHotelDemandDiscoveryStrategies,
  loadHotelGdiConfig,
  normalizeDiscoveryHit,
  prioritizeDiscoveryHits,
  screenDiscoveryHitCommercialRelevance,
} from "./commercial-demand-discovery-v4.js";
import {
  assertLiveDeepResearchLedger,
  loadOrCreateLiveDeepResearchLedger,
  createLiveDeepResearchResearcher,
  preflightLiveDeepResearchPricing,
  maybeExaSearch,
} from "./deep-research-gdi-live-certification-v1.js";
import { runDeepResearchOnExistingGdiOpportunity } from "./deep-research-gdi-integration-v1.js";
import { assertPaidContactProvidersDisabled } from "./commercial-spine-restoration-v41.js";
import { INTERNAL_DISPOSITION } from "./commercial-qualification-v3.js";
import {
  BETHESDA_CANONICAL_HOTEL_ID,
  BETHESDA_CANONICAL_HOTEL_NAME,
  BETHESDA_CHALLENGE_BUDGET_USD,
  BETHESDA_FULL_DEMAND_REPORTS_DIR,
  assertBethesdaReconciliationGate,
  runBethesdaDatabaseReconciliationPreflight,
} from "./bethesda-full-demand-database-reconciliation-v1.js";
import {
  BETHESDA_WEEKLY_NET_NEW_V1,
  WEEKLY_FINDING_CLASS,
  WEEKLY_TARGET,
  buildBethesdaImmutableBaselineManifest,
  buildDiscoveryMemoryFromBaseline,
  classifyFindingAgainstBethesdaBaseline,
  buildBethesdaElevenSegmentFreshDiscoveryPlan,
} from "./bethesda-weekly-net-new-discovery-v1.js";

export const BETHESDA_FULL_DEMAND_LIVE_V1 = "gdi-bethesda-full-demand-live-challenge-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const LIVE_DIR = path.join(ROOT, BETHESDA_FULL_DEMAND_REPORTS_DIR, "live");
const LEDGER_DIR = path.join(ROOT, BETHESDA_FULL_DEMAND_REPORTS_DIR, "live-ledgers");
const DISCOVERY_LEDGER = path.join(LEDGER_DIR, "bethesda-weekly-net-new-discovery.json");
const DEEP_LEDGER = path.join(LEDGER_DIR, "bethesda-weekly-net-new-deep-research.json");

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function hashKey(s) {
  return createHash("sha256").update(String(s || "")).digest("hex").slice(0, 12);
}

function writeJson(p, obj) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, `${JSON.stringify(obj, null, 2)}\n`);
}

function ledgerSpent(ledgerPath) {
  if (!fs.existsSync(ledgerPath)) return 0;
  const led = JSON.parse(fs.readFileSync(ledgerPath, "utf8"));
  return Number(led.estimatedSpentUsd || 0);
}

function ledgerRemaining(ledgerPath, ceiling) {
  return Number((ceiling - ledgerSpent(ledgerPath)).toFixed(6));
}

/**
 * Honest commercial funnel — never report cost/actionable when hotel-facing yield is 0.
 * Housing-page account-development routes are tracked separately from hotel-facing actionable.
 */
export function computeHonestCommercialFunnelMetrics({
  grossDiscoveries = 0,
  validIdentities = 0,
  genuineNetNew = 0,
  credibleGroupDemand = 0,
  bethesdaFit = 0,
  deeplyResearched = 0,
  sourcingPathway = 0,
  usableContactRoute = 0,
  hotelFacingEligible = 0,
  customerActionable = 0,
  reactivated = 0,
  enrichedExisting = 0,
  housingPageLeads = 0,
  exclusionsByReason = {},
  aggregateSpendUsd = 0,
  namedBuyerSuccess = 0,
  verifiedContactRouteSuccess = 0,
} = {}) {
  const actionable = Number(customerActionable) || 0;
  const hotelFacing = Number(hotelFacingEligible) || 0;
  return {
    version: BETHESDA_FULL_DEMAND_LIVE_V1,
    grossDiscoveries,
    validIdentities,
    genuineNetNew,
    credibleGroupDemand,
    bethesdaFit,
    deeplyResearched,
    sourcingPathway,
    usableContactRoute,
    hotelFacingEligible: hotelFacing,
    customerActionableNew: actionable,
    reactivated,
    enrichedExisting,
    housingPageLeadsOnly: housingPageLeads,
    exclusionsByReason,
    namedBuyerIdentificationSuccess: namedBuyerSuccess,
    verifiedContactRouteSuccess,
    costPerCredibleNewUsd: credibleGroupDemand > 0
      ? Number((aggregateSpendUsd / credibleGroupDemand).toFixed(4))
      : null,
    costPerActionableNewUsd: actionable > 0
      ? Number((aggregateSpendUsd / actionable).toFixed(4))
      : null,
    costPerActionableUndefinedBecauseZeroYield: actionable === 0,
    note: actionable === 0
      ? "Cost per actionable opportunity is undefined when hotel-facing actionable yield is zero — do not report $0 or spend/housing-page-count."
      : "Unit economics use hotel-facing / customer-actionable count only.",
  };
}

/** Hotel-facing actionable dispositions (publication still gated separately). */
const HOTEL_FACING_DISPOSITIONS = new Set([
  INTERNAL_DISPOSITION.ACTIVE_OPPORTUNITY,
  INTERNAL_DISPOSITION.PRE_SOURCING_OPPORTUNITY,
  INTERNAL_DISPOSITION.ACCOUNT_DEVELOPMENT,
  INTERNAL_DISPOSITION.COMPETITIVE_FUTURE_CYCLE,
]);

const GENERIC_ORG_TITLES = /^(hotel|hotels|hotel\s*&\s*travel|travel|lodging|housing|venue|venues|catering|meeting\s*planner|hotel\s*headquarters|event\s*hotel.*|exhibitor\s*group\s*housing.*|accommodations?|home|about|contact|news|events|calendar|search)$/i;

function organizationFromHostname(url) {
  try {
    const host = new URL(url).hostname.replace(/^www\./i, "");
    // Prefer known association / event brand tokens from host
    const leaf = host.split(".").slice(0, -1).join(".").split(".").pop() || host;
    const cleaned = leaf
      .replace(/-?org$/i, "")
      .replace(/-?inc$/i, "")
      .replace(/^\d{4}/, "")
      .replace(/-/g, " ")
      .trim();
    if (!cleaned || cleaned.length < 2) return null;
    // Preserve acronyms (ausa, dri, aacpdm) as uppercase when short
    if (/^[a-z0-9]{2,8}$/i.test(cleaned.replace(/\s+/g, ""))) {
      return cleaned.replace(/\s+/g, "").toUpperCase();
    }
    return cleaned.replace(/\b\w/g, (c) => c.toUpperCase());
  } catch {
    return null;
  }
}

function organizationFromTitle(title) {
  const t = text(title);
  // "Hotel & Travel – AMAC Airport Business Conference" → AMAC Airport Business Conference
  const afterDash = t.split(/\s+[|\u2013\u2014–—]\s+/);
  if (afterDash.length >= 2) {
    const right = text(afterDash[afterDash.length - 1]);
    if (right && !GENERIC_ORG_TITLES.test(right) && right.length > 4) return right.slice(0, 120);
  }
  // "Venue | 2026 RCMI Consortium National Conference" → RCMI Consortium National Conference
  const afterPipe = t.split(/\s*\|\s*/);
  if (afterPipe.length >= 2) {
    const right = text(afterPipe[afterPipe.length - 1]).replace(/^\d{4}\s+/, "");
    if (right && !GENERIC_ORG_TITLES.test(right) && right.length > 4) return right.slice(0, 120);
  }
  return null;
}

/** Heuristic: pull org/program from Exa title + URL host + excerpt. */
function parseHitIdentity(hit = {}, segment = null) {
  const title = text(hit.title);
  const excerpt = text(hit.excerpt);
  const url = hit.url || hit.sourceUrl || "";
  const blob = `${title} ${excerpt}`;
  const fromTitle = organizationFromTitle(title);
  const fromHost = organizationFromHostname(url);
  let organizationName = fromTitle || fromHost || title.split(/\s+[|\u2013\u2014\-]\s+/)[0] || title;
  organizationName = text(organizationName).replace(/\s*[|:].*$/, "").slice(0, 120);

  // Reject generic page titles as organizations
  if (GENERIC_ORG_TITLES.test(organizationName) || organizationName.length < 3) {
    organizationName = fromHost || fromTitle || null;
  }

  const yearMatch = blob.match(/\b(202[6-9]|2030)\b/) || String(url).match(/\b(202[6-9]|2030)\b/);
  const programName = /conference|summit|symposium|meeting|tournament|cup|gala|workshop|forum|congress|championship|invitational|housing|hotel/i.test(blob)
    ? title.slice(0, 160)
    : (title.slice(0, 160) || null);

  const geoRelevant = /bethesda|montgomery|nih|walter\s*reed|rockville|dmv|maryland|washington\s*(dc|d\.c)|northern\s*virginia|fairfax|alexandria|arlington|potomac/i.test(blob + " " + url)
    || /bethesda|montgomery|maryland|wash|dc|dmv|nih/i.test(String(segment || ""));

  return {
    organizationName: organizationName ? text(organizationName) : null,
    programName,
    eventYear: yearMatch ? yearMatch[1] : null,
    demandSignalSummary: excerpt.slice(0, 400) || title,
    sourceUrl: url || null,
    publisher: hit.publisher || null,
    independentPublisher: hit.independent === true,
    segment,
    geoHintRelevant: geoRelevant,
    rejectedGenericTitle: !organizationName || GENERIC_ORG_TITLES.test(organizationName),
  };
}

function isWeakOrGenericCandidate(parsed = {}) {
  if (!parsed.organizationName) return true;
  if (parsed.rejectedGenericTitle) return true;
  if (GENERIC_ORG_TITLES.test(parsed.organizationName)) return true;
  if (/^(hotel|travel|lodging|housing|venue|catering)\b/i.test(parsed.organizationName)
    && parsed.organizationName.split(/\s+/).length <= 3) {
    return true;
  }
  return false;
}

function scoreNetNewCandidate(hit = {}, classification = {}) {
  let score = 0;
  if (classification.countsAsNetNew) score += 20;
  if (hit.programName) score += 15;
  if (/hotel|housing|room block|lodging|venue|meeting|conference|tournament|banquet/i.test(
    `${hit.programName || ""} ${hit.demandSignalSummary || ""}`,
  )) score += 18;
  if (hit.eventYear && Number(hit.eventYear) >= 2026) score += 10;
  if (hit.independentPublisher) score += 8;
  if (/bethesda|montgomery|nih|walter reed|rockville|dmv|washington/i.test(
    `${hit.demandSignalSummary || ""} ${hit.organizationName || ""}`,
  )) score += 12;
  if (classification.orgWasKnown) score += 5; // new program at known org still valuable
  return score;
}

function hasUsableContact(opp = {}) {
  const email = opp.contactEmail || opp.primaryContactEmail || opp.contact?.businessEmail || opp.primaryContact?.email;
  const url = opp.contactUrl || opp.organizationContactUrl || opp.publicContactPath;
  if (email && !/gmail|yahoo|hotmail|outlook\.com/i.test(email)) return true;
  if (url && !/^https?:\/\/[^/]+\/?$/i.test(url)) return true;
  return false;
}

/**
 * Preflight for founder-authorized live run.
 */
export function preflightBethesdaFullDemandLive({ founderAuthorized = false } = {}) {
  const pricing = preflightLiveDeepResearchPricing();
  const paidGuard = assertPaidContactProvidersDisabled({ authorizeSurfe: false, authorizePdl: false });
  const recon = runBethesdaDatabaseReconciliationPreflight();
  const reconGate = assertBethesdaReconciliationGate(recon, {
    allowPaidResearch: founderAuthorized,
    founderAuthorizedPaid: founderAuthorized,
  });
  const baseline = buildBethesdaImmutableBaselineManifest({ reconciliation: recon });
  const discoveryPlan = buildBethesdaElevenSegmentFreshDiscoveryPlan({ baseline });

  const checks = {
    founderAuthorized: founderAuthorized === true,
    pricingOk: pricing.ok === true,
    paidContactGuardOk: paidGuard.ok === true,
    reconciliationGateOk: reconGate.ok === true,
    baselineImmutable: baseline.immutable === true,
    baselineSourceRecords: baseline.counts.sourceRecords,
    baselineDistinctOpportunities: baseline.counts.distinctCanonicalOpportunities,
    researchMemoryNotNetNew: baseline.counts.researchMemoryPreviouslyKnownRecords >= 1,
    elevenSegmentsPlanned: discoveryPlan.allElevenSegmentsCovered === true,
    aggregateCeilingUsd: BETHESDA_CHALLENGE_BUDGET_USD.aggregateCeilingUsd,
    discoveryCeilingUsd: BETHESDA_CHALLENGE_BUDGET_USD.discoveryPhaseUsd,
    deepCeilingUsd: BETHESDA_CHALLENGE_BUDGET_USD.deepResearchPhaseUsd,
    perOppCeilingUsd: BETHESDA_CHALLENGE_BUDGET_USD.perOpportunityDeepResearchMaxUsd,
    surfePdlOff: true,
    parallelOff: true,
    airtableOff: true,
    weeklyOff: true,
    exaCredential: pricing.checks?.exaCredential,
    firecrawlCredential: pricing.checks?.firecrawlCredential,
  };

  const ok = checks.founderAuthorized
    && checks.pricingOk
    && checks.paidContactGuardOk
    && checks.reconciliationGateOk
    && checks.baselineImmutable
    && checks.elevenSegmentsPlanned
    && Number(checks.aggregateCeilingUsd) === 9;

  return {
    ok,
    checks,
    pricing,
    reconGate,
    baselineCounts: baseline.counts,
    discoveryPlanSummary: {
      strategyCount: discoveryPlan.strategyCount,
      allEleven: discoveryPlan.allElevenSegmentsCovered,
    },
  };
}

/**
 * Phase 1 — fresh Exa discovery across 11 segments under $1.50 discovery ledger.
 */
export async function executeBethesdaLiveDiscoveryPhase({
  baseline = null,
  maxStrategies = 28,
} = {}) {
  const manifest = baseline || buildBethesdaImmutableBaselineManifest();
  const memory = buildDiscoveryMemoryFromBaseline(manifest);
  const config = loadHotelGdiConfig(BETHESDA_CANONICAL_HOTEL_ID) || {
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    displayName: BETHESDA_CANONICAL_HOTEL_NAME,
  };
  if (!config.displayName) config.displayName = BETHESDA_CANONICAL_HOTEL_NAME;

  const discoveryPlan = buildBethesdaElevenSegmentFreshDiscoveryPlan({ baseline: manifest });
  const strategyPack = generateHotelDemandDiscoveryStrategies(config, { maxPerSegment: 3 });

  // Cover all 11: one primary per segment, then fill extras for productive segments
  const selected = [];
  const bySeg = new Map();
  for (const s of strategyPack.strategies) {
    if (!bySeg.has(s.segment)) bySeg.set(s.segment, []);
    bySeg.get(s.segment).push(s);
  }
  for (const seg of SUPERVISOR_EXPECTED_SEGMENTS) {
    const list = bySeg.get(seg) || [];
    if (list[0]) selected.push(list[0]);
  }
  for (const seg of (discoveryPlan.productiveSegmentsExtraEffort || [])) {
    const list = bySeg.get(seg) || [];
    for (const s of list.slice(1, 3)) {
      if (selected.length >= maxStrategies) break;
      if (!selected.includes(s)) selected.push(s);
    }
  }
  for (const s of strategyPack.strategies) {
    if (selected.length >= maxStrategies) break;
    if (!selected.includes(s)) selected.push(s);
  }

  assertLiveDeepResearchLedger(DISCOVERY_LEDGER);
  loadOrCreateLiveDeepResearchLedger(DISCOVERY_LEDGER, {
    budgetUsd: BETHESDA_CHALLENGE_BUDGET_USD.discoveryPhaseUsd,
    version: `${BETHESDA_FULL_DEMAND_LIVE_V1}-discovery-ledger`,
  });

  const searchLog = [];
  const rawParsed = [];
  const stopReasons = [];

  for (const strategy of selected) {
    const remaining = ledgerRemaining(DISCOVERY_LEDGER, BETHESDA_CHALLENGE_BUDGET_USD.discoveryPhaseUsd);
    if (remaining < 0.004) {
      stopReasons.push("discovery_budget_exhausted");
      searchLog.push({
        segment: strategy.segment,
        strategy: strategy.researchQuestion,
        status: "budget_exhausted",
        hits: 0,
        costUsd: 0,
      });
      break;
    }
    const result = await maybeExaSearch(strategy.researchQuestion, DISCOVERY_LEDGER);
    searchLog.push({
      segment: strategy.segment,
      strategy: strategy.researchQuestion,
      status: result.status,
      hits: (result.hits || []).length,
      costUsd: result.costUsd || 0,
    });
    for (const hit of result.hits || []) {
      const parsed = parseHitIdentity(hit, strategy.segment);
      if (isWeakOrGenericCandidate(parsed)) {
        rawParsed.push({ ...parsed, _screenExclude: "generic_or_weak_identity" });
        continue;
      }
      const screen = screenDiscoveryHitCommercialRelevance(parsed, config || {
        hotelId: BETHESDA_CANONICAL_HOTEL_ID,
        displayName: BETHESDA_CANONICAL_HOTEL_NAME,
        city: "Bethesda",
      });
      if (!screen.pass) {
        rawParsed.push({
          ...parsed,
          _screenExclude: screen.rejectReasons.join("|") || "commercial_screen_fail",
          _screen: screen,
        });
        continue;
      }
      rawParsed.push({ ...parsed, _screen: screen });
    }
  }

  const usableParsed = rawParsed.filter((h) => !h._screenExclude && h.organizationName);
  const normalized = usableParsed.map((h) => normalizeDiscoveryHit(h, {
    hotel: config,
    strategy: { segment: h.segment, researchQuestion: null },
  }));

  // Classify vs immutable baseline (stronger than org-only memory dedupe)
  const classified = [];
  for (let i = 0; i < normalized.length; i += 1) {
    const hit = normalized[i];
    const raw = usableParsed[i] || {};
    let classification = classifyFindingAgainstBethesdaBaseline({
      organizationName: hit.organizationName,
      programName: hit.programName,
      eventName: hit.programName,
      eventYear: raw.eventYear || null,
      demandSignalSummary: hit.demandSignalSummary,
      demandMechanism: hit.programName || hit.demandSignalSummary,
      sourceUrl: hit.sourceUrl,
      segment: hit.segment,
    }, manifest);

    // Force exclude weak demand / past cycles / no program specificity
    if (!hit.programName && !/conference|meeting|tournament|summit|symposium/i.test(hit.demandSignalSummary || "")) {
      classification = {
        class: WEEKLY_FINDING_CLASS.EXCLUDED,
        resolutionKey: classification.resolutionKey,
        matchedBaselineId: null,
        reason: "no_specific_demand_mechanism",
        countsAsNetNew: false,
      };
    } else if (raw.eventYear && Number(raw.eventYear) < 2026) {
      classification = {
        class: WEEKLY_FINDING_CLASS.EXCLUDED,
        resolutionKey: classification.resolutionKey,
        matchedBaselineId: null,
        reason: "past_cycle_not_forward_looking",
        countsAsNetNew: false,
      };
    }

    const score = scoreNetNewCandidate({
      ...hit,
      eventYear: raw.eventYear,
      independentPublisher: hit.independentPublisher,
      geoHintRelevant: raw.geoHintRelevant,
    }, classification) + (raw.geoHintRelevant ? 8 : 0);
    classified.push({
      ...hit,
      eventYear: raw.eventYear || null,
      geoHintRelevant: raw.geoHintRelevant === true,
      classification,
      challengeScore: score,
    });
  }

  // Also record screened-out generic title hits as EXCLUDED for funnel honesty
  for (const weak of rawParsed.filter((h) => h._screenExclude)) {
    classified.push({
      organizationName: weak.organizationName || "(generic title)",
      programName: weak.programName,
      segment: weak.segment,
      sourceUrl: weak.sourceUrl,
      demandSignalSummary: weak.demandSignalSummary,
      eventYear: weak.eventYear,
      classification: {
        class: WEEKLY_FINDING_CLASS.EXCLUDED,
        reason: weak._screenExclude,
        countsAsNetNew: false,
      },
      challengeScore: 0,
    });
  }

  const netNew = classified
    .filter((c) => c.classification.class === WEEKLY_FINDING_CLASS.NET_NEW_OPPORTUNITY)
    .sort((a, b) => b.challengeScore - a.challengeScore);
  const reactivated = classified.filter((c) => c.classification.class === WEEKLY_FINDING_CLASS.REACTIVATED_OPPORTUNITY);
  const enriched = classified.filter((c) => c.classification.class === WEEKLY_FINDING_CLASS.EXISTING_ENRICHED);
  const duplicates = classified.filter((c) => c.classification.class === WEEKLY_FINDING_CLASS.DUPLICATE);
  const excluded = classified.filter((c) => c.classification.class === WEEKLY_FINDING_CLASS.EXCLUDED);
  const unchanged = classified.filter((c) => c.classification.class === WEEKLY_FINDING_CLASS.EXISTING_UNCHANGED);

  const bySegment = {};
  for (const seg of SUPERVISOR_EXPECTED_SEGMENTS) {
    const rows = classified.filter((c) => c.segment === seg);
    bySegment[seg] = {
      label: RESEARCH_SEGMENT_LABEL[seg] || seg,
      strategiesRun: searchLog.filter((s) => s.segment === seg && s.status === "ok").length,
      rawHits: rows.length,
      netNew: rows.filter((r) => r.classification.countsAsNetNew).length,
      duplicates: rows.filter((r) => r.classification.class === WEEKLY_FINDING_CLASS.DUPLICATE).length,
      excluded: rows.filter((r) => r.classification.class === WEEKLY_FINDING_CLASS.EXCLUDED).length,
    };
  }

  const prioritized = prioritizeDiscoveryHits(
    netNew.map((n) => ({
      ...n,
      demandSignalSummary: n.demandSignalSummary,
    })),
    config,
    { maxInvestigate: 22 },
  );

  return {
    discoverySpentUsd: ledgerSpent(DISCOVERY_LEDGER),
    discoveryRemainingUsd: ledgerRemaining(DISCOVERY_LEDGER, BETHESDA_CHALLENGE_BUDGET_USD.discoveryPhaseUsd),
    strategiesSelected: selected.length,
    strategiesExecutedOk: searchLog.filter((s) => s.status === "ok").length,
    searchLog,
    stopReasons,
    memoryKnownOrgCount: (memory.knownOrganizations || []).length,
    funnel: {
      rawHitsParsed: rawParsed.length,
      classified: classified.length,
      netNew: netNew.length,
      reactivated: reactivated.length,
      enriched: enriched.length,
      duplicates: duplicates.length,
      excluded: excluded.length,
      unchanged: unchanged.length,
      deepResearchEligibleQueued: (prioritized.toInvestigate || []).length,
    },
    bySegment,
    netNewCandidates: prioritized.toInvestigate || netNew.slice(0, 22),
    classifiedSummary: classified.map((c) => ({
      organizationName: c.organizationName,
      programName: c.programName,
      segment: c.segment,
      class: c.classification.class,
      countsAsNetNew: c.classification.countsAsNetNew,
      reason: c.classification.reason,
      score: c.challengeScore,
      sourceUrl: c.sourceUrl,
    })),
    reactivated,
    enriched,
    duplicates: duplicates.slice(0, 40),
    excluded: excluded.slice(0, 40),
  };
}

function isNoiseOrCompetitorCandidate(c = {}) {
  const url = String(c.sourceUrl || "");
  const org = String(c.organizationName || "");
  if (/exa\.ai|allevents\.in|eventbrite\.|facebook\.com|linkedin\.com|tripadvisor\./i.test(url)) return true;
  if (/placemakr|hamiltonhotel|hilton\.com|hyatt\.com|ihg\.com|marriott\.com\/en-us\/hotels/i.test(url)) return true;
  if (/^(EXA|AllEvents|Placemakr|Hamiltonhoteldc|Eventphotojournalism)$/i.test(org)) return true;
  if (/competitor|corporate housing & extended/i.test(String(c.programName || ""))) return true;
  return false;
}

function toDeepResearchOpportunity(candidate = {}) {
  const id = `gdi_netnew_${hashKey(`${candidate.organizationName}::${candidate.programName || ""}::${candidate.segment || ""}`)}`;
  const seedUrl = candidate.sourceUrl || null;
  return {
    id,
    opportunityId: id,
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    hotelName: BETHESDA_CANONICAL_HOTEL_NAME,
    organizationName: candidate.organizationName,
    eventName: candidate.programName || null,
    programName: candidate.programName || null,
    canonicalEventName: candidate.programName || null,
    researchSegment: candidate.segment,
    segment: candidate.segment,
    demandSignalSummary: candidate.demandSignalSummary,
    hotelOpportunityThesis:
      `Evaluate whether Bethesda Marriott can compete for lodging/meetings for ${candidate.organizationName}`
      + (candidate.programName ? ` — ${candidate.programName}` : "")
      + `. DMV / NIH / Montgomery catchment relevance must be evidence-backed; do not invent a room block.`,
    sourceUrl: seedUrl,
    officialSource: seedUrl,
    seedUrls: seedUrl ? [seedUrl] : [],
    sources: seedUrl ? [{ url: seedUrl, publisher: candidate.publisher }] : [],
    evidence: [],
    opportunityType: "PRIMARY_PURSUIT",
    discoverySource: "bethesda_full_demand_live_net_new",
    weeklyFindingClass: WEEKLY_FINDING_CLASS.NET_NEW_OPPORTUNITY,
    eventYear: candidate.eventYear || null,
  };
}

/**
 * Phase 2 — deep research strongest NET_NEW under $7.50 / $0.55 per opp.
 */
export async function executeBethesdaLiveDeepResearchPhase({
  netNewCandidates = [],
  maxCandidates = 14,
} = {}) {
  assertLiveDeepResearchLedger(DEEP_LEDGER);
  loadOrCreateLiveDeepResearchLedger(DEEP_LEDGER, {
    budgetUsd: BETHESDA_CHALLENGE_BUDGET_USD.deepResearchPhaseUsd,
    version: `${BETHESDA_FULL_DEMAND_LIVE_V1}-deep-research-ledger`,
  });

  const checkpointDir = path.join(LIVE_DIR, "checkpoints");
  fs.mkdirSync(checkpointDir, { recursive: true });

  const results = [];
  const stopReasons = [];
  const queue = netNewCandidates.slice(0, maxCandidates);

  for (const candidate of queue) {
    const remaining = ledgerRemaining(DEEP_LEDGER, BETHESDA_CHALLENGE_BUDGET_USD.deepResearchPhaseUsd);
    const perOpp = Math.min(
      BETHESDA_CHALLENGE_BUDGET_USD.perOpportunityDeepResearchMaxUsd,
      remaining,
    );
    if (perOpp < 0.02) {
      stopReasons.push("deep_research_budget_exhausted");
      break;
    }

    // Low VoI stop: already have enough actionable with usable contacts
    const actionableSoFar = results.filter((r) => r.actionable === true).length;
    if (actionableSoFar >= WEEKLY_TARGET.netNewActionableMax && remaining < 1.0) {
      stopReasons.push("sufficient_actionable_net_new_low_voi_stop");
      break;
    }

    const opportunity = toDeepResearchOpportunity(candidate);
    const spentBefore = ledgerSpent(DEEP_LEDGER);
    const researcher = createLiveDeepResearchResearcher({ ledgerPath: DEEP_LEDGER });

    let deepResult = null;
    let error = null;
    try {
      deepResult = await runDeepResearchOnExistingGdiOpportunity({
        opportunity,
        researcher,
        corpus: null,
        offlineLongform: false,
        options: {
          allowEarlyBuyerStop: false,
          allowReplan: true,
          maxIterations: 4,
          maxLeadFollowUpsPerOpportunity: 2,
          remainingBudgetUsd: perOpp,
          maxCostUsd: perOpp,
          checkpointDir,
        },
      });
    } catch (err) {
      error = err.message || String(err);
    }

    const spentAfter = ledgerSpent(DEEP_LEDGER);
    const oppSpend = Number((spentAfter - spentBefore).toFixed(6));
    const afterWrap = deepResult?.after || {};
    const after = afterWrap.opportunity || deepResult?.opportunity || opportunity;
    const ownership = afterWrap.contactOwnership || {};
    const namedStakeholders = afterWrap.namedStakeholders || [];
    const salesBrief = deepResult?.hotelSalesBrief || after?.deepResearchEnrichment?.salesBrief || {};
    const housingPageRoute = (/\/(hotel|housing|lodging|travel|accommodat)/i.test(String(candidate.sourceUrl || ""))
      || /hotel|housing|lodging|room\s*block/i.test(String(candidate.programName || "")))
      ? (candidate.sourceUrl || after.sourceUrl || null)
      : null;
    // Housing page URL is evidence, not a sales-usable contact route.
    const ownershipRoute = ownership.salesUsableAsProspectContact
      ? (ownership.route || null)
      : null;
    const contactRoute = ownershipRoute
      || after.contactEmail || after.primaryContactEmail || after.contact?.businessEmail
      || null;
    const contactOk = Boolean(contactRoute) || hasUsableContact(after)
      || (ownership.salesUsableAsProspectContact === true && Boolean(ownership.route));
    const evidenceCount = (after.evidence || []).length + (after.sources || []).length
      + (after.investigatedUrls || []).length;
    const demandCredible = Boolean(
      after.eventName
      || after.programName
      || after.demandSignalSummary
      || candidate.programName
      || evidenceCount > 0
      || afterWrap.qualificationV41?.commerciallyUsefulInternal
      || afterWrap.qualificationV41?.internalAccountDevelopment,
    );
    const q41 = afterWrap.qualificationV41 || {};
    const disposition = String(q41.internalDisposition || after.internalDisposition || "");
    const hotelFit = Boolean(
      after.hotelFitRationale
      || q41.commerciallyUsefulInternal
      || q41.internalAccountDevelopment
      || salesBrief?.evidenceBackedHotelFit
      || (/bethesda|montgomery|nih|dmv|rockville/i.test(String(after.hotelOpportunityThesis || ""))
        && evidenceCount > 0),
    );
    const forwardLooking = !candidate.eventYear || Number(candidate.eventYear) >= 2026;
    // Housing pages are research leads / account-development watch — NOT hotel-facing actionable alone.
    const housingPageLeadOnly = Boolean(housingPageRoute) && !contactOk;
    const accountDevelopmentProspect = demandCredible
      && forwardLooking
      && !isNoiseOrCompetitorCandidate(candidate)
      && HOTEL_FACING_DISPOSITIONS.has(disposition)
      && contactOk
      && q41.commerciallyUsefulInternal === true;
    // Hotel-facing actionable requires V4.1 useful + ownership-valid contact — never housing URL alone.
    const actionable = accountDevelopmentProspect
      && !/insufficient|no.?demand/i.test(String(afterWrap.orsStopReason || after.stopReason || ""));

    const brief = {
      organization: after.organizationName || opportunity.organizationName,
      demandOpportunity: after.eventName || after.programName || candidate.programName
        || after.demandSignalSummary || candidate.demandSignalSummary || null,
      canonicalSegment: after.researchSegment || after.segment || candidate.segment,
      evidenceUrls: [
        ...(after.sources || []).map((s) => s.url || s).filter(Boolean),
        ...(after.evidence || []).map((e) => e.url).filter(Boolean),
        ...(after.investigatedUrls || []),
        after.officialSource,
        after.sourceUrl,
        candidate.sourceUrl,
      ].filter(Boolean).slice(0, 8),
      potentialRevenueMechanism: after.revenueTypes || after.relevantRevenue
        || "rooms_and_or_meetings_inferred_pending_qualification",
      commercialStage: disposition
        || after.commercialStage || after.opportunityType
        || "WATCHLIST_OR_UNRESOLVED",
      datesOrWindow: after.eventDateDisplay || after.eventStartDate || after.eventYear
        || candidate.eventYear || null,
      bethesdaFit: after.hotelOpportunityThesis || after.hotelFitRationale
        || opportunity.hotelOpportunityThesis
        || salesBrief?.evidenceBackedHotelFit || null,
      buyerOrganization: after.organizationName || opportunity.organizationName,
      responsibleFunction: ownership.roleTitle || after.primaryContact?.title
        || after.contactRole || after.buyerRole || null,
      namedPeople: [
        ownership.personName,
        after.primaryContactName || after.contactName || after.primaryContact?.name,
        ...namedStakeholders.map((p) => p?.name || p?.personName).filter(Boolean),
      ].filter(Boolean).slice(0, 5),
      contactRoute: contactRoute || null,
      housingPageEvidenceUrl: housingPageRoute || null,
      contactConfidence: contactOk
        ? (ownership.ownershipClass || "USABLE_ROUTE_PRESENT")
        : (housingPageLeadOnly ? "HOUSING_PAGE_EVIDENCE_ONLY_NOT_CONTACT" : "NO_USABLE_ROUTE"),
      accountDevelopmentActionable: accountDevelopmentProspect === true,
      housingPageLeadOnly,
      recommendedSalesApproach: salesBrief?.recommendedQuestionOrPitch
        || salesBrief?.whatShouldISayOrAsk
        || after.recommendedAction
        || "Qualify demand mechanism, lodging need, and hotel sourcing owner before pitching Bethesda Marriott.",
      unresolvedQuestions: after.unresolvedQuestions || [
        "Confirm lodging/meeting demand (not directory presence alone)",
        "Confirm who selects or contracts hotels",
        "Confirm timing / next cycle and Bethesda competitiveness vs downtown DC",
      ],
      inferenceBoundaries: "Do not invent RFPs, room blocks, overflow awards, or unverified buyer identities",
      qualificationV41Disposition: disposition || null,
      commerciallyUsefulInternal: q41.commerciallyUsefulInternal === true,
    };

    results.push({
      opportunityId: opportunity.id,
      organizationName: opportunity.organizationName,
      programName: opportunity.eventName,
      segment: opportunity.segment,
      spentUsd: oppSpend,
      perOppCeilingUsd: perOpp,
      error,
      actionable,
      housingPageLeadOnly: housingPageLeadOnly === true,
      demandCredible,
      hotelFit,
      contactOk,
      stopReason: afterWrap.orsStopReason || after.stopReason || null,
      brief,
      researcherTrace: {
        exaQueries: researcher.trace?.exaQueries?.length || 0,
        attempted: researcher.trace?.attempted?.length || 0,
        executed: researcher.trace?.executed?.length || 0,
        relatedEntityExecuted: researcher.trace?.relatedEntity?.executed?.length || 0,
      },
    });
  }

  return {
    deepSpentUsd: ledgerSpent(DEEP_LEDGER),
    deepRemainingUsd: ledgerRemaining(DEEP_LEDGER, BETHESDA_CHALLENGE_BUDGET_USD.deepResearchPhaseUsd),
    candidatesQueued: queue.length,
    candidatesResearched: results.length,
    actionableNetNew: results.filter((r) => r.actionable),
    researchedButNotActionable: results.filter((r) => !r.actionable),
    results,
    stopReasons,
  };
}

/**
 * Remediation: re-identity prior discovery hits (no discovery re-spend) and deep-research
 * stronger NET_NEW candidates under remaining deep-research budget.
 */
export async function executeBethesdaLiveRemediationFromPriorDiscovery({
  founderAuthorized = false,
} = {}) {
  if (!founderAuthorized) {
    return { executed: false, reason: "founder_authorized_required" };
  }
  const funnelPath = path.join(LIVE_DIR, "04-candidate-funnel.json");
  if (!fs.existsSync(funnelPath)) {
    return { executed: false, reason: "missing_prior_discovery_funnel" };
  }
  const prior = JSON.parse(fs.readFileSync(funnelPath, "utf8"));
  const baseline = buildBethesdaImmutableBaselineManifest();
  const rows = prior.classifiedSummary || [];

  const reparsed = [];
  for (const row of rows) {
    const parsed = parseHitIdentity({
      title: row.programName || row.organizationName,
      excerpt: row.demandSignalSummary || "",
      url: row.sourceUrl,
      publisher: null,
    }, row.segment);
    // Prefer hostname org when prior org was generic
    if (isWeakOrGenericCandidate({ organizationName: row.organizationName })
      || GENERIC_ORG_TITLES.test(row.organizationName || "")) {
      if (isWeakOrGenericCandidate(parsed)) continue;
      reparsed.push({ ...parsed, segment: row.segment || parsed.segment });
    } else if (!isWeakOrGenericCandidate(parsed)) {
      reparsed.push({
        ...parsed,
        organizationName: parsed.organizationName || row.organizationName,
        segment: row.segment || parsed.segment,
      });
    }
  }

  // Dedupe by org+program
  const seen = new Set();
  const unique = [];
  for (const r of reparsed) {
    const k = `${normalizeDiscoveryHit(r).organizationName}::${r.programName || ""}`.toLowerCase();
    if (seen.has(k)) continue;
    seen.add(k);
    unique.push(r);
  }

  const classified = [];
  for (const hit of unique) {
    let classification = classifyFindingAgainstBethesdaBaseline({
      organizationName: hit.organizationName,
      programName: hit.programName,
      eventName: hit.programName,
      eventYear: hit.eventYear,
      demandSignalSummary: hit.demandSignalSummary,
      demandMechanism: hit.programName || hit.demandSignalSummary,
      sourceUrl: hit.sourceUrl,
      segment: hit.segment,
    }, baseline);
    if (hit.eventYear && Number(hit.eventYear) < 2026) {
      classification = {
        class: WEEKLY_FINDING_CLASS.EXCLUDED,
        reason: "past_cycle_not_forward_looking",
        countsAsNetNew: false,
        resolutionKey: classification.resolutionKey,
        matchedBaselineId: null,
      };
    }
    const score = scoreNetNewCandidate(hit, classification) + (hit.geoHintRelevant ? 8 : 0);
    classified.push({ ...hit, classification, challengeScore: score });
  }

  const netNew = classified
    .filter((c) => c.classification.countsAsNetNew && !isNoiseOrCompetitorCandidate(c))
    .sort((a, b) => b.challengeScore - a.challengeScore);

  writeJson(path.join(LIVE_DIR, "05b-net-new-reconciliation-remediated.json"), {
    priorGenericInflatedNetNew: prior.netNew || prior.classified || null,
    reparsedUsable: unique.length,
    netNewAfterIdentityFix: netNew.length,
    candidates: netNew.slice(0, 30).map((c) => ({
      organizationName: c.organizationName,
      programName: c.programName,
      segment: c.segment,
      score: c.challengeScore,
      sourceUrl: c.sourceUrl,
      geoHintRelevant: c.geoHintRelevant,
      classification: c.classification,
    })),
  });

  // Prefer geo-relevant association/medical housing pages over directories/competitors
  const preferred = netNew.filter((c) =>
    c.geoHintRelevant
    || /nih|nci|association|symposium|conference|tournament|housing|hotel/i.test(
      `${c.organizationName} ${c.programName} ${c.sourceUrl}`,
    ));
  const deepQueue = (preferred.length ? preferred : netNew).slice(0, 12);

  const deep = await executeBethesdaLiveDeepResearchPhase({
    netNewCandidates: deepQueue,
    maxCandidates: 12,
  });

  const actionable = deep.actionableNetNew;

  writeJson(path.join(LIVE_DIR, "06b-deep-research-remediation-traces.json"), {
    spentUsdThisWave: deep.deepSpentUsd,
    remainingUsd: deep.deepRemainingUsd,
    stopReasons: deep.stopReasons,
    results: deep.results,
  });

  writeJson(path.join(LIVE_DIR, "09-top-net-new-list.json"), {
    target: WEEKLY_TARGET,
    actualActionableNetNew: actionable.length,
    paddedFromBaseline: false,
    remediationApplied: true,
    items: actionable.slice(0, WEEKLY_TARGET.netNewActionableMax).map((r, i) => ({
      rank: i + 1,
      ...r.brief,
      opportunityId: r.opportunityId,
      spentUsd: r.spentUsd,
    })),
    belowTarget: actionable.length < WEEKLY_TARGET.netNewActionableMin,
    note: actionable.length < WEEKLY_TARGET.netNewActionableMin
      ? "Yield below aspirational 10–15 after identity remediation — reported honestly"
      : "Within or above aspirational band after remediation",
  });
  writeJson(path.join(LIVE_DIR, "08-hotel-specific-opportunity-briefs.json"), {
    count: actionable.length,
    briefs: actionable.map((r) => r.brief),
    remediationApplied: true,
  });
  writeJson(path.join(LIVE_DIR, "07-buyer-and-contact-intelligence.json"), {
    actionableWithContact: actionable.filter((r) => r.contactOk).length,
    briefs: actionable.map((r) => ({
      opportunityId: r.opportunityId,
      namedPeople: r.brief.namedPeople,
      contactRoute: r.brief.contactRoute,
      contactConfidence: r.brief.contactConfidence,
      responsibleFunction: r.brief.responsibleFunction,
    })),
    remediationApplied: true,
  });

  const discoverySpend = ledgerSpent(DISCOVERY_LEDGER);
  const deepSpend = ledgerSpent(DEEP_LEDGER);
  const aggregate = Number((discoverySpend + deepSpend).toFixed(6));
  const housingLeads = deep.results.filter((r) => r.housingPageLeadOnly).length;
  const funnelMetrics = computeHonestCommercialFunnelMetrics({
    genuineNetNew: netNew.length,
    deeplyResearched: deep.candidatesResearched,
    hotelFacingEligible: actionable.length,
    customerActionable: actionable.length,
    housingPageLeads: housingLeads,
    usableContactRoute: deep.results.filter((r) => r.contactOk).length,
    aggregateSpendUsd: aggregate,
    namedBuyerSuccess: actionable.filter((r) => (r.brief?.namedPeople || []).length > 0).length,
    verifiedContactRouteSuccess: actionable.filter((r) => r.contactOk).length,
  });
  const costPerQualified = funnelMetrics.costPerActionableNewUsd;

  writeJson(path.join(LIVE_DIR, "11-actual-spending-and-unit-economics.json"), {
    ceilingsUsd: { ...BETHESDA_CHALLENGE_BUDGET_USD },
    actualUsd: { discovery: discoverySpend, deepResearch: deepSpend, aggregate },
    remainingUsd: {
      discovery: ledgerRemaining(DISCOVERY_LEDGER, BETHESDA_CHALLENGE_BUDGET_USD.discoveryPhaseUsd),
      deepResearch: ledgerRemaining(DEEP_LEDGER, BETHESDA_CHALLENGE_BUDGET_USD.deepResearchPhaseUsd),
      aggregate: Number((9 - aggregate).toFixed(6)),
    },
    costPerQualifiedNetNewUsd: costPerQualified,
    costPerActionableUndefinedBecauseZeroYield: funnelMetrics.costPerActionableUndefinedBecauseZeroYield,
    honestFunnel: funnelMetrics,
    remediationWave: true,
    ledgers: {
      discovery: DISCOVERY_LEDGER.replace(/\\/g, "/"),
      deepResearch: DEEP_LEDGER.replace(/\\/g, "/"),
    },
  });

  const commercialAssessment = {
    primaryYieldActionableNetNew: actionable.length,
    housingPageLeadsOnly: housingLeads,
    targetBand: [WEEKLY_TARGET.netNewActionableMin, WEEKLY_TARGET.netNewActionableMax],
    withinOrAboveTarget: actionable.length >= WEEKLY_TARGET.netNewActionableMin,
    technicalDiscoveryExecuted: true,
    technicalDeepResearchExecuted: deep.candidatesResearched > 0,
    commercialUsefulness: actionable.length >= 3
      ? (actionable.length >= 10 ? "STRONG" : "PARTIAL_USEFUL")
      : (actionable.length >= 1 ? "THIN" : "INSUFFICIENT"),
    salespersonCouldPursue: actionable.length > 0,
    didNotPadFromBaseline: true,
    remediationIdentityFixApplied: true,
    priorWaveMisidentifiedTitles: true,
    honestNote: actionable.length === 0
      ? "Zero hotel-facing actionable yield — housing-page leads are not counted as actionable."
      : null,
  };

  const weeklyPilotGoNoGo = {
    decision: actionable.length >= 5 && aggregate <= 9 && commercialAssessment.commercialUsefulness !== "INSUFFICIENT"
      ? "CONDITIONAL_GO_LIMITED_INTERNAL_WEEKLY_PILOT"
      : "NO_GO_WEEKLY_PILOT_PENDING_YIELD_IMPROVEMENT",
    weeklyProductionRemainsDisabled: true,
    customerPublication: "OFF",
    rationale: actionable.length >= 5
      ? "Enough commercial signal after remediation for limited internal pilot review — production weekly stays OFF"
      : "Insufficient actionable net-new yield for weekly pilot recommendation",
  };

  writeJson(path.join(LIVE_DIR, "12-commercial-quality-assessment.json"), commercialAssessment);
  writeJson(path.join(LIVE_DIR, "13-blockers-and-weekly-pilot-gonogo.json"), {
    blockers: [
      ...(actionable.length < 10 ? [`NET_NEW_YIELD_BELOW_TARGET_${actionable.length}_OF_10`] : []),
      "INITIAL_WAVE_GENERIC_TITLE_IDENTITY_FAILURE_REMEDIATED",
    ],
    weeklyPilotGoNoGo,
    remediation: {
      netNewAfterIdentityFix: netNew.length,
      deepResearchedThisWave: deep.candidatesResearched,
      actionableThisWave: actionable.length,
    },
  });

  const summary = {
    version: BETHESDA_FULL_DEMAND_LIVE_V1,
    wave: "remediation_identity_fix",
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    executed: true,
    completedAt: new Date().toISOString(),
    funnel: {
      reparsedUsable: unique.length,
      netNewAfterIdentityFix: netNew.length,
      deepResearched: deep.candidatesResearched,
      actionableNetNew: actionable.length,
    },
    spendingUsd: {
      discovery: discoverySpend,
      deepResearch: deepSpend,
      aggregate,
      costPerQualifiedNetNew: costPerQualified,
    },
    commercialAssessment,
    weeklyPilotGoNoGo,
    liveDir: `${BETHESDA_FULL_DEMAND_REPORTS_DIR}/live/`,
  };
  writeJson(path.join(LIVE_DIR, "SUMMARY.json"), summary);
  writeJson(path.join(LIVE_DIR, "DELIVERABLES_LIVE.md"), `# Bethesda Full-Demand Live Challenge — Deliverables (remediated)

**Hotel:** Bethesda Marriott (\`${BETHESDA_CANONICAL_HOTEL_ID}\`)  
**Wave:** identity remediation (generic Exa titles → org/host parsing)  
**Aggregate spend:** $${aggregate} / $9.00  

## Primary yield
- Actionable NET_NEW (remediated): **${actionable.length}**
- NET_NEW after identity fix (pre-deep): **${netNew.length}**
- Padded from baseline: **false**
- Commercial usefulness: **${commercialAssessment.commercialUsefulness}**

## Note on initial wave
First deep-research wave wasted budget on generic page titles ("Hotel & Travel", "Venue"). Identity parsing was fixed; remediation reused discovery URLs without rediscovery spend.

## Weekly pilot
**${weeklyPilotGoNoGo.decision}** — production weekly remains disabled; publication OFF.
`);

  return summary;
}

/**
 * Full authorized live challenge — discovery then deep research — stop for review.
 */
export async function executeBethesdaFullDemandLiveChallenge({
  founderAuthorized = false,
} = {}) {
  const startedAt = new Date().toISOString();
  fs.mkdirSync(LIVE_DIR, { recursive: true });
  fs.mkdirSync(LEDGER_DIR, { recursive: true });

  const preflight = preflightBethesdaFullDemandLive({ founderAuthorized });
  writeJson(path.join(LIVE_DIR, "00-PREFLIGHT.json"), preflight);
  if (!preflight.ok) {
    return {
      executed: false,
      reason: "preflight_failed",
      preflight,
      spentUsd: { discovery: 0, deepResearch: 0, aggregate: 0 },
    };
  }

  const baseline = buildBethesdaImmutableBaselineManifest();
  writeJson(path.join(LIVE_DIR, "01-baseline-freeze.json"), {
    frozenAt: baseline.frozenAt,
    counts: baseline.counts,
    corrections: baseline.corrections,
    indexes: {
      opportunityIdCount: baseline.indexes.baselineOpportunityIds.length,
      resolutionKeyCount: baseline.indexes.baselineResolutionKeys.length,
      organizationKeyCount: baseline.indexes.baselineOrganizationKeys.length,
    },
  });

  const discovery = await executeBethesdaLiveDiscoveryPhase({ baseline });
  writeJson(path.join(LIVE_DIR, "02-live-discovery-ledger.json"), {
    ledgerPath: DISCOVERY_LEDGER.replace(/\\/g, "/"),
    spentUsd: discovery.discoverySpentUsd,
    remainingUsd: discovery.discoveryRemainingUsd,
    ceilingUsd: BETHESDA_CHALLENGE_BUDGET_USD.discoveryPhaseUsd,
    searchLog: discovery.searchLog,
    stopReasons: discovery.stopReasons,
  });
  writeJson(path.join(LIVE_DIR, "03-eleven-segment-coverage-and-yield.json"), discovery.bySegment);
  writeJson(path.join(LIVE_DIR, "04-candidate-funnel.json"), {
    ...discovery.funnel,
    classifiedSummary: discovery.classifiedSummary,
  });
  writeJson(path.join(LIVE_DIR, "05-net-new-reconciliation.json"), {
    netNewCount: discovery.funnel.netNew,
    netNewCandidates: discovery.netNewCandidates.map((c) => ({
      organizationName: c.organizationName,
      programName: c.programName,
      segment: c.segment,
      score: c.challengeScore || c.commercialValueScore,
      sourceUrl: c.sourceUrl,
      classification: c.classification,
    })),
    note: "Only NET_NEW counts toward primary target; research-memory recoveries excluded by gate",
  });

  const deep = await executeBethesdaLiveDeepResearchPhase({
    netNewCandidates: discovery.netNewCandidates,
    maxCandidates: 14,
  });

  writeJson(path.join(LIVE_DIR, "06-deep-research-execution-traces.json"), {
    ledgerPath: DEEP_LEDGER.replace(/\\/g, "/"),
    spentUsd: deep.deepSpentUsd,
    remainingUsd: deep.deepRemainingUsd,
    stopReasons: deep.stopReasons,
    results: deep.results.map((r) => ({
      opportunityId: r.opportunityId,
      organizationName: r.organizationName,
      spentUsd: r.spentUsd,
      actionable: r.actionable,
      contactOk: r.contactOk,
      researcherTrace: r.researcherTrace,
      error: r.error,
      stopReason: r.stopReason,
    })),
  });

  const actionable = deep.actionableNetNew;
  const topList = actionable.slice(0, WEEKLY_TARGET.netNewActionableMax);

  writeJson(path.join(LIVE_DIR, "07-buyer-and-contact-intelligence.json"), {
    actionableWithContact: actionable.filter((r) => r.contactOk).length,
    briefs: actionable.map((r) => ({
      opportunityId: r.opportunityId,
      namedPeople: r.brief.namedPeople,
      contactRoute: r.brief.contactRoute,
      contactConfidence: r.brief.contactConfidence,
      responsibleFunction: r.brief.responsibleFunction,
    })),
  });
  writeJson(path.join(LIVE_DIR, "08-hotel-specific-opportunity-briefs.json"), {
    count: actionable.length,
    briefs: actionable.map((r) => r.brief),
  });
  writeJson(path.join(LIVE_DIR, "09-top-net-new-list.json"), {
    target: WEEKLY_TARGET,
    actualActionableNetNew: actionable.length,
    paddedFromBaseline: false,
    items: topList.map((r, i) => ({
      rank: i + 1,
      ...r.brief,
      opportunityId: r.opportunityId,
      spentUsd: r.spentUsd,
    })),
    belowTarget: actionable.length < WEEKLY_TARGET.netNewActionableMin,
    note: actionable.length < WEEKLY_TARGET.netNewActionableMin
      ? "Yield below aspirational 10–15 — reported honestly; did not substitute baseline opportunities"
      : "Within or above aspirational band",
  });
  writeJson(path.join(LIVE_DIR, "10-reactivated-and-enriched.json"), {
    B_reactivated: discovery.reactivated.map((c) => ({
      organizationName: c.organizationName,
      programName: c.programName,
      reason: c.classification.reason,
    })),
    C_enrichedExisting: discovery.enriched.map((c) => ({
      organizationName: c.organizationName,
      programName: c.programName,
      reason: c.classification.reason,
    })),
    note: "Separate from NET_NEW primary yield — not substitutes",
  });

  const discoverySpend = discovery.discoverySpentUsd;
  const deepSpend = deep.deepSpentUsd;
  const aggregate = Number((discoverySpend + deepSpend).toFixed(6));
  const housingLeads = deep.results.filter((r) => r.housingPageLeadOnly).length;
  const funnelMetrics = computeHonestCommercialFunnelMetrics({
    grossDiscoveries: discovery.funnel?.gross || discovery.strategiesExecutedOk || 0,
    genuineNetNew: discovery.funnel?.netNew || 0,
    deeplyResearched: deep.candidatesResearched,
    hotelFacingEligible: actionable.length,
    customerActionable: actionable.length,
    housingPageLeads: housingLeads,
    usableContactRoute: deep.results.filter((r) => r.contactOk).length,
    aggregateSpendUsd: aggregate,
    namedBuyerSuccess: actionable.filter((r) => (r.brief?.namedPeople || []).length > 0).length,
    verifiedContactRouteSuccess: actionable.filter((r) => r.contactOk).length,
  });
  const costPerQualified = funnelMetrics.costPerActionableNewUsd;

  writeJson(path.join(LIVE_DIR, "11-actual-spending-and-unit-economics.json"), {
    ceilingsUsd: { ...BETHESDA_CHALLENGE_BUDGET_USD },
    actualUsd: {
      discovery: discoverySpend,
      deepResearch: deepSpend,
      aggregate,
    },
    remainingUsd: {
      discovery: discovery.discoveryRemainingUsd,
      deepResearch: deep.deepRemainingUsd,
      aggregate: Number((9 - aggregate).toFixed(6)),
    },
    costPerQualifiedNetNewUsd: costPerQualified,
    costPerActionableUndefinedBecauseZeroYield: funnelMetrics.costPerActionableUndefinedBecauseZeroYield,
    honestFunnel: funnelMetrics,
    ledgers: {
      discovery: DISCOVERY_LEDGER.replace(/\\/g, "/"),
      deepResearch: DEEP_LEDGER.replace(/\\/g, "/"),
    },
    usesPriorBalances: false,
  });

  const commercialAssessment = {
    primaryYieldActionableNetNew: actionable.length,
    housingPageLeadsOnly: housingLeads,
    targetBand: [WEEKLY_TARGET.netNewActionableMin, WEEKLY_TARGET.netNewActionableMax],
    withinOrAboveTarget: actionable.length >= WEEKLY_TARGET.netNewActionableMin,
    technicalDiscoveryExecuted: discovery.strategiesExecutedOk > 0,
    technicalDeepResearchExecuted: deep.candidatesResearched > 0,
    commercialUsefulness: actionable.length >= 3
      ? (actionable.length >= 10 ? "STRONG" : "PARTIAL_USEFUL")
      : (actionable.length >= 1 ? "THIN" : "INSUFFICIENT"),
    salespersonCouldPursue: actionable.length > 0,
    didNotPadFromBaseline: true,
    didNotCountResearchMemoryAsNetNew: true,
    honestNote: actionable.length === 0
      ? "Zero hotel-facing actionable yield — housing-page leads are not counted as actionable."
      : null,
  };

  const blockers = [];
  if (actionable.length < WEEKLY_TARGET.netNewActionableMin) {
    blockers.push(`NET_NEW_YIELD_BELOW_TARGET_${actionable.length}_OF_10`);
  }
  if (discovery.funnel.netNew < 10) {
    blockers.push("LIMITED_NET_NEW_AFTER_BASELINE_DEDUPE");
  }
  if (deep.researchedButNotActionable.length) {
    blockers.push(`DEEP_RESEARCHED_NOT_ACTIONABLE_${deep.researchedButNotActionable.length}`);
  }
  if (discovery.stopReasons.includes("discovery_budget_exhausted")) {
    blockers.push("DISCOVERY_BUDGET_HIT");
  }
  if (deep.stopReasons.includes("deep_research_budget_exhausted")) {
    blockers.push("DEEP_RESEARCH_BUDGET_HIT");
  }
  const contactGaps = deep.results.filter((r) => r.demandCredible && !r.contactOk).length;
  if (contactGaps) blockers.push(`CONTACT_ROUTE_GAPS_${contactGaps}`);

  const weeklyPilotGoNoGo = {
    decision: actionable.length >= 5 && aggregate <= 9 && commercialAssessment.commercialUsefulness !== "INSUFFICIENT"
      ? "CONDITIONAL_GO_LIMITED_INTERNAL_WEEKLY_PILOT"
      : "NO_GO_WEEKLY_PILOT_PENDING_YIELD_IMPROVEMENT",
    weeklyProductionRemainsDisabled: true,
    customerPublication: "OFF",
    rationale: actionable.length >= 5
      ? "Enough commercial signal for a limited internal pilot review — production weekly stays OFF"
      : "Insufficient actionable net-new yield for weekly pilot recommendation",
  };

  writeJson(path.join(LIVE_DIR, "12-commercial-quality-assessment.json"), commercialAssessment);
  writeJson(path.join(LIVE_DIR, "13-blockers-and-weekly-pilot-gonogo.json"), {
    blockers,
    weeklyPilotGoNoGo,
    failureReasonsSample: deep.researchedButNotActionable.slice(0, 10).map((r) => ({
      organizationName: r.organizationName,
      demandCredible: r.demandCredible,
      hotelFit: r.hotelFit,
      contactOk: r.contactOk,
      error: r.error,
      stopReason: r.stopReason,
    })),
  });

  const summary = {
    version: BETHESDA_FULL_DEMAND_LIVE_V1,
    weeklyNetNewVersion: BETHESDA_WEEKLY_NET_NEW_V1,
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    hotelName: BETHESDA_CANONICAL_HOTEL_NAME,
    startedAt,
    completedAt: new Date().toISOString(),
    executed: true,
    funnel: {
      ...discovery.funnel,
      deepResearched: deep.candidatesResearched,
      actionableNetNew: actionable.length,
      topListCount: topList.length,
    },
    spendingUsd: {
      discovery: discoverySpend,
      deepResearch: deepSpend,
      aggregate,
      costPerQualifiedNetNew: costPerQualified,
    },
    commercialAssessment,
    weeklyPilotGoNoGo,
    blockers,
    liveDir: `${BETHESDA_FULL_DEMAND_REPORTS_DIR}/live/`,
  };
  writeJson(path.join(LIVE_DIR, "SUMMARY.json"), summary);
  writeJson(path.join(LIVE_DIR, "DELIVERABLES_LIVE.md"), `# Bethesda Full-Demand Live Challenge — Deliverables

**Hotel:** Bethesda Marriott (\`${BETHESDA_CANONICAL_HOTEL_ID}\`)  
**Executed:** ${summary.completedAt}  
**Aggregate spend:** $${aggregate} / $9.00  

## Primary yield
- Actionable NET_NEW: **${actionable.length}** (target aspirational 10–15)
- Padded from baseline: **false**
- Commercial usefulness: **${commercialAssessment.commercialUsefulness}**

## Funnel
| Stage | Count |
|-------|------:|
| Raw discovery hits | ${discovery.funnel.rawHitsParsed} |
| Classified | ${discovery.funnel.classified} |
| NET_NEW after baseline gate | ${discovery.funnel.netNew} |
| Duplicates | ${discovery.funnel.duplicates} |
| Excluded | ${discovery.funnel.excluded} |
| Deep researched | ${deep.candidatesResearched} |
| Actionable NET_NEW | ${actionable.length} |

## Spend
| Phase | Spent | Ceiling |
|-------|------:|--------:|
| Discovery | $${discoverySpend} | $1.50 |
| Deep research | $${deepSpend} | $7.50 |
| Aggregate | $${aggregate} | $9.00 |
| Cost / qualified NET_NEW | ${costPerQualified ?? "n/a"} | |

## Weekly pilot
**${weeklyPilotGoNoGo.decision}** — production weekly remains disabled; publication OFF.

## Files
00–13 JSON artifacts in this folder + SUMMARY.json
`);

  return summary;
}
