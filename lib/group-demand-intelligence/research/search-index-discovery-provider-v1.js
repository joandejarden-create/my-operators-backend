/**
 * GDI 2.0 Phase 3B-R — reusable search-index discovery provider.
 *
 * Reuses Scaling / Association Exa hit shape and query patterns.
 * Default path is injectible `searchFn` (mocked Exa). Live `maybeExaSearch`
 * only when `authorizeLiveMeteredSearch === true` AND ledgerPath is set —
 * never the default for Phase 3B-R.
 */

import {
  DISCOVERY_ORIGIN,
  extractIndependentCandidatesFromPage,
  investigationIdForDiscovery,
} from "./independent-demand-discovery-v1.js";
import { candidateIdentityKey, dedupeDiscoveryCandidates } from "../candidate-dedupe.js";

export const SEARCH_INDEX_DISCOVERY_PROVIDER_V1 = "gdi-search-index-discovery-provider-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/**
 * Hotel- and segment-specific search questions (Scaling/Association style).
 */
export function buildHotelSegmentSearchQuestions(hotel = {}, segmentConfig = {}) {
  const city = text(hotel.city || hotel.demandTerritory?.label?.split("/")?.[0] || "");
  const market = city || text(hotel.market || hotel.demandTerritory?.label || "the market");
  const segmentId = text(segmentConfig.segmentId || "generic");
  const yearHint = "2025 OR 2026";

  const questions = [];
  const push = (id, researchQuestion, category) => {
    if (!researchQuestion) return;
    questions.push({
      id,
      researchQuestion,
      category: category || segmentId,
      segmentId,
      city: city || null,
      hotelId: hotel.hotelId || null,
    });
  };

  if (/corporate_meetings|cmt|meetings_training/i.test(segmentId)) {
    push(
      "cmt_training_program",
      `${market} corporate training academy OR training cohort OR onboarding academy hotel OR lodging OR overnight OR residential ${yearHint}`,
      "CORPORATE_TRAINING_ACADEMIES",
    );
    push(
      "cmt_leadership_meeting",
      `${market} leadership offsite OR leadership retreat OR annual sales kickoff OR annual sales meeting hotel OR meeting rooms ${yearHint}`,
      "ANNUAL_SALES_KICKOFFS",
    );
    push(
      "cmt_meetings_press",
      `${market} corporate meeting OR company offsite lodging OR overnight meeting rooms ${yearHint}`,
      "CORPORATE_MEETINGS",
    );
  } else if (/corporate_projects|extended_stay|cpe/i.test(segmentId)) {
    push(
      "cpe_tech_implementation",
      `${market} technology implementation OR system cutover OR vendor deployment onsite field team OR extended stay ${yearHint}`,
      "TECHNOLOGY_IMPLEMENTATIONS",
    );
    push(
      "cpe_engineering_deploy",
      `${market} engineering deployment OR construction mobilization OR onsite field crew lodging OR hotel ${yearHint}`,
      "CONSTRUCTION_ENGINEERING_PROJECTS",
    );
    push(
      "cpe_datacenter",
      `${market} data center OR datacenter infrastructure build OR fit-out contractors mobilize OR crew ${yearHint}`,
      "DATACENTER_INFRASTRUCTURE_PROJECTS",
    );
  } else {
    push(
      "generic_demand",
      `${market} corporate project OR training OR meeting hotel lodging OR overnight ${yearHint}`,
      "GENERIC",
    );
  }

  // Honor blocked-placeholder query strings from the independent source plan
  for (const q of segmentConfig.extraSearchQuestions || []) {
    push(q.id || `extra_${questions.length}`, q.researchQuestion || q.query, q.category);
  }

  return questions;
}

/**
 * Create a deterministic mocked Exa-compatible search function.
 * `responses` may be:
 *   - Array of { match?: RegExp|string, status, hits, costUsd }
 *   - Object map of query substring → { status, hits, costUsd }
 *   - Function (query) => result
 */
export function createMockedExaSearchFn(responses = []) {
  if (typeof responses === "function") return responses;

  const rows = Array.isArray(responses)
    ? responses
    : Object.entries(responses || {}).map(([match, body]) => ({ match, ...body }));

  return async function mockedExaSearch(query) {
    const q = text(query);
    for (const row of rows) {
      const m = row.match;
      const hit =
        m == null ||
        m === "*" ||
        (typeof m === "string" && q.toLowerCase().includes(m.toLowerCase())) ||
        (m instanceof RegExp && m.test(q));
      if (!hit) continue;
      if (row.fail === true || row.status === "error") {
        return {
          status: row.status || "error",
          hits: [],
          costUsd: Number(row.costUsd || 0),
          error: row.error || "mocked_provider_failure",
          mocked: true,
        };
      }
      return {
        status: row.status || "ok",
        hits: Array.isArray(row.hits) ? row.hits : [],
        costUsd: Number(row.costUsd || 0),
        mocked: true,
      };
    }
    return { status: "ok", hits: [], costUsd: 0, mocked: true };
  };
}

function hitToPageText(hit = {}) {
  return text([hit.title, hit.excerpt, hit.text, hit.highlights?.join?.(" ")].filter(Boolean).join("\n"));
}

/**
 * Execute search-index discovery pass (mocked or injected). Fail closed on budget /
 * missing authorization. Never invents candidates.
 */
export async function runSearchIndexDiscoveryPass({
  hotel = {},
  segmentConfig = {},
  searchFn = null,
  authorizeSearchIndexDiscovery = false,
  authorizeLiveMeteredSearch = false,
  budgetCeilingUsd = 0,
  ledger = null,
  limits = {},
  liveSearchFn = null,
} = {}) {
  const workingLedger = ledger || {
    externalRetrievals: 0,
    estimatedUsd: 0,
    attemptedUrls: [],
    requests: [],
    searchQueries: [],
  };
  if (!workingLedger.searchQueries) workingLedger.searchQueries = [];

  const base = {
    version: SEARCH_INDEX_DISCOVERY_PROVIDER_V1,
    attempted: false,
    searches: [],
    hits: 0,
    candidates: [],
    rawExtracts: [],
    stopReasons: [],
    estimatedUsd: 0,
    mocked: false,
  };

  if (authorizeSearchIndexDiscovery !== true) {
    return {
      ...base,
      skipped: true,
      reason: "search_index_discovery_not_authorized",
      stopReasons: ["search_index_discovery_not_authorized"],
    };
  }

  let effectiveSearchFn = searchFn;
  let live = false;
  if (typeof effectiveSearchFn !== "function") {
    if (authorizeLiveMeteredSearch === true && typeof liveSearchFn === "function") {
      effectiveSearchFn = liveSearchFn;
      live = true;
    } else {
      return {
        ...base,
        skipped: true,
        reason: "missing_search_provider_authorization",
        stopReasons: ["missing_search_provider_authorization"],
      };
    }
  }

  const maxQueries = Number(limits.maxSearchQueries ?? 6);
  const maxCandidates = Number(limits.maxCandidates ?? 10);
  const ceiling = Number(budgetCeilingUsd);
  if (!(ceiling > 0) && live) {
    return {
      ...base,
      skipped: true,
      reason: "budget_ceiling_required_for_live_search",
      stopReasons: ["budget_exhaustion_or_zero_ceiling"],
    };
  }
  // Offline mocks may use ceiling 0 with zero-cost responses; still allow when mocked.
  const allowZeroBudgetMock = !live;

  const questions = buildHotelSegmentSearchQuestions(hotel, segmentConfig).slice(0, maxQueries);
  const rawExtracts = [];
  let spent = Number(workingLedger.estimatedUsd || 0);

  for (const strategy of questions) {
    if (!allowZeroBudgetMock && spent >= ceiling - 1e-9) {
      base.stopReasons.push("budget_exhaustion");
      break;
    }
    if (live && spent >= ceiling - 1e-9) {
      base.stopReasons.push("budget_exhaustion");
      break;
    }

    base.attempted = true;
    let result;
    try {
      result = await effectiveSearchFn(strategy.researchQuestion, {
        strategy,
        hotel,
        segmentConfig,
        ledger: workingLedger,
      });
    } catch (err) {
      result = {
        status: "error",
        hits: [],
        costUsd: 0,
        error: String(err?.message || err),
      };
      base.stopReasons.push("provider_failure");
    }

    const costUsd = Number(result?.costUsd || 0);
    if (live && costUsd > 0 && spent + costUsd > ceiling + 1e-9) {
      base.stopReasons.push("budget_exhaustion");
      base.searches.push({
        id: strategy.id,
        researchQuestion: strategy.researchQuestion,
        status: "budget_blocked",
        costUsd: 0,
        hitCount: 0,
      });
      break;
    }

    spent += costUsd;
    workingLedger.estimatedUsd = spent;
    workingLedger.externalRetrievals = Number(workingLedger.externalRetrievals || 0) + 1;
    workingLedger.searchQueries.push({
      id: strategy.id,
      query: strategy.researchQuestion,
      status: result?.status || "unknown",
      costUsd,
      mocked: result?.mocked === true || !live,
    });

    const hits = Array.isArray(result?.hits) ? result.hits : [];
    base.hits += hits.length;
    base.mocked = base.mocked || result?.mocked === true || !live;
    base.searches.push({
      id: strategy.id,
      researchQuestion: strategy.researchQuestion,
      status: result?.status || "unknown",
      costUsd,
      hitCount: hits.length,
      error: result?.error || null,
    });

    if (result?.status === "error" || result?.status === "credential_absent") {
      base.stopReasons.push(
        result.status === "credential_absent"
          ? "missing_provider_authorization"
          : "provider_failure",
      );
      continue;
    }

    for (const hit of hits) {
      const url = hit.url || hit.sourceUrl || null;
      if (!url) continue;
      workingLedger.attemptedUrls.push(url);
      const pageText = hitToPageText(hit);
      if (pageText.length < 40) continue;
      const extracted = extractIndependentCandidatesFromPage({
        pageText,
        url,
        hotel,
        segmentConfig,
      });
      for (const row of extracted) {
        rawExtracts.push({
          ...row,
          discoveryOrigin: DISCOVERY_ORIGIN.INDEPENDENT_PUBLIC,
          discoveryChannel: "search_index",
          searchStrategyId: strategy.id,
          searchQuery: strategy.researchQuestion,
          syntheticFixture: false,
          cachedEvidence: false,
          seededCandidate: false,
          mockedSearch: result?.mocked === true || !live,
        });
      }
    }
  }

  // Identity + provenance for each extract
  const withIds = rawExtracts.map((row) => {
    const invId = investigationIdForDiscovery({
      hotelId: hotel.hotelId,
      segment: segmentConfig.segmentId,
      org: row.organizationName,
      activity: row.activityHeadline,
      sourceUrl: row.sourceUrl || row.sources?.[0]?.url,
    });
    return {
      ...row,
      investigationId: invId,
      identityKey: candidateIdentityKey(row),
      researchSegment: segmentConfig.segmentId,
      discoveryOrigin: DISCOVERY_ORIGIN.INDEPENDENT_PUBLIC,
    };
  });

  const deduped = dedupeDiscoveryCandidates(withIds);
  const candidates = (deduped.candidates || withIds)
    .filter((c) => c.discoveryOrigin === DISCOVERY_ORIGIN.INDEPENDENT_PUBLIC)
    .slice(0, maxCandidates);

  base.rawExtracts = rawExtracts;
  base.candidates = candidates;
  base.estimatedUsd = spent;
  base.stopReasons = [...new Set(base.stopReasons)];
  base.ledger = workingLedger;
  base.liveMetered = live;
  base.independentFreshCount = candidates.length;
  base.questionCount = questions.length;
  return base;
}
