/**
 * GDI 2.0 Phase 3B / 3B-R — shared independent demand discovery engine.
 *
 * Finds previously unknown organizations / projects / meeting activities from
 * permitted public sources, then hands candidates into FullE2E investigation.
 *
 * Distinguishes:
 *   SOURCE discovery → CANDIDATE discovery → CANDIDATE validation → commercial qualification
 *
 * Fixtures, cached opportunities, and seeded candidate lists are NEVER labeled
 * as independent discovery.
 *
 * Default path: no-fee direct_public / official_page.
 * Optional Phase 3B-R: search-index discovery via injected `searchFn` (mocked Exa)
 * when `authorizeSearchIndexDiscovery === true`. Live metered Exa is never the
 * default and requires explicit `authorizeLiveMeteredSearch` + liveSearchFn.
 */

import { createHash } from "node:crypto";
import { fetchOfficialPage } from "../official-page-fetch.js";
import { assertPhase2ePublicProviderAuthorization } from "../adaptive-public-research-controller-v1.js";
import { assessRetrievedPageQuality } from "../source-discovery-v1.js";
import { candidateIdentityKey, dedupeDiscoveryCandidates } from "../candidate-dedupe.js";
import { PUBLIC_FETCH_COST_USD } from "../workflow/gdi-evidence-acquisition-v2.js";
import {
  buildHotelSegmentSearchQuestions,
  runSearchIndexDiscoveryPass,
} from "./search-index-discovery-provider-v1.js";

export const INDEPENDENT_DEMAND_DISCOVERY_V1 = "gdi-independent-demand-discovery-v1";

export const DISCOVERY_ORIGIN = Object.freeze({
  INDEPENDENT_PUBLIC: "INDEPENDENT_PUBLIC",
  CACHED_INVENTORY: "CACHED_INVENTORY",
  SYNTHETIC_FIXTURE: "SYNTHETIC_FIXTURE",
  SEEDED_CANDIDATE_LIST: "SEEDED_CANDIDATE_LIST",
});

export const DISCOVERY_STAGE = Object.freeze({
  SOURCE_DISCOVERY: "SOURCE_DISCOVERY",
  CANDIDATE_DISCOVERY: "CANDIDATE_DISCOVERY",
  CANDIDATE_VALIDATION: "CANDIDATE_VALIDATION",
  HANDOFF_TO_FULL_E2E: "HANDOFF_TO_FULL_E2E",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function hashKey(s) {
  return createHash("sha256").update(String(s || "")).digest("hex").slice(0, 16);
}

export function investigationIdForDiscovery({ hotelId, segment, org, activity, sourceUrl }) {
  return `ind_${hashKey([hotelId, segment, org, activity, sourceUrl].join("|"))}`;
}

/**
 * Segment-configurable public source catalog (no search API).
 * Builds concrete URLs from hotel geography — not a strategy list alone.
 */
export function buildIndependentDiscoverySourcePlan(hotel = {}, segmentConfig = {}) {
  const city = text(hotel.city || hotel.demandTerritory?.label?.split("/")?.[0]);
  const citySlug = city
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
  const country = text(hotel.country || "");
  const segmentId = segmentConfig.segmentId || "generic";
  const categoryHints = segmentConfig.sourceCategoryHints || [];

  const sources = [];
  const push = (row) => {
    if (!row?.url || !/^https?:\/\//i.test(row.url)) return;
    sources.push({
      ...row,
      city: city || null,
      segmentId,
      discoveryStage: DISCOVERY_STAGE.SOURCE_DISCOVERY,
    });
  };

  // Geography-bound public pages (direct_public only)
  if (citySlug) {
    for (const hint of categoryHints) {
      if (hint === "municipal_business_news" && citySlug) {
        push({
          id: `muni_news_${citySlug}`,
          category: "municipal_business_news",
          url: "https://invalid.local/blocked-web-search-placeholder",
          note: "Web-search discovery would improve municipal news recall but is not authorized as no-fee direct_public — not fetched.",
          fetchAuthorized: false,
          blockReason: "general_web_search_not_authorized_as_no_fee_direct_public",
          queryWouldHaveBeen: `${city} corporate project groundbreaking OR expansion OR mobilization 2025 OR 2026`,
        });
      }
      if (hint === "construction_trade_press") {
        push({
          id: "construction_dive_home",
          category: "construction_trade_press",
          url: "https://www.constructiondive.com/",
          fetchAuthorized: true,
          note: "Trade press home — extract market-relevant project headlines if present",
        });
      }
      if (hint === "usaspending_awards" && /united states|usa|us\b/i.test(country || "United States")) {
        push({
          id: "usaspending_search",
          category: "government_contract_awards",
          url: "https://www.usaspending.gov/",
          fetchAuthorized: true,
          note: "Federal spending portal entry — may not yield city-scoped awards without search API",
        });
      }
      if (hint === "corporate_ir_newsrooms") {
        // Known public IR/newsroom roots — not hotel-specific seeds of opportunities
        push({
          id: "businesswire_home",
          category: "press_wire",
          url: "https://www.businesswire.com/",
          fetchAuthorized: true,
          note: "Press wire home — independent only if city/project language extracted",
        });
      }
      if (hint === "chamber_events" && citySlug) {
        push({
          id: `chamber_${citySlug}`,
          category: "chamber_events",
          url: `https://www.${citySlug.replace(/-/g, "")}chamber.com/events`,
          fetchAuthorized: true,
          note: "Heuristic chamber URL — often 404; recorded as source attempt",
        });
      }
      if (hint === "meetings_industry_press") {
        push({
          id: "meetings_today_home",
          category: "meetings_industry_press",
          url: "https://www.meetingstoday.com/",
          fetchAuthorized: true,
        });
      }
      if (hint === "training_industry_press") {
        push({
          id: "training_industry_home",
          category: "training_industry_press",
          url: "https://trainingindustry.com/",
          fetchAuthorized: true,
        });
      }
    }
  }

  // Territory keyword pages (official tourism / economic development often public)
  const territory = (hotel.demandTerritory?.includes || []).slice(0, 3);
  for (const t of territory) {
    const slug = text(t)
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .slice(0, 40);
    if (!slug) continue;
    push({
      id: `territory_hint_${slug}`,
      category: "territory_public_hint",
      url: `https://en.wikipedia.org/wiki/${encodeURIComponent(text(t).split(/\s+where\b/)[0].trim())}`,
      fetchAuthorized: true,
      note: "Geography context only — not commercial discovery by itself",
    });
  }

  return {
    version: INDEPENDENT_DEMAND_DISCOVERY_V1,
    hotelId: hotel.hotelId || null,
    city: city || null,
    country: country || null,
    segmentId,
    sources,
    authorizedFetchCount: sources.filter((s) => s.fetchAuthorized).length,
    blockedSearchPlaceholders: sources.filter((s) => s.fetchAuthorized === false).length,
    note: "A discovery plan must produce concrete URLs. Strategy lists alone are insufficient. Google/Serp/Exa search not used (no-fee / not authorized).",
  };
}

/**
 * Extract candidate activity rows from page text (deterministic — no LLM).
 * Does not invent room nights or RFPs.
 */
export function extractIndependentCandidatesFromPage({
  pageText = "",
  url = "",
  hotel = {},
  segmentConfig = {},
} = {}) {
  const blob = text(pageText).slice(0, 20000);
  const city = text(hotel.city || "");
  const patterns = segmentConfig.extractionPatterns || [];
  const out = [];

  for (const pat of patterns) {
    const re = pat.re instanceof RegExp ? pat.re : null;
    if (!re) continue;
    re.lastIndex = 0;
    let m;
    let guard = 0;
    while ((m = re.exec(blob)) && guard < 20) {
      guard += 1;
      let organizationName = text(m.groups?.org || m[1] || "");
      // Drop trailing lowercase glue words accidentally swallowed by extract windows
      organizationName = organizationName
        .replace(/\s+\b(announced|will|host|hosts|hosted|launches|launched|begins|began|was|were|is|are)\b.*$/i, "")
        .trim();
      const activityHeadline = text(m.groups?.activity || m[2] || m[0] || "").slice(0, 180);
      if (!organizationName || organizationName.length < 3) continue;
      if (/^(the|a|an|this|our|new)$/i.test(organizationName)) continue;

      const cityHit = !city || new RegExp(city.split(/\s+/)[0], "i").test(blob);
      const excerpt = blob.slice(Math.max(0, m.index - 80), m.index + 220);

      out.push({
        organizationName,
        activityHeadline: activityHeadline || organizationName,
        title: activityHeadline || organizationName,
        name: organizationName,
        eventName: activityHeadline || organizationName,
        cmtDemandCategory: pat.categoryId || null,
        cpeDemandCategory: pat.categoryId || null,
        evidenceQuote: excerpt.slice(0, 280),
        sources: [{ url, title: pat.label || "independent_public_extract" }],
        sourceUrl: url,
        location: city || null,
        city: city || null,
        discoveryOrigin: DISCOVERY_ORIGIN.INDEPENDENT_PUBLIC,
        syntheticFixture: false,
        cachedEvidence: false,
        seededCandidate: false,
        marketRelevant: cityHit,
        extractionPatternId: pat.id,
        hotelDemandThesis: pat.defaultThesis || null,
        proposedHotelUse: pat.defaultHotelUse || null,
      });
    }
  }

  return out;
}

function validateCandidate(candidate = {}, hotel = {}, segmentConfig = {}) {
  const reasons = [];
  const org = text(candidate.organizationName);
  if (!org || org.length < 3) reasons.push("missing_organization");
  if (!text(candidate.activityHeadline)) reasons.push("missing_activity");
  if (!candidate.sources?.length && !candidate.sourceUrl) reasons.push("missing_provenance");
  if (candidate.discoveryOrigin !== DISCOVERY_ORIGIN.INDEPENDENT_PUBLIC) {
    reasons.push("not_independent_origin");
  }
  if (candidate.syntheticFixture || candidate.cachedEvidence || candidate.seededCandidate) {
    reasons.push("fixture_or_cached_or_seeded_not_independent");
  }

  // Contract/project alone ≠ lodging demand — flag for FullE2E demand investigation
  const alone = segmentConfig.insufficientAlonePatterns || [];
  const blob = `${candidate.activityHeadline} ${candidate.evidenceQuote}`;
  let insufficientAlone = false;
  for (const re of alone) {
    if (re.test(blob) && !(segmentConfig.demandStrengtheningPatterns || []).some((r) => r.test(blob))) {
      insufficientAlone = true;
      reasons.push("activity_alone_does_not_prove_accommodation_demand");
      break;
    }
  }

  const city = text(hotel.city || "");
  const geoOk =
    !city ||
    new RegExp(city.split(/\s+/)[0], "i").test(
      `${candidate.location || ""} ${candidate.city || ""} ${candidate.evidenceQuote || ""} ${candidate.activityHeadline || ""}`,
    );

  if (!geoOk) reasons.push("weak_or_missing_geographic_relevance");

  const ok =
    reasons.filter((r) =>
      /missing_organization|missing_activity|missing_provenance|fixture_or_cached|not_independent/.test(r),
    ).length === 0;

  return {
    ok,
    reasons: [...new Set(reasons)],
    insufficientAlone,
    geoOk,
    identityKey: candidateIdentityKey(candidate),
  };
}

/**
 * Run independent discovery for one hotel + segment config.
 *
 * Options (Phase 3B-R):
 *   authorizeSearchIndexDiscovery + searchFn → mocked/injected search-index path
 *   skipDirectPublicFetches → offline-only (tests); still records source plan
 *   authorizeLiveMeteredSearch + liveSearchFn → live Exa (NOT used in 3B-R)
 */
export async function runIndependentDemandDiscovery({
  hotel = {},
  segmentConfig = {},
  ledger = null,
  limits = {},
  searchFn = null,
  authorizeSearchIndexDiscovery = false,
  authorizeLiveMeteredSearch = false,
  liveSearchFn = null,
  budgetCeilingUsd = 0,
  skipDirectPublicFetches = false,
} = {}) {
  const auth = assertPhase2ePublicProviderAuthorization();
  const maxRetrievals = Number(limits.maxExternalRetrievals ?? 20);
  const maxCandidates = Number(limits.maxCandidates ?? 10);
  const workingLedger = ledger || {
    externalRetrievals: 0,
    estimatedUsd: 0,
    attemptedUrls: [],
    requests: [],
    searchQueries: [],
  };

  const stagesCompleted = [];
  const stopReasons = [];
  const sourcePlan = buildIndependentDiscoverySourcePlan(hotel, segmentConfig);
  const searchQuestions = buildHotelSegmentSearchQuestions(hotel, segmentConfig);
  stagesCompleted.push(DISCOVERY_STAGE.SOURCE_DISCOVERY);

  // Search-index path may run even when Phase 2E public auth is present; it does not
  // substitute for Phase 2E denial of live Exa — live requires separate authorizeLiveMeteredSearch.
  let searchIndexPass = null;
  if (authorizeSearchIndexDiscovery === true) {
    // Attach blocked-placeholder queries as extra search questions
    const extra = (sourcePlan.sources || [])
      .filter((s) => s.fetchAuthorized === false && s.queryWouldHaveBeen)
      .map((s, i) => ({
        id: `placeholder_${s.id || i}`,
        researchQuestion: s.queryWouldHaveBeen,
        category: s.category,
      }));
    const segmentWithExtras = {
      ...segmentConfig,
      extraSearchQuestions: [...(segmentConfig.extraSearchQuestions || []), ...extra],
    };
    searchIndexPass = await runSearchIndexDiscoveryPass({
      hotel,
      segmentConfig: segmentWithExtras,
      searchFn,
      authorizeSearchIndexDiscovery: true,
      authorizeLiveMeteredSearch,
      liveSearchFn,
      budgetCeilingUsd,
      ledger: workingLedger,
      limits: {
        maxSearchQueries: limits.maxSearchQueries ?? Math.min(6, maxRetrievals),
        maxCandidates,
      },
    });
    if (searchIndexPass?.stopReasons?.length) {
      stopReasons.push(...searchIndexPass.stopReasons);
    }
  }

  if (!auth.ok && !(authorizeSearchIndexDiscovery === true && searchIndexPass?.attempted)) {
    return {
      version: INDEPENDENT_DEMAND_DISCOVERY_V1,
      ok: false,
      independentDiscoveryPossible: false,
      limitation:
        "No-fee official_page / direct_public authorization failed — independent discovery cannot execute without search-index authorization + searchFn either.",
      sourcePlan,
      searchQuestions,
      searchIndexPass,
      auth,
      stagesCompleted,
      stopReasons: [...new Set(["provider_authorization_failed", ...stopReasons])],
      candidates: [],
      retrievalsUsed: workingLedger.externalRetrievals || 0,
      estimatedUsd: workingLedger.estimatedUsd || 0,
    };
  }

  const rawExtracts = [];
  const sourceResults = [];

  if (searchIndexPass?.candidates?.length) {
    for (const row of searchIndexPass.rawExtracts || searchIndexPass.candidates) {
      rawExtracts.push(row);
    }
  }

  if (!skipDirectPublicFetches && auth.ok) {
    for (const src of sourcePlan.sources) {
      if (workingLedger.externalRetrievals >= maxRetrievals) {
        stopReasons.push("retrieval_ceiling");
        break;
      }
      if (!src.fetchAuthorized) {
        sourceResults.push({
          ...src,
          fetched: false,
          skipped: true,
          reason:
            authorizeSearchIndexDiscovery && searchIndexPass?.attempted
              ? "deferred_to_search_index_pass"
              : src.blockReason || "fetch_not_authorized",
        });
        continue;
      }

      const page = await fetchOfficialPage(src.url, {
        allowRender: false,
        timeoutMs: 20000,
      });
      workingLedger.externalRetrievals += 1;
      workingLedger.attemptedUrls.push(src.url);
      workingLedger.requests.push({
        url: src.url,
        ok: page?.ok === true,
        provider: "direct_public",
        costUsd: PUBLIC_FETCH_COST_USD,
        category: src.category,
      });
      workingLedger.estimatedUsd += PUBLIC_FETCH_COST_USD;

      const quality = assessRetrievedPageQuality(page, src.url);
      const body = text(page?.text || page?.body || "");
      sourceResults.push({
        id: src.id,
        url: src.url,
        category: src.category,
        fetched: true,
        ok: page?.ok === true && quality.usable !== false,
        soft404: quality.soft404 === true,
        bodyChars: body.length,
        sourceType: quality.sourceType,
      });

      if (!page?.ok || quality.soft404 || body.length < 120) continue;

      const extracted = extractIndependentCandidatesFromPage({
        pageText: body,
        url: src.url,
        hotel,
        segmentConfig,
      });
      for (const row of extracted) {
        rawExtracts.push(row);
      }
    }
  } else if (skipDirectPublicFetches) {
    stopReasons.push("direct_public_fetches_skipped_offline");
    for (const src of sourcePlan.sources) {
      sourceResults.push({
        ...src,
        fetched: false,
        skipped: true,
        reason: "skip_direct_public_fetches",
      });
    }
  }
  stagesCompleted.push(DISCOVERY_STAGE.CANDIDATE_DISCOVERY);

  const validated = [];
  for (const row of rawExtracts) {
    const v = validateCandidate(row, hotel, segmentConfig);
    const invId = investigationIdForDiscovery({
      hotelId: hotel.hotelId,
      segment: segmentConfig.segmentId,
      org: row.organizationName,
      activity: row.activityHeadline,
      sourceUrl: row.sourceUrl,
    });
    validated.push({
      ...row,
      investigationId: invId,
      validation: v,
      researchSegment: segmentConfig.segmentId,
      discoveryOrigin: DISCOVERY_ORIGIN.INDEPENDENT_PUBLIC,
    });
  }
  stagesCompleted.push(DISCOVERY_STAGE.CANDIDATE_VALIDATION);

  const accepted = validated.filter((c) => c.validation?.ok);
  const deduped = dedupeDiscoveryCandidates(accepted);
  const candidates = (deduped.candidates || accepted)
    .filter((c) => c.discoveryOrigin === DISCOVERY_ORIGIN.INDEPENDENT_PUBLIC)
    .slice(0, maxCandidates)
    .map((c) => ({
      ...c,
      syntheticFixture: false,
      cachedEvidence: false,
      seededCandidate: false,
      discoveryOrigin: DISCOVERY_ORIGIN.INDEPENDENT_PUBLIC,
      note: "Independent public extract — accommodation demand still requires FullE2E demand investigation.",
    }));

  stagesCompleted.push(DISCOVERY_STAGE.HANDOFF_TO_FULL_E2E);

  const searchIndexYield = Number(searchIndexPass?.independentFreshCount || 0);
  const independentDiscoveryPossible =
    (sourcePlan.authorizedFetchCount > 0 && auth.ok === true) ||
    (authorizeSearchIndexDiscovery === true && typeof searchFn === "function") ||
    searchIndexYield > 0;
  const limitationNotes = [];
  if (candidates.length === 0) {
    if (authorizeSearchIndexDiscovery !== true) {
      limitationNotes.push(
        "Zero independent candidates extracted from authorized no-fee public pages. Enable authorizeSearchIndexDiscovery + searchFn (mocked) or live Exa after founder auth to expand recall.",
      );
    } else if (!searchIndexPass?.attempted) {
      limitationNotes.push(
        "Search-index discovery authorized but not attempted — check searchFn / provider authorization.",
      );
    } else {
      limitationNotes.push(
        "Search-index pass executed but yielded zero extractable independent candidates after validation/dedupe.",
      );
    }
  }
  if (sourcePlan.blockedSearchPlaceholders > 0 && authorizeSearchIndexDiscovery !== true) {
    limitationNotes.push(
      "General web-search URL placeholders were recorded but not fetched (not authorized as no-fee direct_public).",
    );
  }

  return {
    version: INDEPENDENT_DEMAND_DISCOVERY_V1,
    ok: true,
    independentDiscoveryPossible,
    independentFreshCount: candidates.length,
    hotelId: hotel.hotelId || null,
    segmentId: segmentConfig.segmentId,
    sourcePlan: {
      sourceCount: sourcePlan.sources.length,
      authorizedFetchCount: sourcePlan.authorizedFetchCount,
      blockedSearchPlaceholders: sourcePlan.blockedSearchPlaceholders,
    },
    searchQuestions: searchQuestions.map((q) => ({
      id: q.id,
      researchQuestion: q.researchQuestion,
      category: q.category,
    })),
    searchIndexPass: searchIndexPass
      ? {
          attempted: searchIndexPass.attempted === true,
          skipped: searchIndexPass.skipped === true,
          reason: searchIndexPass.reason || null,
          hits: searchIndexPass.hits || 0,
          independentFreshCount: searchIndexPass.independentFreshCount || 0,
          searches: searchIndexPass.searches || [],
          stopReasons: searchIndexPass.stopReasons || [],
          mocked: searchIndexPass.mocked === true,
          liveMetered: searchIndexPass.liveMetered === true,
          estimatedUsd: searchIndexPass.estimatedUsd || 0,
        }
      : null,
    sourceResults,
    stagesCompleted,
    stopReasons: [...new Set(stopReasons)],
    limitationNotes,
    rawExtractCount: rawExtracts.length,
    validatedCount: validated.length,
    acceptedBeforeDedupe: accepted.length,
    rejectedValidation: validated.filter((c) => !c.validation?.ok).length,
    candidates,
    retrievalsUsed: workingLedger.externalRetrievals,
    estimatedUsd: workingLedger.estimatedUsd,
    ledger: workingLedger,
    auth,
    doesNotCountFixturesOrCachedAsIndependent: true,
  };
}

/**
 * Label and strip false “fresh discovery” claims from mixed candidate bags.
 */
export function partitionDiscoveryOrigins(candidates = []) {
  const buckets = {
    independentPublic: [],
    cachedInventory: [],
    syntheticFixture: [],
    seededCandidateList: [],
    unknown: [],
  };
  for (const c of candidates) {
    if (c.syntheticFixture === true || c.discoveryOrigin === DISCOVERY_ORIGIN.SYNTHETIC_FIXTURE) {
      buckets.syntheticFixture.push(c);
    } else if (c.cachedEvidence === true || c.discoveryOrigin === DISCOVERY_ORIGIN.CACHED_INVENTORY) {
      buckets.cachedInventory.push(c);
    } else if (c.seededCandidate === true || c.discoveryOrigin === DISCOVERY_ORIGIN.SEEDED_CANDIDATE_LIST) {
      buckets.seededCandidateList.push(c);
    } else if (c.discoveryOrigin === DISCOVERY_ORIGIN.INDEPENDENT_PUBLIC) {
      buckets.independentPublic.push(c);
    } else {
      buckets.unknown.push(c);
    }
  }
  return buckets;
}
