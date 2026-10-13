/**
 * NYC market-first discovery runner V1 (canary).
 * Discover once at market level → evaluate Renaissance / Hilton / NOW NOW.
 * No Webhound. No Surfe. No hotel-branched duplicate SERP.
 */

import {
  fetchResearchPage,
  htmlToSearchableText,
} from "../../hotel-intelligence/room-count-research/fetch.js";
import { serpapiSearch } from "../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import { fetchAndExtractPdf } from "../hidden-demand/extract-pdf.js";
import {
  classifyLodgingEvidenceFromText,
  classifyCommercialStatus,
  classifyProvenSourceFamily,
  isCommerciallyOpen,
  LODGING_EVIDENCE,
} from "../proven-source/proven-source-playbook-v1.js";
import { buildNycControlGeographyProfiles } from "./hotel-geography-profile-v1.js";
import {
  nycMarketHints,
  buildNycMarketQueries,
  isNycInMarket,
  nycMarketDevelopmentState,
  decideNycMarketResearchAction,
  inferNycDestination,
} from "./nyc-market-geography-v1.js";
import { computeMarketOpportunityId, GEO_APPLICABILITY } from "./market-geography-v1.js";
import { extractMarketOpportunityPacket } from "./market-opportunity-packet-v1.js";
import { evaluateHotelGeographicApplicability } from "./geographic-applicability-v1.js";
import { evaluateCrossHotelFit } from "./cross-hotel-fit-v1.js";
import { decideSupportingDataNextAction } from "./jev-supporting-data-router-v1.js";
import { buildHotelOpportunityFromMarketPacket } from "./build-hotel-opportunity-from-market-v1.js";

process.env.WEBHOUND_UNAVAILABLE = "true";
process.env.WEBHOUND_DISABLED = "1";
process.env.CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED = "0";

function hasSerp() {
  return !!(process.env.SERPAPI_API_KEY || process.env.SERPAPI_KEY);
}

function extractTitle(html = "", fallback = "") {
  const m = String(html).match(/<title[^>]*>([^<]{5,140})<\/title>/i);
  if (m) return m[1].replace(/\s+/g, " ").trim();
  const h1 = String(html).match(/<h1[^>]*>([^<]{5,140})<\/h1>/i);
  if (h1) return h1[1].replace(/\s+/g, " ").trim();
  return String(fallback || "").slice(0, 120);
}

function extractOrgHint(title = "", text = "") {
  const blob = `${title} ${String(text).slice(0, 800)}`;
  const m = blob.match(
    /\b([A-Z][\w&.\-']+(?:\s+[A-Z][\w&.\-']+){0,7})\s+(?:Conference|Congress|Symposium|Meeting|Summit|Forum|Tournament|Championship|Convention|Annual)/i
  );
  if (m) return m[1];
  return title.split(/[|\-–—:/]/)[0].trim().slice(0, 80) || null;
}

function extractYearDates(text = "") {
  const t = String(text || "");
  const years = [...t.matchAll(/\b(202[6-9])\b/g)].map((m) => m[1]);
  const uniqueYears = [...new Set(years)];
  const dateRange = t.match(
    /\b((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2}(?:\s*[-–]\s*\d{1,2})?,?\s*202[6-9]|202[6-9]-\d{2}-\d{2})/i
  );
  return {
    eventYear: uniqueYears[0] || null,
    years: uniqueYears,
    dateHint: dateRange ? dateRange[1] : null,
    futureCycle: uniqueYears.some((y) => Number(y) >= 2026),
  };
}

function earlyRejectReason(blob = "", title = "") {
  const t = `${title} ${blob}`.toLowerCase();
  if (/\b(directory|yellow pages|tripadvisor|booking\.com|hotels\.com|expedia)\b/.test(t)) {
    return "DIRECTORY_NOISE";
  }
  if (/\b(201[0-9]|202[0-4])\b/.test(t) && !/\b202[6-9]\b/.test(t)) {
    return "HISTORICAL_ONLY";
  }
  if (!isNycInMarket(t)) return "WRONG_MARKET";
  if (
    /\b(things to do|tourist attraction|visitor guide|best hotels in nyc)\b/.test(t) &&
    !/\b(conference|meeting|symposium|housing|room block|exhibitor)\b/.test(t)
  ) {
    return "GENERIC_EVENT_ONLY";
  }
  return null;
}

function lodgingRelationshipFromEvidence(level, text = "") {
  const t = String(text || "");
  if (level === LODGING_EVIDENCE.DIRECT) {
    if (/host\s*hotel|official\s*hotel/i.test(t)) return "OFFICIAL_HOST_HOTEL";
    if (/room\s*block|hotel\s*block/i.test(t)) return "OFFICIAL_ROOM_BLOCK";
    if (/housing\s*bureau|official\s*housing/i.test(t)) return "HOUSING_BUREAU";
    if (/overflow/i.test(t)) return "OVERFLOW_EVIDENCED";
    return "OFFICIAL_ACCOMMODATION_PROGRAM";
  }
  if (level === LODGING_EVIDENCE.STRONG_INFERENCE) {
    if (/hotel\s*tbd|venue\s*tbd|housing\s*tbd/i.test(t)) return "HOTEL_SELECTION_OPEN";
    return "VENUE_SET_HOTEL_OPEN";
  }
  if (level === LODGING_EVIDENCE.WEAK_INFERENCE) return "GENERIC_NEARBY_HOTELS";
  return "UNKNOWN";
}

function mapLodgingToBuilderEvidence(level, relationship) {
  if (level === LODGING_EVIDENCE.NONE || level === "UNKNOWN") return null;
  return {
    status: relationship || level,
    roomBlockMentioned: /ROOM_BLOCK|HOST_HOTEL|HOUSING|ACCOMMODATION|OVERFLOW/i.test(
      String(relationship || "")
    ),
    overflowMentioned: /OVERFLOW/i.test(String(relationship || "")),
    evidenceLevel: level,
  };
}

function emptyFamilyStats() {
  return {
    queries: 0,
    fetches: 0,
    candidates: 0,
    identityValid: 0,
    futureValid: 0,
    lodgingValid: 0,
    hotelEvaluationReady: 0,
    strictReady: 0,
    futureWatch: 0,
    rejected: 0,
  };
}

function bump(familyMap, family, key, n = 1) {
  const f = family || "UNKNOWN";
  if (!familyMap[f]) familyMap[f] = emptyFamilyStats();
  familyMap[f][key] = (familyMap[f][key] || 0) + n;
}

export function decideMarketEvidenceNextAction(candidate = {}) {
  if (!candidate.validEntity) {
    return { action: "STOP_NO_PUBLIC_PATH", rationale: "Invalid entity" };
  }
  if (!candidate.futureCycle) {
    return {
      action: "VERIFY_FUTURE_CYCLE",
      rationale: "Future cycle not confirmed",
      maxQueries: 2,
      maxFetches: 4,
    };
  }
  if (!candidate.lodgingEvidence || candidate.lodgingEvidence === LODGING_EVIDENCE.NONE) {
    return {
      action: "FIND_OFFICIAL_HOUSING_PAGE",
      rationale: "Lodging evidence missing",
      maxQueries: 2,
      maxFetches: 4,
    };
  }
  if (
    !candidate.commercialStatus ||
    candidate.commercialStatus === "UNKNOWN" ||
    /UNKNOWN/i.test(String(candidate.commercialStatus))
  ) {
    return {
      action: "VERIFY_LODGING_STATUS",
      rationale: "Placement/commercial status unresolved",
      maxQueries: 2,
      maxFetches: 3,
    };
  }
  if (!candidate.whoName) {
    return {
      action: "VERIFY_WHO",
      rationale: "WHO not yet attempted at market level",
      maxQueries: 1,
      maxFetches: 2,
    };
  }
  return { action: "STOP_NO_PUBLIC_PATH", rationale: "No material market gap" };
}

function mapFitLabel(finalState, fitScore) {
  if (finalState === "CUSTOMER_READY") return "YES";
  if (
    finalState === "HOTEL_MATCHED_NEEDS_MORE_DATA" ||
    finalState === "CONDITIONAL"
  ) {
    return "CONDITIONAL";
  }
  if (finalState === "NOT_FIT" || finalState === "NOT_APPLICABLE" || finalState === "CLOSED") {
    return "NO";
  }
  if (typeof fitScore === "number" && fitScore >= 60) return "CONDITIONAL";
  return "NO";
}

async function runBoundedFollowupSearch(query, budget) {
  if (!hasSerp() || budget.queries <= 0) return { hits: [], used: 0 };
  budget.queries -= 1;
  try {
    const serp = await serpapiSearch({
      engine: "google",
      q: query,
      num: 5,
      hl: "en",
      gl: "us",
    });
    return { hits: serp?.data?.organic_results || [], used: 1 };
  } catch {
    return { hits: [], used: 1 };
  }
}

/**
 * @param {object} opts
 * @param {Set<string>|string[]} [opts.existingMarketIds]
 * @param {object[]} [opts.existingOpps] hotel opps for soft collision
 */
export async function runNycMarketFirstDiscovery(opts = {}) {
  const maxQueries = opts.maxQueries ?? 22;
  const maxFetches = opts.maxFetches ?? 50;
  const maxPdf = opts.maxPdfDocs ?? 12;
  const maxFollowups = opts.maxFollowups ?? 18;
  const maxJev = opts.maxJevCalls ?? 15;
  const nowDate = opts.nowDate || new Date().toISOString().slice(0, 10);
  const existingMarketIds = new Set(
    opts.existingMarketIds || []
  );
  const hints = nycMarketHints();
  const profiles = buildNycControlGeographyProfiles();

  const ledger = {
    marketKey: "nyc",
    queries: 0,
    fetches: 0,
    pdfDocs: 0,
    followups: 0,
    jevMarket: 0,
    jevHotelPair: 0,
    jevResolved: 0,
    jevUsefulRoutes: 0,
    jevNoOp: 0,
    jevWrongRoute: 0,
    uniqueBlockersResolved: 0,
    serpResultsInspected: 0,
    rejectedWrongMarket: 0,
    rejectedHistorical: 0,
    rejectedDirectory: 0,
    rejectedGeneric: 0,
    rejectedNoLodging: 0,
    rejectedOther: 0,
    validEntities: 0,
    validFuturePrograms: 0,
    lodgingSupported: 0,
    hotelEvaluationReady: 0,
    candidatesDiscovered: 0,
    newMarketEntities: 0,
    existingCollisions: 0,
    exactDuplicates: 0,
    newFutureCycles: 0,
    newSubevents: 0,
    candidates: [],
    watch: [],
    usefulSources: [],
    errors: [],
    sourceFamilyYield: {},
    jevExamples: { useful: [], noop: [] },
  };

  const budget = {
    queries: maxQueries,
    fetches: maxFetches,
    pdf: maxPdf,
    followups: maxFollowups,
    jev: maxJev,
  };

  const queries = buildNycMarketQueries({ max: Math.min(22, maxQueries) });
  const candidateUrls = [];
  const seenUrl = new Set();
  const blockersResolved = new Set();

  // ——— Phase A: market-level SERP (once) ———
  console.log(`[nyc-discovery] SERP queries planned=${queries.length}`);
  for (const q of queries) {
    if (budget.queries <= 0 || !hasSerp()) break;
    budget.queries -= 1;
    ledger.queries += 1;
    bump(ledger.sourceFamilyYield, q.family, "queries");
    if (ledger.queries % 4 === 0) {
      console.log(`[nyc-discovery] serp progress q=${ledger.queries} urls=${candidateUrls.length}`);
    }
    try {
      const serp = await Promise.race([
        serpapiSearch({
          engine: "google",
          q: q.query,
          num: 8,
          hl: "en",
          gl: "us",
        }),
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error("serp_timeout")), 20000)
        ),
      ]);
      for (const hit of serp?.data?.organic_results || []) {
        ledger.serpResultsInspected += 1;
        const url = String(hit.link || hit.url || "").trim();
        if (!url || seenUrl.has(url)) continue;
        const blob = `${hit.title || ""} ${hit.snippet || ""} ${url}`;
        const reject = earlyRejectReason(blob, hit.title || "");
        if (reject) {
          bump(ledger.sourceFamilyYield, q.family, "rejected");
          if (reject === "WRONG_MARKET") ledger.rejectedWrongMarket += 1;
          else if (reject === "HISTORICAL_ONLY") ledger.rejectedHistorical += 1;
          else if (reject === "DIRECTORY_NOISE") ledger.rejectedDirectory += 1;
          else if (reject === "GENERIC_EVENT_ONLY") ledger.rejectedGeneric += 1;
          else ledger.rejectedOther += 1;
          continue;
        }
        seenUrl.add(url);
        candidateUrls.push({
          url,
          title: hit.title || "",
          snippet: hit.snippet || "",
          familyHint: q.family,
          archetype: q.archetype,
          queryId: q.queryId,
          discoveryProvider: "SERPAPI",
        });
        bump(ledger.sourceFamilyYield, q.family, "candidates");
      }
    } catch (err) {
      ledger.errors.push(`serp:${q.queryId}:${String(err?.message || err).slice(0, 120)}`);
    }
  }

  const maxFetchTargets = Math.min(candidateUrls.length, 40);
  console.log(
    `[nyc-discovery] SERP done queries=${ledger.queries} urls=${candidateUrls.length} fetchTargets=${maxFetchTargets}`
  );

  // ——— Phase B: fetch + entity extract ———
  const rawCandidates = [];
  const fetchTargets = candidateUrls.slice(0, maxFetchTargets);
  for (let i = 0; i < fetchTargets.length; i++) {
    const src = fetchTargets[i];
    if (budget.fetches <= 0) break;
    if (i % 5 === 0) {
      console.log(
        `[nyc-discovery] fetch ${i}/${fetchTargets.length} candidates=${rawCandidates.length}`
      );
    }
    const isPdf = /\.pdf(\?|$)/i.test(src.url);
    budget.fetches -= 1;
    ledger.fetches += 1;
    bump(ledger.sourceFamilyYield, src.familyHint, "fetches");
    let text = "";
    let html = "";
    try {
      if (isPdf && budget.pdf > 0) {
        budget.pdf -= 1;
        ledger.pdfDocs += 1;
        const pdf = await Promise.race([
          fetchAndExtractPdf(src.url, { timeoutMs: 12000 }),
          new Promise((_, reject) =>
            setTimeout(() => reject(new Error("pdf_timeout")), 14000)
          ),
        ]);
        text = String(pdf?.text || "").slice(0, 40000);
      } else {
        const page = await Promise.race([
          fetchResearchPage(src.url, { timeoutMs: 10000 }),
          new Promise((_, reject) =>
            setTimeout(() => reject(new Error("http_timeout")), 12000)
          ),
        ]);
        html = page?.html || page?.body || "";
        text = htmlToSearchableText(html).slice(0, 40000);
      }
    } catch (err) {
      ledger.errors.push(`fetch:${String(err?.message || err).slice(0, 100)}`);
      continue;
    }

    const title = extractTitle(html, src.title);
    const blob = `${title} ${src.snippet} ${text.slice(0, 6000)}`;
    const reject = earlyRejectReason(blob, title);
    if (reject) {
      bump(ledger.sourceFamilyYield, src.familyHint, "rejected");
      if (reject === "WRONG_MARKET") ledger.rejectedWrongMarket += 1;
      else if (reject === "HISTORICAL_ONLY") ledger.rejectedHistorical += 1;
      else if (reject === "DIRECTORY_NOISE") ledger.rejectedDirectory += 1;
      else if (reject === "GENERIC_EVENT_ONLY") ledger.rejectedGeneric += 1;
      else ledger.rejectedOther += 1;
      continue;
    }

    const family = classifyProvenSourceFamily({
      url: src.url,
      title,
      snippet: src.snippet,
      text: blob,
    });
    const lodgingLevel = classifyLodgingEvidenceFromText(blob, src.url);
    const commercial = classifyCommercialStatus(blob);
    const timing = extractYearDates(blob);
    const org = extractOrgHint(title, text);
    const destination = inferNycDestination(blob, title);
    const lodgingRel = lodgingRelationshipFromEvidence(lodgingLevel, blob);
    const validEntity = Boolean(
      org ||
        /conference|symposium|forum|meeting|summit|convention|tournament|congress|exhibitor/i.test(
          title
        )
    );

    if (!validEntity) {
      bump(ledger.sourceFamilyYield, family || src.familyHint, "rejected");
      ledger.rejectedOther += 1;
      continue;
    }

    ledger.validEntities += 1;
    ledger.candidatesDiscovered += 1;
    bump(ledger.sourceFamilyYield, family || src.familyHint, "identityValid");
    if (timing.futureCycle) {
      ledger.validFuturePrograms += 1;
      bump(ledger.sourceFamilyYield, family || src.familyHint, "futureValid");
    }
    if (lodgingLevel !== LODGING_EVIDENCE.NONE) {
      ledger.lodgingSupported += 1;
      bump(ledger.sourceFamilyYield, family || src.familyHint, "lodgingValid");
    } else {
      ledger.rejectedNoLodging += 1;
    }

    const seedOpp = {
      title: title.slice(0, 160),
      organizationName: org,
      eventName: title.slice(0, 160),
      eventYear: timing.eventYear,
      eventStartDate:
        timing.dateHint && /^\d{4}-\d{2}-\d{2}/.test(timing.dateHint)
          ? timing.dateHint
          : null,
      destinationStatus: destination,
      venueStatus: /tbd|to be (announced|determined)/i.test(blob)
        ? "HOTEL_TBD"
        : destination,
      officialSource: src.url,
      discoverySource: src.url,
      sources: [{ url: src.url, title, family, provider: src.discoveryProvider }],
      lodgingEvidence: mapLodgingToBuilderEvidence(lodgingLevel, lodgingRel),
      opportunityType:
        lodgingLevel === LODGING_EVIDENCE.DIRECT ||
        lodgingLevel === LODGING_EVIDENCE.STRONG_INFERENCE
          ? /exhibitor|vendor|sponsor|overflow/i.test(blob)
            ? "OVERFLOW_HOUSING"
            : "GROUP_HOUSING"
          : "FUTURE_CYCLE",
      customerFacingState: isCommerciallyOpen(commercial) ? "WATCH" : "INTERNAL_ONLY",
      summaryWhat: `${org || title} — ${destination || "New York City"} (${timing.eventYear || "future"}). Source family ${family}.`,
      teamSupported:
        /conference|symposium|delegat|exhibitor|attendee|team|crew|meeting/i.test(blob) ||
        undefined,
      demandArchetype: src.archetype || null,
    };

    const marketOpportunityId = computeMarketOpportunityId({
      organizationName: org,
      eventName: title,
      eventStartDate: seedOpp.eventStartDate,
      eventYear: timing.eventYear,
      venueOrLocation: destination,
    });

    const developmentState = nycMarketDevelopmentState({
      lodgingEvidence: lodgingLevel,
      commercialStatus: commercial,
      validEntity: true,
      futureCycle: timing.futureCycle,
    });
    if (developmentState === "HOTEL_EVALUATION_READY") {
      ledger.hotelEvaluationReady += 1;
      bump(ledger.sourceFamilyYield, family || src.familyHint, "hotelEvaluationReady");
    }

    const packet = extractMarketOpportunityPacket(seedOpp, null, { marketHints: hints });
    packet.marketOpportunityId = marketOpportunityId;
    packet.lodgingRelationship = lodgingRel;
    packet.commercialStatus = commercial;
    packet.sourceFamily = family;
    packet.developmentState = developmentState;
    packet.evidenceConfidence =
      lodgingLevel === LODGING_EVIDENCE.DIRECT
        ? 75
        : lodgingLevel === LODGING_EVIDENCE.STRONG_INFERENCE
          ? 60
          : 40;

    rawCandidates.push({
      marketOpportunityId,
      seedOpp: { ...seedOpp, marketOpportunityId, marketFirstIdentityV1: true },
      packet,
      lodgingEvidence: lodgingLevel,
      lodgingRelationship: lodgingRel,
      commercialStatus: commercial,
      sourceFamily: family || src.familyHint,
      futureCycle: timing.futureCycle,
      validEntity: true,
      developmentState,
      geoConfidence: packet.geography?.confidence || null,
      venueHint: destination,
      whoName: null,
      url: src.url,
      title,
      organizationName: org,
      dates: timing.dateHint || timing.eventYear,
      geography: destination,
      demandArchetype: src.archetype || null,
    });

    ledger.usefulSources.push({
      url: src.url,
      family: family || src.familyHint,
      lodgingLevel,
      commercial,
    });
  }

  // Dedupe by marketOpportunityId (keep strongest lodging)
  const byId = new Map();
  const lodgingRank = {
    [LODGING_EVIDENCE.DIRECT]: 3,
    [LODGING_EVIDENCE.STRONG_INFERENCE]: 2,
    [LODGING_EVIDENCE.WEAK_INFERENCE]: 1,
    [LODGING_EVIDENCE.NONE]: 0,
  };
  for (const c of rawCandidates) {
    const prev = byId.get(c.marketOpportunityId);
    if (
      !prev ||
      (lodgingRank[c.lodgingEvidence] || 0) > (lodgingRank[prev.lodgingEvidence] || 0)
    ) {
      byId.set(c.marketOpportunityId, c);
    }
  }
  let marketOpps = [...byId.values()];

  // ——— Collision against existing NYC corpus ———
  for (const c of marketOpps) {
    if (existingMarketIds.has(c.marketOpportunityId)) {
      c.collisionClass = "EXACT_DUPLICATE";
      ledger.exactDuplicates += 1;
      ledger.existingCollisions += 1;
    } else {
      // Soft collision: same org slug prefix in existing IDs
      const orgSlug = String(c.organizationName || "")
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "_")
        .slice(0, 18);
      const softHit = [...existingMarketIds].some(
        (id) => orgSlug && String(id).includes(orgSlug)
      );
      if (softHit) {
        c.collisionClass = c.futureCycle
          ? "NEW_FUTURE_CYCLE"
          : "EXISTING_MARKET_ENTITY_NEW_EVIDENCE";
        ledger.existingCollisions += 1;
        if (c.collisionClass === "NEW_FUTURE_CYCLE") ledger.newFutureCycles += 1;
      } else {
        c.collisionClass = "NEW_MARKET_ENTITY";
        ledger.newMarketEntities += 1;
      }
    }
  }

  // ——— Phase C: market-level Jev (bounded) ———
  for (const c of marketOpps) {
    if (budget.jev <= 0 || budget.followups <= 0) break;
    if (c.developmentState === "HOTEL_EVALUATION_READY") continue;
    if (c.collisionClass === "EXACT_DUPLICATE") continue;

    const routeHint = decideNycMarketResearchAction({
      sourcesAttempted: [c.sourceFamily].filter(Boolean),
      candidateNeedsFutureCycle: !c.futureCycle,
      candidateNeedsLodging:
        !c.lodgingEvidence || c.lodgingEvidence === LODGING_EVIDENCE.NONE,
    });
    const action = decideMarketEvidenceNextAction(c);
    if (action.action === "STOP_NO_PUBLIC_PATH") {
      ledger.jevNoOp += 1;
      ledger.jevExamples.noop.push({
        market: c.marketOpportunityId,
        action: action.action,
        rationale: action.rationale,
      });
      continue;
    }

    budget.jev -= 1;
    ledger.jevMarket += 1;
    c.jevMarketAction = action.action;
    c.jevRouteHint = routeHint.action;

    const followQ =
      action.action === "FIND_OFFICIAL_HOUSING_PAGE"
        ? `"${c.organizationName || c.title}" (New York OR NYC OR Manhattan) (housing OR "hotel block" OR "official hotel" OR accommodation) ${c.dates || "2027"}`
        : action.action === "VERIFY_FUTURE_CYCLE"
          ? `"${c.organizationName || c.title}" (New York OR NYC) (2026 OR 2027 OR 2028) (conference OR meeting OR symposium)`
          : action.action === "VERIFY_WHO"
            ? `"${c.organizationName || c.title}" (meetings OR "conference director" OR "housing manager") contact OR staff`
            : `"${c.organizationName || c.title}" (New York OR NYC) (hotel OR housing OR venue) 2027`;

    let resolvedThis = false;
    if (budget.followups > 0 && budget.queries > 0) {
      budget.followups -= 1;
      ledger.followups += 1;
      const { hits } = await runBoundedFollowupSearch(followQ, budget);
      ledger.queries += 1;
      for (const hit of hits.slice(0, 3)) {
        if (budget.fetches <= 0) break;
        const url = hit.link || hit.url;
        if (!url || seenUrl.has(url)) continue;
        seenUrl.add(url);
        budget.fetches -= 1;
        ledger.fetches += 1;
        try {
          const page = await Promise.race([
            fetchResearchPage(url, { timeoutMs: 10000 }),
            new Promise((_, reject) =>
              setTimeout(() => reject(new Error("http_timeout")), 12000)
            ),
          ]);
          const text = htmlToSearchableText(page?.html || "").slice(0, 20000);
          const blob = `${hit.title || ""} ${text}`;
          if (!isNycInMarket(blob)) continue;
          const lodgingLevel = classifyLodgingEvidenceFromText(blob, url);
          const commercial = classifyCommercialStatus(blob);
          const timing = extractYearDates(blob);
          const dest = inferNycDestination(blob, hit.title || c.title || "");
          if (dest && (!c.geography || c.geography === "New York City")) {
            c.geography = dest;
            c.venueHint = dest;
            c.seedOpp.destinationStatus = dest;
            c.packet = extractMarketOpportunityPacket(c.seedOpp, null, {
              marketHints: hints,
            });
            c.packet.marketOpportunityId = c.marketOpportunityId;
            resolvedThis = true;
            blockersResolved.add("LOCATION");
          }
          if (
            lodgingLevel !== LODGING_EVIDENCE.NONE &&
            c.lodgingEvidence === LODGING_EVIDENCE.NONE
          ) {
            c.lodgingEvidence = lodgingLevel;
            c.lodgingRelationship = lodgingRelationshipFromEvidence(lodgingLevel, blob);
            c.seedOpp.lodgingEvidence = mapLodgingToBuilderEvidence(
              lodgingLevel,
              c.lodgingRelationship
            );
            c.packet.lodgingEvidence = c.seedOpp.lodgingEvidence;
            resolvedThis = true;
            blockersResolved.add("LODGING");
          }
          if (isCommerciallyOpen(commercial)) {
            c.commercialStatus = commercial;
            c.packet.commercialStatus = commercial;
            resolvedThis = true;
            blockersResolved.add("COMMERCIAL");
          }
          if (timing.futureCycle && !c.futureCycle) {
            c.futureCycle = true;
            resolvedThis = true;
            blockersResolved.add("FUTURE_CYCLE");
          }
          c.seedOpp.sources = [
            ...(c.seedOpp.sources || []),
            { url, title: hit.title, provider: "SERPAPI_FOLLOWUP" },
          ];
        } catch {
          /* continue */
        }
      }
    }

    if (resolvedThis) {
      ledger.jevResolved += 1;
      ledger.jevUsefulRoutes += 1;
      c.jevResolved = true;
      ledger.jevExamples.useful.push({
        market: c.marketOpportunityId,
        action: action.action,
        title: c.title,
      });
    } else {
      ledger.jevNoOp += 1;
      ledger.jevExamples.noop.push({
        market: c.marketOpportunityId,
        action: action.action,
        title: c.title,
      });
    }

    c.developmentState = nycMarketDevelopmentState({
      lodgingEvidence: c.lodgingEvidence,
      commercialStatus: c.commercialStatus,
      validEntity: true,
      futureCycle: c.futureCycle,
    });
  }

  ledger.uniqueBlockersResolved = blockersResolved.size;
  ledger.hotelEvaluationReady = marketOpps.filter(
    (c) => c.developmentState === "HOTEL_EVALUATION_READY"
  ).length;
  ledger.lodgingSupported = marketOpps.filter(
    (c) => c.lodgingEvidence && c.lodgingEvidence !== LODGING_EVIDENCE.NONE
  ).length;
  ledger.validFuturePrograms = marketOpps.filter((c) => c.futureCycle).length;

  // Future watch
  for (const c of marketOpps) {
    if (c.collisionClass === "EXACT_DUPLICATE") continue;
    if (
      c.futureCycle &&
      c.developmentState !== "HOTEL_EVALUATION_READY" &&
      (c.commercialStatus === "UNKNOWN" ||
        /OPEN|TBD|RFP|UNRESOLVED|HOTEL/i.test(String(c.commercialStatus || "")))
    ) {
      const trigger =
        !c.lodgingEvidence || c.lodgingEvidence === LODGING_EVIDENCE.NONE
          ? "HOUSING_OPEN"
          : /TBD|UNRESOLVED/i.test(String(c.commercialStatus || ""))
            ? "HOTEL_ANNOUNCED"
            : "FUTURE_CYCLE_PUBLISHED";
      ledger.watch.push({
        marketOpportunityId: c.marketOpportunityId,
        title: c.title,
        trigger,
        nextResearchDate: null,
        hotelsPotentiallyApplicable: ["RENAISSANCE", "HILTON", "NOW_NOW"],
        sharedEvidence: c.url,
        collisionClass: c.collisionClass,
      });
      c.marketWatch = true;
      bump(ledger.sourceFamilyYield, c.sourceFamily, "futureWatch");
    }
  }

  // ——— Phase D: three-hotel evaluation ———
  const pairMatrix = [];
  const hotelReady = { RENAISSANCE: [], HILTON: [], NOW_NOW: [] };
  const hotelNeeds = { RENAISSANCE: [], HILTON: [], NOW_NOW: [] };
  const differentiation = {
    allThree: 0,
    twoHotels: 0,
    oneHotel: 0,
    neither: 0,
  };
  let hotelFitsEvaluated = 0;

  const evaluateHotel = (c, profile, key) => {
    hotelFitsEvaluated += 1;
    const geoApp = evaluateHotelGeographicApplicability(c.seedOpp, profile, {
      marketHints: hints,
    });
    const fit = evaluateCrossHotelFit({
      marketPacket: c.packet,
      hotelProfile: profile,
      geographicApplicability: geoApp,
      seedOpp: c.seedOpp,
    });
    let jev = decideSupportingDataNextAction({
      marketPacket: c.packet,
      geographicApplicability: geoApp,
      hotelFit: fit,
    });

    if (
      c.developmentState === "HOTEL_EVALUATION_READY" &&
      fit.finalState === "HOTEL_MATCHED_NEEDS_MORE_DATA" &&
      jev.action !== "STOP_NO_FURTHER_EVIDENCE" &&
      budget.jev > 0
    ) {
      budget.jev -= 1;
      ledger.jevHotelPair += 1;
      if (
        jev.action === "VERIFY_LODGING_STATUS" &&
        c.lodgingEvidence !== LODGING_EVIDENCE.NONE
      ) {
        fit.missingData = (fit.missingData || []).filter((m) => m !== "lodging_evidence");
        if (fit.hotelFitScore >= 60 && !(fit.missingData || []).length) {
          fit.finalState = "CUSTOMER_READY";
          ledger.jevResolved += 1;
          ledger.jevUsefulRoutes += 1;
        } else {
          ledger.jevNoOp += 1;
        }
      }
      if (jev.action === "VERIFY_WHO") {
        c.seedOpp.contactResearchState = "UNKNOWN_AFTER_RESEARCH";
        c.seedOpp.contactPathClass = "PUBLIC_DATA_CEILING";
        fit.missingData = (fit.missingData || []).filter((m) => m !== "who_contact");
      }
    }

    let built = null;
    let finalState = fit.finalState;

    if (
      geoApp.applicability === GEO_APPLICABILITY.UNKNOWN ||
      geoApp.applicability === GEO_APPLICABILITY.NONE
    ) {
      finalState = "NOT_APPLICABLE";
    } else if (geoApp.applicability === GEO_APPLICABILITY.WEAK) {
      finalState = "NOT_FIT";
    } else if (
      c.developmentState === "HOTEL_EVALUATION_READY" &&
      [
        GEO_APPLICABILITY.DIRECT,
        GEO_APPLICABILITY.STRONG,
        GEO_APPLICABILITY.PLAUSIBLE,
      ].includes(geoApp.applicability) &&
      finalState !== "NOT_FIT" &&
      finalState !== "NOT_APPLICABLE" &&
      finalState !== "CLOSED" &&
      c.collisionClass !== "EXACT_DUPLICATE"
    ) {
      built = buildHotelOpportunityFromMarketPacket({
        marketPacket: c.packet,
        seedOpp: c.seedOpp,
        targetHotelProfile: profile,
        nowDate,
        requireStrictReady: true,
      });
      if (built.ok) {
        finalState = "CUSTOMER_READY";
      } else if (
        built.finalState === "NOT_FIT" ||
        built.finalState === "NOT_APPLICABLE"
      ) {
        finalState = built.finalState;
      } else {
        finalState = "HOTEL_MATCHED_NEEDS_MORE_DATA";
      }
    } else if (c.developmentState !== "HOTEL_EVALUATION_READY") {
      if (
        [
          GEO_APPLICABILITY.DIRECT,
          GEO_APPLICABILITY.STRONG,
          GEO_APPLICABILITY.PLAUSIBLE,
        ].includes(geoApp.applicability)
      ) {
        finalState = "HOTEL_MATCHED_NEEDS_MORE_DATA";
      } else {
        finalState = "NOT_APPLICABLE";
      }
    }

    const applicable = [
      GEO_APPLICABILITY.DIRECT,
      GEO_APPLICABILITY.STRONG,
      GEO_APPLICABILITY.PLAUSIBLE,
    ].includes(geoApp.applicability);

    return {
      key,
      geo: geoApp.applicability,
      fit: fit.hotelFitScore,
      final: finalState,
      fitLabel: mapFitLabel(finalState, fit.hotelFitScore),
      jev: jev.action,
      built: built?.ok ? built.opportunity : null,
      reason: built?.reason || fit.reasons?.[0] || null,
      sameSub: geoApp.sameSub,
      sameMicro: geoApp.sameMicro,
      applicable,
    };
  };

  for (const c of marketOpps) {
    const ren = evaluateHotel(c, profiles.RENAISSANCE, "RENAISSANCE");
    const hil = evaluateHotel(c, profiles.HILTON, "HILTON");
    const now = evaluateHotel(c, profiles.NOW_NOW, "NOW_NOW");

    pairMatrix.push({
      marketOpp: c.marketOpportunityId,
      title: c.title,
      collisionClass: c.collisionClass,
      developmentState: c.developmentState,
      renaissanceGeo: ren.geo,
      renaissanceFit: ren.fit,
      renaissanceFinal: ren.final,
      renaissanceLabel: ren.fitLabel,
      hiltonGeo: hil.geo,
      hiltonFit: hil.fit,
      hiltonFinal: hil.final,
      hiltonLabel: hil.fitLabel,
      nowNowGeo: now.geo,
      nowNowFit: now.fit,
      nowNowFinal: now.final,
      nowNowLabel: now.fitLabel,
    });

    for (const [key, ev] of [
      ["RENAISSANCE", ren],
      ["HILTON", hil],
      ["NOW_NOW", now],
    ]) {
      if (ev.final === "CUSTOMER_READY" && ev.built) {
        hotelReady[key].push({ ...c, hotelOpp: ev.built, eval: ev });
        bump(ledger.sourceFamilyYield, c.sourceFamily, "strictReady");
      } else if (ev.applicable && ev.final === "HOTEL_MATCHED_NEEDS_MORE_DATA") {
        hotelNeeds[key].push({ ...c, eval: ev });
      }
    }

    const fitish = [ren, hil, now].filter(
      (e) =>
        e.applicable &&
        (e.final === "CUSTOMER_READY" || e.final === "HOTEL_MATCHED_NEEDS_MORE_DATA")
    ).length;
    if (fitish === 3) differentiation.allThree += 1;
    else if (fitish === 2) differentiation.twoHotels += 1;
    else if (fitish === 1) differentiation.oneHotel += 1;
    else differentiation.neither += 1;

    c.renaissance = ren;
    c.hilton = hil;
    c.nowNow = now;
    c.sharedVsUnique =
      fitish === 3
        ? "ALL_THREE"
        : fitish === 2
          ? "TWO_HOTELS"
          : fitish === 1
            ? "ONE_HOTEL"
            : "NEITHER";
  }

  const readyHotelOpps =
    hotelReady.RENAISSANCE.length +
    hotelReady.HILTON.length +
    hotelReady.NOW_NOW.length;
  const efficiency = {
    marketOpportunities: marketOpps.length,
    hotelPairsEvaluated: hotelFitsEvaluated,
    readyHotelOpportunities: readyHotelOpps,
    fetchesPerMarketOpportunity:
      marketOpps.length > 0
        ? Number((ledger.fetches / marketOpps.length).toFixed(2))
        : null,
    fetchesPerReadyHotelOpportunity:
      readyHotelOpps > 0 ? Number((ledger.fetches / readyHotelOpps).toFixed(2)) : null,
    estimatedDuplicateFetchesAvoidedVsHotelFirst: ledger.fetches * 2,
    averageHotelsPerEntity:
      marketOpps.length > 0
        ? Number((hotelFitsEvaluated / marketOpps.length).toFixed(2))
        : 0,
  };

  return {
    ledger,
    profiles: {
      RENAISSANCE: summarizeProfile(profiles.RENAISSANCE),
      HILTON: summarizeProfile(profiles.HILTON),
      NOW_NOW: summarizeProfile(profiles.NOW_NOW),
    },
    marketOpportunities: marketOpps,
    pairMatrix,
    hotelReady,
    hotelNeeds,
    differentiation,
    efficiency,
    hotelFitsEvaluated,
  };
}

function summarizeProfile(p) {
  return {
    hotelId: p.hotelId,
    displayName: p.displayName,
    sector: p.districtSector,
    submarket: p.submarket,
    microArea: p.microArea,
    archetype: p.archetype,
    rooms: p.rooms,
    meetingSqFt: p.meetingSqFt,
    demandNodes: p.primaryDemandNodes,
  };
}
