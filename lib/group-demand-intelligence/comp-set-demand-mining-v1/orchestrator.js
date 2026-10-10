/**
 * Comp Set Demand Mining V1 orchestrator — bounded public-trace discovery.
 * Phone = discovery pivot only. Page validation required. Thresholds unchanged.
 */

import { serpapiSearch } from "../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import {
  COMP_SET_TARGET_HOTELS,
  resolveCanonicalCompSet,
  applyPublicIdentity,
} from "./resolve-comp-set.js";
import { buildCompSetPublicSearchPivots, selectPriorityPivots } from "./search-pivots.js";
import { buildFormattedPhoneVariants, extractPublicPhoneFromText } from "./phone-variants.js";
import {
  validateCompDemandPage,
  buildCompetitorDemandTrace,
} from "./page-validate.js";
import { buildCompSetRepeatPattern } from "./repeat-pattern.js";
import { buildCrossCompetitorPatterns } from "./cross-competitor.js";
import { buildTargetHotelThesis, deriveWinAngles, FIT_CLASS } from "./target-thesis.js";
import {
  patternEligibleForGdiCandidate,
  patternToGdiDraft,
  classifyCompDemandGdiCandidate,
  resolveBuyerForPattern,
} from "./gdi-conversion.js";
import { jevAdviseResearchLead } from "../opportunity-discovery-v5/next-research.js";
import { fetchCandidatePage } from "../candidate-completion-v2/page-research.js";
import { EVIDENCE_CLASS } from "./evidence-class.js";

function hasSerp() {
  return Boolean(String(process.env.SERPAPI_KEY || process.env.SERPAPI_API_KEY || "").trim());
}

async function runSerp(q, hotel, budget) {
  if (!hasSerp() || budget.queriesLeft <= 0) return [];
  budget.queriesLeft -= 1;
  budget.queriesRun += 1;
  try {
    const serp = await serpapiSearch({
      engine: "google",
      q,
      num: 5,
      hl: hotel.serpHl || "en",
      gl: hotel.serpGl || "us",
    });
    budget.costUsd += 0.05;
    return serp?.data?.organic_results || [];
  } catch (err) {
    budget.errors.push(String(err?.message || err).slice(0, 160));
    budget.costUsd += 0.05;
    return [];
  }
}

/**
 * Light public identity resolve for a competitor (official site / contact).
 * Does not invent phone — only extracts when found on fetched page.
 */
async function resolveCompetitorPublicIdentity(comp, hotel, budget) {
  if (budget.queriesLeft <= 0) return comp;
  const hits = await runSerp(
    `"${comp.canonicalName}" (official OR hotel OR contact OR téléphone OR teléfono) ${hotel.market?.split("/")[0] || ""}`,
    hotel,
    budget
  );
  const top = hits.find(
    (h) =>
      h.link &&
      !/tripadvisor|booking\.com|expedia|yelp|wikipedia|facebook\.com|linkedin\.com|blog/i.test(h.link) &&
      /hotel|marriott|hilton|hyatt|fairmont|melia|nh-|ibis|novotel|yotel|rosewood|sandals|calabash|silversands/i.test(
        h.link + (h.title || "")
      )
  );
  if (!top?.link) return comp;

  const page = await fetchCandidatePage(top.link, { maxChars: 8000 });
  budget.costUsd += 0.01;
  if (!page.ok) {
    return applyPublicIdentity(comp, {
      currentWebsite: top.link,
      domain: (() => {
        try {
          return new URL(top.link).hostname.replace(/^www\./, "");
        } catch {
          return null;
        }
      })(),
    });
  }

  const phone = extractPublicPhoneFromText(page.text || "", hotel.countryHint);
  let domain = null;
  try {
    domain = new URL(page.url || top.link).hostname.replace(/^www\./, "");
  } catch {
    /* ignore */
  }
  const addrMatch = String(page.text || "").match(
    /\b(\d{1,5}\s+[A-Z][\w\s.'-]{6,60}(?:,\s*[A-Z][\w\s-]{2,40}){1,3})\b/
  );

  return applyPublicIdentity(comp, {
    publicPhone: phone,
    address: addrMatch?.[1] || null,
    domain,
    currentWebsite: page.url || top.link,
    knownAliases: [comp.canonicalName],
  });
}

/**
 * Run Comp Set Demand Mining for one target hotel.
 */
export async function runCompSetDemandMiningForHotel(targetHotel = {}, opts = {}) {
  const resolved = resolveCanonicalCompSet(targetHotel, {
    maxCompetitors: opts.maxCompetitors ?? 4,
  });
  const budget = {
    queriesLeft: opts.maxQueries ?? 14,
    queriesRun: 0,
    costUsd: 0,
    errors: [],
  };

  const hotelCtx = { ...targetHotel, ...resolved };
  const competitors = [];
  const allPivots = [];
  const phoneHits = [];
  const pageRows = [];
  const traces = [];
  const deepTraces = [];

  // Phase 1+2: resolve identity + pivots
  for (const comp of resolved.competitors) {
    let c = { ...comp };
    if (opts.resolveIdentity !== false && budget.queriesLeft > 0) {
      c = await resolveCompetitorPublicIdentity(c, hotelCtx, budget);
      if (c.publicPhone) {
        c.formattedPhoneVariants = buildFormattedPhoneVariants(c.publicPhone, hotelCtx.countryHint);
      }
    }
    competitors.push(c);

    const pivots = buildCompSetPublicSearchPivots(c, hotelCtx, {
      maxPivotsPerCompetitor: 20,
    });
    const selected = selectPriorityPivots(pivots, { max: opts.maxPivotsPerCompetitor ?? 4 });
    allPivots.push(...selected);

    let pagesForComp = 0;
    const maxPagesPerComp = opts.maxPagesPerCompetitor ?? 2;

    for (const pivot of selected) {
      if (budget.queriesLeft <= 0) break;
      const hits = await runSerp(pivot.query, hotelCtx, budget);

      for (const hit of hits.slice(0, 3)) {
        const row = {
          pivotId: pivot.pivotId,
          pivotType: pivot.pivotType,
          competitorHotelId: c.competitorHotelId,
          canonicalName: c.canonicalName,
          query: pivot.query,
          hitTitle: hit.title,
          hitUrl: hit.link,
          hitSnippet: hit.snippet,
          phoneVariant: pivot.phoneVariant || "",
        };
        if (/PUBLIC_PHONE/i.test(pivot.pivotType)) phoneHits.push(row);

        if (pagesForComp >= maxPagesPerComp) continue;
        // Skip OTA / directory / social login before fetch — we need third-party group pages
        if (
          /tripadvisor|booking\.com|expedia|hotels\.com|trivago|yelp|traveloka|laterooms|kayak|hotelluxe|elcomparador|facebook\.com\/login|instagram\.com|myboutiquehotel|fivestaralliance\.com\/gallery/i.test(
            hit.link || ""
          )
        ) {
          continue;
        }
        // Skip pure hotel marketing rooms pages unless housing/block language in title/snippet
        const hitBlob = `${hit.title || ""} ${hit.snippet || ""}`;
        if (
          /\/rooms?(\/|$)|\/deals?(\/|$)|check availability|book now/i.test(hit.link || "") &&
          !/hotel block|official hotel|host hotel|accommodation|housing|hébergement|alojamiento/i.test(hitBlob)
        ) {
          continue;
        }

        const validation = await validateCompDemandPage(hit, c, pivot, {});
        budget.costUsd += 0.01;
        pagesForComp += 1;
        pageRows.push({
          competitorHotelId: c.competitorHotelId,
          pivotId: pivot.pivotId,
          url: validation.url,
          pageOk: validation.pageOk,
          pageKind: validation.pageKind,
          evidenceClass: validation.evidenceClass,
          organization: validation.organization,
          feedsDeeperResearch: validation.feedsDeeperResearch,
          reason: validation.reason,
          error: validation.error || "",
        });

        // If page revealed phone and we lacked it
        if (validation.extractedPhone && !c.publicPhone) {
          c = applyPublicIdentity(c, { publicPhone: validation.extractedPhone });
          c.formattedPhoneVariants = buildFormattedPhoneVariants(
            validation.extractedPhone,
            hotelCtx.countryHint
          );
          // update in competitors array
          const idx = competitors.findIndex((x) => x.competitorHotelId === c.competitorHotelId);
          if (idx >= 0) competitors[idx] = c;
        }

        const trace = buildCompetitorDemandTrace(validation, c, hotelCtx, pivot);
        traces.push(trace);
        if (trace.feedsDeeperResearch) deepTraces.push(trace);
      }
    }
  }

  // Repeat patterns per org
  const bySeries = new Map();
  for (const t of deepTraces) {
    const k = t.eventSeriesId || t.organization;
    if (!bySeries.has(k)) bySeries.set(k, []);
    bySeries.get(k).push(t);
  }
  const repeatPatterns = [];
  for (const [, group] of bySeries) {
    const rpt = buildCompSetRepeatPattern(group);
    if (rpt) repeatPatterns.push(rpt);
  }

  const crossPatterns = buildCrossCompetitorPatterns(deepTraces, hotelCtx);

  // Buyer + thesis + win angles + GDI conversion
  const buyerRows = [];
  const theses = [];
  const winAngles = [];
  const jevRows = [];
  const gdiCandidates = [];
  const customerReady = [];
  const futureWatch = [];

  const patternUniverse = crossPatterns.length ? crossPatterns : repeatPatterns.map((r) => ({
    ...r,
    historicHotels: String(r.historicHotels || "").split("|").filter(Boolean),
    historicMarkets: String(r.historicMarkets || "").split("|").filter(Boolean),
    historicCycles: String(r.historicDates || "").split("|").filter(Boolean),
    lodgingPattern: r.roomBlock ? "LODGING_EVIDENCE_PRESENT" : "UNKNOWN",
    evidenceSet: r.evidenceClasses,
    sources: r.sourceUrls,
    repeatCadence: r.cadence,
  }));

  for (const pattern of patternUniverse) {
    const relatedTrace = deepTraces.find(
      (t) => t.organization === pattern.organization || t.eventSeriesId === pattern.eventSeries
    ) || {};
    const buyer = resolveBuyerForPattern(pattern, relatedTrace);
    buyerRows.push({
      patternId: pattern.patternId,
      hotelKey: hotelCtx.hotelKey,
      ...buyer,
    });

    const thesis = buildTargetHotelThesis(pattern, hotelCtx);
    theses.push(thesis);
    winAngles.push(...deriveWinAngles(thesis, hotelCtx));

    // Jev only after validated pattern
    const jev = jevAdviseResearchLead(
      {
        id: pattern.patternId,
        title: pattern.eventProgram || pattern.organization,
        organizationName: pattern.organization,
        eventYear: pattern.nextKnownCycle,
        lodgingEvidence: /LODGING/i.test(pattern.lodgingPattern || "") ? { status: "WEAK" } : null,
        organizationContactUrl: buyer.publicContactPath,
      },
      { admitted: true, stepsTaken: 0, placeNames: [hotelCtx.market] }
    );
    jevRows.push({
      patternId: pattern.patternId,
      hotelKey: hotelCtx.hotelKey,
      ...jev,
    });

    const eligible = patternEligibleForGdiCandidate(pattern, thesis, buyer);
    if (!eligible.ok) continue;

    const draft = patternToGdiDraft(pattern, thesis, buyer, hotelCtx);
    const classified = classifyCompDemandGdiCandidate(draft, {
      nowDate: opts.nowDate || "2026-10-03",
      geoTokens: [hotelCtx.market, hotelCtx.hotelName].filter(Boolean),
    });

    gdiCandidates.push({
      ...draft,
      gdiStage: classified.stage,
      eligibilityMissing: "",
      customerReady: classified.customerReady,
      validFutureWatch: classified.validFutureWatch,
    });
    if (classified.customerReady) customerReady.push(draft);
    else if (classified.validFutureWatch) futureWatch.push(draft);
  }

  return {
    hotelKey: hotelCtx.hotelKey,
    hotelId: hotelCtx.hotelId,
    hotelName: hotelCtx.hotelName,
    compSetSource: resolved.compSetSource,
    competitors,
    pivots: allPivots,
    phoneHits,
    pageRows,
    traces,
    deepTraces,
    repeatPatterns,
    crossPatterns,
    buyerRows,
    theses,
    winAngles,
    jevRows,
    gdiCandidates,
    customerReady,
    futureWatch,
    counts: {
      competitors: competitors.length,
      pivots: allPivots.length,
      phoneHits: phoneHits.length,
      traces: traces.length,
      directConfirmed: traces.filter((t) => t.evidenceClass === EVIDENCE_CLASS.DIRECT_CONFIRMED).length,
      strongAssociation: traces.filter((t) => t.evidenceClass === EVIDENCE_CLASS.STRONG_ASSOCIATION).length,
      deepTraces: deepTraces.length,
      repeatPatterns: repeatPatterns.length,
      futureCycles: repeatPatterns.filter((r) => r.nextKnownCycle || r.nextExpectedCycle).length,
      strongFit: theses.filter((t) => t.fitClass === FIT_CLASS.STRONG_FIT).length,
      plausibleFit: theses.filter((t) => t.fitClass === FIT_CLASS.PLAUSIBLE_FIT).length,
      gdiCandidates: gdiCandidates.length,
      customerReady: customerReady.length,
      futureWatch: futureWatch.length,
    },
    costUsd: budget.costUsd,
    queriesRun: budget.queriesRun,
    errors: budget.errors,
    serpEnabled: hasSerp(),
  };
}

export { COMP_SET_TARGET_HOTELS, hasSerp };
