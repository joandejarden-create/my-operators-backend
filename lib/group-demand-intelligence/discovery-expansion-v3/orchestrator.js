/**
 * Discovery Expansion V3 orchestrator — bounded multilingual multi-scout discovery.
 * Does not lower readiness / watch thresholds.
 */

import { serpapiSearch } from "../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import { classifyEntityTruth } from "../entity-truth-gate-v1.js";
import { classifyCustomerSurfaceOpportunity } from "../customer-surface-revalidation-v1.js";
import { isGdiCustomerOpportunityReady } from "../customer-readiness-gate-v1.js";
import { isValidFutureWatch } from "../future-watch/is-valid-future-watch-v1.js";
import { applyCustomerSurfaceDisposition } from "../customer-surface-revalidation-v1.js";
import {
  resolveGdiMarketLanguages,
  languagesForScoutPass,
} from "./market-languages.js";
import { buildLocalizedQueryMatrix, CANONICAL_INTENTS } from "./localized-intents.js";
import {
  SCOUT_FAMILY,
  buildScoutQueryPlan,
  defaultScoutPriorityForHotel,
  extractHiddenDemandCandidates,
  corporateTriggerImpliesLodgingMotion,
} from "./scouts.js";
import { dedupeCrossLanguageCandidates } from "./cross-language-dedup.js";
import {
  getGdiNextBestResearchAction,
  isPromisingNearMiss,
} from "./next-best-research.js";
import { buildSeriesRecord, seriesMayBeValidFutureWatch } from "./association-series.js";

const DIRECTORY_NOISE_RE =
  /\b(booking\.com|expedia|hotels\.com|trivago|tripadvisor|airbnb|kayak|hotels?\s+near)\b/i;
const SUPPLY_PROMO_RE =
  /\b(book now|room package|hotel week|staycation|our rooms|check availability)\b/i;

function hasSerp() {
  return Boolean(String(process.env.SERPAPI_KEY || process.env.SERPAPI_API_KEY || "").trim());
}

function slugId(hotelKey, title, url) {
  const base = String(title || url || "cand")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 48);
  const host = (() => {
    try {
      return new URL(url).hostname.replace(/\W/g, "").slice(0, 12);
    } catch {
      return "x";
    }
  })();
  return `gdi_v3_${hotelKey.toLowerCase()}_${base}_${host}`.slice(0, 96);
}

function hitToCandidate(hit, meta = {}) {
  const title = String(hit.title || "").trim();
  const link = String(hit.link || hit.url || "").trim();
  const snippet = String(hit.snippet || hit.description || "").trim();
  if (!title || !link) return null;
  if (DIRECTORY_NOISE_RE.test(`${title} ${link} ${snippet}`)) return null;
  if (SUPPLY_PROMO_RE.test(`${title} ${snippet}`) && !/block|tender|RFP|housing|congress/i.test(snippet)) {
    return null;
  }

  // Corporate trigger: require lodging motion
  if (
    meta.scoutFamily === SCOUT_FAMILY.CORPORATE_TRIGGER &&
    !corporateTriggerImpliesLodgingMotion(`${title} ${snippet}`)
  ) {
    return null;
  }

  const orgGuess = title.split(/[|\-—:]/)[0].trim().slice(0, 120);
  const yearMatch = `${title} ${snippet}`.match(/\b(202[6-9]|203[0-2])\b/);
  const lodgingHint =
    /\b(room block|host hotel|housing|accommodation|hôtel officiel|alojamiento|hébergement|hotel block|RFP|tender|licitación)\b/i.test(
      `${title} ${snippet} ${link}`
    );

  const marketPlace = meta.marketPlace || null;
  return {
    id: slugId(meta.hotelKey || "h", title, link),
    title,
    organizationName: orgGuess,
    opportunityName: title,
    opportunityType:
      meta.scoutFamily === SCOUT_FAMILY.PROCUREMENT
        ? "PRIMARY_PURSUIT"
        : lodgingHint
          ? "OVERFLOW_HOUSING"
          : "FUTURE_CYCLE",
    officialSource: link,
    discoverySource: link,
    sources: [{ url: link, kind: "discovery_expansion_v3", scout: meta.scoutFamily }],
    summaryWhat: snippet.slice(0, 280),
    hotelOpportunityThesis: lodgingHint
      ? `${meta.hotelLabel || "Hotel"} can pursue overflow / preferred lodging or group block demand related to ${title}.`
      : `${title} may generate group lodging demand relevant to ${meta.hotelLabel || "the hotel"} — requires lodging confirmation.`,
    whyNow: lodgingHint
      ? "Public lodging / housing / procurement signal visible — confirm cycle and contact path."
      : "Discovery hit — confirm timing, lodging motion, and buyer before promotion.",
    recommendedAction:
      "Validate official source, lodging evidence, and contact path; do not sell until gates pass.",
    summaryWhyMatters: snippet.slice(0, 200) || "Public discovery signal pending validation.",
    summaryWhyHotel: meta.fitLine || "Market-fit pending hotel-specific validation.",
    hotelFitScore: meta.defaultFitScore ?? 48,
    venueStatus: "Unknown",
    // Provenance from market-scoped query (not invented event city from thin SERP titles)
    eventLocationSummary: marketPlace || null,
    destinationStatus: marketPlace || null,
    discoveryMeta: {
      scoutFamily: meta.scoutFamily,
      queryLanguage: meta.queryLanguage,
      localizedQuery: meta.localizedQuery,
      queryFamily: meta.queryFamily,
      feederMarket: meta.feederMarket || null,
      canonicalIntent: meta.canonicalIntent || null,
      marketPlace,
      v3: true,
    },
    queryLanguage: meta.queryLanguage,
    sourceLanguage: meta.queryLanguage,
    eventYear: yearMatch ? yearMatch[1] : null,
    eventStartDate: null,
    lodgingEvidence: lodgingHint
      ? {
          housingPageFound: /accommodation|housing|hotel-block|alojamiento|hébergement/i.test(link),
          roomBlockMentioned: /room.?block|host.?hotel|bloc/i.test(`${title} ${snippet}`),
          status: lodgingHint ? "WEAK" : "NONE",
        }
      : null,
    gdiDiscoveryVersion: "discovery_expansion_v3",
    createdAt: new Date().toISOString(),
  };
}

async function runSerpQuery(q, budget) {
  if (!hasSerp() || budget.queriesLeft <= 0) return { hits: [], costUsd: 0 };
  budget.queriesLeft -= 1;
  budget.queriesRun += 1;
  try {
    const serp = await serpapiSearch({
      engine: "google",
      q: q.localizedQuery,
      num: 5,
      hl: q.serpHl || "en",
      gl: q.serpGl || "us",
    });
    const hits = serp?.data?.organic_results || [];
    budget.costUsd += 0.05;
    return { hits, costUsd: 0.05 };
  } catch (err) {
    budget.errors.push(String(err?.message || err).slice(0, 160));
    return { hits: [], costUsd: 0.05 };
  }
}

function qualifyCandidate(cand, opts = {}) {
  const entity = classifyEntityTruth(cand);
  const surface = classifyCustomerSurfaceOpportunity(cand, opts);
  const ready = isGdiCustomerOpportunityReady(cand, opts);
  const watch = isValidFutureWatch(cand, opts);
  return {
    entityValid: entity.validEntity === true,
    entityClass: entity.entityClass,
    surfaceKeep: surface.keepActive === true,
    surfaceDisposition: surface.disposition,
    customerReady: ready.ok === true,
    readyFailed: ready.failed || [],
    validFutureWatch: watch?.ok === true || watch?.valid === true || watch?.class === "VALID_FUTURE_WATCH",
    watchClass: watch?.class || watch?.validationClass || null,
    watch,
  };
}

/**
 * Run V3 discovery for one hotel (bounded).
 */
export async function runDiscoveryExpansionV3ForHotel(hotelCtx = {}, opts = {}) {
  const hotelId = hotelCtx.hotelId;
  const hotelKey = hotelCtx.hotelKey;
  const langProfile = resolveGdiMarketLanguages(hotelCtx);
  const languages = languagesForScoutPass(langProfile, {
    allowSelective: opts.allowSelective === true,
  });
  const scouts = (opts.scouts || defaultScoutPriorityForHotel(hotelKey)).slice(
    0,
    opts.maxScouts ?? 5
  );
  const budget = {
    queriesLeft: opts.maxQueries ?? 10,
    queriesRun: 0,
    costUsd: 0,
    errors: [],
  };

  const existingIds = new Set((opts.existingOpps || []).map((o) => o.id));
  const existingTitles = new Set(
    (opts.existingOpps || []).map((o) => String(o.title || "").toLowerCase())
  );

  const queryMatrix = [];
  const rawCandidates = [];
  const scoutStats = {};

  for (const scout of scouts) {
    if (budget.queriesLeft <= 0) break;
    const plan = buildScoutQueryPlan(
      scout,
      {
        ...hotelCtx,
        placeNames: hotelCtx.placeNames,
        market: langProfile.market,
      },
      { languages: languages.slice(0, opts.maxLanguagesPerScout ?? 2), maxPerLang: 2 }
    );
    scoutStats[scout] = { queries: 0, hits: 0, candidates: 0 };
    for (const q of plan) {
      if (budget.queriesLeft <= 0) break;
      queryMatrix.push(q);
      const { hits } = await runSerpQuery(q, budget);
      scoutStats[scout].queries += 1;
      scoutStats[scout].hits += hits.length;
      for (const hit of hits.slice(0, 4)) {
        const cand = hitToCandidate(hit, {
          hotelKey,
          hotelLabel: hotelCtx.label,
          scoutFamily: scout,
          queryLanguage: q.queryLanguage,
          localizedQuery: q.localizedQuery,
          queryFamily: q.queryFamily,
          feederMarket: q.feederMarket,
          canonicalIntent: q.canonicalIntent,
          fitLine: hotelCtx.fitLine,
          defaultFitScore: hotelCtx.defaultFitScore ?? 48,
          marketPlace: (hotelCtx.placeNames || []).slice(0, 2).join(", "),
        });
        if (!cand) continue;
        if (existingIds.has(cand.id)) continue;
        if (existingTitles.has(String(cand.title).toLowerCase())) continue;
        rawCandidates.push(cand);
        scoutStats[scout].candidates += 1;
      }
    }
  }

  // Feeder-market slice (EN)
  if (budget.queriesLeft > 0 && (langProfile.feederMarkets || []).length) {
    const feederMatrix = buildLocalizedQueryMatrix({
      intents: [
        CANONICAL_INTENTS.INCENTIVE_GROUP,
        CANONICAL_INTENTS.ASSOCIATION_CONGRESS,
        CANONICAL_INTENTS.ROOM_BLOCK,
      ],
      languages: ["en"],
      marketPlaceNames: hotelCtx.placeNames || [],
      feederMarkets: langProfile.feederMarkets.slice(0, 3),
    }).slice(0, Math.min(3, budget.queriesLeft));
    for (const q of feederMatrix) {
      queryMatrix.push(q);
      const { hits } = await runSerpQuery(q, budget);
      for (const hit of hits.slice(0, 3)) {
        const cand = hitToCandidate(hit, {
          hotelKey,
          hotelLabel: hotelCtx.label,
          scoutFamily: SCOUT_FAMILY.TOUR_DMC,
          queryLanguage: q.queryLanguage,
          localizedQuery: q.localizedQuery,
          queryFamily: q.queryFamily,
          feederMarket: q.feederMarket,
          canonicalIntent: q.canonicalIntent,
          fitLine: hotelCtx.fitLine,
          marketPlace: (hotelCtx.placeNames || []).slice(0, 2).join(", "),
        });
        if (!cand) continue;
        if (existingTitles.has(String(cand.title).toLowerCase())) continue;
        cand.originMarket = q.feederMarket;
        cand.destinationMarket = langProfile.market;
        cand.groupMotion = q.canonicalIntent;
        rawCandidates.push(cand);
      }
    }
  }

  const deduped = dedupeCrossLanguageCandidates(rawCandidates);

  // Hidden demand from strongest parent-like hits
  const hidden = [];
  for (const parent of deduped.unique.slice(0, 5)) {
    for (const sub of extractHiddenDemandCandidates(parent, { maxSub: 2 })) {
      hidden.push({
        ...parent,
        id: slugId(hotelKey, sub.title, parent.officialSource || "h"),
        title: sub.title,
        discoveryMeta: {
          ...(parent.discoveryMeta || {}),
          scoutFamily: SCOUT_FAMILY.HIDDEN_DEMAND,
          subDemandType: sub.subDemandType,
          parentOpportunityId: parent.id,
        },
        hotelOpportunityThesis: `Sub-demand (${sub.subDemandType}) under ${parent.title}: pursue only with identifiable buyer + lodging evidence.`,
      });
    }
  }
  const afterHidden = dedupeCrossLanguageCandidates([...deduped.unique, ...hidden]);

  // Next-best research (one step) for promising near-misses
  const nextBest = [];
  const researched = [];
  let nbrBudget = opts.maxNextBest ?? 4;
  // Prefer lodging-hint / procurement hits for next-best completion
  const nbrQueue = [...afterHidden.unique].sort((a, b) => {
    const score = (x) =>
      (x.lodgingEvidence ? 3 : 0) +
      (x.discoveryMeta?.scoutFamily === SCOUT_FAMILY.PROCUREMENT ? 2 : 0) +
      (x.eventYear ? 1 : 0) +
      (x.hotelFitScore || 0) / 100;
    return score(b) - score(a);
  });
  for (const cand of nbrQueue) {
    if (nbrBudget <= 0 || budget.queriesLeft <= 0) break;
    if (
      !isPromisingNearMiss(cand, {
        marketTokens: hotelCtx.geoTokens || hotelCtx.placeNames || [],
      })
    ) {
      continue;
    }
    const action = getGdiNextBestResearchAction(cand, {
      marketPlace: (hotelCtx.placeNames || [])[0],
    });
    if (!action.query) continue;
    nbrBudget -= 1;
    const { hits } = await runSerpQuery(
      {
        localizedQuery: action.query,
        serpHl: "en",
        serpGl: "us",
      },
      budget
    );
    const top = hits[0];
    const nbrRow = {
      opportunityId: cand.id,
      hotel: hotelKey,
      ...action,
      hitTitle: top?.title || null,
      hitUrl: top?.link || null,
      applied: Boolean(top?.link),
    };
    nextBest.push(nbrRow);
    if (top?.link) {
      const next = { ...cand };
      if (action.blocker === "LODGING") {
        next.lodgingEvidence = {
          ...(next.lodgingEvidence || {}),
          housingPageFound: /housing|accommodation|hotel/i.test(top.link),
          roomBlockMentioned: /block|host hotel/i.test(`${top.title} ${top.snippet || ""}`),
          status: "WEAK",
        };
        next.discoverySource = top.link;
        next.sources = [...(next.sources || []), { url: top.link, kind: "next_best_lodging" }];
      } else if (action.blocker === "TIMING") {
        const ym = `${top.title} ${top.snippet || ""}`.match(/\b(202[6-9])-(\d{2})-(\d{2})\b/) ||
          `${top.title} ${top.snippet || ""}`.match(/\b(20[2-3]\d)\b/);
        if (ym && ym[0].length === 10) next.eventStartDate = ym[0];
        else if (ym) next.eventYear = ym[1] || ym[0];
        next.futureCycleEvidenceState = next.eventStartDate
          ? "CURRENT_FUTURE_CYCLE_CONFIRMED"
          : "FUTURE_CYCLE_UNCONFIRMED";
        next.sources = [...(next.sources || []), { url: top.link, kind: "next_best_timing" }];
      } else if (action.blocker === "WHO" || action.blocker === "CONTACT_PATH") {
        next.organizationContactUrl = top.link;
        next.contactResearchAttempted = true;
        next.contactResearchState = "ATTEMPTED";
        next.sources = [...(next.sources || []), { url: top.link, kind: "next_best_who" }];
      }
      next.whyNow = next.whyNow || action.exactQuestion;
      researched.push(next);
    } else {
      researched.push(cand);
    }
  }

  // Merge researched replacements
  const byId = new Map(afterHidden.unique.map((c) => [c.id, c]));
  for (const r of researched) byId.set(r.id, r);
  const finalCandidates = [...byId.values()];

  // Association series stubs from annual/congress hits
  const seriesRows = [];
  for (const c of finalCandidates) {
    if (!/congress|annual|assemblée|congreso|kongress|meeting/i.test(c.title || "")) continue;
    const series = buildSeriesRecord({
      organization: c.organizationName,
      eventSeriesId: `series_${normalizeLoose(c.organizationName)}`,
      historicalCycles: c.eventYear ? [c.eventYear] : [],
      nextKnownCycle: c.eventStartDate || c.eventYear || null,
      nextValidationTrigger: "Monitor official site for next-cycle dates / housing",
      nextCycleThesis: c.hotelOpportunityThesis,
      historicalRecurrenceReal: /annual|yearly|édition|edicion/i.test(c.title || ""),
      hotelFitScore: c.hotelFitScore,
      sourceUrl: c.officialSource,
      eventStartDate: c.eventStartDate,
      eventYear: c.eventYear,
    });
    seriesRows.push({
      hotel: hotelKey,
      opportunityId: c.id,
      ...series,
      mayWatch: seriesMayBeValidFutureWatch(series),
    });
  }

  const qualified = [];
  const rejected = [];
  const readyList = [];
  const watchList = [];

  for (const c of finalCandidates) {
    const q = qualifyCandidate(c, { nowDate: opts.nowDate || "2026-10-03" });
    const row = { ...c, qualification: q };
    if (q.customerReady) {
      const applied = applyCustomerSurfaceDisposition(c, null, {
        nowDate: opts.nowDate || "2026-10-03",
      });
      readyList.push({ ...applied, qualification: q });
      qualified.push(row);
    } else if (q.validFutureWatch && q.entityValid) {
      watchList.push(row);
      qualified.push(row);
    } else if (q.entityValid && q.surfaceKeep) {
      qualified.push(row);
    } else {
      rejected.push(row);
    }
  }

  // Yield scores
  const useful = readyList.length + watchList.length;
  const languageYield = {};
  const scoutYield = {};
  for (const c of finalCandidates) {
    const lang = c.queryLanguage || c.discoveryMeta?.queryLanguage || "unk";
    const scout = c.discoveryMeta?.scoutFamily || "unk";
    languageYield[lang] = languageYield[lang] || { candidates: 0, useful: 0 };
    scoutYield[scout] = scoutYield[scout] || { candidates: 0, useful: 0 };
    languageYield[lang].candidates += 1;
    scoutYield[scout].candidates += 1;
  }
  for (const c of [...readyList, ...watchList]) {
    const lang = c.queryLanguage || c.discoveryMeta?.queryLanguage || "unk";
    const scout = c.discoveryMeta?.scoutFamily || "unk";
    if (languageYield[lang]) languageYield[lang].useful += 1;
    if (scoutYield[scout]) scoutYield[scout].useful += 1;
  }
  for (const k of Object.keys(languageYield)) {
    const x = languageYield[k];
    x.languageYieldScore = x.candidates ? x.useful / x.candidates : 0;
  }
  for (const k of Object.keys(scoutYield)) {
    const x = scoutYield[k];
    x.scoutYieldScore = x.candidates ? x.useful / x.candidates : 0;
  }

  return {
    hotelId,
    hotelKey,
    langProfile,
    baselineCount: (opts.existingOpps || []).length,
    queryMatrix,
    scoutStats,
    candidatesRaw: rawCandidates.length,
    duplicatesRemoved: afterHidden.removed.length + deduped.removed.length,
    aliasPairs: afterHidden.aliasPairs,
    newCandidates: finalCandidates,
    qualified,
    customerReady: readyList,
    futureWatch: watchList,
    rejected,
    nextBest,
    seriesRows,
    hiddenExtracted: hidden.length,
    languageYield,
    scoutYield,
    usefulCandidateCount: useful,
    costUsd: budget.costUsd,
    queriesRun: budget.queriesRun,
    errors: budget.errors,
    serpEnabled: hasSerp(),
  };
}

function normalizeLoose(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .slice(0, 40);
}

export { SCOUT_FAMILY, hasSerp };
