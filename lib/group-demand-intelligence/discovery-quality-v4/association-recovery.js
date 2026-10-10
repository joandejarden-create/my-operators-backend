/**
 * Surgical AssociationScout recovery — Association-only queries, not broad rediscovery.
 */

import { serpapiSearch } from "../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import {
  SCOUT_FAMILY,
  buildScoutQueryPlan,
} from "../discovery-expansion-v3/scouts.js";
import { resolveGdiMarketLanguages, languagesForScoutPass } from "../discovery-expansion-v3/market-languages.js";
import { dedupeCrossLanguageCandidates } from "../discovery-expansion-v3/cross-language-dedup.js";

function hasSerp() {
  return Boolean(String(process.env.SERPAPI_KEY || process.env.SERPAPI_API_KEY || "").trim());
}

function slugId(hotelKey, title, url) {
  const base = String(title || url || "cand")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 48);
  let host = "x";
  try {
    host = new URL(url).hostname.replace(/\W/g, "").slice(0, 12);
  } catch {
    /* ignore */
  }
  return `gdi_v3_${hotelKey.toLowerCase()}_${base}_${host}`.slice(0, 96);
}

function hitToAssocCandidate(hit, meta = {}) {
  const title = String(hit.title || "").trim();
  const link = String(hit.link || hit.url || "").trim();
  const snippet = String(hit.snippet || "").trim();
  if (!title || !link) return null;
  if (/\b(booking\.com|expedia|tripadvisor|indeed\.|jobs?)\b/i.test(`${title} ${link}`)) {
    return null;
  }
  const orgGuess = title.split(/[|\-—:]/)[0].trim().slice(0, 120);
  const lodgingHint =
    /\b(room block|host hotel|housing|accommodation|hébergement|alojamiento|hotel block)\b/i.test(
      `${title} ${snippet} ${link}`
    );
  const yearMatch = `${title} ${snippet}`.match(/\b(202[6-9]|203[0-2])\b/);
  return {
    id: slugId(meta.hotelKey || "h", title, link),
    title,
    organizationName: orgGuess,
    opportunityName: title,
    opportunityType: lodgingHint ? "OVERFLOW_HOUSING" : "FUTURE_CYCLE",
    officialSource: link,
    discoverySource: link,
    sources: [{ url: link, kind: "association_recovery_v4", scout: SCOUT_FAMILY.ASSOCIATION }],
    summaryWhat: snippet.slice(0, 280),
    hotelOpportunityThesis: lodgingHint
      ? `${meta.hotelLabel}: association lodging/housing signal for ${title}.`
      : `${title} may generate association group demand for ${meta.hotelLabel} — confirm lodging.`,
    whyNow: "Recovered AssociationScout detail row (V3 persistence gap).",
    recommendedAction: "Validate series timing, lodging, organizer before promotion.",
    summaryWhyMatters: snippet.slice(0, 200) || "Association discovery signal.",
    summaryWhyHotel: meta.fitLine || "Market fit pending.",
    hotelFitScore: meta.defaultFitScore ?? 48,
    venueStatus: "Unknown",
    eventLocationSummary: meta.marketPlace || null,
    destinationStatus: meta.marketPlace || null,
    discoveryMeta: {
      scoutFamily: SCOUT_FAMILY.ASSOCIATION,
      queryLanguage: meta.queryLanguage,
      localizedQuery: meta.localizedQuery,
      queryFamily: meta.queryFamily,
      v3: true,
      recoveredV4: true,
    },
    queryLanguage: meta.queryLanguage,
    sourceLanguage: meta.queryLanguage,
    eventYear: yearMatch ? yearMatch[1] : null,
    lodgingEvidence: lodgingHint
      ? {
          housingPageFound: /accommodation|housing/i.test(link),
          roomBlockMentioned: /room.?block|host.?hotel/i.test(`${title} ${snippet}`),
          status: "WEAK",
        }
      : null,
    hotelId: meta.hotelId,
    hotelKey: meta.hotelKey,
    gdiDiscoveryVersion: "discovery_expansion_v3_recovered_v4",
    signalType: "ASSOCIATION_RECOVERED",
  };
}

/**
 * Recover AssociationScout detail candidates for one hotel (bounded).
 */
export async function recoverAssociationScoutForHotel(hotelCtx, opts = {}) {
  const targetCount = opts.targetCount ?? 16;
  const langProfile = resolveGdiMarketLanguages(hotelCtx);
  const languages = languagesForScoutPass(langProfile, { allowSelective: false }).slice(0, 2);
  const plan = buildScoutQueryPlan(
    SCOUT_FAMILY.ASSOCIATION,
    { ...hotelCtx, placeNames: hotelCtx.placeNames, market: langProfile.market },
    { languages, maxPerLang: 2 }
  ).slice(0, opts.maxQueries ?? 4);

  const raw = [];
  let queries = 0;
  let costUsd = 0;
  const errors = [];

  if (!hasSerp()) {
    return {
      candidates: [],
      queries: 0,
      costUsd: 0,
      errors: ["SERPAPI_MISSING"],
      note: "Cannot recover without SERP; ASSOCIATION_RESULTS.csv was never written in V3",
    };
  }

  for (const q of plan) {
    if (raw.length >= targetCount) break;
    queries += 1;
    try {
      const serp = await serpapiSearch({
        engine: "google",
        q: q.localizedQuery,
        num: 5,
        hl: q.serpHl || "en",
        gl: q.serpGl || "us",
      });
      costUsd += 0.05;
      for (const hit of serp?.data?.organic_results || []) {
        const cand = hitToAssocCandidate(hit, {
          hotelKey: hotelCtx.hotelKey,
          hotelId: hotelCtx.hotelId,
          hotelLabel: hotelCtx.label,
          queryLanguage: q.queryLanguage,
          localizedQuery: q.localizedQuery,
          queryFamily: q.queryFamily,
          fitLine: hotelCtx.fitLine,
          defaultFitScore: hotelCtx.defaultFitScore,
          marketPlace: (hotelCtx.placeNames || []).slice(0, 2).join(", "),
        });
        if (cand) raw.push(cand);
      }
    } catch (err) {
      errors.push(String(err?.message || err).slice(0, 120));
      costUsd += 0.05;
    }
  }

  const deduped = dedupeCrossLanguageCandidates(raw);
  return {
    candidates: deduped.unique.slice(0, targetCount),
    duplicatesRemoved: deduped.duplicatesRemoved || 0,
    queries,
    costUsd,
    errors,
    scoutFamily: SCOUT_FAMILY.ASSOCIATION,
  };
}
