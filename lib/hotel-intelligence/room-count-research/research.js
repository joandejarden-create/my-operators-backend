/**
 * Room Count Research Engine — deterministic, evidence-backed, not a crawler.
 *
 * Pipeline per hotel:
 *  1) Fetch known official website (if provided)
 *  2) Targeted SerpApi Google searches (capped) for snippets + URL discovery
 *  3) Fetch ≤3 eligible follow-up pages
 *  4) Extract explicit multilingual room-count phrases only
 *  5) Score confidence + research status; stage evidence locally
 *
 * Never writes Airtable / census.
 */

import { serpapiSearch, safeErrorMessage } from "../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import { extractRoomCountsFromText } from "./extract.js";
import {
  classifySourceUrl,
  isFetchEligibleUrl,
  SOURCE_CATEGORIES,
} from "./trust.js";
import { buildRoomCountQueries, selectFetchCandidates } from "./queries.js";
import { fetchResearchPage, htmlToSearchableText } from "./fetch.js";
import {
  scoreRoomCountResearch,
  RESEARCH_STATUS,
  ROOM_COUNT_CONFIDENCE_VERSION,
} from "./confidence.js";

export const ROOM_COUNT_RESEARCH_VERSION = "room-count-research-engine-v1";

function normalizeWebsite(url) {
  const s = String(url || "").trim();
  if (!s) return null;
  if (/^https?:\/\//i.test(s)) return s;
  return `https://${s}`;
}

/**
 * @param {object} hotel
 * @param {object} [opts]
 */
export async function researchHotelRoomCount(hotel = {}, opts = {}) {
  const t0 = Date.now();
  const env = opts.env || process.env;
  const hotelId = String(hotel.hotel_id || "").trim() || null;
  const name = String(hotel.hotel_name || hotel.name || "").trim();
  const website = normalizeWebsite(hotel.website);
  const maxSearches = Math.min(
    5,
    Math.max(0, Number(opts.maxSearches ?? env.ROOM_COUNT_RESEARCH_MAX_SEARCHES ?? 3))
  );
  const maxPageFetches = Math.min(
    5,
    Math.max(1, Number(opts.maxPageFetches ?? env.ROOM_COUNT_RESEARCH_MAX_FETCHES ?? 3))
  );
  const allowSerpapi =
    opts.allowSerpapi !== false &&
    String(env.ROOM_COUNT_RESEARCH_USE_SERPAPI || "1").trim() !== "0" &&
    Boolean(String(env.SERPAPI_KEY || env.SERPAPI_API_KEY || "").trim());

  /** @type {Array<object>} */
  const observations = [];
  const metrics = {
    searches: 0,
    pages_fetched: 0,
    pages_ok: 0,
    snippets_inspected: 0,
    serpapi_errors: 0,
  };
  const steps = [];

  function addHitsFromText(text, meta) {
    const extracted = extractRoomCountsFromText(text, { url: meta.url });
    for (const hit of extracted.hits || []) {
      if (hit.rejected) continue;
      observations.push({
        value: hit.count,
        source_category: meta.source_category,
        url: meta.url || null,
        quote: hit.quote || null,
        language: hit.language || null,
        method: hit.method,
        confidence_label: hit.confidence,
        rejected: Boolean(hit.rejected),
        observed_at: new Date().toISOString().slice(0, 10),
        step: meta.step,
      });
    }
    return extracted;
  }

  // --- Step 1: official website ---
  if (website && isFetchEligibleUrl(website)) {
    steps.push({ step: "official_hotel_fetch", url: website });
    metrics.pages_fetched += 1;
    const page = await fetchResearchPage(website);
    if (page.ok) {
      metrics.pages_ok += 1;
      addHitsFromText(page.text, {
        url: page.url,
        source_category: SOURCE_CATEGORIES.OFFICIAL_HOTEL,
        step: "official_hotel",
      });
    } else {
      steps.push({
        step: "official_hotel_fetch_failed",
        status: page.status,
        error: page.error || (page.blocked ? "blocked" : "fetch_failed"),
      });
    }
  }

  // --- Steps 2–5: targeted SerpApi Google searches ---
  const queries = buildRoomCountQueries({
    hotel_name: name,
    city: hotel.city,
    country: hotel.country,
    brand: hotel.brand,
    website,
  }).slice(0, maxSearches);

  /** @type {Array<{link?:string,title?:string,snippet?:string}>} */
  let organicPool = [];

  if (allowSerpapi && name) {
    for (const q of queries) {
      metrics.searches += 1;
      steps.push({ step: q.step, q: q.q });
      try {
        const res = await serpapiSearch(
          {
            engine: "google",
            q: q.q,
            num: 8,
            hl: "en",
            gl: "us",
          },
          { timeoutMs: opts.timeoutMs || 60000 }
        );
        if (!res.ok) {
          metrics.serpapi_errors += 1;
          steps.push({
            step: `${q.step}_error`,
            message: safeErrorMessage(res.error?.message || `http_${res.status}`),
          });
          continue;
        }
        const organic = Array.isArray(res.data?.organic_results)
          ? res.data.organic_results
          : [];
        organicPool.push(...organic);
        for (const row of organic) {
          const snippet = String(row.snippet || row.title || "");
          if (!snippet) continue;
          metrics.snippets_inspected += 1;
          const cat = classifySourceUrl(row.link, { hotelWebsite: website });
          addHitsFromText(snippet, {
            url: row.link || null,
            source_category: cat,
            step: `${q.step}_snippet`,
          });
        }
      } catch (err) {
        metrics.serpapi_errors += 1;
        steps.push({
          step: `${q.step}_exception`,
          message: safeErrorMessage(err).slice(0, 120),
        });
      }
    }
  } else if (!allowSerpapi) {
    steps.push({ step: "serpapi_skipped", reason: "disabled_or_missing_key" });
  }

  // --- Step 6: fetch top eligible pages if still weak ---
  const provisional = scoreRoomCountResearch(observations, {
    identity_confidence: hotel.identity_confidence,
  });
  const needMore =
    !provisional.candidate_room_count ||
    provisional.confidence < 0.85 ||
    provisional.research_status === RESEARCH_STATUS.NO_EVIDENCE ||
    provisional.research_status === RESEARCH_STATUS.CONFLICT;

  if (needMore && organicPool.length) {
    const candidates = selectFetchCandidates(
      organicPool,
      { hotelWebsite: website, hotel_name: name },
      classifySourceUrl,
      isFetchEligibleUrl
    ).filter((c) => c.url !== website);

    let fetched = 0;
    for (const cand of candidates) {
      if (fetched >= Math.max(0, maxPageFetches - (website ? 1 : 0))) break;
      // Skip if we already have high-confidence official agreement
      const mid = scoreRoomCountResearch(observations, {
        identity_confidence: hotel.identity_confidence,
      });
      if (
        mid.candidate_room_count &&
        mid.confidence >= 0.9 &&
        mid.research_status === RESEARCH_STATUS.FOUND_MULTI_SOURCE
      ) {
        break;
      }
      metrics.pages_fetched += 1;
      fetched += 1;
      steps.push({ step: "followup_fetch", url: cand.url, category: cand.category });
      const page = await fetchResearchPage(cand.url);
      if (!page.ok) continue;
      metrics.pages_ok += 1;
      const searchable = htmlToSearchableText(page.text);
      addHitsFromText(page.text.length > searchable.length ? page.text : searchable, {
        url: page.url,
        source_category: cand.category || classifySourceUrl(page.url, { hotelWebsite: website }),
        step: "followup_page",
      });
    }
  }

  const scored = scoreRoomCountResearch(observations, {
    identity_confidence: hotel.identity_confidence,
  });

  // Stage evidence if store provided
  if (opts.evidence && hotelId && scored.supporting_sources?.length) {
    for (const src of scored.supporting_sources) {
      try {
        opts.evidence.addEvidence({
          hotel_id: hotelId,
          field: "room_count",
          value: src.value,
          source: "room_count_research",
          source_record_id: src.url || src.source_category,
          confidence: scored.confidence,
          explanation: `${src.source_category}|${src.quote || ""}`.slice(0, 240),
          observed_at: src.observed_at,
        });
      } catch {
        /* ignore staging errors */
      }
    }
  }

  return {
    ok: true,
    hotel_id: hotelId,
    hotel_name: name || null,
    city: hotel.city || null,
    country: hotel.country || null,
    brand: hotel.brand || null,
    website: website || null,
    candidate_room_count: scored.candidate_room_count,
    confidence: scored.confidence,
    supporting_sources: scored.supporting_sources,
    supporting_quotes: (scored.supporting_sources || [])
      .map((s) => s.quote)
      .filter(Boolean),
    conflicts: scored.conflicts,
    review_required: scored.review_required,
    research_status: scored.research_status,
    metrics: {
      ...metrics,
      observations: observations.length,
      runtime_ms: Date.now() - t0,
    },
    steps,
    version: ROOM_COUNT_RESEARCH_VERSION,
    confidence_model: ROOM_COUNT_CONFIDENCE_VERSION,
    airtable_written: false,
    note: "Research-only; census not mutated. Hotelbeds LIVE may later validate agreeing counts.",
  };
}

export { RESEARCH_STATUS, ROOM_COUNT_CONFIDENCE_VERSION };
