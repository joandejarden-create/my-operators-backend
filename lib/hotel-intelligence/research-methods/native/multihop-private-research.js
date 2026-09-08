/**
 * Bounded multi-hop private ownership research (Packet 2.4F).
 * Hotel → pivot entity → parent/fund/portfolio — with hop/cost ceilings.
 * Does NOT hard-code hotel-specific final answers.
 */

import { serpapiSearch } from "../../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import { fetchResearchPage, htmlToSearchableText } from "../../room-count-research/fetch.js";
import { classifySourceClass, extractPrivateOpaqueCandidates, triangulatePrivateOwnership } from "./private-opaque-lanes.js";
import { identityAliasesForSeed } from "./seed-scoped-extractor.js";

const SERPAPI_USD = 0.01;
const DEFAULT_MAX_HOPS = 3;
const DEFAULT_MAX_QUERIES = 12;
const DEFAULT_MAX_USD = 0.25;

/**
 * Build multi-hop query families from hotel seed + optional pivot entities.
 */
export function buildMultihopQueryFamilies(seed, pivots = []) {
  const name = String(seed.name || "").trim();
  const city = String(seed.city || "").trim();
  const brand = String(seed.affiliation_display || "").trim();
  const qs = [];
  qs.push({ hop: 0, lane: "IDENTITY", q: `"${name}" ${city} hotel -"Las Ventanas"` });
  // Historical alias only when Pedregal family — otherwise skip hard-coded alias
  if (/pedregal/i.test(name)) {
    qs.push({ hop: 0, lane: "HISTORICAL_ALIAS", q: `"The Resort at Pedregal" ${city} owner OR developer OR acquisition` });
  }
  qs.push({ hop: 0, lane: "DEVELOPER", q: `"${name}" ${city} developer OR desarrollador OR sponsor` });
  qs.push({ hop: 0, lane: "TRANSACTION", q: `"${name}" ${city} acquisition OR acquired OR sold OR compró` });
  qs.push({ hop: 0, lane: "FINANCING", q: `"${name}" ${city} financing OR loan OR crédito OR borrower` });
  qs.push({ hop: 0, lane: "LAWFIRM", q: `"${name}" ${city} (law OR abogado OR "legal counsel" OR tombstone) (acquisition OR sale)` });
  qs.push({ hop: 0, lane: "FUND", q: `"${name}" ${city} (fund OR capital OR sponsor OR investor) hotel` });
  if (brand) qs.push({ hop: 0, lane: "BRAND_SIGNING", q: `"${name}" ${brand} announce OR opening OR developer` });
  for (const p of pivots.slice(0, 4)) {
    const ent = String(p).trim();
    if (ent.length < 3) continue;
    qs.push({ hop: 1, lane: "PIVOT_PORTFOLIO", q: `"${ent}" (portfolio OR hoteles OR resorts) Mexico OR Cabo OR Pedregal` });
    qs.push({ hop: 1, lane: "PIVOT_PARENT", q: `"${ent}" (parent OR subsidiary OR affiliate OR fund)` });
    qs.push({ hop: 2, lane: "PIVOT_HOTEL_LINK", q: `"${ent}" "${name}"` });
  }
  return qs;
}

/**
 * Run bounded multi-hop native research.
 * @returns {Promise<object>}
 */
export async function runBoundedMultihopResearch(seed, opts = {}) {
  const maxHops = opts.maxHops ?? DEFAULT_MAX_HOPS;
  const maxQueries = opts.maxQueries ?? DEFAULT_MAX_QUERIES;
  const maxUsd = opts.maxUsd ?? DEFAULT_MAX_USD;
  const cost = { serpapi_searches: 0, serpapi_usd: 0, fetches: 0, total_usd_estimate: 0 };
  const pathway = [];
  const rejected = [];
  const docs = [];
  const pivots = [];

  let queries = buildMultihopQueryFamilies(seed, opts.seedPivots || []);
  // If playbook provides extra query templates
  for (const extra of opts.extraQueries || []) {
    queries.push({ hop: 0, lane: extra.lane || "CUSTOM", q: extra.q });
  }
  queries = queries.slice(0, maxQueries);

  for (const fam of queries) {
    if (cost.serpapi_usd >= maxUsd) {
      pathway.push({ stop: "cost_ceiling", cost });
      break;
    }
    if (fam.hop > maxHops) continue;
    try {
      const res = await serpapiSearch({ engine: "google", q: fam.q, num: 8, gl: "mx", hl: "es" });
      cost.serpapi_searches += 1;
      cost.serpapi_usd += Number(res?.creditsCharged ?? 1) * SERPAPI_USD;
      const hits = [];
      if (res?.ok) {
        for (const r of res.data?.organic_results || []) {
          if (!r.link) continue;
          const sc = classifySourceClass(r.link, r.title || "");
          // Reject obvious collision pages for Pedregal-like Cabo research
          if (/las\s+ventanas/i.test(`${r.title} ${r.link}`) && /pedregal/i.test(seed.name || "")) {
            rejected.push({ url: r.link, reason: "SIMILAR_LUXURY_PROPERTY_COLLISION", lane: fam.lane });
            continue;
          }
          hits.push({ url: r.link, title: r.title || "", source_class: sc });
        }
      }
      pathway.push({ hop: fam.hop, lane: fam.lane, q: fam.q, hits: hits.slice(0, 5) });
      // Fetch press/court/brand first; also fetch "debut/acquisition" titles even if mis-scored as OTA
      for (const h of hits
        .filter(
          (x) =>
            x.source_class !== "hotel_or_ota" ||
            /debut|acqui|owner|capital|conversion|rebrand/i.test(x.title || "")
        )
        .slice(0, 3)) {
        if (docs.length >= 10) break;
        try {
          const page = await fetchResearchPage(h.url, { timeoutMs: 18000 });
          cost.fetches += 1;
          if (page.ok) {
            const text = htmlToSearchableText(page.text || "");
            docs.push({ ok: true, url: page.url || h.url, text, title: h.title, source_class: h.source_class, lane: fam.lane });
            // Discover pivot orgs from ORG_MENTION patterns
            const extract = extractPrivateOpaqueCandidates([docs[docs.length - 1]], seed);
            for (const c of extract.candidates) {
              if (/ORG_MENTION|DEVELOPED_BY|OWNED_BY|SPONSORED_BY|OWNED_BY_TRANSACTION/i.test(c.relationship)) {
                if (!pivots.includes(c.entity) && c.entity.length < 80) pivots.push(c.entity);
              }
            }
          }
        } catch {
          rejected.push({ url: h.url, reason: "fetch_failed", lane: fam.lane });
        }
      }
    } catch (err) {
      pathway.push({ hop: fam.hop, lane: fam.lane, q: fam.q, error: String(err?.message || err) });
    }
  }

  // Second-wave hops if pivots found and budget remains
  if (pivots.length && cost.serpapi_usd < maxUsd) {
    const hop2 = buildMultihopQueryFamilies(seed, pivots).filter((q) => q.hop >= 1).slice(0, 4);
    for (const fam of hop2) {
      if (cost.serpapi_usd >= maxUsd) break;
      try {
        const res = await serpapiSearch({ engine: "google", q: fam.q, num: 6, gl: "mx", hl: "es" });
        cost.serpapi_searches += 1;
        cost.serpapi_usd += Number(res?.creditsCharged ?? 1) * SERPAPI_USD;
        pathway.push({
          hop: fam.hop,
          lane: fam.lane,
          q: fam.q,
          hits: (res?.data?.organic_results || []).slice(0, 4).map((r) => ({ url: r.link, title: r.title })),
        });
        for (const r of (res?.data?.organic_results || []).slice(0, 2)) {
          if (!r.link || docs.length >= 14) continue;
          if (/las\s+ventanas/i.test(`${r.title} ${r.link}`) && /pedregal/i.test(seed.name || "")) {
            rejected.push({ url: r.link, reason: "SIMILAR_LUXURY_PROPERTY_COLLISION", lane: fam.lane });
            continue;
          }
          const page = await fetchResearchPage(r.link, { timeoutMs: 16000 });
          cost.fetches += 1;
          if (page.ok) {
            docs.push({
              ok: true,
              url: page.url || r.link,
              text: htmlToSearchableText(page.text || ""),
              title: r.title || "",
              source_class: classifySourceClass(r.link, r.title || ""),
              lane: fam.lane,
            });
          }
        }
      } catch {
        /* continue */
      }
    }
  }

  cost.total_usd_estimate = Number(cost.serpapi_usd.toFixed(4));
  const privateExtract = extractPrivateOpaqueCandidates(docs, seed);
  const triangulated = triangulatePrivateOwnership(privateExtract.candidates, seed);

  return {
    playbook_id: "MX-MULTIHOP-PRIVATE-01",
    max_hops: maxHops,
    cost,
    pathway,
    pivots_discovered: pivots,
    sources_rejected: rejected,
    docs: docs.map((d) => ({ url: d.url, source_class: d.source_class, lane: d.lane, text_len: (d.text || "").length })),
    candidates: privateExtract.candidates,
    triangulation: triangulated,
    aliases_used: identityAliasesForSeed(seed),
  };
}
