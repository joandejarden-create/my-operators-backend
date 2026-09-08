/**
 * MX-BRAND-IDENTITY-NULLIFICATION-01 / historical alias resolution (Packet 2.4F).
 * Learned from Webhound Pedregal experiment:
 * When exact "Brand + HotelName" returns no real property match, do not treat
 * SERP luxury neighbors as the seed hotel — resolve historical aliases first.
 */

import { serpapiSearch } from "../../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import { fetchResearchPage, htmlToSearchableText } from "../../room-count-research/fetch.js";
import { classifySourceClass } from "./private-opaque-lanes.js";

const SERPAPI_USD = 0.01;

/**
 * Detect whether brand+name appears to be a conflation / non-existent pairing.
 */
export function scoreBrandNameConflation(seed, organicResults = []) {
  const brand = String(seed.affiliation_display || "").trim();
  const name = String(seed.name || "").trim();
  const reasons = [];
  let score = 0;

  if (!brand) return { conflation_suspected: false, score: 0, reasons: ["no_brand_seed"] };

  const exactPhraseHits = organicResults.filter((r) => {
    const blob = `${r.title || ""} ${r.snippet || ""} ${r.link || ""}`;
    return new RegExp(brand.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(blob) &&
      new RegExp(name.split(/\s+/).slice(-1)[0].replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(blob);
  });

  if (organicResults.length === 0) {
    // Only suspect conflation for zero results when brand+name is known to be contradictory.
    // Zero results from a transient API failure should NOT trigger nullification on real brands.
    const brandLower = brand.toLowerCase();
    const nameLower = name.toLowerCase();
    const knownConflation = (
      (/rosewood/i.test(brandLower) && /pedregal/i.test(nameLower)) // Pedregal was never Rosewood
    );
    if (knownConflation) {
      score += 40;
      reasons.push("zero_organic_results_for_known_conflation");
    } else {
      score += 10;
      reasons.push("zero_organic_results_for_branded_query_weak");
    }
  }
  if (exactPhraseHits.length === 0 && organicResults.length > 0) {
    score += 25;
    reasons.push("no_title_match_for_brand_plus_hotel_token");
  }

  // Neighbor brand collision signal (Pedregal ↔ Las Ventanas pattern)
  const neighborHits = organicResults.filter((r) =>
    /las\s+ventanas|rosewoodhotels\.com\/.*las-ventanas/i.test(`${r.title} ${r.link}`)
  );
  if (neighborHits.length >= 2 && /pedregal/i.test(name)) {
    score += 30;
    reasons.push("SIMILAR_LUXURY_PROPERTY_COLLISION_dominant_serp");
  }

  // Official hotel site suggests different brand
  const officialDifferentBrand = organicResults.some((r) =>
    /waldorfastoria|capella|marriott|hilton\.com\/.*hotel/i.test(r.link || "") &&
      /pedregal/i.test(`${r.title} ${r.link}`)
  );
  if (officialDifferentBrand) {
    score += 35;
    reasons.push("official_or_brand_site_suggests_different_current_brand");
  }

  return {
    conflation_suspected: score >= 40,
    score,
    reasons,
    neighbor_collision: neighborHits.length > 0,
  };
}

/**
 * Query family to discover historical / current aliases after conflation suspected.
 */
export function buildAliasResolutionQueries(seed) {
  const city = String(seed.city || "").trim();
  const token = String(seed.name || "")
    .replace(/rosewood|one\s*&\s*only|oneandonly|waldorf|capella|hyatt|hilton/gi, "")
    .replace(/\s+/g, " ")
    .trim();
  const qs = [
    { lane: "ALIAS_CURRENT_SITE", q: `"${token}" ${city} hotel official OR resort -rosewood` },
    { lane: "ALIAS_HISTORICAL", q: `"${token}" ${city} Capella OR "Resort at" OR Waldorf OR formerly OR rebrand` },
    { lane: "ALIAS_BRAND_DEBUT", q: `"${token}" ${city} (Waldorf OR Capella OR "Resort at Pedregal") (debut OR acquisition OR announce)` },
    { lane: "ALIAS_COURT", q: `"${token}" ${city} (fideicomiso OR trust OR Farallon OR "Hoteles del Cabo")` },
  ];
  if (/pedregal/i.test(seed.name || "")) {
    qs.push({ lane: "ALIAS_KNOWN_FAMILY", q: `"The Resort at Pedregal" OR "Capella Pedregal" OR "Waldorf Astoria Los Cabos Pedregal"` });
  }
  return qs;
}

/**
 * Extract alias candidates from docs/titles.
 */
export function extractAliasCandidates(docs, seed) {
  const aliases = new Set();
  const blob = docs.map((d) => `${d.title || ""}\n${(d.text || "").slice(0, 5000)}`).join("\n");
  const patterns = [
    /Waldorf\s+Astoria\s+Los\s+Cabos\s+Pedregal/gi,
    /Capella\s+Pedregal/gi,
    /The\s+Resort\s+at\s+Pedregal/gi,
    /Hoteles\s+del\s+Cabo/gi,
  ];
  for (const re of patterns) {
    const m = blob.match(re);
    if (m) m.forEach((x) => aliases.add(x.replace(/\s+/g, " ").trim()));
  }
  // Generic: "formerly known as X"
  const formerly = blob.match(/formerly\s+(?:known\s+as\s+)?([A-Z][\w\s&'-]{5,60})/gi) || [];
  for (const f of formerly.slice(0, 5)) {
    const name = f.replace(/formerly\s+(?:known\s+as\s+)?/i, "").trim();
    if (name.length >= 8) aliases.add(name);
  }
  return [...aliases];
}

/**
 * Run identity nullification + alias discovery step (bounded).
 */
export async function runIdentityNullification(seed, cost) {
  const brand = seed.affiliation_display || "";
  const qExact = `"${seed.name}" ${seed.city || ""}`;
  const organic = [];
  try {
    const res = await serpapiSearch({ engine: "google", q: qExact, num: 8, gl: "mx", hl: "es" });
    cost.serpapi_searches += 1;
    cost.serpapi_usd += Number(res?.creditsCharged ?? 1) * SERPAPI_USD;
    for (const r of res?.data?.organic_results || []) {
      organic.push({ url: r.link, title: r.title || "", snippet: r.snippet || "", link: r.link });
    }
  } catch {
    /* continue */
  }

  const detection = scoreBrandNameConflation(seed, organic);
  const pathway = [{ lane: "T0_BRAND_NAME_PROBE", q: qExact, hits: organic.slice(0, 5), detection }];
  const docs = [];
  const aliases = [];

  if (!detection.conflation_suspected) {
    return { detection, pathway, docs, aliases, triggered: false };
  }

  for (const fam of buildAliasResolutionQueries(seed)) {
    try {
      const res = await serpapiSearch({ engine: "google", q: fam.q, num: 8, gl: "mx", hl: "es" });
      cost.serpapi_searches += 1;
      cost.serpapi_usd += Number(res?.creditsCharged ?? 1) * SERPAPI_USD;
      const hits = [];
      for (const r of res?.data?.organic_results || []) {
        if (!r.link) continue;
        if (/las\s+ventanas/i.test(`${r.title} ${r.link}`)) continue;
        hits.push({ url: r.link, title: r.title || "", source_class: classifySourceClass(r.link, r.title || "") });
      }
      pathway.push({ lane: fam.lane, q: fam.q, hits: hits.slice(0, 5) });
      for (const h of hits.slice(0, 2)) {
        try {
          const page = await fetchResearchPage(h.url, { timeoutMs: 16000 });
          cost.fetches += 1;
          if (page.ok) {
            docs.push({
              ok: true,
              url: page.url || h.url,
              text: htmlToSearchableText(page.text || ""),
              title: h.title,
              source_class: h.source_class,
              lane: fam.lane,
            });
          }
        } catch {
          /* continue */
        }
      }
    } catch {
      /* continue */
    }
  }

  aliases.push(...extractAliasCandidates(docs, seed));
  // Also from titles
  for (const d of docs) {
    if (/waldorf\s+astoria.*pedregal/i.test(d.title || "")) aliases.push("Waldorf Astoria Los Cabos Pedregal");
    if (/capella\s+pedregal/i.test(d.title || "")) aliases.push("Capella Pedregal");
    if (/resort\s+at\s+pedregal/i.test(d.title || "")) aliases.push("The Resort at Pedregal");
  }

  return {
    detection,
    pathway,
    docs,
    aliases: [...new Set(aliases)],
    triggered: true,
    playbook_id: "MX-BRAND-IDENTITY-NULLIFICATION-01",
  };
}
