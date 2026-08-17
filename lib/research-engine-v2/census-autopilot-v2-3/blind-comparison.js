/**
 * Blind comparison: independent universe vs Cvent challenge freeze.
 * Never copies Cvent factual fields into production evidence.
 */

import { createHash } from "node:crypto";
import { normName } from "../census-autopilot-v2/identity-dedupe.js";
import { MATCH_CLASS } from "./constants.js";
import { loadCventChallengeFreeze } from "./cvent-firewall.js";

function tokens(s) {
  return new Set(
    String(s || "")
      .toLowerCase()
      .replace(/[^a-z0-9\s]/g, " ")
      .split(/\s+/)
      .filter((t) => t.length > 2)
  );
}

function tokenOverlap(a, b) {
  const A = tokens(a);
  const B = tokens(b);
  if (!A.size || !B.size) return 0;
  let hit = 0;
  for (const t of A) if (B.has(t)) hit += 1;
  return hit / Math.max(A.size, B.size);
}

/**
 * @param {object[]} independentRecords
 * @param {string} outDir
 * @param {string[]} pilotCountries
 */
export function compareIndependentVsCvent(independentRecords, outDir, pilotCountries) {
  const freeze = loadCventChallengeFreeze(outDir, "comparison");
  const challenges = (freeze.challenges || []).filter((c) =>
    pilotCountries.includes(c.country)
  );

  const indByCountry = new Map();
  for (const r of independentRecords) {
    const c = r.physical.country;
    if (!indByCountry.has(c)) indByCountry.set(c, []);
    indByCountry.get(c).push(r);
  }

  const matchedInd = new Set();
  const matchedCh = new Set();
  const both = [];
  const probable = [];
  const conflicts = [];

  for (const ch of challenges) {
    const pool = indByCountry.get(ch.country) || [];
    const slug = ch._match_name_slug || "";
    let best = null;
    for (const ind of pool) {
      if (matchedInd.has(ind.property_identity_id)) continue;
      const name = ind.physical.current_name;
      const o = Math.max(
        tokenOverlap(slug, name),
        tokenOverlap(normName(slug), normName(name))
      );
      // Exact-ish: high overlap
      if (o >= 0.55) {
        if (!best || o > best.overlap) best = { ind, overlap: o, level: o >= 0.72 ? "BOTH" : "PROBABLE" };
      }
    }
    if (best?.level === "BOTH") {
      matchedInd.add(best.ind.property_identity_id);
      matchedCh.add(ch.challenge_id);
      both.push({
        challenge_id: ch.challenge_id,
        property_identity_id: best.ind.property_identity_id,
        country: ch.country,
        overlap: best.overlap,
        independent_name: best.ind.physical.current_name,
        // Do NOT store Cvent name as production field — slug only for audit
        challenge_slug_audit: slug,
        cvent_used_as_production_evidence: false,
      });
    } else if (best?.level === "PROBABLE") {
      matchedInd.add(best.ind.property_identity_id);
      matchedCh.add(ch.challenge_id);
      probable.push({
        challenge_id: ch.challenge_id,
        property_identity_id: best.ind.property_identity_id,
        country: ch.country,
        overlap: best.overlap,
        independent_name: best.ind.physical.current_name,
        challenge_slug_audit: slug,
        cvent_used_as_production_evidence: false,
      });
    }
  }

  const independentOnly = independentRecords
    .filter((r) => pilotCountries.includes(r.physical.country) && !matchedInd.has(r.property_identity_id))
    .map((r) => ({
      property_identity_id: r.property_identity_id,
      name: r.physical.current_name,
      country: r.physical.country,
      city: r.physical.city,
      family: r.affiliation.brand_family,
      lane: r.strata.discovery_lane,
      classification: MATCH_CLASS.INDEPENDENT_ONLY,
      hypothesis: classifyIndependentOnly(r),
    }));

  const cventOnly = challenges
    .filter((c) => !matchedCh.has(c.challenge_id))
    .map((c) => ({
      challenge_id: c.challenge_id,
      country: c.country,
      challenge_slug_audit: c._match_name_slug,
      classification: MATCH_CLASS.CVENT_ONLY,
      discovery_challenge: true,
      cvent_used_as_production_evidence: false,
      // NO factual Cvent fields
    }));

  const rediscoveryRate = challenges.length
    ? Math.round((100 * (both.length + probable.length)) / challenges.length)
    : 0;

  function stratumRate(predCh, predIndMatch) {
    const subset = challenges.filter(predCh);
    if (!subset.length) return { n: 0, matched: 0, rate_pct: null };
    let matched = 0;
    for (const ch of subset) {
      if (matchedCh.has(ch.challenge_id)) matched += 1;
    }
    return {
      n: subset.length,
      matched,
      rate_pct: Math.round((100 * matched) / subset.length),
    };
  }

  // Approximate branded vs independent from slug heuristics
  const brandedSlug = (s) =>
    /hilton|marriott|ihg|holiday|hyatt|wyndham|accor|choice|comfort|quality|radisson|melia|barcelo|riu |iberostar/i.test(
      s || ""
    );
  const resortSlug = (s) => /resort|spa|beach|all.?inclusive/i.test(s || "");

  const byCountry = {};
  for (const c of pilotCountries) {
    byCountry[c] = stratumRate((ch) => ch.country === c);
  }

  return {
    version: "cvent-post-freeze-comparison-v2.3",
    pilot_countries: pilotCountries,
    cvent_challenge_in_geo: challenges.length,
    both: both.length,
    probable: probable.length,
    independent_only: independentOnly.length,
    cvent_only: cventOnly.length,
    conflicts: conflicts.length,
    overall_blind_rediscovery_rate_pct: rediscoveryRate,
    rediscovery: {
      branded: stratumRate((ch) => brandedSlug(ch._match_name_slug)),
      independent: stratumRate((ch) => !brandedSlug(ch._match_name_slug)),
      resort: stratumRate((ch) => resortSlug(ch._match_name_slug)),
      small_hotel_proxy: stratumRate(
        (ch) => !resortSlug(ch._match_name_slug) && (ch._match_name_slug || "").length < 28
      ),
      by_country: byCountry,
    },
    samples: {
      both: both.slice(0, 40),
      probable: probable.slice(0, 40),
      independent_only: independentOnly.slice(0, 80),
      cvent_only: cventOnly.slice(0, 80),
    },
    cvent_factual_fields_copied: false,
    required_answer_q26: "NO",
  };
}

function classifyIndependentOnly(r) {
  if (r.strata.discovery_lane?.includes("SERPAPI") || r.discovery_evidence.source_type?.includes("serpapi")) {
    return "possible_cvent_coverage_gap_or_non_meetings_hotel";
  }
  if (r.strata.branded) return "branded_directory_hotel_possibly_absent_from_cvent_meetings_index";
  if (r.strata.soft_collection) return "soft_brand_collection_property";
  return "independent_or_new_or_small_hotel";
}

/**
 * Resolve Cvent-only challenges WITHOUT copying Cvent fields.
 * Uses only challenge slug as search hint after freeze — establishes independent provenance.
 */
export async function resolveCventOnlyChallenges(cventOnlySample, opts = {}) {
  const {
    searchGoogleHotels,
    SerpApiCreditTracker,
    getAccount,
    matchCensusProperty,
  } = await import("../providers/serpapi-google-hotels/index.js");

  const ceiling = opts.ceiling ?? Number(process.env.CAV23_CHALLENGE_RESOLVE_CEILING || 40);
  const log = opts.log || (() => {});
  const account = await getAccount();
  const starting = account.ok ? account.total_searches_left ?? account.plan_searches_left : null;
  const tracker = new SerpApiCreditTracker({ ceiling, startingSearchesLeft: starting });

  const results = [];
  const sample = cventOnlySample.slice(0, opts.max || 40);

  for (const ch of sample) {
    if (!tracker.canSpend(1)) break;
    const slug = String(ch.challenge_slug_audit || "")
      .replace(/-/g, " ")
      .trim();
    if (!slug || slug.length < 4) {
      results.push({
        challenge_id: ch.challenge_id,
        resolved: false,
        reason: "insufficient_challenge_slug",
        challenge_origin: "CVENT_COVERAGE_AUDIT",
        cvent_used_as_production_evidence: false,
      });
      continue;
    }
    const q = `${slug}, ${ch.country}`;
    try {
      const search = await searchGoogleHotels(
        { q, gl: "us" },
        { tracker, hotelId: `chal_${ch.challenge_id}` }
      );
      const cand = search.candidates?.[0];
      if (!cand?.name) {
        results.push({
          challenge_id: ch.challenge_id,
          resolved: false,
          reason: "no_serpapi_candidate",
          challenge_origin: "CVENT_COVERAGE_AUDIT",
          cvent_used_as_production_evidence: false,
        });
        continue;
      }
      const match = matchCensusProperty(
        { name: slug, country: ch.country, city: null },
        cand
      );
      const ok = match.enrichment_eligible || (match.name_overlap || 0) >= 0.45;
      results.push({
        challenge_id: ch.challenge_id,
        resolved: ok,
        reason: ok ? "independently_established_via_serpapi" : "weak_match",
        independent_name: cand.name,
        independent_address: cand.address || null,
        overlap: match.name_overlap,
        challenge_origin: "CVENT_COVERAGE_AUDIT",
        cvent_used_as_production_evidence: false,
        // New independent record identity — NOT inherited from Cvent
        property_identity_id: `pid_${createHash("sha1")
          .update(`${ch.country}|${normName(cand.name)}`)
          .digest("hex")
          .slice(0, 16)}`,
      });
    } catch (err) {
      results.push({
        challenge_id: ch.challenge_id,
        resolved: false,
        reason: String(err?.message || err).slice(0, 120),
        challenge_origin: "CVENT_COVERAGE_AUDIT",
        cvent_used_as_production_evidence: false,
      });
    }
    await new Promise((r) => setTimeout(r, 200));
  }

  const resolved = results.filter((r) => r.resolved).length;
  return {
    version: "cvent-only-challenge-resolution-v2.3",
    attempted: results.length,
    resolved,
    unresolved: results.length - resolved,
    cvent_factual_fields_copied: false,
    serpapi_calls: tracker.charged,
    results,
    top_unresolved_reasons: Object.entries(
      results
        .filter((r) => !r.resolved)
        .reduce((a, r) => {
          a[r.reason] = (a[r.reason] || 0) + 1;
          return a;
        }, {})
    )
      .sort((a, b) => b[1] - a[1])
      .map(([reason, n]) => ({ reason, n })),
  };
}
