/**
 * Quarantined coverage challenge adapters — Cvent + Legacy.
 * NEVER feed production claims. Independent rediscovery only.
 */

import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { tokenSimilarity } from "../adapters/adapter-utils.js";

function normName(name) {
  return String(name || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\b(hotel|the|a|an|by|and|resort|inn|suites?)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Extract display candidate from Cvent venue URL (identity for challenge matching only).
 * Does NOT extract rooms/amenities/coords as production evidence.
 */
export function cventCandidateFromUrl(url) {
  const u = String(url || "");
  const m = u.match(/\/venues\/[^/]+\/[^/]+\/([^/]+)\/venue-([a-f0-9-]{36})/i);
  if (!m) {
    return {
      cvent_candidate_id: `cvent_url_${randomUUID().slice(0, 8)}`,
      url: u,
      candidate_name_from_slug: null,
    };
  }
  const slug = decodeURIComponent(m[1]).replace(/-/g, " ");
  return {
    cvent_candidate_id: `cvent_${m[2]}`,
    url: u,
    candidate_name_from_slug: slug,
    candidate_origin_reference: "cvent_latam_harvest",
  };
}

/**
 * Build Cvent → Independent Discovery Challenges.
 * @param {string[]} cventUrls
 * @param {object[]} independentRecords VIC rows
 * @param {object} [opts]
 */
export function buildCventDiscoveryChallenges(cventUrls, independentRecords, opts = {}) {
  const threshold = opts.matchThreshold ?? 0.55;
  const maxChallenges = opts.maxChallenges ?? 500;
  const ind = (independentRecords || []).map((r) => ({
    ...r,
    _n: normName(r.name),
  }));

  const challenges = [];
  let matched = 0;
  let unmatched = 0;

  for (const url of cventUrls || []) {
    if (challenges.length >= maxChallenges) break;
    const cand = cventCandidateFromUrl(url);
    const n = normName(cand.candidate_name_from_slug);
    let best = null;
    if (n) {
      for (const r of ind) {
        const score = tokenSimilarity(n, r._n);
        if (!best || score > best.score) best = { row: r, score };
      }
    }

    if (best && best.score >= threshold) {
      matched += 1;
      challenges.push({
        challenge_type: "CVENT_CANDIDATE_IDENTITY_OVERLAP_BOOKKEEPING",
        cvent_candidate_id: cand.cvent_candidate_id,
        candidate_origin_reference: "cvent_latam_harvest",
        independent_confirmation_status: "Possible identity match — bookkeeping only",
        match_score: Number(best.score.toFixed(3)),
        independent_record_id: best.row.independent_record_id,
        independent_name: best.row.name,
        // Explicitly omit Cvent field values
        cvent_field_values_included: false,
        legacy_used_as_source: false,
        cvent_used_as_source: false,
        production_claim_allowed: false,
        recommended_action: "Do not copy Cvent fields; refresh from Lane A/B only",
      });
    } else {
      unmatched += 1;
      challenges.push({
        challenge_type: "INDEPENDENT DISCOVERY CHALLENGE",
        subtype: "CVENT CANDIDATE NOT FOUND IN VERIFIED INDEPENDENT CENSUS",
        cvent_candidate_id: cand.cvent_candidate_id,
        candidate_origin_reference: "cvent_latam_harvest",
        candidate_name_hint_for_steward_only: cand.candidate_name_from_slug,
        // Name hint is for steward queue matching — research prompts must not include Cvent values
        include_in_research_prompt: false,
        independent_confirmation_status: "Not independently confirmed",
        legacy_used_as_source: false,
        cvent_used_as_source: false,
        production_claim_allowed: false,
        recommended_action:
          "Attempt independent rediscovery via official directories / property sources for geography; escalate if unresolved",
      });
    }
  }

  return {
    version: "census-autopilot-v1-cvent-challenge-adapter",
    role: "COVERAGE CHALLENGE SOURCE ONLY",
    cvent_used_as_source: false,
    urls_considered: (cventUrls || []).length,
    challenges_emitted: challenges.length,
    identity_overlap_bookkeeping: matched,
    independent_discovery_challenges: unmatched,
    challenges,
  };
}

/**
 * Load hotel URLs from a Cvent country harvest JSON (cache).
 * @param {string} harvestPath
 */
export function loadCventHarvestUrls(harvestPath) {
  if (!fs.existsSync(harvestPath)) return [];
  const j = JSON.parse(fs.readFileSync(harvestPath, "utf8"));
  return Array.isArray(j.urls) ? j.urls : [];
}

/**
 * Find Mexico harvest path under reports/cvent-venue-cache/country-results.
 * @param {string} root
 */
export function findMexicoCventHarvest(root) {
  const dir = path.join(root, "reports/cvent-venue-cache/country-results");
  if (!fs.existsSync(dir)) return null;
  for (const f of fs.readdirSync(dir)) {
    if (!f.startsWith("harvest-")) continue;
    const fp = path.join(dir, f);
    try {
      const j = JSON.parse(fs.readFileSync(fp, "utf8"));
      if (j.country === "Mexico") return fp;
    } catch {
      /* skip */
    }
  }
  return null;
}

/**
 * Legacy-only → Independent Discovery Challenge (reuses clean-census pattern).
 * @param {object[]} legacyOnlyRows
 * @param {object[]} independentRecords
 */
export function buildLegacyDiscoveryChallenges(legacyOnlyRows, independentRecords) {
  const ind = (independentRecords || []).map((r) => ({ ...r, _n: normName(r.name) }));
  const challenges = [];

  for (const row of legacyOnlyRows || []) {
    const n = normName(row.legacy_name || row.name);
    let best = null;
    for (const r of ind) {
      const score = tokenSimilarity(n, r._n);
      if (!best || score > best.score) best = { row: r, score };
    }
    if (best && best.score >= 0.55) {
      challenges.push({
        challenge_type: "LEGACY_IDENTITY_OVERLAP_BOOKKEEPING",
        candidate_origin_reference: "legacy_census_quarantine",
        legacy_hotel_id: row.legacy_hotel_id || null,
        independent_record_id: best.row.independent_record_id,
        match_score: Number(best.score.toFixed(3)),
        legacy_used_as_source: false,
        cvent_used_as_source: false,
        production_claim_allowed: false,
        recommended_action: "Link bookkeeping only; adopt fields from independent evidence only",
      });
    } else {
      challenges.push({
        challenge_type: "INDEPENDENT DISCOVERY CHALLENGE",
        subtype: "LEGACY-ONLY CANDIDATE",
        candidate_origin_reference: "legacy_census_quarantine",
        legacy_hotel_id: row.legacy_hotel_id || null,
        legacy_name_for_steward_match_only: row.legacy_name || row.name || null,
        include_in_research_prompt: false,
        independent_confirmation_status: "Not independently confirmed",
        legacy_used_as_source: false,
        cvent_used_as_source: false,
        production_claim_allowed: false,
        recommended_action: "Strict independent rediscovery for geography+family; never direct insert",
      });
    }
  }

  return {
    version: "census-autopilot-v1-legacy-challenge-adapter",
    role: "COVERAGE CHALLENGE SOURCE ONLY",
    legacy_used_as_source: false,
    challenges_emitted: challenges.length,
    challenges,
  };
}

/**
 * Synthetic legacy-only challenge set from prior Mexico overlap summary counts
 * when raw legacy rows are not loaded (no production evidence).
 */
export function buildLegacyChallengeSummaryFromOverlap(overlapSummary) {
  const families = [];
  for (const [family, stats] of Object.entries(overlapSummary || {})) {
    families.push({
      family,
      legacy_only_count: stats.legacy_only || 0,
      independent_only: stats.independent_only || 0,
      exact_matches: stats.exact_matches || 0,
      challenge_policy: "LEGACY-ONLY → INDEPENDENT DISCOVERY CHALLENGE; never direct insert",
      legacy_used_as_source: false,
      cvent_used_as_source: false,
    });
  }
  return {
    version: "census-autopilot-v1-legacy-challenge-adapter-summary",
    role: "COVERAGE CHALLENGE SOURCE ONLY",
    families,
    total_legacy_only: families.reduce((s, f) => s + f.legacy_only_count, 0),
  };
}
