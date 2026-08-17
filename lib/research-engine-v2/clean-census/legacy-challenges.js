/**
 * Legacy-only challenge classes — post-freeze only.
 */

import { randomUUID } from "node:crypto";
import { tokenSimilarity } from "../adapters/adapter-utils.js";
import { RESEARCH_MODES_CLEAN } from "./provenance.js";

function normName(name) {
  return String(name || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\b(hotel|the|a|an|by|and)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Strict Independent Rediscovery — no legacy hotel identity supplied to research.
 * Uses only geography + brand family inventory already independently discovered.
 *
 * @param {object[]} legacyOnlyRows
 * @param {object[]} independentDirectoryRows - from frozen wave discovery
 * @param {object} firewall
 */
export function runStrictIndependentRediscovery(legacyOnlyRows, independentDirectoryRows, firewall) {
  firewall.beginLegacyOnlyChallenge();

  // Strict mode: we do NOT pass legacy names into discovery.
  // We only mark whether the frozen independent universe already covers inventory density.
  return (legacyOnlyRows || []).map((row) => {
    const challenge_id = `chal_strict_${randomUUID().slice(0, 10)}`;
    return {
      challenge_id,
      challenge_class: "Strict Independent Rediscovery",
      counts_toward_strongest_independent_standard: true,
      legacy_hotel_id_quarantined: row.legacy_hotel_id,
      // Intentionally omit legacy_name from research inputs
      research_inputs: {
        geography: row.legacy_country || "Mexico",
        brand_family: "ihg",
        independent_inventory_size: (independentDirectoryRows || []).length,
        legacy_identity_supplied: false,
      },
      challenge_result:
        "Strict mode does not use legacy identity. Steward compares post-hoc only after independent inventory freeze.",
      recommended_action:
        "Expand official directory / operator portfolio / tourism registry discovery for geography+family; escalate unresolved",
      unresolved: true,
      legacy_used_as_source: false,
      research_mode: RESEARCH_MODES_CLEAN.LEGACY_ONLY_CHALLENGE,
      post_hoc_bookkeeping_only: true,
    };
  });
}

/**
 * Targeted Verification — legacy identity may be used AFTER freeze for matching bookkeeping,
 * but production claims still require independent evidence.
 *
 * @param {object[]} legacyOnlyRows
 * @param {object[]} independentDirectoryRows
 * @param {object} firewall
 */
export function runTargetedVerificationChallenges(legacyOnlyRows, independentDirectoryRows, firewall) {
  firewall.beginLegacyOnlyChallenge();

  return (legacyOnlyRows || []).map((row) => {
    const challenge_id = `chal_targeted_${randomUUID().slice(0, 10)}`;
    const legacyName = row.legacy_name;
    let best = null;
    for (const d of independentDirectoryRows || []) {
      const score = tokenSimilarity(normName(legacyName), normName(d.name || d.inferredHotelName));
      if (!best || score > best.score) best = { row: d, score };
    }

    if (best && best.score >= 0.55) {
      return {
        challenge_id,
        challenge_class: "Targeted Verification",
        counts_toward_strongest_independent_standard: false,
        note: "Identity match used for steward bookkeeping only; adopt fields only from independent evidence",
        legacy_hotel_id: row.legacy_hotel_id,
        legacy_name_for_match_only: legacyName,
        challenge_result: "Directory identity match found post-freeze",
        challenge_evidence: {
          source: "official_directory_inventory",
          propertyUrl: best.row.propertyUrl,
          directoryName: best.row.name,
          match_score: best.score,
        },
        recommended_action: "Create/link independent record from directory evidence — do not copy legacy fields",
        unresolved: false,
        legacy_used_as_source: false,
        research_mode: RESEARCH_MODES_CLEAN.LEGACY_ONLY_CHALLENGE,
      };
    }

    return {
      challenge_id,
      challenge_class: "Targeted Verification",
      counts_toward_strongest_independent_standard: false,
      legacy_hotel_id: row.legacy_hotel_id,
      legacy_name_for_match_only: legacyName,
      challenge_result: "No independent directory confirmation",
      recommended_action: "Escalate — Native retry / Human review / Webhound candidate (explicit auth)",
      unresolved: true,
      legacy_used_as_source: false,
      research_mode: RESEARCH_MODES_CLEAN.LEGACY_ONLY_CHALLENGE,
    };
  });
}

export const CHALLENGE_CLASS_RECOMMENDATION = Object.freeze({
  strongest_standard: "Strict Independent Rediscovery",
  steward_efficiency: "Targeted Verification (post-freeze only)",
  rule: "Only Strict challenges count toward the strongest independent-creation standard",
});
