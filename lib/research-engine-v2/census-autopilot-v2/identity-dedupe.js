/**
 * Property identity + dedupe before paid research.
 * Never merge on fuzzy name alone across countries.
 */

import { createHash } from "node:crypto";
import { tokenSimilarity, tokenize } from "../adapters/adapter-utils.js";
import { CLASSIFICATION, CANDIDATE_ORIGINS } from "./constants.js";
import {
  comparePostalIdentitySignal,
  applyPostalToIdentityScore,
} from "../census-postal-code-v1.js";

export function normName(name) {
  return String(name || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\b(hotel|the|a|an|by|and|resort|inn|suites?|spa)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function inferBrandFamily(name) {
  const n = String(name || "").toLowerCase();
  if (/holiday inn|crowne plaza|intercontinental|staybridge|candlewood|even hotels|voco|avid |kimpton|indigo|regent|six senses|garner|atwell|hualuxe|iberostar|joia /.test(n))
    return "IHG";
  if (/hilton|hampton|doubletree|embassy suites|homewood|home2|tru by|canopy|tempo |spark by|lxr |waldorf|conrad|curio|tapestry|signia/.test(n))
    return "Hilton";
  if (/comfort inn|comfort suites|quality inn|clarion|econo lodge|rodeway|sleep inn|mainstay|suburban|ascend |cambria|radisson/.test(n))
    return "Choice";
  if (/marriott|sheraton|westin|w hotel|st\.?\s*regis|ritz.?carlton|autograph|tribute|design hotels|aloft|element|moxy|courtyard|residence inn|springhill|fairfield|four points|le m[eé]ridien|delta hotels|gaylord|jw marriott|edition|protea|ac hotel/.test(n))
    return "Marriott";
  if (/ibis |novotel|mercure|pullman|sofitel|fairmont|swiss[oô]tel|m[oö]venpick|raffles|mgallery|grand mercure|adagio|tribe |handwritten/.test(n))
    return "Accor";
  if (/wyndham|ramada|days inn|super 8|la quinta|travelodge|baymont|microtel|tryp |dolce |registry collection|esplendor|dazzler/.test(n))
    return "Wyndham";
  if (/hyatt|andaz|alila|thompson|caption|unbound|destination |miraval|jdv |jd\.?v/.test(n))
    return "Hyatt";
  if (/anantara|avani|nhow |oaks /.test(n)) return "Minor";
  if (/meli[aá]|paradisus|sol by|innside/.test(n)) return "Melia";
  if (/secrets |dreams |breathless|sunscape|zo[eë]try|impressions /.test(n)) return "Hyatt"; // AMResorts under Hyatt
  return "Independent";
}

function propertyIdentityId(seed) {
  return `pid_${createHash("sha1").update(seed).digest("hex").slice(0, 20)}`;
}

/**
 * Classify + assign provisional property_identity_id.
 * @param {object[]} candidates
 * @param {object[]} vicRecords
 */
export function classifyAndDedupe(candidates, vicRecords) {
  const vicIndex = (vicRecords || []).map((r) => ({
    ...r,
    _n: normName(r.name),
    _country: String(r.country || "Mexico").toLowerCase(),
    _postal: r.postal_code || r.postalCode || r["Postal Code"] || null,
  }));

  // Exact-name buckets within country for candidate↔candidate dedupe
  /** @type {Map<string, object[]>} */
  const byCountryName = new Map();

  const rows = [];
  let probableDup = 0;
  let existingVerified = 0;
  let existingEnrich = 0;
  let newCand = 0;
  let insufficient = 0;
  let conflict = 0;
  let excluded = 0;
  let postal_match_boosts = 0;
  let postal_mismatch_holds = 0;

  for (const c of candidates) {
    const name = c.origin_name;
    const n = normName(name);
    const country = c.origin_country || "Unknown";
    const family = c.family || inferBrandFamily(name);
    const candPostal = c.postal_code || c.postalCode || c["Postal Code"] || null;

    if (!n || n.length < 3) {
      insufficient += 1;
      rows.push({
        ...c,
        property_identity_id: null,
        brand_family_inferred: family,
        classification: CLASSIFICATION.INSUFFICIENT_IDENTITY,
        match_vic_id: null,
        match_score: null,
        postal_identity_signal: "insufficient",
        serpapi_needed: false,
        serpapi_reason: "insufficient_identity",
      });
      continue;
    }

    // VIC match (same country preference)
    let bestVic = null;
    for (const v of vicIndex) {
      if (country && v._country && country.toLowerCase() !== v._country && c.candidate_origin === CANDIDATE_ORIGINS.CVENT_CHALLENGE) {
        // allow cross-country only if score very high later — skip for speed
        if (v._country !== "mexico" || country.toLowerCase() !== "mexico") continue;
      }
      if (c.candidate_origin === CANDIDATE_ORIGINS.CVENT_CHALLENGE && country.toLowerCase() !== v._country) {
        continue;
      }
      let score = tokenSimilarity(n, v._n);
      const postalSignal = comparePostalIdentitySignal(candPostal, v._postal, country);
      if (postalSignal === "match") {
        score = Math.min(1, score + 0.12);
        postal_match_boosts += 1;
      } else if (postalSignal === "mismatch" && score >= 0.55) {
        // Same-ish name + different verified postal → treat as distinct / conflict risk
        score = Math.max(0, score - 0.2);
        postal_mismatch_holds += 1;
      }
      if (!bestVic || score > bestVic.score) {
        bestVic = { row: v, score, postal_identity_signal: postalSignal };
      }
    }

    // Independent seed rows are the VIC itself
    if (c.candidate_origin === CANDIDATE_ORIGINS.VERIFIED_INDEPENDENT) {
      existingVerified += 1;
      const pid = propertyIdentityId(`vic|${c.origin_source_record_id}`);
      rows.push({
        ...c,
        property_identity_id: pid,
        brand_family_inferred: family,
        classification: CLASSIFICATION.EXISTING_VERIFIED_PROPERTY,
        match_vic_id: c.origin_source_record_id,
        match_score: 1,
        serpapi_needed: false,
        serpapi_reason: "already_independent_seed_phase_a",
      });
      continue;
    }

    let classification = CLASSIFICATION.NEW_PROPERTY_CANDIDATE;
    let match_vic_id = null;
    let match_score = null;

    if (bestVic && bestVic.score >= 0.72) {
      classification = CLASSIFICATION.EXISTING_VERIFIED_PROPERTY;
      match_vic_id = bestVic.row.independent_record_id;
      match_score = bestVic.score;
      existingVerified += 1;
    } else if (bestVic && bestVic.score >= 0.55) {
      classification = CLASSIFICATION.PROBABLE_DUPLICATE;
      match_vic_id = bestVic.row.independent_record_id;
      match_score = bestVic.score;
      probableDup += 1;
    } else if (bestVic && bestVic.score >= 0.4 && bestVic.score < 0.55) {
      classification = CLASSIFICATION.IDENTITY_CONFLICT;
      match_vic_id = bestVic.row.independent_record_id;
      match_score = bestVic.score;
      conflict += 1;
    } else {
      newCand += 1;
    }

    // Intra-candidate exact-name dedupe key — include postal when present to avoid over-merge
    const postalKey =
      candPostal && comparePostalIdentitySignal(candPostal, candPostal, country) === "match"
        ? String(candPostal).replace(/\s+/g, "").toUpperCase()
        : "";
    const key = postalKey
      ? `${country.toLowerCase()}|${n}|${postalKey}`
      : `${country.toLowerCase()}|${n}`;
    const bucket = byCountryName.get(key) || [];
    let pid;
    if (match_vic_id) {
      pid = propertyIdentityId(`vic|${match_vic_id}`);
    } else if (bucket.length) {
      pid = bucket[0].property_identity_id;
      if (classification === CLASSIFICATION.NEW_PROPERTY_CANDIDATE) {
        classification = CLASSIFICATION.PROBABLE_DUPLICATE;
        newCand -= 1;
        probableDup += 1;
      }
    } else {
      // Same name+country but different postal → distinct physical property
      const siblingKeys = [...byCountryName.keys()].filter(
        (k) => k.startsWith(`${country.toLowerCase()}|${n}|`) && k !== key
      );
      if (postalKey && siblingKeys.length) {
        // keep as new / distinct
        pid = propertyIdentityId(`cand|${country}|${n}|${postalKey}`);
      } else {
        pid = propertyIdentityId(`cand|${country}|${n}`);
      }
    }
    bucket.push({ property_identity_id: pid });
    byCountryName.set(key, bucket);

    const serpapi_needed =
      classification === CLASSIFICATION.NEW_PROPERTY_CANDIDATE ||
      classification === CLASSIFICATION.EXISTING_NEEDS_ENRICHMENT ||
      classification === CLASSIFICATION.PROBABLE_DUPLICATE;

    rows.push({
      ...c,
      property_identity_id: pid,
      brand_family_inferred: family,
      classification,
      match_vic_id,
      match_score,
      postal_identity_signal: bestVic?.postal_identity_signal || "insufficient",
      serpapi_needed: Boolean(serpapi_needed && classification === CLASSIFICATION.NEW_PROPERTY_CANDIDATE),
      serpapi_reason:
        classification === CLASSIFICATION.NEW_PROPERTY_CANDIDATE
          ? "new_cvent_challenge_needs_independent_confirmation"
          : classification === CLASSIFICATION.EXISTING_VERIFIED_PROPERTY
            ? "already_matched_vic_skip_paid_unless_gap"
            : "review_before_paid",
    });
  }

  // Unique physical hotels ≈ unique property_identity_id among non-insufficient
  const uniquePids = new Set(
    rows.filter((r) => r.property_identity_id).map((r) => r.property_identity_id)
  );

  return {
    rows,
    summary: {
      total_classified: rows.length,
      existing_verified: existingVerified,
      existing_needs_enrichment: existingEnrich,
      probable_duplicates: probableDup,
      new_property_candidates: newCand,
      identity_conflicts: conflict,
      insufficient_identity: insufficient,
      excluded_non_hotel: excluded,
      estimated_unique_physical_hotels: uniquePids.size,
      postal_match_boosts,
      postal_mismatch_holds,
      identity_dedupe_logic_updated: true,
    },
  };
}

export { tokenize, comparePostalIdentitySignal, applyPostalToIdentityScore };
