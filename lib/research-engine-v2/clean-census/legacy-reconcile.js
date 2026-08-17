/**
 * Legacy reconciliation — ONLY after independent freeze.
 * Legacy values never become independent evidence.
 */

import { createHash } from "node:crypto";
import { tokenSimilarity } from "../adapters/adapter-utils.js";
import { RESEARCH_MODES_CLEAN } from "./provenance.js";

const MATCH_STOPWORDS =
  /\b(hotel|the|a|an|by|and|inn|suites|resort|resorts|hotels|collection|all|inclusive|adults|only)\b/g;

/** Brand tokens that must not alone create a match across different properties. */
const BRAND_TOKENS = new Set(
  [
    "hilton",
    "hampton",
    "doubletree",
    "embassy",
    "garden",
    "canopy",
    "curio",
    "tapestry",
    "waldorf",
    "astoria",
    "conrad",
    "motto",
    "tru",
    "homewood",
    "home2",
    "grand",
    "vacations",
    "spark",
    "tempo",
    "signia",
    "lxr",
    "slh",
  ].map(String)
);

/** Weak geo/amenity tokens — insufficient alone for identity match. */
const WEAK_PLACE_TOKENS = new Set([
  "mexico",
  "city",
  "airport",
  "aeropuerto",
  "downtown",
  "centro",
  "historico",
  "riviera",
  "maya",
  "beach",
  "golf",
  "all",
  "inclusive",
  "adults",
  "only",
  "midtown",
  "valle",
  "plaza",
]);

function normName(name) {
  return String(name || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(MATCH_STOPWORDS, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function tokens(s) {
  return String(s || "")
    .split(/\s+/)
    .filter((t) => t.length > 2);
}

function brandTokensIn(nameNorm) {
  return new Set(tokens(nameNorm).filter((t) => BRAND_TOKENS.has(t)));
}

/** Distinctive brand codes excluding the parent token "hilton". */
function distinctiveBrandTokens(brandSet) {
  const d = new Set([...brandSet].filter((t) => t !== "hilton"));
  return d;
}

function placeTokensIn(nameNorm) {
  return new Set(tokens(nameNorm).filter((t) => !BRAND_TOKENS.has(t)));
}

function strongPlaceTokens(placeSet) {
  return new Set([...placeSet].filter((t) => !WEAK_PLACE_TOKENS.has(t)));
}

function setOverlap(a, b) {
  let n = 0;
  for (const x of a) if (b.has(x)) n++;
  return n;
}

/**
 * Identity match independent → legacy.
 * Hardened: brand-only or weak-geo-only overlap cannot create Exact/Probable matches.
 */
export function matchIndependentToLegacy(independentRecord, legacyRows) {
  const iname = normName(independentRecord.fields?.name || "");
  const icity = normName(independentRecord.fields?.city || "");
  const ipid = String(independentRecord.fields?.["Property ID"] || "").toUpperCase();
  const iBrand = brandTokensIn(iname);
  const iPlace = placeTokensIn(iname);
  if (icity) {
    for (const t of tokens(icity)) iPlace.add(t);
  }
  const iStrong = strongPlaceTokens(iPlace);

  let best = null;
  for (const leg of legacyRows || []) {
    const lname = normName(leg.name);
    const lcity = normName(leg.city || "");
    const lBrand = brandTokensIn(lname);
    const lPlace = placeTokensIn(lname);
    if (lcity) for (const t of tokens(lcity)) lPlace.add(t);
    const lStrong = strongPlaceTokens(lPlace);

    let score = tokenSimilarity(iname, lname);
    const brandOverlap = setOverlap(iBrand, lBrand);
    const iDistinct = distinctiveBrandTokens(iBrand);
    const lDistinct = distinctiveBrandTokens(lBrand);
    const distinctBrandOverlap = setOverlap(iDistinct, lDistinct);
    const strongPlaceOverlap = setOverlap(iStrong, lStrong);
    const weakOnlyPlace =
      strongPlaceOverlap === 0 && setOverlap(iPlace, lPlace) > 0;

    if (ipid && String(leg.hotelId || "").toUpperCase() === ipid) score += 0.4;

    if (strongPlaceOverlap >= 1) score += 0.18 * Math.min(strongPlaceOverlap, 2);
    else if (weakOnlyPlace) score -= 0.15;
    else score -= 0.3;

    // Distinctive brand codes (Hampton vs DoubleTree) must align when both present
    if (iDistinct.size && lDistinct.size && distinctBrandOverlap === 0) {
      score -= 0.55;
    } else if (distinctBrandOverlap >= 1) {
      score += 0.12;
    } else if (iBrand.size && lBrand.size && brandOverlap === 0) {
      score -= 0.45;
    } else if (brandOverlap >= 1) {
      score += 0.05;
    }

    // Brand-only / weak-geo-only cannot pass Independent Only floor
    if (strongPlaceOverlap === 0 && !ipid) {
      score = Math.min(score, 0.34);
    }

    if (!best || score > best.score) {
      const placeSim = tokenSimilarity([...iStrong].sort().join(" "), [...lStrong].sort().join(" "));
      best = {
        legacy: leg,
        score,
        brandOverlap,
        distinctBrandOverlap,
        strongPlaceOverlap,
        placeSim,
        iDistinctSize: iDistinct.size,
        lDistinctSize: lDistinct.size,
      };
    }
  }

  if (!best || best.score < 0.4) {
    return { status: "Independent Only", match: null, score: best?.score || 0 };
  }

  const brandsCompatible =
    best.distinctBrandOverlap >= 1 ||
    (best.iDistinctSize === 0 && best.lDistinctSize === 0);

  // Exact: near-identical place signature + matching distinctive brands
  if (
    best.score >= 0.88 &&
    best.strongPlaceOverlap >= 1 &&
    best.placeSim >= 0.85 &&
    brandsCompatible &&
    best.distinctBrandOverlap >= 1
  ) {
    return { status: "Independent + Legacy Match", match: best.legacy, score: best.score };
  }
  // Flagship Hilton (no distinctive sub-brand) exact only with very high place sim
  if (
    best.score >= 0.9 &&
    best.placeSim >= 0.9 &&
    best.strongPlaceOverlap >= 1 &&
    brandsCompatible
  ) {
    return { status: "Independent + Legacy Match", match: best.legacy, score: best.score };
  }
  // Probable: strong place + matching distinctive brands (or both flagship Hilton)
  if (
    best.score >= 0.58 &&
    best.strongPlaceOverlap >= 1 &&
    best.placeSim >= 0.45 &&
    brandsCompatible
  ) {
    return { status: "Probable Match — Needs Identity Review", match: best.legacy, score: best.score };
  }
  return { status: "Independent Only", match: null, score: best.score };
}

export function compareFieldsAfterFreeze(independentRecord, legacyRow, compareFields) {
  if (!legacyRow) {
    return { agreements: [], disagreements: [], independentOnlyFields: [], unresolvedIndependent: [] };
  }

  const agreements = [];
  const disagreements = [];
  const independentOnlyFields = [];
  const unresolvedIndependent = [];

  for (const field of compareFields) {
    const indClaim = (independentRecord.claims || []).find((c) => c.field === field);
    const indVal = indClaim?.value ?? independentRecord.fields?.[field] ?? null;
    const legVal = legacyFieldValue(legacyRow, field);

    if ((indVal == null || indVal === "") && (legVal == null || legVal === "")) continue;

    if (indVal == null || indVal === "") {
      unresolvedIndependent.push({
        field,
        independent_value: null,
        legacy_reference_value: legVal,
        rule: "Independent remains Unknown — legacy value is not evidence",
        legacy_used_as_source: false,
      });
      continue;
    }

    if (legVal == null || legVal === "") {
      independentOnlyFields.push({ field, independent_value: indVal });
      continue;
    }

    if (valuesAgree(indVal, legVal)) {
      agreements.push({ field, value: indVal, legacy_reference_value: legVal });
    } else {
      disagreements.push({
        field,
        independent_value: indVal,
        legacy_reference_value: legVal,
        retained: "independent_value",
        legacy_used_as_source: false,
      });
    }
  }

  return { agreements, disagreements, independentOnlyFields, unresolvedIndependent };
}

function legacyFieldValue(leg, field) {
  const map = {
    name: leg.name,
    Affiliation: leg.affiliation || inferAffiliation(leg.name),
    "Parent Company": leg.parentCompany,
    status: leg.status || leg.currentStatus,
    country: leg.country,
    city: leg.city,
    rooms: leg.rooms,
  };
  return map[field] ?? null;
}

function inferAffiliation(name) {
  if (/Hotel Indigo/i.test(name || "")) return "Hotel Indigo";
  if (/Kimpton/i.test(name || "")) return "Kimpton";
  return null;
}

function valuesAgree(a, b) {
  const na = String(a).trim().toLowerCase();
  const nb = String(b).trim().toLowerCase();
  if (na === nb) return true;
  if (/open/.test(na) && /open/.test(nb)) return true;
  if (/pipeline|coming/.test(na) && /pipeline|coming/.test(nb)) return true;
  return tokenSimilarity(na, nb) >= 0.85;
}

export function reconcileAfterFreeze(frozenUniverse, legacyRows, firewall) {
  if (firewall.getPhase() === "frozen") {
    firewall.beginLegacyReconciliation();
  }
  const legacy = firewall.requestLegacyCensus(() => legacyRows);

  // Score all pairs, then greedy 1:1 assignment (prevents brand/city fan-in)
  /** @type {{ rec: object, match: object }[]} */
  const candidates = [];
  for (const rec of frozenUniverse.records) {
    const match = matchIndependentToLegacy(rec, legacy);
    if (match.match?.hotelId && match.status !== "Independent Only") {
      candidates.push({ rec, match });
    }
  }
  candidates.sort((a, b) => (b.match.score || 0) - (a.match.score || 0));

  const usedLegacy = new Set();
  const usedIndependent = new Set();
  /** @type {Map<string, object>} */
  const assigned = new Map();
  for (const c of candidates) {
    const lid = c.match.match.hotelId;
    const iid = c.rec.independent_record_id;
    if (usedLegacy.has(lid) || usedIndependent.has(iid)) continue;
    usedLegacy.add(lid);
    usedIndependent.add(iid);
    assigned.set(iid, c.match);
  }

  const comparisons = [];
  const matchedLegacyIds = new Set();

  for (const rec of frozenUniverse.records) {
    const match = assigned.get(rec.independent_record_id) || {
      status: "Independent Only",
      match: null,
      score: 0,
    };
    if (match.match?.hotelId) matchedLegacyIds.add(match.match.hotelId);

    const fieldCompare = compareFieldsAfterFreeze(rec, match.match, [
      "name",
      "Affiliation",
      "Parent Company",
      "status",
      "country",
      "city",
      "rooms",
    ]);

    comparisons.push({
      independent_record_id: rec.independent_record_id,
      independent_name: rec.fields?.name,
      legacy_match_status: match.status,
      match_score: match.score,
      legacy_hotel_id: match.match?.hotelId || null,
      legacy_name: match.match?.name || null,
      legacy_status: match.match?.status || null,
      field_compare: fieldCompare,
      reconstruction_status:
        match.status === "Independent + Legacy Match"
          ? "Legacy Match — Independent Reconstruction Complete"
          : rec.reconstruction_status,
      legacy_used_as_source: false,
    });
  }

  const legacyOnly = legacy
    .filter((l) => !matchedLegacyIds.has(l.hotelId))
    .map((l) => ({
      legacy_hotel_id: l.hotelId,
      legacy_name: l.name,
      legacy_status: l.status,
      legacy_country: l.country,
      outcome: "Legacy Only",
      challenge_status: "Independent Confirmation Pending",
      reconstruction_status: "Legacy Only — Independent Confirmation Pending",
      note: "Do NOT add from legacy. Create Independent Discovery Challenge using non-legacy sources.",
      legacy_used_as_source: false,
    }));

  return {
    research_mode: RESEARCH_MODES_CLEAN.LEGACY_RECONCILIATION,
    comparedAt: new Date().toISOString(),
    independent_count: frozenUniverse.records.length,
    legacy_reference_count: legacy.length,
    matches: comparisons.filter((c) => c.legacy_match_status === "Independent + Legacy Match").length,
    probable: comparisons.filter((c) => c.legacy_match_status.includes("Probable")).length,
    independent_only: comparisons.filter((c) => c.legacy_match_status === "Independent Only").length,
    legacy_only: legacyOnly.length,
    comparisons,
    legacy_only_rows: legacyOnly,
  };
}

export function runLegacyOnlyChallenges(legacyOnlyRows, directoryRows, firewall) {
  firewall.beginLegacyOnlyChallenge();

  return (legacyOnlyRows || []).map((row) => {
    const legacyName = row.legacy_name;
    let best = null;
    for (const d of directoryRows || []) {
      const score = tokenSimilarity(normName(legacyName), normName(d.name || d.inferredHotelName));
      if (!best || score > best.score) best = { row: d, score };
    }

    if (best && best.score >= 0.55) {
      return {
        ...row,
        challenge_result: "Independently confirmed via official directory (post-freeze challenge)",
        challenge_evidence: {
          source: "IHG destination directory",
          source_type: "Official Parent Company Directory",
          propertyUrl: best.row.propertyUrl,
          directoryName: best.row.name,
          match_score: best.score,
        },
        recommended_action: "Create independent record from directory evidence (not from legacy row)",
        legacy_used_as_source: false,
        research_mode: RESEARCH_MODES_CLEAN.LEGACY_ONLY_CHALLENGE,
      };
    }

    return {
      ...row,
      challenge_result: "Unable to independently confirm from current official directory inventory",
      challenge_evidence: null,
      recommended_action: "Escalate — Native retry / Human review / Webhound candidate (explicit auth)",
      unresolved: true,
      legacy_used_as_source: false,
      research_mode: RESEARCH_MODES_CLEAN.LEGACY_ONLY_CHALLENGE,
    };
  });
}

export function fingerprintFreeze(records) {
  const payload = JSON.stringify(
    records.map((r) => ({
      id: r.independent_record_id,
      fields: r.fields,
      claim_count: r.independent_claim_count,
    }))
  );
  return createHash("sha256").update(payload).digest("hex");
}
