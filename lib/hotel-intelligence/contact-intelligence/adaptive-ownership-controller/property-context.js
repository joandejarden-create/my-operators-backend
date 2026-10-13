/**
 * Adaptive V2.2 — property-context propagation for owner candidates.
 * Target hotel identity + candidate location + identity_match must survive
 * create → merge → persist → resume → verify.
 */

import { PROPERTY_ENTITY_MATCH } from "../ownership-research-strategy/constants.js";

/** Severity for identity states — higher must never be erased by a softer merge. */
export const PROPERTY_MATCH_SEVERITY = Object.freeze({
  [PROPERTY_ENTITY_MATCH.WRONG_PROPERTY]: 100,
  WRONG_PROPERTY: 100,
  [PROPERTY_ENTITY_MATCH.CONFLICTING_LOCATION]: 90,
  CONFLICTING_LOCATION: 90,
  [PROPERTY_ENTITY_MATCH.UNKNOWN]: 10,
  UNKNOWN: 10,
  [PROPERTY_ENTITY_MATCH.NAME_ONLY_MATCH]: 20,
  NAME_ONLY_MATCH: 20,
  [PROPERTY_ENTITY_MATCH.PARTIAL_MATCH]: 30,
  PARTIAL_MATCH: 30,
  [PROPERTY_ENTITY_MATCH.STRONG_PROPERTY_MATCH]: 50,
  STRONG_PROPERTY_MATCH: 50,
  [PROPERTY_ENTITY_MATCH.EXACT_PROPERTY_MATCH]: 60,
  EXACT_PROPERTY_MATCH: 60,
});

const CONFLICT_STATES = new Set([
  PROPERTY_ENTITY_MATCH.WRONG_PROPERTY,
  PROPERTY_ENTITY_MATCH.CONFLICTING_LOCATION,
  "WRONG_PROPERTY",
  "CONFLICTING_LOCATION",
]);

export function isConflictPropertyMatch(match) {
  return CONFLICT_STATES.has(String(match || ""));
}

export function preferStrongerPropertyMatch(a, b) {
  const sa = PROPERTY_MATCH_SEVERITY[a] || 0;
  const sb = PROPERTY_MATCH_SEVERITY[b] || 0;
  // Conflicts always win over non-conflicts
  if (isConflictPropertyMatch(a) && !isConflictPropertyMatch(b)) return a;
  if (isConflictPropertyMatch(b) && !isConflictPropertyMatch(a)) return b;
  return sa >= sb ? a || b || null : b || a || null;
}

function pick(...vals) {
  for (const v of vals) {
    if (v != null && String(v).trim() !== "") return v;
  }
  return null;
}

/**
 * Build target-hotel context from hotel record + optional lead/source fields.
 */
export function buildPropertyContext({
  hotel = {},
  candidate_city = null,
  candidate_state = null,
  candidate_country = null,
  candidate_address = null,
  property_identity_match = null,
  identity_match_reason = null,
  location_conflict = null,
  historical_or_current = null,
  source_snippet = null,
  source_url = null,
  evidence_refs = [],
} = {}) {
  const target_city = pick(hotel.city, hotel.City, hotel.market);
  const target_state = pick(hotel.state, hotel.State, hotel.region, hotel.uf);
  const target_country = pick(hotel.country, hotel.Country, "Brazil");
  const target_address = pick(hotel.address, hotel.Address, hotel.street_address);

  let match = property_identity_match || null;
  let reason = identity_match_reason || null;
  let conflict = location_conflict === true;

  // Infer location conflict from candidate vs target geography when both present
  if (!match && candidate_city && target_city) {
    const cc = String(candidate_city)
      .toLowerCase()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "");
    const tc = String(target_city)
      .toLowerCase()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "");
    if (cc.length >= 4 && tc.length >= 4 && !cc.includes(tc.slice(0, 5)) && !tc.includes(cc.slice(0, 5))) {
      match = PROPERTY_ENTITY_MATCH.CONFLICTING_LOCATION;
      reason = reason || `candidate_city_${candidate_city}_vs_target_${target_city}`;
      conflict = true;
    }
  }
  if (!match && candidate_state && target_state) {
    const cs = String(candidate_state).toUpperCase().replace(/[^A-Z]/g, "");
    const ts = String(target_state).toUpperCase().replace(/[^A-Z]/g, "");
    if (cs.length === 2 && ts.length === 2 && cs !== ts) {
      match = PROPERTY_ENTITY_MATCH.CONFLICTING_LOCATION;
      reason = reason || `candidate_state_${cs}_vs_target_${ts}`;
      conflict = true;
    }
  }

  if (isConflictPropertyMatch(match)) conflict = true;

  return {
    target_hotel_id: hotel.hotel_id || hotel.id || null,
    target_hotel_name: pick(hotel.hotel_name, hotel.name, hotel.label),
    target_exact_address: target_address,
    target_city,
    target_state,
    target_country,
    candidate_address: candidate_address || null,
    candidate_city: candidate_city || null,
    candidate_state: candidate_state || null,
    candidate_country: candidate_country || null,
    property_identity_match: match || PROPERTY_ENTITY_MATCH.UNKNOWN,
    identity_match_reason: reason,
    location_conflict: conflict,
    historical_or_current: historical_or_current || null,
    source_snippet: source_snippet || null,
    source_url: source_url || null,
    evidence_refs: Array.isArray(evidence_refs) ? evidence_refs : [],
  };
}

/**
 * Conservatively merge two property contexts. Never erase CONFLICTING_LOCATION / WRONG_PROPERTY.
 */
export function mergePropertyContext(prev = null, next = null) {
  if (!prev && !next) return null;
  if (!prev) return { ...next };
  if (!next) return { ...prev };

  const match = preferStrongerPropertyMatch(
    prev.property_identity_match,
    next.property_identity_match
  );
  const conflict =
    prev.location_conflict === true ||
    next.location_conflict === true ||
    isConflictPropertyMatch(match);

  return {
    target_hotel_id: pick(prev.target_hotel_id, next.target_hotel_id),
    target_hotel_name: pick(prev.target_hotel_name, next.target_hotel_name),
    target_exact_address: pick(prev.target_exact_address, next.target_exact_address),
    target_city: pick(prev.target_city, next.target_city),
    target_state: pick(prev.target_state, next.target_state),
    target_country: pick(prev.target_country, next.target_country),
    candidate_address: pick(prev.candidate_address, next.candidate_address),
    candidate_city: pick(prev.candidate_city, next.candidate_city),
    candidate_state: pick(prev.candidate_state, next.candidate_state),
    candidate_country: pick(prev.candidate_country, next.candidate_country),
    property_identity_match: match,
    identity_match_reason: isConflictPropertyMatch(match)
      ? pick(
          isConflictPropertyMatch(prev.property_identity_match) ? prev.identity_match_reason : null,
          isConflictPropertyMatch(next.property_identity_match) ? next.identity_match_reason : null,
          prev.identity_match_reason,
          next.identity_match_reason
        )
      : pick(next.identity_match_reason, prev.identity_match_reason),
    location_conflict: conflict,
    historical_or_current: pick(prev.historical_or_current, next.historical_or_current),
    source_snippet: pick(prev.source_snippet, next.source_snippet),
    source_url: pick(prev.source_url, next.source_url),
    evidence_refs: [
      ...new Set([...(prev.evidence_refs || []), ...(next.evidence_refs || [])]),
    ],
  };
}

/**
 * Gate 2 — target-property relevance (after entity-shape Gate 1).
 * Legal-shaped names may pass Gate 1 while failing Gate 2.
 */
export function assessTargetPropertyRelevance(candidate = {}, hotel = {}) {
  const ctx = candidate.property_context || {};
  const match =
    ctx.property_identity_match ||
    candidate.property_identity_match ||
    candidate.property_identity_relationship ||
    PROPERTY_ENTITY_MATCH.UNKNOWN;

  if (isConflictPropertyMatch(match) || ctx.location_conflict === true) {
    return {
      relevant: false,
      gate: "FAIL_LOCATION",
      property_identity_match: match || PROPERTY_ENTITY_MATCH.CONFLICTING_LOCATION,
      reason: ctx.identity_match_reason || "ENTITY_LOCATION_CONFLICT",
    };
  }

  const entity = String(candidate.entity_name || "");
  const hotelName = String(
    ctx.target_hotel_name || hotel.hotel_name || hotel.name || ""
  );
  const hotelCity = String(ctx.target_city || hotel.city || "");

  // Candidate city conflict vs target (even if match field missing)
  if (ctx.candidate_city && hotelCity) {
    const cc = ctx.candidate_city
      .toLowerCase()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "");
    const tc = hotelCity
      .toLowerCase()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "");
    if (cc.length >= 4 && tc.length >= 4 && !cc.includes(tc.slice(0, 5)) && !tc.includes(cc.slice(0, 5))) {
      return {
        relevant: false,
        gate: "FAIL_LOCATION",
        property_identity_match: PROPERTY_ENTITY_MATCH.CONFLICTING_LOCATION,
        reason: `candidate_city_${ctx.candidate_city}_vs_${hotelCity}`,
      };
    }
  }

  // Unrelated legal-shaped org: no name overlap with hotel and no exact/strong match
  const coreHotel = hotelName
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\b(hotel|pousada|by|wyndham|tryp|ibis|business|shopping)\b/g, " ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  const coreEnt = entity
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\b(ltda|sa|eireli|spe|spv|hotelaria|administradora|hoteleira|comercial)\b/g, " ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  const strongMatch =
    match === PROPERTY_ENTITY_MATCH.EXACT_PROPERTY_MATCH ||
    match === PROPERTY_ENTITY_MATCH.STRONG_PROPERTY_MATCH;

  if (strongMatch) {
    return {
      relevant: true,
      gate: "PASS",
      property_identity_match: match,
      reason: "STRONG_OR_EXACT_PROPERTY_MATCH",
    };
  }

  // Token overlap between entity and hotel core
  const hotelTokens = new Set(coreHotel.split(" ").filter((t) => t.length >= 4));
  const entTokens = coreEnt.split(" ").filter((t) => t.length >= 4);
  const overlap = entTokens.filter((t) => hotelTokens.has(t) || [...hotelTokens].some((h) => h.includes(t) || t.includes(h)));

  if (hotelTokens.size > 0 && overlap.length === 0 && match === PROPERTY_ENTITY_MATCH.UNKNOWN) {
    // Distinct other-hotel legal names (TRYP pattern: Hotel Do Frade / Palace Itaquera)
    if (/\b(hotel|pousada|palace|resort)\b/i.test(entity) && !/\b(administradora|fundo|fii)\b/i.test(entity)) {
      return {
        relevant: false,
        gate: "FAIL_UNRELATED",
        property_identity_match: PROPERTY_ENTITY_MATCH.WRONG_PROPERTY,
        reason: "UNRELATED_LEGAL_SHAPED_ENTITY",
      };
    }
  }

  return {
    relevant: true,
    gate: "PASS",
    property_identity_match: match,
    reason: overlap.length ? "NAME_OVERLAP" : "RELEVANCE_UNCONFIRMED_BUT_ELIGIBLE",
  };
}

/**
 * Extract candidate location hints from raw prose before name cleaning.
 */
export function extractLocationHintsFromProse(raw = "") {
  const text = String(raw || "");
  // Prefer explicit "localizada em City/UF"
  const locSlash = text.match(
    /localizad[oa]\s+em\s+(.+?)\/([A-Z]{2})\b/i
  );
  if (locSlash) {
    return {
      candidate_city: locSlash[1].trim(),
      candidate_state: locSlash[2].toUpperCase(),
      candidate_address: null,
    };
  }
  const loc = text.match(/localizad[oa]\s+em\s+([^,;|]+)/i);
  if (loc) {
    return {
      candidate_city: loc[1].trim(),
      candidate_state: null,
      candidate_address: null,
    };
  }
  const cityState = text.match(
    /\bem\s+([A-ZÁÉÍÓÚÂÊÔÃÕÇ][\wÁÉÍÓÚÂÊÔÃÕÇáéíóúâêôãõç\s']+?)\/([A-Z]{2})\b/i
  );
  if (cityState) {
    return {
      candidate_city: cityState[1].trim(),
      candidate_state: cityState[2].toUpperCase(),
      candidate_address: null,
    };
  }
  return { candidate_city: null, candidate_state: null, candidate_address: null };
}
