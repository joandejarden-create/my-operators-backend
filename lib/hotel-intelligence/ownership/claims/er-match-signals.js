/**
 * Packet 2.5B — Entity match signals (positive / negative).
 * Weak token similarity must not dominate contradictory signals.
 */

import { normalizeEntityName } from "../ids.js";

export const ER_MATCH_SIGNALS_VERSION = "er-match-signals-v1";

export const MATCH_DECISIONS = Object.freeze([
  "MATCHED_HIGH",
  "MATCHED_PROBABLE",
  "AMBIGUOUS",
  "NEW_ENTITY_CANDIDATE",
  "REJECTED_MATCH",
]);

export const RELATION_KINDS = Object.freeze([
  "SAME_ENTITY",
  "PARENT_OF",
  "SUBSIDIARY_OF",
  "AFFILIATE_OF",
  "JV_WITH",
  "INVESTMENT_IN",
  "UNKNOWN_RELATION",
]);

export const ALIAS_STATES = Object.freeze([
  "CANDIDATE_ALIAS",
  "VERIFIED_ALIAS",
  "REJECTED_ALIAS",
]);

/** Positive signal weights (sum can exceed 1; capped later). */
export const POSITIVE_SIGNAL_WEIGHTS = Object.freeze({
  EXACT_NAME: 1.0,
  VERIFIED_ALIAS: 0.95,
  FORMER_NAME: 0.85,
  TICKER_MATCH: 0.9,
  DOMAIN_MATCH: 0.75,
  ADDRESS_MATCH: 0.85,
  COORDINATE_MATCH: 0.9,
  CITY_MATCH: 0.25,
  ROOM_COUNT_MATCH: 0.35,
  BRAND_MATCH: 0.2,
  OPERATOR_MATCH: 0.15,
  OPENING_HISTORY_MATCH: 0.4,
  DEVELOPMENT_HISTORY_MATCH: 0.35,
  SOURCE_SECTION_MATCH: 0.3,
  JURISDICTION_MATCH: 0.2,
  PARENT_CONTEXT_MATCH: 0.25,
});

/** Negative signal penalties (subtracted). */
export const NEGATIVE_SIGNAL_WEIGHTS = Object.freeze({
  DIFFERENT_ADDRESS: 0.8,
  DIFFERENT_COORDINATES: 0.85,
  DIFFERENT_ROOM_COUNT: 0.5,
  DIFFERENT_OPENING_HISTORY: 0.45,
  DIFFERENT_OWNER_HISTORY: 0.4,
  ADJACENT_PROPERTY: 0.95,
  DIFFERENT_BRAND_CHRONOLOGY: 0.35,
  SAME_CITY_SAME_BRAND_COLLISION: 0.7,
  SCOPE_CONFLICT: 0.9,
  JURISDICTION_CONFLICT: 0.85,
  ANTI_MERGE_PAIR: 1.0,
  CROSS_PROPERTY_PROPCO: 0.95,
  BRAND_AS_HOTEL: 0.8,
});

/**
 * Strip corporate suffixes for comparison only — not proof of identity.
 * @param {string} name
 */
export function normalizeOrgCompareKey(name) {
  return normalizeEntityName(name)
    .replace(
      /\b(s a b de c v|s de r l de c v|s a de c v|sab de cv|sa de cv|s de rl|ltd|llc|plc|inc|co|company|grupo)\b/g,
      " "
    )
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Score a raw mention against a registry entity.
 * @param {string} raw
 * @param {object} entity
 * @param {{ kind?: string, hotel_context_id?: string, hotel_name?: string, city?: string, claim_type?: string }} [ctx]
 */
export function scoreEntityMatch(raw, entity, ctx = {}) {
  const rawNorm = normalizeEntityName(raw);
  const nameNorm = normalizeEntityName(entity.name || entity.legal_name || "");
  const signals = [];
  const negatives = [];
  let score = 0;

  if (!rawNorm) {
    return { score: 0, signals, negatives, decision: "REJECTED_MATCH" };
  }

  // Exact name
  if (rawNorm === nameNorm) {
    score += POSITIVE_SIGNAL_WEIGHTS.EXACT_NAME;
    signals.push({ code: "EXACT_NAME", weight: POSITIVE_SIGNAL_WEIGHTS.EXACT_NAME });
  } else if (normalizeOrgCompareKey(raw) === normalizeOrgCompareKey(entity.name || "")) {
    score += POSITIVE_SIGNAL_WEIGHTS.EXACT_NAME * 0.92;
    signals.push({
      code: "EXACT_NAME",
      weight: POSITIVE_SIGNAL_WEIGHTS.EXACT_NAME * 0.92,
      note: "normalized_corporate_suffix",
    });
  }

  // Aliases
  for (const a of entity.aliases || []) {
    const alias = typeof a === "string" ? a : a.alias;
    const aliasType = typeof a === "string" ? "OTHER" : a.alias_type || "OTHER";
    const aNorm = normalizeEntityName(alias);
    if (!aNorm) continue;
    if (a.state === "REJECTED_ALIAS") continue;
    // Bare ticker tokens (e.g. "HOTEL") must not match as org aliases without exchange prefix
    if (
      (aliasType === "TICKER" || aliasType === "TICKER_ALIAS") &&
      aNorm.length <= 5 &&
      !/^bmv\b/.test(rawNorm) &&
      rawNorm === aNorm
    ) {
      continue;
    }
    if (rawNorm === aNorm || normalizeOrgCompareKey(raw) === normalizeOrgCompareKey(alias)) {
      const verified = a.state !== "CANDIDATE_ALIAS" && a.state !== "REJECTED_ALIAS";
      const w =
        aliasType === "FORMER_NAME" || aliasType === "FORMER_BRAND_IDENTITY"
          ? POSITIVE_SIGNAL_WEIGHTS.FORMER_NAME
          : aliasType === "TICKER" || aliasType === "TICKER_ALIAS"
            ? POSITIVE_SIGNAL_WEIGHTS.TICKER_MATCH
            : verified
              ? POSITIVE_SIGNAL_WEIGHTS.VERIFIED_ALIAS
              : POSITIVE_SIGNAL_WEIGHTS.VERIFIED_ALIAS * 0.55;
      score += w;
      signals.push({
        code:
          aliasType === "FORMER_NAME" || aliasType === "FORMER_BRAND_IDENTITY"
            ? "FORMER_NAME"
            : aliasType === "TICKER" || aliasType === "TICKER_ALIAS"
              ? "TICKER_MATCH"
              : verified
                ? "VERIFIED_ALIAS"
                : "SOURCE_SECTION_MATCH",
        weight: w,
        alias,
        alias_state: a.state || "VERIFIED_ALIAS",
      });
      break;
    }
  }

  // Ticker — never match bare common/short symbols without exchange prefix
  if (entity.ticker) {
    const t = String(entity.ticker).toLowerCase();
    const tickerSafe =
      rawNorm === `bmv ${t}` ||
      rawNorm === `bmv: ${t}` ||
      rawNorm === `ticker ${t}` ||
      (t.length >= 6 && rawNorm === t && !/^(hotel|group|corp|inc)$/.test(t));
    if (tickerSafe) {
      // Ticker must not apply to PROPERTY_SPECIFIC
      if (entity.scope === "PROPERTY_SPECIFIC") {
        negatives.push({ code: "SCOPE_CONFLICT", weight: NEGATIVE_SIGNAL_WEIGHTS.SCOPE_CONFLICT });
        score -= NEGATIVE_SIGNAL_WEIGHTS.SCOPE_CONFLICT;
      } else {
        score += POSITIVE_SIGNAL_WEIGHTS.TICKER_MATCH;
        signals.push({ code: "TICKER_MATCH", weight: POSITIVE_SIGNAL_WEIGHTS.TICKER_MATCH });
      }
    }
  }

  // Domain
  if (entity.domain && rawNorm.includes(normalizeEntityName(entity.domain.replace(/\./g, " ")))) {
    score += POSITIVE_SIGNAL_WEIGHTS.DOMAIN_MATCH;
    signals.push({ code: "DOMAIN_MATCH", weight: POSITIVE_SIGNAL_WEIGHTS.DOMAIN_MATCH });
  }

  // City / address / rooms / coords (hotels)
  if (ctx.kind === "hotel" || entity.kind === "hotel") {
    if (entity.city && ctx.city && normalizeEntityName(entity.city) === normalizeEntityName(ctx.city)) {
      score += POSITIVE_SIGNAL_WEIGHTS.CITY_MATCH;
      signals.push({ code: "CITY_MATCH", weight: POSITIVE_SIGNAL_WEIGHTS.CITY_MATCH });
    }
    if (entity.address && ctx.address) {
      if (normalizeEntityName(entity.address) === normalizeEntityName(ctx.address)) {
        score += POSITIVE_SIGNAL_WEIGHTS.ADDRESS_MATCH;
        signals.push({ code: "ADDRESS_MATCH", weight: POSITIVE_SIGNAL_WEIGHTS.ADDRESS_MATCH });
      } else if (
        entity.address &&
        ctx.address &&
        normalizeEntityName(entity.address) !== normalizeEntityName(ctx.address)
      ) {
        // only if both provided and differ materially
      }
    }
    if (
      entity.rooms != null &&
      ctx.rooms != null &&
      Math.abs(Number(entity.rooms) - Number(ctx.rooms)) <= 5
    ) {
      score += POSITIVE_SIGNAL_WEIGHTS.ROOM_COUNT_MATCH;
      signals.push({ code: "ROOM_COUNT_MATCH", weight: POSITIVE_SIGNAL_WEIGHTS.ROOM_COUNT_MATCH });
    } else if (
      entity.rooms != null &&
      ctx.rooms != null &&
      Math.abs(Number(entity.rooms) - Number(ctx.rooms)) >= 80
    ) {
      score -= NEGATIVE_SIGNAL_WEIGHTS.DIFFERENT_ROOM_COUNT;
      negatives.push({
        code: "DIFFERENT_ROOM_COUNT",
        weight: NEGATIVE_SIGNAL_WEIGHTS.DIFFERENT_ROOM_COUNT,
      });
    }
    if (entity.lat != null && entity.lng != null && ctx.lat != null && ctx.lng != null) {
      const dist = haversineKm(entity.lat, entity.lng, ctx.lat, ctx.lng);
      if (dist < 0.15) {
        score += POSITIVE_SIGNAL_WEIGHTS.COORDINATE_MATCH;
        signals.push({ code: "COORDINATE_MATCH", weight: POSITIVE_SIGNAL_WEIGHTS.COORDINATE_MATCH });
      } else if (dist > 1.5) {
        score -= NEGATIVE_SIGNAL_WEIGHTS.DIFFERENT_COORDINATES;
        negatives.push({
          code: "DIFFERENT_COORDINATES",
          weight: NEGATIVE_SIGNAL_WEIGHTS.DIFFERENT_COORDINATES,
        });
      }
    }
  }

  // Jurisdiction
  if (entity.country || entity.jurisdiction) {
    const ej = normalizeEntityName(entity.jurisdiction || entity.country);
    if (ctx.jurisdiction || ctx.country) {
      const cj = normalizeEntityName(ctx.jurisdiction || ctx.country);
      if (ej && cj && ej === cj) {
        score += POSITIVE_SIGNAL_WEIGHTS.JURISDICTION_MATCH;
        signals.push({ code: "JURISDICTION_MATCH", weight: POSITIVE_SIGNAL_WEIGHTS.JURISDICTION_MATCH });
      } else if (ej && cj && ej !== cj) {
        score -= NEGATIVE_SIGNAL_WEIGHTS.JURISDICTION_CONFLICT;
        negatives.push({
          code: "JURISDICTION_CONFLICT",
          weight: NEGATIVE_SIGNAL_WEIGHTS.JURISDICTION_CONFLICT,
        });
      }
    }
  }

  // Contains-match (weak) — only if already some signal or substantial overlap
  if (!signals.length) {
    const entityTokens = nameNorm.split(" ").filter((t) => t.length > 3);
    const rawTokens = rawNorm.split(" ").filter((t) => t.length > 3);
    const overlap = entityTokens.filter((t) => rawTokens.includes(t)).length;
    if (overlap >= 2 && Math.min(rawNorm.length, nameNorm.length) >= 8) {
      // Weak contains — cap low
      const w = 0.35;
      score += w;
      signals.push({ code: "SOURCE_SECTION_MATCH", weight: w, note: "token_overlap_weak" });
    }
  }

  const decision = decisionFromScore(score, signals, negatives, entity, raw, ctx);
  return { score: Math.round(score * 1000) / 1000, signals, negatives, decision };
}

function decisionFromScore(score, signals, negatives, entity, raw, ctx) {
  const hasAnti = negatives.some((n) =>
    ["ANTI_MERGE_PAIR", "ADJACENT_PROPERTY", "CROSS_PROPERTY_PROPCO", "SCOPE_CONFLICT"].includes(
      n.code
    )
  );
  if (hasAnti && score < 1.5) return "REJECTED_MATCH";

  // Brand project names must not auto-match hotels
  if (
    ctx.prefer === "hotel" &&
    entity.kind === "organization" &&
    /brand|project/i.test(entity.entity_subtype || "")
  ) {
    return "REJECTED_MATCH";
  }

  if (
    score >= 0.85 &&
    signals.some((s) =>
      ["EXACT_NAME", "VERIFIED_ALIAS", "TICKER_MATCH", "FORMER_NAME"].includes(s.code)
    )
  ) {
    return "MATCHED_HIGH";
  }
  if (score >= 0.75) return "MATCHED_PROBABLE";
  if (score >= 0.45) return "AMBIGUOUS";
  if (signals.length === 0) return "NEW_ENTITY_CANDIDATE";
  return "AMBIGUOUS";
}

function haversineKm(lat1, lon1, lat2, lon2) {
  const R = 6371;
  const toRad = (d) => (d * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(a));
}
