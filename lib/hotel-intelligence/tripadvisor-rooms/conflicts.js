/**
 * Room-count conflict classification (no majority vote / averaging).
 */

export const CONFLICT_CLASSIFY_VERSION = "room-conflict-classify-v1";

export const CONFLICT_CAUSE = Object.freeze({
  CURRENT_VS_HISTORICAL: "CURRENT_VS_HISTORICAL",
  RENOVATION_OR_EXPANSION: "RENOVATION_OR_EXPANSION",
  PARTIAL_COMPLEX: "PARTIAL_COMPLEX",
  RESORT_COMPLEX_VS_INDIVIDUAL_HOTEL: "RESORT_COMPLEX_VS_INDIVIDUAL_HOTEL",
  ROOMS_VS_ROOMS_AND_SUITES: "ROOMS_VS_ROOMS_AND_SUITES",
  VILLAS_OR_RESIDENCES_INCLUDED: "VILLAS_OR_RESIDENCES_INCLUDED",
  SISTER_PROPERTY_COLLISION: "SISTER_PROPERTY_COLLISION",
  STALE_SOURCE: "STALE_SOURCE",
  MATCH_ERROR: "MATCH_ERROR",
  UNKNOWN_CONFLICT: "UNKNOWN_CONFLICT",
});

/**
 * @param {{
 *   tripadvisor_rooms: number|null,
 *   official_rooms?: number|null,
 *   other_rooms?: number[],
 *   tripadvisor_name?: string|null,
 *   dealality_name?: string|null,
 *   evidence_quotes?: string[],
 *   sister_collision?: boolean,
 * }} ctx
 */
export function classifyRoomConflict(ctx = {}) {
  const ta = Number(ctx.tripadvisor_rooms);
  const official = ctx.official_rooms != null ? Number(ctx.official_rooms) : null;
  const quotes = (ctx.evidence_quotes || []).join(" ").toLowerCase();
  const taName = String(ctx.tripadvisor_name || "").toLowerCase();
  const dealName = String(ctx.dealality_name || "").toLowerCase();

  if (ctx.sister_collision) {
    return {
      cause: CONFLICT_CAUSE.SISTER_PROPERTY_COLLISION,
      prefer: "review",
      note: "Sister-brand or property token mismatch",
    };
  }

  if (Number.isFinite(ta) && Number.isFinite(official)) {
    const diff = Math.abs(ta - official);
    const pct = diff / Math.max(ta, official, 1);

    if (/renovat|expans|ampliaci|remodel|phase\s*2|nueva torre/i.test(quotes)) {
      return {
        cause: CONFLICT_CAUSE.RENOVATION_OR_EXPANSION,
        prefer: "official",
        note: "Renovation/expansion language near official count",
      };
    }
    if (
      /rooms?\s+and\s+suites|habitaciones?\s+y\s+suites|guestrooms?\s+and\s+suites/i.test(
        quotes
      ) &&
      pct >= 0.05
    ) {
      return {
        cause: CONFLICT_CAUSE.ROOMS_VS_ROOMS_AND_SUITES,
        prefer: "official",
        note: "Rooms vs rooms+suites wording",
      };
    }
    if (/villa|residence|residenc|condo/i.test(quotes) && pct >= 0.08) {
      return {
        cause: CONFLICT_CAUSE.VILLAS_OR_RESIDENCES_INCLUDED,
        prefer: "review",
        note: "Villas/residences may be included in one count",
      };
    }
    if (
      (/resort|complex|all[- ]inclusive/i.test(taName) &&
        !/resort|complex/i.test(dealName)) ||
      (/resort|complex/i.test(dealName) && ta < official * 0.7)
    ) {
      return {
        cause: CONFLICT_CAUSE.RESORT_COMPLEX_VS_INDIVIDUAL_HOTEL,
        prefer: "official",
        note: "Possible resort-complex vs single-hotel inventory",
      };
    }
    if (pct <= 0.2 && pct > 0.05) {
      return {
        cause: CONFLICT_CAUSE.CURRENT_VS_HISTORICAL,
        prefer: "official",
        note: "Moderate gap — possible stale vs current inventory",
      };
    }
    if (pct > 0.35) {
      return {
        cause: CONFLICT_CAUSE.MATCH_ERROR,
        prefer: "review",
        note: "Large gap — possible match or complex-boundary error",
      };
    }
  }

  if (/partial|tower|wing|edificio|only\s+\d+\s+of/i.test(quotes)) {
    return {
      cause: CONFLICT_CAUSE.PARTIAL_COMPLEX,
      prefer: "review",
      note: "Partial-complex language",
    };
  }

  return {
    cause: CONFLICT_CAUSE.UNKNOWN_CONFLICT,
    prefer: Number.isFinite(official) ? "official" : "review",
    note: "Insufficient evidence to classify conflict",
  };
}
