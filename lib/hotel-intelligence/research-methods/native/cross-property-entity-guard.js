/**
 * Cross-property entity propagation guard (Packet 2.4D).
 * PROPERTY_SPECIFIC entities (e.g. IHVSF) must not transfer across hotels
 * without independent hotel-specific evidence.
 */

export const ENTITY_SCOPES = Object.freeze([
  "PROPERTY_SPECIFIC",
  "MULTI_PROPERTY",
  "ORGANIZATION_LEVEL",
  "PORTFOLIO_LEVEL",
  "JURISDICTIONAL",
  "UNKNOWN_SCOPE",
]);

export const CROSS_PROPERTY_ENTITY_PROPAGATION_GUARD = "CROSS_PROPERTY_ENTITY_PROPAGATION_GUARD";

/** Known entity → default scope (can be overridden by evidence). */
export const ENTITY_SCOPE_REGISTRY = Object.freeze({
  IHVSF: {
    scope: "PROPERTY_SPECIFIC",
    canonical: "IHVSF / Inmobiliaria en Hotelería Vallarta Santa Fe",
    allowed_hotel_patterns: [
      /krystal\s+grand\s+puerto\s+vallarta/i,
      /hilton\s+puerto\s+vallarta/i,
    ],
    allowed_city_patterns: [/puerto\s+vallarta/i],
    note: "Vallarta Grand property vehicle — not portfolio-wide PropCo",
  },
  "INMOBILIARIA EN HOTELERIA VALLARTA SANTA FE": {
    scope: "PROPERTY_SPECIFIC",
    canonical: "IHVSF / Inmobiliaria en Hotelería Vallarta Santa Fe",
    allowed_hotel_patterns: [/krystal\s+grand\s+puerto\s+vallarta/i, /hilton\s+puerto\s+vallarta/i],
    allowed_city_patterns: [/puerto\s+vallarta/i],
  },
  "GRUPO HOTELERO SANTA FE": {
    scope: "ORGANIZATION_LEVEL",
    canonical: "Grupo Hotelero Santa Fe",
    allowed_hotel_patterns: [/.*/],
    note: "Portfolio/org-level economic control may apply where filings support; PropCo still separate",
  },
  "GRUPO CHARTWELL": {
    scope: "MULTI_PROPERTY",
    canonical: "Grupo Chartwell",
    allowed_hotel_patterns: [/krystal\s+resort\s+puerto\s+vallarta/i, /chartwell/i],
  },
  "FIDEICOMISO PLAYA DEL CARMEN": {
    scope: "PROPERTY_SPECIFIC",
    canonical: "Fideicomiso Playa Del Carmen",
    allowed_hotel_patterns: [/mahekal/i],
    allowed_city_patterns: [/playa\s+del\s+carmen/i],
  },
  MHKL: {
    scope: "PROPERTY_SPECIFIC",
    canonical: "MHKL",
    allowed_hotel_patterns: [/mahekal/i],
  },
});

function normalizeEntityKey(name) {
  return String(name || "")
    .toUpperCase()
    .replace(/[^A-Z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * @param {string} entityName
 * @returns {{ scope: string, canonical?: string, registry?: object }}
 */
export function classifyEntityScope(entityName) {
  const key = normalizeEntityKey(entityName);
  if (!key) return { scope: "UNKNOWN_SCOPE" };
  if (/\bIHVSF\b/.test(key) || /INMOBILIARIA.*VALLARTA\s+SANTA\s+FE/.test(key)) {
    return { scope: "PROPERTY_SPECIFIC", ...ENTITY_SCOPE_REGISTRY.IHVSF };
  }
  if (/FIDEICOMISO\s+PLAYA\s+DEL\s+CARMEN|FIDEICOMISO\s+MAHEKAL/.test(key)) {
    return { scope: "PROPERTY_SPECIFIC", ...ENTITY_SCOPE_REGISTRY["FIDEICOMISO PLAYA DEL CARMEN"] };
  }
  if (/\bMHKL\b/.test(key)) return { scope: "PROPERTY_SPECIFIC", ...ENTITY_SCOPE_REGISTRY.MHKL };
  if (/GRUPO\s+HOTELERO\s+SANTA\s+FE|\bGSF\b|\bHOTEL\b.*SANTA\s+FE/.test(key)) {
    return { scope: "ORGANIZATION_LEVEL", ...ENTITY_SCOPE_REGISTRY["GRUPO HOTELERO SANTA FE"] };
  }
  if (/CHARTWELL/.test(key)) return { scope: "MULTI_PROPERTY", ...ENTITY_SCOPE_REGISTRY["GRUPO CHARTWELL"] };
  if (/FIBRAHOTEL|FIBRA\s+INN|BMV/.test(key)) return { scope: "PORTFOLIO_LEVEL", canonical: entityName };
  if (/XCARET|BELMOND|LVMH/.test(key)) return { scope: "ORGANIZATION_LEVEL", canonical: entityName };
  return { scope: "UNKNOWN_SCOPE", canonical: entityName };
}

/**
 * May entity E from evidence context of hotel A be attached to hotel B?
 *
 * @param {{
 *   entityName: string,
 *   sourceHotelName?: string,
 *   targetHotel: { name?: string, city?: string, rooms?: number|null },
 *   evidenceText?: string,
 *   relationshipKind?: 'propco'|'economic_owner'|'operator'|'other'
 * }} args
 */
export function mayPropagateEntityAcrossHotels(args = {}) {
  const {
    entityName,
    sourceHotelName = null,
    targetHotel = {},
    evidenceText = "",
    relationshipKind = "other",
  } = args;

  const classified = classifyEntityScope(entityName);
  const targetName = String(targetHotel.name || "");
  const targetCity = String(targetHotel.city || "");
  const blob = String(evidenceText || "");

  // Organization / portfolio level economic owner may propagate when relationship is economic_owner
  if (
    (classified.scope === "ORGANIZATION_LEVEL" || classified.scope === "PORTFOLIO_LEVEL") &&
    relationshipKind === "economic_owner"
  ) {
    // Still require seed hotel mention in evidence for HIGH promotion elsewhere; guard allows candidate
    return {
      allowed: true,
      reason: "organization_or_portfolio_level_economic_owner",
      scope: classified.scope,
      guard: CROSS_PROPERTY_ENTITY_PROPAGATION_GUARD,
    };
  }

  // PROPERTY_SPECIFIC PropCo / vehicle — strict
  if (classified.scope === "PROPERTY_SPECIFIC" || relationshipKind === "propco") {
    const patterns = classified.allowed_hotel_patterns || [];
    const cityPatterns = classified.allowed_city_patterns || [];
    const hotelOk = patterns.some((re) => re.test(targetName));
    const cityOk = cityPatterns.length ? cityPatterns.some((re) => re.test(targetCity)) : true;

    // Independent corroboration in evidence: exact hotel name near entity
    const nameEsc = targetName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&").slice(0, 40);
    const coOccur =
      nameEsc.length >= 8 &&
      new RegExp(nameEsc, "i").test(blob) &&
      new RegExp(String(entityName).slice(0, 12).replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(blob);

    // PROPERTY_SPECIFIC requires registered hotel-name patterns.
    // City match + shared annual-report co-occurrence alone is NOT enough
    // (would let IHVSF bleed onto Krystal Resort PV via city + filing blob).
    if (hotelOk && cityOk) {
      return {
        allowed: true,
        reason: "property_specific_hotel_match",
        scope: classified.scope,
        guard: CROSS_PROPERTY_ENTITY_PROPAGATION_GUARD,
      };
    }
    if (hotelOk && coOccur) {
      return {
        allowed: true,
        reason: "property_specific_hotel_match_plus_cooccurrence",
        scope: classified.scope,
        guard: CROSS_PROPERTY_ENTITY_PROPAGATION_GUARD,
      };
    }

    // Same portfolio / operator / short token / same annual report alone is insufficient
    return {
      allowed: false,
      reason: "property_specific_blocked_without_hotel_evidence",
      scope: classified.scope,
      guard: CROSS_PROPERTY_ENTITY_PROPAGATION_GUARD,
      blocked_by: [
        "same_portfolio_insufficient",
        "shared_operator_insufficient",
        "shared_brand_family_insufficient",
        "short_token_insufficient",
        "same_annual_report_insufficient",
        "city_only_cooccurrence_insufficient",
      ],
      sourceHotelName,
      targetHotelName: targetName,
      coOccur_without_hotel_pattern: Boolean(coOccur && cityOk && !hotelOk),
    };
  }

  if (classified.scope === "MULTI_PROPERTY") {
    const patterns = classified.allowed_hotel_patterns || [];
    if (patterns.some((re) => re.test(targetName)) || patterns.some((re) => re.test(blob))) {
      return { allowed: true, reason: "multi_property_pattern_match", scope: classified.scope, guard: CROSS_PROPERTY_ENTITY_PROPAGATION_GUARD };
    }
    return {
      allowed: false,
      reason: "multi_property_no_match",
      scope: classified.scope,
      guard: CROSS_PROPERTY_ENTITY_PROPAGATION_GUARD,
    };
  }

  return {
    allowed: false,
    reason: "unknown_scope_default_deny_for_propco_like",
    scope: classified.scope,
    guard: CROSS_PROPERTY_ENTITY_PROPAGATION_GUARD,
  };
}

/**
 * Filter claims that would illegally propagate PROPERTY_SPECIFIC entities.
 * @param {object[]} claims
 * @param {{ name?: string, city?: string }} hotel
 * @param {string} evidenceText
 */
export function applyCrossPropertyGuardToClaims(claims, hotel, evidenceText = "") {
  const out = [];
  const blocked = [];
  for (const c of claims || []) {
    const kind = /propco/i.test(c.claim_type || "")
      ? "propco"
      : /economic_owner/i.test(c.claim_type || "")
        ? "economic_owner"
        : "other";
    if (!c.entity || kind === "other") {
      out.push(c);
      continue;
    }
    const decision = mayPropagateEntityAcrossHotels({
      entityName: c.entity,
      targetHotel: hotel,
      evidenceText,
      relationshipKind: kind,
    });
    if (kind === "propco" && !decision.allowed) {
      blocked.push({ claim: c, decision });
      continue;
    }
    out.push({
      ...c,
      entity_scope: decision.scope,
      propagation_guard: decision.guard,
      propagation_reason: decision.reason,
    });
  }
  return { claims: out, blocked };
}
