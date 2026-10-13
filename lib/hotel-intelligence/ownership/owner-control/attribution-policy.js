/**
 * Packet 2.8B-2 — PortfolioAttributionPolicy.
 * Connected entity ≠ automatic hotel roll-up.
 */

import { PORTFOLIO_BUCKETS, RELATIONSHIP_TYPES } from "./vocabulary.js";

export const PORTFOLIO_ATTRIBUTION_POLICY_VERSION = "portfolio-attribution-policy-v1";

/**
 * Paths that may place a hotel in OWNED_CONTROLLED for the owner anchor.
 * Each step is evaluated from hotel toward owner (or reverse control chain).
 */
export const OWNED_CONTROL_PATH_RULES = Object.freeze([
  { id: "direct_owned_by_anchor", pattern: ["OWNED_BY→ANCHOR"] },
  { id: "owned_by_propco_controlled_by_anchor", pattern: ["OWNED_BY→PROPCO", "CONTROLLED_BY→ANCHOR"] },
  { id: "owned_by_controlled_subsidiary", pattern: ["OWNED_BY→SUBSIDIARY", "CONTROLLED_BY→ANCHOR"] },
  { id: "controlled_by_anchor", pattern: ["CONTROLLED_BY→ANCHOR"] },
  {
    id: "owner_controlled_jv",
    pattern: ["OWNED_BY→JV", "CONTROLLED_BY→ANCHOR"],
    requires: "control_evidence",
  },
]);

/** Explicitly excluded from OWNED_CONTROLLED. */
export const OWNED_EXCLUSIONS = Object.freeze([
  "OPERATED_BY_ONLY",
  "BRANDED_BY_ONLY",
  "JV_PARTNER_UNRELATED_ASSETS",
  "MINORITY_SHAREHOLDER_UNRELATED_ASSETS",
  "PARENT_REVERSE_OWNERSHIP",
  "DEVELOPED_BY_ONLY",
  "HISTORICAL_WITHOUT_CURRENT",
  "ANNOUNCED_WITHOUT_CURRENT",
]);

/**
 * Classify a hotel relationship for an owner portfolio.
 * @param {{
 *   relationship_type: string,
 *   secondary_relationship_type?: string|null,
 *   economic_owner_verified?: boolean,
 *   control_status?: string|null,
 *   temporal_status?: string|null,
 *   attribution_path?: Array<{relationship_type:string, to_entity_id?:string}>,
 *   notes?: string,
 * }} hotelRel
 * @param {{ owner_entity_id: string }} ctx
 */
export function classifyPortfolioBucket(hotelRel, ctx = {}) {
  const rel = String(hotelRel.relationship_type || "").toUpperCase();
  const secondary = String(hotelRel.secondary_relationship_type || "").toUpperCase();
  const temporal = String(hotelRel.temporal_status || "CURRENT").toUpperCase();
  const path = Array.isArray(hotelRel.attribution_path) ? hotelRel.attribution_path : [];

  if (temporal === "FORMER" || temporal === "HISTORICAL") {
    return {
      bucket: PORTFOLIO_BUCKETS[5], // HISTORICAL_DISPOSED
      include_as_owned: false,
      reason: "historical_or_former",
    };
  }
  if (temporal === "ANNOUNCED" || temporal === "PLANNED") {
    return {
      bucket: PORTFOLIO_BUCKETS[6], // PIPELINE_ANNOUNCED
      include_as_owned: false,
      reason: "announced_or_planned",
    };
  }

  // Hard negatives
  if (rel === "OPERATED_BY" && !hotelRel.economic_owner_verified && secondary !== "OWNED_BY") {
    return {
      bucket: "OPERATED_MANAGED",
      include_as_owned: false,
      reason: "operated_not_owned",
      exclusion: "OPERATED_BY_ONLY",
    };
  }
  if (rel === "BRANDED_BY") {
    return {
      bucket: "UNKNOWN_RELATIONSHIP",
      include_as_owned: false,
      reason: "branded_only",
      exclusion: "BRANDED_BY_ONLY",
    };
  }
  if (rel === "DEVELOPED_BY") {
    return {
      bucket: "DEVELOPED",
      include_as_owned: false,
      reason: "developed_only",
      exclusion: "DEVELOPED_BY_ONLY",
    };
  }
  if (rel === "JV_WITH" || hotelRel.control_status === "JV") {
    return {
      bucket: "JV_PARTIAL",
      include_as_owned: false,
      reason: "jv_without_clear_control_default",
    };
  }
  if (rel === "SPONSORED_BY") {
    return {
      bucket: "SPONSORED_ECONOMIC_INTEREST",
      include_as_owned: false,
      reason: "sponsored_not_controlled",
    };
  }

  const ownedSignal =
    rel === "OWNED_BY" ||
    rel === "CONTROLLED_BY" ||
    hotelRel.economic_owner_verified === true ||
    pathAllowsOwnedAttribution(path, ctx.owner_entity_id);

  if (ownedSignal) {
    return {
      bucket: "OWNED_CONTROLLED",
      include_as_owned: true,
      reason: "owned_or_controlled_path",
    };
  }

  if (rel === "ASSET_MANAGED_BY" || secondary === "OPERATED_BY") {
    return {
      bucket: "OPERATED_MANAGED",
      include_as_owned: false,
      reason: "managed_or_operated",
    };
  }

  return {
    bucket: "UNKNOWN_RELATIONSHIP",
    include_as_owned: false,
    reason: "unclassified",
  };
}

function pathAllowsOwnedAttribution(path, ownerEntityId) {
  if (!path.length || !ownerEntityId) return false;
  const types = path.map((p) => String(p.relationship_type || "").toUpperCase());
  // Hotel → OWNED_BY → X → CONTROLLED_BY → anchor
  if (types.includes("OWNED_BY") && types.includes("CONTROLLED_BY")) return true;
  if (types.length === 1 && types[0] === "OWNED_BY") return true;
  if (types.length === 1 && types[0] === "CONTROLLED_BY") return true;
  return false;
}

/**
 * JV partner fan-out guard: never import partner's unrelated hotels.
 */
export function mayImportHotelsFromRelatedEntity(edge) {
  const rel = String(edge?.relationship_type || "").toUpperCase();
  const association = String(edge?.association_level || "").toUpperCase();
  if (rel === "JV_WITH" || association === "JV_PARTNER") {
    return {
      ok: false,
      reason: "JV_PARTNER_PORTFOLIO_NOT_IMPORTED",
    };
  }
  if (association === "SHAREHOLDER_INFLUENCE" || association === "AFFILIATE") {
    return {
      ok: false,
      reason: "AFFILIATE_NOT_CONTROLLED",
    };
  }
  if (rel === "OPERATED_BY" || rel === "BRANDED_BY" || rel === "DEVELOPED_BY") {
    return {
      ok: false,
      reason: "NON_OWNERSHIP_EDGE",
    };
  }
  if (rel === "CONTROLLED_BY" || rel === "OWNED_BY" || rel === "SUBSIDIARY_OF" || rel === "PARENT_OF") {
    return { ok: true, reason: "control_or_ownership_edge" };
  }
  return { ok: false, reason: "default_deny" };
}

export function assertNoJvPartnerPortfolioLeak(portfolioHotels = []) {
  const leaks = portfolioHotels.filter(
    (h) =>
      h.bucket === "OWNED_CONTROLLED" &&
      /jv_partner|partner_portfolio/i.test(String(h.attribution_note || h.note || ""))
  );
  return {
    ok: leaks.length === 0,
    leaks,
    gate: "JV_PARTNER_PORTFOLIO_NOT_IMPORTED",
  };
}
