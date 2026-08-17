/**
 * Identity enrichment proposals — improve Exact/High matching (no Airtable writes).
 */

import { ihgBrandFromUrl } from "../ihg-brand-directory-extract.js";
import { marshaFromMarriottWebsite } from "../marriott-brand-directory-extract.js";
import { assessEntityMatch } from "./match-confidence.js";
import { canonicalizeObservedBrand } from "./brand-family.js";

/**
 * @param {object} hotel - Dealality hotel
 * @param {object} observation - adapter observation
 * @param {object} [entityMatch]
 */
export function proposeIdentityEnrichment(hotel, observation, entityMatch) {
  /** @type {object[]} */
  const proposals = [];
  if (!observation?.hotelFound || !entityMatch?.allowMaterialCorrection) {
    return {
      hotelId: hotel.hotelId || hotel.recordId,
      hotelName: hotel.name,
      proposals: [],
      note: "No Exact/High official match — identity enrichment withheld",
    };
  }

  const url = observation.officialUrl || "";
  const propertyId =
    observation.rawSignals?.propertyId ||
    observation.rawSignals?.marsha ||
    observation.rawSignals?.ctyhocn ||
    "";

  if (url && !hotel.website) {
    proposals.push({
      field: "Website",
      current_value: hotel.website || null,
      proposed_value: url,
      reason: "Canonical official property URL from Exact/High directory match",
      priority: "high",
      improvesMatch: "official URL bind → Exact",
    });
  }

  if (propertyId && !hotel.propertyId && !hotel.marsha && !hotel.ctyhocn) {
    proposals.push({
      field: "Property ID",
      current_value: null,
      proposed_value: propertyId,
      reason: "Official brand property code / MARSHA / mnemonic",
      priority: "high",
      improvesMatch: "property ID → Exact",
    });
  }

  if (observation.city && !hotel.city) {
    proposals.push({
      field: "city",
      current_value: hotel.city || null,
      proposed_value: observation.city,
      reason: "Normalized city from official directory (not state-level labels)",
      priority: "high",
      improvesMatch: "city hard-gate for Exact/High",
    });
  }

  if (observation.country && hotel.country && observation.country !== hotel.country) {
    // only propose if soft mismatch
    proposals.push({
      field: "country",
      current_value: hotel.country,
      proposed_value: observation.country,
      reason: "Review country label consistency with official directory",
      priority: "medium",
      recommended_action: "Review",
    });
  }

  if (observation.officialHotelName && observation.officialHotelName !== hotel.name) {
    proposals.push({
      field: "known_alias",
      current_value: hotel.name,
      proposed_value: observation.officialHotelName,
      reason: "Official directory display name as alias (do not overwrite census name without steward)",
      priority: "medium",
      recommended_action: "Review",
    });
  }

  if (/marriott\.com/i.test(url)) {
    const marsha = marshaFromMarriottWebsite(url) || observation.rawSignals?.marsha;
    if (marsha) {
      proposals.push({
        field: "MARSHA",
        current_value: hotel.marsha || null,
        proposed_value: marsha,
        reason: "Marriott MARSHA from official URL",
        priority: "high",
        improvesMatch: "MARSHA → Exact",
      });
    }
  }

  if (/ihg\.com/i.test(url)) {
    const brandSeg = ihgBrandFromUrl(url);
    if (brandSeg) {
      proposals.push({
        field: "brand_url_segment",
        current_value: canonicalizeObservedBrand(hotel.currentBrand || ""),
        proposed_value: brandSeg,
        reason: "IHG URL brand segment for contamination checks",
        priority: "medium",
      });
    }
  }

  return {
    hotelId: hotel.hotelId || hotel.recordId,
    hotelName: hotel.name,
    matchLevel: entityMatch.level,
    officialUrl: url || null,
    proposals,
    recommended_action: proposals.some((p) => p.priority === "high") ? "Review" : "No Change",
    note: "PROPOSALS ONLY — no Airtable write",
  };
}

/**
 * Batch identity proposals from freshness results.
 * @param {object[]} results - checkHotelFreshness outputs
 */
export function batchIdentityEnrichmentProposals(results) {
  return (results || []).map((r) =>
    proposeIdentityEnrichment(r.hotel || {}, r.observation || {}, r.entityMatch || {})
  );
}

export { assessEntityMatch };
