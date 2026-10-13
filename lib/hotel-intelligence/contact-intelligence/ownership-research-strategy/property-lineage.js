/**
 * Property identity lineage — current vs historical marketing identity.
 */
import { PROPERTY_LINEAGE_STATE } from "./constants.js";

function norm(s) {
  return String(s || "")
    .normalize("NFD")
    .replace(/\p{M}/gu, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function nameTokens(s) {
  return norm(s)
    .split(/\s+/)
    .filter((t) => t.length >= 3 && !/^(hotel|pousada|the|by|sao|paulo)$/.test(t));
}

function tokenOverlap(a, b) {
  const A = new Set(nameTokens(a));
  const B = new Set(nameTokens(b));
  if (!A.size || !B.size) return 0;
  let hit = 0;
  for (const t of A) if (B.has(t)) hit += 1;
  return hit / Math.max(A.size, B.size);
}

/**
 * Build lineage record from hotel seed + optional observed alternate identities.
 * Does NOT overwrite historical aliases — appends only.
 *
 * @param {object} hotel
 * @param {{ observed_current_name?: string, observed_brand?: string, evidence?: object[] }} [opts]
 */
export function assessPropertyLineage(hotel = {}, opts = {}) {
  const propertyId = hotel.hotel_id || hotel.property_id || null;
  const hpcName = hotel.hotel_name || hotel.official_name || hotel.property_name || "";
  const observed = opts.observed_current_name || null;
  const historicalNames = [
    ...(Array.isArray(hotel.historical_names) ? hotel.historical_names : []),
    ...(Array.isArray(opts.preserve_historical) ? opts.preserve_historical : []),
  ].filter(Boolean);

  let lineage_state = PROPERTY_LINEAGE_STATE.CURRENT_IDENTITY;
  let current_hotel_name = hpcName;
  let same_physical_property = true;
  const evidence = [...(opts.evidence || [])];
  let hpc_current_name_review = false;

  if (observed && hpcName && tokenOverlap(observed, hpcName) < 0.35) {
    // Distinct marketing name at same address → rebrand / rename
    lineage_state = /meli[aá]|ibis|tryp|hilton|marriott|hyatt/i.test(observed)
      ? PROPERTY_LINEAGE_STATE.REBRANDED_PROPERTY
      : PROPERTY_LINEAGE_STATE.RENAMED_PROPERTY;
    current_hotel_name = observed;
    if (!historicalNames.map(norm).includes(norm(hpcName))) {
      historicalNames.push(hpcName);
    }
    hpc_current_name_review = true;
    evidence.push({
      kind: "NAME_MISMATCH_SAME_ADDRESS_CANDIDATE",
      hpc_name: hpcName,
      observed_current_name: observed,
    });
  } else if (!hpcName) {
    lineage_state = PROPERTY_LINEAGE_STATE.IDENTITY_UNCERTAIN;
    same_physical_property = false;
  }

  // Closed / demolished cues from text evidence
  const blob = `${opts.closure_text || ""}`;
  if (/\b(demolid|demolished|redevelop|empreendimento\s+residencial)\b/i.test(blob)) {
    lineage_state = PROPERTY_LINEAGE_STATE.DEMOLISHED_OR_REDEVELOPED;
  } else if (/\b(fechad[oa]|closed\s+permanently|encerrad[oa])\b/i.test(blob)) {
    lineage_state = PROPERTY_LINEAGE_STATE.CLOSED_PROPERTY;
  }

  return {
    property_id: propertyId,
    current_hotel_name,
    historical_names: [...new Set(historicalNames)],
    lineage_state,
    same_physical_property,
    current_brand: opts.observed_brand || hotel.brand || null,
    historical_brand: hotel.historical_brand || null,
    current_operator: opts.observed_operator || hotel.operator || null,
    historical_operator: hotel.historical_operator || null,
    address: hotel.address || hotel.street_address || null,
    city: hotel.city || null,
    state_region: hotel.state_region || hotel.state || null,
    country: hotel.country || null,
    latitude: hotel.latitude ?? null,
    longitude: hotel.longitude ?? null,
    official_url: hotel.website || hotel.official_website || null,
    effective_dates: opts.effective_dates || null,
    hpc_current_name_review,
    note:
      "HPC name matching a request hint is not sufficient evidence that marketing identity is current.",
    evidence,
  };
}

/**
 * Merge a newly observed historical alias without dropping prior ones.
 */
export function appendHistoricalName(lineage, name) {
  if (!lineage || !name) return lineage;
  const n = String(name).trim();
  if (!n) return lineage;
  const list = Array.isArray(lineage.historical_names) ? [...lineage.historical_names] : [];
  if (!list.map(norm).includes(norm(n)) && norm(n) !== norm(lineage.current_hotel_name)) {
    list.push(n);
  }
  return { ...lineage, historical_names: list };
}
