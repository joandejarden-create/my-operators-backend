/**
 * Packet 2.8C-2 — Alias compiler + exact property match + claim/evidence binding + sufficiency.
 */

export const CLAIM_BINDING_VERSION = "claim-evidence-binding-v1";

export function compileHotelAliases(input = {}) {
  const aliases = new Set();
  const add = (v) => {
    const s = String(v || "").trim();
    if (s) aliases.add(s);
  };
  add(input.hotel_name);
  add(input.canonical_trading_name);
  for (const x of input.former_names || []) add(x);
  for (const x of input.former_brands || []) add(x);
  for (const x of input.alt_names || []) add(x);
  for (const x of input.project_names || []) add(x);
  for (const x of input.legal_entity_candidates || []) add(x);
  if (input.city) add(`${input.hotel_name} ${input.city}`);
  if (input.address) add(input.address);
  // Common MX/BM patterns from production-known facts only (caller-supplied)
  return {
    current_hotel_name: input.hotel_name || null,
    former_hotel_names: input.former_names || [],
    former_brands: input.former_brands || [],
    project_names: input.project_names || [],
    legal_entity_candidates: input.legal_entity_candidates || [],
    city: input.city || null,
    address: input.address || null,
    all_aliases: [...aliases],
  };
}

export function exactPropertyMatch({ text = "", aliases = [], city = null, rooms = null } = {}) {
  const blob = String(text || "").toLowerCase();
  const hits = [];
  for (const a of aliases) {
    const n = String(a || "").toLowerCase();
    if (n.length >= 4 && blob.includes(n)) hits.push(a);
  }
  const cityHit = city ? blob.includes(String(city).toLowerCase()) : false;
  const roomsHit =
    rooms != null && new RegExp(`\\b${Number(rooms)}\\b`).test(blob) ? true : false;
  const matched = hits.length > 0 || (cityHit && roomsHit);
  const confidence =
    hits.length >= 2 || (hits.length === 1 && (cityHit || roomsHit))
      ? "HIGH"
      : hits.length === 1
        ? "PROBABLE"
        : matched
          ? "LOW"
          : "NONE";
  return {
    matched,
    confidence,
    alias_hits: hits,
    city_hit: cityHit,
    rooms_hit: roomsHit,
    reject_adjacent: !matched,
  };
}

export function bindClaimToEvidence(claim = {}, evidence = {}) {
  const fragment =
    evidence.fragment ||
    evidence.excerpt ||
    extractFragment(evidence.text || "", claim.claim_text || claim.entities?.[0] || "");
  return {
    version: CLAIM_BINDING_VERSION,
    claim_id: claim.claim_id || `claim_${Date.now().toString(36)}`,
    source_id: evidence.source_id || evidence.url || null,
    evidence_fragment: fragment,
    relationship_type: claim.relationship_type || null,
    event: claim.event || null,
    temporal_status: claim.temporal_status || "UNKNOWN",
    property_match: evidence.property_match || null,
    source_family: evidence.source_family || null,
    document_value: evidence.document_value || null,
  };
}

function extractFragment(text, needle) {
  const t = String(text || "");
  const n = String(needle || "").slice(0, 48);
  if (!t || !n) return t.slice(0, 280);
  const idx = t.toLowerCase().indexOf(n.toLowerCase().slice(0, 24));
  if (idx < 0) return t.slice(0, 280);
  return t.slice(Math.max(0, idx - 80), Math.min(t.length, idx + 200));
}

/**
 * Evidence sufficiency — EXACT/EQUIVALENT capable?
 */
export function assessEvidenceSufficiency(claim = {}, binding = {}, opts = {}) {
  const family = binding.source_family || opts.source_family || "unknown";
  const authority =
    ["issuer_filing", "bmv", "annual_report", "securities_filing", "government", "brand_first_party", "owner_first_party"].includes(
      family
    )
      ? "HIGH"
      : ["hotel_first_party", "operator_first_party", "trade_press"].includes(family)
        ? "MEDIUM"
        : "LOW";
  const directness = binding.evidence_fragment && claim.claim_text ? "DIRECT" : "WEAK";
  const identity = binding.property_match?.matched ? "MATCHED" : "UNMATCHED";
  const temporal = claim.temporal_status && claim.temporal_status !== "UNKNOWN" ? "PRESENT" : "MISSING";
  const independence = (claim.source_urls || []).length >= 2 ? "MULTI" : "SINGLE";
  const contradiction = opts.contradicted ? "CONFLICTED" : "CLEAR";

  const exactCapable =
    authority !== "LOW" &&
    directness === "DIRECT" &&
    identity !== "UNMATCHED" &&
    temporal === "PRESENT" &&
    contradiction === "CLEAR" &&
    !opts.force_partial;

  return {
    authority,
    directness,
    identity,
    temporal_match: temporal,
    independence,
    contradiction_status: contradiction,
    exact_or_equivalent_capable: exactCapable ? "YES" : "NO",
    reasons: exactCapable
      ? ["authority_ok", "direct_fragment", "property_matched", "temporal_present"]
      : [
          authority === "LOW" ? "weak_authority" : null,
          directness !== "DIRECT" ? "weak_directness" : null,
          identity === "UNMATCHED" ? "property_unmatched" : null,
          temporal !== "PRESENT" ? "temporal_missing" : null,
          contradiction === "CONFLICTED" ? "conflicted" : null,
        ].filter(Boolean),
  };
}
