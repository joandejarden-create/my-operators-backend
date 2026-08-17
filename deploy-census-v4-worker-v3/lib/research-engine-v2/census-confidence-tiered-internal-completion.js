/**
 * Confidence-tiered internal Hotel Property Census completion policy.
 * Medium writes allowed for internal census only — not public-facing.
 *
 * Schema note: Phone Confidence / Phone Source URL fields do NOT exist yet.
 * Medium phone provenance is stored in Notes for Steward until schema add.
 */

export const CONFIDENCE_TIERED_INTERNAL_POLICY_VERSION =
  "census-confidence-tiered-internal-completion-v1";

export const CONFIDENCE_TIER = Object.freeze({
  HIGH: "High",
  MEDIUM: "Medium",
  LOW: "Low",
  HOLD: "Hold",
});

export const PUBLIC_SAFE_STATUSES = Object.freeze({
  production_use_owner_facing: "Eligible for Brand Explorer Subset",
  public_display_approved: "Approved",
  radar_public: "Public Map Eligible",
});

export const INTERNAL_ONLY_INSERT_DEFAULTS = Object.freeze({
  "Production Use Status": "Census Only / Not Owner-Facing",
  "Public Display Review Status": "Hold",
  "Radar Display Status": "Hold",
  "Human Review Required": true,
  "Public Census Eligibility": "Not Eligible",
  "Data Confidence Tier": "Medium",
  "Source Confidence": "Medium",
  "Source Type": "other",
  "Enrichment Status": "Partial",
  "Enrichment Priority": "Medium",
});

/** Schema gaps blocking first-class phone provenance fields. */
export const PHONE_PROVENANCE_SCHEMA_TODO = Object.freeze([
  "Phone Confidence (singleSelect: High/Medium/Low/Hold) — MISSING",
  "Phone Source URL (url) — MISSING",
  "Phone Source Type (singleSelect) — MISSING",
]);

/**
 * Resolve confidence-tiered internal completion gates.
 * @param {NodeJS.ProcessEnv} [env]
 */
export function resolveConfidenceTieredInternalGates(env = process.env) {
  const internal =
    String(env.ENABLE_CENSUS_INTERNAL_MEDIUM_COMPLETION || "0").trim() === "1";
  const phone =
    String(env.ENABLE_DATAFORSEO_LOCAL_PHONE_WRITES || "0").trim() === "1";
  const address =
    String(env.ENABLE_DATAFORSEO_LOCAL_ADDRESS_WRITES || "0").trim() === "1";
  const mapboxMedium =
    String(env.ENABLE_MAPBOX_AFTER_MEDIUM_MATCH_HIGH_ADDRESS || "0").trim() ===
    "1";
  const mapboxAfter =
    String(env.ENABLE_MAPBOX_AFTER_VALIDATED_ADDRESS || "0").trim() === "1";
  const rooms =
    String(env.ENABLE_SECONDARY_ROOMS_SOURCES || "0").trim() === "1";
  const inserts =
    String(env.ENABLE_DATAFORSEO_LOCAL_INSERTS || "0").trim() === "1" &&
    String(env.ENABLE_HIGH_CONFIDENCE_INSERTS || "0").trim() === "1";
  const dfsCoords =
    String(env.ENABLE_DATAFORSEO_LOCAL_COORDINATE_WRITES || "0").trim() === "1";
  const secondaryPhone =
    String(env.ENABLE_SECONDARY_PHONE_SOURCES || "0").trim() === "1";

  const blockers = [];
  if (dfsCoords) {
    blockers.push("ENABLE_DATAFORSEO_LOCAL_COORDINATE_WRITES_must_be_0");
  }
  if (secondaryPhone) {
    blockers.push(
      "ENABLE_SECONDARY_PHONE_SOURCES_must_be_0_use_dataforseo_local_match_high_only"
    );
  }
  if (phone && !internal) {
    blockers.push(
      "ENABLE_DATAFORSEO_LOCAL_PHONE_WRITES_requires_ENABLE_CENSUS_INTERNAL_MEDIUM_COMPLETION"
    );
  }
  if (mapboxMedium && !mapboxAfter) {
    blockers.push(
      "ENABLE_MAPBOX_AFTER_MEDIUM_MATCH_HIGH_ADDRESS_requires_ENABLE_MAPBOX_AFTER_VALIDATED_ADDRESS"
    );
  }

  return {
    ok: blockers.length === 0,
    blockers,
    internal_medium_completion: internal,
    address_medium_writes: address,
    phone_medium_writes: internal && phone,
    mapbox_after_medium_address: mapboxMedium && mapboxAfter,
    rooms_secondary_writes: rooms,
    high_confidence_inserts: inserts,
    dataforseo_coordinate_writes: false,
    secondary_phone_writes: false,
    phone_provenance_schema_todo: PHONE_PROVENANCE_SCHEMA_TODO,
    public_exposure_of_medium: false,
    policy_version: CONFIDENCE_TIERED_INTERNAL_POLICY_VERSION,
  };
}

/**
 * Classify whether a census field value is public-safe vs internal-only.
 */
export function classifyFieldExposure(fieldName, confidence) {
  const conf = String(confidence || "").trim();
  const publicSafeFields = new Set([
    "Property Name",
    "Canonical Property Name",
    "Current Brand",
    "City",
    "Country",
    "Market",
    "Submarket",
  ]);
  if (conf === "High" && publicSafeFields.has(fieldName)) {
    return "public_safe_when_display_approved";
  }
  if (conf === "Medium" || conf === "Low" || conf === "Hold") {
    return "internal_only";
  }
  if (fieldName === "Phone") return "internal_only";
  return "internal_until_reviewed";
}

/**
 * Build structured phone provenance note (Phone Confidence field missing).
 */
export function buildPhoneProvenanceNote(meta = {}) {
  const parts = [
    "phone_provenance",
    `confidence=${meta.confidence || "Medium"}`,
    `source=${meta.source || "dataforseo_local_match_high"}`,
    meta.source_url ? `source_url=${meta.source_url}` : null,
    meta.place_id ? `place_id=${meta.place_id}` : null,
    meta.match_class ? `match=${meta.match_class}` : null,
    `public_exposure=false`,
    `reviewed=${meta.reviewed_date || new Date().toISOString().slice(0, 10)}`,
  ].filter(Boolean);
  return parts.join(" | ");
}

/**
 * Merge provenance into existing Notes for Steward without wiping steward text.
 */
export function mergeStewardPhoneNote(existing, note) {
  const prev = String(existing || "").trim();
  const next = String(note || "").trim();
  if (!next) return prev || null;
  if (!prev) return next;
  if (prev.includes("phone_provenance")) {
    return prev.replace(/phone_provenance[\s\S]*?(?=\n\n|$)/, next).trim();
  }
  return `${prev}\n\n${next}`.trim();
}
