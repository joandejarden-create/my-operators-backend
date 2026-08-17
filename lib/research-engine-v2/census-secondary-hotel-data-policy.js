/**
 * Governed secondary hotel-data policy (Rooms / Phone gates).
 *
 * Founder 2026-08-07: secondary sources approved for Rooms / Keys only.
 * Phone remains official-source-only until separate approval.
 */

export const SECONDARY_HOTEL_DATA_POLICY_VERSION =
  "census-secondary-hotel-data-policy-v1";

/** Approved Rooms secondary categories (founder). */
export const APPROVED_ROOMS_SECONDARY_CATEGORIES = Object.freeze([
  "official_parent_brand_source",
  "official_hotel_website",
  "official_factsheet",
  "official_press_release_or_opening_announcement",
  "owner_developer_reit_fund_asset_manager_website",
  "tourism_board_convention_bureau_destination_authority",
  "reputable_hospitality_trade_publication",
  "licensed_hospitality_dataset_if_available",
  "steward_verified_source",
]);

/** Explicitly not approved for Rooms. */
export const FORBIDDEN_ROOMS_SECONDARY_CATEGORIES = Object.freeze([
  "ota_random",
  "google_panel",
  "tripadvisor",
  "ai_estimate",
  "building_size_floor_inference",
  "meeting_room_count",
  "residence_count_unless_hotel_keys",
  "sitewide_default",
  "vague_marketing_description",
]);

/**
 * Airtable Rooms Source Type select options (schema v1.1.4).
 * Government / tourism-board secondaries map to trusted_secondary_source.
 */
export const MAP_ROOMS_SOURCE_TYPE = Object.freeze({
  official_property_page: "official_property_page",
  official_brand_directory: "official_brand_directory",
  official_hotel_website: "official_hotel_website",
  official_press_release: "official_press_release",
  official_development_page: "official_development_page",
  trusted_secondary_source: "trusted_secondary_source",
  steward_review: "steward_review",
});

/** Founder Wave 2 evidence tier labels (Airtable Rooms Evidence Tier select). */
export const ROOMS_EVIDENCE_TIER_SELECT = Object.freeze({
  TIER_1: "Tier 1 Official Parent / Brand Source",
  TIER_2: "Tier 2 Official Hotel Website",
  TIER_3: "Tier 3 Official Press Release",
  TIER_4: "Tier 4 Owner / Developer Source",
  TIER_5: "Tier 5 Tourism Board / Destination Authority",
  TIER_6: "Tier 6 Trusted Industry Source",
  TIER_7: "Tier 7 Steward Verified",
});

/** Logical evidence tiers (codes). Prefer Airtable select via mapEvidenceTierCodeToSelect. */
export const ROOMS_EVIDENCE_TIER = Object.freeze({
  OFFICIAL_HIGH: "official_high",
  OFFICIAL_HOTEL_WEBSITE: "official_hotel_website",
  SECONDARY_TOURISM_BOARD: "secondary_tourism_board_destination_authority",
  SECONDARY_LICENSED_DATASET: "secondary_licensed_hospitality_dataset",
  SECONDARY_TRADE_PUBLICATION: "secondary_hospitality_trade_publication",
  SECONDARY_OWNER_DEVELOPER: "secondary_owner_developer_website",
  STEWARD_VERIFIED: "steward_verified",
  CONFLICT_HOLD: "conflict_steward_hold",
});

export const PHONE_POLICY_REASON = Object.freeze({
  SECONDARY_NOT_APPROVED: "phone_secondary_source_policy_not_approved",
  CENTRAL_RESERVATION: "phone_central_reservation_rejected",
  OFFICIAL_ONLY: "phone_official_source_only",
  OFFICIAL_SOURCE_MISSING: "phone_official_source_missing",
});

/**
 * @param {NodeJS.ProcessEnv|Record<string,string|undefined>} [env]
 * @param {{ roomsEvidenceTierFieldExists?: boolean }} [opts]
 */
export function resolveSecondaryHotelDataPolicy(env = process.env, opts = {}) {
  const hotel = String(env.ENABLE_SECONDARY_HOTEL_DATA_SOURCES || "0") === "1";
  const rooms = String(env.ENABLE_SECONDARY_ROOMS_SOURCES || "0") === "1";
  const phone = String(env.ENABLE_SECONDARY_PHONE_SOURCES || "0") === "1";
  const tierExists = opts.roomsEvidenceTierFieldExists === true;
  return {
    version: SECONDARY_HOTEL_DATA_POLICY_VERSION,
    enable_secondary_hotel_data_sources: hotel,
    enable_secondary_rooms_sources: hotel && rooms,
    enable_secondary_phone_sources: hotel && phone,
    rooms_approved: APPROVED_ROOMS_SECONDARY_CATEGORIES,
    rooms_forbidden: FORBIDDEN_ROOMS_SECONDARY_CATEGORIES,
    phone_policy: phone
      ? "secondary_phone_enabled_but_must_still_reject_central_ota_google"
      : PHONE_POLICY_REASON.SECONDARY_NOT_APPROVED,
    rooms_evidence_tier_field_exists: tierExists,
    schema_gaps: tierExists
      ? []
      : [
          {
            field: "Rooms Evidence Tier",
            status: "missing_from_hotel_property_census",
            fallback: "Rooms Notes evidence_tier=… prefix",
          },
        ],
  };
}

/**
 * Resolve Rooms confidence for a source hit.
 * Founder 2026-08-15: government/tourism lodging registries may be HIGH when
 * exact property identity match is clear (identity_match_high / high match_sim).
 * @param {{
 *   category?: string,
 *   is_official?: boolean,
 *   identity_match_high?: boolean,
 *   match_sim?: number,
 *   match_confidence?: string,
 * }} hit
 */
export function resolveRoomsConfidenceForSource(hit) {
  if (hit?.is_official) return "High";
  if (hit?.category === "steward_verified_source") return "High";
  const registryLike =
    hit?.category === "tourism_board_convention_bureau_destination_authority" ||
    hit?.category === "licensed_hospitality_dataset_if_available";
  if (registryLike) {
    const sim = Number(hit?.match_sim);
    if (
      hit?.identity_match_high === true ||
      String(hit?.match_confidence || "").toLowerCase() === "high" ||
      (Number.isFinite(sim) && sim >= 0.85)
    ) {
      return "High";
    }
    return "Medium"; // ROOMS_CANDIDATE — corroboration later
  }
  if (
    hit?.category === "reputable_hospitality_trade_publication" ||
    hit?.category === "owner_developer_reit_fund_asset_manager_website"
  ) {
    return "Medium";
  }
  return "Medium";
}

/**
 * Map logical category → Airtable Rooms Source Type option.
 * @param {{ is_official?: boolean, category?: string, conflict?: boolean }} hit
 */
export function resolveRoomsSourceTypeForAirtable(hit) {
  if (hit?.conflict) return MAP_ROOMS_SOURCE_TYPE.steward_review;
  if (hit?.is_official) {
    if (hit.category === "official_factsheet") {
      return MAP_ROOMS_SOURCE_TYPE.official_property_page;
    }
    if (hit.category === "official_press_release_or_opening_announcement") {
      return MAP_ROOMS_SOURCE_TYPE.official_press_release;
    }
    if (hit.category === "official_parent_brand_source") {
      return MAP_ROOMS_SOURCE_TYPE.official_brand_directory;
    }
    if (hit.category === "official_hotel_website") {
      return MAP_ROOMS_SOURCE_TYPE.official_hotel_website;
    }
    return MAP_ROOMS_SOURCE_TYPE.official_property_page;
  }
  if (hit?.category === "steward_verified_source") {
    return MAP_ROOMS_SOURCE_TYPE.steward_review;
  }
  return MAP_ROOMS_SOURCE_TYPE.trusted_secondary_source;
}

/**
 * @param {{
 *   evidence_tier: string,
 *   category: string,
 *   adapter?: string,
 *   match_sim?: number,
 *   note?: string,
 * }} meta
 */
export function buildRoomsProvenanceNotes(meta) {
  const parts = [
    `evidence_tier=${meta.evidence_tier}`,
    `category=${meta.category}`,
  ];
  if (meta.adapter) parts.push(`adapter=${meta.adapter}`);
  if (meta.match_sim != null) parts.push(`match_sim=${meta.match_sim}`);
  if (meta.note) parts.push(String(meta.note).slice(0, 240));
  return parts.join(" | ");
}

/**
 * Classify a phone gap under current policy.
 * @param {{ has_phone?: boolean, is_central?: boolean, policy?: ReturnType<typeof resolveSecondaryHotelDataPolicy> }} opts
 */
export function classifyPhoneUnderSecondaryPolicy(opts = {}) {
  const policy = opts.policy || resolveSecondaryHotelDataPolicy();
  if (opts.is_central) {
    return {
      status: PHONE_POLICY_REASON.CENTRAL_RESERVATION,
      write: false,
      reason: PHONE_POLICY_REASON.CENTRAL_RESERVATION,
    };
  }
  if (!opts.has_phone) {
    if (!policy.enable_secondary_phone_sources) {
      return {
        status: PHONE_POLICY_REASON.SECONDARY_NOT_APPROVED,
        write: false,
        reason: PHONE_POLICY_REASON.OFFICIAL_SOURCE_MISSING,
      };
    }
  }
  if (opts.has_phone) {
    return { status: "phone_present", write: false, reason: null };
  }
  if (!policy.enable_secondary_phone_sources) {
    return {
      status: PHONE_POLICY_REASON.SECONDARY_NOT_APPROVED,
      write: false,
      reason: PHONE_POLICY_REASON.SECONDARY_NOT_APPROVED,
    };
  }
  return {
    status: PHONE_POLICY_REASON.OFFICIAL_ONLY,
    write: false,
    reason: "secondary_phone_enabled_but_official_extract_required",
  };
}
