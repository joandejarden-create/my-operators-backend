/**
 * Shared Hotel Property Census full onboarding contract V1.
 * Same gate for ADP, GDI, Brand Explorer — identity alone ≠ enrichment complete.
 */

export const HPC_FULL_ONBOARDING_CONTRACT_V1 = "HPC_FULL_ONBOARDING_CONTRACT_V1";
export const HPC_TABLE_NAME = "Hotel Property Census";
export const HPC_TABLE_ID = "tbl9aY5ijiuIzzWam";

export const HPC_ONBOARDING_STAGES = Object.freeze([
  "HOTEL_DISCOVERED",
  "IDENTITY_RESOLUTION",
  "EXISTING_RECORD_FOUND_OR_NEW_RECORD_REQUIRED",
  "HPC_SCHEMA_COVERAGE_PLAN",
  "EXISTING_DATA_RECOVERY",
  "FIELD_RESEARCH_AND_ENRICHMENT",
  "FIELD_VALIDATION",
  "AIRTABLE_UPSERT",
  "AIRTABLE_READBACK",
  "HPC_COMPLETENESS_CERTIFICATION",
  "DOWNSTREAM_PRODUCT_READY",
]);

export const HPC_CERT_STATES = Object.freeze({
  HPC_RECORD_CREATED: "HPC_RECORD_CREATED",
  HPC_MINIMUM_OPERATIONAL_READY: "HPC_MINIMUM_OPERATIONAL_READY",
  HPC_ENRICHMENT_COMPLETE: "HPC_ENRICHMENT_COMPLETE",
  HPC_CERTIFIED_FOR_ADP: "HPC_CERTIFIED_FOR_ADP",
  HPC_BLOCKED: "HPC_BLOCKED",
});

export const FIELD_STATUS = Object.freeze({
  VERIFIED_POPULATED: "VERIFIED_POPULATED",
  VERIFIED_EXISTING: "VERIFIED_EXISTING",
  RESEARCH_REQUIRED: "RESEARCH_REQUIRED",
  SOURCE_CONFLICT: "SOURCE_CONFLICT",
  NOT_APPLICABLE: "NOT_APPLICABLE",
  NOT_PUBLICLY_AVAILABLE: "NOT_PUBLICLY_AVAILABLE",
  BLOCKED_AUTHORIZATION: "BLOCKED_AUTHORIZATION",
  TECHNICAL_MAPPING_FAILURE: "TECHNICAL_MAPPING_FAILURE",
  COMPUTED_DEPENDENT: "COMPUTED_DEPENDENT",
  EMPTY_NO_REASON: "EMPTY_NO_REASON",
  /** Present but geographically invalid (e.g. 0/0 Null Island) — not certifiable */
  INVALID_GEOGRAPHIC_VALUE: "INVALID_GEOGRAPHIC_VALUE",
});

/** Core identity fields required for MINIMUM_OPERATIONAL_READY */
export const HPC_MINIMUM_IDENTITY_FIELDS = Object.freeze([
  "Property Name",
  "Country",
  "City",
  "Current Brand",
  "Affiliation Status",
  "Family / Source Family",
]);

/** Fields essential before ADP monitoring / attributes (CERTIFIED_FOR_ADP) */
export const HPC_ADP_ESSENTIAL_FIELDS = Object.freeze([
  ...HPC_MINIMUM_IDENTITY_FIELDS,
  "State / Region",
  "Official Property URL",
  "Property Type",
  "Market",
  "Latitude",
  "Longitude",
  "Rooms / Keys",
  "Operator / Management Company",
]);

/** Enrichment targets — research when empty (not all required for ADP gate) */
export const HPC_ENRICHMENT_TARGET_FIELDS = Object.freeze([
  "Address",
  "Postal Code",
  "Phone",
  "Hotel Description - Source Text",
  "Amenities - Source Text",
  "Amenities - Structured Tags",
  "F&B Flag",
  "Meeting Space Flag",
  "Resort / Leisure Flag",
  "Spa Flag",
  "Pool Flag",
  "Opening Date",
  "Owner Name",
  "Brand Family",
  "Canonical Property Name",
  "Submarket",
  "Continent",
  "Sub-Continent",
]);

export const COMPUTED_FIELD_TYPES = Object.freeze([
  "formula",
  "rollup",
  "lookup",
  "count",
  "createdTime",
  "lastModifiedTime",
  "createdBy",
  "lastModifiedBy",
  "autoNumber",
  "button",
  "externalSyncSource",
]);

/**
 * ADP profile → HPC field mapping (null-fill candidates only).
 * Values must come from governed profile evidence — never invented.
 */
export const ADP_PROFILE_TO_HPC_FIELD_MAP = Object.freeze([
  { profileKey: "name", hpcField: "Property Name", strength: "identity" },
  { profileKey: "name", hpcField: "Canonical Property Name", strength: "identity" },
  { profileKey: "brand", hpcField: "Current Brand", strength: "identity" },
  { profileKey: "parentCompany", hpcField: "Brand Family", strength: "identity" },
  { profileKey: "parentCompany", hpcField: "Family / Source Family", strength: "identity", transform: "source_family" },
  { profileKey: "city", hpcField: "City", strength: "geo" },
  { profileKey: "market", hpcField: "Market", strength: "geo" },
  { profileKey: "country", hpcField: "Country", strength: "geo" },
  { profileKey: "state", hpcField: "State / Region", strength: "geo" },
  { profileKey: "region", hpcField: "State / Region", strength: "geo" },
  { profileKey: "address", hpcField: "Address", strength: "geo" },
  { profileKey: "phone", hpcField: "Phone", strength: "contact" },
  { profileKey: "officialWebsite", hpcField: "Official Property URL", strength: "contact" },
  { profileKey: "officialPropertyPageUrl", hpcField: "Official Property URL", strength: "contact" },
  { profileKey: "website", hpcField: "Official Property URL", strength: "contact" },
  { profileKey: "officialBrandDomain", hpcField: "Official Property URL", strength: "contact", transform: "https_host" },
  { profileKey: "rooms", hpcField: "Rooms / Keys", strength: "inventory" },
  { profileKey: "keys", hpcField: "Rooms / Keys", strength: "inventory" },
  { profileKey: "operator", hpcField: "Operator / Management Company", strength: "ops" },
  { profileKey: "managementCompany", hpcField: "Operator / Management Company", strength: "ops" },
  { profileKey: "owner", hpcField: "Owner Name", strength: "ops" },
  { profileKey: "latitude", hpcField: "Latitude", strength: "geo" },
  { profileKey: "longitude", hpcField: "Longitude", strength: "geo" },
  { profileKey: "lat", hpcField: "Latitude", strength: "geo" },
  { profileKey: "lng", hpcField: "Longitude", strength: "geo" },
  { profileKey: "propertyType", hpcField: "Property Type", strength: "asset" },
  { profileKey: "submarket", hpcField: "Submarket", strength: "geo" },
]);

const SOURCE_FAMILY_NORMALIZE = Object.freeze({
  marriott: "Marriott",
  "marriott international": "Marriott",
  hilton: "Hilton",
  "hilton worldwide": "Hilton",
  ihg: "IHG",
  "intercontinental hotels group": "IHG",
  choice: "Choice",
  "choice hotels": "Choice",
  "choice hotels international": "Choice",
  accor: "Accor",
  wyndham: "Wyndham",
  "wyndham hotels": "Wyndham",
  preferred: "Preferred",
  independent: "Independent",
  hyatt: "Independent", // Hyatt not in Family select — leave Research unless Independent
});

export function isFieldEmpty(value) {
  if (value == null) return true;
  if (typeof value === "string" && !value.trim()) return true;
  if (Array.isArray(value) && value.length === 0) return true;
  return false;
}

export function classifyFieldStatus({
  fieldName,
  value,
  schemaField,
  applicable = true,
  pairedLatitude,
  pairedLongitude,
}) {
  if (schemaField?.computed || COMPUTED_FIELD_TYPES.includes(schemaField?.type)) {
    return {
      status: FIELD_STATUS.COMPUTED_DEPENDENT,
      reason: "Computed/formula/lookup — populate underlying sources",
    };
  }
  if (!applicable) {
    return { status: FIELD_STATUS.NOT_APPLICABLE, reason: "Not applicable for this property class" };
  }
  // Coordinates: non-null alone is insufficient — reject Null Island / missing pair
  if (fieldName === "Latitude" || fieldName === "Longitude") {
    const lat = fieldName === "Latitude" ? value : pairedLatitude;
    const lng = fieldName === "Longitude" ? value : pairedLongitude;
    const latN = lat == null || lat === "" ? null : Number(lat);
    const lngN = lng == null || lng === "" ? null : Number(lng);
    const bothFinite = Number.isFinite(latN) && Number.isFinite(lngN);
    if (!bothFinite) {
      if (isFieldEmpty(value)) {
        return {
          status: FIELD_STATUS.EMPTY_NO_REASON,
          reason: "Coordinate missing — not certifiable",
        };
      }
      return {
        status: FIELD_STATUS.INVALID_GEOGRAPHIC_VALUE,
        reason: "Coordinate pair incomplete",
      };
    }
    if (latN === 0 && lngN === 0) {
      return {
        status: FIELD_STATUS.INVALID_GEOGRAPHIC_VALUE,
        reason: "Null Island 0/0 is not a valid hotel location",
      };
    }
    if (latN < -90 || latN > 90 || lngN < -180 || lngN > 180) {
      return {
        status: FIELD_STATUS.INVALID_GEOGRAPHIC_VALUE,
        reason: "Coordinate out of WGS84 range",
      };
    }
    return {
      status: FIELD_STATUS.VERIFIED_EXISTING,
      reason: "Coordinate pair present and within WGS84 range (country checks in geographic integrity)",
    };
  }
  if (!isFieldEmpty(value)) {
    return { status: FIELD_STATUS.VERIFIED_EXISTING, reason: "Present on HPC record" };
  }
  return {
    status: FIELD_STATUS.EMPTY_NO_REASON,
    reason: "Empty without explicit research disposition",
  };
}

/**
 * Certify HPC completeness for a record + field statuses.
 */
export function certifyHpcCompleteness({
  recordId,
  fieldStatuses = {},
  linkedToAdp = false,
} = {}) {
  const statuses = Object.values(fieldStatuses);
  const emptyNoReason = statuses.filter((s) => s.status === FIELD_STATUS.EMPTY_NO_REASON);
  const researchRequired = statuses.filter((s) => s.status === FIELD_STATUS.RESEARCH_REQUIRED);

  const minOk = HPC_MINIMUM_IDENTITY_FIELDS.every((f) => {
    const s = fieldStatuses[f];
    return (
      s &&
      (s.status === FIELD_STATUS.VERIFIED_EXISTING ||
        s.status === FIELD_STATUS.VERIFIED_POPULATED)
    );
  });

  const adpEssentialOk = HPC_ADP_ESSENTIAL_FIELDS.every((f) => {
    const s = fieldStatuses[f];
    return (
      s &&
      (s.status === FIELD_STATUS.VERIFIED_EXISTING ||
        s.status === FIELD_STATUS.VERIFIED_POPULATED ||
        s.status === FIELD_STATUS.NOT_APPLICABLE ||
        s.status === FIELD_STATUS.NOT_PUBLICLY_AVAILABLE) &&
      s.status !== FIELD_STATUS.INVALID_GEOGRAPHIC_VALUE
    );
  });
  const invalidGeo = statuses.filter(
    (s) => s.status === FIELD_STATUS.INVALID_GEOGRAPHIC_VALUE
  );
  // Numeric Lat/Lng alone never certify — 0/0 and incomplete pairs block ADP readiness
  const geoBlocked = invalidGeo.length > 0;

  const enrichmentTargetsAssessed = HPC_ENRICHMENT_TARGET_FIELDS.every(
    (f) => fieldStatuses[f]
  );
  const enrichmentTargetsOk = HPC_ENRICHMENT_TARGET_FIELDS.every((f) => {
    const s = fieldStatuses[f];
    if (!s) return false;
    return ![
      FIELD_STATUS.EMPTY_NO_REASON,
      FIELD_STATUS.RESEARCH_REQUIRED,
      FIELD_STATUS.TECHNICAL_MAPPING_FAILURE,
    ].includes(s.status);
  });
  const enrichmentComplete =
    minOk &&
    enrichmentTargetsAssessed &&
    enrichmentTargetsOk &&
    emptyNoReason.length === 0 &&
    researchRequired.every((s) =>
      [FIELD_STATUS.NOT_PUBLICLY_AVAILABLE, FIELD_STATUS.NOT_APPLICABLE].includes(
        s.status
      )
    );

  const adpEssentialOkGeo = adpEssentialOk && !geoBlocked;

  let certState = HPC_CERT_STATES.HPC_RECORD_CREATED;
  if (!recordId) certState = HPC_CERT_STATES.HPC_BLOCKED;
  else if (geoBlocked) certState = HPC_CERT_STATES.HPC_BLOCKED;
  else if (adpEssentialOkGeo && linkedToAdp) certState = HPC_CERT_STATES.HPC_CERTIFIED_FOR_ADP;
  else if (enrichmentComplete) certState = HPC_CERT_STATES.HPC_ENRICHMENT_COMPLETE;
  else if (minOk) certState = HPC_CERT_STATES.HPC_MINIMUM_OPERATIONAL_READY;

  return {
    contractVersion: HPC_FULL_ONBOARDING_CONTRACT_V1,
    recordId,
    certState,
    minimumOperationalReady: minOk && !geoBlocked,
    adpEssentialReady: adpEssentialOkGeo,
    enrichmentComplete: enrichmentComplete && !geoBlocked,
    certifiedForAdp: certState === HPC_CERT_STATES.HPC_CERTIFIED_FOR_ADP,
    geographicIntegrityBlocked: geoBlocked,
    invalidGeographicFieldCount: invalidGeo.length,
    emptyNoReasonCount: emptyNoReason.length,
    researchRequiredCount: researchRequired.length,
    linkedToAdp,
    assessedAt: new Date().toISOString(),
  };
}

const COUNTRY_NORMALIZE = Object.freeze({
  us: "United States",
  usa: "United States",
  "united states": "United States",
  "united states of america": "United States",
  mx: "Mexico",
  mexico: "Mexico",
  do: "Dominican Republic",
  "dominican republic": "Dominican Republic",
  bm: "Bermuda",
  bermuda: "Bermuda",
  co: "Colombia",
  colombia: "Colombia",
  it: "Italy",
  italy: "Italy",
  es: "Spain",
  spain: "Spain",
  de: "Germany",
  germany: "Germany",
  gd: "Grenada",
  grenada: "Grenada",
  gb: "United Kingdom",
  uk: "United Kingdom",
  "united kingdom": "United Kingdom",
});

export function normalizeCountryForHpc(value) {
  if (value == null || value === "") return null;
  const key = String(value).trim().toLowerCase();
  return COUNTRY_NORMALIZE[key] || String(value).trim();
}

export function extractProfileValue(profile, key, transform) {
  if (!profile) return null;
  let v =
    profile[key] ??
    profile?.geography?.[key] ??
    profile?.contact?.[key] ??
    profile?.fields?.[key] ??
    profile?.identity?.[key] ??
    null;
  if (v == null || v === "") return null;
  if (transform === "https_host") {
    const host = String(v).replace(/^https?:\/\//i, "").split("/")[0];
    if (!host) return null;
    return `https://${host}`;
  }
  if (transform === "source_family") {
    const key = String(v).trim().toLowerCase();
    const mapped = SOURCE_FAMILY_NORMALIZE[key];
    // Only return when mapped to a real Family select option (not forced Independent for Hyatt)
    if (!mapped || (mapped === "Independent" && /hyatt/i.test(String(v)))) return null;
    return mapped;
  }
  if (key === "country") {
    return normalizeCountryForHpc(v);
  }
  if (typeof v === "number" && Number.isFinite(v)) return v;
  if (typeof v === "boolean") return v;
  return String(v).trim() || null;
}

/**
 * Build null-fill proposals from ADP profile → empty HPC fields.
 */
export function proposeNullFillsFromAdpProfile(profile, hpcFields = {}) {
  const proposals = [];
  const seen = new Set();
  for (const map of ADP_PROFILE_TO_HPC_FIELD_MAP) {
    if (seen.has(map.hpcField)) continue;
    const existing = hpcFields[map.hpcField];
    if (!isFieldEmpty(existing)) continue;
    const proposed = extractProfileValue(profile, map.profileKey, map.transform);
    if (proposed == null || proposed === "") continue;
    seen.add(map.hpcField);
    proposals.push({
      field: map.hpcField,
      value: proposed,
      source: `adp_profile.${map.profileKey}`,
      strength: map.strength,
      mode: "NULL_FILL",
      confidence: "PROFILE_GOVERNED",
    });
  }

  // Affiliation Status from affiliation/loyalty text when empty
  if (isFieldEmpty(hpcFields["Affiliation Status"])) {
    const aff = String(profile?.affiliation || profile?.portfolioLoyaltyIdentity || "").trim();
    if (/bonvoy|marriott/i.test(aff)) {
      proposals.push({
        field: "Affiliation Status",
        value: "Branded",
        source: "adp_profile.affiliation",
        strength: "identity",
        mode: "NULL_FILL",
        confidence: "PROFILE_INFERRED_BRANDED",
      });
    } else if (/independent/i.test(aff) || /independent/i.test(String(profile?.brand || ""))) {
      proposals.push({
        field: "Affiliation Status",
        value: "Independent",
        source: "adp_profile.affiliation",
        strength: "identity",
        mode: "NULL_FILL",
        confidence: "PROFILE_INFERRED_INDEPENDENT",
      });
    }
  }

  return proposals;
}
