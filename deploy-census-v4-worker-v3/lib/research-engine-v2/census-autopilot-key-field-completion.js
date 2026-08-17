/**
 * Autopilot Key Field Completion — matrix + queue for Hotel Property Census.
 *
 * Purpose: after discovery/insert, systematically classify and safely complete
 * foundational Census fields (High confidence only). Never Brand Setup / BE /
 * VIC / old Census / owner-operator-dates / Recent Momentum / validation fields.
 *
 * Production write target: Hotel Property Census only (tbl9aY5ijiuIzzWam).
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { MAP_FIRST_PASS } from "./production-census-first-pass-enrichment.js";
import { evaluateProviderReadiness } from "./production-census-description-extraction.js";
import {
  AUTOPILOT_ALLOWED_WRITE_FIELDS,
  AUTOPILOT_FORBIDDEN_FIELDS,
  AUTOPILOT_GEOCODE_FIELDS,
  isForbiddenAutopilotField,
} from "./census-autopilot-field-allowlist.js";
import {
  assertProductionCensusWriteTarget,
  BLOCKED_WRONG_CENSUS_TARGET,
  productionHotelPropertyCensus,
} from "./production-census-source-of-truth.js";
import { isPropertyLevelUrl } from "./production-census-description-extraction.js";
import {
  CANONICAL_PROPERTY_NAME_FIELD,
  CANONICAL_NAME_STATUS,
  CANONICAL_COMPLETION_STATUS,
  CANONICAL_NAME_VERSION,
  buildCanonicalDuplicateIndex,
  classifyCanonicalPropertyName,
  proposeCanonicalPropertyNameWrite,
  hasSafeDirtyMembershipSuffix,
} from "./census-canonical-property-name.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const KEY_FIELD_COMPLETION_VERSION =
  "census-autopilot-key-field-completion-v1";
export const KEY_FIELD_COMPLETION_QUEUE_ID = "key_field_completion";

export const KEY_FIELD_STATUS = Object.freeze({
  COMPLETE: "complete",
  MISSING_CAN_AUTOFILL: "missing_can_autofill",
  MISSING_NEEDS_SOURCE_ADAPTER: "missing_needs_source_adapter",
  MISSING_PROVIDER_BLOCKED: "missing_provider_blocked",
  MISSING_STEWARD_REVIEW: "missing_steward_review",
  MISSING_NOT_SUPPORTED: "missing_not_supported",
  INTENTIONALLY_BLOCKED: "intentionally_blocked",
});

export const KEY_FIELD_COMPLETION_STATUS = Object.freeze({
  READY: "production_census_key_field_completion_ready",
  READY_PROVIDER_BLOCKED:
    "production_census_key_field_completion_ready_provider_blocked",
  NEEDS_SOURCE_ADAPTERS:
    "production_census_key_field_completion_needs_source_adapters",
  BLOCKED: "production_census_key_field_completion_blocked",
});

/** Priority order for completion (founder-specified). */
export const KEY_FIELD_PRIORITY_GROUPS = Object.freeze([
  "core_identity",
  "source",
  "address",
  "state_region",
  "coordinates",
  "public_readiness",
  "classification",
  "rooms",
  "content",
]);

/**
 * Canonical matrix fields. `airtable` = Hotel Property Census field name.
 * `alias` notes founder wording when it differs from schema.
 */
export const KEY_FIELD_MATRIX = Object.freeze([
  // Core identity
  { group: "core_identity", airtable: MAP_FIRST_PASS.propertyName, alias: "Property Name", priority: 1 },
  {
    group: "core_identity",
    airtable: MAP_FIRST_PASS.canonicalPropertyName || CANONICAL_PROPERTY_NAME_FIELD,
    alias: "Canonical Property Name",
    priority: 1,
  },
  { group: "core_identity", airtable: MAP_FIRST_PASS.currentBrand, alias: "Brand", priority: 1 },
  { group: "core_identity", airtable: MAP_FIRST_PASS.city, alias: "City", priority: 1 },
  { group: "core_identity", airtable: MAP_FIRST_PASS.stateRegion, alias: "State / Region", priority: 1 },
  { group: "core_identity", airtable: MAP_FIRST_PASS.country, alias: "Country", priority: 1 },
  { group: "core_identity", airtable: "Brand Family", alias: "Parent Company", priority: 1 },
  // Source
  { group: "source", airtable: MAP_FIRST_PASS.sourceUrl, alias: "Source URL", priority: 2 },
  { group: "source", airtable: MAP_FIRST_PASS.family, alias: "Source Family", priority: 2 },
  { group: "governance", airtable: "Data Confidence Tier", alias: "Data Confidence Tier", priority: 2 },
  { group: "governance", airtable: "Identity Confidence", alias: "Identity Confidence", priority: 2 },
  {
    group: "source",
    airtable: "Source Confidence",
    alias: "Data Confidence Tier",
    priority: 2,
    note: "Schema uses Source Confidence (founder alias: Data Confidence Tier)",
  },
  { group: "source", airtable: "Production Use Status", alias: "Production Use Status", priority: 2 },
  // Address
  { group: "address", airtable: MAP_FIRST_PASS.address, alias: "Address", priority: 3 },
  { group: "address", airtable: MAP_FIRST_PASS.addressConfidence, alias: "Address Confidence", priority: 3 },
  { group: "address", airtable: MAP_FIRST_PASS.addressSourceUrl, alias: "Address Source URL", priority: 3 },
  // Contact (Level 2 — does not block Clean Core)
  { group: "contact", airtable: "Phone", alias: "Phone Number", priority: 4 },
  // Coordinates
  { group: "coordinates", airtable: MAP_FIRST_PASS.latitude, alias: "Latitude", priority: 5 },
  { group: "coordinates", airtable: MAP_FIRST_PASS.longitude, alias: "Longitude", priority: 5 },
  {
    group: "coordinates",
    airtable: MAP_FIRST_PASS.coordinateSourceType,
    alias: "Coordinate Source Type",
    priority: 5,
  },
  {
    group: "coordinates",
    airtable: MAP_FIRST_PASS.coordinateConfidence,
    alias: "Coordinate Confidence",
    priority: 5,
  },
  { group: "coordinates", airtable: MAP_FIRST_PASS.geocodeProvider, alias: "Geocode Provider", priority: 5 },
  { group: "coordinates", airtable: MAP_FIRST_PASS.geocodeMethod, alias: "Geocode Method", priority: 5 },
  {
    group: "coordinates",
    airtable: MAP_FIRST_PASS.geocodeReviewedDate,
    alias: "Geocode Reviewed Date",
    priority: 5,
  },
  // Public readiness
  {
    group: "public_readiness",
    airtable: MAP_FIRST_PASS.radarDisplayStatus,
    alias: "Radar Display Status",
    priority: 6,
  },
  {
    group: "public_readiness",
    airtable: MAP_FIRST_PASS.radarDisplayReason,
    alias: "Radar Display Reason",
    priority: 6,
  },
  {
    group: "public_readiness",
    airtable: MAP_FIRST_PASS.radarGeographyStatus,
    alias: "Radar Geography Status",
    priority: 6,
  },
  {
    group: "public_readiness",
    airtable: MAP_FIRST_PASS.publicCensusEligibility,
    alias: "Public Census Eligibility",
    priority: 6,
  },
  {
    group: "public_readiness",
    airtable: MAP_FIRST_PASS.publicDisplayConfidence,
    alias: "Public Display Confidence",
    priority: 6,
  },
  {
    group: "public_readiness",
    airtable: MAP_FIRST_PASS.publicDisplayReviewStatus,
    alias: "Public Display Review Status",
    priority: 6,
  },
  // Classification
  { group: "classification", airtable: MAP_FIRST_PASS.propertyType, alias: "Property Type", priority: 7 },
  { group: "classification", airtable: MAP_FIRST_PASS.assetContext, alias: "Asset Context", priority: 7 },
  {
    group: "classification",
    airtable: MAP_FIRST_PASS.marketSubmarket,
    alias: "Market / Submarket",
    priority: 7,
  },
  // Rooms
  { group: "rooms", airtable: "Rooms / Keys", alias: "Rooms / Keys", priority: 8 },
  { group: "rooms", airtable: "Rooms Confidence", alias: "Rooms Confidence", priority: 8 },
  { group: "rooms", airtable: "Rooms Source URL", alias: "Rooms Source URL", priority: 8 },
  { group: "rooms", airtable: "Rooms Source Type", alias: "Rooms Source Type", priority: 8 },
  { group: "rooms", airtable: "Rooms Reviewed Date", alias: "Rooms Reviewed Date", priority: 8 },
  // Content
  {
    group: "content",
    airtable: MAP_FIRST_PASS.descriptionSource,
    alias: "Hotel Description - Source Text",
    priority: 9,
  },
  {
    group: "content",
    airtable: MAP_FIRST_PASS.descriptionAi,
    alias: "Hotel Description - AI Summary",
    priority: 9,
  },
  {
    group: "content",
    airtable: MAP_FIRST_PASS.amenitiesSource,
    alias: "Amenities - Source Text",
    priority: 9,
  },
  {
    group: "content",
    airtable: MAP_FIRST_PASS.amenitiesTags,
    alias: "Amenities - Structured Tags",
    priority: 9,
  },
]);

const IDENTITY_FAMILY_PREFIX = Object.freeze({
  hilton: "Hilton",
  choice: "Choice",
  marriott: "Marriott",
  ihg: "IHG",
  accor: "Accor",
  wyndham: "Wyndham",
  preferred: "Preferred",
});

const COUNTRY_SHORT_TO_LABEL = Object.freeze({
  mx: "Mexico",
  do: "Dominican Republic",
  cr: "Costa Rica",
  co: "Colombia",
  pa: "Panama",
  jm: "Jamaica",
  pe: "Peru",
  br: "Brazil",
  cl: "Chile",
  ec: "Ecuador",
  gt: "Guatemala",
  hn: "Honduras",
  sv: "El Salvador",
  pr: "Puerto Rico",
  ar: "Argentina",
});

function isBlank(v) {
  if (v == null || v === "") return true;
  if (Array.isArray(v) && v.length === 0) return true;
  return false;
}

function fieldValue(fields, name) {
  return fields?.[name];
}

/**
 * Parse ind_{family}_{cc}_{code} identity keys.
 * @param {string} identityKey
 */
export function parseCensusIdentityKey(identityKey) {
  const id = String(identityKey || "").trim().toLowerCase();
  const m = id.match(/^ind_([a-z]+)_([a-z]{2})_(.+)$/);
  if (!m) return null;
  return {
    family_slug: m[1],
    family: IDENTITY_FAMILY_PREFIX[m[1]] || null,
    country_short: m[2],
    country: COUNTRY_SHORT_TO_LABEL[m[2]] || null,
    code: m[3],
  };
}

/**
 * Classify one field on one record.
 * @param {object} def KEY_FIELD_MATRIX entry
 * @param {Record<string, unknown>} fields
 * @param {{
 *   providerReady?: boolean,
 *   identityParsed?: object|null,
 * }} [ctx]
 */
export function classifyKeyFieldStatus(def, fields = {}, ctx = {}) {
  const name = def.airtable;
  if (AUTOPILOT_FORBIDDEN_FIELDS.includes(name) || isForbiddenAutopilotField(name)) {
    return {
      field: name,
      alias: def.alias,
      group: def.group,
      status: KEY_FIELD_STATUS.INTENTIONALLY_BLOCKED,
      reason: "forbidden_autopilot_field",
    };
  }

  const providerReady = Boolean(ctx.providerReady);
  const parsed = ctx.identityParsed || null;
  const officialUrl = fieldValue(fields, MAP_FIRST_PASS.officialUrl);
  const sourceUrl = fieldValue(fields, MAP_FIRST_PASS.sourceUrl);
  const address = fieldValue(fields, MAP_FIRST_PASS.address);
  const lat = fieldValue(fields, MAP_FIRST_PASS.latitude);
  const lng = fieldValue(fields, MAP_FIRST_PASS.longitude);
  const hasOfficialCoords = !isBlank(lat) && !isBlank(lng);

  // Canonical Property Name — dedicated High autofill / exact-suffix cleanup
  if (
    name === CANONICAL_PROPERTY_NAME_FIELD ||
    name === MAP_FIRST_PASS.canonicalPropertyName
  ) {
    const c = classifyCanonicalPropertyName(fields, {
      fieldExists: ctx.canonicalFieldExists !== false,
    });
    if (c.status === CANONICAL_NAME_STATUS.FIELD_MISSING) {
      return {
        field: name,
        alias: def.alias,
        group: def.group,
        status: KEY_FIELD_STATUS.MISSING_NOT_SUPPORTED,
        reason: CANONICAL_NAME_STATUS.FIELD_MISSING,
        canonical_status: c.status,
      };
    }
    if (c.status === CANONICAL_NAME_STATUS.COMPLETE_CLEAN) {
      return {
        field: name,
        alias: def.alias,
        group: def.group,
        status: KEY_FIELD_STATUS.COMPLETE,
        reason: c.reason,
        canonical_status: c.status,
      };
    }
    if (
      c.status === CANONICAL_NAME_STATUS.BLANK_CAN_AUTOFILL ||
      c.status === CANONICAL_NAME_STATUS.DIRTY_CAN_CLEAN
    ) {
      return {
        field: name,
        alias: def.alias,
        group: def.group,
        status: KEY_FIELD_STATUS.MISSING_CAN_AUTOFILL,
        reason: c.reason,
        autofill_value: c.candidate,
        canonical_status: c.status,
        allow_overwrite_dirty: c.status === CANONICAL_NAME_STATUS.DIRTY_CAN_CLEAN,
      };
    }
    if (c.status === CANONICAL_NAME_STATUS.INTENTIONALLY_BLANK) {
      return {
        field: name,
        alias: def.alias,
        group: def.group,
        status: KEY_FIELD_STATUS.INTENTIONALLY_BLOCKED,
        reason: c.reason,
        canonical_status: c.status,
      };
    }
    if (
      c.status === CANONICAL_NAME_STATUS.POPULATED_CONFLICT_NEEDS_REVIEW ||
      c.status === CANONICAL_NAME_STATUS.STEWARD_REVIEW_REQUIRED
    ) {
      return {
        field: name,
        alias: def.alias,
        group: def.group,
        status: KEY_FIELD_STATUS.MISSING_STEWARD_REVIEW,
        reason: c.reason,
        canonical_status: c.status,
        existing_canonical: c.existing,
        candidate_canonical: c.candidate,
      };
    }
    return {
      field: name,
      alias: def.alias,
      group: def.group,
      status: KEY_FIELD_STATUS.MISSING_NEEDS_SOURCE_ADAPTER,
      reason: c.reason || "canonical_missing_source_support",
      canonical_status: c.status,
    };
  }

  const value = fieldValue(fields, name);
  if (!isBlank(value)) {
    return {
      field: name,
      alias: def.alias,
      group: def.group,
      status: KEY_FIELD_STATUS.COMPLETE,
      reason: null,
    };
  }

  // Coordinates: provider-blocked vs official vs steward
  if (AUTOPILOT_GEOCODE_FIELDS.includes(name) || def.group === "coordinates") {
    if (name === MAP_FIRST_PASS.latitude || name === MAP_FIRST_PASS.longitude) {
      if (hasOfficialCoords) {
        return {
          field: name,
          alias: def.alias,
          group: def.group,
          status: KEY_FIELD_STATUS.COMPLETE,
          reason: null,
        };
      }
      if (!isBlank(address) && !providerReady) {
        return {
          field: name,
          alias: def.alias,
          group: def.group,
          status: KEY_FIELD_STATUS.MISSING_PROVIDER_BLOCKED,
          reason: "geocode_provider_or_storage_terms_missing",
        };
      }
      if (!isBlank(address) && providerReady) {
        return {
          field: name,
          alias: def.alias,
          group: def.group,
          status: KEY_FIELD_STATUS.MISSING_CAN_AUTOFILL,
          reason: "approved_provider_geocode_from_confirmed_address",
        };
      }
      return {
        field: name,
        alias: def.alias,
        group: def.group,
        status: KEY_FIELD_STATUS.MISSING_NEEDS_SOURCE_ADAPTER,
        reason: "need_official_coords_or_confirmed_address",
      };
    }
    // Provenance fields when coords exist
    if (hasOfficialCoords) {
      return {
        field: name,
        alias: def.alias,
        group: def.group,
        status: KEY_FIELD_STATUS.MISSING_CAN_AUTOFILL,
        reason: "coords_present_provenance_gap",
      };
    }
    if (!providerReady) {
      return {
        field: name,
        alias: def.alias,
        group: def.group,
        status: KEY_FIELD_STATUS.MISSING_PROVIDER_BLOCKED,
        reason: "geocode_provider_or_storage_terms_missing",
      };
    }
    return {
      field: name,
      alias: def.alias,
      group: def.group,
      status: KEY_FIELD_STATUS.MISSING_NEEDS_SOURCE_ADAPTER,
      reason: "coords_missing",
    };
  }

  // Source family from identity key
  if (name === MAP_FIRST_PASS.family && parsed?.family) {
    return {
      field: name,
      alias: def.alias,
      group: def.group,
      status: KEY_FIELD_STATUS.MISSING_CAN_AUTOFILL,
      reason: "identity_key_family",
      autofill_value: parsed.family,
    };
  }

  // Country from identity key when blank
  if (name === MAP_FIRST_PASS.country && parsed?.country) {
    return {
      field: name,
      alias: def.alias,
      group: def.group,
      status: KEY_FIELD_STATUS.MISSING_CAN_AUTOFILL,
      reason: "identity_key_country",
      autofill_value: parsed.country,
    };
  }

  // Source URL from official property URL
  if (name === MAP_FIRST_PASS.sourceUrl && isPropertyLevelUrl(officialUrl)) {
    return {
      field: name,
      alias: def.alias,
      group: def.group,
      status: KEY_FIELD_STATUS.MISSING_CAN_AUTOFILL,
      reason: "copy_official_property_url",
      autofill_value: String(officialUrl).trim(),
    };
  }

  // Address provenance when address present
  if (
    (name === MAP_FIRST_PASS.addressConfidence || name === MAP_FIRST_PASS.addressSourceUrl) &&
    !isBlank(address)
  ) {
    const autofill =
      name === MAP_FIRST_PASS.addressConfidence
        ? "High"
        : isPropertyLevelUrl(officialUrl || sourceUrl)
          ? String(officialUrl || sourceUrl).trim()
          : null;
    if (autofill) {
      return {
        field: name,
        alias: def.alias,
        group: def.group,
        status: KEY_FIELD_STATUS.MISSING_CAN_AUTOFILL,
        reason: "address_present_provenance_gap",
        autofill_value: autofill,
      };
    }
  }

  // Core identity / address gaps without weak inference → steward or source adapter
  if (def.group === "core_identity" || def.group === "address") {
    if (name === MAP_FIRST_PASS.city || name === MAP_FIRST_PASS.stateRegion) {
      return {
        field: name,
        alias: def.alias,
        group: def.group,
        status: KEY_FIELD_STATUS.MISSING_STEWARD_REVIEW,
        reason: "city_or_state_requires_official_source_or_steward",
      };
    }
    if (name === MAP_FIRST_PASS.address) {
      return {
        field: name,
        alias: def.alias,
        group: def.group,
        status: KEY_FIELD_STATUS.MISSING_NEEDS_SOURCE_ADAPTER,
        reason: "official_address_adapter_or_queue",
      };
    }
    if (name === MAP_FIRST_PASS.propertyName || name === MAP_FIRST_PASS.currentBrand) {
      return {
        field: name,
        alias: def.alias,
        group: def.group,
        status: KEY_FIELD_STATUS.MISSING_STEWARD_REVIEW,
        reason: "identity_gap_steward",
      };
    }
  }

  if (def.group === "source") {
    return {
      field: name,
      alias: def.alias,
      group: def.group,
      status: KEY_FIELD_STATUS.MISSING_NEEDS_SOURCE_ADAPTER,
      reason: "source_field_gap",
    };
  }

  if (def.group === "rooms" || def.group === "content" || def.group === "classification" || def.group === "public_readiness") {
    return {
      field: name,
      alias: def.alias,
      group: def.group,
      status: KEY_FIELD_STATUS.MISSING_NEEDS_SOURCE_ADAPTER,
      reason: `${def.group}_enrichment_queue`,
    };
  }

  return {
    field: name,
    alias: def.alias,
    group: def.group,
    status: KEY_FIELD_STATUS.MISSING_NOT_SUPPORTED,
    reason: "unclassified_gap",
  };
}

/**
 * @param {object} record Airtable-style { id, fields }
 * @param {{ providerReady?: boolean }} [opts]
 */
export function classifyRecordKeyFields(record, opts = {}) {
  const fields = record.fields || {};
  const identityKey = String(fields[MAP_FIRST_PASS.identityKey] || "").trim();
  const identityParsed = parseCensusIdentityKey(identityKey);
  const statuses = KEY_FIELD_MATRIX.map((def) =>
    classifyKeyFieldStatus(def, fields, {
      providerReady: opts.providerReady,
      identityParsed,
      canonicalFieldExists: opts.canonicalFieldExists !== false,
    })
  );
  const byStatus = {};
  for (const s of statuses) {
    byStatus[s.status] = (byStatus[s.status] || 0) + 1;
  }
  return {
    record_id: record.id || null,
    identity_key: identityKey || null,
    property_name: fields[MAP_FIRST_PASS.propertyName] || null,
    fields: statuses,
    by_status: byStatus,
    autofill_count: statuses.filter((s) => s.status === KEY_FIELD_STATUS.MISSING_CAN_AUTOFILL)
      .length,
    provider_blocked_count: statuses.filter(
      (s) => s.status === KEY_FIELD_STATUS.MISSING_PROVIDER_BLOCKED
    ).length,
  };
}

/**
 * Build aggregated completion matrix.
 * @param {object[]} censusRecords
 * @param {{ providerReady?: boolean, env?: object }} [opts]
 */
export function buildKeyFieldCompletionMatrix(censusRecords = [], opts = {}) {
  const sot = assertProductionCensusWriteTarget({
    baseName: productionHotelPropertyCensus.baseName,
    tableName: productionHotelPropertyCensus.tableName,
    tableId: productionHotelPropertyCensus.tableId,
  });
  if (!sot.ok) {
    return {
      ok: false,
      status: KEY_FIELD_COMPLETION_STATUS.BLOCKED,
      error: BLOCKED_WRONG_CENSUS_TARGET,
      source_of_truth: sot,
    };
  }

  const provider = evaluateProviderReadiness(opts.env || process.env);
  const providerReady = Boolean(
    opts.providerReady != null ? opts.providerReady : provider.approved_for_geocode_apply
  );

  const total = censusRecords.length;
  /** @type {Record<string, object>} */
  const byField = {};
  for (const def of KEY_FIELD_MATRIX) {
    byField[def.airtable] = {
      field: def.airtable,
      alias: def.alias,
      group: def.group,
      priority: def.priority,
      note: def.note || null,
      complete: 0,
      missing: 0,
      by_status: Object.fromEntries(Object.values(KEY_FIELD_STATUS).map((s) => [s, 0])),
      autofill_opportunities: 0,
      provider_blocked: 0,
      steward_review: 0,
      source_adapter_gaps: 0,
    };
  }

  const recordRows = [];
  const autofillOpportunities = [];
  const providerBlockedCoords = [];
  const sourceAdapterGaps = [];
  const stewardNeeds = [];

  for (const rec of censusRecords) {
    const row = classifyRecordKeyFields(rec, {
      providerReady,
      canonicalFieldExists: opts.canonicalFieldExists !== false,
    });
    recordRows.push({
      record_id: row.record_id,
      identity_key: row.identity_key,
      property_name: row.property_name,
      by_status: row.by_status,
      autofill_count: row.autofill_count,
      provider_blocked_count: row.provider_blocked_count,
    });

    for (const s of row.fields) {
      const agg = byField[s.field];
      if (!agg) continue;
      agg.by_status[s.status] = (agg.by_status[s.status] || 0) + 1;
      if (s.status === KEY_FIELD_STATUS.COMPLETE) agg.complete += 1;
      else agg.missing += 1;

      if (s.status === KEY_FIELD_STATUS.MISSING_CAN_AUTOFILL) {
        agg.autofill_opportunities += 1;
        autofillOpportunities.push({
          record_id: row.record_id,
          identity_key: row.identity_key,
          field: s.field,
          reason: s.reason,
          autofill_value: s.autofill_value ?? null,
          canonical_status: s.canonical_status || null,
        });
      } else if (s.status === KEY_FIELD_STATUS.MISSING_PROVIDER_BLOCKED) {
        agg.provider_blocked += 1;
        if (s.field === MAP_FIRST_PASS.latitude || s.field === MAP_FIRST_PASS.longitude) {
          providerBlockedCoords.push({
            record_id: row.record_id,
            identity_key: row.identity_key,
            field: s.field,
            reason: s.reason,
          });
        }
      } else if (s.status === KEY_FIELD_STATUS.MISSING_STEWARD_REVIEW) {
        agg.steward_review += 1;
        stewardNeeds.push({
          record_id: row.record_id,
          identity_key: row.identity_key,
          field: s.field,
          reason: s.reason,
        });
      } else if (s.status === KEY_FIELD_STATUS.MISSING_NEEDS_SOURCE_ADAPTER) {
        agg.source_adapter_gaps += 1;
        sourceAdapterGaps.push({
          record_id: row.record_id,
          identity_key: row.identity_key,
          field: s.field,
          reason: s.reason,
        });
      }
    }
  }

  // Canonical Property Name rollup
  const canonicalCounts = {
    complete_clean: 0,
    blank: 0,
    dirty: 0,
    blank_can_autofill: 0,
    dirty_can_clean: 0,
    steward: 0,
    blocked_or_missing_source: 0,
    intentionally_blank: 0,
  };
  for (const rec of censusRecords) {
    const c = classifyCanonicalPropertyName(rec.fields || {}, {
      fieldExists: opts.canonicalFieldExists !== false,
    });
    if (c.status === CANONICAL_NAME_STATUS.COMPLETE_CLEAN) canonicalCounts.complete_clean += 1;
    else if (c.status === CANONICAL_NAME_STATUS.BLANK_CAN_AUTOFILL) {
      canonicalCounts.blank += 1;
      canonicalCounts.blank_can_autofill += 1;
    } else if (c.status === CANONICAL_NAME_STATUS.DIRTY_CAN_CLEAN) {
      canonicalCounts.dirty += 1;
      canonicalCounts.dirty_can_clean += 1;
    } else if (
      c.status === CANONICAL_NAME_STATUS.POPULATED_CONFLICT_NEEDS_REVIEW ||
      c.status === CANONICAL_NAME_STATUS.STEWARD_REVIEW_REQUIRED
    ) {
      canonicalCounts.steward += 1;
      if (isBlank(rec.fields?.[CANONICAL_PROPERTY_NAME_FIELD])) canonicalCounts.blank += 1;
      else if (hasSafeDirtyMembershipSuffix(rec.fields?.[CANONICAL_PROPERTY_NAME_FIELD])) {
        canonicalCounts.dirty += 1;
      }
    } else if (c.status === CANONICAL_NAME_STATUS.INTENTIONALLY_BLANK) {
      canonicalCounts.intentionally_blank += 1;
      canonicalCounts.blank += 1;
    } else {
      canonicalCounts.blocked_or_missing_source += 1;
      if (isBlank(rec.fields?.[CANONICAL_PROPERTY_NAME_FIELD])) canonicalCounts.blank += 1;
    }
  }

  for (const agg of Object.values(byField)) {
    agg.completion_pct =
      total > 0 ? Math.round((agg.complete / total) * 1000) / 10 : null;
  }

  const autofillCount = autofillOpportunities.length;
  const providerBlockedUnique = new Set(providerBlockedCoords.map((r) => r.record_id)).size;
  const sourceGapUnique = new Set(sourceAdapterGaps.map((r) => r.record_id)).size;

  let status = KEY_FIELD_COMPLETION_STATUS.READY;
  if (!providerReady && providerBlockedUnique > 0) {
    status = KEY_FIELD_COMPLETION_STATUS.READY_PROVIDER_BLOCKED;
  }
  // Prefer needs_source_adapters when gaps dominate and little autofill remains
  const highSourceGapFields = Object.values(byField).filter(
    (f) => f.source_adapter_gaps > total * 0.5 && f.group !== "coordinates"
  );
  if (highSourceGapFields.length >= 8 && autofillCount < total * 0.05) {
    status = KEY_FIELD_COMPLETION_STATUS.NEEDS_SOURCE_ADAPTERS;
  }

  return {
    ok: true,
    version: KEY_FIELD_COMPLETION_VERSION,
    status,
    generated_at: new Date().toISOString(),
    production_target: {
      base: productionHotelPropertyCensus.baseName,
      table: productionHotelPropertyCensus.tableName,
      table_id: productionHotelPropertyCensus.tableId,
    },
    brand_setup_writes: false,
    brand_explorer_writes: false,
    vic_writes: false,
    total_hotel_property_census_records: total,
    provider_readiness: {
      approved_for_geocode_apply: providerReady,
      block_reason: provider.block_reason,
      recommended: provider.recommended,
    },
    priority_groups: [...KEY_FIELD_PRIORITY_GROUPS],
    fields: Object.values(byField),
    summary: {
      autofill_opportunities: autofillCount,
      provider_blocked_coordinate_records: providerBlockedUnique,
      source_adapter_gap_records: sourceGapUnique,
      steward_review_gaps: stewardNeeds.length,
      canonical_property_name: {
        total_records: total,
        complete_clean: canonicalCounts.complete_clean,
        blank: canonicalCounts.blank,
        dirty: canonicalCounts.dirty,
        safe_autofill: canonicalCounts.blank_can_autofill,
        safe_cleanup: canonicalCounts.dirty_can_clean,
        steward: canonicalCounts.steward,
        blocked_or_missing_source: canonicalCounts.blocked_or_missing_source,
        intentionally_blank: canonicalCounts.intentionally_blank,
      },
    },
    samples: {
      autofill_opportunities: autofillOpportunities.slice(0, 100),
      provider_blocked_coordinates: providerBlockedCoords.slice(0, 100),
      source_adapter_gaps: sourceAdapterGaps.slice(0, 100),
      steward_review_needs: stewardNeeds.slice(0, 100),
    },
    records_scanned: recordRows.length,
    recommended_next_production_cycle_action: recommendNextAction({
      status,
      autofillCount,
      providerReady,
      providerBlockedUnique,
      sourceGapUnique,
    }),
  };
}

function recommendNextAction({
  status,
  autofillCount,
  providerReady,
  providerBlockedUnique,
  sourceGapUnique,
}) {
  if (status === KEY_FIELD_COMPLETION_STATUS.BLOCKED) {
    return "Fix Hotel Property Census source-of-truth guard before continuing.";
  }
  if (autofillCount > 0) {
    return "Run production-cycle with key_field_completion after source_discovery to apply High autofills, then enrichment queues.";
  }
  if (!providerReady && providerBlockedUnique > 0) {
    return "Configure MAPBOX_ACCESS_TOKEN + MAPBOX_PERMANENT_GEOCODING=1 (or Google storage terms) for coordinate completion; continue other fields without waiting.";
  }
  if (sourceGapUnique > 0) {
    return "Continue enrichment queues (address, property type, rooms, description/amenities) and expand discovery adapters for remaining source gaps.";
  }
  return "Re-run production-cycle; key fields largely complete for active scope.";
}

/**
 * Build High-confidence autofill proposals (never overwrite populated values).
 * @param {object[]} censusRecords
 * @param {{ providerReady?: boolean, env?: object }} [opts]
 */
export function proposeKeyFieldAutofills(censusRecords = [], opts = {}) {
  const provider = evaluateProviderReadiness(opts.env || process.env);
  const providerReady = Boolean(
    opts.providerReady != null ? opts.providerReady : provider.approved_for_geocode_apply
  );
  const dupIndex = buildCanonicalDuplicateIndex(censusRecords, {
    isPropertyLevelUrl,
  });

  const proposals = [];
  const providerDecisionNeeded = [];
  const stewardReview = [];
  const sourceAdapterNeeded = [];
  const canonicalExamples = [];
  let canonicalAutofill = 0;
  let canonicalCleanup = 0;
  let canonicalSteward = 0;
  let canonicalDupBlocked = 0;
  let canonicalSkippedIdentical = 0;

  for (const rec of censusRecords) {
    const fields = rec.fields || {};
    const identityKey = String(fields[MAP_FIRST_PASS.identityKey] || "").trim();
    const row = classifyRecordKeyFields(rec, {
      providerReady,
      canonicalFieldExists: opts.canonicalFieldExists !== false,
    });
    /** @type {Record<string, unknown>} */
    const patch = {};

    // Dedicated Canonical Property Name path (blank autofill + dirty cleanup + dup gate)
    const canonProp = proposeCanonicalPropertyNameWrite(rec, dupIndex, {
      fieldExists: opts.canonicalFieldExists !== false,
      isPropertyLevelUrl,
    });
    if (canonProp.action === "autofill" || canonProp.action === "cleanup") {
      if (AUTOPILOT_ALLOWED_WRITE_FIELDS.includes(CANONICAL_PROPERTY_NAME_FIELD)) {
        Object.assign(patch, canonProp.patch);
        if (canonProp.action === "autofill") canonicalAutofill += 1;
        else canonicalCleanup += 1;
        if (canonicalExamples.length < 25) {
          canonicalExamples.push({
            record_id: rec.id,
            action: canonProp.action,
            before: canonProp.before,
            after: canonProp.after,
            property_name: fields[MAP_FIRST_PASS.propertyName] || null,
            brand: fields[MAP_FIRST_PASS.currentBrand] || null,
            city: fields[MAP_FIRST_PASS.city] || null,
            country: fields[MAP_FIRST_PASS.country] || null,
          });
        }
      }
    } else if (canonProp.action === "steward") {
      canonicalSteward += 1;
      if (canonProp.classified?.reason === "duplicate_risk") canonicalDupBlocked += 1;
      stewardReview.push({
        record_id: rec.id,
        identity_key: identityKey,
        field: CANONICAL_PROPERTY_NAME_FIELD,
        reason: canonProp.classified?.reason || "canonical_steward",
        queue: KEY_FIELD_COMPLETION_QUEUE_ID,
        existing: canonProp.classified?.existing ?? null,
        candidate: canonProp.classified?.candidate ?? null,
        duplicate: canonProp.classified?.duplicate || null,
      });
    } else if (canonProp.action === "skip_identical") {
      canonicalSkippedIdentical += 1;
    } else if (
      canonProp.classified?.status === CANONICAL_NAME_STATUS.POPULATED_CONFLICT_NEEDS_REVIEW ||
      canonProp.classified?.status === CANONICAL_NAME_STATUS.STEWARD_REVIEW_REQUIRED
    ) {
      canonicalSteward += 1;
      stewardReview.push({
        record_id: rec.id,
        identity_key: identityKey,
        field: CANONICAL_PROPERTY_NAME_FIELD,
        reason: canonProp.classified?.reason,
        queue: KEY_FIELD_COMPLETION_QUEUE_ID,
        existing: canonProp.classified?.existing ?? null,
        candidate: canonProp.classified?.candidate ?? null,
      });
    }

    for (const s of row.fields) {
      // Canonical handled above (supports dirty overwrite)
      if (
        s.field === CANONICAL_PROPERTY_NAME_FIELD ||
        s.field === MAP_FIRST_PASS.canonicalPropertyName
      ) {
        continue;
      }
      if (s.status === KEY_FIELD_STATUS.MISSING_CAN_AUTOFILL && s.autofill_value != null) {
        // Never overwrite
        if (!isBlank(fields[s.field])) continue;
        // Only allowlisted Autopilot write fields
        if (!AUTOPILOT_ALLOWED_WRITE_FIELDS.includes(s.field)) continue;
        if (AUTOPILOT_GEOCODE_FIELDS.includes(s.field) && !providerReady) {
          // Official provenance fill when coords already present is OK; new lat/lng from provider is not
          if (s.field === MAP_FIRST_PASS.latitude || s.field === MAP_FIRST_PASS.longitude) {
            providerDecisionNeeded.push({
              record_id: rec.id,
              identity_key: identityKey,
              field: s.field,
              reason: s.reason,
              queue: KEY_FIELD_COMPLETION_QUEUE_ID,
            });
            continue;
          }
        }
        patch[s.field] = s.autofill_value;
      } else if (s.status === KEY_FIELD_STATUS.MISSING_PROVIDER_BLOCKED) {
        if (s.field === MAP_FIRST_PASS.latitude || s.field === MAP_FIRST_PASS.longitude) {
          providerDecisionNeeded.push({
            record_id: rec.id,
            identity_key: identityKey,
            field: s.field,
            reason: s.reason,
            queue: KEY_FIELD_COMPLETION_QUEUE_ID,
          });
        }
      } else if (s.status === KEY_FIELD_STATUS.MISSING_STEWARD_REVIEW) {
        stewardReview.push({
          record_id: rec.id,
          identity_key: identityKey,
          field: s.field,
          reason: s.reason,
          queue: KEY_FIELD_COMPLETION_QUEUE_ID,
        });
      } else if (s.status === KEY_FIELD_STATUS.MISSING_NEEDS_SOURCE_ADAPTER) {
        sourceAdapterNeeded.push({
          record_id: rec.id,
          identity_key: identityKey,
          field: s.field,
          reason: s.reason,
          queue: KEY_FIELD_COMPLETION_QUEUE_ID,
        });
      }
    }

    // Official coordinate provenance when lat/lng present but provenance blank
    if (!isBlank(fields[MAP_FIRST_PASS.latitude]) && !isBlank(fields[MAP_FIRST_PASS.longitude])) {
      if (
        isBlank(fields[MAP_FIRST_PASS.coordinateConfidence]) &&
        AUTOPILOT_ALLOWED_WRITE_FIELDS.includes(MAP_FIRST_PASS.coordinateConfidence)
      ) {
        patch[MAP_FIRST_PASS.coordinateConfidence] = "High";
      }
      if (
        isBlank(fields[MAP_FIRST_PASS.coordinateSourceType]) &&
        AUTOPILOT_ALLOWED_WRITE_FIELDS.includes(MAP_FIRST_PASS.coordinateSourceType)
      ) {
        patch[MAP_FIRST_PASS.coordinateSourceType] = "official_brand_directory";
      }
      if (
        isBlank(fields[MAP_FIRST_PASS.geocodeProvider]) &&
        AUTOPILOT_ALLOWED_WRITE_FIELDS.includes(MAP_FIRST_PASS.geocodeProvider)
      ) {
        patch[MAP_FIRST_PASS.geocodeProvider] = "official";
      }
      if (
        isBlank(fields[MAP_FIRST_PASS.geocodeMethod]) &&
        AUTOPILOT_ALLOWED_WRITE_FIELDS.includes(MAP_FIRST_PASS.geocodeMethod)
      ) {
        patch[MAP_FIRST_PASS.geocodeMethod] = "official_directory_coordinates";
      }
      if (
        isBlank(fields[MAP_FIRST_PASS.geocodeReviewedDate]) &&
        AUTOPILOT_ALLOWED_WRITE_FIELDS.includes(MAP_FIRST_PASS.geocodeReviewedDate)
      ) {
        patch[MAP_FIRST_PASS.geocodeReviewedDate] = new Date().toISOString().slice(0, 10);
      }
    }

    if (Object.keys(patch).length) {
      const hasCanonicalDirtyCleanup =
        canonProp.action === "cleanup" &&
        Object.prototype.hasOwnProperty.call(patch, CANONICAL_PROPERTY_NAME_FIELD);
      proposals.push({
        record_id: rec.id,
        identity_key: identityKey || null,
        property_name: fields[MAP_FIRST_PASS.propertyName] || null,
        brand: fields[MAP_FIRST_PASS.currentBrand] || null,
        family: fields[MAP_FIRST_PASS.family] || null,
        queue: KEY_FIELD_COMPLETION_QUEUE_ID,
        action: "propose_high_write",
        confidence: "High",
        write_allowed_now: true,
        allow_overwrite_dirty: hasCanonicalDirtyCleanup,
        allow_normalization_overwrite: hasCanonicalDirtyCleanup,
        patch,
        current_fields: Object.fromEntries(
          Object.keys(patch).map((k) => [k, fields[k] ?? null])
        ),
        method: "key_field_completion_autofill",
        notes: "High-only; Canonical allows exact dirty-suffix cleanup; no weak inference",
      });
    }
  }

  return {
    proposals,
    provider_decision_needed: providerDecisionNeeded,
    steward_review: stewardReview,
    source_adapter_needed: sourceAdapterNeeded,
    provider_ready: providerReady,
    canonical_summary: {
      safe_autofill_proposals: canonicalAutofill,
      safe_cleanup_proposals: canonicalCleanup,
      steward_cases: canonicalSteward,
      duplicate_risks_blocked: canonicalDupBlocked,
      skipped_identical: canonicalSkippedIdentical,
      before_after_examples: canonicalExamples,
    },
  };
}

/**
 * Run key_field_completion queue (controlled: proposals + matrix; no writes).
 * @param {{
 *   censusRecords?: object[],
 *   runDir?: string|null,
 *   writeReports?: boolean,
 *   env?: object,
 *   providerReady?: boolean,
 * }} [opts]
 */
export function runKeyFieldCompletionQueue(opts = {}) {
  const censusRecords = opts.censusRecords || [];
  const matrix = buildKeyFieldCompletionMatrix(censusRecords, opts);
  if (!matrix.ok) {
    return {
      ok: false,
      status: matrix.status,
      error: matrix.error,
      proposals: [],
      matrix,
      airtable_writes: false,
    };
  }

  const proposed = proposeKeyFieldAutofills(censusRecords, opts);
  const canon = proposed.canonical_summary || {};
  let canonicalStatus = CANONICAL_COMPLETION_STATUS.READY_NEEDS_PRODUCTION_CYCLE;
  if (opts.canonicalFieldExists === false) {
    canonicalStatus = CANONICAL_COMPLETION_STATUS.FIELD_MISSING;
  } else if (
    (canon.safe_autofill_proposals || 0) + (canon.safe_cleanup_proposals || 0) === 0 &&
    (canon.steward_cases || 0) > 0
  ) {
    canonicalStatus = CANONICAL_COMPLETION_STATUS.PARTIAL_STEWARD;
  } else if (
    (canon.safe_autofill_proposals || 0) + (canon.safe_cleanup_proposals || 0) > 0 &&
    (canon.steward_cases || 0) > 0
  ) {
    canonicalStatus = CANONICAL_COMPLETION_STATUS.PARTIAL_STEWARD;
  } else if (
    (canon.safe_autofill_proposals || 0) + (canon.safe_cleanup_proposals || 0) > 0
  ) {
    canonicalStatus = CANONICAL_COMPLETION_STATUS.READY_NEEDS_PRODUCTION_CYCLE;
  } else if ((canon.steward_cases || 0) === 0) {
    canonicalStatus = CANONICAL_COMPLETION_STATUS.APPLIED_CLEAN;
  }

  const report = {
    ok: canonicalStatus !== CANONICAL_COMPLETION_STATUS.FIELD_MISSING,
    version: KEY_FIELD_COMPLETION_VERSION,
    queue: KEY_FIELD_COMPLETION_QUEUE_ID,
    status: matrix.status,
    canonical_completion_status: canonicalStatus,
    airtable_writes: false,
    brand_setup_writes: false,
    brand_explorer_writes: false,
    high_proposals: proposed.proposals.length,
    proposals: proposed.proposals,
    provider_decision_needed: proposed.provider_decision_needed,
    steward_review: proposed.steward_review,
    source_adapter_needed: proposed.source_adapter_needed,
    canonical_summary: {
      ...(matrix.summary?.canonical_property_name || {}),
      ...canon,
    },
    matrix_summary: matrix.summary,
    matrix_status: matrix.status,
    recommended_next_production_cycle_action: matrix.recommended_next_production_cycle_action,
  };

  if (opts.writeReports !== false) {
    writeKeyFieldCompletionReports({
      matrix,
      queueReport: report,
      runDir: opts.runDir || null,
    });
    writeCanonicalPropertyNameCompletionReports(report, {
      matrix,
      runDir: opts.runDir || null,
      applied: Boolean(opts.applied),
    });
  }

  return report;
}

/**
 * Persist matrix + docs reports.
 */
export function writeKeyFieldCompletionReports({ matrix, queueReport = null, runDir = null }) {
  const reportsDir = path.join(ROOT, "reports/research-engine-v2");
  const docsDir = path.join(ROOT, "docs/data-intelligence");
  fs.mkdirSync(reportsDir, { recursive: true });
  fs.mkdirSync(docsDir, { recursive: true });

  const jsonPath = path.join(reportsDir, "production-census-key-field-completion-matrix.json");
  const mdPath = path.join(reportsDir, "production-census-key-field-completion-matrix.md");
  const docsPath = path.join(docsDir, "production-census-key-field-completion.md");

  const payload = {
    ...matrix,
    queue_report: queueReport
      ? {
          high_proposals: queueReport.high_proposals,
          provider_decision_needed_count: (queueReport.provider_decision_needed || []).length,
          steward_review_count: (queueReport.steward_review || []).length,
          source_adapter_needed_count: (queueReport.source_adapter_needed || []).length,
        }
      : null,
  };
  fs.writeFileSync(jsonPath, JSON.stringify(payload, null, 2), "utf8");
  fs.writeFileSync(mdPath, renderMatrixMarkdown(payload), "utf8");
  fs.writeFileSync(docsPath, renderDocsMarkdown(payload), "utf8");

  if (runDir) {
    fs.mkdirSync(runDir, { recursive: true });
    fs.writeFileSync(
      path.join(runDir, "key-field-completion-matrix.json"),
      JSON.stringify(payload, null, 2),
      "utf8"
    );
    fs.writeFileSync(
      path.join(runDir, "key-field-completion-matrix.md"),
      renderMatrixMarkdown(payload),
      "utf8"
    );
    if (queueReport) {
      fs.writeFileSync(
        path.join(runDir, "key-field-completion-queue.json"),
        JSON.stringify(queueReport, null, 2),
        "utf8"
      );
    }
  }

  return { jsonPath, mdPath, docsPath };
}

/**
 * Persist Canonical Property Name completion reports.
 */
export function writeCanonicalPropertyNameCompletionReports(queueReport, opts = {}) {
  const reportsDir = path.join(ROOT, "reports/research-engine-v2");
  const docsDir = path.join(ROOT, "docs/data-intelligence");
  fs.mkdirSync(reportsDir, { recursive: true });
  fs.mkdirSync(docsDir, { recursive: true });

  const summary = queueReport.canonical_summary || {};
  const status =
    opts.applied &&
    (summary.safe_autofill_proposals || 0) + (summary.safe_cleanup_proposals || 0) > 0 &&
    (summary.steward_cases || 0) === 0
      ? CANONICAL_COMPLETION_STATUS.APPLIED_CLEAN
      : opts.applied && (summary.steward_cases || 0) > 0
        ? CANONICAL_COMPLETION_STATUS.PARTIAL_STEWARD
        : queueReport.canonical_completion_status ||
          CANONICAL_COMPLETION_STATUS.READY_NEEDS_PRODUCTION_CYCLE;

  const payload = {
    version: CANONICAL_NAME_VERSION,
    generated_at: new Date().toISOString(),
    status,
    write_target: {
      base: productionHotelPropertyCensus.baseName,
      table: productionHotelPropertyCensus.tableName,
      table_id: productionHotelPropertyCensus.tableId,
    },
    airtable_writes: Boolean(opts.applied),
    fields_written: [CANONICAL_PROPERTY_NAME_FIELD],
    records_scanned: opts.matrix?.total_hotel_property_census_records ?? null,
    blank_canonical_names: summary.blank ?? null,
    dirty_canonical_names: summary.dirty ?? null,
    complete_clean: summary.complete_clean ?? null,
    safe_autofills: summary.safe_autofill_proposals ?? summary.safe_autofill ?? 0,
    safe_cleanups: summary.safe_cleanup_proposals ?? summary.safe_cleanup ?? 0,
    skipped_identical: summary.skipped_identical ?? 0,
    steward_cases: summary.steward_cases ?? summary.steward ?? 0,
    duplicate_risks: summary.duplicate_risks_blocked ?? 0,
    before_after_examples: summary.before_after_examples || [],
    constraints: {
      brand_setup_writes: false,
      brand_explorer_writes: false,
      owner_operator_date_writes: false,
      fuzzy_auto_inserts: false,
    },
  };

  const jsonPath = path.join(
    reportsDir,
    "production-census-canonical-property-name-completion.json"
  );
  const mdPath = path.join(
    reportsDir,
    "production-census-canonical-property-name-completion.md"
  );
  const docsPath = path.join(
    docsDir,
    "production-census-canonical-property-name-completion.md"
  );
  const md = renderCanonicalCompletionMarkdown(payload);
  fs.writeFileSync(jsonPath, JSON.stringify(payload, null, 2), "utf8");
  fs.writeFileSync(mdPath, md, "utf8");
  fs.writeFileSync(
    docsPath,
    `# Canonical Property Name Completion

${md}

## Rules

- High confidence only
- Blank autofill from clean Property Name when Brand + City + Country + Source URL present
- Exact membership suffix cleanup only (Radisson Individuals / Preferred Hotels & Resorts)
- Never overwrite materially different populated values
- Duplicate risk → steward
- Hotel Property Census only

## Commands

\`\`\`bash
npm run census:autopilot -- --region CALA --scope active-brand-setup --mode controlled \\
  --strategy fastest-safe --queue key_field_completion --run-until-complete --batch-size 250
\`\`\`
`,
    "utf8"
  );

  if (opts.runDir) {
    fs.mkdirSync(opts.runDir, { recursive: true });
    fs.writeFileSync(
      path.join(opts.runDir, "canonical-property-name-completion.json"),
      JSON.stringify(payload, null, 2),
      "utf8"
    );
  }

  return { jsonPath, mdPath, docsPath, status };
}

function renderCanonicalCompletionMarkdown(payload) {
  const examples = (payload.before_after_examples || [])
    .slice(0, 15)
    .map(
      (e) =>
        `- \`${e.record_id}\` (${e.action}): \`${e.before || "(blank)"}\` → \`${e.after}\``
    )
    .join("\n");
  return `# Production Census — Canonical Property Name Completion

**Status:** \`${payload.status}\`  
**Generated:** ${payload.generated_at}  
**Write target:** ${payload.write_target?.base} → ${payload.write_target?.table} (\`${payload.write_target?.table_id}\`)  
**Airtable writes:** ${payload.airtable_writes ? "yes" : "no (controlled / proposal)"}

## Counts

| Metric | Count |
|--------|------:|
| Records scanned | ${payload.records_scanned ?? "—"} |
| Complete clean | ${payload.complete_clean ?? "—"} |
| Blank canonical | ${payload.blank_canonical_names ?? "—"} |
| Dirty canonical | ${payload.dirty_canonical_names ?? "—"} |
| Safe autofill proposals | ${payload.safe_autofills ?? 0} |
| Safe cleanup proposals | ${payload.safe_cleanups ?? 0} |
| Skipped identical | ${payload.skipped_identical ?? 0} |
| Steward cases | ${payload.steward_cases ?? 0} |
| Duplicate risks blocked | ${payload.duplicate_risks ?? 0} |

## Fields written

- ${CANONICAL_PROPERTY_NAME_FIELD}

## Before / after examples

${examples || "_None in this run._"}

## Guards

- Brand Setup / Brand Explorer: untouched
- VIC / old Census: blocked
- Owner / operator / dates / Recent Momentum / validation fields: blocked
`;
}

function renderMatrixMarkdown(matrix) {
  const lines = [
    `# Hotel Property Census — Key Field Completion Matrix`,
    ``,
    `**Status:** \`${matrix.status}\``,
    `**Generated:** ${matrix.generated_at}`,
    `**Records:** ${matrix.total_hotel_property_census_records}`,
    `**Target:** ${matrix.production_target?.base} → ${matrix.production_target?.table} (\`${matrix.production_target?.table_id}\`)`,
    ``,
    `## Provider readiness`,
    ``,
    `- Geocode apply approved: **${matrix.provider_readiness?.approved_for_geocode_apply}**`,
    `- Block reason: ${matrix.provider_readiness?.block_reason || "—"}`,
    `- Recommended: ${matrix.provider_readiness?.recommended || "—"}`,
    ``,
    `## Summary`,
    ``,
    `| Metric | Count |`,
    `| --- | ---: |`,
    `| Autofill opportunities | ${matrix.summary?.autofill_opportunities ?? 0} |`,
    `| Provider-blocked coordinate records | ${matrix.summary?.provider_blocked_coordinate_records ?? 0} |`,
    `| Source-adapter gap records | ${matrix.summary?.source_adapter_gap_records ?? 0} |`,
    `| Steward-review gaps | ${matrix.summary?.steward_review_gaps ?? 0} |`,
    `| Canonical blank | ${matrix.summary?.canonical_property_name?.blank ?? 0} |`,
    `| Canonical dirty | ${matrix.summary?.canonical_property_name?.dirty ?? 0} |`,
    `| Canonical safe autofill | ${matrix.summary?.canonical_property_name?.safe_autofill ?? 0} |`,
    `| Canonical safe cleanup | ${matrix.summary?.canonical_property_name?.safe_cleanup ?? 0} |`,
    `| Canonical steward | ${matrix.summary?.canonical_property_name?.steward ?? 0} |`,
    ``,
    `## Completion by field`,
    ``,
    `| Group | Field | Complete | Missing | % | Autofill | Provider blocked | Source adapter | Steward |`,
    `| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |`,
  ];

  for (const f of matrix.fields || []) {
    lines.push(
      `| ${f.group} | ${f.alias || f.field} | ${f.complete} | ${f.missing} | ${f.completion_pct ?? "—"} | ${f.autofill_opportunities} | ${f.provider_blocked} | ${f.source_adapter_gaps} | ${f.steward_review} |`
    );
  }

  lines.push(
    ``,
    `## Recommended next production-cycle action`,
    ``,
    matrix.recommended_next_production_cycle_action || "—",
    ``,
    `## Guards`,
    ``,
    `- Brand Setup / Brand Explorer: read-only`,
    `- VIC / old Census: not written`,
    `- Owner / operator / dates / Recent Momentum / Company Validated / Brand Verified: blocked`,
    `- Coordinates: official source or approved provider only`,
    ``
  );
  return lines.join("\n");
}

function renderDocsMarkdown(matrix) {
  return `# Key Field Completion (Hotel Property Census)

**Status:** \`${matrix.status}\`

Autopilot queue \`${KEY_FIELD_COMPLETION_QUEUE_ID}\` classifies foundational Census fields and proposes **High-confidence** autofills only. Wired into production-cycle after \`source_discovery\` / inserts.

## Priority order

1. Core identity (Property Name, **Canonical Property Name**, Brand, City, State / Region, Country)  
2. Source URL / Source Family / confidence / Production Use Status  
3. Address  
4. State / Region  
5. Latitude / Longitude (official or approved provider)  
6. Radar / public readiness  
7. Property Type / Asset Context / Market  
8. Rooms / Keys  
9. Descriptions / Amenities  

## Coordinate rule

- Allowed: official coordinates; approved provider geocode from confirmed official address  
- Mapbox: \`MAPBOX_ACCESS_TOKEN\` + \`MAPBOX_PERMANENT_GEOCODING=1\`  
- Google: \`GOOGLE_GEOCODE_STORAGE_TERMS_REVIEWED=1\`  
- If not approved: do not write lat/long; continue other fields; route to \`provider_decision_needed\`

## Reports

- \`reports/research-engine-v2/production-census-key-field-completion-matrix.md\`
- \`reports/research-engine-v2/production-census-key-field-completion-matrix.json\`

## Latest snapshot

- Records: **${matrix.total_hotel_property_census_records}**
- Autofill opportunities: **${matrix.summary?.autofill_opportunities ?? 0}**
- Provider-blocked coordinate records: **${matrix.summary?.provider_blocked_coordinate_records ?? 0}**
- Next: ${matrix.recommended_next_production_cycle_action || "—"}
`;
}
