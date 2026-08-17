/**
 * Map / Contact / Size Readiness layer for Hotel Property Census.
 *
 * Level 1 Clean Core ≠ blocked by missing lat/long/phone/rooms.
 * Level 2 Map/Contact/Size requires Clean Core + address/coords (+ phone/rooms when official).
 * Level 3 Rich Enrichment requires Level 2 + descriptions/amenities/type/radar.
 *
 * Write target: Hotel Property Census only. Never Brand Setup / Brand Explorer / VIC.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { MAP_FIRST_PASS } from "./production-census-first-pass-enrichment.js";
import {
  CANONICAL_PROPERTY_NAME_FIELD,
  CANONICAL_NAME_STATUS,
  classifyCanonicalPropertyName,
} from "./census-canonical-property-name.js";
import {
  CITY_CLASS,
  classifyAndNormalizeCityState,
  isDescriptorCity,
} from "./census-city-state-normalizer.js";
import {
  QUALITY_GATE_STATUS,
  classifyCoreIdentityQuality,
} from "./census-core-identity-quality.js";
import {
  evaluateCoordinateCompletionEligibility,
} from "./census-coordinate-completion.js";
import {
  estimateMapboxPermanentCost,
  evaluateMapboxPermanentReadiness,
} from "./census-coordinate-provider.js";
import {
  productionHotelPropertyCensus,
} from "./production-census-source-of-truth.js";
import { inferParentCompanyForAutopilot } from "./census-autopilot-parent-inference.js";
import { buildCanonicalBrandDictionary } from "./census-brand-canonical-dictionary.js";
import { evaluateBrandSourceOfTruth } from "./census-brand-normalization.js";
import { evaluateNonActiveCleanCoreEligibility } from "./census-brand-governance.js";
import {
  evaluateParentCompanyCleanCoreGate,
} from "./census-parent-company-normalization.js";
import { isValidCoordPair } from "./production-census-coordinate-extractor.js";

/** Cached Active brand dictionary for Clean Core brand gate (read-only). */
let _brandDictionaryCache = null;
function getBrandDictionaryForCleanCore(opts = {}) {
  if (opts.brandDictionary) return opts.brandDictionary;
  if (!_brandDictionaryCache) {
    _brandDictionaryCache = buildCanonicalBrandDictionary({
      region: opts.region || "CALA",
    });
  }
  return _brandDictionaryCache;
}

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const MAP_CONTACT_SIZE_VERSION = "census-map-contact-size-readiness-v1";
export const CLEAN_CORE_CLASSIFICATION_QUEUE_ID = "clean_core_classification";
export const CORE_IDENTITY_SOURCE_LOOKUP_QUEUE_ID = "core_identity_source_lookup";

export const PHONE_FIELD = "Phone";
export const ROOMS_FIELD = "Rooms / Keys";
export const DATA_CONFIDENCE_TIER_FIELD = "Data Confidence Tier";
export const IDENTITY_CONFIDENCE_FIELD = "Identity Confidence";
export const BRAND_FAMILY_FIELD = "Brand Family";

export const READINESS_LEVEL = Object.freeze({
  CLEAN_CORE: "Level 1: Clean Core",
  MAP_CONTACT_SIZE: "Level 2: Map / Contact / Size Ready",
  RICH_ENRICHMENT: "Level 3: Rich Enrichment Ready",
  BELOW_CLEAN_CORE: "Below Clean Core",
});

export const MAP_CONTACT_SIZE_STATUS = Object.freeze({
  COMPLETE: "production_census_map_contact_size_readiness_complete",
  PARTIAL: "production_census_map_contact_size_readiness_partial_remaining",
  READY_NEEDS_PRODUCTION_CYCLE:
    "production_census_map_contact_size_readiness_ready_needs_production_cycle",
  BLOCKED: "production_census_map_contact_size_readiness_blocked",
});

function isBlank(v) {
  if (v == null) return true;
  if (typeof v === "string" && !v.trim()) return true;
  return false;
}

function numOrNull(v) {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

/**
 * Resolve parent company for Clean Core (Brand Family, Family, or safe slug inference).
 * Read-only — never writes Brand Setup.
 * @param {Record<string, unknown>} fields
 */
export function resolveCensusParentCompany(fields = {}) {
  const brandFamily = String(fields[BRAND_FAMILY_FIELD] || "").trim();
  if (brandFamily) {
    return { parent: brandFamily, source: "brand_family" };
  }
  const family = String(fields[MAP_FIRST_PASS.family] || "").trim();
  if (family) {
    return { parent: family, source: "source_family" };
  }
  const slug = String(fields[MAP_FIRST_PASS.brandSlug] || "").trim();
  const brand = String(fields[MAP_FIRST_PASS.currentBrand] || "").trim();
  if (slug || brand) {
    const inferred = inferParentCompanyForAutopilot({
      brand_slug: slug,
      slug,
      parent_company: null,
    });
    if (inferred?.parent_company && inferred.inference_confidence === "High") {
      return {
        parent: inferred.parent_company,
        source: inferred.inference_source || "safe_slug_inference_read_only",
      };
    }
  }
  return { parent: null, source: null };
}

/**
 * Data Confidence Tier OR Identity Confidence (either satisfies Clean Core tier).
 * @param {Record<string, unknown>} fields
 */
export function resolveDataConfidenceTier(fields = {}) {
  const tier = String(fields[DATA_CONFIDENCE_TIER_FIELD] || "").trim();
  if (tier) return { value: tier, field: DATA_CONFIDENCE_TIER_FIELD };
  const identity = String(fields[IDENTITY_CONFIDENCE_FIELD] || "").trim();
  if (identity) return { value: identity, field: IDENTITY_CONFIDENCE_FIELD };
  return { value: null, field: null };
}

/**
 * Level 1 — Clean Core pass (does NOT require lat/long/phone/rooms).
 * @param {object} record
 * @param {{ canonicalFieldExists?: boolean }} [opts]
 */
export function evaluateCleanCorePass(record, opts = {}) {
  const fields = record?.fields || {};
  const missing = [];
  const blockers = [];

  const propertyName = String(fields[MAP_FIRST_PASS.propertyName] || "").trim();
  const brand = String(fields[MAP_FIRST_PASS.currentBrand] || "").trim();
  const country = String(fields[MAP_FIRST_PASS.country] || "").trim();
  const sourceUrl = String(
    fields[MAP_FIRST_PASS.sourceUrl] || fields[MAP_FIRST_PASS.officialUrl] || ""
  ).trim();
  const sourceFamily = String(fields[MAP_FIRST_PASS.family] || "").trim();
  const canonical = String(fields[CANONICAL_PROPERTY_NAME_FIELD] || "").trim();

  if (!propertyName) missing.push("Property Name");
  if (!canonical) missing.push("Canonical Property Name");
  if (!brand) missing.push("Brand");
  if (!country) missing.push("Country");
  if (!sourceUrl) missing.push("Source URL");
  if (!sourceFamily) missing.push("Source Family");

  // Brand Source-of-Truth — canonical Active dictionary + Census Only official path
  let brandSot = null;
  let nonActiveClean = null;
  if (brand && opts.skipBrandSourceOfTruth !== true) {
    const dictionary = getBrandDictionaryForCleanCore(opts);
    brandSot = evaluateBrandSourceOfTruth(record, dictionary, opts);
    if (!brandSot.pass) {
      blockers.push(`brand_${brandSot.classification || "source_of_truth_fail"}`);
    }
  }

  // Non-active Clean Core eligibility (Census Only official inventory)
  if (opts.skipBrandSourceOfTruth !== true) {
    nonActiveClean = evaluateNonActiveCleanCoreEligibility(record, opts);
    if (nonActiveClean?.governance?.status === "evidence_backed_non_active_brand") {
      if (!nonActiveClean.eligible) {
        for (const r of nonActiveClean.reasons || []) {
          blockers.push(`non_active_${r}`);
        }
      }
    }
  }

  // Clean Core geography — Continent / Sub-Continent required when HPC fields exist.
  // Market / Submarket helpful but not required for Clean Core v1.
  const continent = String(fields.Continent || "").trim();
  const subContinent = String(fields["Sub-Continent"] || "").trim();
  const requireGeo =
    opts.requireContinentSubContinent === true ||
    opts.continentFieldExists === true;
  if (requireGeo) {
    if (!continent) missing.push("Continent");
    if (!subContinent) missing.push("Sub-Continent");
  }

  const parent = resolveCensusParentCompany(fields);
  if (!parent.parent) missing.push("Parent Company");

  // Brand Family must be canonical + consistent with brand / official URL
  let parentGate = null;
  if (opts.skipParentCompanyGate !== true) {
    parentGate = evaluateParentCompanyCleanCoreGate(record, {
      dictionary: opts.brandDictionary || getBrandDictionaryForCleanCore(opts),
      skipConsistencyCheck: opts.skipParentConsistencyCheck === true,
    });
    if (!parentGate.pass && parentGate.blocker) {
      blockers.push(parentGate.blocker);
    }
  }

  const confidence = resolveDataConfidenceTier(fields);
  if (!confidence.value) missing.push("Data Confidence Tier");

  const cityNorm = classifyAndNormalizeCityState(fields);
  if (
    !cityNorm.city_clean ||
    cityNorm.class === CITY_CLASS.UNKNOWN ||
    cityNorm.class === CITY_CLASS.DESCRIPTOR ||
    cityNorm.class === CITY_CLASS.BLANK ||
    cityNorm.class === CITY_CLASS.MIXED_UNRESOLVED ||
    isDescriptorCity(fields.City)
  ) {
    blockers.push(`city_${cityNorm.class || "dirty"}`);
  }
  if (cityNorm.write_allowed) {
    blockers.push("city_pending_normalize");
  }

  const canon = classifyCanonicalPropertyName(fields, {
    fieldExists: opts.canonicalFieldExists !== false,
  });
  const nonActiveHrOk =
    nonActiveClean?.eligible === true &&
    nonActiveClean?.governance?.status === "evidence_backed_non_active_brand";
  if (canon.status !== CANONICAL_NAME_STATUS.COMPLETE_CLEAN) {
    // HR on evidence-backed Census Only forces canonical steward — do not double-block Clean Core
    const hrDrivenCanonicalSteward =
      nonActiveHrOk &&
      fields[MAP_FIRST_PASS.humanReview] === true &&
      canon.status === CANONICAL_NAME_STATUS.STEWARD_REVIEW_REQUIRED;
    if (!hrDrivenCanonicalSteward) {
      blockers.push(`canonical_${canon.status}`);
    }
  }

  if (fields[MAP_FIRST_PASS.humanReview] === true) {
    // Evidence-backed non-active Census Only may remain Clean Core with HR for steward visibility
    const allowHr =
      nonActiveHrOk || opts.allowHumanReviewOnNonActiveCleanCore === true;
    if (!allowHr) {
      blockers.push("human_review_required");
    }
  }

  const identity = classifyCoreIdentityQuality(record, opts);
  if (
    identity.gate_status === QUALITY_GATE_STATUS.BLOCKED_DIRTY_CORE_IDENTITY ||
    identity.gate_status === QUALITY_GATE_STATUS.DUPLICATE_RISK ||
    identity.gate_status === QUALITY_GATE_STATUS.BLOCKED_IDENTITY_CONFLICT
  ) {
    blockers.push(identity.gate_status);
  }

  const pass = missing.length === 0 && blockers.length === 0;

  return {
    pass,
    level: pass ? READINESS_LEVEL.CLEAN_CORE : READINESS_LEVEL.BELOW_CLEAN_CORE,
    missing,
    blockers,
    parent,
    parent_company_gate: parentGate,
    confidence,
    city_class: cityNorm.class,
    canonical_status: canon.status,
    identity_gate_status: identity.gate_status,
    brand_source_of_truth: brandSot,
    non_active_clean_core: nonActiveClean,
    does_not_require_lat_long_phone_rooms: true,
  };
}

/**
 * Level 2 / 3 readiness classification for one record.
 * @param {object} record
 * @param {{
 *   phoneFieldExists?: boolean,
 *   canonicalFieldExists?: boolean,
 * }} [opts]
 */
export function classifyMapContactSizeReadiness(record, opts = {}) {
  const fields = record?.fields || {};
  const clean = evaluateCleanCorePass(record, opts);
  const phoneFieldExists = opts.phoneFieldExists !== false;

  const address = String(fields[MAP_FIRST_PASS.address] || "").trim();
  const addressComplete = Boolean(address);
  const lat = numOrNull(fields[MAP_FIRST_PASS.latitude]);
  const lng = numOrNull(fields[MAP_FIRST_PASS.longitude]);
  const coordsComplete =
    lat != null && lng != null && isValidCoordPair(lat, lng);

  const phoneRaw = phoneFieldExists ? fields[PHONE_FIELD] : null;
  const phoneComplete = phoneFieldExists && !isBlank(phoneRaw);
  const officialUrl = String(
    fields[MAP_FIRST_PASS.officialUrl] || fields[MAP_FIRST_PASS.sourceUrl] || ""
  ).trim();
  const phoneSourceAvailable =
    phoneFieldExists && !phoneComplete && Boolean(officialUrl);

  const rooms = fields[ROOMS_FIELD];
  const roomsComplete = !isBlank(rooms) && Number(rooms) > 0;
  const roomsSourceAvailable = !roomsComplete && Boolean(officialUrl);

  const coordElig = evaluateCoordinateCompletionEligibility(record, opts);
  const latLongEligible = Boolean(coordElig.eligible);

  const blockedDirtyIdentity =
    !clean.pass &&
    (clean.blockers.some((b) => /dirty|unknown|descriptor|canonical/i.test(b)) ||
      clean.identity_gate_status === QUALITY_GATE_STATUS.BLOCKED_DIRTY_CORE_IDENTITY);

  const blockedMissingAddress = clean.pass && !addressComplete;
  const blockedSourceInsufficient =
    clean.pass &&
    addressComplete &&
    !coordsComplete &&
    !latLongEligible &&
    (coordElig.reason === "address_confidence_not_high" ||
      coordElig.reason === "missing_address_source_url" ||
      coordElig.reason === "missing_source_url");

  const phoneOk = !phoneFieldExists || phoneComplete || !phoneSourceAvailable;
  const roomsOk = roomsComplete || !roomsSourceAvailable;
  const level2 =
    clean.pass &&
    addressComplete &&
    coordsComplete &&
    phoneOk &&
    roomsOk;

  const description = !isBlank(fields[MAP_FIRST_PASS.descriptionSource]);
  const amenities = !isBlank(fields[MAP_FIRST_PASS.amenitiesSource]);
  const propertyType = !isBlank(fields[MAP_FIRST_PASS.propertyType]);
  const assetContext = !isBlank(fields[MAP_FIRST_PASS.assetContext]);
  const market = !isBlank(fields[MAP_FIRST_PASS.marketSubmarket]);
  const radar = !isBlank(fields[MAP_FIRST_PASS.radarDisplayStatus]);

  const level3 =
    level2 && description && amenities && propertyType && assetContext && market && radar;

  let level = READINESS_LEVEL.BELOW_CLEAN_CORE;
  if (level3) level = READINESS_LEVEL.RICH_ENRICHMENT;
  else if (level2) level = READINESS_LEVEL.MAP_CONTACT_SIZE;
  else if (clean.pass) level = READINESS_LEVEL.CLEAN_CORE;

  return {
    record_id: record?.id || null,
    level,
    clean_core: clean,
    address_complete: addressComplete,
    lat_long_complete: coordsComplete,
    lat_long_eligible: latLongEligible,
    lat_long_block_reason: coordElig.eligible ? null : coordElig.reason || null,
    phone_field_exists: phoneFieldExists,
    phone_complete: phoneComplete,
    phone_source_available: phoneSourceAvailable,
    rooms_complete: roomsComplete,
    rooms_source_available: roomsSourceAvailable,
    blocked_dirty_identity: blockedDirtyIdentity,
    blocked_missing_address: blockedMissingAddress,
    blocked_source_insufficient: blockedSourceInsufficient,
    level2_ready: level2,
    level3_ready: level3,
  };
}

/**
 * Audit all Census records; write reports.
 * @param {{
 *   censusRecords?: object[],
 *   phoneFieldExists?: boolean,
 *   canonicalFieldExists?: boolean,
 *   env?: object,
 *   writeReports?: boolean,
 *   runDir?: string|null,
 * }} [opts]
 */
export function runMapContactSizeReadinessAudit(opts = {}) {
  const records = opts.censusRecords || [];
  const phoneFieldExists = opts.phoneFieldExists !== false;
  const env = opts.env || process.env;
  const mapbox = evaluateMapboxPermanentReadiness(env);

  const counters = {
    total_records: records.length,
    clean_core: 0,
    map_contact_size_ready: 0,
    rich_enrichment_ready: 0,
    below_clean_core: 0,
    address_complete: 0,
    lat_long_complete: 0,
    lat_long_eligible: 0,
    phone_complete: 0,
    phone_source_available: 0,
    rooms_complete: 0,
    rooms_source_available: 0,
    blocked_dirty_identity: 0,
    blocked_missing_address: 0,
    blocked_source_insufficient: 0,
    steward_conflicts: 0,
  };

  const rows = [];
  for (const rec of records) {
    const row = classifyMapContactSizeReadiness(rec, {
      phoneFieldExists,
      canonicalFieldExists: opts.canonicalFieldExists !== false,
      continentFieldExists: opts.continentFieldExists === true,
      requireContinentSubContinent: opts.requireContinentSubContinent === true,
    });
    rows.push(row);

    if (row.level === READINESS_LEVEL.RICH_ENRICHMENT) {
      counters.rich_enrichment_ready += 1;
      counters.map_contact_size_ready += 1;
      counters.clean_core += 1;
    } else if (row.level === READINESS_LEVEL.MAP_CONTACT_SIZE) {
      counters.map_contact_size_ready += 1;
      counters.clean_core += 1;
    } else if (row.level === READINESS_LEVEL.CLEAN_CORE) {
      counters.clean_core += 1;
    } else {
      counters.below_clean_core += 1;
    }

    if (row.address_complete) counters.address_complete += 1;
    if (row.lat_long_complete) counters.lat_long_complete += 1;
    if (row.lat_long_eligible) counters.lat_long_eligible += 1;
    if (row.phone_complete) counters.phone_complete += 1;
    if (row.phone_source_available) counters.phone_source_available += 1;
    if (row.rooms_complete) counters.rooms_complete += 1;
    if (row.rooms_source_available) counters.rooms_source_available += 1;
    if (row.blocked_dirty_identity) counters.blocked_dirty_identity += 1;
    if (row.blocked_missing_address) counters.blocked_missing_address += 1;
    if (row.blocked_source_insufficient) counters.blocked_source_insufficient += 1;
  }

  const estimatedRequests = counters.lat_long_eligible;
  const cost = estimateMapboxPermanentCost(estimatedRequests, env);

  let status = MAP_CONTACT_SIZE_STATUS.PARTIAL;
  if (counters.total_records === 0) {
    status = MAP_CONTACT_SIZE_STATUS.BLOCKED;
  } else if (counters.lat_long_eligible > 0 && mapbox.ready) {
    status = MAP_CONTACT_SIZE_STATUS.READY_NEEDS_PRODUCTION_CYCLE;
  } else if (
    counters.lat_long_eligible === 0 &&
    counters.phone_source_available === 0 &&
    counters.rooms_source_available === 0 &&
    counters.below_clean_core === 0 &&
    counters.map_contact_size_ready === counters.clean_core
  ) {
    status = MAP_CONTACT_SIZE_STATUS.COMPLETE;
  }

  const report = {
    ok: status !== MAP_CONTACT_SIZE_STATUS.BLOCKED,
    version: MAP_CONTACT_SIZE_VERSION,
    generated_at: new Date().toISOString(),
    status,
    write_target: {
      base: productionHotelPropertyCensus.baseName,
      table: productionHotelPropertyCensus.tableName,
      table_id: productionHotelPropertyCensus.tableId,
    },
    airtable_writes: false,
    brand_setup_writes: false,
    brand_explorer_writes: false,
    phone_field: PHONE_FIELD,
    phone_field_exists: phoneFieldExists,
    phone_schema_note: phoneFieldExists
      ? "Phone field present — High official enrichment allowed"
      : "Phone field missing — report phone_source_available only; no schema create",
    counters,
    mapbox_permanent: {
      ready: mapbox.ready,
      missing_flags: mapbox.missing_flags,
      estimated_geocode_requests: estimatedRequests,
      estimated_cost: cost,
    },
    production_cycle_order: [
      "source_discovery",
      "core_identity_quality",
      "core_identity_source_lookup",
      "clean_core_classification",
      "key_field_completion",
      "address_confirmation",
      "coordinate_completion",
      "phone_number_enrichment",
      "rooms_keys",
      "property_type_asset_context",
      "description_extraction",
      "amenities_extraction",
      "radar_public_readiness",
    ],
    sample_below_clean_core: rows
      .filter((r) => r.level === READINESS_LEVEL.BELOW_CLEAN_CORE)
      .slice(0, 15)
      .map((r) => ({
        record_id: r.record_id,
        missing: r.clean_core.missing,
        blockers: r.clean_core.blockers,
      })),
    sample_lat_long_eligible: rows
      .filter((r) => r.lat_long_eligible)
      .slice(0, 10)
      .map((r) => r.record_id),
  };

  if (opts.writeReports !== false) {
    writeMapContactSizeReports(report, { runDir: opts.runDir || null });
  }

  return report;
}

export function writeMapContactSizeReports(report, opts = {}) {
  const reportsDir = path.join(ROOT, "reports/research-engine-v2");
  const docsDir = path.join(ROOT, "docs/data-intelligence");
  fs.mkdirSync(reportsDir, { recursive: true });
  fs.mkdirSync(docsDir, { recursive: true });

  const jsonPath = path.join(reportsDir, "production-census-map-contact-size-readiness.json");
  const mdPath = path.join(reportsDir, "production-census-map-contact-size-readiness.md");
  const docsPath = path.join(docsDir, "production-census-map-contact-size-readiness.md");
  const md = renderMapContactSizeMarkdown(report);

  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2), "utf8");
  fs.writeFileSync(mdPath, md, "utf8");
  fs.writeFileSync(
    docsPath,
    `# Map / Contact / Size Readiness

${md}

## Rules

- Clean Core does **not** require Latitude, Longitude, Phone, or Rooms / Keys.
- Coordinates only after Clean Core + High Address + Address Source URL + Mapbox Permanent (or official coords).
- Phone only from official property / directory / JSON-LD — never OTA / Google / Mapbox.
- Rooms / Keys only High official property-level counts — never inferred.
- Brand Setup / Brand Explorer / VIC / owner-operator / dates blocked.

## Commands

\`\`\`bash
npm run census:autopilot -- --region CALA --scope active-brand-setup --mode controlled \\
  --strategy fastest-safe --queue clean_core_classification --run-until-complete --batch-size 250
\`\`\`
`,
    "utf8"
  );

  if (opts.runDir) {
    fs.mkdirSync(opts.runDir, { recursive: true });
    fs.writeFileSync(
      path.join(opts.runDir, "map-contact-size-readiness.json"),
      JSON.stringify(report, null, 2),
      "utf8"
    );
  }

  return { jsonPath, mdPath, docsPath };
}

export function renderMapContactSizeMarkdown(report) {
  const c = report.counters || {};
  const cost = report.mapbox_permanent?.estimated_cost || {};
  return `# Production Census — Map / Contact / Size Readiness

**Status:** \`${report.status}\`  
**Generated:** ${report.generated_at}  
**Write target:** ${report.write_target?.base} → ${report.write_target?.table} (\`${report.write_target?.table_id}\`)  
**Phone field:** \`${report.phone_field}\` (exists=${report.phone_field_exists})  
**Airtable writes:** ${report.airtable_writes ? "yes" : "no (audit)"}

## Counts

| Metric | Count |
|--------|------:|
| Total records | ${c.total_records ?? 0} |
| Clean Core (Level 1) | ${c.clean_core ?? 0} |
| Map / Contact / Size Ready (Level 2) | ${c.map_contact_size_ready ?? 0} |
| Rich Enrichment Ready (Level 3) | ${c.rich_enrichment_ready ?? 0} |
| Below Clean Core | ${c.below_clean_core ?? 0} |
| Address complete | ${c.address_complete ?? 0} |
| Lat/Long complete | ${c.lat_long_complete ?? 0} |
| Lat/Long eligible (Mapbox) | ${c.lat_long_eligible ?? 0} |
| Phone complete | ${c.phone_complete ?? 0} |
| Phone source available | ${c.phone_source_available ?? 0} |
| Rooms complete | ${c.rooms_complete ?? 0} |
| Rooms source available | ${c.rooms_source_available ?? 0} |
| Blocked dirty identity | ${c.blocked_dirty_identity ?? 0} |
| Blocked missing address | ${c.blocked_missing_address ?? 0} |
| Blocked source insufficient | ${c.blocked_source_insufficient ?? 0} |

## Mapbox Permanent

- Ready: ${report.mapbox_permanent?.ready ? "yes" : "no"}
- Estimated geocode requests: ${report.mapbox_permanent?.estimated_geocode_requests ?? 0}
- Estimated cost (USD): ${cost.estimated_usd ?? "n/a"} (${cost.basis || cost.note || "—"})

## Production-cycle order

${(report.production_cycle_order || []).map((q, i) => `${i + 1}. ${q}`).join("\n")}

## Guards

- No Clean Core block for missing lat/long/phone/rooms
- No Mapbox on dirty identity
- No phone from third-party sources
- No weak room inference
`;
}

/**
 * Source-lookup routing for dirty identity that may need official page research.
 * No Airtable writes.
 */
export function runCoreIdentitySourceLookup(opts = {}) {
  const records = opts.censusRecords || [];
  const needing = [];
  for (const rec of records) {
    const clean = evaluateCleanCorePass(rec, opts);
    if (clean.pass) continue;
    const fields = rec.fields || {};
    const hasUrl = Boolean(
      String(fields[MAP_FIRST_PASS.officialUrl] || fields[MAP_FIRST_PASS.sourceUrl] || "").trim()
    );
    needing.push({
      record_id: rec.id,
      missing: clean.missing,
      blockers: clean.blockers,
      has_official_url: hasUrl,
      route: hasUrl ? "official_page_source_lookup" : "steward_no_source",
    });
  }
  return {
    version: MAP_CONTACT_SIZE_VERSION,
    queue_id: CORE_IDENTITY_SOURCE_LOOKUP_QUEUE_ID,
    airtable_writes: false,
    candidates: needing,
    counters: {
      scanned: records.length,
      needing_lookup: needing.length,
      with_official_url: needing.filter((n) => n.has_official_url).length,
    },
  };
}
