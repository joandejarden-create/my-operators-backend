/**
 * Clean Core Identity Repair Sprint — audit, classify, report.
 *
 * Focus: Canonical Property Name, City, State / Region only.
 * Never address / Mapbox / phone / rooms / discovery inserts.
 * Write target: Hotel Property Census (tbl9aY5ijiuIzzWam) only.
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
  CALA_CITY_CANONICAL,
  classifyAndNormalizeCityState,
  isAllCapsCity,
  isAllLowerCity,
  isDescriptorCity,
  canonicalCalaCity,
  normalizePlaceKey,
} from "./census-city-state-normalizer.js";
import {
  evaluateCleanCorePass,
  READINESS_LEVEL,
} from "./census-map-contact-size-readiness.js";
import {
  evaluateCoordinateIdentityGate,
  QUALITY_GATE_STATUS,
} from "./census-core-identity-quality.js";
import { productionHotelPropertyCensus } from "./production-census-source-of-truth.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const CLEAN_CORE_IDENTITY_REPAIR_VERSION =
  "census-clean-core-identity-repair-v1";

export const CLEAN_CORE_IDENTITY_REPAIR_STATUS = Object.freeze({
  COMPLETE: "production_census_clean_core_identity_repair_complete",
  PARTIAL_SOURCE_LOOKUP:
    "production_census_clean_core_identity_repair_partial_source_lookup_remaining",
  BLOCKED: "production_census_clean_core_identity_repair_blocked",
});

/** Queues allowed during cleanup-existing-only / identity repair sprint. */
export const CLEAN_CORE_IDENTITY_REPAIR_QUEUE_ORDER = Object.freeze([
  "brand_normalization",
  "parent_company_normalization",
  "core_identity_quality",
  "core_identity_source_lookup",
  "canonical_property_name_completion",
  "city_state_normalization",
  "market_geography_completion",
  "key_field_completion",
  "clean_core_classification",
]);

/** Alias → canonical queue id (dedupe when comma-separated). */
export const CLEAN_CORE_QUEUE_ALIASES = Object.freeze({
  city_state_normalization: "core_identity_quality",
  canonical_property_name_completion: "key_field_completion",
});

/** Fields writable during identity repair (High only). */
export const CLEAN_CORE_IDENTITY_WRITE_FIELDS = Object.freeze([
  "Property Name",
  "Canonical Property Name",
  "Current Brand",
  "Brand Family",
  "City",
  "State / Region",
  "Country",
  "Source URL",
  "Official Property URL",
  "Family / Source Family",
  "Source Type",
  "Source Confidence",
  "Identity Confidence",
  "Data Confidence Tier",
  "Production Use Status",
  "Human Review Required",
  "Enrichment Status",
  "Enrichment Priority",
  "Continent",
  "Sub-Continent",
  "Market",
  "Submarket",
  "Last Reviewed Date",
]);

export const CLEAN_CORE_IDENTITY_FORBIDDEN_WRITE_FIELDS = Object.freeze([
  "Latitude",
  "Longitude",
  "Phone",
  "Rooms / Keys",
  "Address",
  "Address Confidence",
  "Address Source URL",
  "Coordinate Source Type",
  "Coordinate Confidence",
  "Geocode Provider",
  "Geocode Method",
  "Geocode Reviewed Date",
]);

export const RECORD_CLASS = Object.freeze({
  CLEAN_CORE: "Clean Core",
  NEEDS_SOURCE_LOOKUP: "Needs Source Lookup",
  NEEDS_STEWARD_REVIEW: "Needs Steward Review",
  DUPLICATE_RISK: "Duplicate Risk",
  NOT_USABLE_YET: "Not Usable Yet",
});

function isBlank(v) {
  if (v == null) return true;
  if (typeof v === "string" && !v.trim()) return true;
  return false;
}

/**
 * Parse queue CLI (supports comma-separated) + resolve aliases + dedupe.
 * @param {string|string[]|null} queueRaw
 * @param {{ cleanupExistingOnly?: boolean }} [opts]
 */
export function resolveCleanCoreIdentityQueues(queueRaw, opts = {}) {
  let list = [];
  if (Array.isArray(queueRaw)) {
    list = queueRaw.flatMap((q) => String(q).split(",")).map((s) => s.trim()).filter(Boolean);
  } else if (queueRaw) {
    list = String(queueRaw)
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
  } else if (opts.cleanupExistingOnly) {
    list = [...CLEAN_CORE_IDENTITY_REPAIR_QUEUE_ORDER];
  }

  const resolved = [];
  const seen = new Set();
  for (const id of list) {
    const canon = CLEAN_CORE_QUEUE_ALIASES[id] || id;
    if (seen.has(canon)) continue;
    // Drop queues not in repair allowlist when cleanup mode
    if (
      opts.cleanupExistingOnly &&
      !CLEAN_CORE_IDENTITY_REPAIR_QUEUE_ORDER.includes(canon) &&
      !CLEAN_CORE_IDENTITY_REPAIR_QUEUE_ORDER.includes(id)
    ) {
      continue;
    }
    seen.add(canon);
    resolved.push(canon);
  }
  return resolved;
}

/**
 * Strip non-identity fields from High proposals (cleanup-existing-only).
 * @param {object[]} proposals
 */
export function filterCleanCoreIdentityProposals(proposals = []) {
  const out = [];
  for (const p of proposals || []) {
    if (p.action === "insert" || p.type === "insert") continue;
    if (p.queue === "source_discovery") continue;
    const patch = { ...(p.patch || p.fields || {}) };
    for (const k of Object.keys(patch)) {
      if (CLEAN_CORE_IDENTITY_FORBIDDEN_WRITE_FIELDS.includes(k)) {
        delete patch[k];
      } else if (!CLEAN_CORE_IDENTITY_WRITE_FIELDS.includes(k)) {
        // Allow only identity write set during repair sprint
        delete patch[k];
      }
    }
    if (!Object.keys(patch).length) continue;
    out.push({ ...p, patch, fields: patch });
  }
  return out;
}

/**
 * High-confidence city from official URL slug when City is Unknown/blank.
 * Only known CALA_CITY_CANONICAL tokens — never weak hotel-name inference.
 * @param {string} url
 * @param {string} [country]
 */
export function tryCityFromOfficialUrlSlug(url, country = "") {
  const u = String(url || "").toLowerCase();
  if (!u || !/^https?:\/\//i.test(u)) {
    return { ok: false, reason: "no_url" };
  }
  if (/booking\.com|expedia\.|tripadvisor\.|google\.|mapbox\./i.test(u)) {
    return { ok: false, reason: "forbidden_host" };
  }

  // Prefer longer keys first (san jose del cabo before san jose)
  const keys = Object.keys(CALA_CITY_CANONICAL).sort((a, b) => b.length - a.length);
  const slug = u.replace(/^https?:\/\//, "").replace(/[?#].*$/, "");
  let matched = null;
  for (const key of keys) {
    if (key.length < 5 && key !== "lima" && key !== "cali") continue;
    const token = key.replace(/\s+/g, "[-_/]?");
    const re = new RegExp(`(?:^|[-_/])${token}(?:[-_/]|\\.html|\\.shtml|/|$)`, "i");
    if (re.test(slug) || slug.includes(key.replace(/\s+/g, "-")) || slug.includes(key.replace(/\s+/g, "_"))) {
      matched = key;
      break;
    }
  }
  if (!matched) return { ok: false, reason: "no_known_city_token" };

  const city = CALA_CITY_CANONICAL[matched];
  // Country soft-check for Mexico-specific cities
  const mxOnly = /cancun|queretaro|merida|guadalajara|monterrey|vallarta|cabo|oaxaca|puebla|toluca|guadalupe/i.test(
    matched
  );
  const c = String(country || "").toLowerCase();
  if (mxOnly && c && !/mexico|méxico/.test(c)) {
    return { ok: false, reason: "country_mismatch", city };
  }

  return {
    ok: true,
    confidence: "High",
    city,
    reason: "official_url_slug_known_cala_city",
    matched_key: matched,
  };
}

/**
 * Bucket one record's identity issues.
 * @param {object} record
 */
export function auditCoreIdentityRecord(record, opts = {}) {
  const fields = record?.fields || {};
  const cityRaw = String(fields[MAP_FIRST_PASS.city] || "").trim();
  const stateRaw = String(fields[MAP_FIRST_PASS.stateRegion] || "").trim();
  const sourceUrl = String(
    fields[MAP_FIRST_PASS.sourceUrl] || fields[MAP_FIRST_PASS.officialUrl] || ""
  ).trim();
  const family = String(fields[MAP_FIRST_PASS.family] || "").trim();

  const cityNorm = classifyAndNormalizeCityState(fields);
  const canon = classifyCanonicalPropertyName(fields, {
    fieldExists: opts.canonicalFieldExists !== false,
  });
  const clean = evaluateCleanCorePass(record, opts);
  const coordGate = evaluateCoordinateIdentityGate(record, opts);

  const buckets = {
    canonical: {
      blank: isBlank(fields[CANONICAL_PROPERTY_NAME_FIELD]),
      dirty: canon.status === CANONICAL_NAME_STATUS.DIRTY_CAN_CLEAN,
      duplicate_risk: false,
      conflict: canon.status === CANONICAL_NAME_STATUS.POPULATED_CONFLICT_NEEDS_REVIEW,
      safe_autofill:
        canon.status === CANONICAL_NAME_STATUS.BLANK_CAN_AUTOFILL ||
        canon.status === CANONICAL_NAME_STATUS.DIRTY_CAN_CLEAN,
      source_lookup_needed:
        canon.status === CANONICAL_NAME_STATUS.MISSING_SOURCE_SUPPORT ||
        (isBlank(fields[CANONICAL_PROPERTY_NAME_FIELD]) &&
          canon.status !== CANONICAL_NAME_STATUS.BLANK_CAN_AUTOFILL),
      status: canon.status,
    },
    city: {
      unknown: cityNorm.class === CITY_CLASS.UNKNOWN || /^unknown$/i.test(cityRaw),
      blank: cityNorm.class === CITY_CLASS.BLANK || isBlank(cityRaw),
      // True descriptors only — Unknown/N/A are separate buckets (not "Adults Only" style)
      descriptor:
        cityNorm.class === CITY_CLASS.DESCRIPTOR ||
        (isDescriptorCity(cityRaw) &&
          !/^unknown$/i.test(cityRaw) &&
          !/^n\/?a$/i.test(cityRaw) &&
          !/^null$/i.test(cityRaw) &&
          !/^none$/i.test(cityRaw) &&
          !/^tbd$/i.test(cityRaw)),
      all_caps: isAllCapsCity(cityRaw),
      all_lowercase: isAllLowerCity(cityRaw),
      city_state_combined:
        cityNorm.class === CITY_CLASS.SPLIT_CITY_STATE ||
        cityNorm.class === CITY_CLASS.MIXED_UNRESOLVED ||
        (cityRaw.includes(",") && cityNorm.class !== CITY_CLASS.CLEAN),
      malformed: cityNorm.class === CITY_CLASS.STEWARD,
      safe_normalization: Boolean(cityNorm.write_allowed),
      source_lookup_needed:
        cityNorm.class === CITY_CLASS.UNKNOWN ||
        cityNorm.class === CITY_CLASS.DESCRIPTOR ||
        cityNorm.class === CITY_CLASS.BLANK ||
        cityNorm.class === CITY_CLASS.MIXED_UNRESOLVED,
      class: cityNorm.class,
    },
    state: {
      blank: isBlank(stateRaw),
      all_caps: isAllCapsCity(stateRaw),
      all_lowercase: isAllLowerCity(stateRaw),
      included_in_city: cityRaw.includes(",") && cityNorm.write_allowed,
      safe_normalization: Boolean(
        cityNorm.patch?.["State / Region"] ||
          (stateRaw && canonicalCalaCity(stateRaw) === null && normalizePlaceKey(stateRaw))
      ),
      source_lookup_needed: isBlank(stateRaw) && !cityNorm.write_allowed,
    },
    source: {
      url_missing: isBlank(sourceUrl),
      family_missing: isBlank(family),
      source_insufficient: isBlank(sourceUrl),
    },
  };

  // URL slug High city — prefer Official Property URL; directory Source URLs never resolve
  let urlCity = null;
  if (
    (buckets.city.unknown || buckets.city.blank || buckets.city.descriptor) &&
    !buckets.city.safe_normalization
  ) {
    const officialUrl = String(fields[MAP_FIRST_PASS.officialUrl] || "").trim();
    for (const u of [officialUrl, sourceUrl].filter(Boolean)) {
      const slugTry = tryCityFromOfficialUrlSlug(u, fields[MAP_FIRST_PASS.country]);
      if (slugTry.ok) {
        urlCity = slugTry;
        buckets.city.safe_normalization = true;
        buckets.city.source_lookup_needed = false;
        break;
      }
    }
  }

  let recordClass = RECORD_CLASS.NOT_USABLE_YET;
  if (clean.pass) {
    recordClass = RECORD_CLASS.CLEAN_CORE;
  } else if (
    clean.identity_gate_status === QUALITY_GATE_STATUS.DUPLICATE_RISK ||
    canon.status === CANONICAL_NAME_STATUS.STEWARD_REVIEW_REQUIRED
  ) {
    recordClass = RECORD_CLASS.DUPLICATE_RISK;
    buckets.canonical.duplicate_risk = true;
  } else if (
    buckets.city.source_lookup_needed ||
    buckets.canonical.source_lookup_needed ||
    buckets.source.url_missing
  ) {
    recordClass = sourceUrl
      ? RECORD_CLASS.NEEDS_SOURCE_LOOKUP
      : RECORD_CLASS.NEEDS_STEWARD_REVIEW;
  } else if (
    clean.blockers.length ||
    clean.missing.length ||
    buckets.canonical.conflict
  ) {
    recordClass = RECORD_CLASS.NEEDS_STEWARD_REVIEW;
  }

  return {
    record_id: record?.id || null,
    property_name: fields[MAP_FIRST_PASS.propertyName] || null,
    brand: fields[MAP_FIRST_PASS.currentBrand] || null,
    family: family || null,
    buckets,
    clean_core: clean,
    record_class: recordClass,
    coordinate_blocked_dirty_identity: !coordGate.allow_geocode,
    url_city_proposal: urlCity,
    city_patch: cityNorm.write_allowed ? cityNorm.patch : urlCity?.ok ? { City: urlCity.city } : null,
    level: clean.pass ? READINESS_LEVEL.CLEAN_CORE : READINESS_LEVEL.BELOW_CLEAN_CORE,
  };
}

/**
 * Aggregate audit counters across Census.
 * @param {object[]} censusRecords
 */
export function auditAllCoreIdentityIssues(censusRecords = [], opts = {}) {
  const counters = {
    total_records: censusRecords.length,
    clean_core: 0,
    below_clean_core: 0,
    canonical_blank: 0,
    canonical_dirty: 0,
    canonical_conflict: 0,
    canonical_safe_autofill: 0,
    canonical_duplicate_risk: 0,
    unknown_city: 0,
    blank_city: 0,
    descriptor_city: 0,
    all_caps_city: 0,
    all_lowercase_city: 0,
    city_state_combined: 0,
    city_safe_normalization: 0,
    state_region_complete: 0,
    state_blank: 0,
    source_url_complete: 0,
    source_url_missing: 0,
    human_review_required: 0,
    coordinate_blocked_dirty_identity: 0,
    by_class: {
      [RECORD_CLASS.CLEAN_CORE]: 0,
      [RECORD_CLASS.NEEDS_SOURCE_LOOKUP]: 0,
      [RECORD_CLASS.NEEDS_STEWARD_REVIEW]: 0,
      [RECORD_CLASS.DUPLICATE_RISK]: 0,
      [RECORD_CLASS.NOT_USABLE_YET]: 0,
    },
    by_parent: {},
  };

  const rows = [];
  const urlCityProposals = [];

  for (const rec of censusRecords) {
    const row = auditCoreIdentityRecord(rec, opts);
    rows.push(row);
    const b = row.buckets;
    const parent =
      String(rec.fields?.["Brand Family"] || rec.fields?.[MAP_FIRST_PASS.family] || "Unknown").trim() ||
      "Unknown";
    if (!counters.by_parent[parent]) {
      counters.by_parent[parent] = { below_clean_core: 0, unknown_city: 0, canonical_blank: 0 };
    }

    if (row.clean_core.pass) counters.clean_core += 1;
    else {
      counters.below_clean_core += 1;
      counters.by_parent[parent].below_clean_core += 1;
    }

    if (b.canonical.blank) {
      counters.canonical_blank += 1;
      counters.by_parent[parent].canonical_blank += 1;
    }
    if (b.canonical.dirty) counters.canonical_dirty += 1;
    if (b.canonical.conflict) counters.canonical_conflict += 1;
    if (b.canonical.safe_autofill) counters.canonical_safe_autofill += 1;
    if (b.canonical.duplicate_risk) counters.canonical_duplicate_risk += 1;

    if (b.city.unknown) {
      counters.unknown_city += 1;
      counters.by_parent[parent].unknown_city += 1;
    }
    if (b.city.blank) counters.blank_city += 1;
    if (b.city.descriptor) counters.descriptor_city += 1;
    if (b.city.all_caps) counters.all_caps_city += 1;
    if (b.city.all_lowercase) counters.all_lowercase_city += 1;
    if (b.city.city_state_combined) counters.city_state_combined += 1;
    if (b.city.safe_normalization) counters.city_safe_normalization += 1;

    if (!b.state.blank) counters.state_region_complete += 1;
    else counters.state_blank += 1;

    if (!b.source.url_missing) counters.source_url_complete += 1;
    else counters.source_url_missing += 1;

    if (rec.fields?.[MAP_FIRST_PASS.humanReview] === true) {
      counters.human_review_required += 1;
    }
    if (row.coordinate_blocked_dirty_identity) {
      counters.coordinate_blocked_dirty_identity += 1;
    }

    counters.by_class[row.record_class] =
      (counters.by_class[row.record_class] || 0) + 1;

    if (row.url_city_proposal?.ok) {
      urlCityProposals.push({
        record_id: row.record_id,
        city: row.url_city_proposal.city,
        reason: row.url_city_proposal.reason,
      });
    }
  }

  return { counters, rows, url_city_proposals: urlCityProposals };
}

/**
 * Build High proposals from official URL slug city resolutions.
 */
export function buildUrlSlugCityProposals(auditRows = []) {
  const proposals = [];
  for (const row of auditRows) {
    if (!row.url_city_proposal?.ok || !row.city_patch?.City) continue;
    // Only when current city is Unknown/blank/descriptor — never overwrite clean city via slug
    if (
      !(
        row.buckets.city.unknown ||
        row.buckets.city.blank ||
        row.buckets.city.descriptor
      )
    ) {
      continue;
    }
    proposals.push({
      record_id: row.record_id,
      queue: "core_identity_quality",
      action: "propose_high_write",
      confidence: "High",
      write_allowed_now: true,
      allow_normalization_overwrite: true,
      patch: row.city_patch,
      method: "official_url_slug_known_cala_city",
      notes: "High URL-slug city for Unknown/blank only; no hotel-name inference",
    });
  }
  return proposals;
}

/**
 * Full repair report (before and optional after).
 */
export function buildCleanCoreIdentityRepairReport(opts = {}) {
  const before = opts.before || auditAllCoreIdentityIssues(opts.censusRecordsBefore || [], opts);
  const after = opts.after
    ? opts.after
    : opts.censusRecordsAfter
      ? auditAllCoreIdentityIssues(opts.censusRecordsAfter, opts)
      : null;

  const applied = opts.applied || {
    records_fixed: 0,
    fields_written: [],
    examples: [],
  };

  let status = CLEAN_CORE_IDENTITY_REPAIR_STATUS.PARTIAL_SOURCE_LOOKUP;
  const c = after?.counters || before.counters;
  if (c.below_clean_core === 0 && c.unknown_city === 0 && c.canonical_blank === 0) {
    status = CLEAN_CORE_IDENTITY_REPAIR_STATUS.COMPLETE;
  } else if (opts.blocked) {
    status = CLEAN_CORE_IDENTITY_REPAIR_STATUS.BLOCKED;
  }

  const topGaps = Object.entries(c.by_parent || {})
    .map(([parent, v]) => ({
      parent,
      below_clean_core: v.below_clean_core || 0,
      unknown_city: v.unknown_city || 0,
      canonical_blank: v.canonical_blank || 0,
    }))
    .sort((a, b) => b.below_clean_core - a.below_clean_core)
    .slice(0, 15);

  const report = {
    ok: status !== CLEAN_CORE_IDENTITY_REPAIR_STATUS.BLOCKED,
    version: CLEAN_CORE_IDENTITY_REPAIR_VERSION,
    generated_at: new Date().toISOString(),
    status,
    write_target: {
      base: productionHotelPropertyCensus.baseName,
      table: productionHotelPropertyCensus.tableName,
      table_id: productionHotelPropertyCensus.tableId,
    },
    airtable_writes: Boolean(opts.airtable_writes),
    brand_setup_writes: false,
    brand_explorer_writes: false,
    inserts_applied: 0,
    cleanup_existing_only: true,
    paused_queues: [
      "source_discovery_inserts",
      "address_confirmation",
      "coordinate_completion",
      "phone_number_enrichment",
      "rooms_keys",
      "description_extraction",
      "amenities_extraction",
    ],
    queues_executed: opts.queues_executed || [...CLEAN_CORE_IDENTITY_REPAIR_QUEUE_ORDER],
    before: before.counters,
    after: after?.counters || null,
    applied,
    classification: c.by_class,
    top_remaining_source_gaps_by_parent: topGaps,
    examples_before_after: applied.examples || [],
    steward_remaining: (after || before).rows
      .filter((r) =>
        [
          RECORD_CLASS.NEEDS_STEWARD_REVIEW,
          RECORD_CLASS.DUPLICATE_RISK,
          RECORD_CLASS.NEEDS_SOURCE_LOOKUP,
        ].includes(r.record_class)
      )
      .slice(0, 50)
      .map((r) => ({
        record_id: r.record_id,
        class: r.record_class,
        city_class: r.buckets.city.class,
        canonical_status: r.buckets.canonical.status,
        missing: r.clean_core.missing,
        blockers: r.clean_core.blockers,
      })),
    next_recommended_action:
      status === CLEAN_CORE_IDENTITY_REPAIR_STATUS.COMPLETE
        ? "Resume address_confirmation for Clean Core rows, then Mapbox coordinate_completion."
        : "Continue official source lookup for Unknown/descriptor cities and blank Canonical; steward duplicate risks. Keep address/Mapbox paused.",
  };

  if (opts.writeReports !== false) {
    writeCleanCoreIdentityRepairReports(report);
  }

  return report;
}

export function writeCleanCoreIdentityRepairReports(report) {
  const reportsDir = path.join(ROOT, "reports/research-engine-v2");
  const docsDir = path.join(ROOT, "docs/data-intelligence");
  fs.mkdirSync(reportsDir, { recursive: true });
  fs.mkdirSync(docsDir, { recursive: true });

  const jsonPath = path.join(reportsDir, "production-census-clean-core-identity-repair.json");
  const mdPath = path.join(reportsDir, "production-census-clean-core-identity-repair.md");
  const docsPath = path.join(docsDir, "production-census-clean-core-identity-repair.md");
  const md = renderCleanCoreIdentityRepairMarkdown(report);

  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2), "utf8");
  fs.writeFileSync(mdPath, md, "utf8");
  fs.writeFileSync(
    docsPath,
    `# Clean Core Identity Repair

${md}

## Sprint rules

- Cleanup existing Hotel Property Census only — **no new inserts**
- **Paused:** address, Mapbox, phone, rooms, descriptions
- High-confidence City / State / Canonical fixes only
- No weak city inference from hotel name / coordinates / Google / Mapbox
`,
    "utf8"
  );
  return { jsonPath, mdPath, docsPath };
}

function delta(before, after, key) {
  if (!after) return "—";
  const b = before?.[key] ?? 0;
  const a = after?.[key] ?? 0;
  return `${b} → ${a}`;
}

export function renderCleanCoreIdentityRepairMarkdown(report) {
  const b = report.before || {};
  const a = report.after || null;
  const ap = report.applied || {};
  return `# Production Census — Clean Core Identity Repair

**Status:** \`${report.status}\`  
**Generated:** ${report.generated_at}  
**Write target:** ${report.write_target?.base} → ${report.write_target?.table} (\`${report.write_target?.table_id}\`)  
**Airtable writes:** ${report.airtable_writes ? "yes" : "no"}  
**Inserts applied:** ${report.inserts_applied ?? 0}  
**Cleanup existing only:** ${report.cleanup_existing_only ? "yes" : "no"}

## Before → After

| Metric | Count |
|--------|------:|
| Clean Core | ${delta(b, a, "clean_core")} |
| Below Clean Core | ${delta(b, a, "below_clean_core")} |
| Canonical blank | ${delta(b, a, "canonical_blank")} |
| Unknown city | ${delta(b, a, "unknown_city")} |
| Descriptor city | ${delta(b, a, "descriptor_city")} |
| All-caps city | ${delta(b, a, "all_caps_city")} |
| All-lowercase city | ${delta(b, a, "all_lowercase_city")} |
| City/state combined | ${delta(b, a, "city_state_combined")} |
| State / Region complete | ${delta(b, a, "state_region_complete")} |
| Source URL complete | ${delta(b, a, "source_url_complete")} |
| Duplicate risk | ${delta(b, a, "canonical_duplicate_risk")} |
| Human Review Required | ${delta(b, a, "human_review_required")} |
| Coordinate blocked (dirty identity) | ${delta(b, a, "coordinate_blocked_dirty_identity")} |

## Applied

- Records fixed: ${ap.records_fixed ?? 0}
- Fields written: ${(ap.fields_written || []).join(", ") || "—"}

## Classification

${Object.entries(report.classification || {})
  .map(([k, v]) => `- ${k}: ${v}`)
  .join("\n")}

## Top remaining gaps by parent

${(report.top_remaining_source_gaps_by_parent || [])
  .slice(0, 10)
  .map(
    (g) =>
      `- **${g.parent}**: below=${g.below_clean_core}, unknown_city=${g.unknown_city}, canonical_blank=${g.canonical_blank}`
  )
  .join("\n") || "_n/a_"}

## Examples before/after

${(report.examples_before_after || [])
  .slice(0, 15)
  .map((e) => `- \`${e.record_id}\`: ${JSON.stringify(e.before)} → ${JSON.stringify(e.after)}`)
  .join("\n") || "_None_"}

## Next recommended action

${report.next_recommended_action || "—"}

## Paused queues

${(report.paused_queues || []).map((q) => `- ${q}`).join("\n")}
`;
}
