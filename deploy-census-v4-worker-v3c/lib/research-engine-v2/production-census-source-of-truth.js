/**
 * Production Hotel Property Census — single source of truth for Autopilot.
 *
 * Terminology (binding):
 * - "Hotel Property Census" = production property table (only Autopilot write target)
 * - "VIC source claims" = Verified Independent Census evidence / claim library (read-only)
 * - "legacy Census" = old / deprecated Hotel Census tables (never Autopilot write)
 * - "Brand Setup active control list" = Active/Live brands (read-only for Autopilot)
 *
 * No Brand Explorer / Brand Setup / VIC freeze mutation from Autopilot apply.
 */

import { TABLE_IDS } from "./production-census-write.js";
import { resolveTargetBase } from "./production-census-schema-create.js";

export const PRODUCTION_CENSUS_SOT_VERSION = "production-census-source-of-truth-v1";

/** Canonical production write target for Autopilot. */
export const productionHotelPropertyCensus = Object.freeze({
  baseName: "Deal Capture Platform",
  tableName: "Hotel Property Census",
  tableId: TABLE_IDS["Hotel Property Census"] || "tbl9aY5ijiuIzzWam",
  role: "production_property_table",
  allowedWriteTarget: true,
  envBaseResolver: "AIRTABLE_BASE_ID_ALT (Deal Capture Platform)",
});

/** Expected table ID — fail closed if mismatched. */
export const PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID = "tbl9aY5ijiuIzzWam";

/** Supporting production tables (read / explicit queue-approved only — not default Autopilot write). */
export const productionCensusSupportingTables = Object.freeze([
  Object.freeze({
    tableName: "Hotel Property Brand Affiliations",
    tableId: TABLE_IDS["Hotel Property Brand Affiliations"],
    role: "supporting_affiliations",
    allowedWriteTarget: false,
    autopilotDefault: "read_or_explicit_queue_approved",
  }),
  Object.freeze({
    tableName: "Hotel Property Source Evidence",
    tableId: TABLE_IDS["Hotel Property Source Evidence"],
    role: "supporting_source_evidence",
    allowedWriteTarget: false,
    autopilotDefault: "read_or_explicit_queue_approved",
  }),
  Object.freeze({
    tableName: "Hotel Property Steward Review",
    tableId: TABLE_IDS["Hotel Property Steward Review"],
    role: "supporting_steward_review",
    allowedWriteTarget: false,
    autopilotDefault: "read_or_explicit_queue_approved",
  }),
]);

/**
 * Explicitly blocked as Autopilot write targets.
 * Reads may still be allowed where noted (VIC claims, Brand Setup control list).
 */
export const BLOCKED_AUTOPILOT_WRITE_TARGETS = Object.freeze([
  Object.freeze({
    key: "legacy_hotel_census",
    labels: ["Hotel Census", "legacy Hotel Census", "legacy Census", "old Census"],
    role: "legacy_deprecated",
    allowedWriteTarget: false,
    allowedRead: false,
    note: "Not active production; migration/audit mode only",
  }),
  Object.freeze({
    key: "vic_source_claims",
    labels: [
      "Verified Independent Census",
      "Verified Independent Hotel Census",
      "VIC",
      "VIC staging",
      "verified-independent-census",
    ],
    role: "evidence_claim_library",
    allowedWriteTarget: false,
    allowedRead: true,
    note: "VIC source claims — evidence lineage only; never production write",
  }),
  Object.freeze({
    key: "candidates_evidence_staging",
    labels: [
      "Independent Hotel Source Candidates",
      "Independent Hotel Source Evidence",
      "Candidates",
      "Evidence staging",
    ],
    role: "staging",
    allowedWriteTarget: false,
    allowedRead: false,
    note: "Staging / candidates — not Autopilot write target",
  }),
  Object.freeze({
    key: "brand_setup",
    labels: ["Brand Setup", "Brand Setup - Brand Basics", "Brand Setup active control list"],
    role: "brand_control_layer",
    allowedWriteTarget: false,
    allowedRead: true,
    note: "Active/Live brand control list — read-only for Autopilot",
  }),
  Object.freeze({
    key: "brand_explorer",
    labels: [
      "Brand Explorer",
      "Brand Setup - Brand Explorer Presentation",
      "Brand Presentation",
      "presentation",
    ],
    role: "presentation_layer",
    allowedWriteTarget: false,
    allowedRead: false,
    note: "Brand Explorer / presentation — never Autopilot write",
  }),
  Object.freeze({
    key: "company_validated_brand_status",
    labels: ["Company Validated", "Brand Verified", "Brand Status"],
    role: "protected_brand_governance",
    allowedWriteTarget: false,
    allowedRead: false,
    note: "Protected brand governance fields/tables — blocked",
  }),
]);

export const AUTOPILOT_READ_RULES = Object.freeze({
  brand_setup_active_control_list: "read_only",
  hotel_property_census: "read_and_write_allowlisted_fields",
  vic_source_claims: "read_evidence_lineage_only",
  supporting_census_evidence_tables: "read_or_explicit_queue_approved_write",
});

export const AUTOPILOT_WRITE_RULES = Object.freeze({
  allowed: ["Hotel Property Census"],
  forbidden: BLOCKED_AUTOPILOT_WRITE_TARGETS.map((t) => t.key),
});

export const PRECISE_MATCH_SUMMARY_LINE =
  "Matched Active / Live Brand Setup brands to production Hotel Property Census records.";

export const AMBIGUOUS_MATCH_PHRASES = Object.freeze([
  "Matched brands to Census",
  "matched to Census",
  "match to Census",
  "brands to Census",
]);

export const BLOCKED_WRONG_CENSUS_TARGET = "blocked_wrong_census_target";

/**
 * @returns {typeof productionHotelPropertyCensus}
 */
export function getProductionHotelPropertyCensus() {
  return productionHotelPropertyCensus;
}

/**
 * Normalize a table label for blocked-target matching.
 * @param {string} name
 */
function normLabel(name) {
  return String(name || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

/**
 * Detect if a proposed write target is an explicitly blocked table/label.
 * @param {{ tableName?: string, tableId?: string, table?: string }} target
 */
export function classifyBlockedWriteTarget(target = {}) {
  const tableName = target.tableName || target.table || "";
  const tableId = target.tableId || target.table_id || "";
  const n = normLabel(tableName);

  if (
    tableId === productionHotelPropertyCensus.tableId ||
    n === normLabel(productionHotelPropertyCensus.tableName)
  ) {
    return null;
  }

  for (const blocked of BLOCKED_AUTOPILOT_WRITE_TARGETS) {
    for (const label of blocked.labels) {
      const ln = normLabel(label);
      if (n === ln || (ln.length >= 4 && n.includes(ln))) {
        return blocked;
      }
    }
  }

  // Known supporting table IDs — not default Autopilot write
  for (const s of productionCensusSupportingTables) {
    if (tableId && tableId === s.tableId) return { ...s, key: "supporting_not_default_write" };
    if (n === normLabel(s.tableName)) return { ...s, key: "supporting_not_default_write" };
  }

  return null;
}

/**
 * Fail-closed validation of Autopilot production write target.
 * @param {{
 *   baseName?: string,
 *   baseId?: string,
 *   tableName?: string,
 *   table?: string,
 *   tableId?: string,
 *   table_id?: string,
 *   fields?: string[],
 *   allowGeocode?: boolean,
 * }} target
 * @param {{ requireLiveBaseId?: boolean, resolveBase?: () => object }} [opts]
 */
export function assertProductionCensusWriteTarget(target = {}, opts = {}) {
  const errors = [];
  const tableName = target.tableName || target.table || "";
  const tableId = target.tableId || target.table_id || "";
  const baseName = target.baseName || target.base || "";

  const expected = productionHotelPropertyCensus;

  if (tableName && normLabel(tableName) !== normLabel(expected.tableName)) {
    errors.push(`wrong_table_name:${tableName}`);
  }
  if (!tableName && !tableId) {
    errors.push("missing_table_identity");
  }
  if (tableId && tableId !== expected.tableId) {
    errors.push(`wrong_table_id:${tableId}`);
  }
  if (expected.tableId !== PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID) {
    errors.push(`sot_table_id_drift:${expected.tableId}`);
  }
  if (baseName && normLabel(baseName) !== normLabel(expected.baseName)) {
    // Allow env-resolved platform without exact string if baseId matches platform
    if (!target.baseId) errors.push(`wrong_base_name:${baseName}`);
  }

  const blocked = classifyBlockedWriteTarget({ tableName, tableId });
  if (blocked) {
    errors.push(`blocked_write_target:${blocked.key || blocked.role}`);
  }

  // Ambiguous bare "Census" / "Hotel Census" without Hotel Property
  if (
    tableName &&
    /\bcensus\b/i.test(tableName) &&
    normLabel(tableName) !== normLabel(expected.tableName)
  ) {
    errors.push(`ambiguous_census_target:${tableName}`);
  }

  let baseResolution = null;
  if (opts.requireLiveBaseId || target.baseId != null) {
    const resolve = opts.resolveBase || resolveTargetBase;
    baseResolution = resolve();
    if (!baseResolution?.target_base_id) {
      errors.push("missing_platform_base_id");
    }
    if (target.baseId && baseResolution?.target_base_id && target.baseId !== baseResolution.target_base_id) {
      errors.push("base_id_mismatch_vs_platform");
    }
    if (baseResolution?.mvp_base_id && target.baseId === baseResolution.mvp_base_id) {
      errors.push("write_target_is_mvp_base_not_platform");
    }
  }

  const ok =
    errors.length === 0 &&
    (tableId === expected.tableId ||
      (!tableId && normLabel(tableName) === normLabel(expected.tableName)));

  return {
    ok,
    status: ok ? "production_census_write_target_ok" : BLOCKED_WRONG_CENSUS_TARGET,
    code: ok ? null : BLOCKED_WRONG_CENSUS_TARGET,
    errors,
    expected: {
      baseName: expected.baseName,
      tableName: expected.tableName,
      tableId: expected.tableId,
      role: expected.role,
    },
    actual: {
      baseName: baseName || null,
      baseId: target.baseId || null,
      tableName: tableName || null,
      tableId: tableId || null,
    },
    base_resolution: baseResolution
      ? {
          target_role: baseResolution.target_role,
          has_platform: Boolean(baseResolution.platform_base_id),
        }
      : null,
    blocked_classification: blocked,
  };
}

/**
 * Whether a write to tableId/tableName is allowed for Autopilot apply.
 * @param {{ tableId?: string, tableName?: string, table?: string }} target
 */
export function isAllowedAutopilotWriteTarget(target = {}) {
  return assertProductionCensusWriteTarget(target).ok;
}

/**
 * Snapshot for docs / audits / run folders.
 */
export function getProductionCensusSourceOfTruthSnapshot() {
  return {
    version: PRODUCTION_CENSUS_SOT_VERSION,
    productionHotelPropertyCensus,
    productionCensusSupportingTables,
    blockedWriteTargets: BLOCKED_AUTOPILOT_WRITE_TARGETS,
    readRules: AUTOPILOT_READ_RULES,
    writeRules: AUTOPILOT_WRITE_RULES,
    preciseMatchSummaryLine: PRECISE_MATCH_SUMMARY_LINE,
    ambiguousMatchPhrases: AMBIGUOUS_MATCH_PHRASES,
    failClosedCode: BLOCKED_WRONG_CENSUS_TARGET,
  };
}

/**
 * Guard run-summary / match-report copy for vague “Census” phrasing.
 * @param {string} text
 */
export function usesPreciseCensusTerminology(text) {
  const s = String(text || "");
  for (const phrase of AMBIGUOUS_MATCH_PHRASES) {
    if (s.toLowerCase().includes(phrase.toLowerCase())) return false;
  }
  return true;
}

/**
 * Preferred match headline for Autopilot summaries.
 */
export function formatBrandToHotelPropertyCensusMatchLine(stats = {}) {
  const matched = stats.matched ?? stats.census_records_matched ?? null;
  const brands = stats.active_brands ?? stats.active_brands_in_scope ?? null;
  const parts = [PRECISE_MATCH_SUMMARY_LINE];
  if (matched != null) parts.push(`Matched records: ${matched}.`);
  if (brands != null) parts.push(`Active / Live brands in scope: ${brands}.`);
  return parts.join(" ");
}
