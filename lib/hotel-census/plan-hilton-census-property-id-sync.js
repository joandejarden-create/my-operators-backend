/**
 * Plan Property ID backfill for Hilton-parent Hotel Census rows from hilton.com ctyhocn codes.
 */

import { readFileSync, existsSync } from "node:fs";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "./fields.js";
import { getPlatformBase } from "./platform-base.js";
import { getCensusEnrichmentSelectFields } from "./probe-census-enrichment-fields.js";
import { mapCensusRowForDirectoryMatch } from "./match-brand-directory-to-census.js";
import { resolveCensusCtyhocn } from "./plan-hilton-census-descriptions.js";
import { HILTON_MANUAL_PROPERTY_LINKS } from "./hilton-manual-property-links.js";
import {
  CENSUS_PROPERTY_ID_FIELD,
  probeCensusPropertyIdField,
  normalizeCensusPropertyId,
} from "./hilton-property-id-contract.js";

const HILTON_PARENT_FORMULA = `FIND("Hilton", {${CENSUS_FIELDS.parentCompany}})`;

/**
 * @param {string} [planPath]
 * @returns {Map<string, string>} censusRecordId → ctyhocn
 */
export function loadEnrichmentCtyhocnByCensusId(planPath = "reports/hilton-census-enrichment-plan-all-brands.json") {
  /** @type {Map<string, string>} */
  const map = new Map();
  if (!existsSync(planPath)) return map;
  const plan = JSON.parse(readFileSync(planPath, "utf8"));
  for (const row of plan.planRows || []) {
    const id = String(row.censusRecordId || "").trim();
    const code = String(row.directoryBrandPropertyCode || "").trim().toUpperCase();
    if (!id || !code || row.matchConfidence === "none") continue;
    map.set(id, code);
  }
  return map;
}

/**
 * @param {ReturnType<typeof mapCensusRowForDirectoryMatch>} censusRow
 * @param {Map<string, string>} enrichmentByCensusId
 */
export function resolveHiltonPropertyId(censusRow, enrichmentByCensusId) {
  let ctyhocn = resolveCensusCtyhocn(censusRow);
  let source = ctyhocn
    ? censusRow.websiteCtyhocn
      ? "website"
      : "brand_property_code"
    : "";

  if (!ctyhocn) {
    const manual = HILTON_MANUAL_PROPERTY_LINKS.find((m) => m.recordId === censusRow.recordId);
    if (manual?.ctyhocn) {
      ctyhocn = manual.ctyhocn.toUpperCase();
      source = "manual_link";
    }
  }

  if (!ctyhocn && enrichmentByCensusId.has(censusRow.recordId)) {
    ctyhocn = enrichmentByCensusId.get(censusRow.recordId);
    source = "enrichment_plan";
  }

  return { ctyhocn: ctyhocn ? ctyhocn.toUpperCase() : "", source };
}

/**
 * @param {object} [opts]
 * @param {boolean} [opts.fillBlankOnly]
 * @param {string} [opts.enrichmentPlanPath]
 */
export async function planHiltonCensusPropertyIdSync(opts = {}) {
  const base = getPlatformBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const fieldPresent = await probeCensusPropertyIdField(base);
  if (!fieldPresent) {
    throw new Error(`Hotel Census field "${CENSUS_PROPERTY_ID_FIELD}" not found`);
  }

  const enrichmentByCensusId = loadEnrichmentCtyhocnByCensusId(opts.enrichmentPlanPath);

  const selectFields = await getCensusEnrichmentSelectFields(base);
  if (!selectFields.includes(CENSUS_PROPERTY_ID_FIELD)) {
    selectFields.push(CENSUS_PROPERTY_ID_FIELD);
  }

  const records = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: selectFields,
      filterByFormula: HILTON_PARENT_FORMULA,
      pageSize: 100,
    })
    .all();

  /** @type {object[]} */
  const planRows = [];
  /** @type {object[]} */
  const skipped = [];

  for (const rec of records) {
    const censusRow = mapCensusRowForDirectoryMatch(rec);
    const currentRaw = rec.fields?.[CENSUS_PROPERTY_ID_FIELD];
    const current = normalizeCensusPropertyId(currentRaw);
    const { ctyhocn, source } = resolveHiltonPropertyId(censusRow, enrichmentByCensusId);

    if (!ctyhocn) {
      skipped.push({
        censusRecordId: rec.id,
        censusName: censusRow.name,
        currentPropertyId: currentRaw ?? "",
        reason: "no_ctyhocn",
      });
      continue;
    }

    if (current === ctyhocn) {
      skipped.push({
        censusRecordId: rec.id,
        censusName: censusRow.name,
        ctyhocn,
        currentPropertyId: currentRaw ?? "",
        reason: "already_correct",
      });
      continue;
    }

    if (opts.fillBlankOnly && current) {
      skipped.push({
        censusRecordId: rec.id,
        censusName: censusRow.name,
        ctyhocn,
        currentPropertyId: currentRaw ?? "",
        reason: "fill_blank_only_has_value",
      });
      continue;
    }

    planRows.push({
      censusRecordId: rec.id,
      censusName: censusRow.name,
      ctyhocn,
      source,
      currentPropertyId: currentRaw ?? "",
      applyFields: {
        [CENSUS_PROPERTY_ID_FIELD]: ctyhocn,
      },
    });
  }

  return {
    censusRowsScanned: records.length,
    readyToApply: planRows.length,
    skipped,
    planRows,
    propertyIdField: CENSUS_PROPERTY_ID_FIELD,
    enrichmentPlanMatches: enrichmentByCensusId.size,
  };
}
