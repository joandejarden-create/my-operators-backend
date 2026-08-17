/**
 * Plan fill-blank backfill for Hilton census: description, amenities, website, dates.
 */

import { readFileSync, existsSync } from "node:fs";
import { fetchHiltonHotelDescription, pickPrimaryHiltonDescription } from "../hilton-hotel-description-fetch.js";
import { fetchHiltonHotelStatus } from "../hilton-hotel-status-fetch.js";
import { STATUS_OPEN } from "./fields.js";
import { resolveCensusCtyhocn } from "./plan-hilton-census-descriptions.js";
import { mapCensusRowForDirectoryMatch, ctyhocnFromWebsite } from "./match-brand-directory-to-census.js";
import { buildDescriptionEnrichmentFields } from "./hilton-description-enrichment-contract.js";
import {
  buildDirectoryBackfillFields,
  buildFillBlankPatch,
  yearFromDate,
  CENSUS_YEAR_AFFILIATED_FIELD,
  MAP_HILTON_CENSUS_FIELD_BACKFILL,
} from "./hilton-census-field-backfill-contract.js";
import { isBlankCensusValue } from "./brand-directory-enrichment-contract.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "./fields.js";
import { getPlatformBase } from "./platform-base.js";
import { getCensusEnrichmentSelectFields } from "./probe-census-enrichment-fields.js";
import { probeHiltonBackfillFields } from "./hilton-census-field-backfill-contract.js";
import { CENSUS_AMENITIES_TEXT_FIELD } from "../hilton-amenity-map.js";

const HILTON_PARENT_FORMULA = `FIND("Hilton", {${CENSUS_FIELDS.parentCompany}})`;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function hiltonHotelUrl(ctyhocn, website) {
  const w = String(website || "").trim();
  if (w.includes("hilton.com") && w.includes("/hotels/")) return w;
  const code = String(ctyhocn || "").trim().toLowerCase();
  return code ? `https://www.hilton.com/en/hotels/${code}-hotel/` : "";
}

function proposedFromFieldActions(fieldActions, logicalKey) {
  const row = (fieldActions || []).find((a) => a.logicalKey === logicalKey);
  return row?.proposed ?? null;
}

/**
 * @param {string} planPath
 */
export function loadEnrichmentDirectoryIndex(planPath) {
  if (!existsSync(planPath)) return { byCensusId: new Map(), byCtyhocn: new Map() };
  const plan = JSON.parse(readFileSync(planPath, "utf8"));
  /** @type {Map<string, object>} */
  const byCensusId = new Map();
  /** @type {Map<string, object>} */
  const byCtyhocn = new Map();

  for (const row of plan.planRows || []) {
    const code = String(row.directoryBrandPropertyCode || "").trim().toUpperCase();
    if (!code) continue;

    const hints = {
      ctyhocn: code,
      directoryName: row.directoryName,
      website: hiltonHotelUrl(
        code,
        row.applyFields?.Website ||
          proposedFromFieldActions(row.fieldActions, "website") ||
          row.sourceUrl
      ),
      amenitiesText: row.amenitiesTextSuggested || row.applyFields?.Amenities || "",
      amenityFlags: row.amenityFlagsSuggested || {},
      openDate:
        row.applyFields?.["Open Date"] ||
        proposedFromFieldActions(row.fieldActions, "openDate") ||
        null,
      matchConfidence: row.matchConfidence,
    };

    byCtyhocn.set(code, hints);
    if (row.censusRecordId) byCensusId.set(row.censusRecordId, hints);
  }

  return { byCensusId, byCtyhocn };
}

function rowNeedsBackfill(fields, presentWritable) {
  for (const col of presentWritable) {
    if (isBlankCensusValue(fields?.[col])) return true;
  }
  return false;
}

function summarizeBlanks(fields, presentWritable) {
  /** @type {string[]} */
  const blank = [];
  for (const col of presentWritable) {
    if (isBlankCensusValue(fields?.[col])) blank.push(col);
  }
  return blank;
}

/**
 * @param {object} [opts]
 */
export async function planHiltonCensusFieldBackfill(opts = {}) {
  const base = getPlatformBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const { writable: presentWritable, formula: formulaFields } = await probeHiltonBackfillFields(base);
  const selectFields = await getCensusEnrichmentSelectFields(base);
  for (const f of [...presentWritable, CENSUS_YEAR_AFFILIATED_FIELD]) {
    if (!selectFields.includes(f)) selectFields.push(f);
  }

  const records = await base(HOTEL_CENSUS_TABLE)
    .select({ fields: selectFields, filterByFormula: HILTON_PARENT_FORMULA, pageSize: 100 })
    .all();

  const { byCensusId, byCtyhocn } = loadEnrichmentDirectoryIndex(
    opts.enrichmentPlanPath || "reports/hilton-census-enrichment-plan-all-brands.json"
  );

  const planRows = [];
  const skipped = [];
  const blankSummary = {};

  for (let i = 0; i < records.length; i++) {
    const rec = records[i];
    const fields = rec.fields || {};
    if (!rowNeedsBackfill(fields, presentWritable)) continue;

    const censusRow = mapCensusRowForDirectoryMatch(rec);
    let ctyhocn = resolveCensusCtyhocn(censusRow);
    if (!ctyhocn) ctyhocn = byCensusId.get(rec.id)?.ctyhocn || "";

    const dirHints = byCensusId.get(rec.id) || (ctyhocn ? byCtyhocn.get(ctyhocn) : null) || {};
    const website = hiltonHotelUrl(ctyhocn, fields.Website || dirHints.website);

    const applyFields = buildDirectoryBackfillFields(fields, { ...dirHints, website }, presentWritable);

    if (ctyhocn) {
      const needDesc = isBlankCensusValue(fields[MAP_HILTON_CENSUS_FIELD_BACKFILL.hotelDescription]);
      const needOpen = isBlankCensusValue(fields[MAP_HILTON_CENSUS_FIELD_BACKFILL.openDate]);

      if (needDesc || needOpen) {
        if (opts.onProgress) opts.onProgress(`[${i + 1}] ${ctyhocn} — ${censusRow.name}`);
        try {
          if (needDesc) {
            const desc = await fetchHiltonHotelDescription(ctyhocn, { refererUrl: website });
            Object.assign(
              applyFields,
              buildDescriptionEnrichmentFields(fields, desc, {
                fillBlankOnly: true,
                presentFields: presentWritable,
              })
            );
          }
          if (needOpen) {
            const status = await fetchHiltonHotelStatus(ctyhocn, { refererUrl: website });
            if (status.openDate && presentWritable.includes("Open Date")) {
              if (isBlankCensusValue(fields["Open Date"])) {
                applyFields["Open Date"] = status.openDate;
              }
            }
          }
        } catch (err) {
          if (!Object.keys(applyFields).length) {
            skipped.push({
              censusRecordId: rec.id,
              censusName: censusRow.name,
              ctyhocn,
              reason: "graphql_error",
              error: err?.message || String(err),
            });
            if (opts.fetchDelayMs) await sleep(opts.fetchDelayMs);
            continue;
          }
        }
        if (opts.fetchDelayMs) await sleep(opts.fetchDelayMs);
      }
    }

    const openDate = applyFields["Open Date"] || fields["Open Date"];
    if (
      presentWritable.includes(CENSUS_YEAR_AFFILIATED_FIELD) &&
      isBlankCensusValue(fields[CENSUS_YEAR_AFFILIATED_FIELD])
    ) {
      const year = yearFromDate(openDate);
      if (year) applyFields[CENSUS_YEAR_AFFILIATED_FIELD] = year;
    }

    if (!Object.keys(applyFields).length) {
      skipped.push({
        censusRecordId: rec.id,
        censusName: censusRow.name,
        ctyhocn,
        blankFields: summarizeBlanks(fields, presentWritable),
        reason: ctyhocn ? "no_proposed_values" : "no_ctyhocn",
      });
      continue;
    }

    for (const k of Object.keys(applyFields)) {
      blankSummary[k] = (blankSummary[k] || 0) + 1;
    }

    planRows.push({
      censusRecordId: rec.id,
      censusName: censusRow.name,
      ctyhocn,
      blankBefore: summarizeBlanks(fields, presentWritable),
      applyFields,
      status: "ready",
    });
  }

  return {
    presentWritable,
    formulaFields,
    censusRowsScanned: records.length,
    needsBackfill: planRows.length + skipped.length,
    readyToApply: planRows.length,
    skipped,
    fieldFillCounts: blankSummary,
    planRows,
  };
}

/**
 * Audit blank counts across all Hilton rows (no fetch).
 */
export async function auditHiltonCensusFieldBlanks() {
  const base = getPlatformBase();
  const { writable, formula } = await probeHiltonBackfillFields(base);
  const selectFields = [...new Set([...(await getCensusEnrichmentSelectFields(base)), ...writable])];

  const records = await base(HOTEL_CENSUS_TABLE)
    .select({ fields: selectFields, filterByFormula: HILTON_PARENT_FORMULA, pageSize: 100 })
    .all();

  const counts = {};
  for (const col of writable) counts[col] = 0;

  for (const rec of records) {
    const f = rec.fields || {};
    for (const col of writable) {
      if (isBlankCensusValue(f[col])) counts[col]++;
    }
  }

  return { total: records.length, writable, formula, blankCounts: counts };
}
