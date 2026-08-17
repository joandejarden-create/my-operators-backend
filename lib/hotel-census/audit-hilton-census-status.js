/**
 * Audit Hilton Hotel Census status vs hilton.com GraphQL display.open.
 */

import { readFileSync, existsSync } from "node:fs";
import { fetchHiltonHotelStatus } from "../hilton-hotel-status-fetch.js";
import {
  resolveCensusCtyhocn,
} from "./plan-hilton-census-descriptions.js";
import { mapCensusRowForDirectoryMatch } from "./match-brand-directory-to-census.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS, STATUS_OPEN, STATUS_PIPELINE } from "./fields.js";
import { getPlatformBase } from "./platform-base.js";
import { getCensusEnrichmentSelectFields } from "./probe-census-enrichment-fields.js";

const HILTON_PARENT_FORMULA = `FIND("Hilton", {${CENSUS_FIELDS.parentCompany}})`;

export function normalizeCensusStatus(raw) {
  const val = Array.isArray(raw) ? raw[0] : raw;
  const s = String(val || "").trim();
  if (/^open$/i.test(s)) return STATUS_OPEN;
  if (/^pipeline$/i.test(s)) return STATUS_PIPELINE;
  return s || "";
}

function loadEnrichmentCtyhocnMap(planPath) {
  if (!planPath || !existsSync(planPath)) return new Map();
  const plan = JSON.parse(readFileSync(planPath, "utf8"));
  const map = new Map();
  for (const row of plan.planRows || []) {
    const id = String(row.censusRecordId || "").trim();
    const code = String(row.directoryBrandPropertyCode || "").trim().toUpperCase();
    if (!id || !code) continue;
    if (row.matchConfidence === "none") continue;
    map.set(id, code);
  }
  return map;
}

/**
 * @param {object} [opts]
 * @param {string} [opts.enrichmentPlanPath]
 * @param {number} [opts.fetchDelayMs]
 * @param {(msg: string) => void} [opts.onProgress]
 */
export async function auditHiltonCensusStatus(opts = {}) {
  const base = getPlatformBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const enrichmentCtyhocnByCensusId = loadEnrichmentCtyhocnMap(
    opts.enrichmentPlanPath || "reports/hilton-census-enrichment-plan-all-brands.json"
  );

  const selectFields = await getCensusEnrichmentSelectFields(base);
  for (const extra of ["Open Date", "projected_open_date"]) {
    try {
      await base(HOTEL_CENSUS_TABLE).select({ fields: [extra], maxRecords: 1 }).firstPage();
      if (!selectFields.includes(extra)) selectFields.push(extra);
    } catch {
      // optional field
    }
  }

  const records = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: selectFields,
      filterByFormula: HILTON_PARENT_FORMULA,
      pageSize: 100,
    })
    .all();

  const censusRows = records.map(mapCensusRowForDirectoryMatch);
  const auditRows = [];
  const fetchErrors = [];

  for (let i = 0; i < censusRows.length; i++) {
    const row = censusRows[i];
    const censusStatus = normalizeCensusStatus(row.fields?.[CENSUS_FIELDS.status]);
    let ctyhocn = resolveCensusCtyhocn(row);
    let codeSource = ctyhocn ? (row.websiteCtyhocn ? "website" : "brand_property_code") : "";

    if (!ctyhocn && enrichmentCtyhocnByCensusId.has(row.recordId)) {
      ctyhocn = enrichmentCtyhocnByCensusId.get(row.recordId);
      codeSource = "enrichment_plan";
    }

    if (!ctyhocn) {
      auditRows.push({
        censusRecordId: row.recordId,
        censusName: row.name,
        city: row.city,
        country: row.country,
        affiliation: row.affiliation,
        censusStatus,
        hiltonStatus: null,
        verdict: "unverified_no_code",
        codeSource: "",
        ctyhocn: "",
      });
      continue;
    }

    if (opts.onProgress) {
      opts.onProgress(`[${i + 1}/${censusRows.length}] ${ctyhocn} — ${row.name}`);
    }

    try {
      const hilton = await fetchHiltonHotelStatus(ctyhocn, { refererUrl: row.website || undefined });
      let verdict = "match";
      if (censusStatus === STATUS_OPEN && hilton.hiltonStatus === STATUS_PIPELINE) {
        verdict = "mismatch_census_open_hilton_pipeline";
      } else if (censusStatus === STATUS_PIPELINE && hilton.hiltonStatus === STATUS_OPEN) {
        verdict = "mismatch_census_pipeline_hilton_open";
      } else if (censusStatus && censusStatus !== hilton.hiltonStatus) {
        verdict = "mismatch_other";
      } else if (!censusStatus) {
        verdict = "census_status_blank";
      }

      auditRows.push({
        censusRecordId: row.recordId,
        censusName: row.name,
        hiltonName: hilton.name,
        city: row.city,
        country: row.country,
        affiliation: row.affiliation,
        ctyhocn,
        codeSource,
        censusStatus,
        hiltonStatus: hilton.hiltonStatus,
        hiltonOpen: hilton.hiltonOpen,
        hiltonOpenDate: hilton.openDate,
        censusOpenDate: row.fields?.["Open Date"] || null,
        censusProjectedOpenDate: row.fields?.projected_open_date || null,
        verdict,
        suggestedStatus: hilton.hiltonStatus,
      });
    } catch (err) {
      fetchErrors.push({
        censusRecordId: row.recordId,
        censusName: row.name,
        ctyhocn,
        error: err?.message || String(err),
      });
      auditRows.push({
        censusRecordId: row.recordId,
        censusName: row.name,
        city: row.city,
        country: row.country,
        affiliation: row.affiliation,
        ctyhocn,
        codeSource,
        censusStatus,
        hiltonStatus: null,
        verdict: "unverified_graphql_error",
        error: err?.message || String(err),
      });
    }

    if (opts.fetchDelayMs > 0 && i < censusRows.length - 1) await sleep(opts.fetchDelayMs);
  }

  const summary = {
    total: auditRows.length,
    match: auditRows.filter((r) => r.verdict === "match").length,
    censusStatusBlank: auditRows.filter((r) => r.verdict === "census_status_blank").length,
    censusPipelineHiltonOpen: auditRows.filter((r) => r.verdict === "mismatch_census_pipeline_hilton_open")
      .length,
    censusOpenHiltonPipeline: auditRows.filter((r) => r.verdict === "mismatch_census_open_hilton_pipeline")
      .length,
    unverifiedNoCode: auditRows.filter((r) => r.verdict === "unverified_no_code").length,
    unverifiedGraphqlError: auditRows.filter((r) => r.verdict === "unverified_graphql_error").length,
    fetchErrors: fetchErrors.length,
  };

  return { summary, auditRows, fetchErrors };
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * Build Airtable update fields for status correction.
 * @param {object} auditRow
 */
export function buildStatusCorrectionFields(auditRow) {
  const fields = {};
  if (!auditRow.suggestedStatus) return fields;

  if (auditRow.suggestedStatus === STATUS_OPEN) {
    fields[CENSUS_FIELDS.status] = [STATUS_OPEN];
    if (auditRow.hiltonOpenDate && !auditRow.censusOpenDate) {
      fields["Open Date"] = auditRow.hiltonOpenDate;
    }
  } else if (auditRow.suggestedStatus === STATUS_PIPELINE) {
    fields[CENSUS_FIELDS.status] = [STATUS_PIPELINE];
    if (auditRow.hiltonOpenDate && !auditRow.censusProjectedOpenDate) {
      fields.projected_open_date = auditRow.hiltonOpenDate;
    }
  }
  return fields;
}
