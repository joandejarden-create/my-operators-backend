/**
 * Plan Hotel Service Model backfill for branded Hotel Census rows mapped to Brand Setup.
 * Fill-blank only — inherits Brand Setup - Brand Basics "Hotel Service Model".
 */

import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS, CENSUS_INDEPENDENT_AFFILIATION } from "./fields.js";
import { getPlatformBase } from "./platform-base.js";
import { exactMatchKey, loadActiveBrandAliasRows } from "./brand-alias-resolve.js";
import {
  buildAffiliationToCanonicalIndex,
  loadBrandSetupServiceModelIndex,
  normalizeServiceModelForCensus,
  resolveBrandSetupServiceModel,
  serviceModelNeedsUpdate,
} from "./census-brand-service-model-source.js";

/**
 * @param {object} [opts]
 * @param {boolean} [opts.fillBlankOnly] Default true — never overwrite existing census value
 */
export async function planCensusBrandServiceModelBackfill(opts = {}) {
  const fillBlankOnly = opts.fillBlankOnly !== false;
  const base = getPlatformBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const { index: brandSetupServiceModel, invalidBrandValues, brandSetupRows } =
    await loadBrandSetupServiceModelIndex();
  const aliasRows = await loadActiveBrandAliasRows();
  const affiliationToCanonical = buildAffiliationToCanonicalIndex(aliasRows);

  const records = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: [
        CENSUS_FIELDS.name,
        CENSUS_FIELDS.affiliation,
        CENSUS_FIELDS.parentCompany,
        CENSUS_FIELDS.status,
        CENSUS_FIELDS.hotelServiceModel,
      ],
      pageSize: 100,
    })
    .all();

  /** @type {object[]} */
  const planRows = [];
  /** @type {object[]} */
  const skipped = [];

  for (const rec of records) {
    const affiliation = exactMatchKey(rec.fields?.[CENSUS_FIELDS.affiliation]);
    const currentRaw = rec.fields?.[CENSUS_FIELDS.hotelServiceModel];
    const currentServiceModel = normalizeServiceModelForCensus(currentRaw) || valueToStr(currentRaw);

    if (!affiliation) {
      skipped.push({
        censusRecordId: rec.id,
        censusName: rec.fields?.[CENSUS_FIELDS.name],
        affiliation: "",
        reason: "missing_affiliation",
        currentServiceModel,
      });
      continue;
    }

    if (affiliation === CENSUS_INDEPENDENT_AFFILIATION) {
      skipped.push({
        censusRecordId: rec.id,
        censusName: rec.fields?.[CENSUS_FIELDS.name],
        affiliation,
        reason: "independent",
        currentServiceModel,
      });
      continue;
    }

    const resolved = resolveBrandSetupServiceModel(
      affiliation,
      affiliationToCanonical,
      brandSetupServiceModel
    );

    if (!resolved) {
      skipped.push({
        censusRecordId: rec.id,
        censusName: rec.fields?.[CENSUS_FIELDS.name],
        affiliation,
        reason: "not_brand_setup_mapped",
        currentServiceModel,
      });
      continue;
    }

    const { canonicalBrand, serviceModel, matchSource } = resolved;

    if (currentServiceModel && !fillBlankOnly) {
      if (!serviceModelNeedsUpdate(currentServiceModel, serviceModel)) {
        skipped.push({
          censusRecordId: rec.id,
          censusName: rec.fields?.[CENSUS_FIELDS.name],
          affiliation,
          canonicalBrand,
          reason: "already_correct",
          currentServiceModel,
          expectedServiceModel: serviceModel,
        });
        continue;
      }
    } else if (currentServiceModel) {
      if (!serviceModelNeedsUpdate(currentServiceModel, serviceModel)) {
        skipped.push({
          censusRecordId: rec.id,
          censusName: rec.fields?.[CENSUS_FIELDS.name],
          affiliation,
          canonicalBrand,
          reason: "already_correct",
          currentServiceModel,
          expectedServiceModel: serviceModel,
        });
        continue;
      }
      skipped.push({
        censusRecordId: rec.id,
        censusName: rec.fields?.[CENSUS_FIELDS.name],
        affiliation,
        canonicalBrand,
        reason: "census_value_conflict",
        currentServiceModel,
        expectedServiceModel: serviceModel,
        matchSource,
      });
      continue;
    }

    /** @type {Record<string, string>} */
    const applyFields = {
      [CENSUS_FIELDS.hotelServiceModel]: serviceModel,
    };

    planRows.push({
      censusRecordId: rec.id,
      censusName: rec.fields?.[CENSUS_FIELDS.name],
      affiliation,
      canonicalBrand,
      status: rec.fields?.[CENSUS_FIELDS.status] || "",
      parentCompany: rec.fields?.[CENSUS_FIELDS.parentCompany] || "",
      currentServiceModel: currentServiceModel || "",
      expectedServiceModel: serviceModel,
      matchSource,
      applyFields,
    });
  }

  return {
    censusRowsScanned: records.length,
    readyToApply: planRows.length,
    skipped,
    planRows,
    brandSetupRowsScanned: brandSetupRows,
    brandSetupMappedBrands: brandSetupServiceModel.size,
    aliasMappings: affiliationToCanonical.size,
    invalidBrandSetupValues: [...invalidBrandValues.entries()].map(([brand, raw]) => ({
      brand,
      raw,
    })),
    fillBlankOnly,
  };
}

function valueToStr(v) {
  if (v == null) return "";
  return String(v).trim();
}
