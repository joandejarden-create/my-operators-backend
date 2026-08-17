/**
 * Plan Parent Company cleanup for non-Hilton-brand Hotel Census rows.
 */

import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "./fields.js";
import { getPlatformBase } from "./platform-base.js";
import { exactMatchKey } from "./brand-alias-resolve.js";
import { loadActiveBrandAliasRows } from "./brand-alias-resolve.js";
import {
  loadBrandSetupParentIndex,
  buildAliasParentIndex,
  resolveExpectedParentCompany,
  isHiltonBrandAffiliation,
  parentCompanyNeedsUpdate,
  HILTON_PARENT_NORM,
} from "./census-parent-company-source.js";
import { normalizeParentCompanyKey } from "./brand-alias-resolve.js";

/**
 * @param {object} [opts]
 * @param {boolean} [opts.hiltonMisclassifiedOnly] Only rows with Hilton parent on non-Hilton brands
 */
export async function planCensusParentCompanyCleanup(opts = {}) {
  const base = getPlatformBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const { brandSetupParent, hiltonBrandKeys } = await loadBrandSetupParentIndex();
  const aliasRows = await loadActiveBrandAliasRows();
  const aliasParent = buildAliasParentIndex(aliasRows);
  const indexes = { brandSetupParent, aliasParent };

  const records = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: [
        CENSUS_FIELDS.name,
        CENSUS_FIELDS.affiliation,
        CENSUS_FIELDS.parentCompany,
        CENSUS_FIELDS.country,
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
    const currentParent = exactMatchKey(rec.fields?.[CENSUS_FIELDS.parentCompany]);
    const currentParentNorm = normalizeParentCompanyKey(currentParent);

    if (isHiltonBrandAffiliation(affiliation, hiltonBrandKeys)) {
      skipped.push({
        censusRecordId: rec.id,
        censusName: rec.fields?.[CENSUS_FIELDS.name],
        affiliation,
        reason: "hilton_brand_skip",
        currentParent,
      });
      continue;
    }

    const resolved = resolveExpectedParentCompany(affiliation, indexes);
    if (!resolved) {
      if (opts.hiltonMisclassifiedOnly) {
        skipped.push({
          censusRecordId: rec.id,
          censusName: rec.fields?.[CENSUS_FIELDS.name],
          affiliation,
          reason: "no_expected_parent",
          currentParent,
        });
        continue;
      }
      if (currentParentNorm === HILTON_PARENT_NORM) {
        skipped.push({
          censusRecordId: rec.id,
          censusName: rec.fields?.[CENSUS_FIELDS.name],
          affiliation,
          reason: "hilton_parent_no_mapping",
          currentParent,
        });
      } else {
        skipped.push({
          censusRecordId: rec.id,
          censusName: rec.fields?.[CENSUS_FIELDS.name],
          affiliation,
          reason: "no_expected_parent",
          currentParent,
        });
      }
      continue;
    }

    const { expectedParent, source } = resolved;

    if (!parentCompanyNeedsUpdate(currentParent, expectedParent)) {
      skipped.push({
        censusRecordId: rec.id,
        censusName: rec.fields?.[CENSUS_FIELDS.name],
        affiliation,
        reason: "already_correct",
        currentParent,
        expectedParent,
      });
      continue;
    }

    if (opts.hiltonMisclassifiedOnly && currentParentNorm !== HILTON_PARENT_NORM) {
      skipped.push({
        censusRecordId: rec.id,
        censusName: rec.fields?.[CENSUS_FIELDS.name],
        affiliation,
        reason: "not_hilton_parent",
        currentParent,
        expectedParent,
      });
      continue;
    }

    /** @type {Record<string, string|null>} */
    const applyFields = {
      [CENSUS_FIELDS.parentCompany]: expectedParent || null,
    };

    planRows.push({
      censusRecordId: rec.id,
      censusName: rec.fields?.[CENSUS_FIELDS.name],
      affiliation,
      country: rec.fields?.[CENSUS_FIELDS.country],
      currentParent: currentParent || "",
      expectedParent,
      source,
      applyFields,
    });
  }

  return {
    censusRowsScanned: records.length,
    readyToApply: planRows.length,
    skipped,
    planRows,
    brandSetupBrands: brandSetupParent.size,
    aliasMappings: aliasParent.size,
    hiltonBrandCount: hiltonBrandKeys.size,
  };
}
