/**
 * Build footprint.openings presentation rows for a CHI brand (Airtable name).
 */
import { sanitizeExternalCopy } from "./external-owner-voice.mjs";
import { resolveProfileForAirtableName } from "./choice-chi-brand-resolve.mjs";
import { buildCalaOpeningsForProfile } from "./choice-cala-openings-from-census.mjs";

/**
 * @param {string} airtableBrandName
 */
export function buildCalaFootprintOpeningRows(airtableBrandName) {
  const profile = resolveProfileForAirtableName(airtableBrandName);
  const openings = buildCalaOpeningsForProfile(profile.name);
  return openings.map((o) => {
    const row = {
      slotKey: "footprint.openings",
      title: sanitizeExternalCopy(o.title),
      body: sanitizeExternalCopy(o.body),
      sort: o.sort ?? 0,
      caseSummaryOverview: sanitizeExternalCopy(o.caseSummaryOverview),
      caseSummaryOwnerObjective: sanitizeExternalCopy(o.caseSummaryOwnerObjective),
      caseSummaryBrandRelevance: sanitizeExternalCopy(o.caseSummaryBrandRelevance),
      caseSummaryInterpretation: sanitizeExternalCopy(o.caseSummaryInterpretation),
      caseSummaryTags: sanitizeExternalCopy(o.caseSummaryTags),
    };
    const imageUrl = String(o.imageUrl ?? "").trim();
    if (imageUrl) row.imageUrl = imageUrl;
    return row;
  });
}
