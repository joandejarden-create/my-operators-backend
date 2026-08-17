/**
 * Brand URL governance — source priority for Discoverability (Phase 3C.1).
 * Does not write Brand Basics. Audits governed field availability.
 */

export const BRAND_URL_GOVERNANCE_VERSION = "ai_visibility_brand_url_governance_v1";

/** Known Brand Setup - Brand Basics URL fields (from phase3a8 catalog). */
export const BRAND_BASICS_URL_FIELDS = Object.freeze({
  brandWebsite: {
    airtableField: "Brand Website",
    type: "url",
    purpose: "primary_brand_site",
    sourceTier: "company_validated",
  },
  brandedResidencesSourceUrl: {
    airtableField: "Branded Residences Source URL",
    type: "url",
    purpose: "residences_reference",
    sourceTier: "company_validated",
  },
});

export const URL_SOURCE_PRIORITY = Object.freeze([
  "company_validated",
  "governed_brand_data",
  "discovered_candidate_requires_review",
]);

export const CURRENT_URL_SOURCES = Object.freeze({
  brandWebsite: "Brand Setup - Brand Basics · Brand Website",
  brandedResidencesSourceUrl: "Brand Setup - Brand Basics · Branded Residences Source URL",
  companyDomain: "derived_from_brand_website",
  developmentUrl: "NOT_IN_BRAND_BASICS_CATALOG",
  franchiseUrl: "NOT_IN_BRAND_BASICS_CATALOG",
  regionalDomain: "NOT_IN_BRAND_BASICS_CATALOG",
});

export const URL_GOVERNANCE_GAPS = Object.freeze([
  "No dedicated Brand Development URL field in Brand Basics catalog",
  "No dedicated Franchise/Development URL field in Brand Basics catalog",
  "No regional domain field in Brand Basics catalog",
  "Development page URLs may require derivation from Brand Website or manual priority-page config",
]);

export const PROPOSED_FIELDS_IF_REQUIRED = Object.freeze([
  {
    field: "Brand Development URL",
    purpose: "Primary owner/developer development landing page",
    writeAllowed: false,
    note: "Proposed only — do not write without explicit founder approval",
  },
  {
    field: "Franchise Development URL",
    purpose: "Franchise-specific development page when distinct from brand site",
    writeAllowed: false,
    note: "Proposed only",
  },
]);

/**
 * Resolve governed URL for a brand row using priority order.
 * @param {object} brandRow — { brandWebsite, brandedResidencesSourceUrl, ... }
 * @param {object} [opts]
 */
export function resolveGovernedBrandUrl(brandRow = {}, opts = {}) {
  const purpose = opts.purpose || "primary";
  const sources = [];

  if (purpose === "residences") {
    if (
      brandRow.brandedResidencesSourceUrl ||
      brandRow["Branded Residences Source URL"]
    ) {
      const url =
        brandRow.brandedResidencesSourceUrl ||
        brandRow["Branded Residences Source URL"];
      sources.push({
        url,
        field: "Branded Residences Source URL",
        tier: "company_validated",
      });
    }
  } else {
    if (brandRow.brandWebsite || brandRow["Brand Website"]) {
      const url = brandRow.brandWebsite || brandRow["Brand Website"];
      sources.push({ url, field: "Brand Website", tier: "company_validated" });
    }
  }

  if (!sources.length) {
    return {
      ok: false,
      url: null,
      domain: null,
      sourceTier: null,
      dataState: "CONNECTION_REQUIRED",
      gap: "no_governed_url",
    };
  }

  const primary = sources[0];
  let domain = null;
  try {
    domain = new URL(primary.url).hostname.replace(/^www\./, "").toLowerCase();
  } catch {
    domain = null;
  }

  return {
    ok: Boolean(primary.url),
    url: primary.url,
    domain,
    sourceField: primary.field,
    sourceTier: primary.tier,
    sources,
    dataState: primary.url ? "MEASURABLE_PUBLICLY" : "CONNECTION_REQUIRED",
  };
}

/**
 * Normalize brand row from Brand Basics field names.
 */
export function normalizeBrandBasicsUrlRow(row = {}) {
  return {
    brandId: row.brandId || row.id || row["Record_ID"] || null,
    brandName: row.brandName || row["Brand Name"] || null,
    brandWebsite: row.brandWebsite || row["Brand Website"] || null,
    brandedResidencesSourceUrl:
      row.brandedResidencesSourceUrl || row["Branded Residences Source URL"] || null,
    parentCompany: row.parentCompany || row["Parent Company"] || null,
  };
}
