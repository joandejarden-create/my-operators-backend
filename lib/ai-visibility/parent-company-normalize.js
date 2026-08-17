/**
 * Canonical parent-company labels for AI Visibility methodology.
 * Airtable "Parent Company" remains operational SSOT; this normalizes display aliases only.
 * Does not rewrite Brand Basics.
 */

export const PARENT_COMPANY_NORMALIZE_VERSION = "ai_visibility_parent_company_normalize_v1";

/** @type {Record<string, string>} */
export const CANONICAL_PARENT_COMPANIES = Object.freeze({
  marriott: "Marriott International",
  hilton: "Hilton",
  choice: "Choice Hotels",
  ihg: "IHG",
  accor: "Accor",
});

/**
 * @param {unknown} raw
 * @returns {{
 *   observed: string|null,
 *   canonical: string|null,
 *   family: string|null,
 *   normalizationRequired: boolean,
 * }}
 */
export function normalizeParentCompany(raw) {
  if (raw == null || String(raw).trim() === "") {
    return { observed: null, canonical: null, family: null, normalizationRequired: false };
  }
  const observed = String(raw).trim();
  const key = observed.toLowerCase();

  if (key.startsWith("marriott") || key.includes("marriott international")) {
    const canonical = CANONICAL_PARENT_COMPANIES.marriott;
    return {
      observed,
      canonical,
      family: "marriott",
      normalizationRequired: observed !== canonical,
    };
  }
  if (key.startsWith("hilton") || key.includes("hilton worldwide")) {
    const canonical = CANONICAL_PARENT_COMPANIES.hilton;
    return {
      observed,
      canonical,
      family: "hilton",
      normalizationRequired: observed !== canonical,
    };
  }
  if (key.includes("choice")) {
    const canonical = CANONICAL_PARENT_COMPANIES.choice;
    return {
      observed,
      canonical,
      family: "choice",
      normalizationRequired: observed !== canonical,
    };
  }
  if (
    key.includes("ihg") ||
    key.includes("intercontinental hotels") ||
    key.includes("intercontinental hotel")
  ) {
    const canonical = CANONICAL_PARENT_COMPANIES.ihg;
    return {
      observed,
      canonical,
      family: "ihg",
      normalizationRequired: observed !== canonical,
    };
  }
  if (key.includes("accor")) {
    const canonical = CANONICAL_PARENT_COMPANIES.accor;
    return {
      observed,
      canonical,
      family: "accor",
      normalizationRequired: observed !== canonical,
    };
  }

  return {
    observed,
    canonical: observed,
    family: null,
    normalizationRequired: false,
  };
}

/**
 * Do two parent labels refer to the same canonical family?
 */
export function parentsMatchCanonical(a, b) {
  const na = normalizeParentCompany(a);
  const nb = normalizeParentCompany(b);
  if (!na.canonical || !nb.canonical) return false;
  return na.canonical === nb.canonical;
}
