/**
 * CALA-first property example selection and labeling rules (v35E+).
 *
 * Titles follow Ascend openings template: `{Property} {Brand} — {City}`
 * (not `— CALA Property Example`). See openings-property-card-contract.
 *
 * Property examples prioritize CALA, then U.S., then global only when fewer
 * than 3 examples exist at the prior tier.
 */
import {
  buildOpeningsPropertyCardTitle,
  openingsTitleLooksLikeLegacyPropertyExample,
} from "./brand-explorer-openings-property-card-contract.js";

export const CALA_PROPERTY_RULES_VERSION = "v35E-ascend-title";

export const CALA_SECTION_LABEL_DEFAULT =
  "Curated CALA examples · Not a full directory";

export const US_SECTION_LABEL_DEFAULT =
  "Curated U.S. examples · Not a full directory";

function nz(v) {
  return v == null ? "" : String(v).trim();
}

export function isCalaPropertyCatalogEntry(entry) {
  const hay = [entry?.geographyLabel, entry?.meta, entry?.chips, entry?.scenario]
    .filter(Boolean)
    .join(" ");
  return /\bcala\b/i.test(hay);
}

export function isUsPropertyCatalogEntry(entry) {
  const hay = [entry?.geographyLabel, entry?.meta, entry?.chips, entry?.stateRegion]
    .filter(Boolean)
    .join(" ");
  if (isCalaPropertyCatalogEntry(entry)) return false;
  return /\bu\.s\.|united states|usa\b/i.test(hay);
}

export function countCalaPropertyExamples(propertyCatalog = []) {
  return (propertyCatalog || []).filter(isCalaPropertyCatalogEntry).length;
}

/**
 * Select up to `minimum` property examples with CALA → U.S. → global fallback.
 */
export function selectPropertyExamplesWithGeographicFallback(
  propertyCatalog = [],
  { minimum = 3 } = {}
) {
  const cala = (propertyCatalog || []).filter(isCalaPropertyCatalogEntry);
  if (cala.length >= minimum) {
    return {
      selected: cala.slice(0, minimum),
      tierUsed: "cala",
      calaCount: cala.length,
      usFallbackUsed: false,
      globalFallbackUsed: false,
    };
  }

  const us = (propertyCatalog || []).filter(isUsPropertyCatalogEntry);
  const calaAndUs = [...cala, ...us];
  if (calaAndUs.length >= minimum) {
    return {
      selected: calaAndUs.slice(0, minimum),
      tierUsed: cala.length > 0 ? "cala_partial_us" : "us",
      calaCount: cala.length,
      usFallbackUsed: true,
      globalFallbackUsed: false,
    };
  }

  return {
    selected: (propertyCatalog || []).slice(0, minimum),
    tierUsed: "global",
    calaCount: cala.length,
    usFallbackUsed: calaAndUs.length > cala.length,
    globalFallbackUsed: true,
  };
}

export function shouldBlockUsFallbackForBrand(brandConfig, propertyCatalog = []) {
  const rule = nz(brandConfig?.geographicFallbackRule);
  const calaCount = countCalaPropertyExamples(propertyCatalog);
  const minimum = brandConfig?.propertyExampleMinimum || 3;
  if (rule.includes("cala_first") && calaCount >= minimum) return true;
  if (brandConfig?.usFallbackBlockedWhenCalaCount != null) {
    return calaCount >= brandConfig.usFallbackBlockedWhenCalaCount;
  }
  return false;
}

/** Ascend-aligned title wrapper (replaces legacy `— CALA Property Example`). */
export function buildCalaPropertyOpeningTitle(catalog, brandName = "") {
  return buildOpeningsPropertyCardTitle({
    propertyName: catalog?.propertyName,
    brandName: brandName || catalog?.brandName || "",
    marketCity: catalog?.marketCity || catalog?.city || "",
  });
}

export function buildCalaPropertyOpeningCopy(
  catalog,
  { sectionLabel = CALA_SECTION_LABEL_DEFAULT, brandName = "" } = {}
) {
  const title = buildCalaPropertyOpeningTitle(catalog, brandName);
  return {
    title,
    body: catalog.teaser || catalog.ownerRelevance || "",
    meta: catalog.meta || nz(catalog.country) || nz(catalog.marketCity) || "Property",
    chips: catalog.chips || "CALA",
    scenario: catalog.scenario || "",
    sectionLabel,
  };
}

export function isCalaPropertyExampleTitle(title) {
  // Legacy detector — true when old Property Example suffix is present.
  return openingsTitleLooksLikeLegacyPropertyExample(title);
}

/**
 * Governance: Ascend-style titles pass; legacy "— Property Example" suffixes fail.
 */
export function propertyExampleTitlePassesGovernance(title, brandConfig = null) {
  const t = nz(title);
  if (!t) return false;
  if (openingsTitleLooksLikeLegacyPropertyExample(t)) return false;
  if (/—/.test(t)) return true;
  void brandConfig;
  return t.split(/\s+/).length >= 3;
}
