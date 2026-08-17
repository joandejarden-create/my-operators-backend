/**
 * Discoverability Phase 3C.2 — Public Discoverability baseline contract.
 * Reuses Phase 3C.1 taxonomy / URL governance / public checks.
 * No arbitrary Discoverability Score. No fabricated analytics. No Presence causality.
 */

import {
  PRIORITY_DEVELOPMENT_PAGE_TYPES,
  INDEXABILITY_STATUS,
} from "./discoverability-dimensions.js";
import {
  resolveGovernedBrandUrl,
  URL_GOVERNANCE_GAPS,
  BRAND_BASICS_URL_FIELDS,
} from "./brand-url-governance.js";
import { DATA_STATE } from "./discoverability-data-states.js";
import { PRODUCT_DEFINITIONS } from "./discoverability-taxonomy.js";

export const DISCOVERABILITY_PHASE_3C2_VERSION =
  "ai_visibility_discoverability_phase3c2_v1";

export const PHASE_3C2_STATUS = "BASELINE_EXECUTED_PRODUCT_WIRED";

export const PUBLIC_CONTENT_STATE = Object.freeze({
  PUBLIC_CONTENT_FOUND: "PUBLIC_CONTENT_FOUND",
  PUBLIC_CONTENT_NOT_FOUND: "PUBLIC_CONTENT_NOT_FOUND",
  SOURCE_NOT_CONFIGURED: "SOURCE_NOT_CONFIGURED",
  CHECK_FAILED: "CHECK_FAILED",
  NOT_APPLICABLE: "NOT_APPLICABLE",
});

/** Owner-intent families mapped for public content availability (no score). */
export const OWNER_INTENT_CONTENT_FAMILIES = Object.freeze([
  "Brand Selection",
  "Conversion",
  "Soft Brand / Collection",
  "Upper-Upscale",
  "Lifestyle",
  "Owner Flexibility",
  "Branded Residences",
  "Development Strategy",
]);

export const URL_INVENTORY_SLOTS = Object.freeze([
  { id: "official_homepage", label: "Official brand homepage", fieldHint: "Brand Website" },
  { id: "development_page", label: "Development page", fieldHint: "Brand Development URL" },
  { id: "franchise_page", label: "Franchise page", fieldHint: "Franchise Development URL" },
  { id: "owner_page", label: "Owner page", fieldHint: null },
  { id: "conversion_page", label: "Conversion page", fieldHint: null },
  { id: "regional_development_page", label: "Regional development page", fieldHint: null },
  { id: "branded_residences_page", label: "Branded residences page", fieldHint: "Branded Residences Source URL" },
]);

/**
 * Build URL inventory from governed Brand Matrix / Brand Basics facts only.
 * Missing = MISSING_GOVERNED_SOURCE (never fabricated).
 */
export function buildGovernedUrlInventory(brandRow = {}) {
  const homepage = resolveGovernedBrandUrl(brandRow, { purpose: "primary" });
  const residences = resolveGovernedBrandUrl(brandRow, { purpose: "residences" });

  const slots = URL_INVENTORY_SLOTS.map((slot) => {
    let url = null;
    let sourceField = null;
    if (slot.id === "official_homepage" && homepage.url) {
      url = homepage.url;
      sourceField = "Brand Website";
    } else if (slot.id === "branded_residences_page" && residences.url) {
      url = residences.url;
      sourceField = "Branded Residences Source URL";
    } else if (slot.id === "development_page") {
      url =
        brandRow.brandDevelopmentUrl ||
        brandRow["Brand Development URL"] ||
        null;
      sourceField = url ? "Brand Development URL" : null;
    } else if (slot.id === "franchise_page") {
      url =
        brandRow.franchiseDevelopmentUrl ||
        brandRow["Franchise Development URL"] ||
        null;
      sourceField = url ? "Franchise Development URL" : null;
    }

    return {
      ...slot,
      url,
      status: url ? "CONFIGURED" : "MISSING_GOVERNED_SOURCE",
      sourceField,
    };
  });

  return {
    version: DISCOVERABILITY_PHASE_3C2_VERSION,
    brandId: brandRow.brandId || brandRow.id || null,
    slots,
    configuredCount: slots.filter((s) => s.url).length,
    missingCount: slots.filter((s) => !s.url).length,
    URL_GOVERNANCE_GAPS,
    BRAND_BASICS_URL_FIELDS,
    FABRICATED_URLS: 0,
  };
}

/**
 * Map public content availability to owner-intent families (factual states only).
 * Uses inventory + optional public check results — does not invent content.
 */
export function mapOwnerIntentPublicContent(opts = {}) {
  const inventory = opts.inventory || buildGovernedUrlInventory(opts.brandRow || {});
  const checksBySlot = opts.checksBySlot || {};
  const language = opts.language || null;

  return OWNER_INTENT_CONTENT_FAMILIES.map((family) => {
    const preferredSlots =
      family === "Branded Residences"
        ? ["branded_residences_page", "official_homepage"]
        : family === "Conversion"
          ? ["conversion_page", "development_page", "franchise_page", "official_homepage"]
          : family === "Development Strategy"
            ? ["development_page", "franchise_page", "owner_page", "official_homepage"]
            : ["development_page", "official_homepage"];

    const configured = preferredSlots
      .map((id) => inventory.slots.find((s) => s.id === id))
      .filter((s) => s && s.url);

    if (!configured.length) {
      return {
        ownerIntentFamily: family,
        state: PUBLIC_CONTENT_STATE.SOURCE_NOT_CONFIGURED,
        message: `No governed public URL configured for ${family}.`,
        language,
      };
    }

    const check = checksBySlot[configured[0].id];
    if (!check) {
      return {
        ownerIntentFamily: family,
        state: PUBLIC_CONTENT_STATE.SOURCE_NOT_CONFIGURED,
        message: `Governed URL present for ${family}; public baseline check not yet run.`,
        url: configured[0].url,
        language,
        dataState: DATA_STATE.MEASURABLE_PUBLICLY,
      };
    }
    if (check.failed) {
      return {
        ownerIntentFamily: family,
        state: PUBLIC_CONTENT_STATE.CHECK_FAILED,
        message: `Public check failed for ${family}.`,
        url: configured[0].url,
        language,
      };
    }
    if (check.contentFound === true) {
      return {
        ownerIntentFamily: family,
        state: PUBLIC_CONTENT_STATE.PUBLIC_CONTENT_FOUND,
        message: `Public content found for ${family}.`,
        url: configured[0].url,
        language,
      };
    }
    return {
      ownerIntentFamily: family,
      state: PUBLIC_CONTENT_STATE.PUBLIC_CONTENT_NOT_FOUND,
      message: `No relevant public ${family} content confirmed at the governed URL.`,
      url: configured[0].url,
      language,
    };
  });
}

/**
 * Normalize a public page check into Phase 3C.2 baseline properties.
 */
export function normalizePublicBaselineCheck(check = {}) {
  return {
    URL_EXISTS: check.urlExists === true,
    HTTP_ACCESSIBLE: check.httpAccessible === true || check.ok === true,
    INDEXABLE:
      check.indexability === INDEXABILITY_STATUS.TECHNICALLY_INDEXABLE
        ? true
        : check.indexability === INDEXABILITY_STATUS.NOT_TECHNICALLY_INDEXABLE
          ? false
          : null,
    PAGE_TITLE_PRESENT: check.pageTitlePresent === true,
    META_DESCRIPTION_PRESENT: check.metaDescriptionPresent === true,
    STRUCTURED_DATA_PRESENT: check.structuredDataPresent === true ? true : check.structuredDataPresent === false ? false : null,
    CANONICAL_PRESENT: check.canonicalPresent === true,
    CONTENT_RETRIEVABLE: check.contentRetrievable === true || Boolean(check.body),
    RELEVANT_OWNER_DEVELOPMENT_CONTENT_PRESENT:
      check.developmentContentPresent === true,
    REGIONAL_CONTENT_PRESENT: check.regionalContentPresent === true ? true : null,
    LANGUAGE_CONTENT_PRESENT: check.languageContentPresent === true ? true : null,
  };
}

/**
 * Phase 3C.2 product contract snapshot for Brand V1 Wave 1.
 */
export function buildDiscoverabilityPhase3c2Contract(opts = {}) {
  const inventory = buildGovernedUrlInventory(opts.brandRow || {});
  const intentMapping = mapOwnerIntentPublicContent({
    inventory,
    brandRow: opts.brandRow,
    checksBySlot: opts.checksBySlot,
    language: opts.language,
  });

  return {
    version: DISCOVERABILITY_PHASE_3C2_VERSION,
    PHASE_3C1_REUSED: true,
    PHASE_3C2_STATUS,
    DEFINITION: PRODUCT_DEFINITIONS.DISCOVERABILITY,
    PRESENCE_VS_DISCOVERABILITY: {
      AI_PRESENCE: "Did the model surface the brand?",
      PUBLIC_DISCOVERABILITY:
        "Does the brand have publicly accessible, relevant information that AI/search systems can retrieve or cite?",
      CAUSALITY_CLAIMED: false,
    },
    PUBLIC_BASELINE: {
      properties: [
        "URL_EXISTS",
        "HTTP_ACCESSIBLE",
        "INDEXABLE",
        "PAGE_TITLE_PRESENT",
        "META_DESCRIPTION_PRESENT",
        "STRUCTURED_DATA_PRESENT",
        "CANONICAL_PRESENT",
        "CONTENT_RETRIEVABLE",
        "RELEVANT_OWNER_DEVELOPMENT_CONTENT_PRESENT",
        "REGIONAL_CONTENT_PRESENT",
        "LANGUAGE_CONTENT_PRESENT",
      ],
      ARBITRARY_DISCOVERABILITY_SCORE: false,
      LIVE_CHECKS_DEFAULT: false,
      note: "Baseline checks run via governed public-check engine; Wave 1 ships contract + inventory + intent mapping.",
    },
    URL_INVENTORY: inventory,
    OWNER_INTENT_CONTENT_MAPPING: intentMapping,
    REFERRAL_TRAFFIC: {
        STATUS: "CONNECTION_REQUIRED",
      BRAND_V1_BLOCKER: false,
      measuresReserved: [
        "AI_REFERRAL_SESSIONS",
        "AI_REFERRAL_TRAFFIC_SHARE",
        "AI_REFERRALS_TO_DEVELOPMENT_PAGES",
        "AI_REFERRALS_TO_FRANCHISE_PAGES",
        "AI_REFERRALS_TO_OWNER_PAGES",
        "REFERRAL_SOURCE_BY_AI_PLATFORM",
      ],
      FABRICATED_ANALYTICS: 0,
    },
    PRIORITY_PAGE_TYPES: PRIORITY_DEVELOPMENT_PAGE_TYPES,
    EXECUTIVE_NARRATIVE: {
      WHAT_AI_SURFACES: "AI Presence",
      WHAT_AI_CITES: "Citation / Source Intelligence",
      WHAT_OFFICIAL_INFORMATION_EXISTS: "Public Discoverability",
      WHERE_GAPS_EXIST: [
        "Questions Missing",
        "Peer Presence Gaps",
        "Owned Source Citation Gaps",
        "Public Content Gaps",
      ],
    },
    hardGuards: {
      ARBITRARY_DISCOVERABILITY_SCORE: 0,
      CAUSAL_SOURCE_CLAIMS: 0,
      FABRICATED_ANALYTICS: 0,
      PRESENCE_DEFINITION_CHANGES: 0,
    },
  };
}
