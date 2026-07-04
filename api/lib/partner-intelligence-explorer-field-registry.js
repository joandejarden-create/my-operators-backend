/**
 * Partner Intelligence — Explorer field registry (Brand + Operator).
 *
 * Single source of truth for:
 *   - extraction field targets
 *   - published field keys
 *   - read-merge paths into Brand/Operator Explorer APIs
 *
 * Registry version: bump when adding/removing keys (stored on Published rows).
 */
import { PARTNER_INTELLIGENCE_GAP_COPY } from "./partner-intelligence-field-map.js";
import {
  buildFullOperatorExplorerRegistry,
  listLlmExtractableFields,
} from "../../lib/partner-intelligence/operator-explorer-registry-catalog.js";
import {
  buildBrandExplorerRegistry,
  listBrandFieldsForExtraction,
} from "../../lib/partner-intelligence/brand-explorer-registry-catalog.js";

export const PARTNER_INTELLIGENCE_REGISTRY_VERSION = 2;

/** Pilot operator — Arbor Lodging (CALA) new-base Master */
export const PILOT_OPERATORS = {
  arborLodging: {
    key: "arborLodging",
    recordId: "recF5Z87OAqFgndoq",
    companyName: "Arbor Lodging (CALA)",
    referenceFolder: "Arbor Lodging",
    domain: "arborlodging.com",
    region: "CALA",
    explorerUrl: "/operator-explorer-gold-mock.html?id=recF5Z87OAqFgndoq",
  },
};

/** Pilot brand — Kimpton Hotels (IHG) */
export const PILOT_BRANDS = {
  kimptonHotels: {
    key: "kimptonHotels",
    recordId: "recCKuXCmGvxHPfb3",
    brandName: "Kimpton Hotels",
    parentCompany: "IHG Hotels & Resorts",
    referenceFolder: "IHG Hotels & Resorts",
    includeSubpaths: ["brands/Kimpton", "brands/Kimpton Hotels Restaurants", "fdd"],
    brandNameMatch: "kimpton",
    brandSlug: "kimpton",
    region: "Global",
    explorerUrl: "/brand-explorer-combined.html?brand=Kimpton%20Hotels",
  },
};

/**
 * Operator Explorer canonical tabs (11).
 * Tabs marked publishScope: false are excluded from Partner Intelligence publish merge.
 */
export const OPERATOR_EXPLORER_TABS = [
  { tab: "Profile & Positioning", publishScope: true, tabIndex: 1 },
  { tab: "Operating Platform", publishScope: true, tabIndex: 2 },
  { tab: "Brand & Relationships", publishScope: true, tabIndex: 3 },
  { tab: "Markets & Footprint", publishScope: true, tabIndex: 4 },
  { tab: "Owner Engagement & Reporting", publishScope: true, tabIndex: 5 },
  { tab: "Infrastructure & Data", publishScope: true, tabIndex: 6 },
  { tab: "Leadership", publishScope: true, tabIndex: 7 },
  { tab: "Project Fit & Deal Profile", publishScope: true, tabIndex: 8 },
  { tab: "Proof & Track Record", publishScope: true, tabIndex: 9 },
  { tab: "Operator Materials", publishScope: true, tabIndex: 10 },
  { tab: "Dealality Insights", publishScope: false, tabIndex: 11, excludeReason: "Dealality-derived analysis" },
  { tab: "Alignment Context", publishScope: false, optional: true, excludeReason: "OAS deal alignment" },
];

/**
 * Brand Explorer atelier tabs (combined UI).
 */
export const BRAND_EXPLORER_TABS = [
  "Overview",
  "Value to Owners",
  "Operating Model",
  "Owner Considerations",
  "Commercial Engine",
  "Economics & Obligations",
  "Loyalty Program",
  "Footprint & Growth",
  "Brand Materials",
  "Dealality Insight",
];

/**
 * @typedef {object} RegistryField
 * @property {string} fieldKey — stable id stored in Extracted/Published facts
 * @property {'Brand Explorer'|'Operator Explorer'} explorerType
 * @property {string} explorerTab
 * @property {string} explorerSection
 * @property {string} displayLabel
 * @property {string} [responsePath] — dot path on GET API payload for read merge
 * @property {string} [prefillKey] — Operator Setup prefill camelCase key
 * @property {string} [slotKey] — Brand presentation slot key
 * @property {boolean} publishScope
 * @property {boolean} [allowGapCopy] — may publish standard gap sentence
 */

/** @type {RegistryField[]} */
export const OPERATOR_EXPLORER_FIELDS = buildFullOperatorExplorerRegistry();

/** @type {RegistryField[]} */
export const BRAND_EXPLORER_FIELDS = buildBrandExplorerRegistry();

/** Arbor pilot — suggested public source seeds for Phase 8 capture (not auto-created) */
export const PILOT_OPERATOR_SOURCE_CANDIDATES = {
  arborLodging: [
    {
      sourceTitle: "Arbor Lodging — Platforms / CALA",
      sourceUrl: "https://www.arborlodging.com/platforms",
      sourceType: "Website Capture",
      sourceOrigin: "Public Web",
      suggestedQuality: "Medium",
    },
    {
      sourceTitle: "Hotel Investment Today — Arbor Lodging profile",
      sourceUrl:
        "https://www.hotelinvestmenttoday.com/Development/Owners/What-makes-sense-today-for-Arbor-Lodging",
      sourceType: "Press Release",
      sourceOrigin: "Public Web",
      suggestedQuality: "Medium",
    },
    {
      sourceTitle: "Arbor Lodging — Press releases",
      sourceUrl: "https://www.arborlodging.com/press",
      sourceType: "Press Release",
      sourceOrigin: "Public Web",
      suggestedQuality: "Medium",
    },
  ],
};

/**
 * @param {string} fieldKey
 * @param {'Brand Explorer'|'Operator Explorer'} [explorerType]
 * @returns {RegistryField|undefined}
 */
export function getRegistryField(fieldKey, explorerType) {
  const pool =
    explorerType === "Brand Explorer"
      ? BRAND_EXPLORER_FIELDS
      : explorerType === "Operator Explorer"
        ? OPERATOR_EXPLORER_FIELDS
        : [...OPERATOR_EXPLORER_FIELDS, ...BRAND_EXPLORER_FIELDS];
  return pool.find((f) => f.fieldKey === fieldKey);
}

/**
 * Fields eligible for Partner Intelligence publish merge.
 * @param {'Brand Explorer'|'Operator Explorer'} explorerType
 * @returns {RegistryField[]}
 */
export function listPublishableFields(explorerType) {
  const pool = explorerType === "Brand Explorer" ? BRAND_EXPLORER_FIELDS : OPERATOR_EXPLORER_FIELDS;
  return pool.filter((f) => f.publishScope);
}

/**
 * @param {string} explorerTab
 * @returns {RegistryField[]}
 */
export function listOperatorFieldsForExtraction() {
  return listLlmExtractableFields(OPERATOR_EXPLORER_FIELDS);
}

export function listBrandFieldsForExtractionRegistry() {
  return listBrandFieldsForExtraction(BRAND_EXPLORER_FIELDS);
}

export function listOperatorFieldsByTab(explorerTab) {
  return OPERATOR_EXPLORER_FIELDS.filter((f) => f.explorerTab === explorerTab);
}

export { PARTNER_INTELLIGENCE_GAP_COPY };
