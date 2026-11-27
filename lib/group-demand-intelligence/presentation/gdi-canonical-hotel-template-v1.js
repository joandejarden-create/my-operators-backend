/**
 * GDI Canonical Hotel Template V1 — product-wide UI contract (first three tabs).
 *
 * Different hotels share this template; only intelligence content differs.
 * No hotel-specific UI implementations. Portfolio Demand Intelligence is out of scope.
 *
 * Contract: GDI_CANONICAL_HOTEL_TEMPLATE_V1
 */
export const GDI_CANONICAL_HOTEL_TEMPLATE_V1 = "gdi-canonical-hotel-template-v1";

export const GDI_CANONICAL_HOTEL_TEMPLATE_TABS = Object.freeze([
  {
    id: "executive-summary",
    label: "Executive Summary",
    requiredSections: [
      "Commercial Snapshot",
      "Executive Commercial Assessment",
      "Demand Composition",
      "Commercial Demand Outlook",
      "Dealality Research Receipt",
      "Research Productivity",
      "Priority Commercial Opportunities",
      "Commercial Outcomes",
    ],
  },
  {
    id: "opportunities",
    label: "Group & Demand Opportunities",
    requiredSurfaces: [
      "List View",
      "Tiles View",
      "Columns configuration",
      "Contact Details presentation",
      "Actions SVGs",
      "Opportunity drawers",
      "Filters",
      "Compact Market Scope",
    ],
  },
  {
    id: "commercial-priorities",
    label: "Commercial Priorities",
    requiredSurfaces: ["Commercial Priorities"],
  },
]);

/** DOM / CSS / JS markers that must remain present (UI freeze — no visual redesign). */
export const GDI_CANONICAL_HOTEL_TEMPLATE_FREEZE_MARKERS = Object.freeze({
  htmlShell: "public/group-demand-intelligence.html",
  css: "public/css/group-demand-intelligence.css",
  ui: "public/js/group-demand-intelligence/dealality-gdi-ui.js",
  executiveSummary: "public/js/group-demand-intelligence/executive-summary-tab.js",
  listTable: "public/js/group-demand-intelligence/gdi-commercial-list-table-v1.js",
  requiredCssSelectors: [
    ".gdi-es-receipt",
    ".gdi-es-methodology",
    ".gdi-es-funnel",
    ".gdi-market-scope",
    ".gdi-list-actions",
    ".gdi-page",
  ],
  requiredJsSubstrings: [
    "Dealality Research Receipt",
    "How Research Hours Are Estimated",
    "How to Read This Report",
    "resolveTileContactPreviewBlock",
    "Opportunity of the Week",
  ],
  excludedFromThisContract: ["Portfolio Demand Intelligence redesign"],
  sharedContracts: [
    "gdi-executive-summary-v2",
    "gdi-research-productivity-v1",
    "gdi-customer-publication-package-v1",
    "gdi-commercial-list-table-v1",
  ],
});

export function getCanonicalHotelTemplateManifest() {
  return {
    version: GDI_CANONICAL_HOTEL_TEMPLATE_V1,
    productWide: true,
    hotelSpecificUiForbidden: true,
    tabs: GDI_CANONICAL_HOTEL_TEMPLATE_TABS,
    freeze: GDI_CANONICAL_HOTEL_TEMPLATE_FREEZE_MARKERS,
    contextualGuides: true,
    waveLoading: true,
    note:
      "Preserve accepted integrated UI exactly. Correct data/research/persistence only.",
  };
}
