/**
 * AI Demand Positioning — Airtable schema proposal.
 * Apply only via ensure script with ADP_SCHEMA_APPLY=true.
 */

import {
  ADP_PUBLISHED_REPORTS_TABLE,
  ADP_CENSUS_TABLE,
  ADP_PUBLISH_STATUS_OPTIONS,
} from "./airtable-field-map.js";

export const ADP_HOTEL_CENSUS_TABLE = ADP_CENSUS_TABLE;

/** @type {Array<{ name: string, type: string, description?: string, options?: object, primary?: boolean }>} */
export const ADP_PUBLISHED_REPORT_CORE_FIELD_SPECS = [
  {
    name: "Report Name",
    type: "singleLineText",
    description: "Primary — e.g. Waterstone Resort & Marina — 2026-08-19",
    primary: true,
  },
  {
    name: "ADP Property ID",
    type: "singleLineText",
    description: "Internal property id (e.g. adp_waterstone_boca_raton). Stable join key before census link exists.",
  },
  {
    name: "Period ID",
    type: "singleLineText",
    description: "Monitoring period id (adp_period_…).",
  },
  {
    name: "Property Name",
    type: "singleLineText",
    description: "Display name for list views.",
  },
  { name: "City", type: "singleLineText" },
  { name: "State", type: "singleLineText" },
  { name: "Market", type: "singleLineText" },
  {
    name: "Execution Date",
    type: "dateTime",
    description: "Monitoring period execution timestamp.",
    options: { dateFormat: { name: "iso" }, timeFormat: { name: "24hour" }, timeZone: "utc" },
  },
  {
    name: "Published At",
    type: "dateTime",
    description: "When this published snapshot was written.",
    options: { dateFormat: { name: "iso" }, timeFormat: { name: "24hour" }, timeZone: "utc" },
  },
  {
    name: "Publish Status",
    type: "singleSelect",
    description: "Governed publish lifecycle.",
    options: { choices: ADP_PUBLISH_STATUS_OPTIONS.map((name) => ({ name })) },
  },
  {
    name: "Product Version",
    type: "singleLineText",
    description: "Payload contract version (e.g. ai_demand_positioning_v1).",
  },
  {
    name: "Demand Capture Rate",
    type: "number",
    description: "Headline demand capture % for filtering.",
    options: { precision: 1 },
  },
  {
    name: "Provider Count",
    type: "number",
    description: "Providers in this published period.",
    options: { precision: 0 },
  },
  {
    name: "Payload JSON",
    type: "multilineText",
    description: "Pre-computed customer-safe owner payload (UI sections only).",
  },
  {
    name: "Evidence Index JSON",
    type: "multilineText",
    description: "Pre-built evidence drawer index (excerpts only, no full raw corpus).",
  },
  {
    name: "Payload Store Ref",
    type: "singleLineText",
    description: "Non-Airtable storage path for backup/audit (e.g. published/adp_waterstone/report-….json).",
  },
  {
    name: "Census Record ID",
    type: "singleLineText",
    description:
      "Hotel Property Census link — Airtable record id (rec…). Populate when census row exists; enables cross-product joins until native linked field is available in same base.",
  },
];

export function getPublishedReportLinkFieldSpecs() {
  // Only applicable when Published Reports table lives in the same base as Hotel Property Census.
  return [
    {
      name: "Linked Census Property",
      type: "multipleRecordLinks",
      description: `Native link to ${ADP_HOTEL_CENSUS_TABLE} (same base only). Use Census Record ID for dedicated ADP base.`,
      linkedTableName: ADP_HOTEL_CENSUS_TABLE,
    },
  ];
}

export function classifyFieldEnsureAction(existing, spec) {
  if (!existing) return { action: "create", spec };
  if (existing.type !== spec.type) {
    return {
      action: "conflict",
      reason: `type mismatch: existing=${existing.type}, proposed=${spec.type}`,
      spec,
    };
  }
  return { action: "skip", spec };
}

export function getPublishedReportFieldSpecs(includeLinks = false) {
  const specs = [...ADP_PUBLISHED_REPORT_CORE_FIELD_SPECS];
  if (includeLinks) specs.push(...getPublishedReportLinkFieldSpecs());
  return specs;
}
