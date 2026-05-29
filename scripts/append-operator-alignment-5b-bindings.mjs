#!/usr/bin/env node
/**
 * Appends Phase 5B operator field bindings and regenerates build-sheet rows.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  OAS_OPERATOR_SERVICE_OPTIONS,
  OAS_ACTIVE_COUNTRIES_OPTIONS,
  OAS_ACTIVE_MARKETS_OPTIONS,
  OAS_MARKET_PRESENCE_TYPE_OPTIONS,
  OAS_SERVICE_MODELS_SUPPORTED_OPTIONS,
  OAS_MANAGEMENT_STRUCTURES_OPTIONS,
  OAS_EXPERIENCE_LEVEL_OPTIONS,
  OAS_OWNER_REPORTING_LEVEL_OPTIONS,
  OAS_DATA_CONFIDENCE_OPTIONS,
  OAS_SOURCE_TYPE_OPTIONS,
  OAS_BRAND_FAMILIES_OPTIONS,
  OAS_FB_CAPABILITY_OPTIONS,
  OAS_REVENUE_MGMT_CAPABILITY_OPTIONS,
  OAS_SALES_PLATFORM_OPTIONS,
  OAS_GOVERNANCE_CADENCE_OPTIONS,
} from "../lib/operator-alignment-field-options.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const bindingsPath = path.join(ROOT, "api/lib/third-party-operator-new-two-field-bindings.json");

function binding(formKeys, airtableName, fieldType, selectOptions = null) {
  return {
    formKeys: Array.isArray(formKeys) ? formKeys : [formKeys],
    airtableName,
    tableKey: "PERF",
    fieldType,
    selectOptions,
  };
}

const NEW_BINDINGS = [
  binding("activeCountries", "Active Countries", "multipleSelects", OAS_ACTIVE_COUNTRIES_OPTIONS),
  binding("activeMarkets", "Active Markets / Cities", "multipleSelects", OAS_ACTIVE_MARKETS_OPTIONS),
  binding("marketPresenceType", "Market Presence Type", "multipleSelects", OAS_MARKET_PRESENCE_TYPE_OPTIONS),
  binding("serviceModelsSupported", "Service Models Supported", "multipleSelects", OAS_SERVICE_MODELS_SUPPORTED_OPTIONS),
  binding("managementStructuresSupported", "Management Structures Supported", "multipleSelects", OAS_MANAGEMENT_STRUCTURES_OPTIONS),
  binding("offeredServices", "Offered Services", "multipleSelects", OAS_OPERATOR_SERVICE_OPTIONS),
  binding("newBuildOpeningExperience", "New-Build Opening Experience", "singleSelect", OAS_EXPERIENCE_LEVEL_OPTIONS),
  binding(
    "preOpeningSupportCapability",
    "Pre-Opening Support Capability",
    "singleSelect",
    ["Advanced", "Standard", "Limited", "None documented", "Unknown"]
  ),
  binding("ownerReportingLevel", "Owner Reporting Level", "singleSelect", OAS_OWNER_REPORTING_LEVEL_OPTIONS),
  binding("dataConfidenceLevel", "Data Confidence Level", "singleSelect", OAS_DATA_CONFIDENCE_OPTIONS),
  binding("sourceType", "Source Type", "multipleSelects", OAS_SOURCE_TYPE_OPTIONS),
  binding("lastUpdatedDate", "Last Updated Date", "date", null),
  binding("brandFamiliesOperated", "Brand Families Operated", "multipleSelects", OAS_BRAND_FAMILIES_OPTIONS),
  binding("conversionReflagExperience", "Conversion / Reflag Experience", "singleSelect", OAS_EXPERIENCE_LEVEL_OPTIONS),
  binding("softBrandLifestyleExperience", "Soft Brand / Lifestyle Experience", "singleSelect", OAS_EXPERIENCE_LEVEL_OPTIONS),
  binding("fbCapabilityLevel", "F&B Capability Level", "singleSelect", OAS_FB_CAPABILITY_OPTIONS),
  binding("revenueManagementCapability", "Revenue Management Capability", "singleSelect", OAS_REVENUE_MGMT_CAPABILITY_OPTIONS),
  binding("salesPlatform", "Sales Platform", "multipleSelects", OAS_SALES_PLATFORM_OPTIONS),
  binding("governanceCadence", "Governance Cadence", "singleSelect", OAS_GOVERNANCE_CADENCE_OPTIONS),
  binding("minimumKeyCount", "Minimum Key Count", "number", null),
  binding("similarProjectCaseStudies", "Similar Project Case Studies", "multilineText", null),
  // Reuse existing chain scales — explicit binding for build sheet
  binding("chainScalesSupported", "chainScalesSupported", "multipleSelects", null),
];

const doc = JSON.parse(fs.readFileSync(bindingsPath, "utf8"));
const existing = new Set((doc.bindings || []).map((b) => JSON.stringify(b.formKeys) + b.airtableName));
let added = 0;
for (const b of NEW_BINDINGS) {
  const key = JSON.stringify(b.formKeys) + b.airtableName;
  if (existing.has(key)) continue;
  doc.bindings.push(b);
  existing.add(key);
  added += 1;
}
fs.writeFileSync(bindingsPath, JSON.stringify(doc, null, 2) + "\n", "utf8");
console.log("Added bindings:", added, "Total:", doc.bindings.length);
