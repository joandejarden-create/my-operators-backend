/**
 * Registry of Operator Alignment fields to audit against live Airtable schema.
 * plannedOptionsKey → export from operator-alignment-field-options.js
 */

import * as Planned from "./operator-alignment-field-options.js";

export const OAS_AUDIT_TABLES = {
  deals: "Deals",
  si: "Strategic Intent - Operational - Key Challenges",
  mp: "Market - Performance - Deal & Capital Structure",
  master: "Operator Setup - Master",
  profile: "Operator Setup - Profile & Positioning",
  platform: "Operator Setup - Platform & Markets",
  commercial: "Operator Setup - Commercial Fit & Terms",
  governance: "Operator Setup - Governance, Delivery & Diligence",
};

/** @type {Array<{ tableKey: string, fieldName: string, plannedOptions?: string[], plannedOptionsKey?: string }>} */
export const OAS_AUDIT_FIELD_SPECS = [
  // Deals
  { tableKey: "deals", fieldName: Planned.OAS_DEAL_DEALS_FIELD_NAMES.fbComplexity, plannedOptions: Planned.OAS_FB_CAPABILITY_OPTIONS },
  { tableKey: "deals", fieldName: Planned.OAS_DEAL_DEALS_FIELD_NAMES.openingTimeline, plannedOptions: Planned.OAS_OPENING_TIMELINE_OPTIONS },
  // SI — Phase 5B
  { tableKey: "si", fieldName: Planned.OAS_DEAL_SI_FIELD_NAMES.operatorReviewStatus, plannedOptions: Planned.OAS_OPERATOR_REVIEW_STATUS_OPTIONS },
  { tableKey: "si", fieldName: Planned.OAS_DEAL_SI_FIELD_NAMES.preferredManagementStructure, plannedOptions: Planned.OAS_PREFERRED_MANAGEMENT_STRUCTURE_OPTIONS },
  { tableKey: "si", fieldName: Planned.OAS_DEAL_SI_FIELD_NAMES.requiredOperatorServices, plannedOptions: Planned.OAS_OPERATOR_SERVICE_OPTIONS },
  { tableKey: "si", fieldName: Planned.OAS_DEAL_SI_FIELD_NAMES.mustHaveOperatorServices, plannedOptions: Planned.OAS_OPERATOR_SERVICE_OPTIONS },
  { tableKey: "si", fieldName: Planned.OAS_DEAL_SI_FIELD_NAMES.niceToHaveOperatorServices, plannedOptions: Planned.OAS_OPERATOR_SERVICE_OPTIONS },
  { tableKey: "si", fieldName: Planned.OAS_DEAL_SI_FIELD_NAMES.marketPresenceRequirement, plannedOptions: Planned.OAS_MARKET_PRESENCE_REQUIREMENT_OPTIONS },
  { tableKey: "si", fieldName: Planned.OAS_DEAL_SI_FIELD_NAMES.preOpeningSupportNeeded, plannedOptions: Planned.OAS_PREOPENING_SUPPORT_NEEDED_OPTIONS },
  { tableKey: "si", fieldName: Planned.OAS_DEAL_SI_FIELD_NAMES.ownerReportingExpectations, plannedOptions: Planned.OAS_OWNER_REPORTING_EXPECTATIONS_OPTIONS },
  { tableKey: "si", fieldName: Planned.OAS_DEAL_SI_FIELD_NAMES.brandOperatorResponsibilitySplit, plannedOptions: Planned.OAS_BRAND_OPERATOR_SPLIT_OPTIONS },
  { tableKey: "si", fieldName: Planned.OAS_DEAL_SI_FIELD_NAMES.ownerControlPreference, plannedOptions: Planned.OAS_OWNER_CONTROL_PREFERENCE_OPTIONS },
  { tableKey: "si", fieldName: Planned.OAS_DEAL_SI_FIELD_NAMES.commercialPriority, plannedOptions: Planned.OAS_COMMERCIAL_PRIORITY_OPTIONS },
  { tableKey: "si", fieldName: Planned.OAS_DEAL_SI_FIELD_NAMES.localLaborHrSupportNeeded, plannedOptions: Planned.OAS_YES_NO_NA_OPTIONS },
  { tableKey: "si", fieldName: Planned.OAS_DEAL_SI_FIELD_NAMES.procurementSupportNeeded, plannedOptions: Planned.OAS_YES_NO_NA_OPTIONS },
  { tableKey: "si", fieldName: Planned.OAS_DEAL_SI_FIELD_NAMES.ownerInternalOpsCapability, plannedOptions: Planned.OAS_OWNER_INTERNAL_OPS_OPTIONS },
  { tableKey: "si", fieldName: Planned.OAS_DEAL_SI_FIELD_NAMES.brandAgreementStructure, plannedOptions: Planned.OAS_BRAND_AGREEMENT_STRUCTURE_OPTIONS },
  { tableKey: "si", fieldName: Planned.OAS_DEAL_SI_FIELD_NAMES.dealOperatingModel, plannedOptions: Planned.OAS_DEAL_OPERATING_MODEL_OPTIONS },
  { tableKey: "si", fieldName: Planned.OAS_DEAL_SI_FIELD_NAMES.operatorScope, plannedOptions: Planned.OAS_OPERATOR_SCOPE_OPTIONS },
  // Legacy SI/MP used by scoring
  { tableKey: "si", fieldName: "Services Required From Operator" },
  { tableKey: "si", fieldName: "Must-Haves From Brand/Operator" },
  { tableKey: "mp", fieldName: "Preferred Deal Structure" },
  // Operator Platform
  { tableKey: "platform", fieldName: "Active Countries", plannedOptions: Planned.OAS_ACTIVE_COUNTRIES_OPTIONS },
  { tableKey: "platform", fieldName: "Active Markets / Cities", plannedOptions: Planned.OAS_ACTIVE_MARKETS_OPTIONS },
  { tableKey: "platform", fieldName: "Market Presence Type", plannedOptions: Planned.OAS_MARKET_PRESENCE_TYPE_OPTIONS },
  // Operator Profile
  { tableKey: "profile", fieldName: "Service Models Supported", plannedOptions: Planned.OAS_SERVICE_MODELS_SUPPORTED_OPTIONS },
  { tableKey: "profile", fieldName: "chainScalesSupported", plannedOptions: Planned.OAS_CHAIN_SCALES_SUPPORTED_OPTIONS },
  { tableKey: "profile", fieldName: "Brand Families Operated", plannedOptions: Planned.OAS_BRAND_FAMILIES_OPTIONS },
  { tableKey: "profile", fieldName: "Soft Brand / Lifestyle Experience", plannedOptions: Planned.OAS_EXPERIENCE_LEVEL_OPTIONS },
  // Commercial
  { tableKey: "commercial", fieldName: "Management Structures Supported", plannedOptions: Planned.OAS_MANAGEMENT_STRUCTURES_OPTIONS },
  { tableKey: "commercial", fieldName: "New-Build Opening Experience", plannedOptions: Planned.OAS_EXPERIENCE_LEVEL_OPTIONS },
  {
    tableKey: "commercial",
    fieldName: "Pre-Opening Support Capability",
    plannedOptions: ["Advanced", "Standard", "Limited", "None documented", "Unknown"],
  },
  { tableKey: "commercial", fieldName: "Conversion / Reflag Experience", plannedOptions: Planned.OAS_EXPERIENCE_LEVEL_OPTIONS },
  // Governance
  { tableKey: "governance", fieldName: "Offered Services", plannedOptions: Planned.OAS_OPERATOR_SERVICE_OPTIONS },
  { tableKey: "governance", fieldName: "Owner Reporting Level", plannedOptions: Planned.OAS_OWNER_REPORTING_LEVEL_OPTIONS },
  { tableKey: "governance", fieldName: "F&B Capability Level", plannedOptions: Planned.OAS_FB_CAPABILITY_OPTIONS },
  { tableKey: "governance", fieldName: "Revenue Management Capability", plannedOptions: Planned.OAS_REVENUE_MGMT_CAPABILITY_OPTIONS },
  { tableKey: "governance", fieldName: "Sales Platform", plannedOptions: Planned.OAS_SALES_PLATFORM_OPTIONS },
  { tableKey: "governance", fieldName: "Governance Cadence", plannedOptions: Planned.OAS_GOVERNANCE_CADENCE_OPTIONS },
  // Master
  { tableKey: "master", fieldName: "Data Confidence Level", plannedOptions: Planned.OAS_DATA_CONFIDENCE_OPTIONS },
  { tableKey: "master", fieldName: "Source Type", plannedOptions: Planned.OAS_SOURCE_TYPE_OPTIONS },
];

export function fieldRegistryKey(tableKey, fieldName) {
  return `${tableKey}::${fieldName}`;
}
