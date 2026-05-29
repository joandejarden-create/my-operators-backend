/**
 * Planned select / multi-select options for Operator Alignment (reference + schema creation).
 * **Authoritative options for writes/scoring:** `reports/operator-alignment-live-airtable-options.json`
 * (refresh via `node scripts/export-operator-alignment-live-airtable-options.mjs`).
 */

export const OAS_OPERATOR_SERVICE_OPTIONS = [
  "Full hotel management",
  "Pre-opening planning",
  "Opening / transition support",
  "Revenue management",
  "Sales",
  "Distribution / channel management",
  "Digital marketing",
  "Accounting / finance",
  "HR / staffing",
  "Procurement",
  "F&B operations",
  "Brand compliance support",
  "Owner reporting",
  "Asset management support",
  "Technical services coordination",
  "Other",
];

export const OAS_ACTIVE_COUNTRIES_OPTIONS = [
  "Mexico",
  "Dominican Republic",
  "Costa Rica",
  "Panama",
  "Colombia",
  "Peru",
  "Chile",
  "Argentina",
  "Brazil",
  "Jamaica",
  "Puerto Rico",
  "Curaçao",
  "Trinidad & Tobago",
  "Turks & Caicos",
  "United States",
  "Spain",
  "Other",
];

export const OAS_ACTIVE_MARKETS_OPTIONS = [
  "Cancún",
  "Mexico City",
  "Monterrey",
  "Guadalajara",
  "Riviera Maya",
  "Punta Cana",
  "Santo Domingo",
  "San Juan",
  "Panama City",
  "Bogotá",
  "Medellín",
  "Lima",
  "Santiago",
  "Buenos Aires",
  "São Paulo",
  "Rio de Janeiro",
  "San José",
  "Montevideo",
  "Other",
];

export const OAS_MARKET_PRESENCE_TYPE_OPTIONS = [
  "Active operations",
  "Prior experience",
  "Pipeline / signed project",
  "Target market",
  "No known presence",
  "Unknown",
];

export const OAS_SERVICE_MODELS_SUPPORTED_OPTIONS = [
  "Limited-service",
  "Select-service",
  "Focused-service",
  "Full-service",
  "Lifestyle",
  "Boutique",
  "Resort",
  "All-inclusive",
  "Extended stay",
  "Branded residential / mixed-use",
  "Other",
];

/** Matches live Airtable `chainScalesSupported` (2026-05-25 export). */
export const OAS_CHAIN_SCALES_SUPPORTED_OPTIONS = [
  "Luxury",
  "Upper Upscale",
  "Upscale",
  "Upper Midscale",
  "Midscale",
  "Economy",
  "Independent",
];

export const OAS_MANAGEMENT_STRUCTURES_OPTIONS = [
  "Full third-party management",
  "Brand-managed",
  "Franchise support",
  "Commercial-only support",
  "Pre-opening / transition support",
  "Asset management support",
  "Hybrid / project-specific",
  "Other",
];

export const OAS_EXPERIENCE_LEVEL_OPTIONS = [
  "Strong",
  "Moderate",
  "Limited",
  "None documented",
  "Unknown",
];

export const OAS_OWNER_REPORTING_LEVEL_OPTIONS = [
  "Basic owner reporting",
  "Monthly operating review",
  "Institutional reporting",
  "Lender / investor-grade reporting",
  "Custom / project-specific",
  "Unknown",
];

export const OAS_DATA_CONFIDENCE_OPTIONS = [
  "Verified",
  "Operator-provided",
  "Public-source",
  "Inferred",
  "Incomplete",
];

export const OAS_SOURCE_TYPE_OPTIONS = [
  "Operator-provided",
  "Internal research",
  "Public website",
  "Brand / owner conversation",
  "Prior project knowledge",
  "Imported sample data",
  "Unknown",
];

export const OAS_BRAND_FAMILIES_OPTIONS = [
  "Marriott",
  "Hilton",
  "Hyatt",
  "IHG",
  "Choice",
  "Wyndham",
  "Accor",
  "Sonesta",
  "Radisson / Choice",
  "Independent",
  "Soft brands / collections",
  "Other",
];

export const OAS_FB_CAPABILITY_OPTIONS = [
  "None / rooms-only",
  "Limited F&B",
  "Moderate F&B",
  "Significant F&B",
  "Lifestyle / experiential F&B",
  "Unknown",
];

export const OAS_REVENUE_MGMT_CAPABILITY_OPTIONS = [
  "Property-level only",
  "Centralized support",
  "Advanced centralized platform",
  "Third-party partner",
  "None documented",
  "Unknown",
];

export const OAS_SALES_PLATFORM_OPTIONS = [
  "Local sales",
  "Regional sales",
  "Global sales support",
  "Group sales",
  "Digital / e-commerce",
  "None documented",
  "Unknown",
];

export const OAS_GOVERNANCE_CADENCE_OPTIONS = [
  "Monthly",
  "Quarterly",
  "Asset-management style",
  "Board / investor reporting",
  "Custom / project-specific",
  "Unknown",
];

/** Deal intake — operator strategy */
export const OAS_OPERATOR_REVIEW_STATUS_OPTIONS = [
  "Not started",
  "Exploring operator options",
  "Operator review in scope",
  "Ready for operator shortlist",
  "Operator already selected",
  "Not applicable",
  "Unknown",
];

export const OAS_PREFERRED_MANAGEMENT_STRUCTURE_OPTIONS = [
  "Full third-party management",
  "Brand-managed",
  "Franchise with third-party operator",
  "Owner-operated",
  "Owner-operated with commercial support",
  "Commercial-only support",
  "Pre-opening / transition support",
  "Hybrid / project-specific",
  "Undecided",
];

export const OAS_MARKET_PRESENCE_REQUIREMENT_OPTIONS = [
  "Active local market operations required",
  "Active country operations required",
  "Regional experience acceptable",
  "Prior similar-market experience acceptable",
  "Open to new-market operator",
  "Unknown",
];

export const OAS_PREOPENING_SUPPORT_NEEDED_OPTIONS = ["Yes", "No", "Unknown", "Not applicable"];

export const OAS_OWNER_REPORTING_EXPECTATIONS_OPTIONS = OAS_OWNER_REPORTING_LEVEL_OPTIONS;

export const OAS_BRAND_OPERATOR_SPLIT_OPTIONS = [
  "Brand-managed",
  "Brand standards with third-party operator",
  "Franchise with owner/operator execution",
  "Owner-operated with brand support",
  "Operator-led with brand compliance support",
  "Undecided",
  "Not applicable",
];

export const OAS_OWNER_CONTROL_PREFERENCE_OPTIONS = [
  "Owner wants high control",
  "Shared control",
  "Operator-led operations",
  "Institutional governance preferred",
  "Undecided",
  "Unknown",
];

export const OAS_COMMERCIAL_PRIORITY_OPTIONS = [
  "Revenue management",
  "Sales",
  "Distribution",
  "Digital marketing",
  "Loyalty / brand channels",
  "Group sales",
  "Corporate accounts",
  "None specified",
  "Other",
];

export const OAS_YES_NO_NA_OPTIONS = ["Yes", "No", "Unknown", "Not applicable"];

export const OAS_OWNER_INTERNAL_OPS_OPTIONS = [
  "Strong internal hotel operations team",
  "Partial internal capability",
  "Limited internal capability",
  "No internal hotel operations team",
  "Unknown",
];

export const OAS_OPENING_TIMELINE_OPTIONS = [
  "Pre-development",
  "Under construction",
  "0–6 months",
  "6–12 months",
  "12–24 months",
  "24+ months",
  "Unknown",
];

export const OAS_BRAND_AGREEMENT_STRUCTURE_OPTIONS = [
  "Franchise",
  "Management",
  "License",
  "Soft brand / collection affiliation",
  "Brand-managed",
  "Undecided",
  "Not applicable",
];

export const OAS_DEAL_OPERATING_MODEL_OPTIONS = [
  "Owner-operated",
  "Third-party managed",
  "Brand-managed",
  "Hybrid / project-specific",
  "Undecided",
  "Not applicable",
];

export const OAS_OPERATOR_SCOPE_OPTIONS = [
  "Full management",
  "Commercial support",
  "Pre-opening support",
  "Brand compliance support",
  "Owner reporting",
  "Asset management support",
  "Technical services coordination",
  "Not applicable",
  "Other",
];

/** camelCase prefill keys ↔ Airtable column titles */
export const OAS_OPERATOR_PREFILL_KEY_ALIASES = {
  activeCountries: ["Active Countries", "active_countries"],
  activeMarkets: ["Active Markets / Cities", "Active Markets", "active_markets"],
  marketPresenceType: ["Market Presence Type", "market_presence_type"],
  serviceModelsSupported: ["Service Models Supported", "service_models_supported"],
  managementStructuresSupported: [
    "Management Structures Supported",
    "management_structures_supported",
    "bf_selected_deal_structures",
  ],
  offeredServices: ["Offered Services", "offered_services"],
  newBuildOpeningExperience: ["New-Build Opening Experience", "new_build_opening_experience"],
  preOpeningSupportCapability: ["Pre-Opening Support Capability", "pre_opening_support_capability"],
  ownerReportingLevel: ["Owner Reporting Level", "owner_reporting_level"],
  dataConfidenceLevel: ["Data Confidence Level", "data_confidence_level"],
  sourceType: ["Source Type", "source_type"],
  lastUpdatedDate: ["Last Updated Date", "last_updated_date"],
  brandFamiliesOperated: ["Brand Families Operated", "brand_families_operated"],
  conversionReflagExperience: ["Conversion / Reflag Experience", "conversion_reflag_experience"],
  softBrandLifestyleExperience: ["Soft Brand / Lifestyle Experience", "soft_brand_lifestyle_experience"],
  fbCapabilityLevel: ["F&B Capability Level", "fb_capability_level"],
  revenueManagementCapability: ["Revenue Management Capability", "revenue_management_capability"],
  salesPlatform: ["Sales Platform", "sales_platform"],
  governanceCadence: ["Governance Cadence", "governance_cadence"],
  minimumKeyCount: ["Minimum Key Count", "minimum_key_count"],
};

export const OAS_DEAL_SI_FIELD_NAMES = {
  operatorReviewStatus: "Operator Review Status",
  preferredManagementStructure: "Preferred Management Structure",
  requiredOperatorServices: "Required Operator Services",
  mustHaveOperatorServices: "Must-Have Operator Services",
  niceToHaveOperatorServices: "Nice-to-Have Operator Services",
  marketPresenceRequirement: "Market Presence Requirement",
  preOpeningSupportNeeded: "Pre-Opening Support Needed",
  ownerReportingExpectations: "Owner Reporting Expectations",
  brandOperatorResponsibilitySplit: "Brand / Operator Responsibility Split",
  ownerControlPreference: "Owner Control Preference",
  commercialPriority: "Commercial Priority",
  localLaborHrSupportNeeded: "Local Labor / HR Support Needed",
  procurementSupportNeeded: "Procurement Support Needed",
  ownerInternalOpsCapability: "Owner Internal Ops Capability",
  brandAgreementStructure: "Brand Agreement Structure",
  dealOperatingModel: "Operating Model",
  operatorScope: "Operator Scope",
};

export const OAS_DEAL_DEALS_FIELD_NAMES = {
  fbComplexity: "F&B Complexity",
  openingTimeline: "Opening Timeline",
};
