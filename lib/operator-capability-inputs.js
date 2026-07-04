/**
 * Operator Capability Snapshot — canonical field names, options, and legacy mappings.
 * @see docs/operator-capability-inputs-v1.md (field inventory)
 */

export const OCS_INPUTS_VERSION = "ocs-inputs-v1";

/** Single-select sentinel when backfill cannot infer safely. */
export const NEEDS_REVIEW = "Needs Review";

export const DEALS_FIELDS = {
  currentOperatingModel: "Current Operating Model",
  openingTransitionPhase: "Opening / Transition Phase",
  projectType: "Project Type",
};

export const LOCATION_FIELDS = {
  primaryMarketRegion: "Primary Market Region",
  country: "Country",
};

export const SI_FIELDS = {
  preferredFutureOperatingModel: "Preferred Future Operating Model",
  operatorStrategyStatus: "Operator Strategy Status",
  operatorCapabilityPriorities: "Operator Capability Priorities",
  ownerReportingPackage: "Owner Reporting Package",
  ownerReportingFrequency: "Owner Reporting Frequency",
  planSelfManage: "Plan to Self-Manage or Hire Third Party?",
  servicesRequired: "Services Required From Operator",
  preferredReportingFrequency: "Preferred Reporting Frequency",
  whoReceivesBids: "Who should receive bids for this project?",
  whoReceivesBidsAirtable: "Who Should Receive Bids for This Project?",
};

export const MP_FIELDS = {
  preferredDealStructure: "Preferred Deal Structure",
};

export const LEGACY_DEAL_BRAND_FIELDS = {
  currentlyBranded: "Is the hotel currently branded?",
  currentlyManaged: "Is the hotel currently managed by a third-party operator?",
  operatorNameCurrent: "Operator Name Current",
};

/** All P0 form keys (for routing / classification). */
export const P0_DEALS_FORM_FIELDS = [
  DEALS_FIELDS.currentOperatingModel,
  DEALS_FIELDS.openingTransitionPhase,
  DEALS_FIELDS.projectType,
];

export const P0_LOCATION_FORM_FIELDS = [LOCATION_FIELDS.primaryMarketRegion];

export const P0_SI_FORM_FIELDS = [
  SI_FIELDS.preferredFutureOperatingModel,
  SI_FIELDS.operatorStrategyStatus,
  SI_FIELDS.operatorCapabilityPriorities,
  SI_FIELDS.ownerReportingPackage,
  SI_FIELDS.ownerReportingFrequency,
];

export const CURRENT_OPERATING_MODEL_OPTIONS = [
  "Owner-operated (unbranded)",
  "Owner-operated (branded/franchised)",
  "Third-party managed (branded)",
  "Third-party managed (independent/collection)",
  "Brand-managed",
  "Lease/operator lease structure",
  "Mixed/transitioning",
  NEEDS_REVIEW,
  "Unknown",
];

export const PREFERRED_FUTURE_OPERATING_MODEL_OPTIONS = [
  "Owner-operated",
  "Third-party management only",
  "Franchise/license only (owner or third-party operator)",
  "Brand + third-party management",
  "Brand-managed",
  "Lease structure",
  "Undecided / exploring",
  NEEDS_REVIEW,
];

export const OPERATOR_STRATEGY_STATUS_OPTIONS = [
  "Not seeking operator input",
  "Exploring capabilities only",
  "Building shortlist for advisor review",
  "Ready for structured operator review",
  "Already in discussions",
  NEEDS_REVIEW,
];

export const OPERATOR_CAPABILITY_PRIORITY_OPTIONS = [
  "Full hotel management",
  "Pre-opening / opening support",
  "Conversion & PIP execution",
  "Revenue management & distribution",
  "Accounting & owner reporting",
  "Procurement & cost control",
  "F&B / culinary operations",
  "Sales & marketing",
  "HR & training",
  "Technology & systems",
  "Design / renovation PM",
  "Asset management / capex planning",
  "Local market / CALA execution",
  "Lifestyle / experience programming",
  "Crisis / business continuity",
];

export const OWNER_REPORTING_PACKAGE_OPTIONS = [
  "Weekly financial",
  "Monthly P&L",
  "Monthly operating metrics",
  "Quarterly board pack",
  "Annual budget / forecast",
  "Owner portal access",
  "On-demand / ad hoc",
  "Third-party audit support",
];

export const OWNER_REPORTING_FREQUENCY_OPTIONS = [
  "Weekly",
  "Bi-weekly",
  "Monthly",
  "Quarterly",
  "Ad hoc",
];

export const OPENING_TRANSITION_PHASE_OPTIONS = [
  "N/A (stabilized operating)",
  "Planning / entitlement",
  "Pre-construction",
  "Construction",
  "Pre-opening ramp",
  "Soft opening",
  "Reopening after renovation",
  "Rebranding in place",
  NEEDS_REVIEW,
];

export const PRIMARY_MARKET_REGION_OPTIONS = [
  "North America",
  "CALA",
  "Europe",
  "MEA",
  "APAC",
  "Multi-region",
  NEEDS_REVIEW,
];

/** Re-export canonical Project Type options (see lib/project-type.js). */
export {
  PROJECT_TYPE_CANONICAL_OPTIONS as PROJECT_TYPE_ADDITIONAL_OPTIONS,
  PROJECT_TYPE_CANONICAL_OPTIONS,
} from "./project-type.js";

export const CALA_COUNTRIES = new Set(
  [
    "Mexico",
    "Jamaica",
    "Dominican Republic",
    "Puerto Rico",
    "Cuba",
    "Bahamas",
    "Aruba",
    "Curaçao",
    "Cayman Islands",
    "Trinidad and Tobago",
    "Barbados",
    "Haiti",
    "Colombia",
    "Brazil",
    "Argentina",
    "Chile",
    "Peru",
    "Ecuador",
    "Costa Rica",
    "Panama",
    "Guatemala",
    "Honduras",
    "El Salvador",
    "Nicaragua",
    "Venezuela",
    "Uruguay",
    "Paraguay",
    "Bolivia",
  ].map((c) => c.toLowerCase())
);

/** Legacy Services Required → Operator Capability Priorities */
export const SERVICES_TO_PRIORITIES = {
  "Full Management": "Full hotel management",
  "Revenue Management": "Revenue management & distribution",
  "Accounting & Reporting": "Accounting & owner reporting",
  "Sales & Marketing": "Sales & marketing",
  "HR & Training": "HR & training",
};

/** Legacy Preferred Reporting Frequency → Owner Reporting Frequency (same labels). */
export const REPORTING_FREQUENCY_MAP = {
  Weekly: "Weekly",
  Monthly: "Monthly",
  Quarterly: "Quarterly",
  "As Needed": "Ad hoc",
};

/**
 * @param {unknown} val
 * @returns {string}
 */
export function strVal(val) {
  if (val == null || val === "") return "";
  if (typeof val === "string") return val.trim();
  if (typeof val === "number" && Number.isFinite(val)) return String(val);
  if (Array.isArray(val)) {
    return val
      .map((x) => (typeof x === "string" ? x : x && x.name ? String(x.name) : ""))
      .filter(Boolean)
      .join(", ");
  }
  if (typeof val === "object" && val.name) return String(val.name).trim();
  return String(val).trim();
}

/**
 * @param {unknown} val
 * @returns {string[]}
 */
export function listVal(val) {
  if (val == null || val === "") return [];
  if (Array.isArray(val)) {
    return val
      .map((x) => (typeof x === "string" ? x.trim() : x && x.name ? String(x.name).trim() : ""))
      .filter(Boolean);
  }
  if (typeof val === "string") {
    return val
      .split(/\s*,\s*/)
      .map((s) => s.trim())
      .filter(Boolean);
  }
  return [strVal(val)].filter(Boolean);
}

/**
 * @param {Record<string, unknown>} fields — merged deal fields
 * @returns {boolean}
 */
export function isOperatorInScopeFromFields(fields) {
  const f = fields || {};
  const bids = strVal(
    f[SI_FIELDS.whoReceivesBids] || f[SI_FIELDS.whoReceivesBidsAirtable]
  ).toLowerCase();
  if (bids.includes("third-party operators only") || bids.includes("both brands")) {
    return true;
  }
  const preferred = strVal(f[SI_FIELDS.preferredFutureOperatingModel]);
  if (
    /third.party|brand \+ third|brand\+third/i.test(preferred) &&
    !/franchise\/license only/i.test(preferred)
  ) {
    return true;
  }
  const plan = strVal(f[SI_FIELDS.planSelfManage]);
  if (/third.party|third-party managed/i.test(plan)) return true;
  const dealStruct = strVal(f[MP_FIELDS.preferredDealStructure]);
  if (/third.party management|brand \+ third/i.test(dealStruct)) return true;
  return false;
}

/**
 * Infer Primary Market Region from country (conservative).
 * @param {string} country
 * @returns {string}
 */
export function inferPrimaryMarketRegionFromCountry(country) {
  const c = strVal(country).toLowerCase();
  if (!c) return "";
  if (CALA_COUNTRIES.has(c)) return "CALA";
  if (
    ["united states", "usa", "u.s.", "canada", "united states of america"].some((x) =>
      c.includes(x)
    )
  ) {
    return "North America";
  }
  if (
    [
      "united kingdom",
      "france",
      "germany",
      "spain",
      "italy",
      "portugal",
      "netherlands",
      "ireland",
      "switzerland",
    ].some((x) => c === x || c.includes(x))
  ) {
    return "Europe";
  }
  return NEEDS_REVIEW;
}
