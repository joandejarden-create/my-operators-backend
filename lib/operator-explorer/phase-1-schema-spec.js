/**
 * Phase 1 Airtable schema specs for Operator Explorer foundation.
 * Minimal calibration-approved fields only.
 */

export const MASTER_TABLE = "Operator Setup - Master";
export const MASTER_TABLE_ID = "tbl4YPJ3XhnYLHLsD";
export const CLAIMS_TABLE = "Operator Intelligence - Claims";
export const CLAIMS_TABLE_ID = "tblZE18CKPISe1Dcs";
export const PRESENCE_TABLE = "Operator Intelligence - Market Presence";
export const PRESENCE_TABLE_ID = "tblrFqjMNGzxzbZnu";
export const PI_SOURCE_TABLE = "Partner Intelligence - Source Library";
export const PI_SOURCE_TABLE_ID = "tbl8iR7AtkUe0uctp";

export const ASSIGNMENTS_TABLE = "Operator Intelligence - Assignments";
export const BRAND_REL_INTEL_TABLE = "Operator Intelligence - Brand Relationships";

export const OPERATING_MODEL_OPTIONS = [
  "Third-Party",
  "Brand / Operator",
  "Integrated Brand / Operator",
  "Integrated Owner / Brand / Operator",
  "Owner-Operator",
  "Asset Manager",
  "Hybrid",
  "To Be Confirmed",
];

export const MANAGEMENT_AVAILABILITY_OPTIONS = [
  "Confirmed Direct Management",
  "Conditional / Scoped",
  "No Direct Management Identified",
  "Unknown",
];

export const RECORD_PURPOSE_OPTIONS = ["Production", "Research", "Test Fixture"];

export const ASSIGNMENT_STATUS_OPTIONS = ["Current", "Historical", "Announced / Upcoming", "Unclear"];

export const DEVELOPMENT_CONTEXT_OPTIONS = [
  "New Build",
  "Conversion",
  "Reflag",
  "Repositioning",
  "Turnaround",
  "Renovation",
  "Acquisition Transition",
  "Existing Operation / Takeover",
  "Unknown",
];

export const OPERATING_STRUCTURE_OPTIONS = [
  "Third-Party Management",
  "Franchise + Operator",
  "Franchise Only",
  "Owner-Operated",
  "Lease",
  "Asset Management",
  "Brand-managed",
  "To Be Confirmed",
];

export const URBAN_RESORT_OPTIONS = ["Urban", "Resort", "Mixed", "Unknown"];

export const RELATIONSHIP_TYPE_OPTIONS = [
  "Currently Operates",
  "Historically Operated",
  "Announced Assignment",
  "Development Relationship",
  "Corporate Relationship",
  "Brand Managed Capability",
  "Explicit Approved Operator",
  "Other",
];

export const CURRENT_HISTORICAL_OPTIONS = ["Current", "Historical"];

export const PUBLICATION_STATUS_OPTIONS = [
  "Auto-Publish",
  "Publish With Evidence Label",
  "Internal / Validation Required",
  "Insufficient Support",
  "Conflict / Exception",
];

export const EVIDENCE_CLASS_OPTIONS = [
  "primary_authoritative",
  "reliable_independent",
  "operator_reported",
  "referenced",
  "weak",
];

function choices(names) {
  return names.map((name) => ({ name }));
}

function single(name, options) {
  return { name, type: "singleSelect", options: { choices: choices(options) } };
}

function multi(name, options) {
  return { name, type: "multipleSelects", options: { choices: choices(options) } };
}

function text(name) {
  return { name, type: "singleLineText" };
}

function longText(name) {
  return { name, type: "multilineText" };
}

function number(name, precision = 0) {
  return { name, type: "number", options: { precision } };
}

function date(name) {
  return { name, type: "date", options: { dateFormat: { name: "iso" } } };
}

function checkbox(name) {
  return { name, type: "checkbox", options: { color: "greenBright", icon: "check" } };
}

function link(name, linkedTableId) {
  return {
    name,
    type: "multipleRecordLinks",
    options: { linkedTableId },
  };
}

/** New Master fields (create if missing). */
export const MASTER_FIELD_SPECS = [
  single("Record Purpose", RECORD_PURPOSE_OPTIONS),
  single("Operating Model", OPERATING_MODEL_OPTIONS),
  single("Management Availability", MANAGEMENT_AVAILABILITY_OPTIONS),
  text("Operator Aliases"),
  text("Operator Website"),
  text("Operator Parent Company"),
];

/** Claims extensions. */
export const CLAIMS_FIELD_SPECS = [
  link("PI Source Library", PI_SOURCE_TABLE_ID),
];

/** Market Presence minor additions. */
export const PRESENCE_FIELD_SPECS = [
  text("City / Metro"),
  number("Verified Assignment Count", 0),
];

/** Full Assignments table field list (first field = primary). */
export function assignmentsTableFields() {
  return [
    text("Assignment ID"),
    link("Operator", MASTER_TABLE_ID),
    text("Property Name"),
    text("Canonical Property Name"),
    text("Country"),
    text("City / Metro"),
    text("Region"),
    text("Brand"),
    text("Brand Parent"),
    number("Keys / Rooms", 0),
    text("Chain Scale"),
    text("Segment"),
    text("Hotel Type"),
    single("Urban / Resort", URBAN_RESORT_OPTIONS),
    single("Development Context", DEVELOPMENT_CONTEXT_OPTIONS),
    single("Operating / Management Structure", OPERATING_STRUCTURE_OPTIONS),
    single("Assignment Status", ASSIGNMENT_STATUS_OPTIONS),
    date("Assignment Start Date"),
    date("Assignment End Date"),
    text("Owner / Developer"),
    checkbox("All-Inclusive"),
    checkbox("Branded Residences"),
    checkbox("Extended Stay"),
    checkbox("Mixed-Use"),
    checkbox("Meetings / Convention"),
    date("Last Verified"),
    link("PI Source Library", PI_SOURCE_TABLE_ID),
    longText("Source URLs"),
    single("Evidence Class", EVIDENCE_CLASS_OPTIONS),
    single("Publication Status", PUBLICATION_STATUS_OPTIONS),
    text("Conflict Status"),
    longText("Limitations"),
    text("Research Wave"),
    text("Why Comparable"),
    text("Comparability Strength"),
  ];
}

export function brandRelationshipTableFields() {
  return [
    text("Brand Relationship ID"),
    link("Operator", MASTER_TABLE_ID),
    text("Brand"),
    text("Brand Parent"),
    single("Relationship Type", RELATIONSHIP_TYPE_OPTIONS),
    single("Current / Historical", CURRENT_HISTORICAL_OPTIONS),
    text("Geography Scope"),
    text("Region Scope"),
    text("Segment Scope"),
    text("Hotel Type Scope"),
    text("Third-Party Owner Availability"),
    longText("Evidence"),
    longText("Source URLs"),
    link("PI Source Library", PI_SOURCE_TABLE_ID),
    single("Publication Status", PUBLICATION_STATUS_OPTIONS),
    text("Conflict Status"),
    longText("Limitations"),
    date("Last Verified"),
    text("Research Wave"),
  ];
}

export const NEW_MASTER_CREATE_PLAN = [
  {
    provisionalId: "provisional_operator_hyatt",
    company_name: "Hyatt (Managed)",
    aliases: "Hyatt; Hyatt Hotels Corporation",
    parent: "Hyatt Hotels Corporation",
    website: "https://www.hyatt.com",
    operatingModel: "Hybrid",
    managementAvailability: "Confirmed Direct Management",
  },
  {
    provisionalId: "provisional_operator_sonesta",
    company_name: "Sonesta International",
    aliases: "Sonesta",
    parent: "Sonesta",
    website: "https://www.sonesta.com",
    operatingModel: "Brand / Operator",
    managementAvailability: "Confirmed Direct Management",
  },
  {
    provisionalId: "provisional_operator_four_seasons",
    company_name: "Four Seasons Hotels and Resorts",
    aliases: "Four Seasons",
    parent: "Four Seasons",
    website: "https://www.fourseasons.com",
    operatingModel: "Brand / Operator",
    managementAvailability: "Confirmed Direct Management",
  },
  {
    provisionalId: "provisional_operator_rosewood",
    company_name: "Rosewood Hotel Group",
    aliases: "Rosewood",
    parent: "Rosewood",
    website: "https://www.rosewoodhotels.com",
    operatingModel: "Brand / Operator",
    managementAvailability: "Confirmed Direct Management",
  },
  {
    provisionalId: "provisional_operator_mandarin_oriental",
    company_name: "Mandarin Oriental Hotel Group",
    aliases: "MOHG; Mandarin Oriental",
    parent: "MOHG",
    website: "https://www.mandarinoriental.com",
    operatingModel: "Integrated Brand / Operator",
    managementAvailability: "Conditional / Scoped",
  },
  {
    provisionalId: "provisional_operator_radisson",
    company_name: "Radisson Hotel Group",
    aliases: "RHG; Radisson",
    parent: "RHG",
    website: "https://www.radissonhotels.com",
    operatingModel: "Hybrid",
    managementAvailability: "Conditional / Scoped",
  },
  {
    provisionalId: "provisional_operator_melia",
    company_name: "Meliá Hotels International",
    aliases: "Melia; Meliá",
    parent: "Meliá",
    website: "https://www.melia.com",
    operatingModel: "Hybrid",
    managementAvailability: "Conditional / Scoped",
  },
  {
    provisionalId: "provisional_operator_auberge",
    company_name: "Auberge Resorts Collection",
    aliases: "Auberge",
    parent: "Auberge",
    website: "https://www.aubergeresorts.com",
    operatingModel: "Brand / Operator",
    managementAvailability: "Confirmed Direct Management",
  },
  {
    provisionalId: "provisional_operator_shangri_la",
    company_name: "Shangri-La Group",
    aliases: "Shangri-La",
    parent: "Shangri-La Asia",
    website: "https://www.shangri-la.com",
    operatingModel: "Integrated Brand / Operator",
    managementAvailability: "Conditional / Scoped",
  },
  {
    provisionalId: "provisional_operator_barcelo",
    company_name: "Barceló Hotel Group",
    aliases: "Barcelo; Barceló",
    parent: "Barceló",
    website: "https://www.barcelo.com",
    operatingModel: "Integrated Owner / Brand / Operator",
    managementAvailability: "Conditional / Scoped",
  },
];

/** Calibration entities with Operating Model / MA for Master updates. */
export function entityOmMaFromEntitiesJson(entities) {
  const out = {};
  for (const e of entities || []) {
    const id = e.existingMasterId || e.entityId;
    if (!id || String(id).startsWith("provisional_")) continue;
    out[id] = {
      operatingModel: e.operatingModel,
      managementAvailability: e.managementAvailability,
      website: e.website || null,
      aliases: (e.aliases || []).join("; "),
      parent: e.parent || null,
    };
  }
  return out;
}

export function mapPublicationClass(pc) {
  const s = String(pc || "");
  if (/objective verified/i.test(s) || s === "Auto-Publish") return "Auto-Publish";
  if (/evidence qualification|Publish With Evidence/i.test(s)) return "Publish With Evidence Label";
  if (/Internal/i.test(s)) return "Internal / Validation Required";
  if (/Insufficient/i.test(s)) return "Insufficient Support";
  if (/Conflict/i.test(s)) return "Conflict / Exception";
  return "Publish With Evidence Label";
}

export function isAggregateAssignmentName(name) {
  const n = String(name || "");
  return /representative|examples\b|enterprise\b|various\b/i.test(n);
}
