/**
 * Mixed-use / branded-residence intake — field names and select options.
 * Schema setup: scripts/setup-deals-schema-mixed-use-intake.mjs
 * Routing: api/schemas/deal-setup-fields.js
 */

export const MIXED_USE_INTAKE_FIELD_NAMES = Object.freeze({
  numberOfCondoUnits: "Number of Condo / Residence Units",
  brandedResidenceProgramModel: "Branded Residence Program Model",
  condoRentalProgramModel: "Condo Rental Program Model",
  fbOperatingModel: "F&B Operating Model",
  stabilizedAdrUsd: "Stabilized ADR (USD)",
  stabilizedOccupancyPct: "Stabilized Occupancy (%)",
  developmentProformaAvailable: "Development Proforma Available?",
});

export const BRANDED_RESIDENCE_PROGRAM_MODEL_OPTIONS = [
  "Hotel-Branded Residences (Integrated With Hotel)",
  "Branded Residences + Independent Condo Units (Same Building)",
  "Condo-Hotel / Rental Pool Integrated",
  "Residences Only — Hotel Component Separate",
  "To Be Defined With Operator",
];

export const CONDO_RENTAL_PROGRAM_MODEL_OPTIONS = [
  "Operator-Managed Rental Pool (Optional Owner Participation)",
  "Operator-Managed Rental Pool (Mandatory Participation)",
  "Owner-Managed Rental Pool",
  "No Rental Program (Owner Use Only)",
  "To Be Defined With Operator",
];

export const FB_OPERATING_MODEL_OPTIONS = [
  "Hotel-Operated",
  "Third-Party Lease (Independent Restaurateur)",
  "Third-Party Management Agreement (Hotel-Branded)",
  "Hybrid (Hotel Core + Leased Flagship Restaurant)",
  "Not Applicable",
];

export const DEVELOPMENT_PROFORMA_AVAILABLE_OPTIONS = ["Yes", "No", "In Progress"];

/**
 * Form (Proper Case) → live Airtable single-select option text.
 * Remove entries after renaming choices in Airtable UI to match Proper Case.
 */
export const MIXED_USE_SELECT_FORM_TO_AIRTABLE = Object.freeze({
  "Hotel-Branded Residences (Integrated With Hotel)": "Hotel-branded residences (integrated with hotel)",
  "Branded Residences + Independent Condo Units (Same Building)":
    "Branded residences + independent condo units (same building)",
  "Condo-Hotel / Rental Pool Integrated": "Condo-hotel / rental pool integrated",
  "Residences Only — Hotel Component Separate": "Residences only — hotel component separate",
  "To Be Defined With Operator": "To be defined with operator",
  "Operator-Managed Rental Pool (Optional Owner Participation)":
    "Operator-managed rental pool (optional owner participation)",
  "Operator-Managed Rental Pool (Mandatory Participation)":
    "Operator-managed rental pool (mandatory participation)",
  "Owner-Managed Rental Pool": "Owner-managed rental pool",
  "No Rental Program (Owner Use Only)": "No rental program (owner use only)",
  "Hotel-Operated": "Hotel-operated",
  "Third-Party Lease (Independent Restaurateur)": "Third-party lease (independent restaurateur)",
  "Third-Party Management Agreement (Hotel-Branded)": "Third-party management agreement (hotel-branded)",
  "Hybrid (Hotel Core + Leased Flagship Restaurant)": "Hybrid (hotel core + leased flagship restaurant)",
  "Not Applicable": "Not applicable",
  "In Progress": "In progress",
});

/** @type {Record<string, string>} */
export const MIXED_USE_SELECT_AIRTABLE_TO_FORM = Object.freeze(
  Object.fromEntries(Object.entries(MIXED_USE_SELECT_FORM_TO_AIRTABLE).map(([form, at]) => [at, form]))
);

const MIXED_USE_SELECT_FORM_FIELDS = new Set([
  MIXED_USE_INTAKE_FIELD_NAMES.brandedResidenceProgramModel,
  MIXED_USE_INTAKE_FIELD_NAMES.condoRentalProgramModel,
  MIXED_USE_INTAKE_FIELD_NAMES.fbOperatingModel,
  MIXED_USE_INTAKE_FIELD_NAMES.developmentProformaAvailable,
]);

/** @param {string} formName @param {unknown} val */
export function coerceMixedUseSelectForWrite(formName, val) {
  if (!MIXED_USE_SELECT_FORM_FIELDS.has(formName)) return val;
  const s = val == null ? "" : String(val).trim();
  if (!s) return val;
  return MIXED_USE_SELECT_FORM_TO_AIRTABLE[s] ?? s;
}

/** @param {string} formName @param {unknown} val */
export function coerceMixedUseSelectForRead(formName, val) {
  if (!MIXED_USE_SELECT_FORM_FIELDS.has(formName)) return val;
  const s = val == null ? "" : String(val).trim();
  if (!s) return val;
  return MIXED_USE_SELECT_AIRTABLE_TO_FORM[s] ?? s;
}

/** All mixed-use intake form field names (for routing audits). */
export const MIXED_USE_INTAKE_FORM_FIELDS = Object.values(MIXED_USE_INTAKE_FIELD_NAMES);

/**
 * Whether mixed-use intake fields apply to this deal.
 * @param {Record<string, unknown>} fields
 */
export function isMixedUseIntakeInScopeFromFields(fields) {
  const pt = String(fields?.["Project Type"] ?? "").trim();
  const condo = String(fields?.["Condo Residences?"] ?? "").trim();
  const hotelType = String(fields?.["Hotel Type"] ?? "").trim();
  return (
    pt === "Mixed-Use Hospitality Project" ||
    condo === "Yes" ||
    hotelType === "Branded Residences / Condo Hotel"
  );
}

/**
 * Conditional required fields when mixed-use intake is in scope.
 * @param {Record<string, unknown>} fields
 * @returns {string[]}
 */
export function mixedUseIntakeConditionalRequiredFields(fields) {
  if (!isMixedUseIntakeInScopeFromFields(fields)) return [];

  const out = [
    MIXED_USE_INTAKE_FIELD_NAMES.brandedResidenceProgramModel,
    MIXED_USE_INTAKE_FIELD_NAMES.stabilizedAdrUsd,
    MIXED_USE_INTAKE_FIELD_NAMES.stabilizedOccupancyPct,
    MIXED_USE_INTAKE_FIELD_NAMES.developmentProformaAvailable,
  ];

  const condo = String(fields?.["Condo Residences?"] ?? "").trim();
  if (condo === "Yes") {
    out.push(
      MIXED_USE_INTAKE_FIELD_NAMES.numberOfCondoUnits,
      MIXED_USE_INTAKE_FIELD_NAMES.condoRentalProgramModel
    );
  }

  const fb = String(fields?.["F&B Outlets?"] ?? "").trim();
  if (fb === "Yes") {
    out.push(MIXED_USE_INTAKE_FIELD_NAMES.fbOperatingModel);
  }

  return out;
}

/** @param {string} formName @param {unknown} val */
export function coerceMixedUseMpFieldForWrite(formName, val) {
  if (formName === MIXED_USE_INTAKE_FIELD_NAMES.stabilizedAdrUsd) {
    const num = typeof val === "number" ? val : parseFloat(String(val).replace(/[,$]/g, ""));
    return Number.isFinite(num) ? num : val;
  }
  if (formName === MIXED_USE_INTAKE_FIELD_NAMES.stabilizedOccupancyPct) {
    const num = typeof val === "number" ? val : parseFloat(String(val).replace(/%/g, ""));
    if (!Number.isFinite(num)) return val;
    return num > 1 ? num / 100 : num;
  }
  return val;
}

/** @param {string} formName @param {unknown} val */
export function coerceMixedUseMpFieldForRead(formName, val) {
  if (formName === MIXED_USE_INTAKE_FIELD_NAMES.stabilizedOccupancyPct) {
    const num = typeof val === "number" ? val : parseFloat(String(val));
    if (!Number.isFinite(num)) return val;
    return num <= 1 ? Math.round(num * 1000) / 10 : num;
  }
  return val;
}
