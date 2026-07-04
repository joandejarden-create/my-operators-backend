/**
 * Map Project Fit form-shaped objects → Airtable column names (Brand Setup - Project Fit).
 * Mirrors api/brand-library.js updateProjectFitByBrandId field mapping.
 */
import { buildProjectFitFormForBrand } from "./choice-project-fit-profiles.mjs";

const SCALAR_MAP = {
  idealRoomCountMin: "Min - Room Count",
  idealRoomCountMax: "Max - Room Count",
  idealProjectSizeMin: "Min - Ideal Project Size",
  idealProjectSizeMax: "Max - Ideal Project Size",
  minReqOperatorExperienceYears: "Req Operator Exp",
  minLeadTimeMonths: "Min Lead Time",
  preferredOwnerType: "Preferred Owner/Investor Type",
  coBrandingAllowed: "Co-Branding Allowed",
  brandedResidencesAllowed: "Branded Residences Allowed",
  mixedUseAllowed: "Mixed-Use Development Allowed",
  priorityMarketsOther: "Other - Priority Markets Text",
  marketsToAvoidOther: "Other - Markets to Avoid Text",
  milestoneOperatorSelectionMinMonths: "Discussion to Selection - Target Milestones",
  milestoneConstructionStartMinMonths: "Selection to Construction - Target Milestones",
  milestoneSoftOpeningMinMonths: "PreOpen to SoftOpen - Target Milestones",
  milestoneGrandOpeningMinMonths: "SoftOpen to GrandOpen - Target Milestones",
  dateFlexibility: "Flexibility On Dates",
  pipRepositioningDetails: "Typical PIP / Repositioning Profile You Will Consider (If Existing Hotel)",
  ownerHotelExperience: "Owner / Sponsor Hotel Experience",
  ownerNonNegotiableOther: "Other (Text) - Owner Non-Negotiables",
  ownerNonNegotiables: "Owner Non-Negotiables & Decision Rights",
  capexSupport: "CapEx and FF&E Support",
  knownRedFlags:
    "Red Flag Items That Typically Make You Decline or Proceed With Caution - Risk & Compliance",
  esgExpectations: "ESG / Sustainability Expectations You Prefer Projects to Meet - Risk & Compliance",
  idealProjectsAdditionalNotes: "Anything else about your commercial 'sweet spot' we should know?",
  typicalPIPRange: "Typical PIP Range ($/room or %)",
  whoPaysForPIP: "Who Pays for PIP",
};

const MULTI_MAP = {
  idealProjectTypes: "Acceptable Project Type",
  idealBuildingTypes: "Acceptable Building Types",
  idealAgreementTypes: "Acceptable Agreements Type",
  projectStage: "Acceptable Project Stages",
  ownerInvolvementLevel: "Acceptable Owner Involvement Levels",
  ownerNonNegotiableTypes: "Owner Non-Negotiables",
  capitalStatus: "Acceptable Capital Status at Engagement",
  brandStatus: "Brand Status Scenarios You Will Consider",
  feeExpectationVsMarket: "Acceptable Fee Expectations vs Market",
  exitHorizon: "Acceptable Exit Horizon",
  priorityMarkets: "Priority Markets",
  marketsToAvoid: "Markets to Avoid",
};

const NUMERIC = new Set([
  "idealRoomCountMin",
  "idealRoomCountMax",
  "idealProjectSizeMin",
  "idealProjectSizeMax",
  "minReqOperatorExperienceYears",
  "minLeadTimeMonths",
  "milestoneOperatorSelectionMinMonths",
  "milestoneConstructionStartMinMonths",
  "milestoneSoftOpeningMinMonths",
  "milestoneGrandOpeningMinMonths",
]);

const YES_NO = new Set(["coBrandingAllowed", "brandedResidencesAllowed", "mixedUseAllowed"]);

/**
 * @param {Record<string, unknown>} form
 * @returns {Record<string, unknown>}
 */
export function projectFitFormToAirtableFields(form) {
  /** @type {Record<string, unknown>} */
  const fields = {};

  for (const [formKey, col] of Object.entries(SCALAR_MAP)) {
    const val = form[formKey];
    if (val == null || val === "") continue;
    if (NUMERIC.has(formKey)) {
      const num = typeof val === "number" ? val : parseFloat(String(val));
      if (!Number.isNaN(num)) fields[col] = num;
    } else {
      fields[col] = String(val).trim();
    }
  }

  for (const formKey of YES_NO) {
    const val = form[formKey];
    if (val == null || val === "") continue;
    const col = SCALAR_MAP[formKey];
    const s = String(val).trim();
    fields[col] = /^(yes|true|1)$/i.test(s) ? "Yes" : /^(no|false|0)$/i.test(s) ? "No" : s;
  }

  for (const [formKey, col] of Object.entries(MULTI_MAP)) {
    const val = form[formKey];
    if (val == null) continue;
    const arr = Array.isArray(val) ? val : [val];
    const cleaned = arr.map((s) => String(s).trim()).filter(Boolean);
    fields[col] = cleaned;
  }

  return fields;
}

/**
 * @param {string} brandName
 * @returns {Record<string, unknown>|null}
 */
export function buildAirtableFieldsForBrand(brandName) {
  const form = buildProjectFitFormForBrand(brandName);
  if (!form) return null;
  return projectFitFormToAirtableFields(form);
}
