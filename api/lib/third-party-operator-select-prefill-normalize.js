/**
 * Map Airtable single-select values (incl. legacy Basics options) → intake form `<option value>` strings
 * for Operator Setup New / New Two (must match public HTML exactly).
 */

function trimStr(v) {
  if (v == null || v === "") return "";
  return String(v).trim();
}

/** Normalize fancy dashes to ASCII hyphen for comparison with form markup. */
function normalizeDashes(s) {
  return trimStr(s).replace(/\u2013/g, "-").replace(/\u2014/g, "-");
}

const PRIMARY_SERVICE_MODEL_TO_FORM = {
  "Third-party management": "Mixed",
  "Full service": "Full-service focus",
  "Full Service": "Full-service focus",
  "Select service": "Select-service focus",
  "Select Service": "Select-service focus",
};

const COMPANY_SIZE_TO_FORM = {
  "26-50 properties": "Medium (10-50 properties)",
  "26–50 properties": "Medium (10-50 properties)",
};

/** Single select `ownerPortal` — must match Airtable and public operator HTML option values exactly. */
export const OWNER_PORTAL_FORM_VALUES = [
  "Yes - Real-time data",
  "Yes - Daily updates",
  "Yes - Weekly updates",
  "No",
  "Planned",
];

const OWNER_PORTAL_LEGACY_TO_FORM = {
  "Yes - Real-Time Data": "Yes - Real-time data",
  "Yes - Daily Updates": "Yes - Daily updates",
  "Yes - Weekly Updates": "Yes - Weekly updates",
  "Yes - Periodic updates": "Yes - Weekly updates",
  Yes: "Yes - Real-time data", // bare "Yes" from legacy tests / bad payloads
};

/**
 * Map Airtable / legacy strings → form option value for Owner Portal.
 * @param {unknown} raw
 * @returns {string} One of {@link OWNER_PORTAL_FORM_VALUES}, or "" if unknown / empty.
 */
export function normalizeOwnerPortalForForm(raw) {
  const s0 = normalizeDashes(trimStr(raw));
  if (!s0) return "";
  if (OWNER_PORTAL_FORM_VALUES.includes(s0)) return s0;
  if (OWNER_PORTAL_LEGACY_TO_FORM[s0] !== undefined) return OWNER_PORTAL_LEGACY_TO_FORM[s0] || "";
  const lower = s0.toLowerCase();
  for (const opt of OWNER_PORTAL_FORM_VALUES) {
    if (opt.toLowerCase() === lower) return opt;
  }
  return "";
}

/** Must match `caseStudyRowTemplate` `.cs-situation` in `third-party-operator-setup-new-two.html`. */
export const CASE_STUDY_SITUATION_FORM_VALUES = [
  "Pre-Opening",
  "Conversion",
  "Transition",
  "Turnaround",
  "Repositioning",
  "Stabilized",
];

/**
 * Map Airtable single-select (or legacy long text) → form option value for case study Situation.
 * @param {unknown} raw
 * @returns {string} One of {@link CASE_STUDY_SITUATION_FORM_VALUES}, or "" if unknown.
 */
export function normalizeCaseStudySituationForForm(raw) {
  const s0 = normalizeDashes(trimStr(raw));
  if (!s0) return "";
  if (CASE_STUDY_SITUATION_FORM_VALUES.includes(s0)) return s0;
  const lower = s0.toLowerCase();
  for (const opt of CASE_STUDY_SITUATION_FORM_VALUES) {
    if (opt.toLowerCase() === lower) return opt;
  }
  // Legacy free-text / old labels → closest enum (same idea as scripts/fix-case-study-dropdown-values.mjs)
  if (/pre[-\s]?opening|new\s*build|soft\s*open/i.test(s0)) return "Pre-Opening";
  if (/\bconversion\b|reflag|re-flag|from\s+independent/i.test(lower)) return "Conversion";
  if (/\btransition\b|handover|management\s+change|operator\s+change|takeover/i.test(lower)) return "Transition";
  if (/\bturnaround\b|underperform|distress|reinstatement/i.test(lower)) return "Turnaround";
  if (/\breposition|pip-heavy|\bpip\b|rebrand/i.test(lower)) return "Repositioning";
  if (/\bstabiliz|stabilised|steady\s*state|operating\s+asset/i.test(lower)) return "Stabilized";
  return "";
}

/**
 * Normalize `prefill.caseStudiesDetail[].situation` for edit prefill.
 * @param {Record<string, unknown>} prefill Mutated in place.
 */
export function normalizeCaseStudiesDetailSituationsInPrefill(prefill) {
  if (!prefill || !Array.isArray(prefill.caseStudiesDetail)) return;
  for (const row of prefill.caseStudiesDetail) {
    if (!row || typeof row !== "object") continue;
    const n = normalizeCaseStudySituationForForm(row.situation);
    if (n) row.situation = n;
  }
}

/**
 * @param {Record<string, unknown>} prefill Mutated in place.
 */
export function normalizeOperatorSetupSelectPrefill(prefill) {
  if (!prefill || typeof prefill !== "object") return;

  const psmRaw = trimStr(prefill.primaryServiceModel);
  if (psmRaw) {
    const psmDash = normalizeDashes(psmRaw);
    prefill.primaryServiceModel =
      PRIMARY_SERVICE_MODEL_TO_FORM[psmRaw] || PRIMARY_SERVICE_MODEL_TO_FORM[psmDash] || psmDash;
  }

  const csRaw = trimStr(prefill.companySize);
  if (csRaw) {
    const csDash = normalizeDashes(csRaw);
    prefill.companySize = COMPANY_SIZE_TO_FORM[csRaw] || COMPANY_SIZE_TO_FORM[csDash] || csDash;
  }

  prefill.ownerPortal = normalizeOwnerPortalForForm(prefill.ownerPortal);

  normalizeCaseStudiesDetailSituationsInPrefill(prefill);
}
