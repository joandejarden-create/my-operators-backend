/**
 * Derive whether an operator is branded-residence / mixed-use hospitality capable
 * for Operator Explorer list filters and badges.
 */
import { formatListValue } from "./third-party-operator-value-utils.js";

function fieldStr(fields, ...keys) {
  const f = fields || {};
  for (const k of keys) {
    const v = formatListValue(f[k]);
    if (v) return v;
  }
  return "";
}

function parseServiceModels(profileFields, platformFields) {
  const raw =
    profileFields?.serviceModelsSupported ||
    profileFields?.["Service Models Supported"] ||
    platformFields?.serviceModelsSupported ||
    platformFields?.["Service Models Supported"] ||
    "";
  if (Array.isArray(raw)) return raw.map((x) => formatListValue(x)).filter(Boolean);
  return String(raw || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

function positiveCount(fields, ...keys) {
  for (const k of keys) {
    const v = fields?.[k];
    if (v == null || v === "") continue;
    const n = Number(String(v).replace(/,/g, "").trim());
    if (Number.isFinite(n) && n > 0) return true;
  }
  return false;
}

function caseStudiesIndicateResidence(caseStudyRows) {
  return (caseStudyRows || []).some((row) => {
    const ht = fieldStr(row?.fields || row, "hotel_type", "Hotel Type");
    return /branded residence|condo[\s-]?hotel|mixed[\s-]?use/i.test(ht);
  });
}

/**
 * @param {{ profile?: { fields?: object }, platform?: { fields?: object }, commercial?: { fields?: object }, governance?: { fields?: object }, caseStudyRows?: object[] }} input
 * @returns {boolean}
 */
export function deriveBrandedResidentialCapable(input) {
  const pf = input?.profile?.fields || {};
  const plf = input?.platform?.fields || {};
  const cf = input?.commercial?.fields || {};
  const gf = input?.governance?.fields || {};

  const allowed = fieldStr(
    cf,
    "Branded Residences Allowed",
    "brandedResidencesAllowed"
  );
  if (/^no$/i.test(allowed)) return false;
  if (/^yes$/i.test(allowed)) return true;

  const serviceModels = parseServiceModels(pf, plf);
  if (serviceModels.some((s) => /branded residential|mixed-use/i.test(s))) return true;

  const experience = fieldStr(
    cf,
    "Branded Residence Experience Level",
    "brandedResidenceExperienceLevel"
  );
  if (experience && !/none documented|unknown/i.test(experience)) return true;

  const programs = cf["Branded Residence Program Models Supported"] ||
    cf.brandedResidenceProgramModelsSupported;
  if (Array.isArray(programs) && programs.length) return true;
  if (typeof programs === "string" && programs.trim()) return true;

  if (
    positiveCount(
      plf,
      "Branded Residence Properties Managed",
      "brandedResidencePropertiesManaged",
      "Mixed-Use Hospitality Experience",
      "mixedUseHospitalityExperience"
    )
  ) {
    return true;
  }

  if (
    positiveCount(
      cf,
      "Branded Residence Properties Managed",
      "brandedResidencePropertiesManaged"
    )
  ) {
    return true;
  }

  if (/case-by-case/i.test(allowed) && (experience || programs)) return true;

  if (
    fieldStr(gf, "HOA / Condo Association Interface", "hoaCondoAssociationInterface") &&
    !/^none$/i.test(fieldStr(gf, "HOA / Condo Association Interface", "hoaCondoAssociationInterface"))
  ) {
    return true;
  }

  if (caseStudiesIndicateResidence(input?.caseStudyRows)) return true;

  return false;
}
