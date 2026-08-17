/**
 * Map internal recommendation classifier roles → Brand UI copy.
 */

export const ROLE_COPY_VERSION = "ai_visibility_role_copy_v1";

const ROLE_TO_STATUS = Object.freeze({
  first_recommendation: "First Recommended",
  recommendation: "Recommended",
  ranked_recommendation: "Recommended",
  mention: "Mentioned",
  associated_option: "Associated Option",
  comparator: "Mentioned",
  passing: "Mentioned",
  missing: "Missing",
  absent: "Missing",
});

/**
 * @param {string|null|undefined} role
 * @returns {string}
 */
export function mapRecommendationRoleToBrandStatus(role) {
  const key = String(role || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "_");
  if (!key) return "Missing";
  return ROLE_TO_STATUS[key] || "Mentioned";
}

/**
 * Factual evidence descriptors only — no High/Medium/Low confidence.
 * @param {{ promptCount?: number, periodCount?: number, citationCount?: number, observationCount?: number }} meta
 */
export function buildEvidenceDescriptors(meta = {}) {
  const out = [];
  const prompts = Number(meta.promptCount) || 0;
  const periods = Number(meta.periodCount) || 0;
  const citations = Number(meta.citationCount) || 0;
  const observations = Number(meta.observationCount) || 0;

  if (prompts >= 2) out.push(`Repeated across ${prompts} prompts`);
  else if (prompts === 1) out.push("Single prompt observation");

  if (periods >= 2) out.push(`Seen in ${periods} monitoring periods`);
  else if (periods === 1) out.push("Single monitoring period");

  if (citations >= 2) out.push(`${citations} cited sources`);
  else if (citations === 1) out.push("1 cited source");

  if (!out.length && observations <= 1) out.push("Single observation");
  if (!out.length) out.push("Observation recorded");

  return out;
}
