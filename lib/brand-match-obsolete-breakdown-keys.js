/**
 * Obsolete Brand Match breakdown keys from the reverted Fit/Opportunity dual-signal pass.
 * Strip from API responses and cache writes so stale Deal Brand Cache rows cannot resurface them.
 */
export const BRAND_MATCH_OBSOLETE_BREAKDOWN_KEYS = Object.freeze([
  "localBrandPower",
  "marketPriorityGrowth",
  "sameBrandTerritoryHeadroom",
  "conversionReflagReadiness",
  "fitScore",
  "opportunityScore",
  "guidance",
]);

const OBSOLETE_KEY_SET = new Set(BRAND_MATCH_OBSOLETE_BREAKDOWN_KEYS);

const OBSOLETE_LABEL_RE =
  /local brand power|market priority\s*\/\s*growth|same-brand (market )?headroom|conversion\s*\/\s*reflag readiness/i;

/**
 * @param {object|null|undefined} details
 * @returns {object}
 */
export function sanitizeBrandMatchBreakdownDetails(details) {
  if (!details || typeof details !== "object") return {};
  const out = {};
  for (const [key, value] of Object.entries(details)) {
    if (key === "_meta") {
      const meta = value && typeof value === "object" ? { ...value } : {};
      delete meta.guidance;
      delete meta.fitScore;
      delete meta.opportunityScore;
      delete meta.opportunityInsufficient;
      delete meta.pillarBlend;
      delete meta.scoredWeightPctOpportunity;
      delete meta.scoredWeightPctFit;
      out._meta = meta;
      continue;
    }
    if (OBSOLETE_KEY_SET.has(key)) continue;
    if (value && typeof value === "object" && OBSOLETE_LABEL_RE.test(String(value.label || ""))) continue;
    out[key] = value;
  }
  return out;
}
