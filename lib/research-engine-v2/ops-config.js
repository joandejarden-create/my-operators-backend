/**
 * Shadow operations cohort + cadence config (extensible; narrow by default).
 */

export const SHADOW_OPS_CONFIG = Object.freeze({
  configVersion: "shadow-ops-config-v1",
  engineVersion: "shadow-operations-v1",
  externalCostUsd: 0,
  suppressDays: 30,
  reminderDays: 90,
  fetchDelayMs: 280,
  /**
   * No in-repo cron framework — use npm scripts / OS Task Scheduler / GH Actions later.
   * Cadence recommended from measured ~3s / 16 hotels Indigo+Kimpton MX ($0).
   */
  cadence: {
    daily: {
      runType: "daily_lightweight",
      description: "Status/directory check for high-change pipeline/opening cohorts",
      cohorts: ["indigo_kimpton_mexico"],
    },
    weekly: {
      runType: "weekly_integrity",
      description: "Broader affiliation/integrity + Choice/Hilton samples + identity proposals",
      cohorts: ["indigo_kimpton_mexico", "choice_radisson_individuals_sample", "hilton_mx_sample"],
    },
    monthly: {
      runType: "monthly_activation",
      description: "Inactive brand activation completeness + deeper gap scans",
      cohorts: ["activation_benchmark_v1"],
    },
  },
  cohorts: {
    indigo_kimpton_mexico: {
      id: "indigo_kimpton_mexico",
      brandFamily: "ihg",
      countries: ["Mexico"],
      namePatterns: [/Hotel Indigo/i, /Kimpton/i],
      excludeNamePatterns: [/NOI Indigo/i],
      maxHotels: 40,
    },
    choice_radisson_individuals_sample: {
      id: "choice_radisson_individuals_sample",
      brandFamily: "choice",
      countries: ["Mexico", "Colombia", "Panama", "Costa Rica", "Dominican", "Peru", "Chile"],
      namePatterns: [/Radisson Individual/i, /Faranda/i, /Ascend/i, /Comfort Inn/i, /Quality Inn/i],
      maxHotels: 25,
    },
    hilton_mx_sample: {
      id: "hilton_mx_sample",
      brandFamily: "hilton",
      countries: ["Mexico"],
      namePatterns: [/Hilton Garden/i, /Hampton/i, /DoubleTree/i, /Homewood/i, /Curio/i, /Tapestry/i, /Spark/i],
      maxHotels: 20,
      requiresPropertyCode: true,
    },
    activation_benchmark_v1: {
      id: "activation_benchmark_v1",
      type: "brand_activation",
      brands: ["Avani", "Four Points Flex by Sheraton", "Tapestry Collection by Hilton", "Spark by Hilton", "Radisson Collection"],
    },
    retroactive_cleanup_pilot: {
      id: "retroactive_cleanup_pilot",
      description: "Small mixed read-only pilot — steward queue only",
      maxHotels: 45,
      activationBrands: 8,
    },
  },
});

/**
 * @param {object[]} rows - census rows
 * @param {object} cohortDef
 */
export function selectCohortHotels(rows, cohortDef) {
  const countries = cohortDef.countries || [];
  const patterns = cohortDef.namePatterns || [];
  const excludes = cohortDef.excludeNamePatterns || [];
  let out = rows.filter((r) => {
    const countryOk = !countries.length || countries.some((c) => new RegExp(c, "i").test(r.country || ""));
    if (!countryOk) return false;
    const name = r.name || "";
    if (excludes.some((re) => re.test(name))) return false;
    if (!patterns.length) return true;
    return patterns.some((re) => re.test(name));
  });
  if (cohortDef.maxHotels) out = out.slice(0, cohortDef.maxHotels);
  return out;
}
