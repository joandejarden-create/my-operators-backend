/**
 * ADP period eligibility — official baseline epoch vs pre-baseline archive.
 * Pre-baseline periods are retained for audit but excluded from customer trends.
 */

import { isTargetedMeasurementPeriod } from "./data-model.js";
import {
  MEASUREMENT_CONTRACT_VERSION,
  OFFICIAL_BASELINE_EPOCH,
  OFFICIAL_BASELINE_PERIOD_MARKER,
  OFFICIAL_BASELINE_SEQUENCE,
} from "./contracts/adp-measurement-contract-v1.js";

export const ARCHIVE_CLASSES = Object.freeze({
  DEVELOPMENT_ONLY: "DEVELOPMENT_ONLY",
  FULL_PROPERTY_PRE_BASELINE: "FULL_PROPERTY_PRE_BASELINE",
  TARGETED_RESEARCH: "TARGETED_RESEARCH",
  VALIDATION_ONLY: "VALIDATION_ONLY",
  OFFICIAL_PRODUCTION: "OFFICIAL_PRODUCTION",
});

export function isOfficialProductionPeriod(period) {
  if (!period) return false;
  if (period.officialPeriod !== true) return false;
  if (period.measurementPhase !== "OFFICIAL_PRODUCTION") return false;
  if (isTargetedMeasurementPeriod(period)) return false;
  return true;
}

export function isCustomerTrendEligible(period) {
  if (!period) return false;
  if (isTargetedMeasurementPeriod(period)) return false;
  if (period.customerTrendEligible === false) return false;
  if (period.customerVisible === false) return false;
  return isOfficialProductionPeriod(period) && period.customerTrendEligible === true;
}

export function isCustomerCurrentEligible(period) {
  if (!isCustomerTrendEligible(period)) return false;
  if (period.certified !== true) return false;
  if (period.fullProperty !== true && period.measurementScope?.type === "TARGETED_CORE_TRUTH_V1") {
    return false;
  }
  if (period.fullProperty === false) return false;
  return true;
}

export function classifyPreBaselinePeriod(period) {
  if (!period) return null;
  if (isOfficialProductionPeriod(period)) {
    return {
      archiveClass: ARCHIVE_CLASSES.OFFICIAL_PRODUCTION,
      officialPeriod: true,
      customerTrendEligible: period.customerTrendEligible === true,
      customerHistoricalComparisonEligible: period.customerTrendEligible === true,
      officialBaselineEligible: period.baselinePeriod === true,
      measurementPhase: "OFFICIAL_PRODUCTION",
    };
  }
  if (isTargetedMeasurementPeriod(period)) {
    return {
      archiveClass: ARCHIVE_CLASSES.TARGETED_RESEARCH,
      officialPeriod: false,
      customerTrendEligible: false,
      customerHistoricalComparisonEligible: false,
      officialBaselineEligible: false,
      measurementPhase: "PRE_BASELINE",
    };
  }
  const obs = period.observations || [];
  const hasLive = obs.some((o) => !o.dryRun && (o.rawResponse || o.parsed));
  if (!hasLive || period.status === "DRY_RUN_COMPLETE") {
    return {
      archiveClass: ARCHIVE_CLASSES.DEVELOPMENT_ONLY,
      officialPeriod: false,
      customerTrendEligible: false,
      customerHistoricalComparisonEligible: false,
      officialBaselineEligible: false,
      measurementPhase: "PRE_BASELINE",
    };
  }
  return {
    archiveClass: ARCHIVE_CLASSES.FULL_PROPERTY_PRE_BASELINE,
    officialPeriod: false,
    customerTrendEligible: false,
    customerHistoricalComparisonEligible: false,
    officialBaselineEligible: false,
    measurementPhase: "PRE_BASELINE",
  };
}

export function attachOfficialBaselinePeriodMetadata(period, {
  measurementContractHash,
  scenarioUniverseVersion = "adp_scenario_universe_v1",
  entityResolutionVersion = null,
  sourceGovernanceVersion = null,
  providerSet = ["openai", "gemini", "perplexity", "claude"],
  certified = false,
} = {}) {
  return {
    ...period,
    officialPeriod: true,
    baselinePeriod: true,
    baselineSequence: OFFICIAL_BASELINE_SEQUENCE,
    baselineMarker: OFFICIAL_BASELINE_PERIOD_MARKER,
    officialBaselineEpoch: OFFICIAL_BASELINE_EPOCH,
    measurementPhase: "OFFICIAL_PRODUCTION",
    customerTrendEligible: true,
    customerVisible: true,
    fullProperty: true,
    certified: certified === true,
    measurementContractVersion: MEASUREMENT_CONTRACT_VERSION,
    measurementContractHash,
    providerSet: [...providerSet],
    scenarioUniverseVersion,
    entityResolutionVersion,
    sourceGovernanceVersion,
    runTimestamp: period.completedAt || period.executionDate || new Date().toISOString(),
  };
}

/**
 * First official full-property period for a newly onboarded hotel
 * (e.g. Hotel Phillips) — does NOT mutate portfolio Period 001 markers.
 */
export function attachFirstOfficialPropertyPeriodMetadata(period, {
  measurementContractHash,
  baselineMarker,
  baselineSequence = 1,
  scenarioUniverseVersion = "adp_scenario_universe_v1",
  entityResolutionVersion = null,
  sourceGovernanceVersion = null,
  providerSet = ["openai", "gemini", "perplexity", "claude"],
  certified = false,
  priorComparablePeriod = null,
} = {}) {
  if (!baselineMarker) {
    throw new Error("attachFirstOfficialPropertyPeriodMetadata requires baselineMarker");
  }
  return {
    ...period,
    officialPeriod: true,
    firstOfficialPropertyPeriod: true,
    baselinePeriod: true,
    baselineSequence,
    baselineMarker,
    measurementPhase: "OFFICIAL_PRODUCTION",
    customerTrendEligible: true,
    customerVisible: true,
    fullProperty: true,
    certified: certified === true,
    measurementContractVersion: MEASUREMENT_CONTRACT_VERSION,
    measurementContractHash,
    priorComparablePeriod: priorComparablePeriod || null,
    providerSet: [...providerSet],
    scenarioUniverseVersion,
    entityResolutionVersion,
    sourceGovernanceVersion,
    runTimestamp: period.completedAt || period.executionDate || new Date().toISOString(),
  };
}

export function periodsAreContractCompatible(currentPeriod, priorPeriod) {
  if (!currentPeriod || !priorPeriod) {
    return { comparable: false, reason: "missing_period", compatibility: "UNKNOWN" };
  }
  if (!isCustomerTrendEligible(currentPeriod) || !isCustomerTrendEligible(priorPeriod)) {
    return { comparable: false, reason: "pre_baseline_or_ineligible", compatibility: "NOT_COMPARABLE" };
  }
  const curHash = currentPeriod.measurementContractHash;
  const priHash = priorPeriod.measurementContractHash;
  if (!curHash || !priHash) {
    return { comparable: false, reason: "missing_contract_hash", compatibility: "UNKNOWN" };
  }
  if (curHash !== priHash) {
    return { comparable: false, reason: "measurement_contract_hash_mismatch", compatibility: "NOT_COMPARABLE" };
  }
  if (
    currentPeriod.measurementContractVersion &&
    priorPeriod.measurementContractVersion &&
    currentPeriod.measurementContractVersion !== priorPeriod.measurementContractVersion
  ) {
    return { comparable: false, reason: "measurement_contract_version_mismatch", compatibility: "NOT_COMPARABLE" };
  }
  return { comparable: true, reason: null, compatibility: "COMPATIBLE" };
}

export function selectLatestCertifiedOfficialPeriod(periods) {
  const eligible = (periods || [])
    .filter((p) => isCustomerCurrentEligible(p))
    .sort((a, b) => String(a.executionDate || "").localeCompare(String(b.executionDate || "")));
  return eligible.length ? eligible[eligible.length - 1] : null;
}

export function filterCustomerTrendPeriods(periods) {
  return (periods || [])
    .filter((p) => isCustomerTrendEligible(p))
    .sort((a, b) => String(a.executionDate || "").localeCompare(String(b.executionDate || "")));
}
