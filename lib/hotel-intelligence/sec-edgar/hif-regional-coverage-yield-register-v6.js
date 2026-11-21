/**
 * HIF V6 regional coverage & yield register.
 * Coverage = actual research performed + verified results — never eligibility alone.
 */

import fs from "node:fs";
import path from "node:path";
import { SCALE_AMBITIONS_V6 } from "./hif-industrial-scale-operating-contract-v6.js";

export const HIF_REGIONAL_COVERAGE_YIELD_REGISTER_V6 =
  "hif-regional-coverage-yield-register-v6";

export const INTENSIVE_REGION = Object.freeze(
  Object.fromEntries(SCALE_AMBITIONS_V6.initialIntensiveRegions.map((r) => [r, r])),
);

/** Country → intensive region (non-exhaustive starter map; unknown → OTHER_ELIGIBLE). */
export const COUNTRY_TO_INTENSIVE_REGION = Object.freeze({
  US: "NORTH_AMERICA",
  CA: "NORTH_AMERICA",
  MX: "NORTH_AMERICA",
  GT: "CENTRAL_AMERICA",
  BZ: "CENTRAL_AMERICA",
  HN: "CENTRAL_AMERICA",
  SV: "CENTRAL_AMERICA",
  NI: "CENTRAL_AMERICA",
  CR: "CENTRAL_AMERICA",
  PA: "CENTRAL_AMERICA",
  CU: "CARIBBEAN",
  DO: "CARIBBEAN",
  PR: "CARIBBEAN",
  JM: "CARIBBEAN",
  TT: "CARIBBEAN",
  BB: "CARIBBEAN",
  BS: "CARIBBEAN",
  HT: "CARIBBEAN",
  BR: "SOUTH_AMERICA",
  AR: "SOUTH_AMERICA",
  CL: "SOUTH_AMERICA",
  CO: "SOUTH_AMERICA",
  PE: "SOUTH_AMERICA",
  UY: "SOUTH_AMERICA",
  EC: "SOUTH_AMERICA",
  PY: "SOUTH_AMERICA",
  BO: "SOUTH_AMERICA",
  VE: "SOUTH_AMERICA",
  GB: "WESTERN_EUROPE",
  IE: "WESTERN_EUROPE",
  FR: "WESTERN_EUROPE",
  BE: "WESTERN_EUROPE",
  NL: "WESTERN_EUROPE",
  LU: "WESTERN_EUROPE",
  CH: "WESTERN_EUROPE",
  AT: "WESTERN_EUROPE",
  DE: "CENTRAL_EUROPE",
  PL: "CENTRAL_EUROPE",
  CZ: "CENTRAL_EUROPE",
  SK: "CENTRAL_EUROPE",
  HU: "CENTRAL_EUROPE",
  SE: "NORTHERN_EUROPE",
  NO: "NORTHERN_EUROPE",
  DK: "NORTHERN_EUROPE",
  FI: "NORTHERN_EUROPE",
  IS: "NORTHERN_EUROPE",
  ES: "SOUTHERN_EUROPE",
  PT: "SOUTHERN_EUROPE",
  IT: "SOUTHERN_EUROPE",
  GR: "SOUTHERN_EUROPE",
  MT: "SOUTHERN_EUROPE",
  CY: "SOUTHERN_EUROPE",
  RO: "EASTERN_EUROPE",
  BG: "EASTERN_EUROPE",
  HR: "EASTERN_EUROPE",
  RS: "EASTERN_EUROPE",
  UA: "EASTERN_EUROPE",
  EE: "EASTERN_EUROPE",
  LV: "EASTERN_EUROPE",
  LT: "EASTERN_EUROPE",
});

export function mapCountryToIntensiveRegion(countryOrCode) {
  const raw = String(countryOrCode || "")
    .trim()
    .toUpperCase();
  if (!raw || raw === "UNKNOWN" || raw === "NULL") return "UNATTRIBUTED";
  if (COUNTRY_TO_INTENSIVE_REGION[raw]) return COUNTRY_TO_INTENSIVE_REGION[raw];
  // Allow full region names passed directly
  if (INTENSIVE_REGION[raw]) return raw;
  return "OTHER_ELIGIBLE";
}

export function emptyYieldCell() {
  return {
    hotelsDiscovered: 0,
    hotelsIdentityVerified: 0,
    ownersVerified: 0,
    agreementsIdentified: 0,
    contractDatesVerified: 0,
    noticePeriodsVerified: 0,
    qualifiedCommercialOpportunities: 0,
    documentsExamined: 0,
    sourceAvailability: "UNKNOWN",
    costUsd: 0,
    lastResearchedDate: null,
    covered: false, // true only when actual research + results recorded
  };
}

export function buildEmptyRegionalRegister(asOf = null) {
  const byRegion = {};
  for (const r of SCALE_AMBITIONS_V6.initialIntensiveRegions) {
    byRegion[r] = {
      region: r,
      eligible: true,
      covered: false,
      coverageNote: "ELIGIBLE_NOT_COVERED_UNTIL_RESEARCH_PERFORMED",
      byCountry: {},
      bySourceType: {},
      byAgreementFamily: {},
      totals: emptyYieldCell(),
    };
  }
  byRegion.OTHER_ELIGIBLE = {
    region: "OTHER_ELIGIBLE",
    eligible: true,
    covered: false,
    coverageNote: "ELIGIBLE_NOT_COVERED_UNTIL_RESEARCH_PERFORMED",
    byCountry: {},
    bySourceType: {},
    byAgreementFamily: {},
    totals: emptyYieldCell(),
  };
  byRegion.UNATTRIBUTED = {
    region: "UNATTRIBUTED",
    eligible: true,
    covered: false,
    coverageNote: "GEOGRAPHY_NOT_YET_ATTRIBUTED",
    byCountry: {},
    bySourceType: {},
    byAgreementFamily: {},
    totals: emptyYieldCell(),
  };

  return {
    version: HIF_REGIONAL_COVERAGE_YIELD_REGISTER_V6,
    asOf: asOf || new Date().toISOString().slice(0, 10),
    geographicExclusions: "NONE",
    rule: "Do not label a region covered merely because hotels are eligible for discovery.",
    byRegion,
  };
}

/**
 * Apply an observed research yield increment (idempotent callers should dedupe upstream).
 */
export function recordYieldObservation(register, obs = {}) {
  const reg = register || buildEmptyRegionalRegister();
  const country = String(obs.country || "UNKNOWN").toUpperCase();
  const region = obs.region || mapCountryToIntensiveRegion(country);
  if (!reg.byRegion[region]) {
    reg.byRegion[region] = {
      region,
      eligible: true,
      covered: false,
      coverageNote: "ELIGIBLE_NOT_COVERED_UNTIL_RESEARCH_PERFORMED",
      byCountry: {},
      bySourceType: {},
      byAgreementFamily: {},
      totals: emptyYieldCell(),
    };
  }
  const bucket = reg.byRegion[region];
  const cell = bucket.totals;
  const add = (k, n) => {
    cell[k] = (Number(cell[k]) || 0) + (Number(n) || 0);
  };
  add("hotelsDiscovered", obs.hotelsDiscovered);
  add("hotelsIdentityVerified", obs.hotelsIdentityVerified);
  add("ownersVerified", obs.ownersVerified);
  add("agreementsIdentified", obs.agreementsIdentified);
  add("contractDatesVerified", obs.contractDatesVerified);
  add("noticePeriodsVerified", obs.noticePeriodsVerified);
  add("qualifiedCommercialOpportunities", obs.qualifiedCommercialOpportunities);
  add("documentsExamined", obs.documentsExamined);
  add("costUsd", obs.costUsd);
  if (obs.lastResearchedDate) cell.lastResearchedDate = obs.lastResearchedDate;
  if (obs.sourceAvailability) cell.sourceAvailability = obs.sourceAvailability;

  const researched =
    (cell.documentsExamined || 0) > 0 ||
    (cell.hotelsDiscovered || 0) > 0 ||
    (cell.contractDatesVerified || 0) > 0;
  bucket.covered = researched;
  bucket.coverageNote = researched
    ? "ACTUAL_RESEARCH_RECORDED"
    : "ELIGIBLE_NOT_COVERED_UNTIL_RESEARCH_PERFORMED";

  if (country && country !== "UNKNOWN") {
    bucket.byCountry[country] = bucket.byCountry[country] || emptyYieldCell();
    const c = bucket.byCountry[country];
    c.hotelsDiscovered += Number(obs.hotelsDiscovered) || 0;
    c.hotelsIdentityVerified += Number(obs.hotelsIdentityVerified) || 0;
    c.ownersVerified += Number(obs.ownersVerified) || 0;
    c.agreementsIdentified += Number(obs.agreementsIdentified) || 0;
    c.contractDatesVerified += Number(obs.contractDatesVerified) || 0;
    c.noticePeriodsVerified += Number(obs.noticePeriodsVerified) || 0;
    c.qualifiedCommercialOpportunities += Number(obs.qualifiedCommercialOpportunities) || 0;
    c.documentsExamined += Number(obs.documentsExamined) || 0;
    c.costUsd += Number(obs.costUsd) || 0;
    if (obs.lastResearchedDate) c.lastResearchedDate = obs.lastResearchedDate;
  }
  if (obs.sourceType) {
    bucket.bySourceType[obs.sourceType] = bucket.bySourceType[obs.sourceType] || emptyYieldCell();
    const s = bucket.bySourceType[obs.sourceType];
    s.documentsExamined += Number(obs.documentsExamined) || 0;
    s.contractDatesVerified += Number(obs.contractDatesVerified) || 0;
    s.costUsd += Number(obs.costUsd) || 0;
  }
  if (obs.agreementFamily) {
    bucket.byAgreementFamily[obs.agreementFamily] =
      bucket.byAgreementFamily[obs.agreementFamily] || emptyYieldCell();
    const a = bucket.byAgreementFamily[obs.agreementFamily];
    a.agreementsIdentified += Number(obs.agreementsIdentified) || 0;
    a.contractDatesVerified += Number(obs.contractDatesVerified) || 0;
  }
  return reg;
}

export function defaultRegionalRegisterPath(cwd = process.cwd()) {
  return path.join(
    cwd,
    "reports/sec-edgar-hif-v6-industrial-scale/REGIONAL_COVERAGE_YIELD_REGISTER.json",
  );
}

export function saveRegionalRegister(register, filePath) {
  const p = filePath || defaultRegionalRegisterPath();
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, JSON.stringify(register, null, 2), "utf8");
  return p;
}

export function summarizeCoverage(register) {
  const regions = Object.values(register.byRegion || {});
  return {
    intensiveRegionsEligible: SCALE_AMBITIONS_V6.initialIntensiveRegions.length,
    intensiveRegionsWithActualResearch: regions.filter(
      (r) => SCALE_AMBITIONS_V6.initialIntensiveRegions.includes(r.region) && r.covered,
    ).length,
    regionsLabeledCoveredWithoutResearch: regions.filter(
      (r) => r.covered === false && r.eligible === true,
    ).length,
    rule: register.rule,
  };
}

export default {
  HIF_REGIONAL_COVERAGE_YIELD_REGISTER_V6,
  INTENSIVE_REGION,
  COUNTRY_TO_INTENSIVE_REGION,
  mapCountryToIntensiveRegion,
  buildEmptyRegionalRegister,
  recordYieldObservation,
  saveRegionalRegister,
  summarizeCoverage,
  emptyYieldCell,
};
