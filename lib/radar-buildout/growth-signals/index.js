/**
 * CALA growth signal profile registry — query and aggregate helpers.
 */

import { COUNTRY_CONFIG_LIST } from "../country-configs.js";
import { getSubmarketOptionsForCountry } from "../../radar-submarket-registry.js";
import {
  normalizeSubmarketGrowthProfile,
  validateAllGrowthProfiles,
} from "./growth-signal-schema.js";
import { GROWTH_SIGNAL_TYPES, GROWTH_SIGNAL_TYPE_IDS } from "./growth-signal-types.js";
import { CARIBBEAN_BUILT_GROWTH_PROFILES } from "./profiles-caribbean-built.js";
import { CARIBBEAN_PLANNED_GROWTH_PROFILES } from "./profiles-caribbean-planned.js";
import { CENTRAL_AMERICA_GROWTH_PROFILES } from "./profiles-central-america.js";
import { SOUTH_AMERICA_GROWTH_PROFILES } from "./profiles-south-america.js";
import { MEXICO_GROWTH_PROFILES } from "./profiles-mexico.js";

const RAW_PROFILES = [
  ...CARIBBEAN_BUILT_GROWTH_PROFILES,
  ...CARIBBEAN_PLANNED_GROWTH_PROFILES,
  ...CENTRAL_AMERICA_GROWTH_PROFILES,
  ...SOUTH_AMERICA_GROWTH_PROFILES,
  ...MEXICO_GROWTH_PROFILES,
];

/** @type {ReturnType<typeof normalizeSubmarketGrowthProfile>[]} */
export const CALA_GROWTH_PROFILES = RAW_PROFILES.map(normalizeSubmarketGrowthProfile);

/**
 * @param {{ country?: string, submarket?: string, signalType?: string, profileStatus?: string }} [filter]
 */
export function listGrowthProfiles(filter = {}) {
  let rows = CALA_GROWTH_PROFILES;
  if (filter.country) {
    const c = String(filter.country).trim();
    rows = rows.filter((p) => p.country.toLowerCase() === c.toLowerCase());
  }
  if (filter.submarket) {
    const s = String(filter.submarket).trim();
    rows = rows.filter((p) => p.submarket.toLowerCase() === s.toLowerCase());
  }
  if (filter.profileStatus) {
    rows = rows.filter((p) => p.profileStatus === filter.profileStatus);
  }
  if (filter.signalType) {
    rows = rows.filter((p) =>
      p.signals.some((sig) => sig.signalType === filter.signalType)
    );
  }
  return rows;
}

/**
 * @param {string} country
 */
export function getGrowthProfilesForCountry(country) {
  return listGrowthProfiles({ country });
}

/**
 * @param {string} country
 * @param {string} submarket
 */
export function getGrowthProfile(country, submarket) {
  return (
    listGrowthProfiles({ country, submarket })[0] ||
    null
  );
}

/**
 * Flatten all signals with profile context.
 * @param {{ country?: string, signalType?: string }} [filter]
 */
export function listGrowthSignals(filter = {}) {
  const profiles = listGrowthProfiles(filter);
  const out = [];
  for (const profile of profiles) {
    for (const signal of profile.signals) {
      if (filter.signalType && signal.signalType !== filter.signalType) continue;
      out.push({
        ...signal,
        profileStatus: profile.profileStatus,
        earlyEntryOpportunity: profile.earlyEntryOpportunity,
        primaryBuildProducts: profile.primaryBuildProducts,
      });
    }
  }
  return out;
}

/**
 * CALA coverage summary vs submarket registry.
 */
export function buildGrowthSignalCoverageSummary() {
  const byCountry = {};
  for (const country of COUNTRY_CONFIG_LIST) {
    const submarkets = getSubmarketOptionsForCountry(country).filter((s) => s !== "Other");
    const profiles = getGrowthProfilesForCountry(country);
    const covered = new Set(profiles.map((p) => p.submarket));
    const signals = profiles.reduce((n, p) => n + p.signals.length, 0);
    byCountry[country] = {
      country,
      submarketTotal: submarkets.length,
      profileCount: profiles.length,
      submarketsCovered: [...covered].sort(),
      submarketsMissing: submarkets.filter((s) => !covered.has(s)),
      signalCount: signals,
      researchedProfiles: profiles.filter((p) => p.profileStatus === "researched").length,
      skeletonProfiles: profiles.filter((p) => p.profileStatus === "skeleton").length,
    };
  }

  const totals = Object.values(byCountry).reduce(
    (acc, row) => {
      acc.countries += 1;
      acc.profiles += row.profileCount;
      acc.signals += row.signalCount;
      acc.researched += row.researchedProfiles;
      return acc;
    },
    { countries: 0, profiles: 0, signals: 0, researched: 0 }
  );

  return {
    generatedAt: new Date().toISOString(),
    signalTypes: GROWTH_SIGNAL_TYPE_IDS.map((id) => ({
      id,
      ...GROWTH_SIGNAL_TYPES[id],
    })),
    totals,
    byCountry,
  };
}

export function validateCalaGrowthProfiles() {
  return validateAllGrowthProfiles(RAW_PROFILES);
}

export {
  GROWTH_SIGNAL_TYPES,
  GROWTH_SIGNAL_TYPE_IDS,
};
