/**
 * Validate and normalize CALA submarket growth signal profiles.
 */

import { getSubmarketOptionsForCountry } from "../../radar-submarket-registry.js";
import { getCountryConfig } from "../country-configs.js";
import {
  GROWTH_SIGNAL_TYPE_IDS,
  GROWTH_DIRECTION_OPTIONS,
  GROWTH_PROFILE_STATUS,
  EARLY_ENTRY_OPPORTUNITY,
  getGrowthSignalTypeMeta,
} from "./growth-signal-types.js";

const SOURCE_OPTIONS = new Set([
  "Public Source",
  "Manual Research",
  "Analyst Review",
  "Broker Insight",
  "Owner Input",
]);

function str(v) {
  return String(v ?? "").trim();
}

function slug(s) {
  return str(s)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

/**
 * @param {object} raw
 * @param {object} ctx
 */
export function normalizeGrowthSignal(raw, ctx = {}) {
  const typeMeta = getGrowthSignalTypeMeta(raw.signalType);
  const buildImplication =
    str(raw.buildImplication) ||
    typeMeta?.buildImplication ||
    "Review official source for owner/brand build timing.";

  const recommendedHotelTypes = Array.isArray(raw.recommendedHotelTypes)
    ? raw.recommendedHotelTypes.map(str).filter(Boolean)
    : typeMeta?.defaultHotelTypes || [];

  return {
    id:
      str(raw.id) ||
      `${slug(ctx.country)}-${slug(ctx.submarket)}-${str(raw.signalType)}`,
    country: str(ctx.country || raw.country),
    region: str(ctx.region || raw.region),
    submarket: str(ctx.submarket || raw.submarket),
    linkedAnchorNames: Array.isArray(raw.linkedAnchorNames)
      ? raw.linkedAnchorNames.map(str).filter(Boolean)
      : raw.linkedAnchorName
        ? [str(raw.linkedAnchorName)]
        : [],
    signalType: str(raw.signalType),
    signalLabel: typeMeta?.label || str(raw.signalType),
    direction: GROWTH_DIRECTION_OPTIONS.includes(str(raw.direction))
      ? str(raw.direction)
      : "unknown",
    buildImplication,
    recommendedHotelTypes,
    timeHorizon: str(raw.timeHorizon),
    summary: str(raw.summary),
    ownerBrandTakeaway: str(raw.ownerBrandTakeaway),
    source: SOURCE_OPTIONS.has(str(raw.source)) ? str(raw.source) : "Public Source",
    sourceReference: str(raw.sourceReference),
    dataConfidence: ["High", "Medium", "Low"].includes(str(raw.dataConfidence))
      ? str(raw.dataConfidence)
      : "Medium",
    lastReviewed: str(raw.lastReviewed) || new Date().toISOString().slice(0, 10),
  };
}

/**
 * @param {object} raw
 */
export function normalizeSubmarketGrowthProfile(raw) {
  const country = str(raw.country);
  const config = getCountryConfig(country);
  const region = str(raw.region || config?.region || "");
  const submarket = str(raw.submarket);
  const profileStatus = Object.values(GROWTH_PROFILE_STATUS).includes(
    str(raw.profileStatus)
  )
    ? str(raw.profileStatus)
    : GROWTH_PROFILE_STATUS.SKELETON;

  const signals = (raw.signals || []).map((s) =>
    normalizeGrowthSignal(s, { country, region, submarket })
  );

  const earlyEntry = EARLY_ENTRY_OPPORTUNITY.includes(str(raw.earlyEntryOpportunity))
    ? str(raw.earlyEntryOpportunity)
    : "unknown";

  return {
    country,
    region,
    submarket,
    profileStatus,
    earlyEntryOpportunity: earlyEntry,
    primaryBuildProducts: Array.isArray(raw.primaryBuildProducts)
      ? raw.primaryBuildProducts.map(str).filter(Boolean)
      : [],
    ownerBrandSummary: str(raw.ownerBrandSummary),
    signals,
    signalCount: signals.length,
    lastReviewed:
      str(raw.lastReviewed) ||
      signals.reduce((max, s) => (s.lastReviewed > max ? s.lastReviewed : max), "") ||
      new Date().toISOString().slice(0, 10),
  };
}

/**
 * @param {ReturnType<typeof normalizeSubmarketGrowthProfile>} profile
 */
export function validateSubmarketGrowthProfile(profile) {
  const errors = [];
  const warnings = [];

  if (!profile.country) errors.push("country is required");
  if (!profile.submarket) errors.push("submarket is required");

  const allowed = getSubmarketOptionsForCountry(profile.country);
  if (profile.country && profile.submarket && !allowed.includes(profile.submarket)) {
    errors.push(
      `submarket "${profile.submarket}" not in registry for ${profile.country}`
    );
  }

  if (!profile.signals.length && profile.profileStatus === GROWTH_PROFILE_STATUS.RESEARCHED) {
    warnings.push("researched profile has no signals");
  }

  for (const sig of profile.signals) {
    if (!GROWTH_SIGNAL_TYPE_IDS.includes(sig.signalType)) {
      errors.push(`invalid signalType: ${sig.signalType}`);
    }
    if (!sig.sourceReference) {
      warnings.push(`signal ${sig.id} missing sourceReference`);
    }
    if (!sig.summary) warnings.push(`signal ${sig.id} missing summary`);
    if (!sig.ownerBrandTakeaway) {
      warnings.push(`signal ${sig.id} missing ownerBrandTakeaway`);
    }
    for (const anchor of sig.linkedAnchorNames) {
      if (!anchor) warnings.push(`signal ${sig.id} has empty linkedAnchorName`);
    }
  }

  return { valid: errors.length === 0, errors, warnings };
}

/**
 * @param {ReturnType<typeof normalizeSubmarketGrowthProfile>[]} profiles
 */
export function validateAllGrowthProfiles(profiles) {
  const byKey = new Map();
  const results = [];
  let errorCount = 0;
  let warningCount = 0;

  for (const raw of profiles) {
    const profile = normalizeSubmarketGrowthProfile(raw);
    const v = validateSubmarketGrowthProfile(profile);
    const key = `${profile.country}::${profile.submarket}`;
    if (byKey.has(key)) {
      v.errors.push(`duplicate profile key: ${key}`);
      v.valid = false;
    }
    byKey.set(key, profile);
    errorCount += v.errors.length;
    warningCount += v.warnings.length;
    results.push({ profile, ...v });
  }

  return {
    ok: errorCount === 0,
    profileCount: profiles.length,
    errorCount,
    warningCount,
    results,
  };
}
