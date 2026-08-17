/**
 * Resolve Airtable Brand Basics names to Tier1Profile (or generated stub).
 */
import { TIER1_BRANDS } from "./choice-tier1-explorer-profiles.mjs";
import { buildStubProfile } from "./choice-chi-stub-profile.mjs";

/** Airtable Brand Name → profile key used in fixtures / FDD / overview content */
export const AIRTABLE_TO_PROFILE_NAME = {
  "Park Inn by Choice": "Park Inn by Radisson (Choice)",
  "Radisson by Choice": "Radisson (Choice)",
  "Radisson Blu by Choice": "Radisson Blu (Choice)",
  "Radisson Individuals by Choice": "Radisson Individual (Choice)",
  "Radisson RED by Choice": "Radisson RED  (Choice)",
  "Radisson Collection by Choice": "Radisson Collection  (Choice)",
  "Park Plaza by Choice": "Park Plaza (Choice)",
  "Country Inn & Suites by Choice": "Country Inn & Suites by Radisson (Choice)",
  "Country Inn & Suites": "Country Inn & Suites by Radisson (Choice)",
  "Country Inn & Suites by Radisson": "Country Inn & Suites by Radisson (Choice)",
};

/**
 * @param {string} requestedName — manifest / CLI brand filter
 * @param {string[]} chiBrandNames — live Brand Basics names (CHI)
 * @returns {string|null}
 */
export function resolveChiBrandBasicsName(requestedName, chiBrandNames) {
  const req = String(requestedName || "").trim();
  if (!req) return null;
  if (chiBrandNames.includes(req)) return req;

  const profile = resolveProfileForAirtableName(req).name;
  for (const liveName of chiBrandNames) {
    const liveProfile = AIRTABLE_TO_PROFILE_NAME[liveName] || liveName;
    if (liveProfile === profile || liveName === req) return liveName;
  }
  for (const [alias, profileName] of Object.entries(AIRTABLE_TO_PROFILE_NAME)) {
    if (alias === req || profileName === profile) {
      if (chiBrandNames.includes(alias)) return alias;
    }
  }
  return null;
}

/**
 * @param {string} airtableBrandName
 * @returns {import('./choice-tier1-explorer-profiles.mjs').Tier1Profile}
 */
export function resolveProfileForAirtableName(airtableBrandName) {
  const airtable = String(airtableBrandName || "").trim();
  const profileName = AIRTABLE_TO_PROFILE_NAME[airtable] || airtable;
  const tier1 = TIER1_BRANDS.find((b) => b.name === profileName);
  if (tier1) return tier1;
  return buildStubProfile(profileName, airtable);
}
