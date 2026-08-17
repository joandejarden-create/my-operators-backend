/**
 * Re-export from the full brand Deal Terms profile library.
 */
export {
  getDealTermsProfile,
  getActiveLiveDealTermsProfile,
  BRAND_DEAL_TERMS_OVERRIDES as ACTIVE_LIVE_DEAL_TERMS_PROFILES,
  BRAND_TO_CHOICE_FDD_KEY as ACTIVE_LIVE_TO_CHOICE_FDD_KEY,
  BRAND_TO_CHOICE_FDD_KEY,
  BRAND_DEAL_TERMS_OVERRIDES,
  PARENT_DEAL_TERMS_TEMPLATES,
  baseFranchise,
  softBrandCollection,
  membershipNetwork,
} from "./brand-deal-terms-profiles.mjs";

import {
  BRAND_DEAL_TERMS_OVERRIDES,
  BRAND_TO_CHOICE_FDD_KEY,
} from "./brand-deal-terms-profiles.mjs";

export function listActiveLiveDealTermsBrandNames() {
  return [
    ...new Set([
      ...Object.keys(BRAND_DEAL_TERMS_OVERRIDES),
      ...Object.keys(BRAND_TO_CHOICE_FDD_KEY),
    ]),
  ].sort();
}
