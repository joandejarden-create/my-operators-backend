/**
 * Operator Explorer hero badge lines — sourced from Operator Setup - Master (same pattern as Brand Basics).
 */

function trimStr(v) {
  if (v == null || v === "") return "";
  return String(v).trim();
}

/** Airtable column names on Operator Setup - Master */
export const OPERATOR_EXPLORER_HERO_AIRTABLE = {
  verification: "Explorer Hero Verification",
  dataSource: "Explorer Hero Data Source",
};

/** UI fallbacks when Master fields are empty (live profiles only). */
export const OPERATOR_EXPLORER_HERO_UI_DEFAULTS = {
  verification: "",
  dataSource: "Live Airtable / Operator Setup data",
};

/** Embedded demo profile (no record id) — presentation sample only. */
export const OPERATOR_EXPLORER_HERO_DEMO_LABELS = {
  verification: "Demo — Not Brand-Verified",
  dataSource: "Mock Data for Presentation",
};

/**
 * @param {Record<string, unknown>|null|undefined} masterFields
 * @returns {{ explorerHeroVerification: string, explorerHeroDataSource: string }}
 */
export function pickExplorerHeroLabelsFromMasterFields(masterFields) {
  const f = masterFields || {};
  return {
    explorerHeroVerification: trimStr(f[OPERATOR_EXPLORER_HERO_AIRTABLE.verification]),
    explorerHeroDataSource: trimStr(f[OPERATOR_EXPLORER_HERO_AIRTABLE.dataSource]),
  };
}

/**
 * @param {{ explorerHeroVerification?: string, explorerHeroDataSource?: string }} picked
 * @param {{ isDemo?: boolean }} [opts]
 */
export function resolveExplorerHeroLabelsForUi(picked, opts = {}) {
  const p = picked || {};
  if (opts.isDemo) {
    return {
      explorerHeroVerification:
        trimStr(p.explorerHeroVerification) || OPERATOR_EXPLORER_HERO_DEMO_LABELS.verification,
      explorerHeroDataSource:
        trimStr(p.explorerHeroDataSource) || OPERATOR_EXPLORER_HERO_DEMO_LABELS.dataSource,
    };
  }
  return {
    explorerHeroVerification:
      trimStr(p.explorerHeroVerification) || OPERATOR_EXPLORER_HERO_UI_DEFAULTS.verification,
    explorerHeroDataSource:
      trimStr(p.explorerHeroDataSource) || OPERATOR_EXPLORER_HERO_UI_DEFAULTS.dataSource,
  };
}
