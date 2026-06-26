/**
 * Capital Provider Explorer hero badge lines — Capital Setup - Capital Providers.
 * Same pattern as Operator Explorer (Operator Setup - Master) and Brand Explorer.
 */

function trimStr(v) {
  if (v == null || v === "") return "";
  return String(v).trim();
}

/** Airtable column names on Capital Setup - Capital Providers */
export const CAPITAL_PROVIDER_EXPLORER_HERO_AIRTABLE = {
  verification: "Explorer Hero Verification",
  dataSource: "Explorer Hero Data Source",
};

/** UI fallbacks when provider fields are empty (live profiles). */
export const CAPITAL_PROVIDER_EXPLORER_HERO_UI_DEFAULTS = {
  verification: "",
  dataSource: "Live Airtable / Capital Setup data",
};

/** Embedded demo profile — presentation sample only. */
export const CAPITAL_PROVIDER_EXPLORER_HERO_DEMO_LABELS = {
  verification: "Demo — Not Capital Provider-Verified",
  dataSource: "Mock Data for Presentation",
};

/** Public-source seed profiles (not lender-verified). */
export const CAPITAL_PROVIDER_EXPLORER_HERO_PUBLIC_SEED_LABELS = {
  verification: "Public Source — Not Capital Provider-Verified",
  dataSource: "Curated Public Sources",
};

/**
 * @param {Record<string, unknown>|null|undefined} providerFields
 * @returns {{ explorerHeroVerification: string, explorerHeroDataSource: string }}
 */
export function pickExplorerHeroLabelsFromProviderFields(providerFields) {
  const f = providerFields || {};
  return {
    explorerHeroVerification: trimStr(
      f[CAPITAL_PROVIDER_EXPLORER_HERO_AIRTABLE.verification]
    ),
    explorerHeroDataSource: trimStr(
      f[CAPITAL_PROVIDER_EXPLORER_HERO_AIRTABLE.dataSource]
    ),
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
        trimStr(p.explorerHeroVerification) ||
        CAPITAL_PROVIDER_EXPLORER_HERO_DEMO_LABELS.verification,
      explorerHeroDataSource:
        trimStr(p.explorerHeroDataSource) ||
        CAPITAL_PROVIDER_EXPLORER_HERO_DEMO_LABELS.dataSource,
    };
  }
  return {
    explorerHeroVerification:
      trimStr(p.explorerHeroVerification) ||
      CAPITAL_PROVIDER_EXPLORER_HERO_UI_DEFAULTS.verification,
    explorerHeroDataSource:
      trimStr(p.explorerHeroDataSource) ||
      CAPITAL_PROVIDER_EXPLORER_HERO_UI_DEFAULTS.dataSource,
  };
}
