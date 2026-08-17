/**
 * Reconstruction wave engine — configurable independent discovery → research → freeze.
 */

import { createResearchFirewall, ResearchFirewallError } from "./research-firewall.js";
import { discoverIhgMexicoAll, discoverIhgIndigoKimptonMexico } from "./group-discovery.js";
import { buildIndependentCohortRecords } from "./independent-record.js";
import {
  discoverHiltonMexicoAll,
  buildHiltonIndependentCohortRecords,
} from "./hilton-mexico-discovery.js";
import {
  discoverChoiceMexicoAll,
  buildChoiceIndependentCohortRecords,
} from "./choice-mexico-discovery.js";
import {
  discoverMarriottMexicoAll,
  buildMarriottIndependentCohortRecords,
} from "./marriott-mexico-discovery.js";
import { fingerprintFreeze } from "./legacy-reconcile.js";
import { VIC_ENGINE_VERSION, VIC_CONFIG_VERSION } from "./verified-record.js";

/**
 * @param {object} waveConfig
 * @param {{ fetchDelayMs?: number, onProgress?: Function, directoryPath?: string, graphqlStatus?: boolean, fetchPropertyPage?: boolean, fetchPropertyPages?: boolean, includeSlh?: boolean, sitemapPath?: string }} [opts]
 */
export async function runReconstructionWave(waveConfig, opts = {}) {
  const firewall = createResearchFirewall({ phase: "independent_research" });
  const startedAt = new Date().toISOString();

  // Prove fail-closed
  let firewallPreFreezeBlocked = false;
  try {
    firewall.requestLegacyCensus(() => {
      throw new Error("should_not_run");
    });
  } catch (err) {
    if (err instanceof ResearchFirewallError) firewallPreFreezeBlocked = true;
    else throw err;
  }

  let discovery;
  let records;
  const group = String(waveConfig.group || "").toUpperCase();
  const geography = String(waveConfig.geography || "");
  const brands = waveConfig.brands || null;
  const waveId = waveConfig.id || `${group}_${geography}`;

  if (group === "IHG" && /mexico/i.test(geography)) {
    if (Array.isArray(brands) && brands.length && brands.every((b) => /indigo|kimpton/i.test(b))) {
      discovery = discoverIhgIndigoKimptonMexico(firewall, { directoryPath: opts.directoryPath });
    } else {
      discovery = discoverIhgMexicoAll(firewall, {
        directoryPath: opts.directoryPath,
        brands: brands && brands.length ? brands : null,
      });
    }
    records = await buildIndependentCohortRecords(discovery, firewall, {
      fetchDelayMs: opts.fetchDelayMs,
      onProgress: opts.onProgress,
      reconstructionWave: waveId,
    });
  } else if (group === "HILTON" && /mexico/i.test(geography)) {
    discovery = await discoverHiltonMexicoAll(firewall, {
      delayMs: opts.fetchDelayMs ?? 200,
      onProgress: opts.onProgress,
      includeSlh: opts.includeSlh !== false,
    });
    records = await buildHiltonIndependentCohortRecords(discovery, firewall, {
      fetchDelayMs: opts.fetchDelayMs,
      onProgress: opts.onProgress,
      reconstructionWave: waveId,
      graphqlStatus: opts.graphqlStatus !== false,
      fetchPropertyPage: opts.fetchPropertyPage === true,
    });
  } else if (group === "CHOICE" && /mexico/i.test(geography)) {
    discovery = await discoverChoiceMexicoAll(firewall, {
      delayMs: opts.fetchDelayMs ?? 200,
      onProgress: opts.onProgress,
      sitemapPath: opts.sitemapPath,
      fetchPropertyPages: opts.fetchPropertyPages === true,
    });
    records = await buildChoiceIndependentCohortRecords(discovery, firewall, {
      fetchDelayMs: opts.fetchDelayMs,
      onProgress: opts.onProgress,
      reconstructionWave: waveId,
      fetchPropertyPages: opts.fetchPropertyPages === true,
    });
  } else if (group === "MARRIOTT" && /mexico/i.test(geography)) {
    discovery = await discoverMarriottMexicoAll(firewall, {
      delayMs: opts.fetchDelayMs ?? 200,
      onProgress: opts.onProgress,
    });
    records = await buildMarriottIndependentCohortRecords(discovery, firewall, {
      fetchDelayMs: opts.fetchDelayMs,
      onProgress: opts.onProgress,
      reconstructionWave: waveId,
      fetchPropertyPages: opts.fetchPropertyPages === true,
    });
  } else {
    throw new Error(`Wave group/geography not yet wired: ${group} / ${geography}`);
  }

  // Attach wave id
  for (const r of records) {
    r.reconstruction_wave = waveId;
    r.engine_version = VIC_ENGINE_VERSION;
  }

  const freeze_hash = fingerprintFreeze(records);
  const snapshot = {
    wave: waveConfig,
    config_version: VIC_CONFIG_VERSION,
    engine_version: VIC_ENGINE_VERSION,
    research_mode: "clean_census_reconstruction",
    discovery_basis: discovery.discovery_basis,
    discovery_sources: discovery.discovery_sources,
    brandBreakdown: discovery.brandBreakdown || null,
    brandPagesOk: discovery.brandPagesOk || null,
    fetchErrors: discovery.fetchErrors || null,
    startedAt,
    legacy_used_as_source: false,
    legacyComparisonAfterFreeze: waveConfig.legacyComparisonAfterFreeze !== false,
    records,
    freeze_hash_sha256: freeze_hash,
  };

  const frozen = firewall.freezeIndependentUniverse(snapshot);

  return {
    firewall,
    discovery,
    frozen,
    freeze_hash,
    firewallPreFreezeBlocked,
    startedAt,
    completedResearchAt: new Date().toISOString(),
  };
}

export const DEFAULT_WAVE_CONFIGS = Object.freeze([
  {
    id: "wave1_ihg_mexico_all",
    group: "IHG",
    geography: "Mexico",
    brands: null,
    researchProfile: "full_census",
    legacyComparisonAfterFreeze: true,
    firstPartyValidationEligible: true,
    targetFields: "MATERIAL_CENSUS_FIELDS",
  },
  {
    id: "wave1b_hilton_mexico",
    group: "Hilton",
    geography: "Mexico",
    brands: null,
    researchProfile: "full_census",
    legacyComparisonAfterFreeze: true,
    firstPartyValidationEligible: true,
    targetFields: "MATERIAL_CENSUS_FIELDS",
    discovery: "live_hilton_locations_mexico_brand_pages",
  },
  {
    id: "wave1c_choice_mexico",
    group: "Choice",
    geography: "Mexico",
    brands: null,
    researchProfile: "full_census",
    legacyComparisonAfterFreeze: true,
    firstPartyValidationEligible: true,
    targetFields: "MATERIAL_CENSUS_FIELDS",
    discovery: "live_choice_mexico_regional_jsonld",
  },
  {
    id: "wave1d_marriott_mexico",
    group: "Marriott",
    geography: "Mexico",
    brands: null,
    researchProfile: "full_census",
    legacyComparisonAfterFreeze: true,
    firstPartyValidationEligible: true,
    targetFields: "MATERIAL_CENSUS_FIELDS",
    discovery: "live_marriott_mexico_country_hotel_sitemap",
  },
  {
    id: "wave1b_ihg_cala",
    group: "IHG",
    geography: "CALA",
    brands: null,
    researchProfile: "full_census",
    legacyComparisonAfterFreeze: true,
    status: "planned",
  },
  {
    id: "wave2_hyatt_mexico",
    group: "Hyatt",
    geography: "Mexico",
    status: "planned",
  },
]);
