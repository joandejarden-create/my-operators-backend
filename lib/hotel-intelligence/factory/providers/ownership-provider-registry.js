/**
 * Packet 2.8B+2.8C-0 — OwnershipProvider registry.
 * Hotel Explorer assembler must NOT switch on hotel IDs.
 * In GOLDEN_RECONSTRUCTION_MODE, legacy cohorts cannot answer.
 */

import { isGoldenReconstructionMode } from "../golden-reconstruction/mode.js";
import {
  buildOwnershipFromOwnerControl,
  hasOwnerControlPortfolio,
  OWNER_CONTROL_PROVIDER_ID,
} from "../golden-reconstruction/owner-control-provider.js";
import { buildGsfCohortHotelOwnershipReportOnly } from "../../ownership/golden-demo/gsf-cohort.js";
import {
  buildOwnershipGroupProfile as buildGsfGroupProfile,
  buildNeighborsPayload as buildGsfNeighbors,
} from "../../ownership/golden-demo/gsf-cohort.js";
import {
  isCambridgeHotelId,
  buildCambridgeHotelOwnershipReport,
  buildDovetailOwnershipGroupProfile,
} from "../../ownership/golden-demo/cambridge-cohort.js";
import {
  isMexicoExplorerDemoHotelId,
  buildMexicoExplorerDemoOwnershipReport,
  buildMexicoExplorerOwnershipGroupProfile,
  tryBuildMexicoExplorerNeighborsPayload,
} from "../../ownership/golden-demo/mexico-explorer-demo-cohort.js";

export const OWNERSHIP_PROVIDER_REGISTRY_VERSION = "ownership-provider-registry-v2";

const ownerControlProvider = Object.freeze({
  id: OWNER_CONTROL_PROVIDER_ID,
  legacy: false,
  matches: (hotelId) => hasOwnerControlPortfolio(hotelId),
  buildHotelReport: (hotelId) => buildOwnershipFromOwnerControl(hotelId),
  buildGroupProfile: null,
  buildNeighbors: null,
});

const legacyProviders = Object.freeze([
  {
    id: "cambridge",
    legacy: true,
    matches: (hotelId) => isCambridgeHotelId(hotelId),
    buildHotelReport: (hotelId) => {
      const r = buildCambridgeHotelOwnershipReport(hotelId);
      return { ...r, provider_id: "cambridge" };
    },
    buildGroupProfile: (slug) => buildDovetailOwnershipGroupProfile(slug),
    buildNeighbors: null,
  },
  {
    id: "mexico_explorer",
    legacy: true,
    matches: (hotelId) => isMexicoExplorerDemoHotelId(hotelId),
    buildHotelReport: (hotelId) => {
      const r = buildMexicoExplorerDemoOwnershipReport(hotelId);
      return { ...r, provider_id: "mexico_explorer" };
    },
    buildGroupProfile: (slug) => {
      const r = buildMexicoExplorerOwnershipGroupProfile(slug);
      return r?.ok ? r : null;
    },
    buildNeighbors: (opts) => tryBuildMexicoExplorerNeighborsPayload(opts),
  },
  {
    id: "gsf_or_census_empty",
    legacy: true,
    matches: () => true,
    buildHotelReport: (hotelId) => {
      const r = buildGsfCohortHotelOwnershipReportOnly(hotelId);
      return {
        ...r,
        provider_id: r.in_cohort ? "gsf" : "census_empty",
        provider_legacy: Boolean(r.in_cohort),
      };
    },
    buildGroupProfile: (slug) => buildGsfGroupProfile(slug),
    buildNeighbors: (opts) => buildGsfNeighbors(opts),
  },
]);

/** Ordered providers — first match wins. Assembler never switches on hotel IDs. */
export const OWNERSHIP_PROVIDERS = Object.freeze([ownerControlProvider, ...legacyProviders]);

function censusEmptyReport(hotelId) {
  return {
    ok: false,
    in_cohort: false,
    provider_id: "census_empty",
    provider_legacy: false,
    hotel: { airtable_record_id: hotelId },
    report: {
      ownership_and_control: {
        economic_owner_or_group: { known: false },
        legal_property_owner_propco: { known: false },
      },
      executive_summary: { verification_bucket: "UNKNOWN" },
      decision_authority: { people: [] },
    },
    organization: null,
    deep_research: null,
  };
}

/**
 * Provider list for current mode.
 * Reconstruction: owner_control first, then census_empty only (no legacy cohorts).
 */
export function activeOwnershipProviders() {
  if (isGoldenReconstructionMode()) {
    return [
      ownerControlProvider,
      {
        id: "census_empty",
        legacy: false,
        matches: () => true,
        buildHotelReport: (hotelId) => censusEmptyReport(hotelId),
        buildGroupProfile: null,
        buildNeighbors: null,
      },
    ];
  }
  return OWNERSHIP_PROVIDERS;
}

export function resolveOwnershipProvider(hotelId) {
  const id = String(hotelId || "").trim();
  for (const p of activeOwnershipProviders()) {
    if (p.matches(id)) return p;
  }
  return activeOwnershipProviders().at(-1);
}

export function buildHotelOwnershipReportViaProviders(hotelId) {
  const provider = resolveOwnershipProvider(hotelId);
  const report = provider.buildHotelReport(String(hotelId || "").trim());
  return {
    ...report,
    provider_id: report.provider_id || provider.id,
    provider_legacy: report.provider_legacy ?? Boolean(provider.legacy),
  };
}

export function buildOwnershipGroupProfileViaProviders(slug) {
  if (isGoldenReconstructionMode()) {
    return { ok: false, error: "legacy_group_blocked_in_reconstruction_mode", provider_id: null };
  }
  for (const p of OWNERSHIP_PROVIDERS) {
    if (typeof p.buildGroupProfile !== "function") continue;
    try {
      const r = p.buildGroupProfile(slug);
      if (r && (r.ok === true || r.group || r.organization)) {
        return { ...r, provider_id: p.id };
      }
    } catch {
      /* try next */
    }
  }
  return { ok: false, error: "ownership_group_not_found", provider_id: null };
}

export function buildNeighborsViaProviders(opts = {}) {
  if (isGoldenReconstructionMode()) {
    return { ok: false, error: "legacy_neighbors_blocked_in_reconstruction_mode", provider_id: null };
  }
  for (const p of OWNERSHIP_PROVIDERS) {
    if (typeof p.buildNeighbors !== "function") continue;
    try {
      const r = p.buildNeighbors(opts);
      if (r && r.ok !== false && (r.nodes || r.neighbors || r.ok === true)) {
        return { ...r, provider_id: p.id };
      }
    } catch {
      /* try next */
    }
  }
  return { ok: false, error: "neighbors_not_found", provider_id: null };
}

export function listOwnershipProviderIds() {
  return activeOwnershipProviders().map((p) => p.id);
}
