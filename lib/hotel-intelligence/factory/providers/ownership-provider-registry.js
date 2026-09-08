/**
 * Packet 2.8B — OwnershipProvider registry.
 * Hotel Explorer assembler must NOT switch on hotel IDs.
 * Cohorts match() here only — data providers behind shared interface.
 */

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

export const OWNERSHIP_PROVIDER_REGISTRY_VERSION = "ownership-provider-registry-v1";

/** Ordered providers — first match wins. Assembler never switches on hotel IDs. */
export const OWNERSHIP_PROVIDERS = Object.freeze([
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

export function resolveOwnershipProvider(hotelId) {
  const id = String(hotelId || "").trim();
  for (const p of OWNERSHIP_PROVIDERS) {
    if (p.matches(id)) return p;
  }
  return OWNERSHIP_PROVIDERS[OWNERSHIP_PROVIDERS.length - 1];
}

export function buildHotelOwnershipReportViaProviders(hotelId) {
  const provider = resolveOwnershipProvider(hotelId);
  const report = provider.buildHotelReport(String(hotelId || "").trim());
  return {
    ...report,
    provider_id: report.provider_id || provider.id,
    provider_legacy: report.provider_legacy ?? Boolean(provider.legacy && report.in_cohort),
  };
}

export function buildOwnershipGroupProfileViaProviders(slug) {
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
  return OWNERSHIP_PROVIDERS.map((p) => p.id);
}
