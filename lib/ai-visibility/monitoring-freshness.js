/**
 * Client-facing monitoring freshness chrome (display-only).
 * Uses existing batch summaries — does not create monitoring data or change metrics.
 */

import {
  KNOWN_AI_VISIBILITY_PROVIDER_IDS,
  normalizeProviderId,
  isAllProvidersSelector,
} from "./provider-dimension.js";
import { isFullBaselineSummary } from "./provider-baseline-state.js";
import {
  isMultiSlotBatchSummary,
  multiSlotSummaryMatchesStoreFilter,
} from "./multi-slot-geography.js";

export const MONITORING_FRESHNESS_VERSION =
  "ai_visibility_monitoring_freshness_v1";

function parseDate(v) {
  if (!v) return null;
  const t = Date.parse(String(v));
  return Number.isFinite(t) ? t : null;
}

function formatClientDate(isoOrMs) {
  const ms = typeof isoOrMs === "number" ? isoOrMs : parseDate(isoOrMs);
  if (ms == null) return null;
  try {
    return new Intl.DateTimeFormat("en-US", {
      month: "short",
      day: "numeric",
      year: "numeric",
      timeZone: "UTC",
    }).format(new Date(ms));
  } catch {
    return new Date(ms).toISOString().slice(0, 10);
  }
}

/**
 * Build freshness meta for the selected geography cohort from existing store summaries.
 * @param {{
 *   store: object,
 *   geographyScope?: string|null,
 *   commercialRegion?: string|null,
 *   country?: string|null,
 *   language?: string|null,
 *   availableProviders?: Array<{id:string,completedBatchCount?:number}>,
 *   provider?: string|null,
 * }} args
 */
export async function buildMonitoringFreshness(args = {}) {
  const knownN = KNOWN_AI_VISIBILITY_PROVIDER_IDS.length;
  const available = Array.isArray(args.availableProviders)
    ? args.availableProviders
    : [];
  const completedProviderIds = [
    ...new Set(
      available
        .map((p) => normalizeProviderId(p.id || p))
        .filter((id) => id && !isAllProvidersSelector(id))
    ),
  ];
  const providersCompleted = completedProviderIds.length;
  const providersExpected = knownN;
  const partial =
    providersCompleted > 0 && providersCompleted < providersExpected;

  let latestMs = null;
  const perProviderLatest = {};
  /** @type {Record<string, number>} */
  const perProviderLatestAny = {};
  let latestMsAny = null;
  const store = args.store;
  if (store && typeof store.listBatchSummaries === "function") {
    const rows = (await store.listBatchSummaries({})) || [];
    const geoScope = args.geographyScope
      ? String(args.geographyScope).trim()
      : "";
    const region = String(args.commercialRegion || "").trim();
    const country = args.country ? String(args.country).trim() : "";

    for (const row of rows) {
      if (row.status !== "completed" && row.status !== "partial") continue;
      const purpose = String(
        row.monitoringRunPurpose || row.runPurpose || ""
      ).toLowerCase();
      if (purpose === "validation") continue;
      if (!isFullBaselineSummary(row)) continue;
      const id = normalizeProviderId(row.provider);
      if (!id) continue;

      const ms = parseDate(row.completedAt || row.batchDate || row.savedAt);
      if (ms == null) continue;

      if (!perProviderLatestAny[id] || ms > perProviderLatestAny[id]) {
        perProviderLatestAny[id] = ms;
      }
      if (latestMsAny == null || ms > latestMsAny) latestMsAny = ms;

      let geoOk = true;
      if (geoScope || region || country) {
        if (isMultiSlotBatchSummary(row)) {
          geoOk = multiSlotSummaryMatchesStoreFilter(row, {
            geographyScope: geoScope || undefined,
            commercialRegion: region || undefined,
            country: country || undefined,
          });
        } else {
          if (geoScope) {
            const rowScope = String(row.cohort?.geographyScope || "").trim();
            if (rowScope.toLowerCase() !== geoScope.toLowerCase()) geoOk = false;
          }
          if (geoOk && region) {
            const rowRegion = String(row.cohort?.commercialRegion || "").trim();
            if (rowRegion.toLowerCase() !== region.toLowerCase()) geoOk = false;
          }
          if (geoOk && country) {
            const rowCountry = String(row.cohort?.country || "").trim();
            if (rowCountry.toLowerCase() !== country.toLowerCase()) geoOk = false;
          }
        }
      }
      if (!geoOk) continue;

      if (!perProviderLatest[id] || ms > perProviderLatest[id]) {
        perProviderLatest[id] = ms;
      }
      if (latestMs == null || ms > latestMs) latestMs = ms;
    }
  }

  // Align with provider selector: if geo filter yields no dates, fall back to any completed baseline.
  if (latestMs == null && latestMsAny != null) {
    latestMs = latestMsAny;
    Object.assign(perProviderLatest, perProviderLatestAny);
  }

  // Fall back to availableProviders-derived completion without dates
  const hasCompleted = providersCompleted > 0 || latestMs != null;
  const lastMonitoredAt =
    latestMs != null ? new Date(latestMs).toISOString() : null;
  const lastMonitoredDisplay = hasCompleted
    ? formatClientDate(latestMs) || "Date unavailable"
    : null;

  return {
    version: MONITORING_FRESHNESS_VERSION,
    LAST_MONITORED_AT: lastMonitoredAt,
    LAST_MONITORED_DISPLAY: lastMonitoredDisplay,
    PROVIDERS_COMPLETED: providersCompleted,
    PROVIDERS_EXPECTED: providersExpected,
    PROVIDERS_COMPLETED_DISPLAY: `${providersCompleted} of ${providersExpected}`,
    PARTIAL_MONITORING: partial,
    NOT_MONITORED: !hasCompleted,
    STATUS: !hasCompleted
      ? "NOT_MONITORED"
      : partial
        ? "PARTIAL_MONITORING"
        : "COMPLETE",
    MODE_LABEL: "Latest monitored results",
    AUTOMATED_MONITORING: false,
    CONTINUOUS_MONITORING: false,
    providerLatestCompletedAt: Object.fromEntries(
      Object.entries(perProviderLatest).map(([id, ms]) => [
        id,
        new Date(ms).toISOString(),
      ])
    ),
  };
}
