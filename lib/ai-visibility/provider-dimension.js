/**
 * AI Visibility — Provider as a first-class analytical dimension.
 *
 * Observation/metric scope conceptually:
 *   entity + geography + intent + provider + monitoring period + metric version
 *
 * Hard rules:
 * - Provider-specific results remain primary source of truth.
 * - "All Providers" is DERIVED only — never an All Providers run or provider record.
 * - No arbitrary All AI / GEO / consensus composite scores.
 * - Provider filtering happens in the read/data layer (not UI-only).
 * - Do not silently substitute another provider when the requested one has no data.
 * - Missing provider data ≠ zero.
 */

import { isFullBaselineSummary } from "./provider-baseline-state.js";
import {
  isMultiSlotBatchSummary,
  multiSlotSummaryMatchesStoreFilter,
} from "./multi-slot-geography.js";

export const DEFAULT_AI_VISIBILITY_PROVIDER = "openai";

/** Derived selector — never an All Providers run or provider record. */
export const ALL_PROVIDERS_SELECTOR_ID = "all";

export const KNOWN_AI_VISIBILITY_PROVIDER_IDS = Object.freeze([
  "openai",
  "gemini",
  "perplexity",
  "claude",
]);

/** Product labels for known provider ids — LLM / answer-product names, not company names. */
export const AI_VISIBILITY_PROVIDER_LABELS = Object.freeze({
  openai: "ChatGPT",
  gemini: "Gemini",
  perplexity: "Perplexity",
  claude: "Claude",
  [ALL_PROVIDERS_SELECTOR_ID]: "All Providers",
});

/**
 * Cross-provider concepts — All Providers derived view is implemented;
 * arbitrary blended scores remain forbidden.
 */
export const AI_VISIBILITY_CROSS_PROVIDER_STATUS = Object.freeze({
  ALL_AI: "NOT_IMPLEMENTED",
  ALL_PROVIDERS: "DERIVED_ONLY",
  BLENDED_AI_PRESENCE: "NOT_IMPLEMENTED",
  PROVIDER_WEIGHTS: "NONE",
  PROVIDER_CONSENSUS: "FUTURE_READY",
  PROVIDER_DIVERGENCE: "DERIVED_PRESENCE_AGREEMENT",
  ARBITRARY_SCORE: false,
});

/**
 * Normalize provider from string | { name } | null.
 * @param {unknown} value
 * @returns {string|null} lowercase canonical id, or null if empty/unknown shape
 */
export function normalizeProviderId(value) {
  if (value == null || value === "") return null;
  let s = "";
  if (typeof value === "object" && value.name != null) {
    s = String(value.name).trim().toLowerCase();
  } else {
    s = String(value).trim().toLowerCase();
  }
  if (!s) return null;
  if (s === "all_providers" || s === "all-providers") return ALL_PROVIDERS_SELECTOR_ID;
  // Company / product aliases → canonical provider id
  if (
    s === "chatgpt" ||
    s === "gpt" ||
    s === "gpt-4" ||
    s === "gpt-4o" ||
    s === "gpt4" ||
    s === "gpt4o"
  ) {
    return "openai";
  }
  if (s === "anthropic") return "claude";
  if (s === "google" || s === "google gemini") return "gemini";
  return s;
}

/**
 * @param {unknown} value
 * @returns {boolean}
 */
export function isAllProvidersSelector(value) {
  const id = normalizeProviderId(value);
  return id === ALL_PROVIDERS_SELECTOR_ID;
}

/**
 * @param {unknown} value
 * @param {string} [fallback=DEFAULT_AI_VISIBILITY_PROVIDER]
 */
export function resolveProviderId(value, fallback = DEFAULT_AI_VISIBILITY_PROVIDER) {
  const id = normalizeProviderId(value);
  if (isAllProvidersSelector(id)) return ALL_PROVIDERS_SELECTOR_ID;
  return id || fallback;
}

/**
 * Scaffold provider for store batch/language reads when All Providers is selected.
 * Prefer the default (openai / ChatGPT) when measured — never alphabetical-first (Claude),
 * which can surface empty observation loads and false Not Monitored KPIs.
 *
 * @param {Array<{id?: string}|string>} availableProviders
 * @param {string} [fallback=DEFAULT_AI_VISIBILITY_PROVIDER]
 * @returns {string}
 */
export function pickScaffoldDataProvider(
  availableProviders = [],
  fallback = DEFAULT_AI_VISIBILITY_PROVIDER
) {
  const ids = [
    ...new Set(
      (availableProviders || [])
        .map((p) => normalizeProviderId(p?.id ?? p))
        .filter(Boolean)
        .filter((id) => !isAllProvidersSelector(id))
    ),
  ];
  if (ids.includes(fallback)) return fallback;
  return ids[0] || fallback;
}

/**
 * @param {string|null|undefined} providerId
 */
export function formatProviderLabel(providerId) {
  const id = normalizeProviderId(providerId);
  if (!id) return AI_VISIBILITY_PROVIDER_LABELS.openai;
  if (AI_VISIBILITY_PROVIDER_LABELS[id]) return AI_VISIBILITY_PROVIDER_LABELS[id];
  return id.charAt(0).toUpperCase() + id.slice(1);
}

/**
 * True when both sides resolve to the same canonical provider id.
 */
export function providersMatch(a, b) {
  const left = normalizeProviderId(a);
  const right = normalizeProviderId(b);
  if (!left || !right) return false;
  return left === right;
}

/**
 * Build provider selector options: All Providers (derived) + known providers.
 * Missing provider data ≠ zero — mark monitored=false / Not Monitored.
 *
 * @param {Array<{ id: string, label?: string, completedBatchCount?: number }>} measuredProviders
 */
export function buildProviderSelectorOptions(measuredProviders = []) {
  const measured = new Map();
  for (const p of measuredProviders) {
    const id = normalizeProviderId(p.id || p);
    if (!id || isAllProvidersSelector(id)) continue;
    measured.set(id, {
      id,
      label: p.label || formatProviderLabel(id),
      completedBatchCount: p.completedBatchCount || 0,
      monitored: true,
      mode: "PROVIDER_SPECIFIC",
    });
  }

  const options = [
    {
      id: ALL_PROVIDERS_SELECTOR_ID,
      label: "All Providers",
      mode: "DERIVED",
      derivedOnly: true,
      monitored: measured.size > 0,
      ALL_PROVIDERS_RUN: false,
      ALL_PROVIDERS_PROVIDER_RECORD: false,
    },
  ];

  for (const id of KNOWN_AI_VISIBILITY_PROVIDER_IDS) {
    if (measured.has(id)) {
      options.push(measured.get(id));
    } else {
      options.push({
        id,
        label: AI_VISIBILITY_PROVIDER_LABELS[id] || formatProviderLabel(id),
        mode: "PROVIDER_SPECIFIC",
        monitored: false,
        completedBatchCount: 0,
        availability: "not_monitored",
      });
    }
  }

  for (const [id, row] of measured) {
    if (!KNOWN_AI_VISIBILITY_PROVIDER_IDS.includes(id)) {
      options.push(row);
    }
  }

  return options;
}

/**
 * List providers that have at least one completed/partial batch summary.
 * Optional geography filters narrow the option set to datasets for that cohort.
 *
 * Never invents gemini/perplexity without completed data.
 *
 * @param {{
 *   store: object,
 *   geographyScope?: string|null,
 *   commercialRegion?: string|null,
 *   region?: string|null,
 *   country?: string|null,
 * }} args
 * @returns {Promise<Array<{ id: string, label: string, completedBatchCount: number }>>}
 */
export async function listAvailableAiVisibilityProviders(args = {}) {
  const { store } = args;
  if (!store || typeof store.listBatchSummaries !== "function") return [];

  const rows = (await store.listBatchSummaries({})) || [];
  const geoScope = args.geographyScope ? String(args.geographyScope).trim() : "";
  const region = String(args.commercialRegion || args.region || "").trim();
  const country = args.country ? String(args.country).trim() : "";

  const counts = new Map();
  for (const row of rows) {
    if (row.status !== "completed" && row.status !== "partial") continue;
    // Validation-only and partial baselines must not appear as measured production providers.
    const purpose = String(row.monitoringRunPurpose || row.runPurpose || "").toLowerCase();
    if (purpose === "validation") continue;
    if (!isFullBaselineSummary(row)) continue;
    const id = normalizeProviderId(row.provider);
    if (!id) continue;

    if (geoScope || region || country) {
      if (isMultiSlotBatchSummary(row)) {
        if (
          !multiSlotSummaryMatchesStoreFilter(row, {
            geographyScope: geoScope || undefined,
            commercialRegion: region || undefined,
            country: country || undefined,
          })
        ) {
          continue;
        }
      } else {
        if (geoScope) {
          const rowScope = String(row.cohort?.geographyScope || "").trim();
          if (rowScope.toLowerCase() !== geoScope.toLowerCase()) continue;
        }
        if (region) {
          const rowRegion = String(row.cohort?.commercialRegion || "").trim();
          if (rowRegion.toLowerCase() !== region.toLowerCase()) continue;
        }
        if (country) {
          const rowCountry = String(row.cohort?.country || "").trim();
          if (rowCountry.toLowerCase() !== country.toLowerCase()) continue;
        }
      }
    }

    counts.set(id, (counts.get(id) || 0) + 1);
  }

  // If geography filter yielded nothing, fall back to any completed providers in store
  // so the filter bar can still show OpenAI while the selected geo is Not Monitored.
  if (!counts.size && (geoScope || region || country)) {
    for (const row of rows) {
      if (row.status !== "completed" && row.status !== "partial") continue;
      const id = normalizeProviderId(row.provider);
      if (!id) continue;
      counts.set(id, (counts.get(id) || 0) + 1);
    }
  }

  return [...counts.entries()]
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([id, completedBatchCount]) => ({
      id,
      label: formatProviderLabel(id),
      completedBatchCount,
    }));
}

/**
 * Build the provider block returned on read APIs.
 * @param {{
 *   store: object,
 *   requestedProvider?: string|null,
 *   geographyScope?: string|null,
 *   commercialRegion?: string|null,
 *   country?: string|null,
 *   defaultProvider?: string,
 * }} args
 */
export async function resolveProviderReadContext(args = {}) {
  const defaultProvider = args.defaultProvider || DEFAULT_AI_VISIBILITY_PROVIDER;
  const requestedRaw = normalizeProviderId(args.requestedProvider) || defaultProvider;
  const requested = isAllProvidersSelector(requestedRaw)
    ? ALL_PROVIDERS_SELECTOR_ID
    : requestedRaw;
  const available = await listAvailableAiVisibilityProviders({
    store: args.store,
    geographyScope: args.geographyScope,
    commercialRegion: args.commercialRegion,
    country: args.country,
  });
  const availableIds = available.map((p) => p.id);
  const selectorOptions = buildProviderSelectorOptions(available);
  const allDerived = isAllProvidersSelector(requested);
  const hasCompletedForProvider = allDerived
    ? availableIds.length > 0
    : availableIds.includes(requested);

  return {
    provider: requested,
    providerLabel: formatProviderLabel(requested),
    providerMode: allDerived ? "DERIVED" : "PROVIDER_SPECIFIC",
    availableProviders: available,
    /** Full selector: All Providers + OpenAI/Gemini/Perplexity/Claude (missing ≠ zero). */
    providerSelectorOptions: selectorOptions,
    providerHasCompletedData: hasCompletedForProvider,
    ALL_PROVIDERS_DERIVED: allDerived,
    ALL_PROVIDERS_RUN: false,
    ALL_PROVIDERS_PROVIDER_RECORD: false,
    crossProvider: { ...AI_VISIBILITY_CROSS_PROVIDER_STATUS },
    NO_SILENT_PROVIDER_FALLBACK: true,
  };
}
