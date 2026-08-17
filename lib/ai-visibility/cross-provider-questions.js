/**
 * Cross-provider Questions Missing — All Providers derived layer.
 *
 * Provider-specific results remain source of truth.
 * Never averages or sums provider Missing counts.
 * Never uses OpenAI as a silent proxy for All Providers.
 *
 * States (per comparable prompt × subject):
 *   MISSING_ACROSS_ALL_PROVIDERS — absent on every comparable monitored provider
 *   PRESENT_ON_ANY_PROVIDER — present on ≥1 comparable provider
 *   PRESENT_ACROSS_ALL_COMPARABLE — present on every comparable monitored provider
 *   PROVIDER_DISAGREEMENT — present on some, missing on others
 *
 * Not Monitored / no observation for a provider ≠ absence.
 */

import { loadObservationsFromBatchSummary } from "./cohort-observations.js";
import {
  formatProviderLabel,
  KNOWN_AI_VISIBILITY_PROVIDER_IDS,
  normalizeProviderId,
} from "./provider-dimension.js";

export const CROSS_PROVIDER_QUESTIONS_VERSION =
  "ai_visibility_cross_provider_questions_v1";

export const CROSS_PROVIDER_QUESTION_STATE = Object.freeze({
  MISSING_ACROSS_ALL_PROVIDERS: "MISSING_ACROSS_ALL_PROVIDERS",
  PRESENT_ON_ANY_PROVIDER: "PRESENT_ON_ANY_PROVIDER",
  PRESENT_ACROSS_ALL_COMPARABLE: "PRESENT_ACROSS_ALL_COMPARABLE",
  PROVIDER_DISAGREEMENT: "PROVIDER_DISAGREEMENT",
  NOT_COMPARABLE: "NOT_COMPARABLE",
});

/**
 * @param {object} obs
 * @param {string} entityId
 */
function observationHasEntity(obs, entityId) {
  if (!entityId || !obs) return false;
  if ((obs.presentEntityIds || []).includes(entityId)) return true;
  const mentions = obs.mentions || obs.payload?.mentions || [];
  return mentions.some(
    (m) =>
      m.entityId === entityId ||
      m.resolvedEntityId === entityId ||
      m.canonicalEntityId === entityId
  );
}

/**
 * Load successful slot-scoped observations per provider for one geography × language.
 * @param {{
 *   store: object,
 *   geoFilter: object,
 *   language?: string|null,
 *   providers?: string[],
 * }} args
 */
export async function loadObservationsByProviderForCohort(args = {}) {
  const {
    store,
    geoFilter,
    language = "en",
    providers = KNOWN_AI_VISIBILITY_PROVIDER_IDS,
  } = args;
  // Dynamic import avoids brand-read-service ↔ this module cycle.
  const { findMatchingSummaries } = await import("./brand-read-service.js");
  /** @type {Record<string, { monitored: boolean, observations: object[], batchId: string|null, promptIds: string[] }>} */
  const byProvider = {};

  for (const raw of providers) {
    const pid = normalizeProviderId(raw);
    if (!pid || pid === "all") continue;
    try {
      const summaries = await findMatchingSummaries(store, geoFilter, pid, {
        language: language || "en",
      });
      const latest = summaries[0];
      if (!latest) {
        byProvider[pid] = {
          monitored: false,
          observations: [],
          batchId: null,
          promptIds: [],
        };
        continue;
      }
      const { observations } = await loadObservationsFromBatchSummary(store, latest, {
        matchedSlotKeys: latest._matchedSlotKeys?.length
          ? latest._matchedSlotKeys
          : undefined,
        language: language || "en",
      });
      const obs = observations || [];
      const promptIds = [
        ...new Set(obs.map((o) => o.promptId).filter(Boolean)),
      ];
      byProvider[pid] = {
        monitored: obs.length > 0,
        observations: obs,
        batchId: latest.batchId || null,
        promptIds,
      };
    } catch (err) {
      if (typeof console !== "undefined" && console.warn) {
        console.warn(
          "[cross-provider-questions] load failed:",
          pid,
          err?.message || err
        );
      }
      byProvider[pid] = {
        monitored: false,
        observations: [],
        batchId: null,
        promptIds: [],
        error: err?.message || String(err),
      };
    }
  }

  return byProvider;
}

/**
 * Classify one prompt for a subject brand across comparable providers.
 * @param {{
 *   promptId: string,
 *   promptText?: string|null,
 *   promptFamily?: string|null,
 *   byProvider: Record<string, { monitored: boolean, observations: object[] }>,
 *   subjectBrandId: string,
 * }} args
 */
export function classifyPromptCrossProviderState(args = {}) {
  const { promptId, byProvider, subjectBrandId } = args;
  /** @type {string[]} */
  const providersPresent = [];
  /** @type {string[]} */
  const providersMissing = [];
  /** @type {string[]} */
  const providersMonitored = [];

  let promptText = args.promptText || null;
  let promptFamily = args.promptFamily || null;

  for (const [pid, pack] of Object.entries(byProvider || {})) {
    if (!pack?.monitored) continue;
    const obsForPrompt = (pack.observations || []).filter(
      (o) => o.success !== false && o.promptId === promptId
    );
    if (!obsForPrompt.length) continue; // Not Monitored for this prompt — skip
    providersMonitored.push(pid);
    const present = obsForPrompt.some((o) =>
      observationHasEntity(o, subjectBrandId)
    );
    if (present) providersPresent.push(pid);
    else providersMissing.push(pid);
    if (!promptText) {
      promptText = obsForPrompt[0]?.promptText || promptId;
    }
    if (!promptFamily) {
      promptFamily =
        obsForPrompt[0]?.promptFamily ||
        obsForPrompt[0]?.intentTerritory ||
        null;
    }
  }

  if (providersMonitored.length === 0) {
    return {
      promptId,
      QUESTION: promptText || promptId,
      PROMPT_FAMILY: promptFamily || "Unspecified",
      CROSS_PROVIDER_STATE: CROSS_PROVIDER_QUESTION_STATE.NOT_COMPARABLE,
      PROVIDERS_MONITORED: [],
      PROVIDERS_PRESENT: [],
      PROVIDERS_MISSING: [],
      comparable: false,
    };
  }

  let state;
  if (providersPresent.length === providersMonitored.length) {
    state = CROSS_PROVIDER_QUESTION_STATE.PRESENT_ACROSS_ALL_COMPARABLE;
  } else if (providersPresent.length === 0) {
    state = CROSS_PROVIDER_QUESTION_STATE.MISSING_ACROSS_ALL_PROVIDERS;
  } else {
    state = CROSS_PROVIDER_QUESTION_STATE.PROVIDER_DISAGREEMENT;
  }

  return {
    promptId,
    QUESTION: promptText || promptId,
    PROMPT_FAMILY: promptFamily || "Unspecified",
    CROSS_PROVIDER_STATE: state,
    PROVIDERS_MONITORED: providersMonitored,
    PROVIDERS_PRESENT: providersPresent,
    PROVIDERS_MISSING: providersMissing,
    PRESENT_ON_ANY_PROVIDER: providersPresent.length > 0,
    MISSING_ACROSS_ALL_PROVIDERS:
      state === CROSS_PROVIDER_QUESTION_STATE.MISSING_ACROSS_ALL_PROVIDERS,
    PROVIDER_DISAGREEMENT:
      state === CROSS_PROVIDER_QUESTION_STATE.PROVIDER_DISAGREEMENT,
    comparable: true,
  };
}

/**
 * Brand-level All Providers Questions Missing.
 * Count = prompts where subject missing across all comparable monitored providers.
 * Denominator = unique prompts with ≥1 comparable provider observation.
 *
 * @param {{
 *   byProvider: Record<string, { monitored: boolean, observations: object[] }>,
 *   subjectBrandId: string,
 * }} args
 */
export function computeBrandCrossProviderQuestionsMissing(args = {}) {
  const { byProvider, subjectBrandId } = args;
  const promptIds = new Set();
  for (const pack of Object.values(byProvider || {})) {
    if (!pack?.monitored) continue;
    for (const o of pack.observations || []) {
      if (o.promptId && o.success !== false) promptIds.add(o.promptId);
    }
  }

  const rows = [];
  for (const promptId of [...promptIds].sort()) {
    rows.push(
      classifyPromptCrossProviderState({
        promptId,
        byProvider,
        subjectBrandId,
      })
    );
  }

  const comparable = rows.filter((r) => r.comparable);
  const missingAcrossAll = comparable.filter((r) => r.MISSING_ACROSS_ALL_PROVIDERS);
  const disagreement = comparable.filter((r) => r.PROVIDER_DISAGREEMENT);
  const presentAcrossAll = comparable.filter(
    (r) =>
      r.CROSS_PROVIDER_STATE ===
      CROSS_PROVIDER_QUESTION_STATE.PRESENT_ACROSS_ALL_COMPARABLE
  );
  const presentOnAny = comparable.filter((r) => r.PRESENT_ON_ANY_PROVIDER);
  const denominator = comparable.length;
  const missingCount = missingAcrossAll.length;

  return {
    version: CROSS_PROVIDER_QUESTIONS_VERSION,
    mode: "DERIVED",
    ALL_PROVIDERS_RUN: false,
    OPENAI_SCAFFOLD: false,
    aggregation: "missing_across_all_comparable_providers",
    denominator,
    questionsMissingCount: missingCount,
    questionsMissingRate: denominator > 0 ? missingCount / denominator : null,
    PRESENT_ACROSS_ALL_N: presentAcrossAll.length,
    PRESENT_ON_ANY_N: presentOnAny.length,
    PROVIDER_DISAGREEMENT_N: disagreement.length,
    MISSING_ACROSS_ALL_N: missingCount,
    rows: comparable,
    watchlistRows: missingAcrossAll.map((r) => ({
      QUESTION: r.QUESTION,
      PROMPT_FAMILY: r.PROMPT_FAMILY,
      PROVIDERS_PRESENT: r.PROVIDERS_PRESENT.map(formatProviderLabel),
      PROVIDERS_MISSING: r.PROVIDERS_MISSING.map(formatProviderLabel),
      CROSS_PROVIDER_STATE: r.CROSS_PROVIDER_STATE,
      SUBJECT_PRESENCE: "MISSING_ACROSS_ALL_PROVIDERS",
      promptId: r.promptId,
    })),
    disagreementRows: disagreement.map((r) => ({
      QUESTION: r.QUESTION,
      PROMPT_FAMILY: r.PROMPT_FAMILY,
      PROVIDERS_PRESENT: r.PROVIDERS_PRESENT.map(formatProviderLabel),
      PROVIDERS_MISSING: r.PROVIDERS_MISSING.map(formatProviderLabel),
      CROSS_PROVIDER_STATE: r.CROSS_PROVIDER_STATE,
      promptId: r.promptId,
    })),
  };
}

/**
 * Portfolio-level: missing = no entitled brand observed on any comparable provider.
 * @param {{
 *   byProvider: Record<string, { monitored: boolean, observations: object[] }>,
 *   entitledBrandIds: string[],
 * }} args
 */
export function computePortfolioCrossProviderQuestionsMissing(args = {}) {
  const { byProvider, entitledBrandIds = [] } = args;
  const entitled = [...new Set((entitledBrandIds || []).filter(Boolean))];
  const promptIds = new Set();
  for (const pack of Object.values(byProvider || {})) {
    if (!pack?.monitored) continue;
    for (const o of pack.observations || []) {
      if (o.promptId && o.success !== false) promptIds.add(o.promptId);
    }
  }

  const rows = [];
  for (const promptId of [...promptIds].sort()) {
    /** @type {string[]} */
    const providersMonitored = [];
    /** @type {string[]} */
    const providersWithAnyEntitled = [];
    let promptText = null;
    let promptFamily = null;

    for (const [pid, pack] of Object.entries(byProvider || {})) {
      if (!pack?.monitored) continue;
      const obsForPrompt = (pack.observations || []).filter(
        (o) => o.success !== false && o.promptId === promptId
      );
      if (!obsForPrompt.length) continue;
      providersMonitored.push(pid);
      const anyEntitled = obsForPrompt.some((o) =>
        entitled.some((id) => observationHasEntity(o, id))
      );
      if (anyEntitled) providersWithAnyEntitled.push(pid);
      if (!promptText) promptText = obsForPrompt[0]?.promptText || promptId;
      if (!promptFamily) {
        promptFamily =
          obsForPrompt[0]?.promptFamily ||
          obsForPrompt[0]?.intentTerritory ||
          null;
      }
    }

    if (!providersMonitored.length) continue;

    const missingAcrossAll = providersWithAnyEntitled.length === 0;
    rows.push({
      promptId,
      QUESTION: promptText || promptId,
      PROMPT_FAMILY: promptFamily || "Unspecified",
      PROVIDERS_MONITORED: providersMonitored,
      PROVIDERS_WITH_ENTITLED_PRESENCE: providersWithAnyEntitled,
      MISSING_ACROSS_ALL_PROVIDERS: missingAcrossAll,
      PRESENT_ON_ANY_PROVIDER: !missingAcrossAll,
      CROSS_PROVIDER_STATE: missingAcrossAll
        ? CROSS_PROVIDER_QUESTION_STATE.MISSING_ACROSS_ALL_PROVIDERS
        : CROSS_PROVIDER_QUESTION_STATE.PRESENT_ON_ANY_PROVIDER,
    });
  }

  const denominator = rows.length;
  const missingCount = rows.filter((r) => r.MISSING_ACROSS_ALL_PROVIDERS).length;

  return {
    version: CROSS_PROVIDER_QUESTIONS_VERSION,
    mode: "DERIVED",
    ALL_PROVIDERS_RUN: false,
    OPENAI_SCAFFOLD: false,
    aggregation: "portfolio_no_entitled_brand_on_any_comparable_provider",
    denominator,
    questionsMissingCount: missingCount,
    questionsMissingRate: denominator > 0 ? missingCount / denominator : null,
    rows,
  };
}
