/**
 * Load Observation[] from a completed batch summary via stored evidence.
 * Used by read APIs and offline reprocess — no provider calls.
 *
 * Evidence resolution (see evidence-resolution.js):
 *   1. run.evidenceId
 *   2. legacy run.responseId → evidence.responseId (exactly one match)
 */

import { buildObservationFromExtracted } from "./metrics.js";
import {
  normalizeLanguage,
  recordMatchesLanguage,
  resolveRecordLanguage,
} from "./language-dimension.js";
import { languageFromRunSlot, runMatchesSlotFilter, deriveRunSlotKey } from "./multi-slot-geography.js";
import {
  buildEvidenceResolutionIndex,
  EVIDENCE_RESOLUTION_MODES,
  resolveEvidenceForRun,
} from "./evidence-resolution.js";

/**
 * Prefer explicit evidence language; else slot key (CALA_ES → es); else treat-missing-as-en.
 * @param {object|null|undefined} evidence
 * @param {string|null|undefined} runSlot
 * @returns {"en"|"es"|null}
 */
export function resolveEvidenceObservationLanguage(evidence, runSlot) {
  const explicit = normalizeLanguage(
    evidence?.language ?? evidence?.payload?.language
  );
  if (explicit) return explicit;
  const fromSlot = languageFromRunSlot(runSlot);
  if (fromSlot) return fromSlot;
  return resolveRecordLanguage({ language: null }, { treatMissingAsEn: true });
}

/**
 * @param {object} store
 * @param {object} summary
 * @param {{ intentFilter?: string|null, matchedSlotKeys?: string[]|null, language?: string|null }} [opts]
 */
export async function loadObservationsFromBatchSummary(store, summary, opts = {}) {
  const intentFilter = opts.intentFilter ? String(opts.intentFilter).trim() : null;
  const matchedSlotKeys = Array.isArray(opts.matchedSlotKeys) ? opts.matchedSlotKeys : null;
  const wantLanguage = normalizeLanguage(opts.language);
  if (!summary?.batchId || typeof store.listBatchRuns !== "function") {
    return {
      observations: [],
      evidenceRows: [],
      intentTagged: 0,
      intentUntagged: 0,
      intentMissing: false,
      resolutionStats: {
        completedRuns: 0,
        evidenceResolved: 0,
        unresolved: 0,
        ambiguous: 0,
        byMode: {},
      },
    };
  }

  const runs = (await store.listBatchRuns(summary.batchId)) || [];
  const provider =
    summary.provider?.name || summary.provider || runs[0]?.provider || null;
  const index = await buildEvidenceResolutionIndex(store, {
    batchId: summary.batchId,
    provider,
  });

  const observations = [];
  const evidenceRows = [];
  let intentTagged = 0;
  let intentUntagged = 0;
  let completedRuns = 0;
  let evidenceResolved = 0;
  let unresolved = 0;
  let ambiguous = 0;
  /** @type {Record<string, number>} */
  const byMode = {};

  for (const run of runs) {
    if (run.status !== "completed") continue;
    completedRuns += 1;
    if (matchedSlotKeys?.length && !runMatchesSlotFilter(run, matchedSlotKeys)) {
      continue;
    }

    const resolved = await resolveEvidenceForRun(store, run, {
      index,
      batchId: summary.batchId,
      provider,
    });
    byMode[resolved.mode] = (byMode[resolved.mode] || 0) + 1;

    if (resolved.mode === EVIDENCE_RESOLUTION_MODES.AMBIGUOUS_EVIDENCE_LINK) {
      ambiguous += 1;
      continue;
    }
    if (!resolved.evidence) {
      unresolved += 1;
      continue;
    }
    evidenceResolved += 1;

    const evidence = resolved.evidence;
    const derivedSlot = deriveRunSlotKey(run);
    const resolvedLanguage = resolveEvidenceObservationLanguage(evidence, derivedSlot || run.slot);
    if (
      wantLanguage &&
      !recordMatchesLanguage({ language: resolvedLanguage }, wantLanguage, {
        treatMissingAsEn: true,
      })
    ) {
      continue;
    }
    const intent = evidence.intentTerritory || null;
    if (intent) intentTagged += 1;
    else intentUntagged += 1;
    if (intentFilter && intent && intent !== intentFilter) continue;
    if (intentFilter && !intent) continue;

    const mentions = evidence.payload?.mentions || [];
    const citations = evidence.payload?.citations || [];
    const observation = buildObservationFromExtracted({
      observationId: evidence.evidenceId,
      promptId: evidence.promptId || run.promptId,
      provider: evidence.provider || summary.provider || run.provider,
      periodKey: summary.batchId,
      success: true,
      mentions,
      citations,
      geography: evidence.regionName || summary.cohort?.commercialRegion || null,
      intentTerritory: intent,
    });
    observation.evidenceId = evidence.evidenceId;
    observation.responseId =
      evidence.responseId || run.responseId || observation.observationId || evidence.evidenceId;
    observation.runId = run.runId || evidence.runId || null;
    observation.batchId = summary.batchId;
    observation.promptText = evidence.promptText || evidence.promptId || run.promptId;
    observation.slot = derivedSlot || run.slot || null;
    observation.language = resolvedLanguage;
    observation.geographyKey =
      run.geographyKey ||
      evidence.regionName ||
      summary.cohort?.commercialRegion ||
      observation.geography ||
      null;
    observation.promptFamily =
      evidence.promptFamily ||
      evidence.payload?.promptFamily ||
      intent ||
      null;
    observation.citationCapability =
      evidence.citationCapability ||
      evidence.payload?.citationCapability ||
      null;
    observation.model = evidence.model || run.model || null;
    observation.timestamp =
      evidence.timestamp || run.completedAt || run.timestamp || null;
    observation.evidenceResolutionMode = resolved.mode;
    observation.status = "completed";
    observations.push(observation);
    evidenceRows.push({
      evidenceId: evidence.evidenceId,
      responseId: observation.responseId,
      promptId: evidence.promptId || run.promptId,
      promptText: evidence.promptText || evidence.promptId || run.promptId,
      intentTerritory: intent,
      mentions,
      citations,
      presentEntityIds: observation.presentEntityIds,
      recommendedEntityIds: observation.recommendedEntityIds,
      top3RecommendedEntityIds: observation.top3RecommendedEntityIds,
      slot: derivedSlot || run.slot || null,
      language: resolvedLanguage,
      evidenceResolutionMode: resolved.mode,
      model: observation.model,
      timestamp: observation.timestamp,
    });
  }

  return {
    observations,
    evidenceRows,
    intentTagged,
    intentUntagged,
    intentMissing: intentTagged === 0 && intentUntagged > 0,
    resolutionStats: {
      completedRuns,
      evidenceResolved,
      unresolved,
      ambiguous,
      byMode,
    },
  };
}
