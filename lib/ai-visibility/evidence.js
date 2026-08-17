/**
 * Evidence assembly — data only, not generated prose.
 * Future "View Evidence" UI depends on this shape.
 */

import { randomUUID } from "crypto";
import { METRIC_VERSION } from "./config.js";

function newEvidenceId() {
  return `ev_${randomUUID().replace(/-/g, "").slice(0, 16)}`;
}

/**
 * @param {{
 *   prompt: object,
 *   run: object,
 *   response: object,
 *   mentions?: object[],
 *   citations?: object[],
 *   metrics?: object,
 * }} args
 */
export function assembleEvidenceRecord(args) {
  const {
    prompt,
    run,
    response,
    mentions = [],
    citations = [],
    metrics = null,
    geography = null,
    language = null,
  } = args;
  const geo =
    geography ||
    metrics?.observation?.geography ||
    prompt?.geographyNormalized ||
    null;
  const resolvedLanguage =
    language ||
    prompt?.language ||
    run?.language ||
    response?.language ||
    null;
  return {
    evidenceId: newEvidenceId(),
    promptId: prompt?.promptId || response?.promptId || null,
    promptVersion: prompt?.version || run?.promptVersion || null,
    promptText: prompt?.text || null,
    geographyScope: geo?.geographyScope || prompt?.geographyScope || null,
    regionName: geo?.regionName || prompt?.region || null,
    subregionName: geo?.subregionName || prompt?.subregion || null,
    countryName: geo?.countryName || prompt?.country || null,
    marketName: geo?.marketName || prompt?.market || null,
    geographyModelVersion: geo?.geographyModelVersion || null,
    language: resolvedLanguage,
    semanticPairId: prompt?.semanticPairId || run?.semanticPairId || null,
    intentTerritory: prompt?.intentTerritory || null,
    runId: run?.runId || response?.runId || null,
    responseId: response?.responseId || null,
    provider: response?.provider || run?.provider || null,
    model: response?.model || run?.model || null,
    timestamp: response?.createdAt || run?.completedAt || new Date().toISOString(),
    mentionIds: (mentions || []).map((m) => m.mentionId).filter(Boolean),
    citationIds: (citations || []).map((c) => c.citationId).filter(Boolean),
    metricVersion: METRIC_VERSION,
    payload: {
      rawResponseText: response?.text ?? null,
      mentions,
      citations,
      metrics,
      geography: geo,
      language: resolvedLanguage,
      citationCapability: response?.citationCapability || null,
      parserVersion: response?.parserVersion || null,
      runStatus: run?.status || null,
    },
  };
}

/**
 * Compact trace object for metric → evidence linkage.
 */
export function metricEvidenceTrace({ metricResult, evidenceId, observationIds = [] }) {
  return {
    metricVersion: metricResult?.metricVersion || METRIC_VERSION,
    metric: metricResult?.metric || null,
    evidenceId,
    observationIds,
    formula: {
      numerator: metricResult?.numerator ?? null,
      denominator: metricResult?.denominator ?? null,
      value: metricResult?.value ?? null,
    },
  };
}
