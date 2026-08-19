/**
 * Intersection / common-grain Presence — UNION denominators prohibited.
 * Pairwise architecture: subject and each peer scored on the same scenario measurement grains.
 */

import {
  collectStoredResponses,
  commonCohortKey,
  matchBrandInText,
  DEFAULT_RESPONSE_DIRS,
  DATASET_NAMESPACE,
} from "./presence-corpus.js";
import {
  buildScenarioRegistryIndex,
  loadScenarioRegistry,
  resolvePromptScenario,
} from "../scenario-registry.js";
import { buildPromptMetadataById } from "../associations/prompt-metadata-lookup.js";
import { IDS } from "./benchmark-brand-ids.js";
import { listBenchmarkEligibleMembers } from "./benchmark-eligible-universe.js";

export const COMMON_GRAIN_METHOD = "PAIRWISE";
export const UNION_GRAIN_BENCHMARK = "PROHIBITED";
export const INTERSECTION_GRAIN_VERSION = "intersection_grains_v1";

export function intersectionGrainKey(record) {
  return [
    record.scenarioId || "unmapped",
    record.promptFamily || record.promptId || "unknown_family",
    record.promptId || "unknown_prompt",
    record.provider || "unknown_provider",
    record.language || "unknown_lang",
    record.geography || "unknown_geo",
    record.promptVersion || "1",
  ].join("|");
}

export function matchBrandDisambiguated(text, member) {
  if (!text || !member) return { matched: false, ambiguous: false };
  if (member.disambiguation === "RADISSON_CORE_FLAG" || member.brandId === IDS.RADISSON) {
    const stripped = String(text)
      .replace(/radisson\s+blu/gi, " ")
      .replace(/radisson\s+red/gi, " ")
      .replace(/radisson\s+individuals/gi, " ");
    const byChoice = /radisson by choice/i.test(stripped);
    const standalone = /\bradisson\b/i.test(stripped);
    return { matched: byChoice || standalone, ambiguous: false };
  }
  return matchBrandInText(text, member.aliases || [member.brandName]);
}

function attachScenario(record, promptMap, scenarioIndex) {
  const meta = record.promptId ? promptMap.get(record.promptId) : null;
  const promptFamily = meta?.promptFamily || record.promptFamily || null;
  const resolved = resolvePromptScenario(
    {
      promptId: record.promptId,
      promptFamily,
      intentTerritory: record.intentTerritory || meta?.intentTerritory,
    },
    scenarioIndex
  );
  return {
    ...record,
    promptFamily,
    scenarioId: resolved.scenarioId,
    scenarioStatus: resolved.scenarioStatus,
    grainKey: null,
  };
}

/**
 * Build per-brand present-grain sets from stored OPEN_ENDED responses.
 * Denominator grains are measured responses, not union of positive mentions.
 */
export function buildScenarioMeasurementIndex(opts = {}) {
  const responses = opts.responses || collectStoredResponses(opts.responseDirs || DEFAULT_RESPONSE_DIRS);
  const promptMap = opts.promptMap || buildPromptMetadataById();
  const registry = opts.registry || loadScenarioRegistry();
  const scenarioIndex = opts.scenarioIndex || buildScenarioRegistryIndex(registry);
  const members = opts.members || listBenchmarkEligibleMembers();

  const grainsByScenario = new Map();
  const providersByScenario = new Map();
  const annotated = [];

  for (const raw of responses) {
    const rec = attachScenario(raw, promptMap, scenarioIndex);
    rec.grainKey = intersectionGrainKey(rec);
    annotated.push(rec);
    if (rec.scenarioStatus !== "MAPPED" || !rec.scenarioId) continue;
    if (!grainsByScenario.has(rec.scenarioId)) grainsByScenario.set(rec.scenarioId, new Set());
    grainsByScenario.get(rec.scenarioId).add(rec.grainKey);
    if (!providersByScenario.has(rec.scenarioId)) providersByScenario.set(rec.scenarioId, new Set());
    providersByScenario.get(rec.scenarioId).add(rec.provider || "unknown");
  }

  const presentByBrandScenario = new Map();
  for (const member of members) {
    const byScenario = new Map();
    for (const rec of annotated) {
      if (rec.scenarioStatus !== "MAPPED" || !rec.scenarioId) continue;
      const { matched } = matchBrandDisambiguated(rec.text, member);
      if (!matched) continue;
      if (!byScenario.has(rec.scenarioId)) byScenario.set(rec.scenarioId, new Set());
      byScenario.get(rec.scenarioId).add(rec.grainKey);
    }
    presentByBrandScenario.set(member.brandId, byScenario);
  }

  return {
    version: INTERSECTION_GRAIN_VERSION,
    datasetNamespace: DATASET_NAMESPACE,
    responsesScanned: responses.length,
    grainsByScenario,
    providersByScenario,
    presentByBrandScenario,
    UNION_GRAIN_BENCHMARK,
    COMMON_GRAIN_METHOD,
  };
}

function setPresence(presentSet, grainSet) {
  if (!grainSet.size) {
    return {
      commonGrains: 0,
      presentCommonGrains: 0,
      presenceCommon: null,
    };
  }
  let present = 0;
  for (const g of grainSet) {
    if (presentSet.has(g)) present += 1;
  }
  return {
    commonGrains: grainSet.size,
    presentCommonGrains: present,
    presenceCommon: present / grainSet.size,
  };
}

/**
 * Pairwise common grains = scenario measurement grains (shared OPEN_ENDED corpus).
 * Each peer Presence is computed on the same grains as the subject.
 */
export function computePairwiseScenarioPresence(subjectId, peerId, scenarioId, measurementIndex) {
  const grainSet = measurementIndex.grainsByScenario.get(scenarioId) || new Set();
  const subjectPresent = measurementIndex.presentByBrandScenario.get(subjectId)?.get(scenarioId) || new Set();
  const peerPresent = measurementIndex.presentByBrandScenario.get(peerId)?.get(scenarioId) || new Set();
  const subject = setPresence(subjectPresent, grainSet);
  const peer = setPresence(peerPresent, grainSet);
  return {
    scenarioId,
    subjectBrandId: subjectId,
    peerBrandId: peerId,
    commonGrains: subject.commonGrains,
    subjectPresentCommonGrains: subject.presentCommonGrains,
    peerPresentCommonGrains: peer.presentCommonGrains,
    subjectPresenceCommon: subject.presenceCommon,
    peerPresenceCommon: peer.presenceCommon,
    method: COMMON_GRAIN_METHOD,
    unionGrainUsed: false,
  };
}

export function percentile(sorted, p) {
  if (!sorted.length) return null;
  const idx = Math.min(sorted.length - 1, Math.max(0, Math.ceil((p / 100) * sorted.length) - 1));
  return sorted[idx];
}

export function summarizeGrainDistribution(values) {
  const sorted = [...values].filter((n) => typeof n === "number").sort((a, b) => a - b);
  if (!sorted.length) {
    return { n: 0, P10: null, P25: null, MEDIAN: null, P75: null, P90: null };
  }
  return {
    n: sorted.length,
    P10: percentile(sorted, 10),
    P25: percentile(sorted, 25),
    MEDIAN: percentile(sorted, 50),
    P75: percentile(sorted, 75),
    P90: percentile(sorted, 90),
  };
}

export function scenarioProviderClass(scenarioId, measurementIndex) {
  const providers = [...(measurementIndex.providersByScenario.get(scenarioId) || [])];
  const n = providers.filter((p) => p && p !== "unknown").length;
  if (n >= 3) return { class: "MULTI_PROVIDER_STRONG", providers };
  if (n === 2) return { class: "MULTI_PROVIDER_LIMITED", providers };
  if (n === 1) return { class: "SINGLE_PROVIDER_ONLY", providers };
  return { class: "NO_PROVIDER", providers };
}
