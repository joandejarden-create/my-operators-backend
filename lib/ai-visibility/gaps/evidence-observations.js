/**
 * Convert stored evidence rows to Observation-shaped records for gap detection.
 */

import { buildObservationFromExtracted } from "../metrics.js";
import { enrichEvidenceWithPromptMetadata } from "../associations/prompt-metadata-lookup.js";
import { resolvePromptScenario, buildScenarioRegistryIndex, loadScenarioRegistry } from "../scenario-registry.js";

function normLang(v) {
  const s = String(v || "en").toLowerCase();
  return s.startsWith("es") || s === "spanish" ? "es" : "en";
}

function geoMatches(ev, geographyKey) {
  if (!geographyKey) return true;
  const geo = ev.commercialRegion || ev.countryName || ev.geographyScope || ev.regionName;
  if (geographyKey === "CALA") return geo === "CALA" || ev.commercialRegion === "CALA";
  return geo === geographyKey;
}

/**
 * @param {object[]} evidence
 * @param {object} [options]
 */
export function observationsFromEvidence(evidence = [], options = {}) {
  const registry = options.registry || loadScenarioRegistry(options.registryPath);
  const scenarioIndex = buildScenarioRegistryIndex(registry);
  const wantLang = options.language ? normLang(options.language) : null;
  const wantGeo = options.geography || null;
  const rows = [];

  for (const raw of evidence) {
    const ev = enrichEvidenceWithPromptMetadata(raw);
    const lang = normLang(ev.language || ev.payload?.language);
    if (wantLang && lang !== wantLang) continue;
    if (wantGeo && !geoMatches(ev, wantGeo)) continue;

    const mentions = ev.payload?.mentions || [];
    const citations = ev.payload?.citations || [];
    const scenario = resolvePromptScenario(
      { promptId: ev.promptId, promptFamily: ev.promptFamily, intentTerritory: ev.intentTerritory },
      scenarioIndex
    );

    const obs = buildObservationFromExtracted({
      observationId: ev.evidenceId,
      promptId: ev.promptId,
      provider: ev.provider,
      periodKey: ev.batchId || ev.runId || null,
      success: true,
      mentions,
      citations,
      geography: ev.commercialRegion || ev.countryName || null,
      intentTerritory: ev.intentTerritory || null,
    });

    rows.push({
      ...obs,
      evidenceId: ev.evidenceId,
      responseId: ev.responseId,
      promptText: ev.promptText || null,
      promptFamily: ev.promptFamily || null,
      language: lang,
      geographyKey: ev.commercialRegion || ev.countryName || ev.geographyScope || null,
      commercialRegion: ev.commercialRegion || null,
      scenarioId: scenario.scenarioId,
      scenarioStatus: scenario.scenarioStatus,
      intentFamily: scenario.intentFamily || null,
      ownerPriority: scenario.ownerPriority || null,
      commercialPriority: scenario.commercialPriority || null,
      variantGroupId: scenario.variantGroupId || null,
      mentions,
    });
  }

  return rows;
}
