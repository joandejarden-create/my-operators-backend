/**
 * Evidence corpus audit for AI Brand Association research (P0B).
 */

import { createBrandAiVisibilityReadStore } from "../storage/index.js";
import { resolvePromptScenario, buildScenarioRegistryIndex, loadScenarioRegistry } from "../scenario-registry.js";
import { enrichEvidenceWithPromptMetadata } from "./prompt-metadata-lookup.js";

function normLang(v) {
  const s = String(v || "en").toLowerCase();
  if (s.startsWith("es") || s === "spanish") return "es";
  return "en";
}

function geoKey(ev) {
  if (ev.commercialRegion) return ev.commercialRegion;
  if (ev.countryName) return ev.countryName;
  if (ev.geographyScope === "Global") return "Global";
  return ev.geographyScope || "Unknown";
}

/**
 * @param {object} [options]
 */
export async function auditAssociationEvidenceCorpus(options = {}) {
  const store = options.store || createBrandAiVisibilityReadStore({});
  const registry = options.registry || loadScenarioRegistry(options.registryPath);
  const scenarioIndex = buildScenarioRegistryIndex(registry);
  const evidence = (await store.listEvidence({})) || [];

  const byProvider = {};
  const byLanguage = {};
  const byGeography = {};
  const byScenario = {};
  let withMentions = 0;
  let withSnippets = 0;
  let withCitations = 0;
  let withRawText = 0;

  for (const raw of evidence) {
    const ev = enrichEvidenceWithPromptMetadata(raw);
    const provider = String(ev.provider || "openai").toLowerCase();
    byProvider[provider] = (byProvider[provider] || 0) + 1;
    const lang = normLang(ev.language || ev.payload?.language);
    byLanguage[lang] = (byLanguage[lang] || 0) + 1;
    const geo = geoKey(ev);
    byGeography[geo] = (byGeography[geo] || 0) + 1;

    const mentions = ev.payload?.mentions || [];
    if (mentions.length) withMentions += 1;
    if (mentions.some((m) => m.contextSnippet || m.snippet)) withSnippets += 1;
    if ((ev.payload?.citations || []).length) withCitations += 1;
    if (String(ev.payload?.rawResponseText || "").trim()) withRawText += 1;

    const scenario = resolvePromptScenario(
      {
        promptId: ev.promptId,
        promptFamily: ev.promptFamily,
        intentTerritory: ev.intentTerritory,
      },
      scenarioIndex
    );
    const sid = scenario.scenarioId || "UNMAPPED";
    byScenario[sid] = (byScenario[sid] || 0) + 1;
  }

  return {
    totalResponsesAvailable: evidence.length,
    responsesByProvider: byProvider,
    responsesByLanguage: byLanguage,
    responsesByGeography: byGeography,
    responsesByScenario: byScenario,
    responsesWithEntityMentions: withMentions,
    responsesWithMentionSnippets: withSnippets,
    responsesWithCitations: withCitations,
    responsesWithRawText: withRawText,
    reuseExistingEvidence: evidence.length >= 120 ? "YES" : "PARTIAL",
    NEW_PROVIDER_CALLS: 0,
    evidence: evidence.map(enrichEvidenceWithPromptMetadata),
  };
}
