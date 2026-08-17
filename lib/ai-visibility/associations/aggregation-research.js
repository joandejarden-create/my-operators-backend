/**
 * Association aggregation + competitive gap research (P0B — internal only).
 */

import { classifyAssociationsFromEvidence } from "./deterministic-extractor.js";
import { resolvePromptScenario, buildScenarioRegistryIndex, loadScenarioRegistry } from "../scenario-registry.js";
import { enrichEvidenceWithPromptMetadata } from "./prompt-metadata-lookup.js";

export const ASSOCIATION_STRENGTH_DESCRIPTORS = Object.freeze({
  SINGLE_OBSERVATION: { minProviders: 1, minVariants: 1, minPeriods: 1 },
  EMERGING: { minObservations: 2, samePolarity: true },
  REPEATED_ACROSS_VARIANTS: { minVariants: 2 },
  REPEATED_ACROSS_PROVIDERS: { minProviders: 2 },
  REPEATED_ACROSS_PERIODS: { minPeriods: 2 },
  STRONGLY_REPEATED: { minProviders: 3, minPeriods: 2, minVariants: 2 },
});

function normLang(v) {
  const s = String(v || "en").toLowerCase();
  return s.startsWith("es") || s === "spanish" ? "es" : "en";
}

function descriptorForGroup(observations) {
  const providers = new Set(observations.map((o) => o.provider).filter(Boolean));
  const periods = new Set(observations.map((o) => o.periodKey).filter(Boolean));
  const variants = new Set(observations.map((o) => o.promptId).filter(Boolean));
  if (providers.size >= 3 && periods.size >= 2 && variants.size >= 2) {
    return "STRONGLY_REPEATED";
  }
  if (providers.size >= 2) return "REPEATED_ACROSS_PROVIDERS";
  if (periods.size >= 2) return "REPEATED_ACROSS_PERIODS";
  if (variants.size >= 2) return "REPEATED_ACROSS_VARIANTS";
  if (observations.length >= 2) return "EMERGING";
  return "SINGLE_OBSERVATION";
}

/**
 * Aggregate association observations for one brand.
 * @param {object[]} evidence
 * @param {string} brandId
 * @param {object} [options]
 */
export function aggregateBrandAssociations(evidence = [], brandId, options = {}) {
  const registry = options.registry || loadScenarioRegistry(options.registryPath);
  const scenarioIndex = buildScenarioRegistryIndex(registry);
  const rows = [];

  for (const raw of evidence) {
    const ev = enrichEvidenceWithPromptMetadata(raw);
    const lang = normLang(ev.language || ev.payload?.language);
    if (options.language && lang !== options.language) continue;

    const { publishable } = classifyAssociationsFromEvidence(ev, options);
    for (const p of publishable) {
      if (p.entityId !== brandId) continue;
      if (options.scenarioId && p.scenarioId !== options.scenarioId) continue;
      if (p.scenarioStatus === "UNMAPPED" && options.requireMappedScenario) continue;
      rows.push(p);
    }
  }

  const byAttr = new Map();
  for (const r of rows) {
    const key = `${r.attributeId}|${r.polarity}`;
    if (!byAttr.has(key)) byAttr.set(key, []);
    byAttr.get(key).push(r);
  }

  return [...byAttr.entries()].map(([key, obs]) => {
    const [attributeId, polarity] = key.split("|");
    const providers = [...new Set(obs.map((o) => o.provider).filter(Boolean))];
    const variants = [...new Set(obs.map((o) => o.promptId).filter(Boolean))];
    const periods = [...new Set(obs.map((o) => o.periodKey).filter(Boolean))];
    const scenarios = [...new Set(obs.map((o) => o.scenarioId).filter(Boolean))];
    return {
      attributeId,
      polarity,
      observationCount: obs.length,
      positiveObservations: polarity === "POSITIVE" ? obs.length : 0,
      negativeObservations: polarity === "NEGATIVE" ? obs.length : 0,
      providers,
      variants,
      periods,
      scenarios,
      descriptor: descriptorForGroup(obs),
    };
  });
}

/**
 * Competitive association comparison for same scenario/geo/language cohort.
 */
export function researchCompetitiveAssociationGap(args = {}) {
  const {
    evidence = [],
    subjectBrandId,
    peerBrandId,
    scenarioId,
    geographyKey = "CALA",
    language = "en",
    attributeId = "OWNER_FLEXIBILITY",
    options = {},
  } = args;

  const filterEv = evidence.map(enrichEvidenceWithPromptMetadata).filter((ev) => {
    const lang = normLang(ev.language || ev.payload?.language);
    if (lang !== language) return false;
    const geo = ev.commercialRegion || ev.countryName || ev.geographyScope;
    if (geographyKey && geo !== geographyKey && ev.geographyScope !== geographyKey) {
      if (geographyKey === "CALA" && ev.commercialRegion !== "CALA") return false;
    }
    return true;
  });

  const subject = aggregateBrandAssociations(filterEv, subjectBrandId, {
    ...options,
    language,
    scenarioId,
    requireMappedScenario: true,
  }).filter((r) => r.attributeId === attributeId);

  const peer = aggregateBrandAssociations(filterEv, peerBrandId, {
    ...options,
    language,
    scenarioId,
    requireMappedScenario: true,
  }).filter((r) => r.attributeId === attributeId);

  const subjectPos = subject.find((r) => r.polarity === "POSITIVE")?.observationCount || 0;
  const peerPos = peer.find((r) => r.polarity === "POSITIVE")?.observationCount || 0;

  let status = "UNSUPPORTED";
  if (subjectPos > 0 && peerPos > 0) {
    status = peerPos > subjectPos ? "SUPPORTED" : peerPos === subjectPos ? "PARTIAL" : "PARTIAL";
  } else if (peerPos > 0 && subjectPos === 0) {
    status = "PARTIAL";
  }

  return {
    subjectBrandId,
    peerBrandId,
    scenarioId,
    geographyKey,
    language,
    attributeId,
    status,
    subjectObservations: subject,
    peerObservations: peer,
    example:
      status === "SUPPORTED" || status === "PARTIAL"
        ? `Peer shows ${peerPos} positive ${attributeId} observations vs subject ${subjectPos} in comparable cohort.`
        : "Insufficient comparable association evidence for competitive statement.",
  };
}
