/**
 * Competitive Gap Engine (P0C) — Presence-first + limited validated Association support.
 * No provider calls. No Airtable writes. No certified metric changes.
 */

import { createHash } from "crypto";
import { buildPeerPresentSubjectMissing } from "../questions-missing-intelligence.js";
import { computeAiPresenceRate, computeQuestionsMissing } from "../metrics.js";
import { loadScenarioRegistry, buildScenarioRegistryIndex } from "../scenario-registry.js";
import { resolvePeerSetMembership, peerSetBrandNamesById, PEER_SET_ID_V2 } from "../peer-sets.js";
import { aggregateBrandAssociations } from "../associations/aggregation-research.js";
import { isAssociationAttributeProductionEligible } from "./association-eligibility.js";
import {
  classifyGapPersistence,
  classifyGapPriority,
  classifyTrendStatus,
  GAP_LIFECYCLE,
} from "./gap-priority.js";
import { buildGapId, dedupeGaps, GAP_ENGINE_RULE_VERSION, GAP_CLASSES } from "./gap-identity.js";
import { buildTruthLayerHook } from "./truth-layer-hook.js";
import { observationsFromEvidence } from "./evidence-observations.js";

export { GAP_ENGINE_RULE_VERSION, GAP_CLASSES };

function presentIds(obs) {
  return new Set(obs.presentEntityIds || []);
}

function citationIdsFromObs(obs) {
  return (obs.citations || []).map((c) => c.citationId).filter(Boolean);
}

/**
 * A. PEER_PRESENT_BRAND_MISSING — subject absent, peer present, same scenario/geo/lang.
 */
export function detectPeerPresentBrandMissingGaps(observations = [], opts = {}) {
  const subjectBrandId = opts.subjectBrandId;
  const peerEntityIds = opts.peerEntityIds || [];
  const peerSetId = opts.peerSetId || PEER_SET_ID_V2;
  const peerNamesById = opts.peerNamesById || {};
  const scenarioByPrompt = opts.scenarioByPrompt || new Map();
  if (!subjectBrandId || !peerEntityIds.length) return [];

  const peerSet = new Set(peerEntityIds.filter((id) => id !== subjectBrandId));
  const gaps = [];

  for (const obs of observations) {
    if (!obs.success) continue;
    const present = presentIds(obs);
    if (present.has(subjectBrandId)) continue;
    const peersPresent = [...present].filter((id) => peerSet.has(id));
    if (!peersPresent.length) continue;

    const scenarioId = obs.scenarioId || scenarioByPrompt.get(obs.promptId)?.scenarioId || null;
    if (obs.scenarioStatus === "UNMAPPED" && opts.requireMappedScenario) continue;

    gaps.push({
      gapClass: "PEER_PRESENT_BRAND_MISSING",
      subjectBrandId,
      peerBrandIds: peersPresent,
      scenarioId,
      intentFamily: obs.intentFamily || null,
      ownerPriority: obs.ownerPriority || null,
      commercialPriority: obs.commercialPriority || "STANDARD",
      geography: obs.geographyKey || obs.commercialRegion || null,
      language: obs.language || "en",
      provider: obs.provider || null,
      promptId: obs.promptId || null,
      evidenceIds: obs.evidenceId ? [obs.evidenceId] : [],
      citationIds: citationIdsFromObs(obs),
      subjectPresence: false,
      peerPresence: true,
      questionsMissing: 1,
      observationCount: 1,
      providers: obs.provider ? [obs.provider] : [],
      variants: obs.promptId ? [obs.promptId] : [],
      periods: obs.periodKey ? [obs.periodKey] : [],
      lifecycleStatus: "ACTIVE",
      trendStatus: classifyTrendStatus(obs.periodKey ? 1 : 0),
      comparisonWindow: opts.comparisonWindow || "latest",
      peerSetId,
    });
  }

  return gaps;
}

/**
 * Aggregate A-class gaps into B. PERSISTENT_SCENARIO_GAP.
 */
export function aggregatePersistentScenarioGaps(rawGaps = [], opts = {}) {
  const subjectBrandId = opts.subjectBrandId;
  const byKey = new Map();

  for (const g of rawGaps) {
    if (g.gapClass !== "PEER_PRESENT_BRAND_MISSING") continue;
    const key = [
      g.subjectBrandId,
      g.scenarioId || "UNMAPPED",
      g.geography || "",
      g.language || "",
      g.peerSetId || "",
    ].join("|");

    if (!byKey.has(key)) {
      byKey.set(key, {
        gapClass: "PERSISTENT_SCENARIO_GAP",
        subjectBrandId: g.subjectBrandId,
        peerBrandIds: [],
        scenarioId: g.scenarioId,
        intentFamily: g.intentFamily,
        ownerPriority: g.ownerPriority,
        commercialPriority: g.commercialPriority || "STANDARD",
        geography: g.geography,
        language: g.language,
        evidenceIds: [],
        citationIds: [],
        promptIds: [],
        providers: [],
        variants: [],
        periods: [],
        peerSetId: g.peerSetId,
        observationCount: 0,
        questionsMissing: 0,
        subjectPresence: false,
        peerPresence: true,
        lifecycleStatus: "ACTIVE",
        comparisonWindow: g.comparisonWindow || "latest",
      });
    }
    const row = byKey.get(key);
    row.observationCount += 1;
    row.questionsMissing += 1;
    for (const pid of g.peerBrandIds || []) {
      if (!row.peerBrandIds.includes(pid)) row.peerBrandIds.push(pid);
    }
    for (const eid of g.evidenceIds || []) {
      if (!row.evidenceIds.includes(eid)) row.evidenceIds.push(eid);
    }
    for (const cid of g.citationIds || []) {
      if (!row.citationIds.includes(cid)) row.citationIds.push(cid);
    }
    if (g.promptId && !row.promptIds.includes(g.promptId)) row.promptIds.push(g.promptId);
    if (g.provider && !row.providers.includes(g.provider)) row.providers.push(g.provider);
    if (g.promptId && !row.variants.includes(g.promptId)) row.variants.push(g.promptId);
    if (g.periods) {
      for (const p of g.periods) {
        if (!row.periods.includes(p)) row.periods.push(p);
      }
    }
  }

  const out = [];
  for (const row of byKey.values()) {
    row.persistence = classifyGapPersistence(row);
    row.classification = classifyGapPriority(row.commercialPriority, row.persistence);
    row.trendStatus = classifyTrendStatus(row.periods.length);
    if (!row.classification) {
      row.lifecycleStatus = "INSUFFICIENT_DATA";
      continue;
    }
    row.gapId = buildGapId({
      gapClass: row.gapClass,
      subjectBrandId: row.subjectBrandId,
      peerBrandIds: row.peerBrandIds,
      scenarioId: row.scenarioId,
      geography: row.geography,
      language: row.language,
      peerSetId: row.peerSetId,
      comparisonWindow: row.comparisonWindow,
    });
    row.ruleVersion = GAP_ENGINE_RULE_VERSION;
    row.createdAt = new Date().toISOString();
    row.truthLayer = buildTruthLayerHook(row);
    out.push(row);
  }

  return out;
}

/**
 * C. VALIDATED_ASSOCIATION_GAP — production-eligible attributes only (DISTRIBUTION in P0C).
 */
export function detectValidatedAssociationGaps(evidence = [], opts = {}) {
  const subjectBrandId = opts.subjectBrandId;
  const peerBrandIds = (opts.peerEntityIds || []).filter((id) => id !== subjectBrandId);
  const attributeId = opts.attributeId || "DISTRIBUTION";
  const scenarioId = opts.scenarioId || null;
  const geography = opts.geography || null;
  const language = opts.language || "en";

  if (!isAssociationAttributeProductionEligible(attributeId)) {
    return { gaps: [], blocked: true, reason: "attribute_not_production_eligible" };
  }

  const subjectAgg = aggregateBrandAssociations(evidence, subjectBrandId, {
    language,
    scenarioId,
    requireMappedScenario: true,
    peerNames: opts.peerNames || [],
  }).filter((r) => r.attributeId === attributeId);

  const gaps = [];
  for (const peerId of peerBrandIds) {
    const peerAgg = aggregateBrandAssociations(evidence, peerId, {
      language,
      scenarioId,
      requireMappedScenario: true,
      peerNames: opts.peerNames || [],
    }).filter((r) => r.attributeId === attributeId);

    const subjectPos = subjectAgg.find((r) => r.polarity === "POSITIVE")?.observationCount || 0;
    const peerPos = peerAgg.find((r) => r.polarity === "POSITIVE")?.observationCount || 0;

    if (peerPos === 0) continue;
    if (subjectPos >= peerPos) continue;

    const comparable =
      scenarioId &&
      geography &&
      language &&
      peerAgg.some((r) => r.observationCount > 0) &&
      subjectAgg.length >= 0;

    if (!comparable) {
      gaps.push({
        gapClass: "VALIDATED_ASSOCIATION_GAP",
        lifecycleStatus: "NOT_COMPARABLE",
        subjectBrandId,
        peerBrandIds: [peerId],
        attributeId,
        scenarioId,
        geography,
        language,
        ruleVersion: GAP_ENGINE_RULE_VERSION,
      });
      continue;
    }

    gaps.push({
      gapClass: "VALIDATED_ASSOCIATION_GAP",
      subjectBrandId,
      peerBrandIds: [peerId],
      attributeId,
      scenarioId,
      geography,
      language,
      subjectAssociationCount: subjectPos,
      peerAssociationCount: peerPos,
      subjectPresence: subjectPos > 0,
      peerPresence: peerPos > 0,
      persistence: classifyGapPersistence({
        observationCount: peerPos - subjectPos,
        providers: peerAgg.flatMap((r) => r.providers || []),
        variants: peerAgg.flatMap((r) => r.variants || []),
        periods: peerAgg.flatMap((r) => r.periods || []),
      }),
      lifecycleStatus: "ACTIVE",
      comparisonWindow: opts.comparisonWindow || "latest",
      peerSetId: opts.peerSetId || PEER_SET_ID_V2,
      ruleVersion: GAP_ENGINE_RULE_VERSION,
      createdAt: new Date().toISOString(),
      truthLayer: buildTruthLayerHook({ gapClass: "VALIDATED_ASSOCIATION_GAP", subjectBrandId, scenarioId }),
    });
  }

  for (const g of gaps) {
    if (g.lifecycleStatus === "NOT_COMPARABLE") continue;
    g.classification = classifyGapPriority(opts.commercialPriority || "HIGH", g.persistence);
    g.gapId = buildGapId({
      gapClass: g.gapClass,
      subjectBrandId: g.subjectBrandId,
      peerBrandIds: g.peerBrandIds,
      scenarioId: g.scenarioId,
      geography: g.geography,
      language: g.language,
      attributeId: g.attributeId,
      peerSetId: g.peerSetId,
    });
  }

  return { gaps: gaps.filter((g) => g.lifecycleStatus !== "NOT_COMPARABLE" || opts.includeNotComparable), blocked: false };
}

/**
 * D. Truth layer placeholder gaps — structure only.
 */
export function buildTruthLayerPlaceholderGaps(subjectBrandId, scenarioIds = []) {
  return scenarioIds.map((scenarioId) => ({
    gapClass: "AI_PERCEPTION_VS_DEALALITY_FACT_GAP",
    subjectBrandId,
    scenarioId,
    lifecycleStatus: "INSUFFICIENT_DATA",
    classification: null,
    productionEligible: false,
    ruleVersion: GAP_ENGINE_RULE_VERSION,
    truthLayer: buildTruthLayerHook({ gapClass: "AI_PERCEPTION_VS_DEALALITY_FACT_GAP", subjectBrandId, scenarioId }),
    gapId: buildGapId({
      gapClass: "AI_PERCEPTION_VS_DEALALITY_FACT_GAP",
      subjectBrandId,
      scenarioId,
      comparisonWindow: "p0d_placeholder",
    }),
    note: "P0D Truth Layer — structure only; no Census claims in P0C.",
  }));
}

/**
 * Finalize A-class gaps with IDs and classification.
 */
function finalizeRawGaps(rawGaps = []) {
  return rawGaps.map((g) => {
    const persistence = classifyGapPersistence(g);
    const classification = classifyGapPriority(g.commercialPriority, persistence);
    return {
      ...g,
      persistence,
      classification,
      gapId: buildGapId({
        gapClass: g.gapClass,
        subjectBrandId: g.subjectBrandId,
        peerBrandIds: g.peerBrandIds,
        scenarioId: g.scenarioId,
        geography: g.geography,
        language: g.language,
        peerSetId: g.peerSetId,
      }),
      ruleVersion: GAP_ENGINE_RULE_VERSION,
      createdAt: new Date().toISOString(),
      truthLayer: buildTruthLayerHook(g),
    };
  }).filter((g) => g.classification != null);
}

/**
 * Run full gap detection for one subject brand.
 */
export function detectCompetitiveGapsForBrand(opts = {}) {
  const observations = opts.observations || [];
  const evidence = opts.evidence || [];
  const subjectBrandId = opts.subjectBrandId;
  const peerMembership = opts.peerMembership || resolvePeerSetMembership({
    peerSetId: opts.peerSetId || PEER_SET_ID_V2,
    commercialRegion: opts.geography || "CALA",
  });
  const peerEntityIds = opts.peerEntityIds || peerMembership.entityIds || [];
  const peerNamesById = opts.peerNamesById || peerSetBrandNamesById(opts.peerSetId || PEER_SET_ID_V2);

  const rawA = detectPeerPresentBrandMissingGaps(observations, {
    subjectBrandId,
    peerEntityIds,
    peerSetId: opts.peerSetId || PEER_SET_ID_V2,
    peerNamesById,
    requireMappedScenario: opts.requireMappedScenario !== false,
    comparisonWindow: opts.comparisonWindow || "latest",
  });

  const gapsA = finalizeRawGaps(dedupeGaps(rawA));
  const gapsB = aggregatePersistentScenarioGaps(rawA, { subjectBrandId });

  let researchAssociationGaps = 0;
  const gapsC = [];
  if (isAssociationAttributeProductionEligible("DISTRIBUTION")) {
    const { gaps, blocked } = detectValidatedAssociationGaps(evidence, {
      subjectBrandId,
      peerEntityIds,
      attributeId: "DISTRIBUTION",
      geography: opts.geography,
      language: opts.language,
      peerNames: Object.values(peerNamesById),
      peerSetId: opts.peerSetId || PEER_SET_ID_V2,
    });
    if (!blocked) gapsC.push(...gaps);
  }

  // Count research-only association gaps internally (never client-visible)
  for (const attr of ["OWNER_FLEXIBILITY", "LOYALTY", "CONVERSION_SUITABILITY"]) {
    if (isAssociationAttributeProductionEligible(attr)) continue;
    const { gaps } = detectValidatedAssociationGaps(evidence, {
      subjectBrandId,
      peerEntityIds,
      attributeId: attr,
      geography: opts.geography,
      language: opts.language,
      peerNames: Object.values(peerNamesById),
    });
    researchAssociationGaps += (gaps || []).length;
  }

  const registry = opts.registry || loadScenarioRegistry();
  const scenarioIds = (registry.scenarios || []).map((s) => s.scenarioId).slice(0, 3);
  const gapsD = buildTruthLayerPlaceholderGaps(subjectBrandId, scenarioIds);

  const presence = computeAiPresenceRate(observations, subjectBrandId);
  const qm = computeQuestionsMissing(observations, subjectBrandId);

  const peerPresent = buildPeerPresentSubjectMissing(observations, {
    subjectBrandId,
    peerEntityIds,
    peerNamesById,
  });

  const allProduction = dedupeGaps([...gapsA, ...gapsB, ...gapsC]);

  return {
    subjectBrandId,
    subjectBrandName: peerNamesById[subjectBrandId] || opts.subjectBrandName || null,
    presence,
    questionsMissing: qm,
    peerPresentSubjectMissing: peerPresent,
    gaps: allProduction,
    gapClassCounts: {
      PEER_PRESENT_BRAND_MISSING: gapsA.length,
      PERSISTENT_SCENARIO_GAP: gapsB.length,
      VALIDATED_ASSOCIATION_GAP: gapsC.length,
      AI_PERCEPTION_VS_DEALALITY_FACT_GAP: gapsD.length,
    },
    truthLayerPlaceholders: gapsD,
    researchAssociationGapsCreated: researchAssociationGaps,
    clientVisibleResearchAssociationGaps: 0,
    ruleVersion: GAP_ENGINE_RULE_VERSION,
  };
}

/**
 * Executive-safe highlights from production-qualified gaps only.
 */
export function buildExecutiveGapHighlights(allGaps = [], brandNamesById = {}) {
  const production = allGaps.filter(
    (g) => g.classification && g.lifecycleStatus !== "NOT_COMPARABLE"
  );
  const ranked = [...production].sort((a, b) => {
    const order = { HIGH_PRIORITY: 4, PRIORITY: 3, REVIEW: 2, MONITOR: 1 };
    return (order[b.classification] || 0) - (order[a.classification] || 0);
  });

  const top = ranked[0] || null;
  const largest =
    top &&
    buildExecutiveGapCopy(top, brandNamesById);

  const highest =
    ranked.find((g) => g.classification === "HIGH_PRIORITY") ||
    ranked.find((g) => g.classification === "PRIORITY") ||
    null;

  return {
    LARGEST_COMPETITIVE_GAP: largest,
    HIGHEST_PRIORITY_REVIEW: highest
      ? buildExecutiveGapCopy(highest, brandNamesById)
      : null,
    productionQualified: Boolean(largest || highest),
  };
}

function buildExecutiveGapCopy(gap, brandNamesById = {}) {
  const subject = brandNamesById[gap.subjectBrandId] || gap.subjectBrandId;
  const peerNames = (gap.peerBrandIds || [])
    .map((id) => brandNamesById[id] || id)
    .slice(0, 3);
  const scenario = gap.scenarioId || gap.intentFamily || "monitored scenario";
  const count = gap.observationCount || gap.questionsMissing || 1;
  const peerPhrase =
    peerNames.length >= 2 ? `${peerNames.length} peers` : peerNames[0] || "peer brands";

  return {
    title: gap.gapClass === "VALIDATED_ASSOCIATION_GAP" ? "Association gap" : "Competitive gap",
    scenario,
    subject,
    peers: peerNames,
    persistence: gap.persistence,
    commercialPriority: gap.commercialPriority,
    classification: gap.classification,
    fact:
      gap.gapClass === "VALIDATED_ASSOCIATION_GAP"
        ? `${subject} shows lower repeated ${gap.attributeId} association than ${peerPhrase} in comparable monitoring.`
        : `${subject} absent in ${count} comparable observation${count === 1 ? "" : "s"} while ${peerPhrase} appeared.`,
    evidence: `${count} evidence-backed observation${count === 1 ? "" : "s"} in ${gap.geography || "region"} (${gap.language || "en"}).`,
    gapId: gap.gapId,
    productionQualified: true,
  };
}

/**
 * Run gap engine for multiple brands (batch).
 */
export function runCompetitiveGapEngine(opts = {}) {
  const observations = opts.observations || [];
  const evidence = opts.evidence || [];
  const brandIds = opts.brandIds || [];
  const brandNamesById = opts.brandNamesById || {};
  const results = [];
  let allGaps = [];

  for (const brandId of brandIds) {
    const result = detectCompetitiveGapsForBrand({
      ...opts,
      observations,
      evidence,
      subjectBrandId: brandId,
      subjectBrandName: brandNamesById[brandId],
    });
    results.push(result);
    allGaps = allGaps.concat(result.gaps);
  }

  const highlights = buildExecutiveGapHighlights(allGaps, brandNamesById);
  const priorityCounts = { HIGH_PRIORITY: 0, PRIORITY: 0, REVIEW: 0, MONITOR: 0 };
  for (const g of allGaps) {
    if (g.classification && priorityCounts[g.classification] != null) {
      priorityCounts[g.classification] += 1;
    }
  }

  const classCounts = {
    PEER_PRESENT_BRAND_MISSING: 0,
    PERSISTENT_SCENARIO_GAP: 0,
    VALIDATED_ASSOCIATION_GAP: 0,
    AI_PERCEPTION_VS_DEALALITY_FACT_GAP: 0,
  };
  for (const g of allGaps) {
    if (classCounts[g.gapClass] != null) classCounts[g.gapClass] += 1;
  }
  for (const r of results) {
    classCounts.AI_PERCEPTION_VS_DEALALITY_FACT_GAP +=
      r.gapClassCounts?.AI_PERCEPTION_VS_DEALALITY_FACT_GAP || 0;
  }

  let researchAssoc = 0;
  for (const r of results) researchAssoc += r.researchAssociationGapsCreated || 0;

  const withEvidence = allGaps.filter((g) => (g.evidenceIds || []).length > 0).length;
  const withCitations = allGaps.filter((g) => (g.citationIds || []).length > 0).length;
  const notComparable = allGaps.filter((g) => g.lifecycleStatus === "NOT_COMPARABLE").length;
  const insufficientHistory = allGaps.filter((g) => g.trendStatus === "INSUFFICIENT_HISTORY").length;

  return {
    runId: createHash("sha256").update(String(Date.now())).digest("hex").slice(0, 12),
    ruleVersion: GAP_ENGINE_RULE_VERSION,
    NEW_PROVIDER_CALLS: 0,
    brandResults: results,
    gaps: allGaps,
    gapClassCounts: classCounts,
    priorityCounts,
    executiveHighlights: highlights,
    researchAssociationGapsCreated: researchAssoc,
    clientVisibleResearchAssociationGaps: 0,
    gapsWithEvidence: withEvidence,
    gapsWithCitations: withCitations,
    notComparable,
    insufficientHistory,
    comparablePeriodGaps: allGaps.length - insufficientHistory,
  };
}

/**
 * Load corpus + run for geography/language cohort.
 */
export async function runCompetitiveGapEngineFromStore(store, opts = {}) {
  const evidence = (await store.listEvidence({})) || [];
  const observations = observationsFromEvidence(evidence, {
    geography: opts.geography || "CALA",
    language: opts.language || "en",
  });
  return runCompetitiveGapEngine({
    ...opts,
    evidence: evidence.filter((raw) => {
      const lang = String(raw.language || raw.payload?.language || "en").toLowerCase();
      const normalized = lang.startsWith("es") ? "es" : "en";
      if (opts.language && normalized !== opts.language) return false;
      const geo = raw.commercialRegion || raw.countryName;
      if (opts.geography === "CALA") return geo === "CALA";
      return !opts.geography || geo === opts.geography;
    }),
    observations,
  });
}
