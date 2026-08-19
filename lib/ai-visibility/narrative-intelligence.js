/**
 * Narrative Intelligence V1 — evidence-first narrative extraction and aggregation.
 * No provider calls. Reuses deterministic association spans; does not alter certified classifiers.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { createHash } from "crypto";
import { classifyAssociationsFromEvidence } from "./associations/deterministic-extractor.js";
import { enrichEvidenceWithPromptMetadata } from "./associations/prompt-metadata-lookup.js";
import { resolvePromptProvenance } from "./prompt-provenance.js";
import {
  STAGE_B_AUTHORITATIVE_REPORT_REL_PATH,
  STAGE_B_AUTHORITATIVE_WAVE_ID,
  STAGE_B_NON_AUTHORITATIVE_WAVE_IDS,
} from "./stability-policy.js";
import {
  mapAttributeToNarrativeFamily,
  classifyBrandRelationship,
  isExecutiveSafeBrandRelationship,
  NARRATIVE_FAMILY_LABELS,
  classifyNarrativeFamilyProductionState,
} from "./narrative-taxonomy.js";
import {
  getBrandDecisionEligibility,
  loadDecisionEligibilityConfig,
} from "./brand-decision-eligibility.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.join(__dirname, "..", "..");

export const NARRATIVE_INTELLIGENCE_VERSION = "ai_visibility_narrative_intelligence_v1";

export const PORTFOLIO_BRANDS = Object.freeze({
  "Autograph Collection": "recEJCTDj1zrsjPM6",
  "AC Hotels by Marriott": "rec9aZp7GHtzUEg0c",
  Westin: "recIPuBC50fv13zRR",
  "Design Hotels": "rec02zPClpWUTCyXM",
});

function newNarrativeId(seed) {
  return `nar_${createHash("sha256").update(seed).digest("hex").slice(0, 16)}`;
}

function normLang(v) {
  const s = String(v || "en").toLowerCase();
  return s.startsWith("es") || s === "spanish" ? "es" : "en";
}

function scenarioTerritoryFromScenarioId(scenarioId) {
  const map = {
    s_cala_independent_affiliation: "Collection / Soft Brand",
    s_cala_collection_affiliation: "Collection / Soft Brand",
    s_cala_soft_brand_shortlist: "Collection / Soft Brand",
    s_mx_collection_affiliation: "Collection / Soft Brand",
    s_cala_conversion: "Conversion",
    s_cala_branded_residences: "Branded Residences",
    s_cala_lifestyle_positioning: "Lifestyle Positioning",
    s_cala_upper_upscale: "Upper-Upscale Positioning",
  };
  return map[scenarioId] || null;
}

/**
 * Load authoritative Stage B stability report only.
 */
export function loadAuthoritativeStabilityReport(reportPath) {
  const rel = reportPath || STAGE_B_AUTHORITATIVE_REPORT_REL_PATH;
  const abs = path.isAbsolute(rel) ? rel : path.join(REPO_ROOT, rel);
  if (!fs.existsSync(abs)) {
    throw new Error(`Authoritative stability report missing: ${rel}`);
  }
  const report = JSON.parse(fs.readFileSync(abs, "utf8"));
  if (report.waveId !== STAGE_B_AUTHORITATIVE_WAVE_ID) {
    throw new Error(
      `Stability report wave mismatch: expected ${STAGE_B_AUTHORITATIVE_WAVE_ID}, got ${report.waveId}`
    );
  }
  if (
    STAGE_B_NON_AUTHORITATIVE_WAVE_IDS.some((w) => w === report.waveId && report.stageBEvidenceCount > 31)
  ) {
    throw new Error("Authoritative stability report appears to include archived wave evidence");
  }
  return { report, path: abs };
}

/**
 * Audit stored evidence for narrative pipeline inputs.
 * @param {object[]} evidence
 */
export function auditNarrativeInputReadiness(evidence = []) {
  let responses = 0;
  let citations = 0;
  let mentions = 0;
  let associationReady = 0;
  let truthReady = 0;

  for (const ev of evidence) {
    const text = ev?.payload?.rawResponseText;
    if (text && String(text).trim()) responses += 1;
    citations += (ev?.payload?.citations || []).length;
    mentions += (ev?.payload?.mentions || []).length;
    if (text && (ev?.payload?.mentions || []).length) associationReady += 1;
    if (text && ev?.responseId && ev?.promptId) truthReady += 1;
  }

  const missing = [];
  if (!responses) missing.push("response_text");
  if (!mentions) missing.push("entity_mentions");
  if (!citations) missing.push("citations");

  const readiness =
    responses >= 100 && mentions >= 100 && citations >= 50
      ? "HIGH"
      : responses >= 20
        ? "MEDIUM"
        : "LOW";

  return {
    RESPONSES_AVAILABLE: responses,
    CITATIONS_AVAILABLE: citations,
    ENTITY_MENTIONS_AVAILABLE: mentions,
    ASSOCIATION_SPANS_AVAILABLE: associationReady,
    TRUTH_SPANS_AVAILABLE: truthReady,
    STABILITY_METADATA_AVAILABLE: fs.existsSync(
      path.join(REPO_ROOT, STAGE_B_AUTHORITATIVE_REPORT_REL_PATH)
    ),
    NARRATIVE_INPUT_READINESS: readiness,
    MISSING_INPUTS: missing,
  };
}

/**
 * Extract narrative observations from one evidence record.
 */
export function extractNarrativeObservationsFromEvidence(evidence, options = {}) {
  const ev = enrichEvidenceWithPromptMetadata(evidence);
  const provenance = resolvePromptProvenance(
    { promptId: ev.promptId, ...(ev.promptMeta || {}) },
    options
  );
  const promptOrigin = provenance?.promptOrigin || ev.promptOrigin || "SCENARIO";

  const { publishable } = classifyAssociationsFromEvidence(ev, options);
  const observations = [];

  for (const row of publishable) {
    const narrativeFamily = mapAttributeToNarrativeFamily(row.attributeId);
    const relationshipToBrand = classifyBrandRelationship(row);
    observations.push({
      narrativeFamily,
      narrativeLabel: NARRATIVE_FAMILY_LABELS[narrativeFamily] || narrativeFamily,
      attributeId: row.attributeId,
      polarity: row.polarity,
      relationshipToBrand,
      evidenceSpan: row.supportingSpan?.text || null,
      evidenceSpanStart: row.supportingSpan?.start ?? null,
      evidenceSpanEnd: row.supportingSpan?.end ?? null,
      brandId: row.entityId,
      brandName: row.entityName,
      responseId: ev.responseId,
      evidenceId: ev.evidenceId,
      promptId: ev.promptId,
      promptOrigin,
      scenarioId: row.scenarioId,
      ownerIntentFamily: ev.intentTerritory || provenance?.ownerIntentSubtheme || null,
      provider: ev.provider,
      language: normLang(ev.language || ev.payload?.language),
      geography: row.geographyKey || ev.commercialRegion || ev.regionName || "CALA",
      timestamp: ev.timestamp,
      citationIds: row.citationIds || [],
      hasProviderCitation: row.hasProviderCitation,
      entityBinding: row.entityBinding,
    });
  }
  return observations;
}

/**
 * Extract all narrative observations from evidence corpus.
 */
export function extractAllNarrativeObservations(evidence = [], options = {}) {
  const all = [];
  for (const ev of evidence) {
    all.push(...extractNarrativeObservationsFromEvidence(ev, options));
  }
  return all;
}

function buildStabilityIndex(stabilityReport) {
  const byPromptProvider = new Map();
  for (const grain of stabilityReport?.grains || []) {
    const key = `${grain.PROMPT_ID}|${grain.PROVIDER}`;
    byPromptProvider.set(key, grain);
  }
  return byPromptProvider;
}

function resolveStabilityContext(observations, stabilityIndex) {
  const keys = new Set(
    observations.map((o) => `${o.promptId}|${String(o.provider || "").toLowerCase()}`)
  );
  const contexts = [];
  for (const key of keys) {
    const grain = stabilityIndex.get(key);
    if (grain) contexts.push(grain);
  }
  if (!contexts.length) {
    return {
      recurrenceState: observations.length >= 3 ? "EARLY_REPEATED" : observations.length >= 2 ? "EARLY_REPEATED" : "ONE_OFF",
      timeWindow: "SAME_RUN_REPETITION",
      label: observations.length >= 2 ? "Early repeated evidence" : "One-off observation",
    };
  }
  const recurrenceStates = [...new Set(contexts.map((c) => c.RECURRENCE_STATE))];
  const timeWindows = [...new Set(contexts.map((c) => c.TIME_WINDOW))];
  return {
    recurrenceState: recurrenceStates.includes("RECURRENT")
      ? "RECURRENT"
      : recurrenceStates.includes("EARLY_REPEATED_EVIDENCE")
        ? "EARLY_REPEATED"
        : recurrenceStates[0] || "ONE_OFF",
    timeWindow: timeWindows.includes("SHORT_TERM") ? "SHORT_TERM" : timeWindows[0] || "SAME_RUN_REPETITION",
    grains: contexts.map((c) => ({
      promptId: c.PROMPT_ID,
      provider: c.PROVIDER,
      observations: c.OBSERVATIONS,
      recurrenceState: c.RECURRENCE_STATE,
      timeWindow: c.TIME_WINDOW,
    })),
  };
}

function isScenarioEligible(brandId, scenarioId, eligibilityConfig) {
  const territory = scenarioTerritoryFromScenarioId(scenarioId);
  if (!territory) return { eligible: true, reason: "unmapped_scenario_pass_through" };
  const row = getBrandDecisionEligibility(brandId, territory, eligibilityConfig);
  if (row.state === "NOT_ELIGIBLE") {
    return { eligible: false, reason: row.reason, territory };
  }
  return { eligible: true, reason: row.reason || "eligible", territory };
}

function classifyMateriality(observationCount, providerCount, scenarioCount, competitorGap) {
  if (observationCount >= 5 && providerCount >= 2) return "MATERIAL";
  if (observationCount >= 3 || providerCount >= 2) return "RELEVANT";
  if (competitorGap === "COMPETITOR_STRONGER") return "RELEVANT";
  if (observationCount >= 1) return "CONTEXTUAL";
  return "INSUFFICIENT_CONTEXT";
}

function classifyDisposition({ productionState, materiality, observationCount, relationshipToBrand }) {
  if (!isExecutiveSafeBrandRelationship(relationshipToBrand)) return "INSUFFICIENT_EVIDENCE";
  if (productionState === "RESEARCH_ONLY" || productionState === "BLOCKED") return "MONITOR_ONLY";
  if (materiality === "MATERIAL" && observationCount >= 3) return "REVIEW_REQUIRED";
  if (materiality === "RELEVANT") return "REVIEW_REQUIRED";
  if (observationCount === 1) return "MONITOR_ONLY";
  return "NO_ACTION_EXPECTED_POSITIONING";
}

/**
 * Aggregate observations into qualified narrative objects for one brand.
 */
export function aggregateBrandNarratives(args = {}) {
  const {
    brandId,
    brandName,
    observations = [],
    stabilityReport = null,
    eligibilityConfig = loadDecisionEligibilityConfig(),
    familyPrecision = {},
    minObservations = 1,
  } = args;

  const stabilityIndex = buildStabilityIndex(stabilityReport);
  const filtered = observations.filter((o) => o.brandId === brandId);
  const byKey = new Map();

  for (const o of filtered) {
    const key = `${o.narrativeFamily}|${o.polarity}|${o.relationshipToBrand}`;
    if (!byKey.has(key)) byKey.set(key, []);
    byKey.get(key).push(o);
  }

  const narratives = [];
  for (const [key, obs] of byKey.entries()) {
    if (obs.length < minObservations) continue;
    const [narrativeFamily, polarity] = key.split("|");
    const relationshipToBrand = obs[0].relationshipToBrand;
    if (relationshipToBrand === "GENERAL_CONTEXT") continue;

    const responseIds = [...new Set(obs.map((o) => o.responseId).filter(Boolean))];
    const promptIds = [...new Set(obs.map((o) => o.promptId).filter(Boolean))];
    const providers = [...new Set(obs.map((o) => o.provider).filter(Boolean))];
    const languages = [...new Set(obs.map((o) => o.language).filter(Boolean))];
    const geographies = [...new Set(obs.map((o) => o.geography).filter(Boolean))];
    const scenarios = [...new Set(obs.map((o) => o.scenarioId).filter(Boolean))];
    const promptOrigins = [...new Set(obs.map((o) => o.promptOrigin).filter(Boolean))];
    const ownerIntents = [...new Set(obs.map((o) => o.ownerIntentFamily).filter(Boolean))];

    const scenarioEligible = scenarios.every((sid) => {
      const check = isScenarioEligible(brandId, sid, eligibilityConfig);
      return check.eligible;
    });
    if (!scenarioEligible && scenarios.length) continue;

    const comparableResponseCount = responseIds.length;
    const timestamps = obs.map((o) => o.timestamp).filter(Boolean).sort();
    const stabilityContext = resolveStabilityContext(obs, stabilityIndex);
    const productionState = classifyNarrativeFamilyProductionState(narrativeFamily, {
      validationPrecision: familyPrecision[narrativeFamily] ?? familyPrecision.ALL ?? null,
      hasEvidence: true,
    });
    const materiality = classifyMateriality(
      obs.length,
      providers.length,
      scenarios.length,
      null
    );
    const reviewDisposition = classifyDisposition({
      productionState,
      materiality,
      observationCount: obs.length,
      relationshipToBrand,
    });

    narratives.push({
      narrativeId: newNarrativeId(`${brandId}|${key}`),
      brandId,
      brandName,
      narrativeFamily,
      narrativeLabel: NARRATIVE_FAMILY_LABELS[narrativeFamily] || narrativeFamily,
      polarity,
      relationshipToBrand,
      evidenceSpans: obs.slice(0, 8).map((o) => ({
        text: o.evidenceSpan,
        responseId: o.responseId,
        promptId: o.promptId,
        provider: o.provider,
        language: o.language,
        brandId: o.brandId,
      })),
      responseIds,
      promptIds,
      promptOrigins,
      scenarioIds: scenarios,
      ownerIntentFamilies: ownerIntents,
      providers,
      languages,
      geographies,
      observationCount: obs.length,
      comparableResponseCount,
      firstObservedAt: timestamps[0] || null,
      lastObservedAt: timestamps[timestamps.length - 1] || null,
      recurrence: {
        label: `Appeared in ${obs.length} observation(s) across ${comparableResponseCount} comparable response(s)`,
        responseCoverage: `${comparableResponseCount} responses`,
        providerCoverage: providers.length,
        promptCoverage: promptIds.length,
      },
      stabilityContext,
      materiality,
      productionState,
      reviewDisposition,
    });
  }

  return narratives.sort((a, b) => b.observationCount - a.observationCount);
}

export function buildPortfolioNarratives(args = {}) {
  const {
    evidence = [],
    stabilityReport = null,
    portfolio = PORTFOLIO_BRANDS,
    familyPrecision = {},
  } = args;
  const observations = extractAllNarrativeObservations(evidence, args);
  const byBrand = {};
  for (const [brandName, brandId] of Object.entries(portfolio)) {
    byBrand[brandName] = aggregateBrandNarratives({
      brandId,
      brandName,
      observations,
      stabilityReport,
      familyPrecision,
    });
  }
  return { observations, byBrand };
}
