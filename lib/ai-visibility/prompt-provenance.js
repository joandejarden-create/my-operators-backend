/**
 * Prompt origin / observed-demand provenance (Hotel Brand AI Intelligence).
 *
 * Origin answers: where did this question come from?
 * Owner Intent answers: what owner decision does it represent?
 * Scenario answers: what commercial context is being tested?
 *
 * Orthogonal to scenarioId. Does not change certified measurement.
 * Does not invent search volume or numeric confidence.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  buildScenarioRegistryIndex,
  loadScenarioRegistry,
  resolvePromptScenario,
} from "./scenario-registry.js";
import { KNOWN_AI_VISIBILITY_PROVIDER_IDS } from "./provider-dimension.js";
import { buildPromptMetadataById } from "./associations/prompt-metadata-lookup.js";

function resolveEstimatedUsdPerCallLocal() {
  const n = Number(process.env.AI_VISIBILITY_EST_USD_PER_CALL || "0.25");
  return Number.isFinite(n) && n >= 0 ? n : 0.25;
}

export const PROMPT_PROVENANCE_VERSION = "ai_visibility_prompt_provenance_v1";
export const DEMAND_SIGNAL_REGISTRY_VERSION = "ai_visibility_demand_signals_v1";
export const OBSERVED_DEMAND_SEED_VERSION = "ai_visibility_observed_demand_seed_v1";

export const PROMPT_ORIGINS = Object.freeze([
  "OBSERVED",
  "DERIVED",
  "SCENARIO",
  "LEGACY_UNCLASSIFIED",
]);

export const CLIENT_PROMPT_ORIGINS = Object.freeze(["OBSERVED", "DERIVED", "SCENARIO"]);

export const ORIGIN_SOURCE_TYPES = Object.freeze([
  "SEARCH_DEMAND_DATASET",
  "PAA",
  "RELATED_QUESTION",
  "SEARCH_QUERY_DATASET",
  "PUBLIC_QUESTION_SOURCE",
  "SEARCH_CONSOLE",
  "DEALALITY_USER_BEHAVIOR",
  "LICENSED_SEO_DATASET",
  "OTHER_OBSERVED_SOURCE",
  "EXPERT_SCENARIO",
]);

export const DEMAND_TIERS = Object.freeze(["HIGH", "MEDIUM", "LOW", "UNKNOWN"]);

export const SOURCE_CONFIDENCE = Object.freeze([
  "DIRECT_MEASURED",
  "STRONG_OBSERVED",
  "SUPPORTED",
  "WEAK",
  "UNKNOWN",
]);

export const PROVENANCE_STATUSES = Object.freeze([
  "VALIDATED",
  "CANDIDATE",
  "NEEDS_EVIDENCE",
  "LEGACY",
]);

export const SAMPLING_PRIORITIES = Object.freeze([
  "CRITICAL",
  "HIGH",
  "STANDARD",
  "EXPLORATORY",
]);

export const CREATED_BY_METHODS = Object.freeze([
  "OBSERVED_DEMAND",
  "DERIVED_FROM_OBSERVED",
  "EXPERT_SCENARIO",
  "LEGACY_UNCLASSIFIED",
]);

/** Prompt-origin source vs AI-response citation source — never conflate. */
export const SOURCE_NAMESPACES = Object.freeze({
  PROMPT_ORIGIN: "PROMPT_ORIGIN_SOURCE",
  RESPONSE_CITATION: "RESPONSE_CITATION_SOURCE",
});

export const GAP_DEMAND_PRIORITIZATION_HOOK = Object.freeze({
  ENABLED: false,
  USES_DEMAND_TIER: false,
  RECALCULATES_P0C: false,
  note: "Metadata hook only. Competitive gap raw P0C logic is unchanged.",
});

export const OBSERVED_PROMPT_MIX_MIN_THEMES = 10;

export const CLIENT_PROMPT_ORIGIN_COPY = Object.freeze({
  lensesCurrent:
    "Our prompt architecture distinguishes observed demand from expert scenario intelligence. The current monitored library is scenario-led while observed demand sources are being validated.",
  lensesAfterActivation:
    "We use both observed demand and expert scenario intelligence.",
  lenses:
    "Our prompt architecture distinguishes observed demand from expert scenario intelligence. The current monitored library is scenario-led while observed demand sources are being validated.",
  methodology:
    "Observed-demand prompts are grounded in external query or demand signals. Scenario prompts are expert-designed owner/developer decision situations.",
  observedBadge: "Observed",
  derivedBadge: "Derived",
  scenarioBadge: "Scenario",
  derivedFromPrefix: "Derived from observed theme",
  expertScenario: "Expert scenario",
});

/**
 * Qualitative demand tier — no invented monthly volume.
 * HIGH / MEDIUM / LOW require a recorded methodology plus supporting evidence.
 */
export const DEMAND_TIER_METHOD = Object.freeze({
  version: "ai_visibility_demand_tier_v1",
  exactVolumeInvented: false,
  numericConfidence: false,
  assignmentRule:
    "HIGH, MEDIUM, and LOW require demandMethodology plus at least one demand evidence object whose sourceConfidence is not UNKNOWN. Otherwise demandTier = UNKNOWN.",
  HIGH:
    "Methodology recorded and evidence meets the high bar: licensed exact-query volume in the top relative band of a named dataset; or Search Console query with material impressions/clicks; or the same query/theme recurring across three or more independent observed sources.",
  MEDIUM:
    "Methodology recorded and evidence meets the medium bar: licensed relative rank in the mid band; or repeated PAA / related-question appearance across dates; or Search Console query with measurable but modest impressions.",
  LOW:
    "Methodology recorded and evidence meets the low bar: a supported observed appearance (lower licensed rank band, or repeated public-question theme) without comparable volume.",
  UNKNOWN:
    "Default when no comparable demand methodology is available. Acceptable. Do not display as High demand or as search volume.",
});

const FORBIDDEN_VOLUME_KEYS = Object.freeze([
  "monthlySearchVolume",
  "searchVolume",
  "exactSearchVolume",
  "monthlyVolume",
  "ownerSearchesPerMonth",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FIXTURES_DIR = path.join(__dirname, "..", "..", "fixtures", "ai-visibility");
const DEFAULT_OVERLAY_PATH = path.join(FIXTURES_DIR, "prompt-provenance-v1.json");
const DEFAULT_SIGNALS_PATH = path.join(FIXTURES_DIR, "demand-signals-v1.json");
const DEFAULT_SEED_PATH = path.join(FIXTURES_DIR, "observed-demand-seed-v1.json");

let overlayCache = null;
let signalsCache = null;
let seedCache = null;
let scenarioIndexCache = null;

function normStr(v) {
  if (v == null) return null;
  const s = String(v).trim();
  return s ? s : null;
}

export function resetPromptProvenanceCaches() {
  overlayCache = null;
  signalsCache = null;
  seedCache = null;
  scenarioIndexCache = null;
}

export function loadPromptProvenanceOverlay(overlayPath = DEFAULT_OVERLAY_PATH) {
  if (overlayPath === DEFAULT_OVERLAY_PATH && overlayCache) return overlayCache;
  const raw = fs.existsSync(overlayPath)
    ? JSON.parse(fs.readFileSync(overlayPath, "utf8"))
    : { overlayVersion: PROMPT_PROVENANCE_VERSION, classifications: [] };
  const byPromptId = new Map();
  for (const row of raw.classifications || []) {
    if (row?.promptId) byPromptId.set(row.promptId, row);
  }
  const loaded = {
    ...raw,
    overlayVersion: raw.overlayVersion || PROMPT_PROVENANCE_VERSION,
    overlayPath,
    byPromptId,
  };
  if (overlayPath === DEFAULT_OVERLAY_PATH) overlayCache = loaded;
  return loaded;
}

export function loadDemandSignalRegistry(signalsPath = DEFAULT_SIGNALS_PATH) {
  if (signalsPath === DEFAULT_SIGNALS_PATH && signalsCache) return signalsCache;
  const raw = fs.existsSync(signalsPath)
    ? JSON.parse(fs.readFileSync(signalsPath, "utf8"))
    : { registryVersion: DEMAND_SIGNAL_REGISTRY_VERSION, signals: [] };
  const byId = new Map();
  for (const s of raw.signals || []) {
    if (s?.demandSignalId) byId.set(s.demandSignalId, s);
  }
  const loaded = {
    ...raw,
    registryVersion: raw.registryVersion || DEMAND_SIGNAL_REGISTRY_VERSION,
    signalsPath,
    byId,
  };
  if (signalsPath === DEFAULT_SIGNALS_PATH) signalsCache = loaded;
  return loaded;
}

export function loadObservedDemandSeed(seedPath = DEFAULT_SEED_PATH) {
  if (seedPath === DEFAULT_SEED_PATH && seedCache) return seedCache;
  const raw = fs.existsSync(seedPath)
    ? JSON.parse(fs.readFileSync(seedPath, "utf8"))
    : {
        seedVersion: OBSERVED_DEMAND_SEED_VERSION,
        seedStatus: "OBSERVED_DEMAND_SEED_BLOCKED",
        themes: [],
      };
  const loaded = {
    ...raw,
    seedVersion: raw.seedVersion || OBSERVED_DEMAND_SEED_VERSION,
    seedPath,
  };
  if (seedPath === DEFAULT_SEED_PATH) seedCache = loaded;
  return loaded;
}

function getScenarioIndex(options = {}) {
  if (options.scenarioIndex) return options.scenarioIndex;
  if (scenarioIndexCache) return scenarioIndexCache;
  const registry = options.registry || loadScenarioRegistry(options.registryPath);
  scenarioIndexCache = buildScenarioRegistryIndex(registry);
  return scenarioIndexCache;
}

function hasForbiddenVolume(obj) {
  if (!obj || typeof obj !== "object") return [];
  return FORBIDDEN_VOLUME_KEYS.filter((k) => obj[k] != null && obj[k] !== "");
}

/**
 * @param {object} evidence
 * @returns {{ ok: boolean, errors: string[] }}
 */
export function validateDemandEvidence(evidence = {}) {
  const errors = [];
  if (!normStr(evidence.demandSignalId) && !normStr(evidence.evidenceReference)) {
    errors.push("demand_evidence_requires_id_or_reference");
  }
  if (!normStr(evidence.sourceType)) errors.push("demand_evidence_missing_source_type");
  else if (!ORIGIN_SOURCE_TYPES.includes(evidence.sourceType)) {
    errors.push(`unsupported_origin_source_type:${evidence.sourceType}`);
  }
  if (evidence.sourceType === "EXPERT_SCENARIO") {
    errors.push("expert_scenario_is_not_observed_evidence");
  }
  if (!normStr(evidence.sourceName)) errors.push("demand_evidence_missing_source_name");
  if (evidence.sourceConfidence && !SOURCE_CONFIDENCE.includes(evidence.sourceConfidence)) {
    errors.push(`unsupported_source_confidence:${evidence.sourceConfidence}`);
  }
  if (typeof evidence.sourceConfidence === "number") {
    errors.push("numeric_confidence_forbidden");
  }
  errors.push(...hasForbiddenVolume(evidence).map((k) => `forbidden_volume_field:${k}`));
  if (evidence.sourceNamespace === SOURCE_NAMESPACES.RESPONSE_CITATION) {
    errors.push("prompt_origin_must_not_use_citation_source_namespace");
  }
  return { ok: errors.length === 0, errors };
}

function collectEvidence(record, signalRegistry) {
  const list = [];
  if (Array.isArray(record.demandEvidence)) list.push(...record.demandEvidence);
  if (Array.isArray(record.demandSignalIds)) {
    for (const id of record.demandSignalIds) {
      const sig = signalRegistry?.byId?.get(id);
      if (sig) list.push(sig);
      else list.push({ demandSignalId: id, _unresolved: true });
    }
  }
  if (record.demandSignalId && signalRegistry?.byId?.has(record.demandSignalId)) {
    list.push(signalRegistry.byId.get(record.demandSignalId));
  }
  return list;
}

function evidenceSupportsObserved(evidenceList) {
  const usable = evidenceList.filter((e) => !e._unresolved);
  if (!usable.length) return { ok: false, errors: ["observed_requires_provenance_evidence"] };
  const errors = [];
  for (const e of usable) {
    const v = validateDemandEvidence(e);
    if (!v.ok) errors.push(...v.errors);
  }
  const unresolved = evidenceList.filter((e) => e._unresolved);
  if (unresolved.length) {
    errors.push(
      ...unresolved.map((e) => `unresolved_demand_signal:${e.demandSignalId}`)
    );
  }
  return { ok: errors.length === 0, errors, evidence: usable };
}

function methodologyAllowsTier(tier, record, evidenceList) {
  if (!tier || tier === "UNKNOWN") return { ok: true };
  if (!DEMAND_TIERS.includes(tier)) {
    return { ok: false, errors: [`unsupported_demand_tier:${tier}`] };
  }
  if (!normStr(record.demandMethodology)) {
    return { ok: false, errors: ["demand_tier_requires_methodology"] };
  }
  const confidences = (evidenceList || [])
    .map((e) => e.sourceConfidence)
    .filter(Boolean);
  if (!confidences.length || confidences.every((c) => c === "UNKNOWN")) {
    return { ok: false, errors: ["demand_tier_requires_non_unknown_evidence"] };
  }
  return { ok: true };
}

/**
 * Validate an explicit provenance overlay / Airtable row. Does not infer origin from wording.
 * @param {object} record
 * @param {{ overlay?: object, signals?: object, promptsById?: Map }} [ctx]
 */
export function validateProvenanceRecord(record = {}, ctx = {}) {
  const errors = [];
  const origin = normStr(record.promptOrigin);
  if (!origin) {
    return { ok: false, errors: ["missing_prompt_origin"] };
  }
  if (!PROMPT_ORIGINS.includes(origin)) {
    return { ok: false, errors: [`unsupported_prompt_origin:${origin}`] };
  }

  errors.push(...hasForbiddenVolume(record).map((k) => `forbidden_volume_field:${k}`));
  if (typeof record.confidence === "number" || typeof record.sourceConfidence === "number") {
    errors.push("numeric_confidence_forbidden");
  }
  if (record.originSourceNamespace === SOURCE_NAMESPACES.RESPONSE_CITATION) {
    errors.push("prompt_origin_must_not_use_citation_source_namespace");
  }

  const signals = ctx.signals || loadDemandSignalRegistry(ctx.signalsPath);
  const evidenceList = collectEvidence(record, signals);

  if (origin === "OBSERVED") {
    const ev = evidenceSupportsObserved(evidenceList);
    if (!ev.ok) errors.push(...ev.errors);
    if (!normStr(record.originSourceType) && !ev.evidence?.length) {
      errors.push("observed_requires_origin_source_type");
    }
  }

  if (origin === "DERIVED") {
    const parentPromptId = normStr(record.derivedFromObservedPromptId);
    const parentSignalId = normStr(record.derivedFromDemandSignalId);
    if (!parentPromptId && !parentSignalId) {
      errors.push("derived_requires_observed_parent");
    }
    if (parentPromptId) {
      const parent =
        ctx.promptsById?.get(parentPromptId) ||
        ctx.overlay?.byPromptId?.get(parentPromptId) ||
        null;
      const parentOrigin = parent?.promptOrigin || parent?.resolvedOrigin;
      if (parent && parentOrigin && parentOrigin !== "OBSERVED") {
        errors.push("derived_parent_must_be_observed");
      }
      if (!parent && ctx.requireResolvedParent) {
        errors.push(`derived_parent_prompt_not_found:${parentPromptId}`);
      }
    }
    if (parentSignalId && !signals.byId.has(parentSignalId) && ctx.requireResolvedParent) {
      errors.push(`derived_parent_signal_not_found:${parentSignalId}`);
    }
  }

  if (origin === "SCENARIO") {
    // Demand evidence not required. scenarioId may be absent (still expert-created).
  }

  const tierCheck = methodologyAllowsTier(record.demandTier, record, evidenceList);
  if (!tierCheck.ok) errors.push(...tierCheck.errors);

  return { ok: errors.length === 0, errors };
}

export function computeSamplingPriority(input = {}) {
  const client = normStr(input.clientPriority);
  if (client && SAMPLING_PRIORITIES.includes(client)) return client;
  const commercial = normStr(input.commercialPriority);
  if (commercial === "CRITICAL") return "CRITICAL";
  if (input.demandTier === "HIGH") return "HIGH";
  if (commercial === "HIGH") return "HIGH";
  if (commercial === "INVESTIGATION" || input.demandTier === "LOW") return "EXPLORATORY";
  return "STANDARD";
}

function emptyProvenanceBase(prompt = {}) {
  return {
    promptId: prompt.promptId || null,
    promptOrigin: "LEGACY_UNCLASSIFIED",
    originSourceType: null,
    originSourceName: null,
    originSourceReference: null,
    originSourceNamespace: SOURCE_NAMESPACES.PROMPT_ORIGIN,
    observedQuery: null,
    observedTheme: null,
    demandTier: "UNKNOWN",
    demandSignalType: null,
    demandGeography: null,
    dateObserved: null,
    demandEvidenceCount: 0,
    demandMethodology: null,
    derivedFromObservedPromptId: null,
    derivedFromDemandSignalId: null,
    scenarioId: prompt.scenarioId || null,
    ownerIntentFamily: prompt.intentTerritory || null,
    ownerIntentSubtheme: null,
    provenanceStatus: "LEGACY",
    provenanceNotes: null,
    createdByMethod: "LEGACY_UNCLASSIFIED",
    lastProvenanceReviewAt: null,
    samplingPriority: computeSamplingPriority({
      commercialPriority: prompt.commercialPriority,
      demandTier: "UNKNOWN",
    }),
    provenanceVersion: PROMPT_PROVENANCE_VERSION,
  };
}

/**
 * Resolve origin without inferring from wording.
 * Overlay (validated) > Airtable origin (validated) > scenario mapping → SCENARIO > LEGACY_UNCLASSIFIED.
 */
export function resolvePromptProvenance(prompt = {}, options = {}) {
  const overlay = options.overlay || loadPromptProvenanceOverlay(options.overlayPath);
  const signals = options.signals || loadDemandSignalRegistry(options.signalsPath);
  const scenarioIndex = getScenarioIndex(options);
  const scenario = resolvePromptScenario(prompt, scenarioIndex);

  const overlayRow = overlay.byPromptId.get(prompt.promptId) || null;
  const airtableRow =
    prompt.promptOrigin && PROMPT_ORIGINS.includes(prompt.promptOrigin)
      ? {
          promptOrigin: prompt.promptOrigin,
          originSourceType: prompt.originSourceType,
          originSourceName: prompt.originSourceName,
          originSourceReference: prompt.originSourceReference,
          observedQuery: prompt.observedQuery,
          observedTheme: prompt.observedTheme,
          demandTier: prompt.demandTier,
          demandSignalType: prompt.demandSignalType,
          demandGeography: prompt.demandGeography,
          dateObserved: prompt.dateObserved,
          demandEvidenceCount: prompt.demandEvidenceCount,
          demandMethodology: prompt.demandMethodology,
          derivedFromObservedPromptId: prompt.derivedFromObservedPromptId,
          derivedFromDemandSignalId: prompt.derivedFromDemandSignalId,
          ownerIntentSubtheme: prompt.ownerIntentSubtheme,
          provenanceStatus: prompt.provenanceStatus,
          provenanceNotes: prompt.provenanceNotes,
          createdByMethod: prompt.createdByMethod,
          lastProvenanceReviewAt: prompt.lastProvenanceReviewAt,
          samplingPriority: prompt.samplingPriority,
          demandEvidence: prompt.demandEvidence,
          demandSignalIds: prompt.demandSignalIds,
          demandSignalId: prompt.demandSignalId,
        }
      : null;

  const candidate = overlayRow || airtableRow;
  let resolved = emptyProvenanceBase({
    ...prompt,
    scenarioId: scenario.scenarioId,
    commercialPriority: scenario.commercialPriority,
  });
  resolved.scenarioId = scenario.scenarioId || prompt.scenarioId || null;
  resolved.ownerIntentFamily = prompt.intentTerritory || null;

  if (candidate) {
    const merged = { ...resolved, ...candidate, promptId: prompt.promptId };
    const v = validateProvenanceRecord(merged, {
      overlay,
      signals,
      promptsById: options.promptsById,
      requireResolvedParent: Boolean(options.requireResolvedParent),
    });
    if (v.ok) {
      resolved = {
        ...resolved,
        ...merged,
        originSourceNamespace: SOURCE_NAMESPACES.PROMPT_ORIGIN,
        provenanceStatus: merged.provenanceStatus || "VALIDATED",
        demandTier: merged.demandTier || "UNKNOWN",
        demandEvidenceCount: Array.isArray(merged.demandEvidence)
          ? merged.demandEvidence.length
          : Number(merged.demandEvidenceCount) || 0,
        samplingPriority:
          merged.samplingPriority ||
          computeSamplingPriority({
            demandTier: merged.demandTier,
            commercialPriority: scenario.commercialPriority,
            clientPriority: merged.clientPriority,
          }),
      };
      return resolved;
    }
    resolved.provenanceNotes = `overlay_invalid:${v.errors.join(",")}`;
  }

  if (scenario.scenarioStatus === "MAPPED") {
    resolved.promptOrigin = "SCENARIO";
    resolved.originSourceType = "EXPERT_SCENARIO";
    resolved.originSourceName = "Dealality Owner Decision Scenario registry";
    resolved.originSourceReference = scenario.scenarioId;
    resolved.provenanceStatus = "VALIDATED";
    resolved.createdByMethod = "EXPERT_SCENARIO";
    resolved.demandTier = "UNKNOWN";
    resolved.demandMethodology = null;
    resolved.samplingPriority = computeSamplingPriority({
      commercialPriority: scenario.commercialPriority,
      demandTier: "UNKNOWN",
    });
    return resolved;
  }

  return resolved;
}

export function attachPromptProvenance(prompt = {}, options = {}) {
  const provenance = resolvePromptProvenance(prompt, options);
  return { ...prompt, provenance, ...pickAttachFields(provenance) };
}

function pickAttachFields(provenance) {
  return {
    promptOrigin: provenance.promptOrigin,
    originSourceType: provenance.originSourceType,
    originSourceName: provenance.originSourceName,
    originSourceReference: provenance.originSourceReference,
    originSourceNamespace: provenance.originSourceNamespace,
    observedQuery: provenance.observedQuery,
    observedTheme: provenance.observedTheme,
    demandTier: provenance.demandTier,
    demandSignalType: provenance.demandSignalType,
    demandGeography: provenance.demandGeography,
    dateObserved: provenance.dateObserved,
    demandEvidenceCount: provenance.demandEvidenceCount,
    demandMethodology: provenance.demandMethodology,
    derivedFromObservedPromptId: provenance.derivedFromObservedPromptId,
    derivedFromDemandSignalId: provenance.derivedFromDemandSignalId,
    ownerIntentSubtheme: provenance.ownerIntentSubtheme,
    provenanceStatus: provenance.provenanceStatus,
    provenanceNotes: provenance.provenanceNotes,
    createdByMethod: provenance.createdByMethod,
    lastProvenanceReviewAt: provenance.lastProvenanceReviewAt,
    samplingPriority: provenance.samplingPriority,
  };
}

/**
 * Client-safe origin fields. LEGACY_UNCLASSIFIED is not a positive UI category.
 */
export function toClientPromptOrigin(record = {}) {
  const origin = record.promptOrigin;
  const show = CLIENT_PROMPT_ORIGINS.includes(origin);
  if (!show) {
    return {
      promptOrigin: null,
      demandTier: null,
      originBadge: null,
      originDetail: null,
      showOriginBadge: false,
    };
  }
  let originBadge = CLIENT_PROMPT_ORIGIN_COPY.scenarioBadge;
  let originDetail = CLIENT_PROMPT_ORIGIN_COPY.expertScenario;
  if (origin === "OBSERVED") {
    originBadge = CLIENT_PROMPT_ORIGIN_COPY.observedBadge;
    const bits = [
      record.originSourceType || null,
      record.demandTier && record.demandTier !== "UNKNOWN" ? record.demandTier : null,
      record.dateObserved || null,
    ].filter(Boolean);
    originDetail = bits.join(" · ") || null;
  } else if (origin === "DERIVED") {
    originBadge = CLIENT_PROMPT_ORIGIN_COPY.derivedBadge;
    originDetail = record.observedTheme
      ? `${CLIENT_PROMPT_ORIGIN_COPY.derivedFromPrefix}: ${record.observedTheme}`
      : CLIENT_PROMPT_ORIGIN_COPY.derivedFromPrefix;
  }
  return {
    promptOrigin: origin,
    demandTier: origin === "OBSERVED" ? record.demandTier || "UNKNOWN" : null,
    originBadge,
    originDetail,
    showOriginBadge: true,
    originSourceType: origin === "OBSERVED" ? record.originSourceType || null : null,
    dateObserved: origin === "OBSERVED" ? record.dateObserved || null : null,
    observedTheme: origin === "DERIVED" ? record.observedTheme || null : null,
  };
}

export function shouldShowExecutivePromptMix(summary = {}) {
  const observed = Number(summary.observed) || 0;
  return observed >= OBSERVED_PROMPT_MIX_MIN_THEMES;
}

export function buildPromptOriginSummary(prompts = [], options = {}) {
  const counts = {
    observed: 0,
    derived: 0,
    scenario: 0,
    legacyUnclassified: 0,
  };
  const resolved = [];
  for (const p of prompts) {
    const rec = p.provenance || resolvePromptProvenance(p, options);
    resolved.push(rec);
    if (rec.promptOrigin === "OBSERVED") counts.observed += 1;
    else if (rec.promptOrigin === "DERIVED") counts.derived += 1;
    else if (rec.promptOrigin === "SCENARIO") counts.scenario += 1;
    else counts.legacyUnclassified += 1;
  }
  const summary = {
    version: PROMPT_PROVENANCE_VERSION,
    total: prompts.length,
    ...counts,
    showPromptMix: false,
    CLIENT_COPY: CLIENT_PROMPT_ORIGIN_COPY.methodology,
    EXECUTIVE_STORY: CLIENT_PROMPT_ORIGIN_COPY.lenses,
  };
  summary.showPromptMix = shouldShowExecutivePromptMix(summary);
  return summary;
}

export function enrichRowWithPromptOriginFromLibrary(row = {}, options = {}) {
  const meta = row.promptId ? buildPromptMetadataById().get(row.promptId) : null;
  return enrichRowWithPromptOrigin(row, {
    ...options,
    prompt: options.prompt || meta || undefined,
  });
}

export function listGovernedLibraryPrompts(effectiveGeo = null) {
  const all = [...buildPromptMetadataById().values()].filter(
    (p) => p.active !== false && p.monitoringEligible !== false
  );
  const region = effectiveGeo?.commercialRegion || (effectiveGeo?.key !== "Global" ? effectiveGeo?.key : null);
  if (!region) return all;
  const scoped = all.filter((p) => {
    if (p.geographyScope === "Global") return true;
    return String(p.commercialRegion || "").toLowerCase() === String(region).toLowerCase();
  });
  return scoped.length ? scoped : all;
}

export function buildLibraryPromptOriginSummary(effectiveGeo = null, options = {}) {
  return buildPromptOriginSummary(listGovernedLibraryPrompts(effectiveGeo), options);
}

export function enrichRowWithPromptOrigin(row = {}, options = {}) {
  const prompt = options.prompt || {
    promptId: row.promptId,
    promptFamily: row.promptFamily || row.PROMPT_FAMILY || row.intentTerritory,
    intentTerritory: row.intentTerritory || row.PROMPT_FAMILY,
    version: row.promptVersion || row.version || "1",
    active: true,
    monitoringEligible: true,
    geographyScope: row.geographyScope || "Region",
    entityScope: "Brand",
    promptText: row.question || row.QUESTION || "",
    language: row.language,
    commercialRegion: row.commercialRegion || row.REGION,
    country: row.country,
  };
  const provenance = resolvePromptProvenance(prompt, options);
  const client = toClientPromptOrigin(provenance);
  return {
    ...row,
    promptOrigin: client.promptOrigin,
    demandTier: client.demandTier,
    originBadge: client.originBadge,
    originDetail: client.originDetail,
    showOriginBadge: client.showOriginBadge,
    originSourceType: client.originSourceType,
    dateObserved: client.dateObserved,
    observedTheme: client.observedTheme,
  };
}

export function auditPromptUniverse(prompts = [], options = {}) {
  const active = prompts.filter((p) => p.active !== false && p.monitoringEligible !== false);
  const summary = buildPromptOriginSummary(active, options);
  const scenarioIndex = getScenarioIndex(options);
  let currentlyScenarioMapped = 0;
  for (const p of active) {
    const s = resolvePromptScenario(p, scenarioIndex);
    if (s.scenarioStatus === "MAPPED") currentlyScenarioMapped += 1;
  }
  const overlay = options.overlay || loadPromptProvenanceOverlay(options.overlayPath);
  const observedWithExistingEvidence = [...overlay.byPromptId.values()].filter(
    (r) => r.promptOrigin === "OBSERVED"
  ).length;
  return {
    TOTAL_ACTIVE_PROMPTS: active.length,
    CURRENTLY_SCENARIO_MAPPED: currentlyScenarioMapped,
    LIKELY_SCENARIO: summary.scenario,
    POSSIBLE_DERIVED: 0,
    OBSERVED_WITH_EXISTING_EVIDENCE: observedWithExistingEvidence,
    UNKNOWN_ORIGIN: summary.legacyUnclassified,
    OBSERVED: summary.observed,
    DERIVED: summary.derived,
    SCENARIO: summary.scenario,
    LEGACY_UNCLASSIFIED: summary.legacyUnclassified,
    autoRelabelForbidden: true,
  };
}

export function estimateProvenanceMonitoringCost(input = {}) {
  const existing = Number(input.existingPrompts) || 0;
  const proposedObserved = Number(input.proposedObserved) || 0;
  const proposedDerived = Number(input.proposedDerived) || 0;
  const incremental = proposedObserved + proposedDerived;
  const providers = Array.isArray(input.providers)
    ? input.providers
    : [...KNOWN_AI_VISIBILITY_PROVIDER_IDS];
  const usdPerCall =
    typeof input.usdPerCall === "number" ? input.usdPerCall : resolveEstimatedUsdPerCallLocal();
  const callsPerRun = (existing + incremental) * providers.length;
  const costPerRun = callsPerRun * usdPerCall;
  return {
    EXISTING_PROMPTS: existing,
    PROPOSED_OBSERVED: proposedObserved,
    PROPOSED_DERIVED: proposedDerived,
    TOTAL_INCREMENT: incremental,
    PROVIDERS: providers.length,
    USD_PER_CALL: usdPerCall,
    CALLS_PER_FULL_RUN: callsPerRun,
    COST_PER_FULL_RUN: costPerRun,
    WEEKLY_MONTHLY_COST: costPerRun * (52 / 12),
    BIWEEKLY_MONTHLY_COST: costPerRun * (26 / 12),
    MONTHLY_COST: costPerRun,
    PROVIDER_CALLS: 0,
    MONITORING_RUNS: 0,
  };
}

export const EXISTING_OBSERVED_SIGNAL_SOURCES = Object.freeze([
  {
    SOURCE: "Brand website Search Console / discoverability adapters",
    METHOD: "Owned-domain indexability and public crawl — not owner-query demand",
    SIGNALS_FOUND: 0,
    USABLE: "NO",
  },
  {
    SOURCE: "AI Visibility prompt Source / Rationale",
    METHOD: "Dealality design notes for governed scenario prompts",
    SIGNALS_FOUND: 0,
    USABLE: "NO",
  },
  {
    SOURCE: "DataForSEO Keywords Data + Google Organic SERP (existing account)",
    METHOD: "Google Ads search volume live + SERP PAA/related; sample + targeted refinement 2026-08-17",
    SIGNALS_FOUND: 13,
    DISTINCT_THEMES: 9,
    USABLE: "FILE_STORE_ONLY",
    LIVE_MONITORING: "NO",
    BLOCKER: null,
    NOTE: "phase spent 0.474. Remaining 1.526 unused. Account funding is not a project budget. Activation gate failed at 9/10 distinct themes after PAA quality filter.",
  },
  {
    SOURCE: "Captured PAA / related-question research assets",
    METHOD: "None present",
    SIGNALS_FOUND: 0,
    USABLE: "NO",
  },
  {
    SOURCE: "Dealality user-behavior query log for owner-intent themes",
    METHOD: "None present",
    SIGNALS_FOUND: 0,
    USABLE: "NO",
  },
]);
