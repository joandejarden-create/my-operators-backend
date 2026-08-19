/**
 * Customer-safe scenario benchmark payloads — certified values only.
 * No provider calls. Redacts internal benchmark engine fields.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadScenarioRegistry, buildScenarioRegistryIndex } from "../scenario-registry.js";
import { buildPromptMetadataById } from "../associations/prompt-metadata-lookup.js";
import { resolveObservationScenario } from "./prompt-scenario-bridge.js";
import {
  BENCHMARK_SCOPES,
  benchmarkScopeFromProvider,
  lookupScopeCertification,
  loadProviderScopedCertificationRegistry,
} from "./provider-scoped-benchmark-certification.js";
import { resolveScenarioCommercialPeers } from "./scenario-peer-eligibility.js";
import { loadBenchmarkEligibleUniverse } from "./benchmark-eligible-universe.js";
import { computeAiPresenceRate } from "../metrics.js";
import {
  buildFutureOwnerIntentBenchmarkRow,
  buildFutureQuestionsMissingRow,
  redactFutureCompetitivePeerPayload,
  auditFutureCustomerPayload,
  CUSTOMER_SAFE_PEER_LIMIT,
} from "./scenario-benchmark-tab-integration.js";
import {
  isScenarioBenchmarkUiLive,
  loadFinalCertificationReport,
  listCertifiedCandidates,
} from "./scenario-benchmark-final-certification.js";
import { SCENARIO_IDS as S } from "./benchmark-brand-ids.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_COMPOSITION_PATH = path.join(
  __dirname,
  "..",
  "..",
  "..",
  "reports",
  "ai-visibility",
  "scenario-benchmark-composition-v1.json"
);

/** Customer-facing owner-intent rows — high-commercial scenarios only. */
export const CUSTOMER_SCENARIO_DISPLAY_ORDER = Object.freeze([
  S.SOFT_BRAND,
  S.CONVERSION_SUITABILITY,
  S.OWNER_FLEXIBILITY,
  S.INDEPENDENT_UU_CONVERSION,
  S.NEWBUILD_UU,
]);

const CUSTOMER_BENCHMARK_STATUS = Object.freeze({
  PRODUCTION_VALIDATED: "VALIDATED",
  PRODUCTION_VALIDATED_NARROW: "VALIDATED_NARROW",
  DETAIL_ONLY: "Benchmark still developing",
  LIMITED: "Benchmark still developing",
  SUPPRESSED: "Benchmark still developing",
  LIMITED_COMPARABLE_EVIDENCE: "Benchmark still developing",
  BENCHMARK_STILL_DEVELOPING: "Benchmark still developing",
});

/** Customer-facing proper-case labels — never expose enum/snake_case. */
export const CUSTOMER_SCENARIO_DISPLAY_LABELS = Object.freeze({
  [S.SOFT_BRAND]: "Soft Brand Affiliation",
  [S.OWNER_FLEXIBILITY]: "Owner Flexibility",
  [S.CONVERSION_SUITABILITY]: "Conversion Suitability",
  [S.LIFESTYLE]: "Lifestyle / Individuality",
  [S.INDEPENDENT_UU_CONVERSION]: "Independent Conversion",
  [S.NEWBUILD_UU]: "New Build Upper Upscale",
  [S.BRANDED_RESIDENCES]: "Branded Residences",
  [S.MARKET_ENTRY]: "Market Entry / Geographic Relevance",
  [S.DISTRIBUTION_LOYALTY]: "Distribution / Loyalty",
  [S.OWNER_ECONOMICS]: "Owner Economics",
  [S.CHAIN_SCALE]: "Upper-Upscale Brand Selection",
  [S.HMA_VS_FRANCHISE]: "HMA vs Franchise",
});

const SCENARIO_INTENT_LABELS = CUSTOMER_SCENARIO_DISPLAY_LABELS;

function pctDisplay(rate) {
  if (typeof rate !== "number" || !Number.isFinite(rate)) return null;
  return `${Math.round(rate * 100)}%`;
}

function positionCopy(indexValue, relativeGapPct) {
  if (indexValue == null || relativeGapPct == null) return null;
  const gap = Math.round(Math.abs(relativeGapPct));
  if (indexValue > 100) return `${gap}% above benchmark`;
  if (indexValue < 100) return `${gap}% below benchmark`;
  return "At competitive parity";
}

const PRIORITY_PEER_NAMES = Object.freeze([
  "Curio Collection by Hilton",
  "Tribute Portfolio",
  "Vignette Collection",
  "Autograph Collection",
  "Tapestry Collection by Hilton",
  "Ascend Hotel Collection",
  "Handwritten Collection",
  "MGallery",
]);

function selectCorePeerNames(corePeerNames = []) {
  const filtered = [...new Set(corePeerNames.filter(Boolean))];
  const prioritized = [];
  for (const pname of PRIORITY_PEER_NAMES) {
    if (filtered.includes(pname)) prioritized.push(pname);
  }
  for (const name of filtered) {
    if (!prioritized.includes(name)) prioritized.push(name);
  }
  return prioritized.slice(0, CUSTOMER_SAFE_PEER_LIMIT);
}

function selectObservedCompetitors(observed = [], coreNames = []) {
  const coreSet = new Set(coreNames);
  return observed.filter((n) => n && !coreSet.has(n)).slice(0, CUSTOMER_SAFE_PEER_LIMIT);
}

let cachedCompositionReport = null;

export function loadCompositionReport(opts = {}) {
  if (cachedCompositionReport && !opts.refresh) return cachedCompositionReport;
  const filePath = opts.compositionReportPath || DEFAULT_COMPOSITION_PATH;
  if (!fs.existsSync(filePath)) return { rows: [] };
  cachedCompositionReport = JSON.parse(fs.readFileSync(filePath, "utf8"));
  return cachedCompositionReport;
}

function corePeerNamesFromComposition(compRow) {
  return (compRow?.corePeers || [])
    .filter((p) => p.commercialRelation === "CORE")
    .map((p) => p.peerBrandName)
    .filter(Boolean);
}

export function getCustomerScenarioDisplayLabel(scenarioId, fallback = null) {
  if (CUSTOMER_SCENARIO_DISPLAY_LABELS[scenarioId]) {
    return CUSTOMER_SCENARIO_DISPLAY_LABELS[scenarioId];
  }
  const registry = loadScenarioRegistry();
  const row = (registry.scenarios || []).find((s) => s.scenarioId === scenarioId);
  if (row?.scenarioName) {
    return String(row.scenarioName)
      .replace(/\s*\/\s*/g, " ")
      .replace(/\b\w/g, (c) => c.toUpperCase())
      .replace(/\s+/g, " ")
      .trim();
  }
  return fallback;
}

/**
 * Subject Presence for one scenario from comparable observations (provider-specific or derived).
 */
export function computeScenarioSubjectPresence(brandId, scenarioId, observations = [], opts = {}) {
  if (!brandId || !scenarioId) return null;
  const scenarioIndex = opts.scenarioIndex || buildScenarioRegistryIndex(opts.registry || loadScenarioRegistry());
  const promptMap = opts.promptMap || buildPromptMetadataById();
  const filtered = [];
  for (const o of observations || []) {
    if (!o || o.success === false) continue;
    const resolved = resolveObservationScenario(o, { scenarioIndex, promptMap });
    if (resolved.scenarioId === scenarioId) filtered.push(o);
  }
  if (!filtered.length) return null;
  const rate = computeAiPresenceRate(filtered, brandId);
  return typeof rate.value === "number" ? Math.round(rate.value * 10000) / 10000 : null;
}

function buildOwnerIntentRowFromSources({
  scenarioId,
  cert,
  comp,
  scopeCert = null,
  indexChgVsPrior = null,
  certifyIndex = true,
  useCompositionPresence = true,
  subjectPresenceOverride = undefined,
  benchmarkScope = BENCHMARK_SCOPES.ALL_PROVIDERS,
}) {
  const scopeIsProviderSpecific = benchmarkScope !== BENCHMARK_SCOPES.ALL_PROVIDERS;
  let productionClass = "LIMITED";
  if (
    scopeCert?.certificationStatus === "PRODUCTION_VALIDATED" ||
    scopeCert?.certificationStatus === "PRODUCTION_VALIDATED_NARROW"
  ) {
    productionClass = scopeCert.certificationStatus;
  } else if (scopeIsProviderSpecific) {
    productionClass = "LIMITED";
  } else {
    productionClass = cert?.FINAL_STATUS || comp?.productionClass || "LIMITED";
  }
  const certified =
    certifyIndex &&
    (productionClass === "PRODUCTION_VALIDATED" ||
      productionClass === "PRODUCTION_VALIDATED_NARROW");
  const coreNames = selectCorePeerNames(
    cert?.CORE_PEERS || corePeerNamesFromComposition(comp)
  );
  const observed = selectObservedCompetitors(cert?.secondaryPeers || [], coreNames);
  let subjectPresence = null;
  if (subjectPresenceOverride !== undefined) {
    subjectPresence = subjectPresenceOverride;
  } else if (useCompositionPresence) {
    subjectPresence =
      typeof cert?.SUBJECT_PRESENCE === "number"
        ? Math.round(cert.SUBJECT_PRESENCE * 100) / 100
        : typeof comp?.subjectPresence === "number"
          ? Math.round(comp.subjectPresence * 100) / 100
          : null;
  }
  const indexValue = certified
    ? scopeCert?.certifiedIndex ?? cert?.INDEX ?? comp?.indexCore ?? null
    : null;
  const relativeGapPct = certified
    ? scopeCert?.relativeGapPct ?? cert?.RELATIVE_GAP ?? null
    : null;
  const intentLabel = getCustomerScenarioDisplayLabel(
    scenarioId,
    cert?.intentLabel || comp?.scenarioName || null
  );

  const scopeLabel =
    benchmarkScope === BENCHMARK_SCOPES.ALL_PROVIDERS
      ? "All Providers benchmark"
      : benchmarkScope === BENCHMARK_SCOPES.OPENAI
        ? "OpenAI benchmark"
        : benchmarkScope === BENCHMARK_SCOPES.GEMINI
          ? "Gemini benchmark"
          : benchmarkScope === BENCHMARK_SCOPES.PERPLEXITY
            ? "Perplexity benchmark"
            : benchmarkScope === BENCHMARK_SCOPES.CLAUDE
              ? "Claude benchmark"
              : "Provider benchmark";

  return buildFutureOwnerIntentBenchmarkRow(
    {
      scenarioId,
      intentLabel,
      scope: benchmarkScope,
      subjectPresence,
      indexValue,
      relativeGapPct,
      indexChgVsPrior,
      productionClass,
      selectedCorePeers: coreNames,
      selectedObservedCompetitors: observed,
      benchmarkScopeLabel: scopeLabel,
      evidenceSummary: certified
        ? `AI Presence Index ${indexValue} · ${positionCopy(indexValue, relativeGapPct)}`
        : null,
    },
    { customerIndexRendering: true, certifyIndex }
  );
}

/**
 * Owner-intent benchmark rows for one brand (all governed scenarios with safe states).
 */
export function buildOwnerIntentBenchmarksForBrand(brandId, opts = {}) {
  const uiLive = isScenarioBenchmarkUiLive(opts);
  if (!uiLive) {
    return { ownerIntentBenchmarks: [], SCENARIO_BENCHMARK_UI: "OFF", CUSTOMER_INDEX_RENDERING: "OFF" };
  }

  const allProvidersMode = opts.allProvidersMode !== false;
  const benchmarkScope =
    opts.benchmarkScope || benchmarkScopeFromProvider(opts.provider, allProvidersMode);
  loadProviderScopedCertificationRegistry(opts);
  const observations = opts.observations || [];
  const scenarioIndex = buildScenarioRegistryIndex(loadScenarioRegistry());

  const report = loadFinalCertificationReport(opts);
  const composition = loadCompositionReport(opts);
  const certifiedForBrand = (report.candidates || []).filter((c) => c.subjectId === brandId);
  const compositionForBrand = (composition.rows || []).filter((r) => r.subjectId === brandId);
  const certByScenario = new Map(certifiedForBrand.map((c) => [c.scenarioId, c]));
  const compByScenario = new Map(compositionForBrand.map((r) => [r.scenarioId, r]));
  const builtByScenario = new Map();

  for (const scenarioId of CUSTOMER_SCENARIO_DISPLAY_ORDER) {
    const cert = certByScenario.get(scenarioId);
    const comp = compByScenario.get(scenarioId);
    if (!cert && !comp) continue;
    const scopeCert = lookupScopeCertification(brandId, scenarioId, benchmarkScope, opts);
    const scopeIsCertified =
      scopeCert?.certificationStatus === "PRODUCTION_VALIDATED" ||
      scopeCert?.certificationStatus === "PRODUCTION_VALIDATED_NARROW";
    const observedPresence = allProvidersMode
      ? undefined
      : computeScenarioSubjectPresence(brandId, scenarioId, observations, { scenarioIndex });
    builtByScenario.set(
      scenarioId,
      buildOwnerIntentRowFromSources({
        scenarioId,
        cert,
        comp,
        scopeCert,
        indexChgVsPrior: null,
        certifyIndex: scopeIsCertified,
        useCompositionPresence: allProvidersMode,
        subjectPresenceOverride: observedPresence,
        benchmarkScope,
      })
    );
  }

  for (const cand of certifiedForBrand) {
    if (!builtByScenario.has(cand.scenarioId)) {
      const scopeCert = lookupScopeCertification(brandId, cand.scenarioId, benchmarkScope, opts);
      const scopeIsCertified =
        scopeCert?.certificationStatus === "PRODUCTION_VALIDATED" ||
        scopeCert?.certificationStatus === "PRODUCTION_VALIDATED_NARROW";
      const observedPresence = allProvidersMode
        ? undefined
        : computeScenarioSubjectPresence(brandId, cand.scenarioId, observations, { scenarioIndex });
      builtByScenario.set(
        cand.scenarioId,
        buildOwnerIntentRowFromSources({
          scenarioId: cand.scenarioId,
          cert: cand,
          comp: compByScenario.get(cand.scenarioId),
          scopeCert,
          indexChgVsPrior: null,
          certifyIndex: scopeIsCertified,
          useCompositionPresence: allProvidersMode,
          subjectPresenceOverride: observedPresence,
          benchmarkScope,
        })
      );
    }
  }

  if (!builtByScenario.size) {
    return { ownerIntentBenchmarks: [], SCENARIO_BENCHMARK_UI: "OFF", CUSTOMER_INDEX_RENDERING: "OFF" };
  }

  const rows = CUSTOMER_SCENARIO_DISPLAY_ORDER.map((scenarioId) => {
    if (builtByScenario.has(scenarioId)) return builtByScenario.get(scenarioId);
    return buildFutureOwnerIntentBenchmarkRow(
      {
        scenarioId,
        intentLabel: getCustomerScenarioDisplayLabel(scenarioId),
        subjectPresence: null,
        indexValue: null,
        relativeGapPct: null,
        indexChgVsPrior: null,
        productionClass: "SUPPRESSED",
        selectedCorePeers: [],
        selectedObservedCompetitors: [],
        evidenceSummary: null,
      },
      { customerIndexRendering: true, certifyIndex: false }
    );
  });

  const payload = redactFutureCompetitivePeerPayload({
    ownerIntentBenchmarks: rows,
    CUSTOMER_INDEX_RENDERING: allProvidersMode
      ? "LIVE_CERTIFIED_VALUES_ONLY"
      : "EXACT_SCOPE_CERTIFIED_VALUES_ONLY",
  });

  return {
    ...payload,
    SCENARIO_BENCHMARK_UI: "LIVE_CERTIFIED_VALUES_ONLY",
    OWNER_INTENT_VISIBLE: rows.length > 0,
    ALL_PROVIDERS_OWNER_INTENT_VISIBLE: allProvidersMode && rows.length > 0,
    BENCHMARK_SCOPE: benchmarkScope,
    EXACT_SCOPE_CERTIFIED_SHOW_NUMBER: true,
    PROVIDER_SPECIFIC_INDEX_CERTIFICATION: "EXACT_SCOPE",
    positionCopyByScenario: Object.fromEntries(
      rows
        .filter((r) => r.indexValue != null)
        .map((r) => [r.scenarioId, positionCopy(r.indexValue, r.relativeGapPct)])
    ),
    subjectPresenceDisplayByScenario: Object.fromEntries(
      rows.map((r) => [r.scenarioId, pctDisplay(r.subjectPresence)])
    ),
  };
}

function resolveScenarioForRow(row, scenarioIndex, promptMap) {
  if (row.scenarioId) return row.scenarioId;
  const resolved = resolveObservationScenario(row, { scenarioIndex, promptMap });
  return resolved.scenarioId;
}

function corePeersPresentForScenario(subjectId, scenarioId, observations, opts = {}) {
  const universe = loadBenchmarkEligibleUniverse();
  const peers = resolveScenarioCommercialPeers(subjectId, scenarioId, { universe });
  const coreIds = new Set(
    peers.calculationPeers.filter((p) => p.commercialRelation === "CORE").map((p) => p.peerBrandId)
  );
  const namesById = opts.brandNamesById || {};
  const presentIds = new Set();
  for (const obs of observations || []) {
    if (!obs || obs.success === false) continue;
    const ids = obs.presentEntityIds || [];
    for (const id of ids) {
      if (coreIds.has(id)) presentIds.add(id);
    }
    const mentions = obs.mentions || obs.payload?.mentions || [];
    for (const m of mentions) {
      const id = m.entityId || m.resolvedEntityId || m.canonicalEntityId;
      if (id && coreIds.has(id)) presentIds.add(id);
    }
  }
  return [...presentIds]
    .map((id) => namesById[id] || null)
    .filter(Boolean)
    .slice(0, CUSTOMER_SAFE_PEER_LIMIT);
}

function observedCompetitorForRow(row, corePresent, opts = {}) {
  const peers = row.PEERS_PRESENT || [];
  const coreSet = new Set(corePresent);
  for (const p of peers) {
    const name = p.entityName || p.name || opts.brandNamesById?.[p.entityId];
    if (name && !coreSet.has(name)) return name;
  }
  return null;
}

function priorityForRow(row, corePresent) {
  const missingAll =
    row.CROSS_PROVIDER_STATE === "MISSING_ACROSS_ALL_PROVIDERS" ||
    row.SUBJECT_PRESENCE === "MISSING_ACROSS_ALL_PROVIDERS";
  const providerMissing = (row.PROVIDERS_MISSING || []).length;
  if (missingAll && corePresent.length >= 2) return "PRIORITY";
  if (missingAll || providerMissing >= 3) return "REVIEW";
  return "MONITOR";
}

function competitiveContextCopy(corePresent, subjectMissing = true) {
  if (!corePresent.length) return null;
  if (subjectMissing) {
    return "Direct peers are represented in this owner-decision scenario while your brand is absent.";
  }
  return "Core peers appear in this scenario with measurable representation.";
}

/**
 * Enrich Questions Missing watchlist rows with governed competitive context.
 */
export function enrichQuestionsMissingWithCompetitiveContext(rows = [], opts = {}) {
  const { subjectBrandId, observations = [], brandNamesById = {} } = opts;
  if (!subjectBrandId || !rows.length) return rows;

  const registry = loadScenarioRegistry();
  const scenarioIndex = buildScenarioRegistryIndex(registry);
  const promptMap = buildPromptMetadataById();

  return rows.map((row) => {
    const scenarioId = resolveScenarioForRow(row, scenarioIndex, promptMap);
    const obsForPrompt = (observations || []).filter(
      (o) => o.promptId && row.promptId && o.promptId === row.promptId
    );
    const corePresent = scenarioId
      ? corePeersPresentForScenario(
          subjectBrandId,
          scenarioId,
          obsForPrompt.length ? obsForPrompt : observations,
          { brandNamesById }
        )
      : [];
    const observed = observedCompetitorForRow(row, corePresent, { brandNamesById });
    const priority = priorityForRow(row, corePresent);
    const enriched = buildFutureQuestionsMissingRow({
      scenarioId,
      intentLabel: SCENARIO_INTENT_LABELS[scenarioId] || row.PROMPT_FAMILY || null,
      missingProviderCount: (row.PROVIDERS_MISSING || []).length || null,
      comparableProviderCount: (row.PROVIDERS_MONITORED || row.PROVIDERS_PRESENT || []).length || null,
      corePeersPresent: corePresent,
      observedCompetitors: observed ? [observed] : [],
      recurrenceState: row.CROSS_PROVIDER_STATE || row.SUBJECT_PRESENCE || null,
      priority,
      competitiveContext: competitiveContextCopy(
        corePresent,
        row.SUBJECT_PRESENCE !== "Present" && row.presenceObserved !== true
      ),
    });
    return {
      ...row,
      scenarioId: enriched.scenarioId,
      intentLabel: enriched.intentLabel,
      corePeersPresent: enriched.corePeersPresent,
      observedCompetitor: enriched.observedCompetitors?.[0] || null,
      priority: enriched.priority,
      competitiveContext: enriched.competitiveContext,
      missingProviderCount: enriched.missingProviderCount,
      comparableProviderCount: enriched.comparableProviderCount,
    };
  });
}

export function getCertifiedExecutiveBenchmarkContext(brandId, opts = {}) {
  if (!isScenarioBenchmarkUiLive(opts)) return null;
  const certified = listCertifiedCandidates(opts).filter((c) => c.subjectId === brandId);
  if (!certified.length) return null;
  const primary =
    certified.find((c) => c.FINAL_STATUS === "PRODUCTION_VALIDATED") || certified[0];
  return {
    brandId,
    scenarioId: primary.scenarioId,
    scenarioName: primary.intentLabel,
    indexValue: primary.INDEX,
    relativeGapPct: primary.RELATIVE_GAP,
    subjectPresence: primary.SUBJECT_PRESENCE,
    finalStatus: primary.FINAL_STATUS,
    corePeerNames: selectCorePeerNames(primary.CORE_PEERS),
    positionCopy: positionCopy(primary.INDEX, primary.RELATIVE_GAP),
    narrow: primary.FINAL_STATUS === "PRODUCTION_VALIDATED_NARROW",
  };
}

export function auditCustomerBenchmarkPayload(payload = {}) {
  return auditFutureCustomerPayload(payload);
}

export { isScenarioBenchmarkUiLive, CUSTOMER_BENCHMARK_STATUS, SCENARIO_INTENT_LABELS, positionCopy };
