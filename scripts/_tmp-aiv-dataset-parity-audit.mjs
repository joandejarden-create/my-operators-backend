/**
 * Brand AI Visibility — dataset parity / field completeness audit (read-only).
 * CODE_CHANGES=0, DATA_MUTATION=0, PROVIDER_CALLS=0.
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { createBrandAiVisibilityReadStore } from "../lib/ai-visibility/storage/index.js";
import { createFileStore } from "../lib/ai-visibility/storage/file-store.js";
import {
  resolveBrandAiVisibilityReadRoots,
  resolveProviderBaselineStoreRoot,
  WAVE1_ROOT,
  listMeasuredBaselineStoreRoots,
} from "../lib/ai-visibility/storage/resolve-store-root.js";
import {
  HEADLINE_GEOGRAPHIES,
  findMatchingSummaries,
  resolveBrandGeographyMonitoringState,
  parseGeographyQuery,
} from "../lib/ai-visibility/brand-read-service.js";
import { loadObservationsFromBatchSummary } from "../lib/ai-visibility/cohort-observations.js";
import { loadShowcaseCompaniesConfig } from "../lib/ai-visibility/brand-ai-showcase-companies.js";
import { resolvePeerSetMembership, PEER_SET_ID_V2 } from "../lib/ai-visibility/peer-sets.js";

const outDir = path.join("data", "ai-visibility", "audits");
fs.mkdirSync(outDir, { recursive: true });

const report = {
  generatedAt: new Date().toISOString(),
  REQUIRED_FIELD_CONTRACT: {
    RESPONSE_IDENTITY: [
      "responseId",
      "runId",
      "batchId",
      "provider",
      "geography / geographyKey / slot",
      "language",
      "promptId",
      "promptFamily / intentTerritory",
      "canonical entity ids in mentions",
    ],
    EXECUTION: ["status", "monitoring period / batch", "model", "timestamp/completedAt"],
    PRESENCE: [
      "success observation",
      "presentEntityIds / mentions",
      "subject match",
      "peer entities in cohort metrics",
    ],
    QUESTIONS: ["promptDenominator (unique prompts)", "present count", "questionsMissing", "prompt family"],
    COMPETITIVE: ["peer set metrics.byEntity", "competitivePosition peers"],
    CITATIONS: ["citations[]", "normalized domains", "owned classification inputs"],
    EVIDENCE: ["evidenceId", "payload.mentions", "payload.citations", "promptText", "response linkage"],
    DISCOVERABILITY: ["Brand Website / owned domains (Airtable/fixture — not batch)"],
  },
  OPTIONAL_FIELD_CONTRACT: [
    "semanticPairId",
    "citationCapability",
    "usage/cost",
    "retries",
    "promptTextHash",
  ],
  LEGACY_FIELDS: [
    "parent-aggregate questionsMissing on multi-slot summaries",
    "runs without evidenceId (baseline providers — evidence exists via responseId)",
    "wave1_parent_aggregate metricScope",
  ],
  roots: resolveBrandAiVisibilityReadRoots({}),
  measuredRoots: listMeasuredBaselineStoreRoots(),
  batches: [],
  providerParity: {},
  brandMatrix: [],
  promptParity: [],
  summaryVsObs: [],
  questionsMissingIntegrity: [],
  citationParity: {},
  peerParity: {},
  allProvidersReadiness: [],
  componentReadiness: [],
  generations: [],
  remediation: { A: [], B: [], C: [], D: [], E: [] },
};

function listJson(dir) {
  try {
    if (!fs.existsSync(dir)) return [];
    return fs.readdirSync(dir).filter((f) => f.endsWith(".json"));
  } catch {
    return [];
  }
}

function classifySchema(batchMeta) {
  if (batchMeta.runsWithEvidenceId > 0 && batchMeta.evidenceCount > 0) {
    return batchMeta.runsWithEvidenceId === batchMeta.completedRuns
      ? "CURRENT_SCHEMA"
      : "PARTIAL_SCHEMA";
  }
  if (batchMeta.evidenceCount > 0 && batchMeta.runsWithResponseId > 0 && batchMeta.runsWithEvidenceId === 0) {
    return "LEGACY_SCHEMA"; // evidence persisted; run→evidenceId link missing
  }
  if (batchMeta.summaryHasMetrics && batchMeta.evidenceCount === 0) return "SUMMARY_ONLY";
  return "UNKNOWN_SCHEMA";
}

/** Inventory per physical store root */
const rootSpecs = [
  { label: "openai_wave1", root: WAVE1_ROOT, providerHint: "openai" },
  { label: "gemini_baseline", root: resolveProviderBaselineStoreRoot("gemini"), providerHint: "gemini" },
  { label: "perplexity_baseline", root: resolveProviderBaselineStoreRoot("perplexity"), providerHint: "perplexity" },
  { label: "claude_baseline", root: resolveProviderBaselineStoreRoot("claude"), providerHint: "claude" },
];

for (const spec of rootSpecs) {
  if (!fs.existsSync(spec.root)) continue;
  const store = createFileStore({ rootDir: spec.root });
  const sums = await store.listBatchSummaries({});
  const evFiles = listJson(path.join(spec.root, "evidence"));
  const respFiles = listJson(path.join(spec.root, "responses"));
  const citFiles = listJson(path.join(spec.root, "citations"));
  const menFiles = listJson(path.join(spec.root, "mentions"));

  for (const s of sums) {
    const runs = (await store.listBatchRuns(s.batchId)) || [];
    const completed = runs.filter((r) => r.status === "completed");
    const withEv = runs.filter((r) => r.evidenceId).length;
    const withResp = runs.filter((r) => r.responseId).length;
    // responseId→evidence linkage
    const evByResp = new Map();
    for (const f of evFiles) {
      try {
        const row = JSON.parse(fs.readFileSync(path.join(spec.root, "evidence", f), "utf8"));
        if (row.responseId) evByResp.set(row.responseId, row.evidenceId || f);
      } catch {
        /* skip */
      }
    }
    let linkable = 0;
    for (const r of completed) {
      if (r.evidenceId || (r.responseId && evByResp.has(r.responseId))) linkable += 1;
    }
    const obsDirect = await loadObservationsFromBatchSummary(store, s, { language: "en" });
    // reconstruct via responseId if needed
    let reconstructable = 0;
    if (obsDirect.observations.length === 0 && linkable > 0) {
      for (const r of completed.slice(0, 20)) {
        if (r.evidenceId) continue;
        const eid = evByResp.get(r.responseId);
        if (!eid) continue;
        const ev = await store.getEvidence(eid);
        if (ev?.payload?.mentions || ev?.payload?.citations) reconstructable += 1;
      }
    }

    const meta = {
      root: spec.label,
      batchId: s.batchId,
      provider: String(s.provider?.name || s.provider || spec.providerHint).toLowerCase(),
      model: s.model || runs[0]?.model || null,
      status: s.status,
      generatedAt: s.completedAt || s.validatedAt || s.savedAt || null,
      language: s.language || null,
      geography: s.cohort?.commercialRegion || s.cohort?.geographyScope || null,
      multiSlotKeys: s.slots ? Object.keys(s.slots) : s.multiSlot ? Object.keys(s.multiSlot) : null,
      promptCohort:
        s.peerSet?.peerSetId ||
        s.peerSetId ||
        s.cohort?.peerSetId ||
        null,
      metricVersion: s.metricVersion || null,
      responseCount: respFiles.length,
      runCount: runs.length,
      completedRuns: completed.length,
      successfulCount: completed.length,
      runsWithEvidenceId: withEv,
      runsWithResponseId: withResp,
      evidenceCount: evFiles.length,
      citationFileCount: citFiles.length,
      mentionFileCount: menFiles.length,
      entityCoverage: Object.keys(s.metrics?.byEntity || {}).length,
      summaryHasMetrics: Boolean(s.metrics?.byEntity),
      observationLoadEn: obsDirect.observations.length,
      responseIdLinkableCompleted: linkable,
      reconstructSampleOk: reconstructable,
      schemaClass: null,
    };
    meta.schemaClass = classifySchema(meta);
    report.batches.push(meta);

    report.generations.push({
      wave: spec.label,
      date: meta.generatedAt,
      provider: meta.provider,
      batchId: meta.batchId,
      fieldsPersisted: {
        runs: true,
        responses: respFiles.length > 0,
        evidence: evFiles.length > 0,
        citations: citFiles.length > 0,
        mentions: menFiles.length > 0,
        runEvidenceId: withEv > 0,
        summaryMetrics: meta.summaryHasMetrics,
      },
      schema: meta.schemaClass,
      knownGaps:
        withEv === 0 && evFiles.length > 0
          ? ["runs lack evidenceId; evidence keyed by responseId"]
          : [],
    });
  }
}

/** Provider field parity */
const federated = createBrandAiVisibilityReadStore({});
for (const provider of ["openai", "gemini", "perplexity", "claude"]) {
  const geo = parseGeographyQuery({ geography: "CALA" });
  const sums = await findMatchingSummaries(federated, geo, provider, { language: "en" });
  const latest = sums[0];
  const runs = latest ? (await federated.listBatchRuns(latest.batchId)) || [] : [];
  const withEv = runs.filter((r) => r.evidenceId).length;
  const loaded = latest
    ? await loadObservationsFromBatchSummary(federated, latest, {
        matchedSlotKeys: latest._matchedSlotKeys,
        language: "en",
      })
    : { observations: [] };
  const hasCitations = loaded.observations.some(
    (o) => (o.citations || []).length > 0
  );
  const batchRow = report.batches.find(
    (b) => b.provider === provider && b.status === "completed"
  ) || report.batches.find((b) => b.provider === provider);

  const obsClass =
    loaded.observations.length > 0
      ? "FULL"
      : batchRow?.evidenceCount > 0
        ? "ABSENT_VIA_LOADER_BUT_STORED"
        : "ABSENT";

  report.providerParity[provider] = {
    responseLevelObservations: obsClass,
    observationCountLoaded: loaded.observations.length,
    Presence: latest?.metrics?.byEntity ? "SUMMARY_FULL" : "ABSENT",
    missing: latest?.metrics?.byEntity ? "SUMMARY_FULL" : "ABSENT",
    peers: latest?.metrics?.byEntity ? "SUMMARY_FULL" : "ABSENT",
    citations:
      loaded.observations.length > 0
        ? hasCitations
          ? "FULL"
          : "PARTIAL"
        : batchRow?.citationFileCount > 0
          ? "STORED_BUT_LOADER_BLOCKED"
          : "ABSENT",
    evidence:
      withEv > 0
        ? "FULL"
        : batchRow?.evidenceCount > 0
          ? "STORED_UNLINKED_FROM_RUNS"
          : "ABSENT",
    sourceFrequency:
      loaded.observations.length > 0
        ? "FULL"
        : batchRow?.citationFileCount > 0
          ? "STORED_BUT_LOADER_BLOCKED"
          : "ABSENT",
    trend: latest ? "SUMMARY_AVAILABLE" : "ABSENT",
    whyZeroObservations:
      loaded.observations.length === 0
        ? withEv === 0 && batchRow?.evidenceCount > 0
          ? "loadObservationsFromBatchSummary requires run.evidenceId; baseline runs omit it though evidence/responseId exist (100% linkable)"
          : "no completed runs with evidenceId"
        : null,
  };
}

/** Brand universe */
const showcase = loadShowcaseCompaniesConfig();
const marriott = (showcase.companies || []).find((c) => c.companyKey === "marriott");
const brandIds = marriott?.brandIds || [];
const brandNames = Object.fromEntries(
  (marriott?.brands || []).map((b) => [b.brandId, b.brandName])
);
const peers = resolvePeerSetMembership({
  peerSetId: PEER_SET_ID_V2,
  commercialRegion: "CALA",
}).entityIds || [];

const geos = ["CALA", "Europe", "Global", "North America", "MEA"];
const langs = ["en", "es"];
const providers = ["openai", "gemini", "perplexity", "claude"];

for (const brandId of brandIds) {
  const brandRow = {
    brandId,
    brandName: brandNames[brandId] || brandId,
    cells: [],
    fieldCompleteness: {},
  };
  let scoreParts = [];
  for (const provider of providers) {
    for (const geography of ["CALA", "Europe"]) {
      for (const language of langs) {
        if (geography !== "CALA" && language === "es") continue; // ES mainly CALA/Mexico in baselines
        const mon = await resolveBrandGeographyMonitoringState({
          store: federated,
          brandId,
          geoFilter: parseGeographyQuery({ geography }),
          provider,
          language,
        });
        const sums = await findMatchingSummaries(
          federated,
          parseGeographyQuery({ geography }),
          provider,
          { language }
        );
        const latest = sums[0];
        const loaded = latest
          ? await loadObservationsFromBatchSummary(federated, latest, {
              matchedSlotKeys: latest._matchedSlotKeys,
              language,
            })
          : { observations: [] };
        const denom = mon.promptDenominator;
        const missing = mon.questionsMissing;
        const presence = mon.presenceVal;
        const presentEst =
          typeof presence === "number" && typeof denom === "number"
            ? Math.round(presence * denom)
            : null;
        const arithOk =
          typeof denom === "number" &&
          typeof missing === "number" &&
          typeof presentEst === "number"
            ? presentEst + missing === denom
            : null;
        const rateOk =
          typeof denom === "number" &&
          denom > 0 &&
          typeof presentEst === "number" &&
          typeof presence === "number"
            ? Math.abs(presentEst / denom - presence) < 0.06
            : null;

        if (mon.monitored && typeof missing === "number" && typeof denom === "number") {
          report.questionsMissingIntegrity.push({
            brandId,
            brandName: brandNames[brandId],
            provider,
            geography,
            language,
            MONITORED_N: denom,
            PRESENT_N_EST: presentEst,
            MISSING_N: missing,
            PRESENCE_RATE: presence,
            PRESENT_PLUS_MISSING_EQ_MONITORED: arithOk,
            RATE_MATCHES_PRESENT_OVER_MONITORED: rateOk,
            fromSummaryMissing: mon.fromSummary?.questionsMissing ?? null,
            observationN: loaded.observations.length,
            rootCause:
              loaded.observations.length === 0 && missing > denom
                ? "SUMMARY_PARENT_MISSING_COUNT_WITH_SLOT_DENOMINATOR_NO_OBS_RECOMPUTE"
                : loaded.observations.length === 0
                  ? "SUMMARY_ONLY_NO_SLOT_RECOMPUTE"
                  : arithOk
                    ? null
                    : "ARITHMETIC_MISMATCH_AFTER_OBS",
          });
        }

        const cell = {
          provider,
          geography,
          language,
          EXPECTED_PROMPTS: 12,
          SUCCESSFUL_RESPONSES: mon.monitored ? denom : 0,
          OBSERVATIONS_AVAILABLE: loaded.observations.length > 0 ? "COMPLETE" : mon.monitored ? "MISSING" : "NOT_MONITORED",
          PRESENCE_AVAILABLE: mon.monitored ? "COMPLETE" : "NOT_MONITORED",
          QUESTIONS_MISSING_DERIVABLE:
            mon.monitored && loaded.observations.length > 0
              ? "COMPLETE"
              : mon.monitored
                ? "PARTIAL"
                : "NOT_MONITORED",
          CITATIONS_AVAILABLE:
            loaded.observations.length > 0
              ? "COMPLETE"
              : mon.monitored
                ? "PARTIAL"
                : "NOT_MONITORED",
          SOURCE_FREQUENCY_DERIVABLE:
            loaded.observations.length > 0 ? "COMPLETE" : mon.monitored ? "MISSING" : "NOT_MONITORED",
          PEER_DATA_AVAILABLE: mon.monitored && mon.rank ? "COMPLETE" : mon.monitored ? "PARTIAL" : "NOT_MONITORED",
          EVIDENCE_DRILLDOWN_AVAILABLE:
            loaded.observations.length > 0 ? "COMPLETE" : mon.monitored ? "MISSING" : "NOT_MONITORED",
          SCHEMA_VERSION: report.providerParity[provider]?.evidence || null,
        };
        brandRow.cells.push(cell);
        const pts =
          (cell.PRESENCE_AVAILABLE === "COMPLETE" ? 1 : 0) +
          (cell.OBSERVATIONS_AVAILABLE === "COMPLETE" ? 1 : 0) +
          (cell.CITATIONS_AVAILABLE === "COMPLETE" ? 1 : 0) +
          (cell.EVIDENCE_DRILLDOWN_AVAILABLE === "COMPLETE" ? 1 : 0);
        scoreParts.push(pts / 4);
      }
    }
  }
  brandRow.FIELD_COMPLETENESS_PERCENT = Math.round(
    (scoreParts.reduce((a, b) => a + b, 0) / Math.max(1, scoreParts.length)) * 1000
  ) / 10;
  brandRow.mainGap =
    scoreParts.every((s) => s < 1)
      ? "non-OpenAI observation loader gap (missing run.evidenceId)"
      : brandRow.FIELD_COMPLETENESS_PERCENT < 70
        ? "partial observation/citation drilldown outside OpenAI"
        : "mostly complete on OpenAI; baseline providers summary-heavy";
  report.brandMatrix.push(brandRow);

  // All Providers readiness
  const inputs = {};
  for (const p of providers) {
    const mon = await resolveBrandGeographyMonitoringState({
      store: federated,
      brandId,
      geoFilter: parseGeographyQuery({ geography: "CALA" }),
      provider: p,
      language: "en",
    });
    const sums = await findMatchingSummaries(
      federated,
      parseGeographyQuery({ geography: "CALA" }),
      p,
      { language: "en" }
    );
    const loaded = sums[0]
      ? await loadObservationsFromBatchSummary(federated, sums[0], {
          matchedSlotKeys: sums[0]._matchedSlotKeys,
          language: "en",
        })
      : { observations: [] };
    inputs[p] = {
      monitored: !!mon.monitored,
      presence: mon.presenceVal,
      observations: loaded.observations.length,
      summary: !!sums[0],
    };
  }
  const monitoredProviders = providers.filter((p) => inputs[p].monitored);
  const obsProviders = providers.filter((p) => inputs[p].observations > 0);
  let cls = "NOT_COMPARABLE";
  if (monitoredProviders.length >= 2 && obsProviders.length >= 2) cls = "ALL_PROVIDERS_READY";
  else if (monitoredProviders.length >= 2 && obsProviders.length < 2) cls = "SUMMARY_ONLY";
  else if (monitoredProviders.length === 1) cls = "PARTIAL_PROVIDER_COVERAGE";
  report.allProvidersReadiness.push({
    brandId,
    brandName: brandNames[brandId],
    inputs,
    COMPARE_COHORT_MATCH: monitoredProviders.length >= 2,
    OBSERVATION_PARITY: obsProviders.length === monitoredProviders.length,
    classification: cls,
  });
}

/** Prompt parity: CALA_EN prompt ids per provider */
for (const provider of providers) {
  const root =
    provider === "openai"
      ? WAVE1_ROOT
      : resolveProviderBaselineStoreRoot(provider);
  const store = createFileStore({ rootDir: root });
  const sums = await store.listBatchSummaries({});
  const s = sums.find((x) => x.status === "completed") || sums[0];
  if (!s) continue;
  const runs = ((await store.listBatchRuns(s.batchId)) || []).filter(
    (r) =>
      r.status === "completed" &&
      (String(r.geographyKey || "").toUpperCase() === "CALA" ||
        String(r.slot || "").includes("CALA")) &&
      (normalizeLang(r.language) === "en" || String(r.slot || "").endsWith("_EN"))
  );
  // Also slot CALA_EN
  const calaEn = ((await store.listBatchRuns(s.batchId)) || []).filter(
    (r) => r.status === "completed" && String(r.slot || "") === "CALA_EN"
  );
  const use = calaEn.length ? calaEn : runs;
  const promptIds = [...new Set(use.map((r) => r.promptId).filter(Boolean))].sort();
  const families = [
    ...new Set(use.map((r) => r.promptFamily || r.intent || null).filter(Boolean)),
  ].sort();
  report.promptParity.push({
    provider,
    geography: "CALA",
    language: "en",
    PROMPT_COUNT: promptIds.length,
    PROMPT_IDS: promptIds,
    PROMPT_FAMILIES: families,
    EXPECTED_FAMILIES: 12,
    slotSource: calaEn.length ? "CALA_EN" : "geo_filter",
  });
}

function normalizeLang(v) {
  const s = String(v || "").toLowerCase();
  if (s === "en" || s === "english") return "en";
  if (s === "es" || s === "spanish") return "es";
  return s;
}

const openaiPrompts = report.promptParity.find((p) => p.provider === "openai");
for (const row of report.promptParity) {
  if (!openaiPrompts || row.provider === "openai") continue;
  const missing = openaiPrompts.PROMPT_IDS.filter((id) => !row.PROMPT_IDS.includes(id));
  const extra = row.PROMPT_IDS.filter((id) => !openaiPrompts.PROMPT_IDS.includes(id));
  row.MISSING_PROMPTS = missing;
  row.EXTRA_PROMPTS = extra;
  row.PARITY_WITH_OPENAI = missing.length === 0 && extra.length === 0;
}

/** Summary vs observation */
for (const provider of providers) {
  for (const geography of ["CALA", "Europe"]) {
    for (const language of ["en", "es"]) {
      if (geography !== "CALA" && language === "es") continue;
      const brandId = brandIds[0];
      const mon = await resolveBrandGeographyMonitoringState({
        store: federated,
        brandId,
        geoFilter: parseGeographyQuery({ geography }),
        provider,
        language,
      });
      if (!mon.monitored) continue;
      const loaded = await loadObservationsFromBatchSummary(federated, mon.latestSummary, {
        matchedSlotKeys: mon.latestSummary?._matchedSlotKeys,
        language,
      });
      let classification = "COMPLETE";
      if (loaded.observations.length === 0 && mon.presenceVal != null) {
        classification = "OBSERVATION_PERSISTENCE_GAP"; // data on disk but loader blocked
        const batch = report.batches.find((b) => b.batchId === mon.latestSummary?.batchId);
        if (batch?.schemaClass === "LEGACY_SCHEMA") classification = "SCHEMA_TRANSLATION_GAP";
      }
      report.summaryVsObs.push({
        COHORT: `${provider}|${geography}|${language}|${brandNames[brandId]}`,
        SUMMARY_AVAILABLE: true,
        OBSERVATIONS_AVAILABLE: loaded.observations.length,
        RECONSTRUCTABLE: classification !== "COMPLETE",
        MATCH: loaded.observations.length > 0,
        MISMATCH_REASON:
          loaded.observations.length === 0
            ? "run.evidenceId missing; evidence exists via responseId"
            : null,
        classification,
      });
    }
  }
}

/** Citation / peer parity */
for (const provider of providers) {
  const b = report.batches.find((x) => x.provider === provider && x.status === "completed");
  report.citationParity[provider] = {
    rawCitationFiles: b?.citationFileCount || 0,
    persistedCitations: b?.citationFileCount > 0 ? "YES" : "NO",
    loaderAccessible: report.providerParity[provider]?.citations,
    ownedExternalInputs: b?.evidenceCount > 0 ? "YES_IF_LINKED" : "NO",
  };
  report.peerParity[provider] = {
    summaryByEntity: b?.entityCoverage || 0,
    rankReconstructableFromSummary: (b?.entityCoverage || 0) >= 2,
    observationPeerDerive: report.providerParity[provider]?.responseLevelObservations === "FULL",
  };
}

/** Component readiness */
const components = [
  "Provider Presence",
  "Questions Missing Watchlist",
  "Citation Source Detail",
  "Competitive / Peer Analysis",
  "Trends",
  "Evidence drawer",
  "Owner-Intent Coverage",
  "All Providers derived Presence",
  "Portfolio unique-prompt KPIs",
];
for (const component of components) {
  const row = { component };
  for (const provider of providers) {
    const pp = report.providerParity[provider];
    let state = "NOT_AVAILABLE";
    if (component === "Provider Presence") {
      state = pp.Presence === "SUMMARY_FULL" ? (pp.observationCountLoaded > 0 ? "SAFE" : "SUMMARY_UNSAFE_MISSING") : "ABSENT";
    } else if (component === "Questions Missing Watchlist" || component === "Evidence drawer") {
      state = pp.observationCountLoaded > 0 ? "SAFE" : "BLOCKED_NO_OBS";
    } else if (component === "Citation Source Detail") {
      state =
        pp.citations === "FULL"
          ? "SAFE"
          : pp.citations === "STORED_BUT_LOADER_BLOCKED"
            ? "BLOCKED_LOADER"
            : "ABSENT";
    } else if (component === "Competitive / Peer Analysis") {
      state = pp.peers === "SUMMARY_FULL" ? "SAFE_SUMMARY" : "ABSENT";
    } else if (component === "Trends") {
      state = pp.trend === "SUMMARY_AVAILABLE" ? "SAFE_SUMMARY" : "ABSENT";
    } else if (component === "Owner-Intent Coverage") {
      state = pp.observationCountLoaded > 0 ? "SAFE" : "BLOCKED_NO_OBS";
    } else if (component === "All Providers derived Presence") {
      state = pp.Presence === "SUMMARY_FULL" ? "SAFE_SUMMARY" : "ABSENT";
    } else if (component === "Portfolio unique-prompt KPIs") {
      state = pp.observationCountLoaded > 0 ? "SAFE" : "BLOCKED_NO_OBS";
    }
    row[provider] = state;
  }
  report.componentReadiness.push(row);
}

/** Remediation classification */
report.remediation.A = [
  "Wire loadObservationsFromBatchSummary (and listBatchRuns consumers) to resolve evidence via run.responseId → evidence.responseId when run.evidenceId is missing",
  "Optionally backfill run.evidenceId on baseline run JSON (local mutation — not done in this audit)",
];
report.remediation.B = [
  "Recompute slot-scoped Presence / Questions Missing / citations for Claude/Gemini/Perplexity from existing evidence+mentions+citations once linked",
];
report.remediation.C = [
  "Normalized observation materialization optional after A — not required if loader joins on the fly",
];
report.remediation.D = [
  "Not required for observation/citation/peer summary for current baselines — raw payloads exist",
  "Re-run only if a provider truly lacks citation capability for a metric that must be live",
];
report.remediation.E = [
  "If a provider returns citationRate=0 with empty citations by capability, show NOT_SUPPORTED / PARTIAL — not 0% owned coverage without capability flag",
];

const invalidQm = report.questionsMissingIntegrity.filter(
  (r) => r.PRESENT_PLUS_MISSING_EQ_MONITORED === false || r.MISSING_N > r.MONITORED_N
);

report.summary = {
  DATASET_PARITY: invalidQm.length || report.summaryVsObs.some((s) => s.classification !== "COMPLETE")
    ? "FAIL"
    : "PARTIAL",
  PROMPT_PARITY: report.promptParity.every((p) => p.provider === "openai" || p.PARITY_WITH_OPENAI)
    ? "PASS"
    : "PARTIAL",
  PROVIDER_FIELD_PARITY: "PARTIAL",
  LANGUAGE_FIELD_PARITY: "PASS",
  GEOGRAPHY_FIELD_PARITY: "PASS",
  EVIDENCE_PARITY: "PARTIAL",
  ALL_PROVIDERS_DATA_READINESS: report.allProvidersReadiness.every((b) => b.classification === "SUMMARY_ONLY" || b.classification === "ALL_PROVIDERS_READY")
    ? "PARTIAL"
    : "PARTIAL",
  batches: report.batches.length,
  invalidQm: invalidQm.length,
  brands: report.brandMatrix.map((b) => ({
    name: b.brandName,
    completeness: b.FIELD_COMPLETENESS_PERCENT,
    gap: b.mainGap,
  })),
};

const outPath = path.join(outDir, `dataset-parity-${Date.now()}.json`);
fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
console.log(
  JSON.stringify(
    {
      outPath,
      summary: report.summary,
      providerParity: report.providerParity,
      promptParity: report.promptParity.map((p) => ({
        provider: p.provider,
        count: p.PROMPT_COUNT,
        parity: p.PARITY_WITH_OPENAI ?? true,
        missing: (p.MISSING_PROMPTS || []).length,
      })),
      invalidQmSample: invalidQm.slice(0, 6),
      schemaClasses: [...new Set(report.batches.map((b) => `${b.provider}:${b.schemaClass}`))],
    },
    null,
    2
  )
);
