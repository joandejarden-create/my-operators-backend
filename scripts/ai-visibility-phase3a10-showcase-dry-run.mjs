#!/usr/bin/env node
/**
 * Phase 3A.10 — Wave-1 showcase monitoring dry run (NO provider calls).
 *
 *   node scripts/ai-visibility-phase3a10-showcase-dry-run.mjs
 *
 * Validates 84-prompt matrix, peer v2, OpenAI request buildability,
 * fingerprints, cost/retry budgets, Wave-1 storage namespace.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  buildWave1ShowcaseDryRunPlan,
  WAVE1_BASELINE_SERIES_ID,
  WAVE1_COST_EVIDENCE,
  WAVE1_RETRY_POLICY,
} from "../lib/ai-visibility/wave1-showcase-plan.js";
import { buildOpenAiVisibilityRequest } from "../lib/ai-visibility/providers/openai.js";
import {
  normalizeVisibilityProviderResponse,
  listNormalizedProviderContractFields,
} from "../lib/ai-visibility/providers/normalized-response.js";
import {
  resolveAiVisibilityStoreRoot,
  WAVE1_ROOT,
  PHASE2E_ROOT,
} from "../lib/ai-visibility/storage/resolve-store-root.js";
import { isRetryableProviderError, isAuthProviderError, BATCH_HEALTH } from "../lib/ai-visibility/execution-batch.js";
import { ProviderError } from "../lib/ai-visibility/providers/base-provider.js";
import { ACTIVE_SHOWCASE_INTENTS } from "../lib/ai-visibility/showcase-intents.js";
import { METRIC_VERSION } from "../lib/ai-visibility/config.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "data", "ai-visibility", "phase3a10-showcase-monitoring-dry-run.json");
const QA_OUT = path.join(ROOT, "data", "ai-visibility", "phase3a10-founder-prompt-qa-sample.json");

function main() {
  const plan = buildWave1ShowcaseDryRunPlan();

  // OpenAI request build (no network)
  let buildable = 0;
  const buildFailures = [];
  for (const exec of plan.EXECUTIONS) {
    const built = buildOpenAiVisibilityRequest({
      prompt: { text: exec.promptText, promptId: exec.promptId },
      model: process.env.AI_VISIBILITY_MODEL || "gpt-5.6",
      enableWebSearch: true,
    });
    if (built.ok) buildable += 1;
    else buildFailures.push({ promptId: exec.promptId, errors: built.errors });
  }

  // Normalized contract smoke
  const sampleNorm = normalizeVisibilityProviderResponse(
    {
      provider: "openai",
      model: "gpt-5.6",
      text: "Synthetic dry-run only.",
      citations: [],
      usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
      latencyMs: 1,
      providerMeta: { responseId: "resp_dry_run" },
      raw: { id: "resp_dry_run", dryRun: true },
    },
    {
      promptId: "p_dry",
      promptVersion: "1",
      geography: "GLOBAL",
      language: "en",
      intent: "Conversion",
      peerSetVersion: "2",
      rawArtifactUri: path.join(WAVE1_ROOT, "responses", "resp_dry_run.json"),
    }
  );

  // Failure simulation (local only)
  const failureSims = [
    {
      name: "503",
      err: new ProviderError("upstream", { type: "upstream_error", status: 503, retryable: true }),
    },
    {
      name: "timeout",
      err: new ProviderError("timeout", { type: "timeout", status: 408, retryable: true }),
    },
    {
      name: "auth",
      err: new ProviderError("auth", { type: "auth_error", status: 401, retryable: false }),
    },
    {
      name: "malformed",
      err: new ProviderError("parse", { type: "parse_error", status: null, retryable: false }),
    },
  ].map((s) => ({
    name: s.name,
    retryable: isRetryableProviderError(s.err),
    auth: isAuthProviderError(s.err),
    expectedBatchHealth:
      s.name === "auth" ? BATCH_HEALTH.PROVIDER_AUTH_ERROR : BATCH_HEALTH.PARTIAL_PROVIDER_FAILURE,
    note: "Provider failure must not be counted as Brand absence in metrics.",
  }));

  const wave1Store = resolveAiVisibilityStoreRoot({ wave1: true });
  const legacyStore = { rootDir: PHASE2E_ROOT };

  // Founder QA sample
  const byIntent = new Map();
  for (const intent of ACTIVE_SHOWCASE_INTENTS) {
    byIntent.set(
      intent,
      plan.EXECUTIONS.filter((e) => e.intent === intent)
    );
  }
  const sample = [];
  const pick = (pred, label) => {
    const hit = plan.EXECUTIONS.find(pred);
    if (hit) {
      sample.push({
        PROMPT_ID: hit.promptId,
        INTENT: hit.intent,
        GEO: hit.geographyKey,
        LANGUAGE: hit.language,
        FRAMING: hit.promptFamily,
        SLOT: hit.slot,
        TEXT: hit.promptText,
        QA_NOTE: label,
      });
    }
  };
  for (const intent of ACTIVE_SHOWCASE_INTENTS) {
    const rows = byIntent.get(intent) || [];
    const families = [...new Set(rows.map((r) => r.promptFamily))];
    for (const fam of families) {
      pick((e) => e.intent === intent && e.promptFamily === fam && e.slot === "GLOBAL_EN", "intent_framing_global");
    }
  }
  pick((e) => e.slot === "CALA_EN" && e.intent === "Conversion", "cala_en");
  pick((e) => e.slot === "CALA_ES" && e.intent === "Conversion", "cala_es");
  pick((e) => e.slot === "MEXICO_EN" && e.intent === "Branded Residences", "mexico_en");
  pick((e) => e.slot === "MEXICO_ES" && e.intent === "Branded Residences", "mexico_es");
  pick((e) => e.slot === "EUROPE_EN" && e.intent === "Lifestyle Positioning", "europe_en");
  pick(
    (e) => e.slot === "NORTH_AMERICA_EN" && e.intent === "Upper-Upscale Positioning",
    "na_en"
  );

  const liveCommandPreview = [
    "# DO NOT RUN until Founder approval (Phase 3A.11)",
    "AI_VISIBILITY_ENABLED=true \\",
    "AI_VISIBILITY_LIVE_TEST=true \\",
    "AI_VISIBILITY_MODEL=gpt-5.6 \\",
    `AI_VISIBILITY_STORE_ROOT=${WAVE1_ROOT} \\`,
    `AI_VISIBILITY_MAX_BATCH_COST_USD=${WAVE1_COST_EVIDENCE.RECOMMENDED_HARD_CAP_USD} \\`,
    "AI_VISIBILITY_MAX_TEST_RUNS=84 \\",
    "AI_VISIBILITY_EST_USD_PER_CALL=0.68 \\",
    "node scripts/ai-visibility-phase2a-live-env.mjs scripts/ai-visibility-execute.mjs \\",
    "  --wave1-showcase --provider openai --peer-set peers_uu_collection_lifestyle_owner_decision_v2 \\",
    "  --execute",
    "",
    "# Dry-run only (safe):",
    "node scripts/ai-visibility-phase3a10-showcase-dry-run.mjs",
    "node scripts/ai-visibility-execute.mjs --wave1-showcase --dry-run",
  ].join("\n");

  const report = {
    generatedAt: new Date().toISOString(),
    BUILD_STATUS: plan.ok && buildable === 84 && buildFailures.length === 0
      ? "BRAND_AI_VISIBILITY_PHASE_3A10_SHOWCASE_MONITORING_DRY_RUN_PASS"
      : "BRAND_AI_VISIBILITY_PHASE_3A10_SHOWCASE_MONITORING_DRY_RUN_BLOCKED",
    LIVE_PROVIDER_CALLS: 0,
    MONITORING_RUNS: 0,
    plan,
    OPENAI_DRY_RUN: {
      REQUESTS_BUILDABLE: buildable,
      FAILURES: buildFailures,
      LIVE_CALLS: 0,
    },
    NORMALIZED_PROVIDER_CONTRACT: {
      VERSION: sampleNorm.contractVersion,
      FIELDS: listNormalizedProviderContractFields(),
      SAMPLE_STATUS: sampleNorm.status,
    },
    PROVIDER_NEUTRAL_CORE: {
      A_PROVIDER_NEUTRAL: [
        "lib/ai-visibility/load-prompts.js",
        "lib/ai-visibility/prompt-cohort.js",
        "lib/ai-visibility/wave1-showcase-plan.js",
        "lib/ai-visibility/language-dimension.js",
        "lib/ai-visibility/commercial-geography.js",
        "lib/ai-visibility/peer-sets.js",
        "lib/ai-visibility/brand-decision-eligibility.js",
        "lib/ai-visibility/execution-batch.js",
        "lib/ai-visibility/metrics.js",
        "lib/ai-visibility/evidence.js",
        "lib/ai-visibility/storage/file-store.js",
        "lib/ai-visibility/providers/normalized-response.js",
      ],
      B_OPENAI_SPECIFIC: [
        "lib/ai-visibility/providers/openai.js (buildOpenAiVisibilityRequest, runVisibilityPrompt, annotation parsing)",
        "Responses API web_search tool schema",
        "url_citation annotation extraction",
        "OpenAI usage token field names",
      ],
    },
    FUTURE_PROVIDERS: {
      gemini: {
        PROMPT_COMPATIBLE: true,
        LANGUAGE_COMPATIBLE: true,
        GEO_COMPATIBLE: true,
        PEER_COMPATIBLE: true,
        RAW_RESPONSE_CONTRACT_COMPATIBLE: true,
        CITATION_NORMALIZATION_RISK: "HIGH — citation/tool schema differs; do not assume OpenAI url_citation",
        KNOWN_ADAPTER_WORK_REQUIRED: "Gemini adapter + citation normalization + pricing ledger",
      },
      perplexity: {
        PROMPT_COMPATIBLE: true,
        LANGUAGE_COMPATIBLE: true,
        GEO_COMPATIBLE: true,
        PEER_COMPATIBLE: true,
        RAW_RESPONSE_CONTRACT_COMPATIBLE: true,
        CITATION_NORMALIZATION_RISK: "HIGH — native citations differ; Source Divergence later",
        KNOWN_ADAPTER_WORK_REQUIRED: "Perplexity adapter + citation mapping",
      },
      claude: {
        PROMPT_COMPATIBLE: true,
        LANGUAGE_COMPATIBLE: true,
        GEO_COMPATIBLE: true,
        PEER_COMPATIBLE: true,
        RAW_RESPONSE_CONTRACT_COMPATIBLE: true,
        CITATION_NORMALIZATION_RISK: "MEDIUM/HIGH — tool/citation surface differs by configuration",
        KNOWN_ADAPTER_WORK_REQUIRED: "Claude adapter + web-tool citation mapping",
      },
    },
    STORAGE: {
      WAVE1_NAMESPACE: WAVE1_ROOT,
      LEGACY_PHASE2E: PHASE2E_ROOT,
      LEGACY_ISOLATED: wave1Store.rootDir !== legacyStore.rootDir,
      wave1Store,
      destinations: {
        batch: path.join(WAVE1_ROOT, "batches"),
        run: path.join(WAVE1_ROOT, "runs"),
        rawResponse: path.join(WAVE1_ROOT, "responses"),
        mentions: path.join(WAVE1_ROOT, "mentions"),
        citations: path.join(WAVE1_ROOT, "citations"),
        evidence: path.join(WAVE1_ROOT, "evidence"),
        metricSnapshots: path.join(WAVE1_ROOT, "metric-snapshots"),
        summaries: path.join(WAVE1_ROOT, "summaries"),
        cost: path.join(WAVE1_ROOT, "summaries"),
        errors: path.join(WAVE1_ROOT, "runs"),
        checkpoints: path.join(WAVE1_ROOT, "checkpoints"),
      },
      STORAGE_ABSTRACTION_MIGRATION_READY: true,
      MIGRATION_BLOCKERS: [
        "No Postgres repository yet — file-store interface is the migration seam",
        "Large raw payloads should map to object storage keys via rawArtifactUri",
      ],
    },
    FAILURE_SIMS: failureSims,
    COST_GUARD: {
      LOW: WAVE1_COST_EVIDENCE.LOW,
      EXPECTED: WAVE1_COST_EVIDENCE.EXPECTED,
      HIGH: WAVE1_COST_EVIDENCE.HIGH,
      WARNING_THRESHOLD_USD: WAVE1_COST_EVIDENCE.WARNING_THRESHOLD_USD,
      RECOMMENDED_HARD_CAP: WAVE1_COST_EVIDENCE.RECOMMENDED_HARD_CAP_USD,
    },
    CALL_BUDGET: WAVE1_RETRY_POLICY,
    TREND_BASELINE: {
      SERIES_ID: WAVE1_BASELINE_SERIES_ID,
      provider: "openai",
      peer: "peers_uu_collection_lifestyle_owner_decision_v2",
      metricVersion: METRIC_VERSION,
      promptFamily: "phase3a9_showcase_v1",
      language: "explicit",
      geography: "explicit",
      note: "Not comparable to peer-v1 Phase 2E history. Trend unavailable until later comparable period.",
    },
    LIVE_COMMAND_PREVIEW: liveCommandPreview,
    DO_NOT_RUN: true,
  };

  // Remove bulky prompt texts from main report executions (keep ids)
  report.plan = {
    ...plan,
    EXECUTIONS: plan.EXECUTIONS.map((e) => {
      const { promptText, ...rest } = e;
      return rest;
    }),
  };

  fs.mkdirSync(path.dirname(OUT), { recursive: true });
  fs.writeFileSync(OUT, JSON.stringify(report, null, 2) + "\n");
  fs.writeFileSync(QA_OUT, JSON.stringify({ generatedAt: new Date().toISOString(), sample }, null, 2) + "\n");

  console.log(
    JSON.stringify(
      {
        BUILD_STATUS: report.BUILD_STATUS,
        ok: plan.ok,
        LOADED: plan.PROMPT_LIBRARY.LOADED,
        MATRIX_TOTAL: plan.MATRIX.TOTAL,
        PAIRS: plan.SEMANTIC_PAIRS.TOTAL,
        PEER: plan.PEER.COUNT,
        OPENAI_BUILDABLE: buildable,
        FAILURES: buildFailures.length,
        FINGERPRINT_UNIQUE: plan.FINGERPRINTS.UNIQUE,
        COLLISIONS: plan.FINGERPRINTS.COLLISIONS,
        LIVE_PROVIDER_CALLS: 0,
        WAVE1_NAMESPACE: WAVE1_ROOT,
        errors: plan.errors,
      },
      null,
      2
    )
  );
}

main();
