/**
 * Phase 3B.2 post-validation audit report builder.
 */

import fs from "fs";
import path from "path";
import { parseDomain } from "./extract-citations.js";
import { CITATION_RATE_COMPATIBILITY } from "./providers/cross-provider-signals.js";
import {
  project84CallCost,
  RECOMMENDED_FULL_WAVE_HARD_CAP_USD,
} from "./provider-validation-cost.js";
import { MONITORING_RUN_PURPOSE, isValidationMonitoringRun } from "./monitoring-run-purpose.js";
import { listNormalizedProviderContractFields } from "./providers/normalized-response.js";
import { listNormalizedCitationContractFields } from "./providers/normalized-citation.js";

function readJson(p) {
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function avg(arr) {
  if (!arr.length) return null;
  return Number((arr.reduce((a, b) => a + b, 0) / arr.length).toFixed(0));
}

function median(arr) {
  if (!arr.length) return null;
  const s = [...arr].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : Number(((s[m - 1] + s[m]) / 2).toFixed(0));
}

/**
 * Build provider result summary from validation wave stats file.
 */
export function summarizeProviderValidationStats(stats) {
  if (!stats || stats.status === "NOT_EXECUTED_MISSING_CREDENTIAL") {
    return {
      provider: stats?.provider,
      status: stats?.status || "NOT_EXECUTED",
      DATASET_STATUS: "NOT_EXECUTED",
    };
  }

  const latencies = stats.latencies || [];
  const cost = stats.costLedger?.actualUsd ?? null;
  const providerReported = stats.costLedger?.providerReportedUsd ?? null;

  let datasetStatus = "READY";
  if (stats.stoppedReason === "provider_auth_error") datasetStatus = "BLOCKED";
  else if (stats.FAILED > 0 || stats.stoppedReason) datasetStatus = "READY_WITH_NON_BLOCKING_ISSUES";
  if (stats.SUCCEEDED === 0) datasetStatus = "BLOCKED";

  return {
    provider: stats.provider,
    PLANNED: stats.PLANNED,
    ATTEMPTED: stats.ATTEMPTED,
    SUCCEEDED: stats.SUCCEEDED,
    FAILED: stats.FAILED,
    RETRIES: stats.RETRIES,
    TOTAL_ATTEMPTS: stats.TOTAL_ATTEMPTS,
    WEB_SEARCH_USED: stats.webSearchUsed,
    RESPONSES_WITH_GROUNDING: stats.responsesWithGrounding,
    RESPONSES_WITH_CITATIONS: stats.responsesWithCitations,
    TOTAL_NORMALIZED_CITATIONS: stats.totalNormalizedCitations,
    TOTAL_SEARCH_RESULTS: stats.totalSearchResults,
    UNIQUE_DOMAINS: stats.uniqueDomains?.size ?? (Array.isArray(stats.uniqueDomains) ? stats.uniqueDomains.length : 0),
    INPUT_TOKENS: stats.inputTokens,
    OUTPUT_TOKENS: stats.outputTokens,
    COST: cost,
    PROVIDER_REPORTED_COST: providerReported,
    AVG_LATENCY: avg(latencies),
    MEDIAN_LATENCY: median(latencies),
    MIN_LATENCY: latencies.length ? Math.min(...latencies) : null,
    MAX_LATENCY: latencies.length ? Math.max(...latencies) : null,
    ERRORS: stats.errors || [],
    ACTIVATION_GATE: stats.activationGate?.RESULT || null,
    DATASET_STATUS: datasetStatus,
    MODEL_REQUESTED: stats.modelRequested,
    MODEL_RETURNED: stats.ACTUAL_MODEL_RETURNED,
    MODEL_MATCH: stats.MODEL_MATCH,
    storeRoot: stats.storeRoot,
    waveId: stats.waveId,
    PEER_MENTIONS: stats.peerMentions,
    RECOMMENDED_PEER_MENTIONS: stats.recommendedPeerMentions,
    STOP_REASONS: stats.stopReasons || [],
  };
}

export function assessGoNoGo(summary) {
  if (summary.status === "NOT_EXECUTED_MISSING_CREDENTIAL") return "BLOCKED";
  if (summary.DATASET_STATUS === "BLOCKED") return "BLOCKED";
  if (summary.ACTIVATION_GATE === "FAIL") return "HARDEN";
  if (summary.SUCCEEDED >= 10 && summary.ACTIVATION_GATE === "PASS") return "GO";
  if (summary.SUCCEEDED >= 8) return "HARDEN";
  return "BLOCKED";
}

export function buildPhase3b2AuditReport(reportPath) {
  const report = readJson(reportPath);
  if (!report) return { ok: false, error: "report_not_found" };

  const providerSummaries = {};
  const goNoGo = {};
  const costCalibration = {};

  for (const [provider, stats] of Object.entries(report.results || {})) {
    const summary = summarizeProviderValidationStats(stats);
    providerSummaries[provider] = summary;
    goNoGo[provider.toUpperCase()] = assessGoNoGo(summary);
    if (summary.SUCCEEDED > 0 && summary.COST != null) {
      costCalibration[provider] = {
        VALIDATION_COST: summary.COST,
        ...project84CallCost(summary.COST, summary.SUCCEEDED),
        RECOMMENDED_HARD_CAP: RECOMMENDED_FULL_WAVE_HARD_CAP_USD[provider],
      };
    }
  }

  return {
    ok: true,
    parentValidationId: report.parentValidationId,
    waveIds: report.waveIds,
    credentials: report.credentials,
    providerSummaries,
    goNoGo,
    costCalibration,
    citationRate: CITATION_RATE_COMPATIBILITY,
    CROSS_PROVIDER_CITATION_RATE_DIRECTLY_COMPARABLE: "NO",
    MONITORING_RUN_TYPE_SUPPORTED: true,
    VALIDATION_SEPARATE_FROM_BASELINE: true,
    OPENAI_BASELINE_UNTOUCHED: report.openAiBaselineUntouched,
    REQUIRES_OPENAI_RERUN: "NO",
    DISCOVERABILITY_BUSINESS_IMPACT_NEXT_PRIORITY_RETAINED: true,
    contractFields: listNormalizedProviderContractFields("v1_1"),
    citationFields: listNormalizedCitationContractFields(),
    completedAt: report.completedAt,
  };
}
