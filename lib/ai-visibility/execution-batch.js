/**
 * AI Visibility execution batch model (Phase 2E).
 */

import { createHash, randomBytes } from "crypto";
import {
  METRIC_VERSION,
  RECOMMENDATION_CLASSIFIER_VERSION,
  CITATION_ASSOC_VERSION,
  GEOGRAPHY_MODEL_VERSION,
  PARSER_VERSION,
  RESOLVER_VERSION,
} from "./config.js";
import {
  AI_VISIBILITY_LANGUAGE_VERSION,
  requireSupportedLanguage,
} from "./language-dimension.js";

export const EXECUTION_BATCH_VERSION = "ai_visibility_execution_batch_v1";

export const BATCH_STATUSES = Object.freeze([
  "planned",
  "running",
  "partial",
  "completed",
  "failed",
  "cancelled",
]);

export const BATCH_HEALTH = Object.freeze({
  HEALTHY: "HEALTHY",
  PARTIAL_PROVIDER_FAILURE: "PARTIAL_PROVIDER_FAILURE",
  COST_LIMIT_REACHED: "COST_LIMIT_REACHED",
  PROMPT_CONFIG_ERROR: "PROMPT_CONFIG_ERROR",
  ENTITY_INDEX_ERROR: "ENTITY_INDEX_ERROR",
  PEER_SET_ERROR: "PEER_SET_ERROR",
  PROVIDER_AUTH_ERROR: "PROVIDER_AUTH_ERROR",
});

/**
 * Safe unique batch id — no confidential entity/client data.
 */
export function createBatchId(now = new Date()) {
  const y = now.getUTCFullYear();
  const m = String(now.getUTCMonth() + 1).padStart(2, "0");
  const d = String(now.getUTCDate()).padStart(2, "0");
  const rand = randomBytes(4).toString("hex");
  return `aiv_batch_${y}${m}${d}_${rand}`;
}

export function hashPromptText(text) {
  return createHash("sha256").update(String(text || "")).digest("hex").slice(0, 16);
}

/**
 * Freeze cohort into a batch + manifest (before provider calls).
 */
export function createExecutionBatch({
  cohort,
  stakeholder,
  entityScope,
  geographyScope,
  commercialRegion = null,
  country = null,
  subregion = null,
  language: languageArg = null,
  provider = "openai",
  model,
  peerSet = null,
  entityIndexFingerprint = null,
  now = new Date(),
}) {
  const langReq = requireSupportedLanguage(languageArg ?? cohort?.language ?? "en");
  if (!langReq.ok) {
    throw Object.assign(new Error(langReq.message), {
      code: langReq.reasonCode,
      health: BATCH_HEALTH.PROMPT_CONFIG_ERROR,
    });
  }
  const language = langReq.language;
  const languageSource =
    languageArg != null && String(languageArg).trim() !== ""
      ? "explicit"
      : cohort?.language
        ? "cohort"
        : "default_en_compat";

  const batchId = createBatchId(now);
  const members = cohort?.members || [];
  const plannedRuns = members.length;

  const batch = {
    batchId,
    batchVersion: EXECUTION_BATCH_VERSION,
    stakeholder: stakeholder || null,
    entityScope: entityScope || null,
    geographyScope: geographyScope || null,
    commercialRegion: commercialRegion || null,
    country: country || null,
    subregion: subregion || null,
    language,
    languageSource,
    languageModelVersion: AI_VISIBILITY_LANGUAGE_VERSION,
    LANGUAGE_HOMOGENEOUS: true,
    cohortFingerprint: cohort?.fingerprint || null,
    peerSetId: peerSet?.peerSetId || null,
    peerSetVersion: peerSet?.peerSetVersion || null,
    peerEntityIds: peerSet?.entityIds || null,
    peerSetValid: peerSet?.ok !== false && peerSet?.canonicalValid !== false,
    peerSetError: peerSet?.error || null,
    provider,
    model,
    promptIds: members.map((m) => m.promptId),
    promptVersions: members.map((m) => ({
      promptId: m.promptId,
      version: m.version,
      language: m.language || language,
      semanticPairId: m.semanticPairId || null,
    })),
    requestedAt: now.toISOString(),
    startedAt: null,
    completedAt: null,
    status: "planned",
    health: null,
    plannedRuns,
    successfulRuns: 0,
    failedRuns: 0,
    skippedRuns: 0,
    retries: 0,
    usage: {
      inputTokens: 0,
      outputTokens: 0,
      totalTokens: 0,
      estimatedCost: 0,
    },
    entityIndexFingerprint,
    resolverVersion: RESOLVER_VERSION,
    classifierVersion: RECOMMENDATION_CLASSIFIER_VERSION,
    citationAssocVersion: CITATION_ASSOC_VERSION,
    metricVersion: METRIC_VERSION,
    geographyModelVersion: GEOGRAPHY_MODEL_VERSION,
    parserVersion: PARSER_VERSION,
    errorSummary: [],
  };

  const manifest = {
    batchId,
    cohortFingerprint: batch.cohortFingerprint,
    prompts: members.map((m) => ({
      promptId: m.promptId,
      version: m.version,
      promptFamily: m.promptFamily || null,
      intentTerritory: m.intentTerritory || null,
      geographyScope: m.geographyScope || null,
      commercialRegion: m.commercialRegion || null,
      country: m.country || null,
      promptTextHash: hashPromptText(m.promptText || ""),
      language: m.language || language,
      semanticPairId: m.semanticPairId || null,
    })),
    provider,
    model,
    geographyScope: batch.geographyScope,
    commercialRegion: batch.commercialRegion,
    country: batch.country,
    language: batch.language,
    languageModelVersion: batch.languageModelVersion,
    peerSetId: batch.peerSetId,
    peerSetVersion: batch.peerSetVersion,
    peerEntityIds: batch.peerEntityIds,
    peerSetValid: batch.peerSetValid,
    resolverVersion: batch.resolverVersion,
    classifierVersion: batch.classifierVersion,
    citationAssocVersion: batch.citationAssocVersion,
    metricVersion: batch.metricVersion,
    geographyModelVersion: batch.geographyModelVersion,
    parserVersion: batch.parserVersion,
    entityIndexFingerprint,
    plannedRuns,
    createdAt: now.toISOString(),
  };

  return { batch, manifest };
}

export function deriveBatchStatus({ successfulRuns, failedRuns, costLimitReached }) {
  if (costLimitReached && successfulRuns > 0) return "partial";
  if (successfulRuns > 0 && failedRuns > 0) return "partial";
  if (successfulRuns > 0 && failedRuns === 0) return "completed";
  if (successfulRuns === 0 && failedRuns > 0) return "failed";
  return "failed";
}

export function deriveBatchHealth({
  status,
  costLimitReached,
  entityIndexError,
  peerSetError,
  providerAuthError,
  promptConfigError,
}) {
  if (promptConfigError) return BATCH_HEALTH.PROMPT_CONFIG_ERROR;
  if (entityIndexError) return BATCH_HEALTH.ENTITY_INDEX_ERROR;
  if (providerAuthError) return BATCH_HEALTH.PROVIDER_AUTH_ERROR;
  if (costLimitReached) return BATCH_HEALTH.COST_LIMIT_REACHED;
  if (peerSetError) return BATCH_HEALTH.PEER_SET_ERROR;
  if (status === "partial") return BATCH_HEALTH.PARTIAL_PROVIDER_FAILURE;
  if (status === "completed") return BATCH_HEALTH.HEALTHY;
  return BATCH_HEALTH.PARTIAL_PROVIDER_FAILURE;
}

/** Transient provider errors eligible for one retry. */
export function isRetryableProviderError(err) {
  if (!err) return false;
  if (err.retryable === true) return true;
  const status = err.status ?? err.statusCode ?? null;
  if ([429, 500, 502, 503, 504, 408].includes(status)) return true;
  const type = String(err.type || "").toLowerCase();
  if (["rate_limit", "timeout", "upstream_error"].includes(type)) return true;
  return false;
}

export function isAuthProviderError(err) {
  const status = err?.status ?? err?.statusCode ?? null;
  if (status === 401 || status === 403) return true;
  const type = String(err?.type || "").toLowerCase();
  return type === "auth_error" || type === "config_error";
}

/**
 * Accidental duplicate protection within a short safety window.
 * Legitimate weekly monitoring is not blocked by this alone.
 */
export function findDuplicateRecentBatch(batches, key, windowMs = 15 * 60 * 1000, now = Date.now()) {
  const list = Array.isArray(batches) ? batches : [];
  for (const b of list) {
    if (!b) continue;
    const match =
      b.cohortFingerprint === key.cohortFingerprint &&
      b.provider === key.provider &&
      b.model === key.model &&
      b.geographyScope === key.geographyScope &&
      String(b.commercialRegion || "") === String(key.commercialRegion || "") &&
      String(b.country || "") === String(key.country || "") &&
      String(b.language || "en") === String(key.language || "en");
    if (!match) continue;
    const ts = Date.parse(b.requestedAt || b.startedAt || b.createdAt || 0);
    if (Number.isFinite(ts) && now - ts < windowMs) {
      if (["planned", "running", "completed", "partial"].includes(b.status)) {
        return b;
      }
    }
  }
  return null;
}

export function buildDuplicateKey(batchLike) {
  return {
    cohortFingerprint: batchLike.cohortFingerprint,
    provider: batchLike.provider,
    model: batchLike.model,
    geographyScope: batchLike.geographyScope,
    commercialRegion: batchLike.commercialRegion || null,
    country: batchLike.country || null,
    language: batchLike.language || "en",
  };
}
