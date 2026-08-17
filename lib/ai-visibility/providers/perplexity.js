/**
 * Perplexity adapter for AI Visibility (Phase 3B.1).
 *
 * Execution mode: Sonar Chat Completions (NOT Agent API).
 * Rationale: Sonar best represents consumer recommendation/search experience with
 * web-grounded answers, top-level citations[], search_results[] metadata, EN/ES support.
 *
 * Docs: https://docs.perplexity.ai/guides/chat-completions
 * No live calls in dry-run / test paths unless explicitly enabled later.
 */

import { PARSER_VERSION } from "../config.js";
import {
  ProviderError,
  normalizeProviderHttpError,
  redactSecrets,
} from "./base-provider.js";
import { buildProviderCapabilityFlags } from "./provider-interface.js";
import { classifyProviderError } from "./provider-errors.js";
import { estimateProviderCost } from "./provider-cost.js";
import { normalizeProviderCitations } from "./normalized-citation.js";
import { normalizeVisibilityProviderResponse } from "./normalized-response.js";

export const RECOMMENDED_PERPLEXITY_EXECUTION_MODE = "sonar";
export const RECOMMENDED_PERPLEXITY_EXECUTION_MODE_WHY =
  "Sonar chat completions provide realistic web-grounded recommendation behavior, top-level citations, search_results metadata, EN/ES support, stable API, and reproducible monitoring — Agent API is orchestration-oriented and less representative of consumer UX.";

const DEFAULT_MODEL = "sonar";
const API_URL = "https://api.perplexity.ai/chat/completions";

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function resolvePerplexityModel(model) {
  return (
    nz(model) ||
    nz(process.env.AI_VISIBILITY_PERPLEXITY_MODEL) ||
    DEFAULT_MODEL
  );
}

export function getPerplexityProviderCapabilities() {
  return buildProviderCapabilityFlags({
    supportsWebSearch: true,
    supportsInlineCitations: false,
    supportsTopLevelCitationList: true,
    supportsSearchResultMetadata: true,
    supportsWebGroundingMetadata: false,
    supportsUsageTokens: true,
    supportsProviderCost: true,
    supportsResponseId: true,
    supportsMultilingual: true,
    supportsToolUse: false,
  });
}

export function getPerplexityModelIdentity(args = {}) {
  const model = resolvePerplexityModel(args.model);
  return { provider: "perplexity", providerVendor: "Perplexity", providerModel: model };
}

/**
 * Build Perplexity Sonar request without network I/O.
 */
export function buildPerplexityVisibilityRequest(args = {}) {
  const {
    prompt,
    model,
    enableWebSearch = true,
    timeoutMs = Number(process.env.AI_VISIBILITY_PROVIDER_TIMEOUT_MS || 120000),
  } = args;

  const resolvedModel = resolvePerplexityModel(model);
  const inputText = nz(prompt?.text);
  const errors = [];
  if (!inputText) errors.push("missing_prompt_text");
  if (!resolvedModel) errors.push("missing_model");

  const body = {
    model: resolvedModel,
    messages: [{ role: "user", content: inputText }],
  };
  // Sonar models are inherently web-grounded; return_citations when supported
  if (enableWebSearch) {
    body.return_citations = true;
    body.return_related_questions = false;
  }

  return {
    ok: errors.length === 0,
    errors,
    provider: "perplexity",
    executionMode: RECOMMENDED_PERPLEXITY_EXECUTION_MODE,
    api: "chat/completions",
    url: API_URL,
    method: "POST",
    model: resolvedModel,
    enableWebSearch: Boolean(enableWebSearch),
    timeoutMs: Number.isFinite(timeoutMs) ? timeoutMs : 120000,
    headersTemplate: {
      Authorization: "Bearer ${PERPLEXITY_API_KEY}",
      "Content-Type": "application/json",
    },
    body,
    promptId: prompt?.promptId || null,
    LIVE_PROVIDER_CALL: false,
  };
}

function extractPerplexityText(raw) {
  const choice = raw?.choices?.[0];
  const content = choice?.message?.content;
  if (typeof content === "string") return content.trim();
  if (Array.isArray(content)) {
    return content
      .map((b) => (typeof b === "string" ? b : b?.text || ""))
      .filter(Boolean)
      .join("\n")
      .trim();
  }
  return "";
}

function extractPerplexityCitations(raw) {
  const urlList = Array.isArray(raw?.citations) ? raw.citations : [];
  const searchResults = Array.isArray(raw?.search_results) ? raw.search_results : [];

  const citations = [];
  urlList.forEach((url, i) => {
    const u = nz(url);
    if (!u) return;
    const match = searchResults.find((sr) => nz(sr.url) === u);
    citations.push({
      url: u,
      title: match?.title ? String(match.title) : null,
      startIndex: null,
      endIndex: null,
      citedText: match?.snippet ? String(match.snippet) : null,
      providerSupplied: true,
      citationType: "top_level_url",
      citationPosition: i + 1,
      searchResultRank: match ? searchResults.indexOf(match) + 1 : null,
      rawProviderCitation: match || { url: u },
    });
  });

  const normalizedSearchResults = searchResults.map((sr, i) => ({
    rank: i + 1,
    url: nz(sr.url) || null,
    title: sr.title ? String(sr.title) : null,
    date: sr.date || sr.last_updated || null,
    snippet: sr.snippet ? String(sr.snippet) : null,
    raw: sr,
  }));

  return {
    citations,
    searchResults: normalizedSearchResults,
    searchMetadata: {
      citationCount: urlList.length,
      searchResultCount: searchResults.length,
    },
    webSearchUsed: urlList.length > 0 || searchResults.length > 0,
  };
}

export function extractPerplexityUsage(raw) {
  const u = raw?.usage;
  if (!u) return null;
  const cost = raw?.cost ?? u?.cost ?? null;
  return {
    inputTokens: u.prompt_tokens ?? u.input_tokens ?? null,
    outputTokens: u.completion_tokens ?? u.output_tokens ?? null,
    totalTokens: u.total_tokens ?? null,
    providerCostUsd: cost?.total_cost ?? cost ?? null,
  };
}

export function extractPerplexityEvidence(raw, ctx = {}) {
  const extracted = extractPerplexityCitations(raw);
  return {
    citations: normalizeProviderCitations(extracted.citations, {
      provider: "perplexity",
      fingerprint: ctx.fingerprint,
    }),
    searchResults: extracted.searchResults,
    searchMetadata: extracted.searchMetadata,
  };
}

export function normalizePerplexityResponse(raw, ctx = {}) {
  const text = extractPerplexityText(raw);
  const extracted = extractPerplexityCitations(raw);
  const usage = extractPerplexityUsage(raw);
  const model = ctx.model || raw?.model || resolvePerplexityModel();

  let citationCapability = "unsupported";
  if (extracted.citations.length > 0) citationCapability = "supported";
  else if (extracted.webSearchUsed) citationCapability = "partial";

  const providerCost = usage?.providerCostUsd ?? null;

  return normalizeVisibilityProviderResponse(
    {
      provider: "perplexity",
      providerVendor: "Perplexity",
      model,
      text,
      citations: extracted.citations,
      searchResults: extracted.searchResults,
      searchMetadata: extracted.searchMetadata,
      usage,
      providerCost,
      citationCapability,
      parserVersion: PARSER_VERSION,
      providerCapabilities: getPerplexityProviderCapabilities(),
      finishReason: raw?.choices?.[0]?.finish_reason || null,
      providerMeta: {
        responseId: raw?.id || null,
        executionMode: RECOMMENDED_PERPLEXITY_EXECUTION_MODE,
      },
      raw,
    },
    { ...ctx, useV1_1: true, providerVendor: "Perplexity", providerCost }
  );
}

export function classifyPerplexityError(err) {
  return classifyProviderError(err);
}

export function estimatePerplexityCost(usage, options = {}) {
  return estimateProviderCost("perplexity", usage, options);
}

export async function runPerplexityVisibilityPrompt(args = {}) {
  const built = buildPerplexityVisibilityRequest(args);
  if (!built.ok) {
    throw new ProviderError(built.errors.join(", "), {
      type: "validation_error",
      retryable: false,
    });
  }

  const apiKey = nz(args.apiKey) || nz(process.env.PERPLEXITY_API_KEY);
  if (!apiKey) {
    throw new ProviderError("PERPLEXITY_API_KEY not configured", {
      type: "config_error",
      retryable: false,
    });
  }

  const { fetchImpl = fetch, timeoutMs = built.timeoutMs } = args;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  const started = Date.now();

  let res;
  let raw;
  try {
    res = await fetchImpl(built.url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(built.body),
      signal: controller.signal,
    });
    raw = await res.json().catch(() => ({}));
  } catch (err) {
    clearTimeout(timer);
    if (err?.name === "AbortError") {
      throw new ProviderError("Perplexity request timed out", { type: "timeout", retryable: true });
    }
    throw new ProviderError(err?.message || "Perplexity network error", {
      type: "network_error",
      retryable: true,
    });
  } finally {
    clearTimeout(timer);
  }

  const latencyMs = Date.now() - started;
  if (!res.ok) {
    const msg = raw?.error?.message || raw?.detail || `Perplexity API error (${res.status})`;
    throw normalizeProviderHttpError(res.status, msg);
  }

  const text = extractPerplexityText(raw);
  const extracted = extractPerplexityCitations(raw);
  let citationCapability = extracted.citations.length > 0 ? "supported" : "partial";

  return {
    provider: "perplexity",
    providerVendor: "Perplexity",
    model: built.model,
    text,
    citations: extracted.citations,
    searchResults: extracted.searchResults,
    searchMetadata: extracted.searchMetadata,
    usage: extractPerplexityUsage(raw),
    latencyMs,
    citationCapability,
    parserVersion: PARSER_VERSION,
    providerMeta: redactSecrets({
      api: "chat/completions",
      executionMode: RECOMMENDED_PERPLEXITY_EXECUTION_MODE,
      responseId: raw?.id || null,
    }),
    raw,
  };
}

export const perplexityProvider = {
  id: "perplexity",
  vendor: "Perplexity",
  getProviderCapabilities: getPerplexityProviderCapabilities,
  getModelIdentity: getPerplexityModelIdentity,
  buildRequest: buildPerplexityVisibilityRequest,
  execute: runPerplexityVisibilityPrompt,
  normalizeResponse: normalizePerplexityResponse,
  extractUsage: extractPerplexityUsage,
  extractEvidence: extractPerplexityEvidence,
  classifyError: classifyPerplexityError,
  estimateCost: estimatePerplexityCost,
  runVisibilityPrompt: runPerplexityVisibilityPrompt,
};

export const PERPLEXITY_EVIDENCE_READY = true;
