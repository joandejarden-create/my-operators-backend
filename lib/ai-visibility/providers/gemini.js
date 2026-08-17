/**
 * Gemini adapter for AI Visibility (Phase 3B.1).
 *
 * Verified against Google Generative Language API docs as of 2026-08-14:
 * - generateContent with tools: [{ google_search: {} }] for grounded answers.
 * - groundingMetadata provides groundingChunks, groundingSupports, webSearchQueries.
 * - Do not fabricate inline spans when provider only supplies chunk-level grounding.
 *
 * Docs: https://ai.google.dev/gemini-api/docs/google-search
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

const DEFAULT_MODEL = "gemini-3.6-flash";
const API_BASE = "https://generativelanguage.googleapis.com/v1beta";

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function resolveGeminiModel(model) {
  return nz(model) || nz(process.env.AI_VISIBILITY_GEMINI_MODEL) || DEFAULT_MODEL;
}

function buildGeminiUrl(model) {
  return `${API_BASE}/models/${encodeURIComponent(model)}:generateContent`;
}

export function getGeminiProviderCapabilities() {
  return buildProviderCapabilityFlags({
    supportsWebSearch: true,
    supportsInlineCitations: true,
    supportsTopLevelCitationList: false,
    supportsSearchResultMetadata: true,
    supportsWebGroundingMetadata: true,
    supportsUsageTokens: true,
    supportsProviderCost: false,
    supportsResponseId: false,
    supportsMultilingual: true,
    supportsToolUse: true,
  });
}

export function getGeminiModelIdentity(args = {}) {
  const model = resolveGeminiModel(args.model);
  return { provider: "gemini", providerVendor: "Google", providerModel: model };
}

/**
 * Build Gemini generateContent request without network I/O.
 */
export function buildGeminiVisibilityRequest(args = {}) {
  const {
    prompt,
    model,
    enableWebSearch = true,
    timeoutMs = Number(process.env.AI_VISIBILITY_PROVIDER_TIMEOUT_MS || 180000),
  } = args;

  const resolvedModel = resolveGeminiModel(model);
  const inputText = nz(prompt?.text);
  const errors = [];
  if (!inputText) errors.push("missing_prompt_text");
  if (!resolvedModel) errors.push("missing_model");

  const body = {
    contents: [{ role: "user", parts: [{ text: inputText }] }],
  };
  if (enableWebSearch) {
    body.tools = [{ google_search: {} }];
  }

  return {
    ok: errors.length === 0,
    errors,
    provider: "gemini",
    api: "generateContent",
    url: buildGeminiUrl(resolvedModel),
    method: "POST",
    model: resolvedModel,
    enableWebSearch: Boolean(enableWebSearch),
    timeoutMs: Number.isFinite(timeoutMs) ? timeoutMs : 180000,
    headersTemplate: {
      "Content-Type": "application/json",
      "x-goog-api-key": "${GEMINI_API_KEY}",
    },
    body,
    promptId: prompt?.promptId || null,
    LIVE_PROVIDER_CALL: false,
  };
}

function extractGeminiText(raw) {
  const parts = raw?.candidates?.[0]?.content?.parts || [];
  return parts
    .map((p) => (p?.text ? String(p.text) : ""))
    .filter(Boolean)
    .join("\n")
    .trim();
}

function extractGeminiGrounding(raw) {
  const grounding = raw?.candidates?.[0]?.groundingMetadata || raw?.groundingMetadata || {};
  const chunks = Array.isArray(grounding.groundingChunks) ? grounding.groundingChunks : [];
  const supports = Array.isArray(grounding.groundingSupports) ? grounding.groundingSupports : [];
  const webSearchQueries = Array.isArray(grounding.webSearchQueries)
    ? grounding.webSearchQueries
    : [];

  const citations = [];
  const searchResults = [];

  chunks.forEach((chunk, i) => {
    const web = chunk?.web || {};
    const url = nz(web.uri) || null;
    if (!url) return;
    searchResults.push({
      rank: i + 1,
      url,
      title: nz(web.title) || null,
      domain: null,
      snippet: null,
      raw: chunk,
    });
  });

  supports.forEach((support, i) => {
    const segment = support?.segment || {};
    const chunkIndices = Array.isArray(support.groundingChunkIndices)
      ? support.groundingChunkIndices
      : [];
    for (const idx of chunkIndices) {
      const chunk = chunks[idx];
      const web = chunk?.web || {};
      const url = nz(web.uri) || null;
      if (!url) continue;
      citations.push({
        url,
        title: nz(web.title) || null,
        startIndex: segment.startIndex ?? segment.start_index ?? null,
        endIndex: segment.endIndex ?? segment.end_index ?? null,
        citedText: nz(segment.text) || null,
        providerSupplied: true,
        citationType: "grounding_support",
        rawProviderCitation: support,
        searchResultRank: idx + 1,
      });
    }
    if (!chunkIndices.length && support?.uri) {
      citations.push({
        url: nz(support.uri) || null,
        title: null,
        startIndex: segment.startIndex ?? null,
        endIndex: segment.endIndex ?? null,
        citedText: nz(segment.text) || null,
        providerSupplied: true,
        citationType: "grounding_support",
        rawProviderCitation: support,
        citationPosition: i + 1,
      });
    }
  });

  // Deduplicate by URL while preserving first occurrence
  const seen = new Set();
  const deduped = [];
  for (const c of citations) {
    if (!c.url || seen.has(c.url)) continue;
    seen.add(c.url);
    deduped.push(c);
  }

  return {
    citations: deduped.map((c, i) => ({ ...c, citationPosition: i + 1 })),
    searchResults,
    searchMetadata: {
      webSearchQueries,
      groundingChunks: chunks.length,
      groundingSupports: supports.length,
    },
    webSearchUsed: Boolean(webSearchQueries.length || chunks.length),
  };
}

export function extractGeminiUsage(raw) {
  const meta = raw?.usageMetadata;
  if (!meta) return null;
  return {
    inputTokens: meta.promptTokenCount ?? null,
    outputTokens: meta.candidatesTokenCount ?? null,
    totalTokens: meta.totalTokenCount ?? null,
  };
}

export function extractGeminiEvidence(raw, ctx = {}) {
  const grounded = extractGeminiGrounding(raw);
  return {
    citations: normalizeProviderCitations(grounded.citations, {
      provider: "gemini",
      fingerprint: ctx.fingerprint,
    }),
    searchResults: grounded.searchResults,
    searchMetadata: grounded.searchMetadata,
  };
}

export function normalizeGeminiResponse(raw, ctx = {}) {
  const text = extractGeminiText(raw);
  const grounded = extractGeminiGrounding(raw);
  const usage = extractGeminiUsage(raw);
  const model = ctx.model || raw?.modelVersion || resolveGeminiModel();

  let citationCapability = "unsupported";
  if (ctx.enableWebSearch !== false) {
    if (grounded.citations.length > 0) citationCapability = "supported";
    else if (grounded.webSearchUsed) citationCapability = "partial";
    else citationCapability = "unavailable";
  }

  return normalizeVisibilityProviderResponse(
    {
      provider: "gemini",
      providerVendor: "Google",
      model,
      text,
      citations: grounded.citations,
      searchResults: grounded.searchResults,
      searchMetadata: grounded.searchMetadata,
      usage,
      citationCapability,
      parserVersion: PARSER_VERSION,
      providerCapabilities: getGeminiProviderCapabilities(),
      finishReason: raw?.candidates?.[0]?.finishReason || null,
      toolUsage: grounded.webSearchUsed ? { google_search: true } : null,
      providerSpecificMetadata: { groundingMetadata: raw?.candidates?.[0]?.groundingMetadata || null },
      raw,
    },
    { ...ctx, useV1_1: true, providerVendor: "Google" }
  );
}

export function classifyGeminiError(err) {
  return classifyProviderError(err);
}

export function estimateGeminiCost(usage, options = {}) {
  return estimateProviderCost("gemini", usage, options);
}

/**
 * Execute Gemini request — only when API key present and not dry-run.
 */
export async function runGeminiVisibilityPrompt(args = {}) {
  const built = buildGeminiVisibilityRequest(args);
  if (!built.ok) {
    throw new ProviderError(built.errors.join(", "), {
      type: "validation_error",
      retryable: false,
    });
  }

  const apiKey = nz(args.apiKey) || nz(process.env.GEMINI_API_KEY);
  if (!apiKey) {
    throw new ProviderError("GEMINI_API_KEY not configured", {
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
    res = await fetchImpl(`${built.url}?key=${encodeURIComponent(apiKey)}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(built.body),
      signal: controller.signal,
    });
    raw = await res.json().catch(() => ({}));
  } catch (err) {
    clearTimeout(timer);
    if (err?.name === "AbortError") {
      throw new ProviderError("Gemini request timed out", { type: "timeout", retryable: true });
    }
    throw new ProviderError(err?.message || "Gemini network error", {
      type: "network_error",
      retryable: true,
    });
  } finally {
    clearTimeout(timer);
  }

  const latencyMs = Date.now() - started;
  if (!res.ok) {
    const msg = raw?.error?.message || `Gemini API error (${res.status})`;
    throw normalizeProviderHttpError(res.status, msg);
  }

  const text = extractGeminiText(raw);
  const grounded = extractGeminiGrounding(raw);
  let citationCapability = "unsupported";
  if (built.enableWebSearch) {
    if (grounded.citations.length > 0) citationCapability = "supported";
    else if (grounded.webSearchUsed) citationCapability = "partial";
    else citationCapability = "unavailable";
  }

  return {
    provider: "gemini",
    providerVendor: "Google",
    model: built.model,
    text,
    citations: grounded.citations,
    searchResults: grounded.searchResults,
    searchMetadata: grounded.searchMetadata,
    usage: extractGeminiUsage(raw),
    latencyMs,
    citationCapability,
    parserVersion: PARSER_VERSION,
    providerMeta: redactSecrets({
      api: "generateContent",
      webSearchRequested: built.enableWebSearch,
      webSearchUsed: grounded.webSearchUsed,
      searchQueries: grounded.searchMetadata?.webSearchQueries || [],
    }),
    raw,
  };
}

export const geminiProvider = {
  id: "gemini",
  vendor: "Google",
  getProviderCapabilities: getGeminiProviderCapabilities,
  getModelIdentity: getGeminiModelIdentity,
  buildRequest: buildGeminiVisibilityRequest,
  execute: runGeminiVisibilityPrompt,
  normalizeResponse: normalizeGeminiResponse,
  extractUsage: extractGeminiUsage,
  extractEvidence: extractGeminiEvidence,
  classifyError: classifyGeminiError,
  estimateCost: estimateGeminiCost,
  runVisibilityPrompt: runGeminiVisibilityPrompt,
};

export const GEMINI_EVIDENCE_READY = true;
