/**
 * Claude adapter for AI Visibility (Phase 3B.1).
 *
 * Verified against Anthropic Messages API docs as of 2026-08-14:
 * - web_search_20260209 server tool with allowed_callers: ["direct"] for monitoring.
 * - Bounded max_uses — no unlimited search loops.
 * - Pause/continuation handling for server tools (max 2 continuation turns).
 *
 * Docs: https://docs.anthropic.com/en/docs/agents-and-tools/tool-use/web-search-tool
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
import { CLAUDE_RETRY_POLICY } from "./provider-retry-policy.js";

const DEFAULT_MODEL = "claude-sonnet-4-6";
const API_URL = "https://api.anthropic.com/v1/messages";
const WEB_SEARCH_TOOL_TYPE = "web_search_20260209";

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function resolveClaudeModel(model) {
  return nz(model) || nz(process.env.AI_VISIBILITY_CLAUDE_MODEL) || DEFAULT_MODEL;
}

/** Bounded web-search policy — no unlimited loops. */
export const CLAUDE_WEB_SEARCH_POLICY = Object.freeze({
  toolType: WEB_SEARCH_TOOL_TYPE,
  maxUses: CLAUDE_RETRY_POLICY.webSearchMaxUses,
  allowedCallers: ["direct"],
  pauseContinuationEnabled: CLAUDE_RETRY_POLICY.pauseContinuationEnabled,
  maxContinuationTurns: 2,
  timeoutMs: CLAUDE_RETRY_POLICY.timeoutMsDefault,
});

export function getClaudeProviderCapabilities() {
  return buildProviderCapabilityFlags({
    supportsWebSearch: true,
    supportsInlineCitations: true,
    supportsTopLevelCitationList: false,
    supportsSearchResultMetadata: true,
    supportsWebGroundingMetadata: true,
    supportsUsageTokens: true,
    supportsProviderCost: false,
    supportsResponseId: true,
    supportsMultilingual: true,
    supportsToolUse: true,
  });
}

export function getClaudeModelIdentity(args = {}) {
  const model = resolveClaudeModel(args.model);
  return { provider: "claude", providerVendor: "Anthropic", providerModel: model };
}

/**
 * Build Claude Messages API request without network I/O.
 */
export function buildClaudeVisibilityRequest(args = {}) {
  const {
    prompt,
    model,
    enableWebSearch = true,
    maxSearchUses = CLAUDE_WEB_SEARCH_POLICY.maxUses,
    timeoutMs = Number(process.env.AI_VISIBILITY_PROVIDER_TIMEOUT_MS || 300000),
  } = args;

  const resolvedModel = resolveClaudeModel(model);
  const inputText = nz(prompt?.text);
  const errors = [];
  if (!inputText) errors.push("missing_prompt_text");
  if (!resolvedModel) errors.push("missing_model");

  const body = {
    model: resolvedModel,
    max_tokens: 4096,
    messages: [{ role: "user", content: inputText }],
  };

  if (enableWebSearch) {
    // Explicit allowed_callers:["direct"] — web_search_20260209 defaults to
    // code_execution dynamic filtering, which adds latency/agentic behavior
    // inappropriate for governed recommendation monitoring.
    body.tools = [
      {
        type: WEB_SEARCH_TOOL_TYPE,
        name: "web_search",
        max_uses: maxSearchUses,
        allowed_callers: [...CLAUDE_WEB_SEARCH_POLICY.allowedCallers],
      },
    ];
    body.tool_choice = { type: "auto" };
  }

  return {
    ok: errors.length === 0,
    errors,
    provider: "claude",
    api: "messages",
    url: API_URL,
    method: "POST",
    model: resolvedModel,
    enableWebSearch: Boolean(enableWebSearch),
    webSearchPolicy: { ...CLAUDE_WEB_SEARCH_POLICY, maxUses: maxSearchUses },
    timeoutMs: Number.isFinite(timeoutMs) ? timeoutMs : 300000,
    headersTemplate: {
      "x-api-key": "${ANTHROPIC_API_KEY}",
      "anthropic-version": "2023-06-01",
      "Content-Type": "application/json",
    },
    body,
    promptId: prompt?.promptId || null,
    LIVE_PROVIDER_CALL: false,
  };
}

function extractClaudeTextBlocks(content = []) {
  const textParts = [];
  for (const block of content) {
    if (block?.type === "text" && block.text) {
      textParts.push(String(block.text));
    }
  }
  return textParts.join("\n").trim();
}

function extractClaudeCitations(content = []) {
  const citations = [];
  const searchResults = [];
  let webSearchUsed = false;

  for (const block of content) {
    if (block?.type === "web_search_tool_result") {
      webSearchUsed = true;
      const results = Array.isArray(block.content) ? block.content : [];
      results.forEach((r, i) => {
        const url = nz(r.url) || null;
        if (!url) return;
        searchResults.push({
          rank: i + 1,
          url,
          title: nz(r.title) || null,
          snippet: nz(r.page_age) || null,
          raw: r,
        });
      });
    }
    if (block?.type === "text") {
      const blockCitations = Array.isArray(block.citations) ? block.citations : [];
      for (const c of blockCitations) {
        citations.push({
          url: nz(c.url) || null,
          title: nz(c.title) || null,
          citedText: nz(c.cited_text) || null,
          startIndex: c.start_char_index ?? c.start_index ?? null,
          endIndex: c.end_char_index ?? c.end_index ?? null,
          providerSupplied: true,
          citationType: c.type || "web_search_result_location",
          rawProviderCitation: c,
        });
      }
    }
  }

  const deduped = [];
  const seen = new Set();
  for (const c of citations) {
    const key = `${c.url}|${c.startIndex}|${c.endIndex}`;
    if (!c.url || seen.has(key)) continue;
    seen.add(key);
    deduped.push(c);
  }

  return {
    citations: deduped.map((c, i) => ({ ...c, citationPosition: i + 1 })),
    searchResults,
    webSearchUsed,
  };
}

export function extractClaudeUsage(raw) {
  const u = raw?.usage;
  if (!u) return null;
  return {
    inputTokens: u.input_tokens ?? null,
    outputTokens: u.output_tokens ?? null,
    totalTokens:
      u.input_tokens != null && u.output_tokens != null
        ? u.input_tokens + u.output_tokens
        : null,
  };
}

export function extractClaudeEvidence(raw, ctx = {}) {
  const content = Array.isArray(raw?.content) ? raw.content : [];
  const extracted = extractClaudeCitations(content);
  return {
    citations: normalizeProviderCitations(extracted.citations, {
      provider: "claude",
      fingerprint: ctx.fingerprint,
    }),
    searchResults: extracted.searchResults,
    searchMetadata: {
      webSearchUsed: extracted.webSearchUsed,
      stopReason: raw?.stop_reason || null,
    },
  };
}

export function normalizeClaudeResponse(raw, ctx = {}) {
  const content = Array.isArray(raw?.content) ? raw.content : [];
  const text = extractClaudeTextBlocks(content);
  const extracted = extractClaudeCitations(content);
  const usage = extractClaudeUsage(raw);
  const model = ctx.model || raw?.model || resolveClaudeModel();

  let citationCapability = "unsupported";
  if (ctx.enableWebSearch !== false) {
    if (extracted.citations.length > 0) citationCapability = "supported";
    else if (extracted.webSearchUsed) citationCapability = "partial";
    else citationCapability = "unavailable";
  }

  return normalizeVisibilityProviderResponse(
    {
      provider: "claude",
      providerVendor: "Anthropic",
      model,
      text,
      citations: extracted.citations,
      searchResults: extracted.searchResults,
      searchMetadata: extracted.searchMetadata,
      usage,
      citationCapability,
      parserVersion: PARSER_VERSION,
      providerCapabilities: getClaudeProviderCapabilities(),
      stopReason: raw?.stop_reason || null,
      toolUsage: extracted.webSearchUsed ? { web_search: true } : null,
      providerMeta: { responseId: raw?.id || null },
      raw,
    },
    { ...ctx, useV1_1: true, providerVendor: "Anthropic" }
  );
}

export function classifyClaudeError(err) {
  return classifyProviderError(err);
}

export function estimateClaudeCost(usage, options = {}) {
  return estimateProviderCost("claude", usage, options);
}

/**
 * Execute with bounded pause/continuation for server tools.
 */
export async function runClaudeVisibilityPrompt(args = {}) {
  const built = buildClaudeVisibilityRequest(args);
  if (!built.ok) {
    throw new ProviderError(built.errors.join(", "), {
      type: "validation_error",
      retryable: false,
    });
  }

  const apiKey =
    nz(args.apiKey) ||
    nz(process.env.ANTHROPIC_API_KEY) ||
    nz(process.env.CLAUDE_API_KEY);
  if (!apiKey) {
    throw new ProviderError("ANTHROPIC_API_KEY not configured", {
      type: "config_error",
      retryable: false,
    });
  }

  const { fetchImpl = fetch, timeoutMs = built.timeoutMs } = args;
  const maxTurns = CLAUDE_WEB_SEARCH_POLICY.maxContinuationTurns + 1;
  let messages = [...built.body.messages];
  let raw = null;
  const started = Date.now();

  for (let turn = 0; turn < maxTurns; turn += 1) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);

    let res;
    try {
      res = await fetchImpl(built.url, {
        method: "POST",
        headers: {
          "x-api-key": apiKey,
          "anthropic-version": "2023-06-01",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ ...built.body, messages }),
        signal: controller.signal,
      });
      raw = await res.json().catch(() => ({}));
    } catch (err) {
      clearTimeout(timer);
      if (err?.name === "AbortError") {
        throw new ProviderError("Claude request timed out", { type: "timeout", retryable: true });
      }
      throw new ProviderError(err?.message || "Claude network error", {
        type: "network_error",
        retryable: true,
      });
    } finally {
      clearTimeout(timer);
    }

    if (!res.ok) {
      const msg = raw?.error?.message || `Claude API error (${res.status})`;
      throw normalizeProviderHttpError(res.status, msg);
    }

    if (raw?.stop_reason !== "pause_turn" || turn >= maxTurns - 1) break;

    messages = [
      ...messages,
      { role: "assistant", content: raw.content },
      { role: "user", content: "Continue." },
    ];
  }

  const latencyMs = Date.now() - started;
  const content = Array.isArray(raw?.content) ? raw.content : [];
  const text = extractClaudeTextBlocks(content);
  const extracted = extractClaudeCitations(content);

  let citationCapability = "unsupported";
  if (built.enableWebSearch) {
    if (extracted.citations.length > 0) citationCapability = "supported";
    else if (extracted.webSearchUsed) citationCapability = "partial";
    else citationCapability = "unavailable";
  }

  return {
    provider: "claude",
    providerVendor: "Anthropic",
    model: built.model,
    text,
    citations: extracted.citations,
    searchResults: extracted.searchResults,
    searchMetadata: extracted.searchMetadata,
    usage: extractClaudeUsage(raw),
    latencyMs,
    citationCapability,
    parserVersion: PARSER_VERSION,
    stopReason: raw?.stop_reason || null,
    providerMeta: redactSecrets({
      api: "messages",
      webSearchRequested: built.enableWebSearch,
      webSearchUsed: extracted.webSearchUsed,
      responseId: raw?.id || null,
      webSearchPolicy: CLAUDE_WEB_SEARCH_POLICY,
    }),
    raw,
  };
}

export const claudeProvider = {
  id: "claude",
  vendor: "Anthropic",
  getProviderCapabilities: getClaudeProviderCapabilities,
  getModelIdentity: getClaudeModelIdentity,
  buildRequest: buildClaudeVisibilityRequest,
  execute: runClaudeVisibilityPrompt,
  normalizeResponse: normalizeClaudeResponse,
  extractUsage: extractClaudeUsage,
  extractEvidence: extractClaudeEvidence,
  classifyError: classifyClaudeError,
  estimateCost: estimateClaudeCost,
  runVisibilityPrompt: runClaudeVisibilityPrompt,
};

export const CLAUDE_EVIDENCE_READY = true;
