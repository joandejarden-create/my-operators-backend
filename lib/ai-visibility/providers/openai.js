/**
 * OpenAI adapter for AI Visibility.
 *
 * Verified against OpenAI primary docs (Web search guide / Responses API)
 * as of 2026-08-13 (Phase 1) and re-checked Phase 2A:
 * - Prefer Responses API with tools: [{ type: "web_search" }] for grounded answers.
 * - Docs currently exemplify models such as gpt-5.6 / gpt-5.5 for web search;
 *   Phase 2A default remains env AI_VISIBILITY_MODEL or gpt-4.1 until founder sets
 *   a verified production model for this monitoring use-case.
 * - Citations appear as message content annotations with type "url_citation"
 *   (url, title, start_index, end_index). Do not fabricate citations.
 * - Optional sources list (web_search_call.action.sources) may exceed inline citations;
 *   Phase 2A stores provider-supplied url_citation annotations only.
 * - Existing Dealality Chat Completions enrichment paths are NOT equivalent.
 * - If web_search / annotations unavailable for the configured model or account,
 *   citationCapability is marked unsupported/unavailable and citations stay empty.
 *
 * Docs: https://developers.openai.com/api/docs/guides/tools-web-search
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
import { normalizeVisibilityProviderResponse } from "./normalized-response.js";

const RESPONSES_URL = "https://api.openai.com/v1/responses";

function nz(v) {
  return v == null ? "" : String(v).trim();
}

/**
 * Build OpenAI web_search user_location from governed provider location context.
 * Docs: tools[].user_location approximate { country, city, region, timezone }.
 * @param {object|null|undefined} loc
 * @returns {object|null}
 */
export function buildOpenAiWebSearchUserLocation(loc) {
  if (!loc || typeof loc !== "object") return null;
  const city = nz(loc.city);
  const region = nz(loc.region);
  const country = nz(loc.country);
  const timezone = nz(loc.timezone);
  if (!country && !city && !region) return null;
  const out = { type: "approximate" };
  if (country) out.country = country;
  if (city) out.city = city;
  if (region) out.region = region;
  if (timezone) out.timezone = timezone;
  return out;
}

/**
 * Build OpenAI Responses API request payload without network I/O (Phase 3A.10 dry-run).
 * Does not require API key. Does not send requests.
 *
 * @param {{ prompt: object, model?: string, context?: object, enableWebSearch?: boolean, timeoutMs?: number }} args
 */
export function buildOpenAiVisibilityRequest(args = {}) {
  const {
    prompt,
    model,
    context = {},
    enableWebSearch = true,
    timeoutMs = Number(process.env.AI_VISIBILITY_PROVIDER_TIMEOUT_MS || 60000),
  } = args;

  const resolvedModel = nz(model) || nz(process.env.AI_VISIBILITY_MODEL) || "gpt-5.6";
  const inputText = nz(prompt?.text);
  const errors = [];
  if (!inputText) errors.push("missing_prompt_text");
  if (!resolvedModel) errors.push("missing_model");

  const body = {
    model: resolvedModel,
    input: inputText,
  };
  const userLocation = buildOpenAiWebSearchUserLocation(
    context.providerLocationContext || context.userLocation || null
  );
  if (enableWebSearch) {
    body.tools = userLocation
      ? [{ type: "web_search", user_location: userLocation }]
      : [{ type: "web_search" }];
  }
  if (context?.instructions) {
    body.instructions = String(context.instructions);
  }

  return {
    ok: errors.length === 0,
    errors,
    provider: "openai",
    api: "responses",
    url: RESPONSES_URL,
    method: "POST",
    model: resolvedModel,
    enableWebSearch: Boolean(enableWebSearch),
    providerLocationContext: userLocation,
    timeoutMs: Number.isFinite(timeoutMs) ? timeoutMs : 60000,
    headersTemplate: {
      Authorization: "Bearer ${OPENAI_API_KEY}",
      "Content-Type": "application/json",
    },
    body,
    promptId: prompt?.promptId || null,
    LIVE_PROVIDER_CALL: false,
  };
}

function extractTextAndAnnotations(raw) {
  const citations = [];
  const textParts = [];
  let webSearchUsed = false;

  const output = Array.isArray(raw?.output) ? raw.output : [];
  for (const item of output) {
    if (item?.type === "web_search_call") webSearchUsed = true;
    if (item?.type !== "message") continue;
    const content = Array.isArray(item.content) ? item.content : [];
    for (const block of content) {
      if (block?.type === "output_text" || block?.type === "text") {
        if (block.text) textParts.push(String(block.text));
      }
      const annotations = Array.isArray(block?.annotations) ? block.annotations : [];
      for (const ann of annotations) {
        if (ann?.type !== "url_citation") continue;
        citations.push({
          url: nz(ann.url) || null,
          title: nz(ann.title) || null,
          startIndex: ann.start_index ?? null,
          endIndex: ann.end_index ?? null,
          providerSupplied: true,
        });
      }
    }
  }

  // Fallback: some SDK wrappers expose output_text
  if (!textParts.length && nz(raw?.output_text)) {
    textParts.push(nz(raw.output_text));
  }

  return {
    text: textParts.join("\n").trim(),
    citations,
    webSearchUsed,
  };
}

function mapUsage(raw) {
  const u = raw?.usage;
  if (!u || typeof u !== "object") return null;
  return {
    inputTokens: u.input_tokens ?? u.prompt_tokens ?? null,
    outputTokens: u.output_tokens ?? u.completion_tokens ?? null,
    totalTokens: u.total_tokens ?? null,
  };
}

/**
 * @param {{ prompt: object, model?: string, context?: object, fetchImpl?: typeof fetch, apiKey?: string, timeoutMs?: number, enableWebSearch?: boolean }} args
 */
export async function runVisibilityPrompt(args) {
  const {
    prompt,
    model,
    context = {},
    fetchImpl = fetch,
    apiKey = process.env.OPENAI_API_KEY,
    timeoutMs = Number(process.env.AI_VISIBILITY_PROVIDER_TIMEOUT_MS || 60000),
    enableWebSearch = true,
  } = args;

  if (!nz(apiKey)) {
    throw new ProviderError("OPENAI_API_KEY not configured", {
      type: "config_error",
      retryable: false,
    });
  }

  const resolvedModel = nz(model) || nz(process.env.AI_VISIBILITY_MODEL) || "gpt-4.1";
  const inputText = nz(prompt?.text);
  if (!inputText) {
    throw new ProviderError("Prompt text is required", {
      type: "validation_error",
      retryable: false,
    });
  }

  const body = {
    model: resolvedModel,
    input: inputText,
  };
  const userLocation = buildOpenAiWebSearchUserLocation(
    context.providerLocationContext || context.userLocation || null
  );
  if (enableWebSearch) {
    body.tools = userLocation
      ? [{ type: "web_search", user_location: userLocation }]
      : [{ type: "web_search" }];
  }
  if (context?.instructions) {
    body.instructions = String(context.instructions);
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  const started = Date.now();

  let res;
  let raw;
  try {
    res = await fetchImpl(RESPONSES_URL, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
      signal: controller.signal,
    });
    raw = await res.json().catch(() => ({}));
  } catch (err) {
    clearTimeout(timer);
    if (err?.name === "AbortError") {
      throw new ProviderError("OpenAI request timed out", {
        type: "timeout",
        retryable: true,
      });
    }
    throw new ProviderError(err?.message || "OpenAI network error", {
      type: "network_error",
      retryable: true,
    });
  } finally {
    clearTimeout(timer);
  }

  const latencyMs = Date.now() - started;

  if (!res.ok) {
    const msg = raw?.error?.message || `OpenAI API error (${res.status})`;
    throw normalizeProviderHttpError(res.status, msg);
  }

  const extracted = extractTextAndAnnotations(raw);
  let citationCapability = "unsupported";
  if (enableWebSearch) {
    if (extracted.citations.length > 0) citationCapability = "supported";
    else if (extracted.webSearchUsed) citationCapability = "partial";
    else citationCapability = "unavailable";
  }

  return {
    provider: "openai",
    model: resolvedModel,
    text: extracted.text,
    citations: extracted.citations.map((c, i) => ({
      ...c,
      citationPosition: i + 1,
    })),
    usage: mapUsage(raw),
    latencyMs,
    citationCapability,
    parserVersion: PARSER_VERSION,
    providerMeta: redactSecrets({
      api: "responses",
      webSearchRequested: enableWebSearch,
      webSearchUsed: extracted.webSearchUsed,
      providerLocationContext: userLocation,
      responseId: raw?.id || null,
      status: raw?.status || null,
    }),
    // Retain full provider payload for reprocessing (no auth headers).
    raw,
  };
}

export const openaiProvider = {
  id: "openai",
  vendor: "OpenAI",
  getProviderCapabilities: () =>
    buildProviderCapabilityFlags({
      supportsWebSearch: true,
      supportsInlineCitations: true,
      supportsTopLevelCitationList: false,
      supportsSearchResultMetadata: true,
      supportsUsageTokens: true,
      supportsResponseId: true,
      supportsToolUse: true,
    }),
  getModelIdentity: (args = {}) => ({
    provider: "openai",
    providerVendor: "OpenAI",
    providerModel: nz(args.model) || nz(process.env.AI_VISIBILITY_MODEL) || "gpt-5.6",
  }),
  buildRequest: buildOpenAiVisibilityRequest,
  execute: runVisibilityPrompt,
  normalizeResponse: (raw, ctx = {}) => {
    const extracted = extractTextAndAnnotations(raw);
    return normalizeVisibilityProviderResponse(
      {
        provider: "openai",
        providerVendor: "OpenAI",
        model: ctx.model || raw?.model || null,
        text: extracted.text,
        citations: extracted.citations.map((c, i) => ({
          ...c,
          citationPosition: i + 1,
        })),
        usage: mapUsage(raw),
        citationCapability: extracted.citations.length ? "supported" : "partial",
        parserVersion: PARSER_VERSION,
        providerMeta: { responseId: raw?.id || null },
        raw,
      },
      { ...ctx, useV1_1: true, providerVendor: "OpenAI" }
    );
  },
  extractUsage: mapUsage,
  extractEvidence: (raw) => {
    const extracted = extractTextAndAnnotations(raw);
    return { citations: extracted.citations, searchResults: null };
  },
  classifyError: classifyProviderError,
  estimateCost: (usage, options) => estimateProviderCost("openai", usage, options),
  runVisibilityPrompt,
};
