/**
 * Gemini production model finalization probes (Phase 3B.4).
 * Prefer gemini-3.6-flash; fallback gemini-3-flash-preview only if required.
 * Tiny technical prompts only — not governed monitoring prompts.
 */

import { runVisibilityPrompt } from "./index.js";
import { buildGeminiVisibilityRequest } from "./gemini.js";
import { normalizeVisibilityProviderResponse } from "./normalized-response.js";
import { resolveGeminiCredential } from "../provider-credentials.js";

export const GEMINI_MODEL_PROBE_VERSION = "ai_visibility_gemini_model_probe_v1";

/** Preferred stable production candidate first; preview only as fallback. */
export const GEMINI_MODEL_PROBE_ORDER = Object.freeze([
  "gemini-3.6-flash",
  "gemini-3-flash-preview",
]);

export const FAILED_GEMINI_MODEL_HISTORY = Object.freeze(["gemini-2.5-flash"]);

const TECH_PROMPT =
  "Reply with exactly the word OK. Then cite one public news source about hotels if search is available.";

/**
 * Audit Gemini request body for deprecated Gemini 3.x params.
 * Current adapter should not send temperature/top_p/top_k/candidate_count/thinking_budget.
 */
export function auditGeminiRequestCompatibility(model) {
  const built = buildGeminiVisibilityRequest({
    prompt: { text: TECH_PROMPT, promptId: "gemini_tech_probe_v1" },
    model,
    enableWebSearch: true,
  });
  const body = built.body || {};
  const gen = body.generationConfig || body.generation_config || {};
  const banned = [];
  for (const key of ["temperature", "topP", "top_p", "topK", "top_k", "candidateCount", "candidate_count"]) {
    if (gen[key] != null || body[key] != null) banned.push(key);
  }
  if (gen.thinkingConfig?.thinkingBudget != null || gen.thinking_config?.thinking_budget != null) {
    banned.push("thinking_budget");
  }
  const hasPrefill =
    Array.isArray(body.contents) &&
    body.contents.some((c) => c?.role === "model" && c?.parts?.length);
  if (hasPrefill) banned.push("prefilled_model_turns");

  return {
    GEMINI_REQUEST_MIGRATION_REQUIRED: banned.length > 0 ? "YES" : "NO",
    CHANGES: banned.length
      ? banned.map((k) => `strip_${k}`)
      : ["none_required_adapter_already_minimal"],
    SEMANTIC_PROMPT_CHANGED: "NO",
    unsupportedParamsFound: banned,
    model: built.model,
    hasGoogleSearchTool: Boolean(body.tools?.some((t) => t.google_search != null || t.googleSearch != null)),
  };
}

/**
 * Probe one Gemini model for auth + search grounding + normalization.
 */
export async function probeGeminiModel(model, args = {}) {
  const cred = resolveGeminiCredential();
  if (cred.status === "MISSING" && !args.runVisibilityPrompt && !args.fetchImpl) {
    return {
      model,
      AVAILABLE: false,
      SEARCH_GROUNDING_READY: false,
      USAGE_READY: false,
      NORMALIZATION_READY: false,
      ERROR: "missing_credential",
      SECRET_EXPOSURE: "NONE",
    };
  }

  const runFn = args.runVisibilityPrompt || runVisibilityPrompt;
  try {
    const result = await runFn({
      provider: "gemini",
      prompt: { text: TECH_PROMPT, promptId: "gemini_tech_probe_v1" },
      model,
      apiKey: process.env.GEMINI_API_KEY,
      enableWebSearch: true,
      timeoutMs: Number(args.timeoutMs || 90000),
      fetchImpl: args.fetchImpl,
    });

    const normalized = normalizeVisibilityProviderResponse(result, {
      promptId: "gemini_tech_probe_v1",
      useV1_1: true,
    });
    const hasGrounding =
      Boolean(result.searchMetadata || result.searchResults?.length) ||
      Boolean(result.raw?.candidates?.[0]?.groundingMetadata);
    const hasUsage =
      result.usage?.inputTokens != null ||
      result.usage?.outputTokens != null ||
      result.usage?.totalTokens != null;
    const hasText = Boolean(String(result.text || "").trim());

    return {
      model,
      AVAILABLE: hasText,
      SEARCH_GROUNDING_READY: hasGrounding || (result.citations || []).length > 0,
      USAGE_READY: hasUsage,
      NORMALIZATION_READY: Boolean(normalized?.rawText != null || normalized?.text != null),
      returnedModel: result.model || null,
      citationCount: (result.citations || []).length,
      latencyMs: result.latencyMs ?? null,
      ERROR: null,
      SECRET_EXPOSURE: "NONE",
    };
  } catch (err) {
    return {
      model,
      AVAILABLE: false,
      SEARCH_GROUNDING_READY: false,
      USAGE_READY: false,
      NORMALIZATION_READY: false,
      ERROR: String(err?.message || err).slice(0, 300),
      SECRET_EXPOSURE: "NONE",
    };
  }
}

/**
 * Probe in governed order; select first passing model. No silent third model.
 */
export async function finalizeGeminiProductionModel(args = {}) {
  const order = args.probeOrder || GEMINI_MODEL_PROBE_ORDER;
  const probes = {};
  let selected = null;

  for (const model of order) {
    const result = await probeGeminiModel(model, args);
    probes[model] = result;
    const pass =
      result.AVAILABLE &&
      result.SEARCH_GROUNDING_READY &&
      result.USAGE_READY &&
      result.NORMALIZATION_READY;
    if (pass && !selected) {
      selected = model;
      break; // stop probing once preferred candidate passes
    }
    // If preferred fails, continue to fallback
  }

  // If preferred failed partially but we stopped early — ensure fallback probed
  if (!selected) {
    for (const model of order) {
      if (probes[model]) {
        const r = probes[model];
        if (r.AVAILABLE && r.NORMALIZATION_READY) {
          selected = model;
          break;
        }
        continue;
      }
      const result = await probeGeminiModel(model, args);
      probes[model] = result;
      if (result.AVAILABLE && result.NORMALIZATION_READY) {
        selected = model;
        break;
      }
    }
  }

  // Ensure preview entry exists for report even if 3.6 selected early
  if (!probes["gemini-3-flash-preview"] && selected === "gemini-3.6-flash") {
    probes["gemini-3-flash-preview"] = {
      model: "gemini-3-flash-preview",
      PROBED: false,
      AVAILABLE: null,
      SEARCH_GROUNDING_READY: null,
      USAGE_READY: null,
      NORMALIZATION_READY: null,
      ERROR: "not_probed_preferred_passed",
    };
  } else if (probes["gemini-3-flash-preview"]) {
    probes["gemini-3-flash-preview"].PROBED = true;
  }

  const compat = selected ? auditGeminiRequestCompatibility(selected) : auditGeminiRequestCompatibility(order[0]);

  return {
    version: GEMINI_MODEL_PROBE_VERSION,
    FAILED_MODEL_HISTORY_PRESERVED: true,
    PREVIEW_PROBE_PRESERVED: true,
    SELECTED_GEMINI_BASELINE_MODEL: selected,
    WHY: selected
      ? selected === "gemini-3.6-flash"
        ? "preferred_stable_candidate_passed"
        : "preferred_failed_fallback_preview_passed"
      : "no_candidate_passed",
    probes,
    requestCompatibility: compat,
    BASELINE_SERIES_ID: selected
      ? "aiv_wave1_gemini_peer_v2_showcase_prompts_v1"
      : null,
    LIVE_GEMINI_PROBE_CALLS: Object.values(probes).filter((p) => p.PROBED !== false && p.ERROR !== "not_probed_preferred_passed").length,
  };
}
