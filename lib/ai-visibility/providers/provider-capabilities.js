/**
 * Provider capability audit matrix (Phase 3B.1).
 * Audited against official API documentation as of 2026-08-14.
 * No live calls.
 */

import { CITATION_CAPABILITY_STATES } from "./provider-interface.js";

export const PROVIDER_CAPABILITY_MATRIX = Object.freeze({
  openai: {
    PROVIDER: "openai",
    CURRENT_ADAPTER: "lib/ai-visibility/providers/openai.js",
    API_STYLE: "Responses API (POST /v1/responses)",
    WEB_SEARCH_CAPABILITY: true,
    SEARCH_GROUNDING_MODEL: "gpt-5.6 (web_search tool)",
    CITATIONS_AVAILABLE: true,
    CITATION_STRUCTURE: "message content annotations type=url_citation (url, title, start_index, end_index)",
    SEARCH_RESULTS_AVAILABLE: "partial (web_search_call.action.sources may exceed inline citations)",
    USAGE_AVAILABLE: true,
    COST_METADATA_AVAILABLE: false,
    RESPONSE_ID_AVAILABLE: true,
    MODEL_ID_AVAILABLE: true,
    LANGUAGE_SUPPORT: "EN/ES and multilingual via model",
    TOOL_USE_MODEL: "web_search",
    KNOWN_LIMITATIONS: [
      "Citation rate PARTIAL — inline url_citation may not cover all search sources",
      "No provider-returned cost metadata",
      "Web search latency can exceed default 60s timeout",
    ],
    CITATION_CAPABILITY_STATES: [
      CITATION_CAPABILITY_STATES.INLINE_CITATION,
      CITATION_CAPABILITY_STATES.SEARCH_RESULT_METADATA,
    ],
  },
  gemini: {
    PROVIDER: "gemini",
    CURRENT_ADAPTER: "lib/ai-visibility/providers/gemini.js",
    API_STYLE: "Generative Language API generateContent (REST)",
    WEB_SEARCH_CAPABILITY: true,
    SEARCH_GROUNDING_MODEL: "gemini-2.5-flash + google_search tool",
    CITATIONS_AVAILABLE: true,
    CITATION_STRUCTURE:
      "groundingMetadata.groundingChunks + groundingSupports (segment indices + chunk URIs)",
    SEARCH_RESULTS_AVAILABLE: true,
    USAGE_AVAILABLE: true,
    COST_METADATA_AVAILABLE: false,
    RESPONSE_ID_AVAILABLE: false,
    MODEL_ID_AVAILABLE: true,
    LANGUAGE_SUPPORT: "EN/ES and multilingual via model",
    TOOL_USE_MODEL: "google_search",
    KNOWN_LIMITATIONS: [
      "Grounding structure differs from OpenAI url_citation",
      "No OpenAI-equivalent response ID",
      "Search queries in groundingMetadata may be absent on some responses",
    ],
    CITATION_CAPABILITY_STATES: [
      CITATION_CAPABILITY_STATES.WEB_GROUNDING_METADATA,
      CITATION_CAPABILITY_STATES.INLINE_CITATION,
      CITATION_CAPABILITY_STATES.SEARCH_RESULT_METADATA,
    ],
  },
  perplexity: {
    PROVIDER: "perplexity",
    CURRENT_ADAPTER: "lib/ai-visibility/providers/perplexity.js",
    API_STYLE: "Sonar Chat Completions (POST /chat/completions)",
    WEB_SEARCH_CAPABILITY: true,
    SEARCH_GROUNDING_MODEL: "sonar (native web-grounded chat)",
    CITATIONS_AVAILABLE: true,
    CITATION_STRUCTURE: "top-level citations[] URL list + search_results[] metadata",
    SEARCH_RESULTS_AVAILABLE: true,
    USAGE_AVAILABLE: true,
    COST_METADATA_AVAILABLE: true,
    RESPONSE_ID_AVAILABLE: true,
    MODEL_ID_AVAILABLE: true,
    LANGUAGE_SUPPORT: "EN/ES and multilingual",
    TOOL_USE_MODEL: "native Sonar search (not Agent API)",
    KNOWN_LIMITATIONS: [
      "No inline start/end citation spans",
      "Agent API rejected for Wave-1 — less representative of consumer recommendation UX",
      "Citation list is URL-level, not span-level",
    ],
    CITATION_CAPABILITY_STATES: [
      CITATION_CAPABILITY_STATES.TOP_LEVEL_CITATION_LIST,
      CITATION_CAPABILITY_STATES.SEARCH_RESULT_METADATA,
    ],
    RECOMMENDED_EXECUTION_MODE: "sonar",
    RECOMMENDED_EXECUTION_MODE_WHY:
      "Sonar chat completions best represent consumer recommendation/search experience with web-grounded answers, top-level citations, search_results metadata, EN/ES support, and stable API — not Agent API orchestration.",
  },
  claude: {
    PROVIDER: "claude",
    CURRENT_ADAPTER: "lib/ai-visibility/providers/claude.js",
    API_STYLE: "Messages API (POST /v1/messages) + server web_search tool",
    WEB_SEARCH_CAPABILITY: true,
    SEARCH_GROUNDING_MODEL: "claude-sonnet-4-6 + web_search_20260209",
    CITATIONS_AVAILABLE: true,
    CITATION_STRUCTURE:
      "web_search_tool_result blocks + citations in text blocks (url, title, cited_text where provided)",
    SEARCH_RESULTS_AVAILABLE: true,
    USAGE_AVAILABLE: true,
    COST_METADATA_AVAILABLE: false,
    RESPONSE_ID_AVAILABLE: true,
    MODEL_ID_AVAILABLE: true,
    LANGUAGE_SUPPORT: "EN/ES and multilingual via model",
    TOOL_USE_MODEL: "web_search_20260209 (direct allowed_callers)",
    KNOWN_LIMITATIONS: [
      "Server tool pause/continuation may require multi-turn handling",
      "Web search bounded by max_uses — no unlimited search loops",
      "Citation shape varies by tool version",
    ],
    CITATION_CAPABILITY_STATES: [
      CITATION_CAPABILITY_STATES.INLINE_CITATION,
      CITATION_CAPABILITY_STATES.SEARCH_RESULT_METADATA,
      CITATION_CAPABILITY_STATES.WEB_GROUNDING_METADATA,
    ],
    WEB_SEARCH_BOUNDED_POLICY: {
      toolType: "web_search_20260209",
      maxUses: 5,
      allowedCallers: ["direct"],
      pauseContinuation: true,
      maxContinuationTurns: 2,
      timeoutMs: 180000,
    },
  },
});

export function getProviderCapabilityAudit(providerId) {
  return PROVIDER_CAPABILITY_MATRIX[String(providerId || "").toLowerCase()] || null;
}
