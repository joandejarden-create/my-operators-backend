/**
 * Claude web-search tool finalization audit (Phase 3B.4).
 * Prefer simplest reproducible behavior for recommendation monitoring.
 */

import { CLAUDE_WEB_SEARCH_POLICY, buildClaudeVisibilityRequest } from "./claude.js";
import { CLAUDE_RETRY_POLICY } from "./provider-retry-policy.js";

export const CLAUDE_TOOL_AUDIT_VERSION = "ai_visibility_claude_tool_audit_v1";

/** Known Anthropic web_search tool types (newest first for awareness). */
export const CLAUDE_WEB_SEARCH_TOOL_CANDIDATES = Object.freeze([
  "web_search_20260318",
  "web_search_20260209",
  "web_search_20250305",
]);

/**
 * Deterministic tool selection for Dealality monitoring.
 * Keep web_search_20260209 + allowed_callers:["direct"] — citations/grounding
 * without code-execution dynamic filtering latency/agentic variance.
 */
export function finalizeClaudeWebSearchTool() {
  const current = CLAUDE_WEB_SEARCH_POLICY.toolType;
  const latestCompatible = "web_search_20260318";
  // Do not switch merely because newer exists — 20260318 adds response_inclusion;
  // current methodology does not require it, and changing tool version mid-program
  // would alter execution semantics without governed need.
  const selected = "web_search_20260209";
  const allowedCallers = ["direct"];
  const maxUses = CLAUDE_RETRY_POLICY.webSearchMaxUses;

  const built = buildClaudeVisibilityRequest({
    prompt: { text: "OK", promptId: "claude_tool_audit_v1" },
    enableWebSearch: true,
    maxSearchUses: maxUses,
  });
  const tool = built.body?.tools?.[0] || {};

  return {
    version: CLAUDE_TOOL_AUDIT_VERSION,
    CURRENT_TOOL_VERSION: current,
    LATEST_COMPATIBLE_TOOL_VERSION: latestCompatible,
    SELECTED_TOOL_VERSION: selected,
    SELECTION_REASON:
      "Keep web_search_20260209 with explicit allowed_callers:direct for reproducible web-grounded citations without dynamic-filtering/code-execution agentic variance. Newer 20260318 not required for current methodology.",
    SELECTED_ALLOWED_CALLERS: allowedCallers,
    DYNAMIC_FILTERING_ENABLED: false,
    WHY:
      "Dealality monitoring needs web-grounded answers and citations with simplest reproducible execution — not arbitrary extra agentic filtering.",
    MAX_USES: maxUses,
    CHANGE_REQUIRED: false,
    MAX_USES_WHY: "max_uses=5 remains appropriate for governed owner-decision prompts; do not raise due to prior timeouts.",
    TIMEOUT_MS: Number(process.env.AI_VISIBILITY_PROVIDER_TIMEOUT_MS || CLAUDE_RETRY_POLICY.timeoutMsDefault),
    REQUEST_HAS_ALLOWED_CALLERS: Array.isArray(tool.allowed_callers) && tool.allowed_callers.includes("direct"),
    REQUEST_TOOL_TYPE: tool.type || null,
  };
}
