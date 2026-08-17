/**
 * Minimal live billing/execution probe — no secret exposure (Phase 3B.3).
 */

import { resolveClaudeCredential } from "./provider-credentials.js";
import { runVisibilityPrompt } from "./providers/index.js";
import { classifyProviderError } from "./providers/provider-errors.js";

const PROBE_PROMPT = "Reply with exactly: OK";

/**
 * One minimal Claude API call to distinguish billing vs adapter readiness.
 * @param {object} [args]
 */
export async function probeClaudeBillingExecution(args = {}) {
  const cred = resolveClaudeCredential();
  if (cred.status === "MISSING") {
    return {
      CLAUDE_CREDENTIAL_READY: "NO",
      CLAUDE_BILLING_EXECUTION_READY: "NO",
      EXECUTION_READY: "NO",
      reason: "missing_credential",
      envVarExpected: "ANTHROPIC_API_KEY",
      SECRET_EXPOSURE: "NONE",
    };
  }

  const runFn = args.runVisibilityPrompt || runVisibilityPrompt;
  try {
    const result = await runFn({
      provider: "claude",
      prompt: { text: PROBE_PROMPT, promptId: "claude_billing_probe_v1" },
      model: args.model || process.env.AI_VISIBILITY_CLAUDE_MODEL || "claude-sonnet-4-6",
      apiKey: process.env.ANTHROPIC_API_KEY,
      enableWebSearch: false,
      timeoutMs: Number(args.timeoutMs || 45000),
      fetchImpl: args.fetchImpl,
    });

    return {
      CLAUDE_CREDENTIAL_READY: "YES",
      CLAUDE_BILLING_EXECUTION_READY: "YES",
      EXECUTION_READY: "YES",
      modelReturned: result?.model || null,
      latencyMs: result?.latencyMs ?? null,
      SECRET_EXPOSURE: "NONE",
    };
  } catch (err) {
    const classified = classifyProviderError(err);
    const billingLike = /credit balance|billing|payment|insufficient.*credit|quota exceeded/i.test(
      classified.message
    );

    if (billingLike || (classified.category === "AUTH" && /credit|billing|balance/i.test(classified.message))) {
      return {
        CLAUDE_CREDENTIAL_READY: "YES",
        CLAUDE_BILLING_EXECUTION_READY: "NO",
        EXECUTION_READY: "NO",
        reason: "insufficient_billing_or_auth",
        category: classified.category,
        SECRET_EXPOSURE: "NONE",
      };
    }

    if (classified.category === "AUTH") {
      return {
        CLAUDE_CREDENTIAL_READY: "YES",
        CLAUDE_BILLING_EXECUTION_READY: "NO",
        EXECUTION_READY: "NO",
        reason: "auth_error",
        category: classified.category,
        SECRET_EXPOSURE: "NONE",
      };
    }

    return {
      CLAUDE_CREDENTIAL_READY: "YES",
      CLAUDE_BILLING_EXECUTION_READY: "UNKNOWN",
      EXECUTION_READY: "UNKNOWN",
      reason: classified.category,
      category: classified.category,
      SECRET_EXPOSURE: "NONE",
    };
  }
}
