/**
 * Multi-provider dry-run builder (Phase 3B.1).
 * Builds 252 future requests (84 × 3 providers) without live calls.
 */

import { buildWave1ShowcaseDryRunPlan, buildWave1ExecutionFingerprint } from "../wave1-showcase-plan.js";
import { buildGeminiVisibilityRequest } from "./gemini.js";
import { buildPerplexityVisibilityRequest } from "./perplexity.js";
import { buildClaudeVisibilityRequest } from "./claude.js";
import { buildOpenAiVisibilityRequest } from "./openai.js";
import { WAVE1_PEER_SET_ID } from "../wave1-showcase-plan.js";
import { METRIC_VERSION } from "../config.js";

const NEW_PROVIDERS = ["gemini", "perplexity", "claude"];

const BUILDERS = {
  openai: (args) => buildOpenAiVisibilityRequest({ ...args, model: args.model || "gpt-5.6" }),
  gemini: (args) => buildGeminiVisibilityRequest({ ...args, model: args.model || "gemini-3.6-flash" }),
  perplexity: (args) =>
    buildPerplexityVisibilityRequest({ ...args, model: args.model || "sonar" }),
  claude: (args) =>
    buildClaudeVisibilityRequest({ ...args, model: args.model || "claude-sonnet-4-6" }),
};

const DEFAULT_MODELS = {
  openai: "gpt-5.6",
  gemini: "gemini-3.6-flash",
  perplexity: "sonar",
  claude: "claude-sonnet-4-6",
};

/**
 * @param {object} [options]
 * @param {string[]} [options.providers]
 */
export function buildMultiProviderDryRunReport(options = {}) {
  const plan = buildWave1ShowcaseDryRunPlan(options);
  const providers = options.providers || NEW_PROVIDERS;
  const failures = [];
  const byProvider = {};

  for (const provider of providers) {
    const builder = BUILDERS[provider];
    if (!builder) {
      failures.push({ provider, error: "unknown_provider" });
      continue;
    }

    let buildable = 0;
    const providerFailures = [];

    for (const exec of plan.EXECUTIONS) {
      const built = builder({
        prompt: { text: exec.promptText, promptId: exec.promptId },
        model: DEFAULT_MODELS[provider],
      });

      if (!built.ok) {
        providerFailures.push({
          promptId: exec.promptId,
          errors: built.errors,
        });
        continue;
      }

      if (built.LIVE_PROVIDER_CALL !== false) {
        providerFailures.push({
          promptId: exec.promptId,
          errors: ["live_call_not_disabled"],
        });
        continue;
      }

      buildable += 1;
    }

    byProvider[provider] = {
      BUILDABLE: buildable,
      EXPECTED: 84,
      FAILURES: providerFailures,
      MODEL: DEFAULT_MODELS[provider],
    };

    if (providerFailures.length) {
      failures.push(...providerFailures.map((f) => ({ provider, ...f })));
    }
  }

  const totalBuildable = providers.reduce((sum, p) => sum + (byProvider[p]?.BUILDABLE || 0), 0);
  const expectedTotal = providers.length * 84;

  return {
    planOk: plan.ok,
    providers,
    GEMINI_BUILDABLE: byProvider.gemini?.BUILDABLE ?? 0,
    PERPLEXITY_BUILDABLE: byProvider.perplexity?.BUILDABLE ?? 0,
    CLAUDE_BUILDABLE: byProvider.claude?.BUILDABLE ?? 0,
    TOTAL_BUILDABLE: totalBuildable,
    EXPECTED_TOTAL: expectedTotal,
    FAILURES: failures,
    byProvider,
    LIVE_PROVIDER_CALLS: 0,
  };
}

/**
 * Validate fingerprint provider isolation across all four providers for one prompt.
 */
export function validateFingerprintProviderIsolation(exec) {
  const providers = ["openai", "gemini", "perplexity", "claude"];
  const fingerprints = new Map();

  for (const provider of providers) {
    const fp = buildWave1ExecutionFingerprint({
      provider,
      promptId: exec.promptId,
      promptVersion: exec.version,
      semanticPairId: exec.semanticPairId,
      geographyKey: exec.geographyKey,
      language: exec.language,
      intent: exec.intent,
      promptFamily: exec.promptFamily,
      peerSetId: WAVE1_PEER_SET_ID,
      peerSetVersion: "2",
      metricVersion: METRIC_VERSION,
    });
    if (fingerprints.has(fp.fingerprint)) {
      return {
        valid: false,
        collision: fp.fingerprint,
        providers: [fingerprints.get(fp.fingerprint), provider],
      };
    }
    fingerprints.set(fp.fingerprint, provider);
  }

  return {
    valid: true,
    samples: [...fingerprints.entries()].map(([fingerprint, provider]) => ({
      provider,
      fingerprint,
    })),
  };
}

/**
 * Audit semantic prompt parity — user prompt text must be identical across providers.
 */
export function auditSemanticPromptParity(plan) {
  const differences = [];
  for (const exec of plan.EXECUTIONS) {
    for (const provider of NEW_PROVIDERS) {
      const builder = BUILDERS[provider];
      const built = builder({
        prompt: { text: exec.promptText, promptId: exec.promptId },
      });
      const userText = extractUserPromptText(provider, built);
      if (userText !== exec.promptText.trim()) {
        differences.push({
          promptId: exec.promptId,
          provider,
          expectedLen: exec.promptText.trim().length,
          gotLen: userText.length,
        });
      }
    }
  }
  return {
    SEMANTIC_PROMPT_PARITY: differences.length === 0 ? "YES" : "NO",
    differences: differences.slice(0, 10),
    OPENAI: differences.some((d) => d.provider === "openai") ? "NO" : "YES",
    GEMINI: differences.some((d) => d.provider === "gemini") ? "NO" : "YES",
    PERPLEXITY: differences.some((d) => d.provider === "perplexity") ? "NO" : "YES",
    CLAUDE: differences.some((d) => d.provider === "claude") ? "NO" : "YES",
  };
}

function extractUserPromptText(provider, built) {
  const body = built?.body;
  if (!body) return "";
  if (provider === "openai") return String(body.input || "").trim();
  if (provider === "gemini") {
    return String(body.contents?.[0]?.parts?.[0]?.text || "").trim();
  }
  if (provider === "perplexity") {
    return String(body.messages?.[0]?.content || "").trim();
  }
  if (provider === "claude") {
    return String(body.messages?.[0]?.content || "").trim();
  }
  return "";
}

export function findSampleExecution(plan, filter = {}) {
  return (
    plan.EXECUTIONS.find(
      (e) =>
        (!filter.country || e.geographyKey === filter.country) &&
        (!filter.language || e.language === filter.language) &&
        (!filter.intent || e.intent === filter.intent) &&
        (!filter.promptFamily || e.promptFamily === filter.promptFamily)
    ) || plan.EXECUTIONS[0]
  );
}
