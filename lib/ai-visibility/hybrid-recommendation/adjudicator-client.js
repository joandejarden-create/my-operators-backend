/**
 * Constrained recommendation adjudicator client.
 * Uses existing AI Visibility provider adapters. One provider per DEV run.
 * No web search. Ambiguous cases only (caller enforces).
 */

import { runVisibilityPrompt } from "../providers/index.js";
import {
  resolveOpenAiCredential,
  resolveClaudeCredential,
  resolveGeminiCredential,
} from "../provider-credentials.js";
import { estimateProviderCost } from "../providers/provider-cost.js";
import {
  ADJUDICATOR_PROMPT_VERSION,
  buildAdjudicatorSystemInstructions,
  buildAdjudicatorUserPayload,
  buildAdjudicatorPromptText,
} from "./adjudicator-prompt.js";
import {
  parseAdjudicatorText,
  validateAdjudicatorOutput,
  ADJUDICATION_VALIDATION_FAILED,
} from "./adjudicator-validate.js";

export const ADJUDICATOR_CLIENT_VERSION =
  "ai_visibility_recommendation_adjudicator_client_v1";

/**
 * Pick a single configured provider for DEV adjudication.
 * Preference: openai → claude → gemini. Provider choice is not client-facing output.
 */
export function resolveAdjudicatorProvider() {
  const forced = String(process.env.AI_VISIBILITY_ADJUDICATOR_PROVIDER || "")
    .trim()
    .toLowerCase();
  if (forced) {
    return { provider: forced, model: defaultModelFor(forced) };
  }
  if (resolveOpenAiCredential().status !== "MISSING") {
    return { provider: "openai", model: defaultModelFor("openai") };
  }
  if (resolveClaudeCredential().status !== "MISSING") {
    return { provider: "claude", model: defaultModelFor("claude") };
  }
  if (resolveGeminiCredential().status !== "MISSING") {
    return { provider: "gemini", model: defaultModelFor("gemini") };
  }
  return { provider: null, model: null, error: "NO_PROVIDER_CREDENTIAL" };
}

function defaultModelFor(provider) {
  if (provider === "openai") {
    return (
      process.env.AI_VISIBILITY_ADJUDICATOR_MODEL ||
      process.env.AI_VISIBILITY_MODEL ||
      "gpt-4.1-mini"
    );
  }
  if (provider === "claude") {
    return (
      process.env.AI_VISIBILITY_ADJUDICATOR_MODEL ||
      process.env.AI_VISIBILITY_CLAUDE_MODEL ||
      "claude-sonnet-4-6"
    );
  }
  if (provider === "gemini") {
    return (
      process.env.AI_VISIBILITY_ADJUDICATOR_MODEL ||
      process.env.AI_VISIBILITY_GEMINI_MODEL ||
      "gemini-2.5-flash"
    );
  }
  return "unknown";
}

/** Rough USD estimate per call for budgeting before execute. */
export function estimateAdjudicatorCallCostUsd(model) {
  const m = String(model || "").toLowerCase();
  if (m.includes("mini") || m.includes("flash") || m.includes("haiku")) return 0.01;
  if (m.includes("sonnet") || m.includes("4.1") || m.includes("gpt-4")) return 0.03;
  return 0.04;
}

export function buildEvidenceRefIds(packageParts = {}) {
  const refs = [];
  if (packageParts.entityLocalEvidence) refs.push("entity_local_span");
  if (packageParts.sectionHeading) refs.push("section_heading");
  if (packageParts.sectionIntro) refs.push("section_intro");
  if (packageParts.structuralEvidence) refs.push("structural_evidence");
  if (packageParts.cueFacts) refs.push("cue_facts");
  return refs;
}

/**
 * @param {object} args
 */
export async function runConstrainedAdjudicator(args = {}) {
  const {
    entityName,
    entityLocalEvidence,
    sectionHeading,
    sectionIntro,
    structuralEvidence,
    cueFacts,
    plausibleRoles,
    ambiguityReasons,
    evidence,
    entityPresent = true,
    dryRun = false,
    fetchImpl,
    executeFn,
    provider: providerOverride,
    model: modelOverride,
  } = args;

  const resolved = resolveAdjudicatorProvider();
  const provider = providerOverride || resolved.provider;
  const model = modelOverride || resolved.model;

  const payload = buildAdjudicatorUserPayload({
    entityName,
    entityLocalEvidence,
    sectionHeading,
    sectionIntro,
    structuralEvidence,
    cueFacts,
    plausibleRoles,
    ambiguityReasons,
  });
  const allowedEvidenceRefIds = buildEvidenceRefIds(payload);
  const instructions = buildAdjudicatorSystemInstructions();
  const promptText = buildAdjudicatorPromptText(payload);

  if (dryRun) {
    return {
      ok: true,
      dryRun: true,
      LIVE_PROVIDER_CALL: false,
      promptVersion: ADJUDICATOR_PROMPT_VERSION,
      provider,
      model,
      estimatedCostUsd: estimateAdjudicatorCallCostUsd(model),
      payload,
      allowedEvidenceRefIds,
    };
  }

  if (!provider) {
    return {
      ok: false,
      LIVE_PROVIDER_CALL: false,
      code: "NO_PROVIDER_CREDENTIAL",
      error: resolved.error || "No adjudicator provider credential configured",
    };
  }

  const run =
    executeFn ||
    ((a) =>
      runVisibilityPrompt({
        ...a,
        provider,
        enableWebSearch: false,
        fetchImpl,
      }));

  const started = Date.now();
  let providerResult;
  try {
    providerResult = await run({
      provider,
      prompt: { text: promptText, promptId: "hybrid_recommendation_adjudicator_v1" },
      model,
      context: { instructions },
      enableWebSearch: false,
      timeoutMs: Number(process.env.AI_VISIBILITY_ADJUDICATOR_TIMEOUT_MS || 45000),
    });
  } catch (err) {
    return {
      ok: false,
      LIVE_PROVIDER_CALL: true,
      provider,
      error: String(err?.message || err),
      code: "ADJUDICATOR_PROVIDER_ERROR",
      latencyMs: Date.now() - started,
      model,
    };
  }

  const text =
    providerResult?.normalized?.text ||
    providerResult?.text ||
    providerResult?.output_text ||
    "";
  const parsed = parseAdjudicatorText(text);
  if (!parsed.ok) {
    return {
      ok: false,
      LIVE_PROVIDER_CALL: true,
      provider,
      code: ADJUDICATION_VALIDATION_FAILED,
      errors: [`json_parse_failed:${parsed.error}`],
      rawText: String(text).slice(0, 500),
      latencyMs: Date.now() - started,
      model,
      usage: providerResult?.usage || null,
    };
  }

  const validated = validateAdjudicatorOutput(parsed.value, {
    evidence,
    entityPresent,
    allowedEvidenceRefIds,
    entityLocalEvidence,
  });

  const usage = providerResult?.usage || null;
  let actualCostUsd = null;
  try {
    const est = estimateProviderCost(usage, { provider, model });
    actualCostUsd =
      typeof est?.usd === "number"
        ? est.usd
        : typeof est?.estimatedUsd === "number"
          ? est.estimatedUsd
          : null;
  } catch {
    actualCostUsd = null;
  }
  if (actualCostUsd == null) actualCostUsd = estimateAdjudicatorCallCostUsd(model);

  if (!validated.ok) {
    return {
      ok: false,
      LIVE_PROVIDER_CALL: true,
      provider,
      code: validated.code,
      errors: validated.errors,
      raw: parsed.value,
      latencyMs: Date.now() - started,
      model,
      usage,
      actualCostUsd,
      promptVersion: ADJUDICATOR_PROMPT_VERSION,
    };
  }

  return {
    ok: true,
    LIVE_PROVIDER_CALL: true,
    provider,
    selectedRole: validated.selectedRole,
    evidenceRefs: validated.evidenceRefs,
    taxonomyRule: validated.taxonomyRule,
    ambiguityResolved: validated.ambiguityResolved,
    latencyMs: Date.now() - started,
    model,
    usage,
    actualCostUsd,
    promptVersion: ADJUDICATOR_PROMPT_VERSION,
    clientVersion: ADJUDICATOR_CLIENT_VERSION,
  };
}
