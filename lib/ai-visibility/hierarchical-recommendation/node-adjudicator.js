/**
 * Narrow semantic node adjudicator prompts (Q3–Q6 only).
 * Separate contracts — not one generic 10-way prompt.
 */

import { NODE_DEFINITIONS, NODE_OUTPUTS } from "./tree.js";
import { parseAdjudicatorText } from "../hybrid-recommendation/adjudicator-validate.js";
import { runVisibilityPrompt } from "../providers/index.js";
import { resolveAdjudicatorProvider, estimateAdjudicatorCallCostUsd } from "../hybrid-recommendation/adjudicator-client.js";

export const NODE_ADJUDICATOR_VERSION = "ai_visibility_hierarchical_node_adjudicator_v1";

function buildNodeSystem(nodeId) {
  const def = NODE_DEFINITIONS[nodeId];
  const allowed = NODE_OUTPUTS[nodeId];
  return [
    "You are making ONE narrow semantic decision about how a hotel brand/operator/entity appears in an AI response.",
    "Use only the supplied evidence. Do not invent market preference.",
    "",
    `NODE: ${nodeId} — ${def.title}`,
    `QUESTION: ${def.question}`,
    "",
    "Choose exactly one of:",
    ...allowed.map((a) => `- ${a}`),
    "",
    "Rules:",
    "- Positive language alone is not lead/first.",
    "- Consideration-list numbering alone is not meaningful ranking.",
    "- Meaningful order requires explicit preference/priority semantics.",
    "- Lead requires #1 / first / top / primary (or equivalent).",
    "",
    "Return ONLY JSON:",
    '{"selected":"<ENUM>","evidenceRefs":["entity_local_span","cue_facts"],"rationale":"<short>"}',
    "No markdown. No extra keys. No confidence score.",
  ].join("\n");
}

export function buildNodeUserPayload(nodeId, input) {
  return {
    nodeId,
    entityName: String(input.entityName || ""),
    entityLocalEvidence: String(input.entityLocalEvidence || "").slice(0, 1000),
    sectionHeading: String(input.sectionHeading || "").slice(0, 200),
    cueFacts: input.cueFacts || {},
    structuralEvidence: input.structuralEvidence || {},
    allowed: [...NODE_OUTPUTS[nodeId]],
    note: "Deterministic prediction intentionally omitted.",
  };
}

export function validateNodeAdjudicatorOutput(nodeId, raw) {
  const errors = [];
  if (!raw || typeof raw !== "object") return { ok: false, errors: ["missing_object"] };
  const selected = String(raw.selected || "").trim();
  if (!NODE_OUTPUTS[nodeId].includes(selected)) errors.push(`invalid_enum:${selected}`);
  const refs = Array.isArray(raw.evidenceRefs) ? raw.evidenceRefs.map(String) : [];
  if (!refs.length) errors.push("evidence_refs_required");
  if (!String(raw.rationale || "").trim()) errors.push("rationale_required");
  if (errors.length) return { ok: false, errors, selected: selected || null };
  return { ok: true, selected, evidenceRefs: refs, rationale: String(raw.rationale).trim() };
}

/**
 * @param {{ nodeId: string, entityName: string, entityLocalEvidence: string, cueFacts: object, structuralEvidence: object, sectionHeading?: string, dryRun?: boolean, executeFn?: Function, provider?: string, model?: string }} args
 */
export async function adjudicateHierarchicalNode(args = {}) {
  const { nodeId, dryRun = false, executeFn, provider: providerOverride, model: modelOverride } = args;
  if (!NODE_OUTPUTS[nodeId]) {
    return { ok: false, error: `unknown_node:${nodeId}`, LIVE_PROVIDER_CALL: false };
  }

  const resolved = resolveAdjudicatorProvider();
  const provider = providerOverride || resolved.provider;
  const model = modelOverride || resolved.model;
  const payload = buildNodeUserPayload(nodeId, args);
  const promptText = [buildNodeSystem(nodeId), "", JSON.stringify(payload, null, 2)].join("\n");

  if (dryRun) {
    return {
      ok: true,
      dryRun: true,
      LIVE_PROVIDER_CALL: false,
      nodeId,
      provider,
      model,
      estimatedCostUsd: estimateAdjudicatorCallCostUsd(model),
      payload,
    };
  }

  if (!provider) {
    return { ok: false, LIVE_PROVIDER_CALL: false, code: "NO_PROVIDER_CREDENTIAL", nodeId };
  }

  const run =
    executeFn ||
    ((a) => runVisibilityPrompt({ ...a, provider, enableWebSearch: false }));

  const started = Date.now();
  let providerResult;
  try {
    providerResult = await run({
      provider,
      prompt: { text: promptText, promptId: `hierarchical_node_${nodeId}` },
      model,
      enableWebSearch: false,
      timeoutMs: Number(process.env.AI_VISIBILITY_ADJUDICATOR_TIMEOUT_MS || 45000),
    });
  } catch (err) {
    return {
      ok: false,
      LIVE_PROVIDER_CALL: true,
      nodeId,
      provider,
      error: String(err?.message || err),
      code: "NODE_PROVIDER_ERROR",
      latencyMs: Date.now() - started,
    };
  }

  const text = providerResult?.text || providerResult?.normalized?.text || "";
  const parsed = parseAdjudicatorText(text);
  if (!parsed.ok) {
    return {
      ok: false,
      LIVE_PROVIDER_CALL: true,
      nodeId,
      provider,
      code: "NODE_JSON_PARSE_FAILED",
      errors: [parsed.error],
      rawText: String(text).slice(0, 400),
      latencyMs: Date.now() - started,
      actualCostUsd: estimateAdjudicatorCallCostUsd(model),
    };
  }

  const validated = validateNodeAdjudicatorOutput(nodeId, parsed.value);
  if (!validated.ok) {
    return {
      ok: false,
      LIVE_PROVIDER_CALL: true,
      nodeId,
      provider,
      code: "NODE_VALIDATION_FAILED",
      errors: validated.errors,
      raw: parsed.value,
      latencyMs: Date.now() - started,
      actualCostUsd: estimateAdjudicatorCallCostUsd(model),
    };
  }

  return {
    ok: true,
    LIVE_PROVIDER_CALL: true,
    nodeId,
    provider,
    model,
    selected: validated.selected,
    evidenceRefs: validated.evidenceRefs,
    rationale: validated.rationale,
    latencyMs: Date.now() - started,
    actualCostUsd: estimateAdjudicatorCallCostUsd(model),
    version: NODE_ADJUDICATOR_VERSION,
  };
}
