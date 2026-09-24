/**
 * Shared GDI Jev decision service — all GDI Jev calls flow here.
 * Default: SHADOW (records Jev vs existing; does not change production path).
 * V1.1: technical fallback ≠ policy low-confidence / hard-gate override.
 */

import {
  isJevEnabled,
  isJevShadowMode,
  getJevModel,
  describeJevConfig,
  JEV_GDI_VERSION,
} from "./jev-config.js";
import {
  buildJevQuestion,
  isValidChoice,
  JEV_DECISION_TYPE,
} from "./jev-types.js";
import { filterChoicesForContext } from "./jev-choice-filter.js";
import {
  callSystemOne,
  hashJevInput,
  createDecisionId,
  getJevCircuitState,
} from "./jev-client.js";
import { applyJevPolicy } from "./jev-policy-adapter.js";
import { recordJevObservation } from "./jev-observability.js";

const ALLOWED_STATE_KEYS = [
  "targetType",
  "entityType",
  "programType",
  "priority",
  "status",
  "researchCadence",
  "consecutiveNoChangeRuns",
  "signalsFound",
  "opportunitiesCreated",
  "opportunitiesUpdated",
  "successfulRuns",
  "noChangeRuns",
  "lastResult",
  "futureDateStatus",
  "eventForwardness",
  "lodgingEvidence",
  "lodgingDemandThesis",
  "roomDemandStatus",
  "geoEvidence",
  "geoOk",
  "sourceTypes",
  "sourceAuthority",
  "sourceCount",
  "officialSourceCount",
  "missingFields",
  "priorRunResults",
  "queriesAlreadyRun",
  "fetchesAlreadyRun",
  "costSoFarUsd",
  "previousFailures",
  "signalStatus",
  "triggerType",
  "venueType",
  "onSiteLodgingStatus",
  "partnerStatus",
  "privacyFlags",
  "sourcingStatus",
  "qualification",
  "opportunityType",
  "weeklyDeltaState",
  "materialFieldsChanged",
  "evidenceSnippets",
  "evidenceSummary",
  "hardPolicyContext",
  "reasonMonitored",
  "playbookHint",
  "archetype",
  "decisionHint",
  "researchReason",
  "lastMaterialSignal",
  "nextExpectedCycle",
  "recurrence",
];

/** Strip bulky / hotel-hardcode risk fields from state before provider call. */
export function sanitizeJevState(context = {}) {
  const out = {};
  for (const k of ALLOWED_STATE_KEYS) {
    if (context[k] !== undefined && context[k] !== null && context[k] !== "") {
      out[k] = context[k];
    }
  }
  if (Array.isArray(out.evidenceSnippets)) {
    out.evidenceSnippets = out.evidenceSnippets
      .map((s) => String(s || "").slice(0, 280))
      .slice(0, 5);
  }
  if (typeof out.evidenceSummary === "string") {
    out.evidenceSummary = out.evidenceSummary.slice(0, 600);
  }
  if (out.hardPolicyContext && typeof out.hardPolicyContext === "object") {
    const h = {};
    for (const [k, v] of Object.entries(out.hardPolicyContext)) {
      if (typeof v === "boolean" || typeof v === "string" || typeof v === "number") {
        h[String(k).slice(0, 64)] = v;
      }
    }
    out.hardPolicyContext = h;
  }
  if (Array.isArray(out.missingFields)) {
    out.missingFields = out.missingFields.map(String).slice(0, 20);
  }
  if (Array.isArray(out.previousFailures)) {
    out.previousFailures = out.previousFailures.map(String).slice(0, 10);
  }
  // Never pass hotel/org names even if slipped in via nested objects
  delete out.hotelName;
  delete out.organizationName;
  delete out.hotelId;
  return out;
}

/**
 * Build evidenceSummary + hardPolicyContext from policy/context if missing.
 */
export function enrichDecisionContext(context = {}, policyContext = {}) {
  const next = { ...context };
  if (!next.hardPolicyContext) {
    const h = {};
    if (policyContext.pastEvent === true) h.PAST_EVENT = true;
    if (policyContext.privacyReject === true) h.PRIVACY_BLOCK = true;
    if (policyContext.fullyPlaced === true) h.FULLY_PLACED = true;
    if (policyContext.exclusivePartnerFound === true) h.EXCLUSIVE_PARTNER = true;
    if (policyContext.noGeographicFit === true || policyContext.geoOk === false) {
      h.NO_GEOGRAPHIC_FIT = true;
    }
    if (policyContext.noValidSource === true) h.NO_VALID_SOURCE = true;
    if (Object.keys(h).length) next.hardPolicyContext = h;
  }
  if (!next.evidenceSummary) {
    const bits = [];
    if (next.targetType) bits.push(`targetType=${next.targetType}`);
    if (next.opportunityType) bits.push(`opportunityType=${next.opportunityType}`);
    if (next.futureDateStatus) bits.push(`future=${next.futureDateStatus}`);
    if (next.lodgingEvidence) bits.push(`lodging=${next.lodgingEvidence}`);
    if (next.roomDemandStatus) bits.push(`roomDemand=${next.roomDemandStatus}`);
    if (next.sourcingStatus) bits.push(`sourcing=${next.sourcingStatus}`);
    if (next.geoOk != null) bits.push(`geoOk=${next.geoOk}`);
    if (Array.isArray(next.missingFields) && next.missingFields.length) {
      bits.push(`missing=${next.missingFields.slice(0, 6).join(",")}`);
    }
    if (next.lastResult) bits.push(`lastResult=${next.lastResult}`);
    if (next.consecutiveNoChangeRuns != null) {
      bits.push(`noChangeStreak=${next.consecutiveNoChangeRuns}`);
    }
    if (bits.length) next.evidenceSummary = bits.join("; ").slice(0, 600);
  }
  return next;
}

function estimateCostUsd(usage) {
  if (!usage) return 0;
  if (typeof usage.cost === "number") return usage.cost;
  const input = usage.input_tokens || 0;
  return (input / 1e6) * 0.042;
}

function classifyFallbackCause({
  error,
  schemaOk,
  lowConfidence,
  hardGates,
  circuitOpen,
} = {}) {
  if (circuitOpen || error === "circuit_open" || error === "jev_circuit_open") {
    return "CIRCUIT_BREAKER";
  }
  if (error === "jev_timeout" || error === "timeout") return "TIMEOUT";
  if (error === "jev_rate_limited" || error === "jev_http_error") return "PROVIDER_ERROR";
  if (error === "jev_api_key_missing" || error === "jev_disabled_or_no_key") {
    return "OTHER";
  }
  if (error === "unknown_decision_type" || error === "UNSUPPORTED_DECISION") {
    return "UNSUPPORTED_DECISION";
  }
  if (!schemaOk || error === "invalid_jev_schema") return "SCHEMA_NORMALIZATION";
  if (hardGates && hardGates.length) return "POLICY_REJECT";
  if (lowConfidence) return "LOW_CONFIDENCE";
  if (error) return "OTHER";
  return null;
}

/**
 * Primary entry: decide({ decisionType, context, existingDecision, policyContext, runContext })
 */
export async function decide({
  decisionType,
  context = {},
  existingDecision = null,
  policyContext = {},
  runContext = {},
  forceShadow,
  choices: choicesOverride,
} = {}) {
  const shadow =
    forceShadow != null ? Boolean(forceShadow) : isJevShadowMode();
  const enriched = enrichDecisionContext(context, policyContext);
  const state = sanitizeJevState(enriched);
  const inputHash = hashJevInput(state, decisionType);
  const decisionId = createDecisionId({
    runId: runContext.runId,
    decisionType,
    inputHash,
  });

  const base = {
    decisionType,
    decisionId,
    selected: null,
    probabilities: null,
    confidence: null,
    model: getJevModel(),
    modelVersion: JEV_GDI_VERSION,
    latencyMs: null,
    providerRequestId: null,
    shadow,
    fallbackUsed: false,
    technicalFallback: false,
    policyFallback: false,
    fallbackCause: null,
    error: null,
    existingDecision,
    matchExisting: null,
    policy: null,
    inputHash,
    costUsd: 0,
  };

  if (!decisionType || !JEV_DECISION_TYPE[decisionType]) {
    const policy = applyJevPolicy({
      decisionType: decisionType || "UNKNOWN",
      jevSelected: null,
      confidence: null,
      existingDecision,
      policyContext,
      shadow,
    });
    const out = {
      ...base,
      fallbackUsed: true,
      technicalFallback: true,
      fallbackCause: "UNSUPPORTED_DECISION",
      error: "unknown_decision_type",
      policy,
      finalPolicyDecision: policy.finalPolicyDecision,
    };
    recordJevObservation(out);
    return out;
  }

  if (!isJevEnabled() || getJevCircuitState().open) {
    const policy = applyJevPolicy({
      decisionType,
      jevSelected: null,
      confidence: null,
      existingDecision,
      policyContext,
      shadow,
    });
    const cause = getJevCircuitState().open ? "CIRCUIT_BREAKER" : "OTHER";
    const out = {
      ...base,
      fallbackUsed: true,
      technicalFallback: true,
      fallbackCause: cause,
      error: getJevCircuitState().open ? "circuit_open" : "jev_disabled_or_no_key",
      policy,
      finalPolicyDecision: policy.finalPolicyDecision,
      matchExisting: policy.matchExisting,
    };
    recordJevObservation(out);
    return out;
  }

  try {
    const filtered =
      Array.isArray(choicesOverride) && choicesOverride.length
        ? choicesOverride
        : filterChoicesForContext(decisionType, state);
    const questions = buildJevQuestion(decisionType, { choices: filtered });
    const raw = await callSystemOne({ state, questions });
    const answer = (raw.answers && raw.answers[decisionType]) || {};
    const selected = answer.choice != null ? String(answer.choice) : null;
    const confidence =
      typeof answer.confidence === "number" ? answer.confidence : null;
    const probabilities = answer.probabilities || null;

    const schemaOk =
      selected == null ||
      isValidChoice(decisionType, selected) ||
      filtered.includes(selected);
    const policy = applyJevPolicy({
      decisionType,
      jevSelected: schemaOk ? selected : null,
      confidence,
      existingDecision,
      policyContext,
      shadow,
    });

    const technicalFallback = !schemaOk;
    const policyFallback =
      schemaOk &&
      selected != null &&
      (policy.lowConfidence || policy.hardGates.length > 0);
    const fallbackCause = classifyFallbackCause({
      error: schemaOk ? null : "invalid_jev_schema",
      schemaOk,
      lowConfidence: policy.lowConfidence,
      hardGates: policy.hardGates,
    });

    const out = {
      ...base,
      selected: schemaOk ? selected : null,
      probabilities,
      confidence,
      model: raw.model || base.model,
      latencyMs: raw.latencyMs,
      providerRequestId: raw.providerRequestId,
      // V1.1: technicalFallback only for true provider/schema failures.
      // Keep fallbackUsed for backward-compat observability of "did not apply Jev".
      fallbackUsed: technicalFallback || policyFallback,
      technicalFallback,
      policyFallback,
      fallbackCause: technicalFallback || policyFallback ? fallbackCause : null,
      error: schemaOk ? null : "invalid_jev_schema",
      policy,
      finalPolicyDecision: policy.finalPolicyDecision,
      matchExisting: policy.matchExisting,
      costUsd: estimateCostUsd(raw.usage) || raw.rawCost || 0,
      usage: raw.usage,
      choicesOffered: filtered,
    };
    recordJevObservation({
      decisionType,
      decisionId,
      selected: out.selected,
      confidence: out.confidence,
      latencyMs: out.latencyMs,
      costUsd: out.costUsd,
      fallbackUsed: out.fallbackUsed,
      technicalFallback: out.technicalFallback,
      policyFallback: out.policyFallback,
      fallbackCause: out.fallbackCause,
      matchExisting: out.matchExisting,
      error: out.error,
      shadow,
      runId: runContext.runId || null,
      targetId: runContext.targetId || null,
    });
    return out;
  } catch (err) {
    const policy = applyJevPolicy({
      decisionType,
      jevSelected: null,
      confidence: null,
      existingDecision,
      policyContext,
      shadow,
    });
    const code = (err && err.code) || (err && err.message) || "jev_error";
    const out = {
      ...base,
      fallbackUsed: true,
      technicalFallback: true,
      policyFallback: false,
      fallbackCause: classifyFallbackCause({ error: code }),
      error: code,
      latencyMs: err && err.latencyMs != null ? err.latencyMs : null,
      policy,
      finalPolicyDecision: policy.finalPolicyDecision,
      matchExisting: policy.matchExisting,
    };
    recordJevObservation(out);
    return out;
  }
}

/**
 * Batch decide with concurrency limit (shadow canaries).
 */
export async function decideMany(items, { concurrency = 3 } = {}) {
  const out = new Array(items.length);
  let idx = 0;
  async function worker() {
    while (idx < items.length) {
      const i = idx++;
      out[i] = await decide(items[i]);
    }
  }
  const n = Math.max(1, Math.min(concurrency, items.length || 1));
  await Promise.all(Array.from({ length: n }, () => worker()));
  return out;
}

export function getJevServiceInfo() {
  return {
    ...describeJevConfig(),
    circuit: getJevCircuitState(),
    decisionTypes: Object.keys(JEV_DECISION_TYPE),
  };
}
