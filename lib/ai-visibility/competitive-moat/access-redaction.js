/**
 * API-layer access control and customer payload redaction.
 * Server-side enforcement — not CSS or frontend filtering.
 */

import { ACCESS_DEPTH } from "../access-depth.js";
import {
  auditCustomerPayloadForBlockedSignals,
  INTERNAL_ONLY_FIELD_PATTERNS,
} from "./blocked-signals.js";
import {
  CUSTOMER_PAYLOAD_ALLOWLIST,
  INTERNAL_ONLY_FIELDS,
  redactToCustomerAllowlist,
} from "./customer-payload.js";
import { buildInternalBenchmarkPayload } from "./internal-payload.js";

export const ACCESS_REDaction_VERSION = "competitive_moat_access_redaction_v1";

export const PERMISSION_CLASSES = Object.freeze([
  "INTERNAL_ADMIN",
  "CUSTOMER_ENTITY",
  "CUSTOMER_EXECUTIVE",
  "CUSTOMER_ANALYST",
]);

export const CUSTOMER_ACCESS_RULES = Object.freeze({
  FULL_COMPETITOR_MATRIX: "BLOCKED",
  FULL_PROMPT_LIBRARY: "BLOCKED",
  RAW_OBSERVATION_LEDGER: "BLOCKED",
  METHODOLOGY_WEIGHTS: "BLOCKED",
  COHORT_SELECTION_RULES: "SAFE_SUMMARY_ONLY",
  BENCHMARK_SUMMARY: "ALLOWED",
  OWN_ENTITY_DETAIL: "ALLOWED",
  LIMITED_OBSERVED_COMPETITORS: "ALLOWED",
});

/**
 * Resolve whether caller may access internal benchmark diagnostics.
 */
export function resolveBenchmarkAccessClass(viewerContext = {}) {
  if (viewerContext.internalAdmin === true || viewerContext.role === "INTERNAL_ADMIN") {
    return "INTERNAL_ADMIN";
  }
  if (viewerContext.executive === true) return "CUSTOMER_EXECUTIVE";
  if (viewerContext.analyst === true) return "CUSTOMER_ANALYST";
  return "CUSTOMER_ENTITY";
}

/**
 * Build appropriate benchmark response by access class.
 */
export function buildBenchmarkResponseForAccess(opts = {}) {
  const accessClass = opts.accessClass || resolveBenchmarkAccessClass(opts.viewerContext);
  if (accessClass === "INTERNAL_ADMIN") {
    return {
      accessClass,
      payload: buildInternalBenchmarkPayload(opts),
      redacted: false,
    };
  }
  const customerPayload = redactToCustomerAllowlist(opts.customerPayload || {});
  const audit = auditCustomerPayloadForBlockedSignals(customerPayload);
  return {
    accessClass,
    payload: customerPayload,
    redacted: true,
    leakAudit: audit,
  };
}

/**
 * Redact peer rows to benchmark-safe view for comparative access.
 */
export function redactPeerMatrixForCustomer(peerRows = [], subjectId, accessDepth) {
  if (accessDepth === ACCESS_DEPTH.NONE) return [];
  if (accessDepth === ACCESS_DEPTH.DEEP) {
    return peerRows.filter((r) => r.entityId === subjectId);
  }
  return peerRows
    .filter((r) => r.entityId === subjectId)
    .map((r) => ({
      entityId: r.entityId,
      entityName: r.entityName,
      aiPresenceRate: r.aiPresenceRate ?? r.presenceRate ?? null,
      isSubject: true,
    }));
}

/**
 * Audit endpoint serialization for methodology leaks.
 */
export function auditPayloadForMethodologyLeaks(payload = {}, opts = {}) {
  const isInternal = opts.accessClass === "INTERNAL_ADMIN";
  const violations = [];
  const stack = [{ path: "", value: payload }];
  while (stack.length) {
    const { path: p, value } = stack.pop();
    if (value == null || typeof value !== "object") continue;
    if (Array.isArray(value)) {
      value.forEach((v, i) => stack.push({ path: `${p}[${i}]`, value: v }));
      continue;
    }
    for (const [key, val] of Object.entries(value)) {
      const fullPath = p ? `${p}.${key}` : key;
      if (!isInternal && INTERNAL_ONLY_FIELDS.includes(key)) {
        violations.push(`internal_field_on_customer_payload:${fullPath}`);
      }
      if (!isInternal) {
        for (const pattern of INTERNAL_ONLY_FIELD_PATTERNS) {
          if (pattern.test(key)) violations.push(`pattern_leak:${fullPath}`);
        }
      }
      if (val != null && typeof val === "object") stack.push({ path: fullPath, value: val });
    }
  }
  const blocked = auditCustomerPayloadForBlockedSignals(payload);
  const blockedViolations = isInternal
    ? blocked.violations.filter((v) => !v.startsWith("internal_only_field:"))
    : blocked.violations;
  violations.push(...blockedViolations);
  return { ok: violations.length === 0, violations };
}

/**
 * Strip prompt corpus fields from customer-facing responses.
 */
export function redactPromptCorpusFromResponse(response = {}) {
  const blocked = [
    "promptText",
    "promptTextHash",
    "mutationId",
    "mutationRule",
    "promptGenerationRules",
    "fullPromptInventory",
    "hiddenNegativePrompts",
    "weightingMethodology",
  ];
  const out = { ...response };
  for (const key of blocked) delete out[key];
  if (Array.isArray(out.prompts)) {
    out.prompts = out.prompts.map((p) => ({
      scenarioFamily: p.intentTerritory || p.scenarioId || null,
      intentName: p.intentName || null,
      businessMeaning: p.ownerDecision || p.description || null,
    }));
  }
  return out;
}

export { CUSTOMER_PAYLOAD_ALLOWLIST, INTERNAL_ONLY_FIELDS };
