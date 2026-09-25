/**
 * ADP Jev shadow adapter — low-risk evidence/source/follow-up routing only.
 *
 * Does NOT determine: finding truth, root cause, competitor identity,
 * final action, customer narrative, or persistence authorization.
 * APPLY remains OFF.
 */

import { JEV_DECISION_TYPE, CHOICES } from "../group-demand-intelligence/jev/jev-types.js";
import { decide } from "../group-demand-intelligence/jev/jev-decision-service.js";
import { isJevShadowMode } from "../group-demand-intelligence/jev/jev-config.js";

export const ADP_JEV_SHADOW_VERSION = "adp_jev_shadow_adapter_v1";

/** Allowed ADP shadow decision types only. */
export const ADP_JEV_DECISION_TYPES = Object.freeze({
  ADP_EVIDENCE_UTILITY: "SOURCE_UTILITY",
  ADP_SOURCE_PRIORITY: "SOURCE_UTILITY",
  ADP_FOLLOWUP_RESEARCH_TYPE: "FOLLOWUP_TYPE",
});

export const ADP_JEV_FORBIDDEN = Object.freeze([
  "finding_truth",
  "root_cause_truth",
  "competitor_identity",
  "final_action",
  "customer_narrative",
  "persistence_authorization",
  "canonical_identity",
]);

/**
 * Contract description for replication gates / founder reports.
 */
export function describeAdpJevShadowAdapter() {
  return {
    version: ADP_JEV_SHADOW_VERSION,
    shadowAdapterExists: true,
    productionAdpBehaviorChanged: false,
    apply: false,
    applyReady: false,
    allowed: Object.keys(ADP_JEV_DECISION_TYPES),
    forbidden: [...ADP_JEV_FORBIDDEN],
    note: "Shadow-only reuse of GDI Jev decide(); never mutates ADP snapshot or Airtable overlay.",
  };
}

function mapEvidenceUtilityExisting(rank) {
  const r = String(rank || "").toUpperCase();
  if (r === "PRIMARY" || r === "HIGH") return "PRIMARY_EVIDENCE";
  if (r === "SUPPORTING" || r === "MEDIUM") return "SUPPORTING_EVIDENCE";
  if (r === "LOW") return "LOW_VALUE";
  return "SUPPORTING_EVIDENCE";
}

function mapFollowupExisting(type) {
  const t = String(type || "").toUpperCase();
  if (CHOICES.FOLLOWUP_TYPE.includes(t)) return t;
  return "SOURCE";
}

/**
 * Run ADP evidence/source/follow-up shadow comparisons.
 * @param {object} pack - { evidenceItems?: [], sourceItems?: [], followups?: [] }
 */
export async function runAdpJevShadowEvaluation(pack = {}, opts = {}) {
  const shadow = opts.shadow !== false && isJevShadowMode();
  const evidenceItems = Array.isArray(pack.evidenceItems) ? pack.evidenceItems : [];
  const sourceItems = Array.isArray(pack.sourceItems) ? pack.sourceItems : [];
  const followups = Array.isArray(pack.followups) ? pack.followups : [];
  const limit = opts.limit || 8;

  const calls = [];
  let jevBetter = 0;
  let currentBetter = 0;
  let same = 0;
  let unknown = 0;
  let highConfWrong = 0;
  const started = Date.now();

  async function oneCall({ decisionType, existingDecision, context, choices, label }) {
    try {
      const d = await decide({
        decisionType,
        context: {
          ...context,
          hardPolicyContext: "adp_jev_shadow_only_no_apply",
          productSurface: "ADP",
        },
        existingDecision,
        forceShadow: true,
        choices,
      });
      const selected = d?.selected || null;
      const finalPolicy = d?.finalPolicyDecision || existingDecision;
      const agree = String(finalPolicy) === String(existingDecision);
      if (agree) same += 1;
      else unknown += 1;
      if (d?.technicalFallback) unknown += 1;
      calls.push({
        label,
        decisionType,
        existingDecision,
        jevSelected: selected,
        finalPolicyDecision: finalPolicy,
        shadow: true,
        appliedToAdp: false,
        agreement: agree,
        technicalFallback: Boolean(d?.technicalFallback),
        confidence: d?.confidence ?? null,
      });
    } catch (err) {
      unknown += 1;
      calls.push({
        label,
        decisionType,
        error: String(err?.message || err),
        technicalFallback: true,
        appliedToAdp: false,
      });
    }
  }

  for (const item of evidenceItems.slice(0, limit)) {
    await oneCall({
      label: item.id || item.url || "evidence",
      decisionType: JEV_DECISION_TYPE.SOURCE_UTILITY,
      existingDecision: mapEvidenceUtilityExisting(item.rank || item.utility),
      choices: CHOICES.SOURCE_UTILITY,
      context: {
        sourceAuthority: item.sourceAuthority || "UNKNOWN",
        evidenceSummary: String(item.summary || item.title || "").slice(0, 240),
        missingFields: item.missingFields || [],
        recurrence: "UNKNOWN",
        lodgingEvidence: "UNKNOWN",
      },
    });
  }

  for (const item of sourceItems.slice(0, limit)) {
    await oneCall({
      label: item.id || item.url || "source",
      decisionType: JEV_DECISION_TYPE.SOURCE_UTILITY,
      existingDecision: mapEvidenceUtilityExisting(item.priority || item.rank),
      choices: CHOICES.SOURCE_UTILITY,
      context: {
        sourceAuthority: item.sourceAuthority || "UNKNOWN",
        evidenceSummary: String(item.title || item.url || "").slice(0, 240),
        missingFields: [],
        recurrence: "UNKNOWN",
        lodgingEvidence: "UNKNOWN",
      },
    });
  }

  for (const item of followups.slice(0, limit)) {
    await oneCall({
      label: item.id || item.type || "followup",
      decisionType: JEV_DECISION_TYPE.FOLLOWUP_TYPE,
      existingDecision: mapFollowupExisting(item.type),
      choices: CHOICES.FOLLOWUP_TYPE,
      context: {
        sourceAuthority: "UNKNOWN",
        evidenceSummary: String(item.reason || item.gap || "").slice(0, 240),
        missingFields: item.missingFields || ["evidence"],
        recurrence: "UNKNOWN",
        lodgingEvidence: "UNKNOWN",
      },
    });
  }

  return {
    ok: true,
    version: ADP_JEV_SHADOW_VERSION,
    shadow,
    appliedToAdp: false,
    apply: false,
    applyReady: false,
    productionAdpBehaviorChanged: false,
    calls: calls.length,
    latencyMs: Date.now() - started,
    jevBetter,
    currentBetter,
    same,
    unknown,
    highConfWrong,
    forbiddenEnforced: ADP_JEV_FORBIDDEN,
    sample: calls.slice(0, 6),
    note: "ADP Jev shadow ranks evidence/source/follow-up only; findings untouched.",
  };
}
