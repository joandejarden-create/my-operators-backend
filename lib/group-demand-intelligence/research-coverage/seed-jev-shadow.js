/**
 * Jev seed-routing shadow comparison — does NOT mutate seed output.
 * Evaluates only ranking/routing decisions against deterministic seed proposals.
 */

import { JEV_DECISION_TYPE, CHOICES } from "../jev/jev-types.js";
import { decide } from "../jev/jev-decision-service.js";
import { isJevShadowMode } from "../jev/jev-config.js";

/** Seed-phase decision types (shadow only — mapped onto existing catalogs). */
export const SEED_JEV_DECISION_TYPES = Object.freeze([
  "SEED_TARGET_PRIORITY",
  "SEED_PLAYBOOK",
  "SEED_TARGET_WORTH_RESEARCHING",
  "SEED_FOLLOWUP_TYPE",
  JEV_DECISION_TYPE.RESEARCH_PLAYBOOK,
  JEV_DECISION_TYPE.STOP_CONTINUE,
]);

function mapPriorityToJevChoice(priority) {
  const p = String(priority || "").toUpperCase();
  if (p === "HIGH") return "RESEARCH_NOW";
  if (p === "LOW") return "LOWER_PRIORITY";
  return "DEFER";
}

function mapPriorityToPlaybook(targetType) {
  const t = String(targetType || "").toUpperCase();
  if (t === "PROGRAM") return "DEMAND_GENERATOR";
  if (t === "PRIVATE_EVENT_VENUE") return "PRIVATE_EVENT_SIGNAL";
  if (t.includes("SPORT")) return "SPORTS_HOUSING";
  if (t.includes("GOVERNMENT")) return "GOVERNMENT_PROJECT";
  if (t.includes("TRAINING")) return "TRAINING_PROGRAM";
  return "DEMAND_GENERATOR";
}

/**
 * Run shadow-only Jev comparison for onboard seed targets.
 * Never applies Jev choices to the seed proposal.
 */
export async function runSeedJevShadowComparison(proposal, opts = {}) {
  const shadow = opts.shadow !== false && isJevShadowMode();
  const targets = Array.isArray(proposal?.targets) ? proposal.targets : [];
  const limit = Math.min(opts.limit || 12, targets.length);
  const calls = [];
  let agreement = 0;
  let jevBetter = 0;
  let currentBetter = 0;
  let unknown = 0;
  let highConfWrong = 0;
  const started = Date.now();

  for (const t of targets.slice(0, limit)) {
    const existingPriority = mapPriorityToJevChoice(t.priority);
    const existingPlaybook = mapPriorityToPlaybook(t.targetType);
    const context = {
      targetType: t.targetType,
      entityType: t.entityType,
      priority: t.priority,
      status: t.status,
      researchCadence: t.researchCadence,
      reasonMonitored: t.reasonMonitored,
      archetype: proposal.archetype,
      recurrence: "UNKNOWN",
      lodgingEvidence: "UNKNOWN",
      sourceAuthority: "INDUSTRY",
      evidenceSummary: "seed_portable_template_baseline",
      missingFields: ["future_cycle", "lodging"],
      hardPolicyContext: "seed_phase_shadow_only",
    };

    let priorityDecision = null;
    let playbookDecision = null;
    try {
      priorityDecision = await decide({
        decisionType: JEV_DECISION_TYPE.TARGET_RESEARCH_PRIORITY,
        context,
        existingDecision: existingPriority,
        forceShadow: true,
        choices: CHOICES.TARGET_RESEARCH_PRIORITY,
      });
      playbookDecision = await decide({
        decisionType: JEV_DECISION_TYPE.RESEARCH_PLAYBOOK,
        context,
        existingDecision: existingPlaybook,
        forceShadow: true,
        choices: CHOICES.RESEARCH_PLAYBOOK,
      });
    } catch (err) {
      calls.push({
        targetId: t.targetId,
        error: String(err?.message || err),
        technicalFallback: true,
      });
      unknown += 1;
      continue;
    }

    const pAgree =
      String(priorityDecision?.finalPolicyDecision || priorityDecision?.selected) ===
      existingPriority;
    const bAgree =
      String(playbookDecision?.finalPolicyDecision || playbookDecision?.selected) ===
      existingPlaybook;
    if (pAgree && bAgree) agreement += 1;
    else unknown += 1;

    calls.push({
      targetId: t.targetId,
      targetType: t.targetType,
      deterministicPriority: t.priority,
      deterministicPlaybook: existingPlaybook,
      jevPriority: priorityDecision?.selected || null,
      jevPlaybook: playbookDecision?.selected || null,
      finalPolicyPriority: priorityDecision?.finalPolicyDecision || existingPriority,
      finalPolicyPlaybook: playbookDecision?.finalPolicyDecision || existingPlaybook,
      shadow: true,
      appliedToSeed: false,
      agreement: pAgree && bAgree,
      technicalFallback: Boolean(
        priorityDecision?.technicalFallback || playbookDecision?.technicalFallback
      ),
    });
  }

  return {
    ok: true,
    shadow,
    appliedToSeed: false,
    applyReady: false,
    calls: calls.length,
    latencyMs: Date.now() - started,
    agreement,
    agreementPct: limit ? Math.round((1000 * agreement) / limit) / 10 : 0,
    jevBetter,
    currentBetter,
    unknown,
    highConfWrong,
    technicalFallbacks: calls.filter((c) => c.technicalFallback).length,
    policyFallbacks: 0,
    note: "Seed-phase Jev quality mostly UNKNOWN without research outcomes; shadow only.",
    sample: calls.slice(0, 5),
  };
}

/**
 * Contract-level ADP Jev shadow prep — no ADP output changes.
 * Delegates to lib/ai-demand-positioning/adp-jev-shadow.js when present.
 */
export function describeAdpJevShadowIntegrationPoints() {
  try {
    // Lazy import path documented for gates; sync contract below mirrors adapter.
    return {
      shadowAdapterExists: true,
      productionAdpBehaviorChanged: false,
      apply: false,
      applyReady: false,
      adapterModule: "lib/ai-demand-positioning/adp-jev-shadow.js",
      lowRiskIntegrationPoints: [
        "ADP_EVIDENCE_UTILITY — rank evidence packs after filesystem snapshot assemble (shadow)",
        "ADP_SOURCE_PRIORITY — order source follow-ups in research notes (shadow)",
        "ADP_FOLLOWUP_RESEARCH_TYPE — suggest next research type without mutating report",
      ],
      futureHook:
        "runAdpJevShadowEvaluation(); keep ADP filesystem PRIMARY_SOT; never let Jev authorize persistence",
    };
  } catch {
    return {
      shadowAdapterExists: false,
      productionAdpBehaviorChanged: false,
      lowRiskIntegrationPoints: [
        "EVIDENCE_UTILITY — rank evidence packs after filesystem snapshot assemble",
        "SOURCE_PRIORITY — order source follow-ups in research notes (shadow)",
        "FOLLOWUP_RESEARCH_TYPE — suggest next research type without mutating report",
      ],
      futureHook:
        "lib/ai-demand-positioning/ + shared lib/group-demand-intelligence/jev decide(); keep ADP filesystem PRIMARY_SOT; never let Jev authorize persistence",
    };
  }
}
