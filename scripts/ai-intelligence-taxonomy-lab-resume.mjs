#!/usr/bin/env node
/**
 * Classifier lab CONTINUE after taxonomy resolution.
 * Up to 10 additional DEV cycles. Holdout never accessed.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { spawnSync } from "child_process";
import {
  acceptanceDecision,
  diffErrors,
  ROLES,
} from "../lib/ai-visibility/classifier-lab/score-dev.js";
import {
  CANDIDATES,
  snapshotLabTargets,
  restoreLabTargets,
} from "../lib/ai-visibility/classifier-lab/candidates.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const LEDGER_PATH = path.join(ROOT, "data/ai-visibility/validation/classifier-lab-ledger.json");
const ERROR_LEDGER_PATH = path.join(ROOT, "data/ai-visibility/validation/classifier-error-ledger.json");
const REPORT_PATH = path.join(
  ROOT,
  "data/ai-visibility/validation/taxonomy-resolution-lab-resume-report.json"
);
const GT_BASELINE_PATH = path.join(
  ROOT,
  "data/ai-visibility/validation/taxonomy-resolution-gt-effect-baseline.json"
);
const SCORE_ONCE = path.join(ROOT, "scripts/_classifier-lab-score-once.mjs");

const GOVERNANCE = new Set([
  "FIRST_VS_ASSOCIATED_CONSIDERATION_LIST_TAXONOMY",
  "RANKED_VS_ASSOCIATED_CONSIDERATION_LIST_TAXONOMY",
  "FIRST_LABELED_BUT_ONLY_EXPLICIT_POSITIVE_CUE",
  "FIRST_UNDERPROMOTED_TO_ASSOCIATED",
]);

const GATES = {
  ACCURACY: 0.98,
  FIRST_REC: 0.98,
  ENTITY: 0.98,
};

function score(tag) {
  const r = spawnSync(process.execPath, [SCORE_ONCE, tag], {
    cwd: ROOT,
    encoding: "utf8",
    maxBuffer: 64 * 1024 * 1024,
  });
  if (r.status !== 0) throw new Error(r.stderr || r.stdout || "score_failed");
  return JSON.parse(r.stdout);
}

function nowIso() {
  return new Date().toISOString();
}

function hybridAnalysis(errors) {
  const ambiguousRoots = new Set([
    "COMPARATOR_BOUNDARY",
    "FIRST_UNDERPROMOTED_TO_ASSOCIATED",
    "FIRST_UNDERPROMOTED_TO_EXPLICIT",
    "MISSING_FIRST_RECALL",
    "MISSING_RANKED_RECALL",
    "FALSE_EXPLICIT_OVER_PROMOTION",
    "PAIR_discussed_TO_ranked_recommendation",
    "PAIR_first_recommendation_TO_passing_mention",
  ]);
  const clearRoots = new Set([
    "MISSING_ASSOCIATED_FROM_DECISION_SET_MEMBERSHIP",
    "MISSING_ASSOCIATED_RECALL",
    "FALSE_ASSOCIATED_FROM_NEUTRAL_OR_PROFILE_CATALOG",
    "FALSE_ASSOCIATED_OVER_PROMOTION",
    "FALSE_ASSOCIATED_FROM_BROAD_CONSIDERATION_PROPAGATION",
    "MISSING_EXPLICIT_POSITIVE_OR_SECTION",
    "MISSING_FIRST_FROM_LEAD_CUE",
    "MISSING_FIRST_FROM_CONFIRMED_RANK_OR_SHORTLIST",
    "MISSING_RANKED_FROM_CONFIRMED_STRUCTURE",
  ]);
  let DETERMINISTIC_CLEAR_CASES = 0;
  let AMBIGUOUS_CASES = 0;
  let POTENTIAL_ADJUDICATOR_CASES = 0;
  for (const e of errors) {
    if (GOVERNANCE.has(e.rootCause) || ambiguousRoots.has(e.rootCause)) {
      AMBIGUOUS_CASES++;
      POTENTIAL_ADJUDICATOR_CASES++;
    } else if (clearRoots.has(e.rootCause)) {
      DETERMINISTIC_CLEAR_CASES++;
    } else {
      AMBIGUOUS_CASES++;
      POTENTIAL_ADJUDICATOR_CASES++;
    }
  }
  return {
    DETERMINISTIC_CLEAR_CASES,
    AMBIGUOUS_CASES,
    POTENTIAL_ADJUDICATOR_CASES,
    architecture:
      "deterministic evidence extraction → deterministic role when evidence decisive → constrained adjudicator only when evidence state is ambiguous → adjudicator chooses ONLY from governed taxonomy → full evidence trace → deterministic validation of returned enum",
    HYBRID_RECOMMENDED_FOR_NEXT_PHASE:
      POTENTIAL_ADJUDICATOR_CASES >= 20 ||
      (errors.length > 0 && POTENTIAL_ADJUDICATOR_CASES / errors.length >= 0.35),
  };
}

const ledger = JSON.parse(fs.readFileSync(LEDGER_PATH, "utf8"));
const gtBaseline = JSON.parse(fs.readFileSync(GT_BASELINE_PATH, "utf8"));
const experimentsBefore = (ledger.experiments || []).length;
const acceptedBefore = (ledger.experiments || []).filter((e) => e.accepted).length;
const already = new Set((ledger.experiments || []).map((e) => e.experimentId));
const accepted = new Set(
  (ledger.experiments || []).filter((e) => e.accepted).map((e) => e.experimentId)
);

const priorLabFinal = ledger.final?.METRICS_AFTER || ledger.final?.FINAL || null;

let current = score("lab_resume_gt_baseline");
const runBaseline = {
  metrics: current.metrics,
  classMetrics: current.classMetrics,
  errorCount: current.errorCount,
  clusters: current.clusters,
  pairs: current.pairs,
  errors: current.errors,
};

console.log("RESUME baseline (post taxonomy GT)", {
  acc: current.metrics.ACCURACY,
  errors: current.errorCount,
  first: current.metrics.FIRST_REC,
});

let cyclesAdded = 0;
const maxAdd = Number(process.env.CLASSIFIER_LAB_MAX_ADD || 10);
const exhausted = { ...(ledger.exhaustedClusters || {}) };
let stopReason = null;
const additionalExperiments = [];

while (cyclesAdded < maxAdd) {
  cyclesAdded++;
  const cycle = (ledger.cycles?.length || 0) + 1;
  console.log(`\nRESUME cycle ${cycle} (+${cyclesAdded}/${maxAdd})`);

  const ranked = current.clusters.filter(
    (c) => !GOVERNANCE.has(c.rootCause) && exhausted[c.rootCause]?.status !== "DETERMINISTIC_LIMIT_REACHED"
  );
  if (!ranked.length) {
    stopReason = "no_safe_actionable_clusters";
    console.log("No actionable clusters");
    break;
  }
  const target = ranked[0];
  console.log(" target", target.rootCause, target.count);

  let cands = CANDIDATES.filter(
    (c) =>
      !already.has(c.id) &&
      (c.errorCluster === target.rootCause ||
        (target.rootCause.includes("ASSOCIATED") && c.errorCluster.includes("ASSOCIATED")) ||
        (target.rootCause.includes("FIRST") && c.errorCluster.includes("FIRST")) ||
        (target.rootCause.includes("RANKED") && c.errorCluster.includes("RANK")) ||
        (target.rootCause.includes("EXPLICIT") && c.errorCluster.includes("EXPLICIT")) ||
        (target.rootCause.includes("PROFILE") && c.errorCluster.includes("PROFILE")) ||
        (target.rootCause.includes("COMPARATOR") && c.errorCluster.includes("COMPARATOR")))
  );
  if (!cands.length) cands = CANDIDATES.filter((c) => !already.has(c.id));
  if (!cands.length) {
    exhausted[target.rootCause] = {
      status: "DETERMINISTIC_LIMIT_REACHED",
      reason: "no candidates left for cluster",
      count: target.count,
    };
    continue;
  }

  const snap = snapshotLabTargets();
  const passes = [];
  const tried = [];

  for (const cand of cands.slice(0, 4)) {
    console.log("  try", cand.id);
    restoreLabTargets(snap);
    tried.push(cand.id);
    already.add(cand.id);
    try {
      cand.apply();
    } catch (e) {
      restoreLabTargets(snap);
      const rec = {
        experimentId: cand.id,
        cycle,
        hypothesis: cand.hypothesis,
        errorCluster: cand.errorCluster,
        accepted: false,
        rejectionReason: String(e.message || e),
        timestamp: nowIso(),
        phase: "taxonomy_resolution_lab_resume",
      };
      ledger.experiments.push(rec);
      additionalExperiments.push(rec);
      console.log("  REJECT patch", e.message);
      continue;
    }
    const after = score(`lab_${cand.id}_c${cycle}`);
    const decision = acceptanceDecision(current, after);
    const diff = diffErrors(current.errors, after.errors);
    const rec = {
      experimentId: cand.id,
      cycle,
      classifierVersion: `lab_${cand.id}_c${cycle}`,
      hypothesis: cand.hypothesis,
      errorCluster: cand.errorCluster,
      codeChange: `apply ${cand.id}`,
      affectedCases: [...diff.fixedErrors, ...diff.newErrors].slice(0, 40),
      beforeMetrics: current.metrics,
      afterMetrics: after.metrics,
      classMetricsBefore: current.classMetrics,
      classMetricsAfter: after.classMetrics,
      fixedErrors: diff.fixedErrors,
      newErrors: diff.newErrors,
      accepted: decision.accepted,
      rejectionReason: decision.rejectionReason,
      timestamp: nowIso(),
      phase: "taxonomy_resolution_lab_resume",
    };
    ledger.experiments.push(rec);
    additionalExperiments.push(rec);
    restoreLabTargets(snap);
    if (decision.accepted) {
      console.log(
        "  PASS",
        cand.id,
        current.metrics.ACCURACY.toFixed(4),
        "→",
        after.metrics.ACCURACY.toFixed(4)
      );
      passes.push({
        cand,
        after,
        delta: after.metrics.ACCURACY - current.metrics.ACCURACY,
      });
    } else {
      console.log("  REJECT", cand.id, decision.rejectionReason);
    }
  }

  passes.sort(
    (a, b) =>
      b.delta - a.delta || (b.after.metrics.MACRO_F1 ?? 0) - (a.after.metrics.MACRO_F1 ?? 0)
  );
  for (const p of passes.slice(1)) {
    const exp = ledger.experiments.filter((e) => e.experimentId === p.cand.id).slice(-1)[0];
    if (exp) {
      exp.accepted = false;
      exp.rejectionReason = (exp.rejectionReason || "") + "|not_selected_as_cycle_winner";
    }
  }

  const best = passes[0];
  if (best) {
    restoreLabTargets(snap);
    best.cand.apply();
    accepted.add(best.cand.id);
    current = best.after;
    console.log("  KEEP", best.cand.id);
  } else {
    restoreLabTargets(snap);
    const priorFails = (exhausted[target.rootCause]?.failStreak || 0) + 1;
    exhausted[target.rootCause] = {
      status:
        priorFails >= 2
          ? "DETERMINISTIC_LIMIT_REACHED"
          : GOVERNANCE.has(target.rootCause)
            ? "HUMAN_GOVERNANCE_REQUIRED"
            : "IRREDUCIBLE_AMBIGUITY",
      reason: "all candidates rejected",
      count: target.count,
      tried,
      failStreak: priorFails,
    };
  }

  ledger.cycles.push({
    cycle,
    targetCluster: target.rootCause,
    experiments: tried,
    accepted: best?.cand?.id || null,
    metrics: current.metrics,
    errorCount: current.errorCount,
    timestamp: nowIso(),
    phase: "taxonomy_resolution_lab_resume",
  });
  ledger.exhaustedClusters = { ...exhausted };
  fs.writeFileSync(LEDGER_PATH, JSON.stringify(ledger, null, 2));

  if (
    current.metrics.ACCURACY >= GATES.ACCURACY &&
    current.metrics.FIRST_REC >= GATES.FIRST_REC &&
    current.metrics.ENTITY_P >= GATES.ENTITY
  ) {
    stopReason = "dev_gates_pass";
    break;
  }

  const open = current.clusters.filter(
    (c) =>
      !GOVERNANCE.has(c.rootCause) &&
      exhausted[c.rootCause]?.status !== "DETERMINISTIC_LIMIT_REACHED" &&
      exhausted[c.rootCause]?.status !== "IRREDUCIBLE_AMBIGUITY"
  );
  if (!open.length && !best) {
    stopReason = "no_safe_generalized_improvement";
    break;
  }
}

current = score("lab_resume_final");
const hybrid = hybridAnalysis(current.errors);
const detLimit =
  Object.values(exhausted).some((v) => v?.status === "DETERMINISTIC_LIMIT_REACHED") ||
  (additionalExperiments.filter((e) => e.accepted).length === 0 &&
    additionalExperiments.length > 0);

const gtReview = current.errors
  .filter((e) => GOVERNANCE.has(e.rootCause))
  .slice(0, 40)
  .map((e) => ({
    caseId: e.caseId,
    entity: e.entity,
    humanRole: e.humanRole,
    predictedRole: e.predictedRole,
    rootCause: e.rootCause,
  }));

const acceptedExps = (ledger.experiments || []).filter((e) => e.accepted);
const rejectedExps = (ledger.experiments || []).filter((e) => !e.accepted);
const addAccepted = additionalExperiments.filter((e) => e.accepted);
const addRejected = additionalExperiments.filter((e) => !e.accepted);

const gates = {
  ENTITY_GATE:
    current.metrics.ENTITY_P >= 0.98 && current.metrics.ENTITY_R >= 0.98 ? "PASS" : "FAIL",
  RECOMMENDATION_GATE: current.metrics.ACCURACY >= 0.98 ? "PASS" : "FAIL",
  FIRST_REC_GATE: current.metrics.FIRST_REC >= 0.98 ? "PASS" : "FAIL",
  CLASS_BALANCE_STATUS:
    current.metrics.MACRO_F1 != null && current.metrics.MACRO_F1 >= 0.85 ? "OK" : "WEAK",
};

let NEXT_STEP = "MORE_DETERMINISTIC_DEVELOPMENT_JUSTIFIED";
if (gates.RECOMMENDATION_GATE === "PASS" && gates.FIRST_REC_GATE === "PASS") {
  NEXT_STEP = "READY_FOR_FINAL_UNTOUCHED_HOLDOUT_EVALUATION";
} else if (gtReview.length >= 5) {
  NEXT_STEP = "HUMAN_GROUND_TRUTH_DECISION_REQUIRED";
} else if (hybrid.HYBRID_RECOMMENDED_FOR_NEXT_PHASE && detLimit) {
  NEXT_STEP = "HYBRID_CLASSIFIER_DESIGN_RECOMMENDED";
} else if (detLimit) {
  NEXT_STEP = "HYBRID_CLASSIFIER_DESIGN_RECOMMENDED";
}

const status =
  gates.ENTITY_GATE === "PASS" &&
  (NEXT_STEP === "READY_FOR_FINAL_UNTOUCHED_HOLDOUT_EVALUATION" ||
    NEXT_STEP === "HYBRID_CLASSIFIER_DESIGN_RECOMMENDED" ||
    NEXT_STEP === "MORE_DETERMINISTIC_DEVELOPMENT_JUSTIFIED" ||
    NEXT_STEP === "HUMAN_GROUND_TRUTH_DECISION_REQUIRED")
    ? gates.RECOMMENDATION_GATE === "PASS" && gates.FIRST_REC_GATE === "PASS"
      ? "AI_INTELLIGENCE_TAXONOMY_RESOLUTION_AND_LAB_RESUME_PASS"
      : "AI_INTELLIGENCE_TAXONOMY_RESOLUTION_AND_LAB_RESUME_REVIEW_REQUIRED"
    : "AI_INTELLIGENCE_TAXONOMY_RESOLUTION_AND_LAB_RESUME_BLOCKED";

const report = {
  phase: "AI_INTELLIGENCE_TAXONOMY_RESOLUTION_AND_LAB_RESUME_COMPLETE",
  status,
  stopReason,
  PRIOR_LAB_FINAL: priorLabFinal,
  GROUND_TRUTH_EFFECT_ONLY: {
    metrics: gtBaseline.metrics,
    errorCount: gtBaseline.errorCount,
    classMetrics: gtBaseline.classMetrics,
  },
  RESUME_BASELINE: runBaseline.metrics,
  FINAL: current.metrics,
  CLASS_METRICS: current.classMetrics,
  confusionPairs: current.pairs,
  ADDITIONAL_CYCLES: cyclesAdded,
  EXPERIMENTS_TESTED_ADDITIONAL: additionalExperiments.length,
  ACCEPTED_ADDITIONAL: addAccepted.length,
  REJECTED_ADDITIONAL: addRejected.length,
  EXPERIMENTS_TESTED_TOTAL: ledger.experiments.length,
  ACCEPTED_TOTAL: acceptedExps.length,
  REJECTED_TOTAL: rejectedExps.length,
  ERRORS_BEFORE_LAB_RESUME: runBaseline.errorCount,
  ERRORS_AFTER_LAB_RESUME: current.errorCount,
  ERRORS_BEFORE_TAXONOMY_GT: 108,
  RESOLVED_CLUSTERS: addAccepted.map((e) => ({
    experimentId: e.experimentId,
    errorCluster: e.errorCluster,
    fixed: e.fixedErrors?.length ?? null,
    accuracyAfter: e.afterMetrics?.ACCURACY,
  })),
  UNRESOLVED_CLUSTERS: current.clusters.map((c) => ({
    rootCause: c.rootCause,
    count: c.count,
    status: GOVERNANCE.has(c.rootCause)
      ? "HUMAN_GOVERNANCE_REQUIRED"
      : exhausted[c.rootCause]?.status || "OPEN",
  })),
  DETERMINISTIC_LIMIT_REACHED: detLimit ? "YES" : "NO",
  HYBRID_CLASSIFIER_CANDIDATE_ANALYSIS: hybrid,
  GROUND_TRUTH_REVIEW_NEEDED: gtReview,
  gates,
  NEXT_STEP,
  HOLDOUT_ACCESSED: "NO",
  HOLDOUT_CASES_INSPECTED: 0,
  HOLDOUT_METRICS_RUN: "NO",
  hardGuards: {
    LIVE_PROVIDER_CALLS: 0,
    NEW_MONITORING: 0,
    PUBLIC_CRAWL: 0,
    HOLDOUT_ACCESS: 0,
    HOLDOUT_TUNING: 0,
    UNAUTHORIZED_GT_CHANGES: 0,
    THRESHOLD_LOWERING: 0,
    CASE_SPECIFIC_CLASSIFIER_RULES: 0,
    AIRTABLE_WRITES: 0,
    SCHEMA_CHANGES: 0,
    DEPLOYS: 0,
  },
  taxonomyResolution: {
    TOTAL: 17,
    KEEP: 0,
    AMEND: 17,
    DEFER: 0,
    BY_TRANSITION: {
      "first_recommendation => explicit_recommendation": 13,
      "first_recommendation => associated_option": 3,
      "ranked_recommendation => associated_option": 1,
    },
    DEV_N: 290,
  },
};

ledger.final = report;
ledger.status = status;
ledger.updatedAt = nowIso();
ledger.resumePhase = "taxonomy_resolution_lab_resume";
fs.writeFileSync(LEDGER_PATH, JSON.stringify(ledger, null, 2));
fs.writeFileSync(REPORT_PATH, JSON.stringify(report, null, 2));

const errLedger = {
  version: "classifier_error_ledger_v1",
  updatedAt: nowIso(),
  classifierVersion: "ai_visibility_recommendation_classifier_v4_1_lab_resume",
  errors: current.errors.map((e) => ({
    caseId: e.caseId,
    entity: e.entity,
    humanRole: e.humanRole,
    predictedRole: e.predictedRole,
    provider: e.provider,
    language: e.language,
    geography: e.geography,
    rootCause: e.rootCause,
    status: "OPEN",
    firstSeenVersion: "v4_1",
    lastSeenVersion: "v4_1_lab_resume",
    resolvedByExperiment: null,
    notes: e.pair,
  })),
};
fs.writeFileSync(ERROR_LEDGER_PATH, JSON.stringify(errLedger, null, 2));

console.log(
  JSON.stringify(
    {
      status: report.status,
      NEXT_STEP: report.NEXT_STEP,
      ADDITIONAL_CYCLES: report.ADDITIONAL_CYCLES,
      EXPERIMENTS_TESTED: report.EXPERIMENTS_TESTED_ADDITIONAL,
      ACCEPTED: report.ACCEPTED_ADDITIONAL,
      REJECTED: report.REJECTED_ADDITIONAL,
      ACC: report.FINAL.ACCURACY,
      MACRO_F1: report.FINAL.MACRO_F1,
      FIRST_REC: report.FINAL.FIRST_REC,
      ERRORS: report.ERRORS_AFTER_LAB_RESUME,
      DETERMINISTIC_LIMIT_REACHED: report.DETERMINISTIC_LIMIT_REACHED,
      HYBRID: report.HYBRID_CLASSIFIER_CANDIDATE_ANALYSIS.HYBRID_RECOMMENDED_FOR_NEXT_PHASE,
      accepted: report.RESOLVED_CLUSTERS,
    },
    null,
    2
  )
);
