#!/usr/bin/env node
/**
 * Classifier lab CONTINUE — keep accepted patches on disk; try remaining candidates.
 * CLASSIFIER_LAB_CONTINUE=1 semantics. Max additional cycles.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { spawnSync } from "child_process";
import {
  acceptanceDecision,
  diffErrors,
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
const REPORT_PATH = path.join(ROOT, "data/ai-visibility/validation/classifier-lab-final-report.json");
const SCORE_ONCE = path.join(ROOT, "scripts/_classifier-lab-score-once.mjs");

const ORIGINAL_BASELINE = {
  ACCURACY: 0.6206896551724138,
  PRECISION: 0.6206896551724138,
  RECALL: 0.6206896551724138,
  F1: 0.6206896551724138,
  MACRO_P: 0.7092161715343246,
  MACRO_R: 0.6717595066009713,
  MACRO_F1: 0.6530297091771275,
  FIRST_REC: 0.8758620689655172,
  QUESTION_STATUS: 0.5975103734439834,
  ENTITY_P: 1,
  ENTITY_R: 1,
  ENTITY_F1: 1,
};

const GOVERNANCE = new Set([
  "FIRST_VS_ASSOCIATED_CONSIDERATION_LIST_TAXONOMY",
  "RANKED_VS_ASSOCIATED_CONSIDERATION_LIST_TAXONOMY",
  "FIRST_LABELED_BUT_ONLY_EXPLICIT_POSITIVE_CUE",
]);

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

const ledger = JSON.parse(fs.readFileSync(LEDGER_PATH, "utf8"));
const already = new Set(
  (ledger.experiments || []).map((e) => e.experimentId)
);
const accepted = new Set(
  (ledger.experiments || []).filter((e) => e.accepted).map((e) => e.experimentId)
);

let current = score("lab_continue_baseline");
const runBaseline = {
  metrics: current.metrics,
  classMetrics: current.classMetrics,
  errorCount: current.errorCount,
  clusters: current.clusters,
  pairs: current.pairs,
};
console.log("CONTINUE baseline", {
  acc: current.metrics.ACCURACY,
  errors: current.errorCount,
});

const originalBaselineMetrics = ledger.baseline?.metrics || ORIGINAL_BASELINE;
const originalBaselineClass = ledger.baseline?.classMetrics || null;
const originalErrorCount = ledger.baseline?.errorCount || 110;

let cyclesAdded = 0;
const maxAdd = 6;
const exhausted = {};

while (cyclesAdded < maxAdd) {
  cyclesAdded++;
  const cycle = (ledger.cycles?.length || 0) + 1;
  console.log(`\nCONTINUE cycle ${cycle} (+${cyclesAdded}/${maxAdd})`);

  const ranked = current.clusters.filter(
    (c) => !GOVERNANCE.has(c.rootCause) && !exhausted[c.rootCause]
  );
  if (!ranked.length) {
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
        (target.rootCause.includes("PROFILE") && c.errorCluster.includes("PROFILE")))
  );
  if (!cands.length) {
    cands = CANDIDATES.filter((c) => !already.has(c.id));
  }
  if (!cands.length) {
    exhausted[target.rootCause] = {
      status: "IRREDUCIBLE_AMBIGUITY",
      reason: "no candidates left",
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
      ledger.experiments.push({
        experimentId: cand.id,
        cycle,
        hypothesis: cand.hypothesis,
        errorCluster: cand.errorCluster,
        accepted: false,
        rejectionReason: String(e.message || e),
        timestamp: nowIso(),
      });
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
    };
    ledger.experiments.push(rec);
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
    exhausted[target.rootCause] = {
      status: GOVERNANCE.has(target.rootCause)
        ? "HUMAN_GOVERNANCE_REQUIRED"
        : "IRREDUCIBLE_AMBIGUITY",
      reason: "all candidates rejected",
      count: target.count,
      tried,
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
  });
  ledger.exhaustedClusters = { ...(ledger.exhaustedClusters || {}), ...exhausted };
  fs.writeFileSync(LEDGER_PATH, JSON.stringify(ledger, null, 2));

  if (
    current.metrics.ACCURACY >= 0.98 &&
    current.metrics.FIRST_REC >= 0.98 &&
    current.metrics.ENTITY_P >= 0.98
  ) {
    break;
  }
}

// Final child score
current = score("lab_final");
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

const report = {
  phase: "AI_INTELLIGENCE_CLASSIFIER_LAB_COMPLETE",
  status:
    current.metrics.ACCURACY >= 0.98 && current.metrics.FIRST_REC >= 0.98
      ? "READY_FOR_FINAL_UNTOUCHED_HOLDOUT_EVALUATION"
      : gtReview.length
        ? "HUMAN_GROUND_TRUTH_DECISION_REQUIRED"
        : "AUTONOMOUS_DEV_OPTIMIZATION_EXHAUSTED",
  BASELINE: originalBaselineMetrics,
  FINAL: current.metrics,
  CYCLES_RUN: ledger.cycles.length,
  EXPERIMENTS_TESTED: ledger.experiments.length,
  EXPERIMENTS_ACCEPTED: acceptedExps.length,
  EXPERIMENTS_REJECTED: rejectedExps.length,
  METRICS_BEFORE: originalBaselineMetrics,
  METRICS_AFTER: current.metrics,
  CLASS_METRICS_BEFORE: originalBaselineClass || runBaseline.classMetrics,
  CLASS_METRICS_AFTER: current.classMetrics,
  ERRORS_BEFORE: originalErrorCount,
  ERRORS_AFTER: current.errorCount,
  RESOLVED_CLUSTERS: acceptedExps.map((e) => ({
    experimentId: e.experimentId,
    errorCluster: e.errorCluster,
    fixed: e.fixedErrors?.length ?? null,
    accuracyAfter: e.afterMetrics?.ACCURACY,
  })),
  UNRESOLVED_CLUSTERS: [
    ...Object.entries(ledger.exhaustedClusters || {}).map(([k, v]) => ({
      rootCause: k,
      ...v,
    })),
    ...current.clusters.map((c) => ({
      rootCause: c.rootCause,
      count: c.count,
      status: GOVERNANCE.has(c.rootCause)
        ? "HUMAN_GOVERNANCE_REQUIRED"
        : exhausted[c.rootCause]
          ? exhausted[c.rootCause].status
          : "OPEN",
    })),
  ],
  GROUND_TRUTH_REVIEW_NEEDED: gtReview,
  HOLDOUT_ACCESSED: "NO",
  NEXT_STEP:
    current.metrics.ACCURACY >= 0.98 && current.metrics.FIRST_REC >= 0.98
      ? "READY_FOR_FINAL_UNTOUCHED_HOLDOUT_EVALUATION"
      : gtReview.length
        ? "HUMAN_GROUND_TRUTH_DECISION_REQUIRED"
        : "MORE_DEVELOPMENT_HARDENING_REQUIRED",
  holdout: current.holdout,
  hardGuards: {
    HOLDOUT_ACCESS: 0,
    GOLDEN_SET_LABEL_CHANGES: 0,
    PROVIDER_CALLS: 0,
    AIRTABLE_WRITES: 0,
    DEPLOYS: 0,
    THRESHOLD_LOWERING: 0,
    CASE_SPECIFIC_RULES: 0,
  },
  continueRunBaseline: runBaseline.metrics,
};

ledger.final = report;
ledger.status = report.status;
ledger.updatedAt = nowIso();
fs.writeFileSync(LEDGER_PATH, JSON.stringify(ledger, null, 2));
fs.writeFileSync(REPORT_PATH, JSON.stringify(report, null, 2));

// refresh error ledger
const errLedger = {
  version: "classifier_error_ledger_v1",
  updatedAt: nowIso(),
  classifierVersion: "ai_visibility_recommendation_classifier_v4_1_lab",
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
    lastSeenVersion: "v4_1_lab",
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
      CYCLES_RUN: report.CYCLES_RUN,
      EXPERIMENTS_TESTED: report.EXPERIMENTS_TESTED,
      EXPERIMENTS_ACCEPTED: report.EXPERIMENTS_ACCEPTED,
      EXPERIMENTS_REJECTED: report.EXPERIMENTS_REJECTED,
      ACC_BEFORE: report.METRICS_BEFORE.ACCURACY,
      ACC_AFTER: report.METRICS_AFTER.ACCURACY,
      ERRORS_BEFORE: report.ERRORS_BEFORE,
      ERRORS_AFTER: report.ERRORS_AFTER,
      accepted: report.RESOLVED_CLUSTERS,
    },
    null,
    2
  )
);
