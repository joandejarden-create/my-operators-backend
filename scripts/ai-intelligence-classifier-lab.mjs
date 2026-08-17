#!/usr/bin/env node
/**
 * AI Intelligence Classifier Lab — autonomous DEV optimization loop.
 *
 * Hard guards: HOLDOUT=0, no GT changes, no provider calls, no Airtable,
 * no threshold lowering, no case-specific rules.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  scoreDevRecommendationLab,
  acceptanceDecision,
  diffErrors,
} from "../lib/ai-visibility/classifier-lab/score-dev.js";
import {
  CANDIDATES,
  snapshotLabTargets,
  restoreLabTargets,
  candidatesForCluster,
} from "../lib/ai-visibility/classifier-lab/candidates.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const LEDGER_PATH = path.join(ROOT, "data/ai-visibility/validation/classifier-lab-ledger.json");
const ERROR_LEDGER_PATH = path.join(
  ROOT,
  "data/ai-visibility/validation/classifier-error-ledger.json"
);
const REPORT_PATH = path.join(
  ROOT,
  "data/ai-visibility/validation/classifier-lab-final-report.json"
);

const MAX_CYCLES = 10;
const GOVERNANCE_CLUSTERS = new Set([
  "FIRST_VS_ASSOCIATED_CONSIDERATION_LIST_TAXONOMY",
  "RANKED_VS_ASSOCIATED_CONSIDERATION_LIST_TAXONOMY",
  "FIRST_LABELED_BUT_ONLY_EXPLICIT_POSITIVE_CUE",
]);

function nowIso() {
  return new Date().toISOString();
}

function loadJson(p, fallback) {
  if (!fs.existsSync(p)) return fallback;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function saveJson(p, obj) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, JSON.stringify(obj, null, 2));
}

function gatesPass(metrics) {
  return (
    (metrics.ACCURACY ?? 0) >= 0.98 &&
    (metrics.PRECISION ?? 0) >= 0.98 &&
    (metrics.RECALL ?? 0) >= 0.98 &&
    (metrics.FIRST_REC ?? 0) >= 0.98 &&
    (metrics.ENTITY_P ?? 1) >= 0.98 &&
    (metrics.ENTITY_R ?? 1) >= 0.98
  );
}

function updateErrorLedger(errorLedger, score, classifierVersion, experimentId = null) {
  const byKey = new Map((errorLedger.errors || []).map((e) => [`${e.caseId}||${e.entity}`, e]));
  const seen = new Set();
  for (const e of score.errors) {
    const key = `${e.caseId}||${e.entity}`;
    seen.add(key);
    const prev = byKey.get(key);
    if (prev) {
      prev.predictedRole = e.predictedRole;
      prev.humanRole = e.humanRole;
      prev.rootCause = e.rootCause;
      prev.lastSeenVersion = classifierVersion;
      prev.status = "OPEN";
      prev.provider = e.provider;
      prev.language = e.language;
      prev.geography = e.geography;
    } else {
      byKey.set(key, {
        caseId: e.caseId,
        entity: e.entity,
        humanRole: e.humanRole,
        predictedRole: e.predictedRole,
        provider: e.provider,
        language: e.language,
        geography: e.geography,
        rootCause: e.rootCause,
        status: "OPEN",
        firstSeenVersion: classifierVersion,
        lastSeenVersion: classifierVersion,
        resolvedByExperiment: null,
        notes: e.pair,
      });
    }
  }
  for (const [key, prev] of byKey) {
    if (!seen.has(key) && prev.status === "OPEN") {
      prev.status = "RESOLVED";
      prev.resolvedByExperiment = experimentId;
      prev.lastSeenVersion = classifierVersion;
    }
  }
  return {
    updatedAt: nowIso(),
    classifierVersion,
    errors: [...byKey.values()],
  };
}

function markGovernanceClusters(score, unresolvedMeta) {
  for (const c of score.clusters) {
    if (GOVERNANCE_CLUSTERS.has(c.rootCause)) {
      unresolvedMeta[c.rootCause] = {
        status: "HUMAN_GOVERNANCE_REQUIRED",
        reason:
          "Taxonomy tension between consideration-list membership and first/ranked GT labels; no safe generalized rule without GT/taxonomy decision",
        count: c.count,
      };
    }
  }
}

async function runExperiment({
  candidate,
  before,
  cycle,
  classifierVersion,
  ledger,
}) {
  const snap = snapshotLabTargets();
  let after = null;
  let accepted = false;
  let rejectionReason = null;
  let fixedErrors = [];
  let newErrors = [];
  let applyError = null;

  try {
    candidate.apply();
    // bust module cache by dynamic import with query? Node caches by path — need to spawn subprocess for clean import
  } catch (err) {
    restoreLabTargets(snap);
    applyError = String(err?.message || err);
  }

  if (!applyError) {
    // Score in child process so modules reload
    const scorePath = path.join(ROOT, "scripts/_classifier-lab-score-once.mjs");
    fs.writeFileSync(
      scorePath,
      `
import { scoreDevRecommendationLab } from "../lib/ai-visibility/classifier-lab/score-dev.js";
const s = await scoreDevRecommendationLab({ classifierVersion: process.argv[2] });
process.stdout.write(JSON.stringify(s));
`
    );
    try {
      const { spawnSync } = await import("child_process");
      const r = spawnSync(process.execPath, [scorePath, classifierVersion], {
        cwd: ROOT,
        encoding: "utf8",
        maxBuffer: 64 * 1024 * 1024,
      });
      if (r.status !== 0) {
        applyError = r.stderr || r.stdout || "score_failed";
      } else {
        after = JSON.parse(r.stdout);
      }
    } catch (err) {
      applyError = String(err?.message || err);
    }
  }

  if (applyError || !after) {
    restoreLabTargets(snap);
    const rec = {
      experimentId: candidate.id,
      cycle,
      classifierVersion,
      hypothesis: candidate.hypothesis,
      errorCluster: candidate.errorCluster,
      codeChange: candidate.id,
      affectedCases: [],
      beforeMetrics: before.metrics,
      afterMetrics: null,
      classMetricsBefore: before.classMetrics,
      classMetricsAfter: null,
      fixedErrors: [],
      newErrors: [],
      accepted: false,
      rejectionReason: applyError || "score_failed",
      timestamp: nowIso(),
    };
    ledger.experiments.push(rec);
    return { accepted: false, after: before, snapRestored: true };
  }

  const decision = acceptanceDecision(before, after);
  const diff = diffErrors(before.errors, after.errors);
  fixedErrors = diff.fixedErrors;
  newErrors = diff.newErrors;
  accepted = decision.accepted;
  rejectionReason = decision.rejectionReason;

  if (!accepted) {
    restoreLabTargets(snap);
  }

  const rec = {
    experimentId: candidate.id,
    cycle,
    classifierVersion,
    hypothesis: candidate.hypothesis,
    errorCluster: candidate.errorCluster,
    codeChange: `apply ${candidate.id}`,
    affectedCases: [...fixedErrors, ...newErrors].slice(0, 40),
    beforeMetrics: before.metrics,
    afterMetrics: after.metrics,
    classMetricsBefore: before.classMetrics,
    classMetricsAfter: after.classMetrics,
    fixedErrors,
    newErrors,
    accepted,
    rejectionReason,
    timestamp: nowIso(),
  };
  ledger.experiments.push(rec);

  return { accepted, after: accepted ? after : before, snapRestored: !accepted, decision };
}

async function main() {
  // Fresh run — preserve prior ledger history under priorRuns if present
  const prior = loadJson(LEDGER_PATH, null);
  const ledger = {
    version: "classifier_lab_v1",
    createdAt: nowIso(),
    priorRuns: prior ? [...(prior.priorRuns || []), { closedAt: prior.updatedAt, status: prior.status, experiments: prior.experiments?.length }] : [],
    experiments: [],
    cycles: [],
    exhaustedClusters: {},
    status: "RUNNING",
  };
  let errorLedger = {
    version: "classifier_error_ledger_v1",
    updatedAt: nowIso(),
    errors: [],
  };

  console.log("LAB: baseline score...");
  const baseline = await scoreDevRecommendationLab({
    classifierVersion: "ai_visibility_recommendation_classifier_v4_1",
  });
  ledger.baseline = {
    metrics: baseline.metrics,
    classMetrics: baseline.classMetrics,
    errorCount: baseline.errorCount,
    clusters: baseline.clusters,
    pairs: baseline.pairs,
    timestamp: nowIso(),
  };
  errorLedger = updateErrorLedger(
    errorLedger,
    baseline,
    "ai_visibility_recommendation_classifier_v4_1"
  );
  saveJson(ERROR_LEDGER_PATH, errorLedger);
  saveJson(LEDGER_PATH, ledger);

  let current = baseline;
  const exhausted = new Set();
  const acceptedIds = [];
  const rejectedIds = [];
  let cyclesRun = 0;
  let stopReason = null;
  const unresolvedMeta = {};
  markGovernanceClusters(current, unresolvedMeta);

  if (gatesPass(current.metrics)) {
    stopReason = "READY_FOR_FINAL_UNTOUCHED_HOLDOUT_EVALUATION";
  }

  for (let cycle = 1; cycle <= MAX_CYCLES && !stopReason; cycle++) {
    cyclesRun = cycle;
    console.log(`\nLAB cycle ${cycle}/${MAX_CYCLES}`);
    markGovernanceClusters(current, unresolvedMeta);

    const ranked = current.clusters.filter(
      (c) =>
        !exhausted.has(c.rootCause) &&
        unresolvedMeta[c.rootCause]?.status !== "HUMAN_GOVERNANCE_REQUIRED"
    );
    if (!ranked.length) {
      stopReason = "AUTONOMOUS_DEV_OPTIMIZATION_EXHAUSTED";
      break;
    }

    const target = ranked[0];
    console.log(`  target cluster: ${target.rootCause} (n=${target.count})`);

    let cands = candidatesForCluster(target.rootCause, new Set([...rejectedIds, ...acceptedIds]));
    if (!cands.length) {
      // related family
      cands = CANDIDATES.filter(
        (c) =>
          !acceptedIds.includes(c.id) &&
          !rejectedIds.includes(c.id) &&
          (c.errorCluster === target.rootCause ||
            (target.rootCause.includes("ASSOCIATED") && c.errorCluster.includes("ASSOCIATED")) ||
            (target.rootCause.includes("FIRST") && c.errorCluster.includes("FIRST")) ||
            (target.rootCause.includes("RANKED") && c.errorCluster.includes("RANK")) ||
            (target.rootCause.includes("EXPLICIT") && c.errorCluster.includes("EXPLICIT")))
      );
    }
    if (!cands.length) {
      cands = CANDIDATES.filter((c) => !acceptedIds.includes(c.id) && !rejectedIds.includes(c.id));
    }

    if (!cands.length) {
      ledger.exhaustedClusters[target.rootCause] = {
        status: "IRREDUCIBLE_AMBIGUITY",
        reason: "No remaining generalized candidates for cluster",
        count: target.count,
      };
      exhausted.add(target.rootCause);
      ledger.cycles.push({
        cycle,
        targetCluster: target.rootCause,
        experiments: [],
        accepted: null,
        timestamp: nowIso(),
      });
      continue;
    }

    // Independent tests from the same baseline snapshot for this cycle
    const cycleSnap = snapshotLabTargets();
    const trialResults = [];
    const cycleExps = [];

    for (const cand of cands.slice(0, 4)) {
      console.log(`  try ${cand.id}...`);
      restoreLabTargets(cycleSnap);
      const versionTag = `lab_${cand.id}_c${cycle}`;
      const result = await runExperiment({
        candidate: cand,
        before: current,
        cycle,
        classifierVersion: versionTag,
        ledger,
      });
      // runExperiment restores on reject; on accept leaves applied — force restore for independence
      restoreLabTargets(cycleSnap);
      cycleExps.push(cand.id);
      if (result.accepted) {
        console.log(
          `  CANDIDATE PASS ${cand.id} acc ${current.metrics.ACCURACY.toFixed(4)} → ${result.after.metrics.ACCURACY.toFixed(4)}`
        );
        trialResults.push({ cand, after: result.after, delta: result.after.metrics.ACCURACY - current.metrics.ACCURACY });
        acceptedIds.push(cand.id); // provisional — final pick below
      } else {
        const reason =
          ledger.experiments.filter((e) => e.experimentId === cand.id).slice(-1)[0]
            ?.rejectionReason || "rejected";
        console.log(`  REJECT ${cand.id}: ${reason}`);
        rejectedIds.push(cand.id);
      }
    }

    // Pick best accepted trial (highest accuracy, then macro F1)
    trialResults.sort(
      (a, b) =>
        b.delta - a.delta ||
        (b.after.metrics.MACRO_F1 ?? 0) - (a.after.metrics.MACRO_F1 ?? 0)
    );
    // Remove provisional accepts that weren't selected
    for (const t of trialResults.slice(1)) {
      const idx = acceptedIds.lastIndexOf(t.cand.id);
      if (idx >= 0) acceptedIds.splice(idx, 1);
      rejectedIds.push(t.cand.id);
      const exp = ledger.experiments.filter((e) => e.experimentId === t.cand.id).slice(-1)[0];
      if (exp) {
        exp.accepted = false;
        exp.rejectionReason = (exp.rejectionReason || "") + "|not_selected_as_cycle_winner";
      }
    }

    const best = trialResults[0] || null;
    if (best) {
      // Re-apply winner and keep
      restoreLabTargets(cycleSnap);
      best.cand.apply();
      current = best.after;
      errorLedger = updateErrorLedger(errorLedger, current, `lab_${best.cand.id}_c${cycle}`, best.cand.id);
      console.log(`  KEEP WINNER ${best.cand.id}`);
    } else {
      restoreLabTargets(cycleSnap);
      ledger.exhaustedClusters[target.rootCause] = {
        status: GOVERNANCE_CLUSTERS.has(target.rootCause)
          ? "HUMAN_GOVERNANCE_REQUIRED"
          : "IRREDUCIBLE_AMBIGUITY",
        reason: "All candidate fixes rejected by acceptance rule",
        count: target.count,
        tried: cycleExps,
      };
      exhausted.add(target.rootCause);
      if (GOVERNANCE_CLUSTERS.has(target.rootCause)) {
        unresolvedMeta[target.rootCause] = ledger.exhaustedClusters[target.rootCause];
      }
    }

    ledger.cycles.push({
      cycle,
      targetCluster: target.rootCause,
      experiments: cycleExps,
      accepted: best?.cand?.id || null,
      metrics: current.metrics,
      errorCount: current.errorCount,
      timestamp: nowIso(),
    });
    saveJson(LEDGER_PATH, ledger);
    saveJson(ERROR_LEDGER_PATH, errorLedger);

    if (gatesPass(current.metrics)) {
      stopReason = "READY_FOR_FINAL_UNTOUCHED_HOLDOUT_EVALUATION";
      break;
    }

    if (cycle === MAX_CYCLES) {
      stopReason = "AUTONOMOUS_DEV_OPTIMIZATION_EXHAUSTED";
    }
  }

  // Final score confirmation in child process (avoid parent module cache)
  const scorePath = path.join(ROOT, "scripts/_classifier-lab-score-once.mjs");
  if (!fs.existsSync(scorePath)) {
    fs.writeFileSync(
      scorePath,
      `
import { scoreDevRecommendationLab } from "../lib/ai-visibility/classifier-lab/score-dev.js";
const s = await scoreDevRecommendationLab({ classifierVersion: process.argv[2] });
process.stdout.write(JSON.stringify(s));
`
    );
  }
  const { spawnSync } = await import("child_process");
  const fr = spawnSync(
    process.execPath,
    [scorePath, "ai_visibility_recommendation_classifier_v4_1_lab"],
    { cwd: ROOT, encoding: "utf8", maxBuffer: 64 * 1024 * 1024 }
  );
  if (fr.status !== 0) {
    throw new Error(fr.stderr || fr.stdout || "final_score_failed");
  }
  const finalScore = JSON.parse(fr.stdout);
  current = finalScore;
  errorLedger = updateErrorLedger(
    errorLedger,
    finalScore,
    "ai_visibility_recommendation_classifier_v4_1_lab"
  );

  const gtReview = finalScore.errors
    .filter((e) => GOVERNANCE_CLUSTERS.has(e.rootCause))
    .slice(0, 40)
    .map((e) => ({
      caseId: e.caseId,
      entity: e.entity,
      humanRole: e.humanRole,
      predictedRole: e.predictedRole,
      rootCause: e.rootCause,
    }));

  const report = {
    phase: "AI_INTELLIGENCE_CLASSIFIER_LAB_COMPLETE",
    status: stopReason || "AUTONOMOUS_DEV_OPTIMIZATION_EXHAUSTED",
    BASELINE: ledger.baseline.metrics,
    FINAL: finalScore.metrics,
    CYCLES_RUN: cyclesRun,
    EXPERIMENTS_TESTED: ledger.experiments.length,
    EXPERIMENTS_ACCEPTED: ledger.experiments.filter((e) => e.accepted).length,
    EXPERIMENTS_REJECTED: ledger.experiments.filter((e) => !e.accepted).length,
    METRICS_BEFORE: ledger.baseline.metrics,
    METRICS_AFTER: finalScore.metrics,
    CLASS_METRICS_BEFORE: ledger.baseline.classMetrics,
    CLASS_METRICS_AFTER: finalScore.classMetrics,
    ERRORS_BEFORE: ledger.baseline.errorCount,
    ERRORS_AFTER: finalScore.errorCount,
    RESOLVED_CLUSTERS: acceptedIds.map((id) => {
      const exp = ledger.experiments.find((e) => e.experimentId === id && e.accepted);
      return { experimentId: id, errorCluster: exp?.errorCluster, fixed: exp?.fixedErrors?.length };
    }),
    UNRESOLVED_CLUSTERS: [
      ...Object.entries(ledger.exhaustedClusters).map(([k, v]) => ({ rootCause: k, ...v })),
      ...Object.entries(unresolvedMeta)
        .filter(([k]) => !ledger.exhaustedClusters[k])
        .map(([k, v]) => ({ rootCause: k, ...v })),
      ...finalScore.clusters
        .filter(
          (c) =>
            !acceptedIds.some(
              (id) =>
                ledger.experiments.find((e) => e.experimentId === id && e.accepted)?.errorCluster ===
                c.rootCause
            )
        )
        .map((c) => ({
          rootCause: c.rootCause,
          count: c.count,
          status: GOVERNANCE_CLUSTERS.has(c.rootCause)
            ? "HUMAN_GOVERNANCE_REQUIRED"
            : exhausted.has(c.rootCause)
              ? "IRREDUCIBLE_AMBIGUITY"
              : "OPEN",
        })),
    ],
    GROUND_TRUTH_REVIEW_NEEDED: gtReview,
    HOLDOUT_ACCESSED: "NO",
    NEXT_STEP:
      stopReason === "READY_FOR_FINAL_UNTOUCHED_HOLDOUT_EVALUATION"
        ? "READY_FOR_FINAL_UNTOUCHED_HOLDOUT_EVALUATION"
        : gtReview.length
          ? "HUMAN_GROUND_TRUTH_DECISION_REQUIRED"
          : "MORE_DEVELOPMENT_HARDENING_REQUIRED",
    holdout: finalScore.holdout,
    hardGuards: {
      HOLDOUT_ACCESS: 0,
      GOLDEN_SET_LABEL_CHANGES: 0,
      PROVIDER_CALLS: 0,
      AIRTABLE_WRITES: 0,
      DEPLOYS: 0,
      THRESHOLD_LOWERING: 0,
      CASE_SPECIFIC_RULES: 0,
    },
  };

  ledger.final = report;
  ledger.status = report.status;
  ledger.updatedAt = nowIso();
  saveJson(LEDGER_PATH, ledger);
  saveJson(ERROR_LEDGER_PATH, errorLedger);
  saveJson(REPORT_PATH, report);

  console.log("\n=== LAB COMPLETE ===");
  console.log(JSON.stringify({
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
  }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
