/**
 * No-spend readiness validation for the first live ownership research experiment.
 * Produces the founder manifest; never enables paid network.
 */

import fs from "node:fs";
import path from "node:path";
import {
  buildLiveExperimentManifest,
  LIVE_EXPERIMENT_MANIFEST_ID,
  LIVE_EXPERIMENT_MANIFEST_VERSION,
  RESEARCH_METHODS,
} from "./live-experiment-manifest-v1.js";
import { runPhaseASafetySuite } from "./live-activation.js";
import { P3B_M1_HPC_IDS } from "./cohort.js";
import { createIsolatedDiscoveryBags } from "../benchmark-p3a/harness.js";
import { createBenchmarkStagingStore } from "../benchmark-p3a/staging-store.js";
import { scoreOwnershipQuality } from "./quality-scorecard-v1.js";
import { adjudicateFinding, buildMaterialFindingTemplate } from "./adjudication-protocol-v1.js";
import { toDiscoveryCandidate } from "./discovery-candidates.js";
import { OWNERSHIP_SUCCESS_TIER } from "../ownership-research-objective.js";

export async function runLiveExperimentReadinessValidation(opts = {}) {
  const manifest = buildLiveExperimentManifest(opts);
  const phaseA = await runPhaseASafetySuite(opts.phaseA || {});
  const store = createBenchmarkStagingStore({
    runId: opts.runId || `p3b_m1_exp_ready_${Date.now()}`,
    memoryOnly: opts.memoryOnly !== false,
  });

  const arms = [
    RESEARCH_METHODS.A_EXA,
    RESEARCH_METHODS.B_PARALLEL_SEARCH,
    RESEARCH_METHODS.C_PARALLEL_TASK,
    RESEARCH_METHODS.D_DEALALITY_NATIVE,
  ];
  const bags = createIsolatedDiscoveryBags(arms);
  const leakage = [];
  for (const a of arms) {
    for (const b of arms) {
      if (a === b) continue;
      bags.append(a, { claimType: "SEED", value: "x", isSeedFact: true });
      const leak = bags.readOther(a, b);
      if (leak?.ok !== false) leakage.push({ a, b });
    }
  }

  // Discovery candidate must not adjudicate as verified ownership
  const candidate = toDiscoveryCandidate({
    url: "https://example.com/ownership-filing",
    title: "Example Owner Candidate",
  });
  const adj = adjudicateFinding(candidate, {});
  const scoreEmpty = scoreOwnershipQuality({
    classifierInput: { stagedOnly: true, ultimateOwnerUnresolved: true },
    actual_ownership_accuracy: 0,
  });

  const graphGate = store.assertNoProductionGraphWrite({
    productionAirtableWrite: false,
  });

  const comparisonSkeleton = (manifest.cohort.hotels || []).flatMap((hotel) =>
    Object.values(RESEARCH_METHODS).map((method) => ({
      method,
      hotel,
      ownership_quality: null,
      verified_people: null,
      verified_routes: null,
      total_cost: method === RESEARCH_METHODS.D_DEALALITY_NATIVE
        ? "NOT_EXECUTED_COST_UNKNOWN"
        : method === RESEARCH_METHODS.E_HYBRID_WATERFALL
          ? "NOT_EXECUTED_LABELED_SIMULATION_ONLY"
          : null,
      cost_efficiency: null,
      executed: false,
    }))
  );

  const materialFindingTemplate = buildMaterialFindingTemplate({
    hotel_hpc_id: P3B_M1_HPC_IDS[0],
  });

  const ok =
    manifest.ok === true &&
    manifest.paid_execution_authorized === false &&
    phaseA.ok === true &&
    leakage.length === 0 &&
    adj.accepted === false &&
    scoreEmpty.full_actionable === false &&
    scoreEmpty.ownership_success_tier === OWNERSHIP_SUCCESS_TIER.UNRESOLVED &&
    graphGate.ok === true &&
    manifest.financial_controls.expected_cost_is_not_max_guarantee === true &&
    manifest.methods[RESEARCH_METHODS.D_DEALALITY_NATIVE].executed_when_authorized ===
      false;

  const report = {
    ok,
    phase: "P3B-M1-LIVE-EXPERIMENT-READINESS",
    manifest_id: LIVE_EXPERIMENT_MANIFEST_ID,
    manifest_version: LIVE_EXPERIMENT_MANIFEST_VERSION,
    paid_execution_authorized: false,
    proposed_budget_not_authorized: true,
    phaseASafety: { ok: phaseA.ok },
    armIsolation: { leakageViolations: leakage },
    adjudicationSmoke: adj,
    scorecardSmoke: {
      quality_score_pct: scoreEmpty.quality_score_pct,
      full_actionable: scoreEmpty.full_actionable,
      numerical_pass_is_not_full_actionable:
        scoreEmpty.numerical_pass_is_not_full_actionable,
    },
    materialFindingTemplate,
    comparisonSkeleton,
    manifest,
    staging: { ...store.policy, graphGate },
    stop: "STOP_BEFORE_PAID_EXECUTION",
  };

  store.writeJson("live-experiment-readiness-manifest.json", {
    runId: store.runId,
    paid_authorized: false,
  });

  return report;
}

export function writeLiveExperimentReadinessArtifacts(report, opts = {}) {
  const outDir =
    opts.outDir ||
    path.join(
      process.cwd(),
      "reports/hotel-explorer/he-intel-001-research-benchmark"
    );
  fs.mkdirSync(outDir, { recursive: true });
  const manifestPath = path.join(outDir, "p3b-m1-live-experiment-manifest.json");
  const readinessPath = path.join(outDir, "p3b-m1-live-experiment-readiness.json");
  fs.writeFileSync(manifestPath, JSON.stringify(report.manifest, null, 2));
  fs.writeFileSync(readinessPath, JSON.stringify(report, null, 2));
  return { manifestPath, readinessPath };
}
