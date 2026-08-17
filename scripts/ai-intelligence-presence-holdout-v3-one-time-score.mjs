#!/usr/bin/env node
/**
 * One-time Presence Holdout v3 score (Presence only).
 *
 *   node scripts/ai-intelligence-presence-holdout-v3-one-time-score.mjs
 *   node scripts/ai-intelligence-presence-holdout-v3-one-time-score.mjs --seal-only
 *
 * PROVIDER_CALLS=0 · no tuning · no second run
 */
import fs from "fs";
import {
  runPresenceHoldoutV3OneTimeScore,
  verifyHoldoutV3Seal,
  DEFAULT_HOLDOUT_V3_MANIFEST,
} from "../lib/ai-visibility/validation/presence-holdout-v3-score.js";

const SEAL_ONLY = process.argv.includes("--seal-only");

if (SEAL_ONLY) {
  const manifest = JSON.parse(fs.readFileSync(DEFAULT_HOLDOUT_V3_MANIFEST, "utf8"));
  const seal = verifyHoldoutV3Seal(manifest);
  console.log("HOLDOUT_V3_SEAL_CHECK");
  console.log(JSON.stringify(seal, null, 2));
  process.exit(seal.ok ? 0 : 1);
}

const report = runPresenceHoldoutV3OneTimeScore({ persist: true });

console.log("PRESENCE_HOLDOUT_V3_ONE_TIME_SCORE_COMPLETE");
console.log(`status=${report.status}`);
if (report.stopReason) {
  console.log(`stopReason=${report.stopReason}`);
  if (report.seal) console.log(JSON.stringify(report.seal, null, 2));
  process.exit(1);
}

const g = report.productionGate;
console.log(`gate=${g.PRESENCE_HOLDOUT_V3_GATE} P_gate=${g.PRECISION_GATE} R_gate=${g.RECALL_GATE}`);
console.log(
  `TP=${report.confusionMatrix.TP} TN=${report.confusionMatrix.TN} FP=${report.confusionMatrix.FP} FN=${report.confusionMatrix.FN}`
);
console.log(
  `P=${report.metrics.PRECISION_PCT} R=${report.metrics.RECALL_PCT} Acc=${report.metrics.ACCURACY_PCT}`
);
console.log(`errors=${report.errors.TOTAL_ERRORS}`);
console.log(`nextStep=${report.nextStep}`);
if (report.persisted) console.log(`wrote=${report.persisted.reportPath}`);
