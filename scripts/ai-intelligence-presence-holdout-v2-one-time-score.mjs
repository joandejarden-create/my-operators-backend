#!/usr/bin/env node
/**
 * One-time Presence Holdout v2 score (Presence only).
 *
 *   node scripts/ai-intelligence-presence-holdout-v2-one-time-score.mjs
 *   node scripts/ai-intelligence-presence-holdout-v2-one-time-score.mjs --seal-only
 *
 * PROVIDER_CALLS=0 · no tuning · no second run
 */
import {
  runPresenceHoldoutV2OneTimeScore,
  verifyHoldoutV2Seal,
  DEFAULT_HOLDOUT_V2_MANIFEST,
} from "../lib/ai-visibility/validation/presence-holdout-v2-score.js";
import fs from "fs";

const SEAL_ONLY = process.argv.includes("--seal-only");

if (SEAL_ONLY) {
  const manifest = JSON.parse(fs.readFileSync(DEFAULT_HOLDOUT_V2_MANIFEST, "utf8"));
  const seal = verifyHoldoutV2Seal(manifest);
  console.log("HOLDOUT_V2_SEAL_CHECK");
  console.log(JSON.stringify(seal, null, 2));
  process.exit(seal.ok ? 0 : 1);
}

const report = runPresenceHoldoutV2OneTimeScore({ persist: true });

console.log("PRESENCE_HOLDOUT_V2_ONE_TIME_SCORE_COMPLETE");
console.log(`status=${report.status}`);
console.log(`gate=${report.productionGate?.PRESENCE_HOLDOUT_V2_GATE || report.stopReason}`);
if (report.confusionMatrix) {
  console.log(
    `TP=${report.confusionMatrix.TP} TN=${report.confusionMatrix.TN} FP=${report.confusionMatrix.FP} FN=${report.confusionMatrix.FN}`
  );
  console.log(
    `P=${report.metrics.PRECISION_PCT} R=${report.metrics.RECALL_PCT} Acc=${report.metrics.ACCURACY_PCT}`
  );
}
console.log(`nextStep=${report.nextStep}`);
if (report.persisted) console.log(`wrote=${report.persisted.reportPath}`);
