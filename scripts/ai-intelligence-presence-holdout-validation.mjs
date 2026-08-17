#!/usr/bin/env node
/**
 * Final Presence-only holdout validation.
 * LIVE_PROVIDER_CALLS: 0. HOLDOUT_TUNING: 0. CLASSIFIER_CHANGES: 0.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { runPresenceHoldoutValidation } from "../lib/ai-visibility/validation/presence-holdout-validation.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(
  __dirname,
  "../data/ai-visibility/validation/presence-holdout-validation.json"
);

const report = await runPresenceHoldoutValidation({});
fs.mkdirSync(path.dirname(OUT), { recursive: true });
fs.writeFileSync(OUT, JSON.stringify(report, null, 2) + "\n", "utf8");

console.log("AI_INTELLIGENCE_PRESENCE_HOLDOUT_VALIDATION_COMPLETE");
console.log(`status=${report.status}`);
console.log(`nextStep=${report.nextStep}`);
if (report.stopReason) console.log(`stopReason=${report.stopReason}`);
if (report.presence) {
  console.log(
    `N=${report.presence.N} P=${report.presence.PRECISION} R=${report.presence.RECALL} F1=${report.presence.F1}`
  );
  console.log(`HOLDOUT_GATE=${report.gate?.HOLDOUT_GATE}`);
  console.log(`errors=${report.errors?.TOTAL}`);
}
console.log(`Wrote ${OUT}`);
