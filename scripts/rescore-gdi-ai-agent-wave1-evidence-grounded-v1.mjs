#!/usr/bin/env node
/**
 * Offline rescore of preserved Wave 1 results with evidence-grounded evaluator.
 * No network, no paid providers, no Airtable/publication writes.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  rescoreWave1Report,
  analyzeAdaptiveStopBehavior,
} from "../lib/group-demand-intelligence/experiments/gdi-ai-agent-wave1-evidence-grounded-evaluator-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT_DIR = path.join(ROOT, "reports/gdi-ai-agent-incremental-value-experiment-v1");
const INPUT = path.join(REPORT_DIR, "WAVE1_RESULTS.json");
const OUT_RESCORE = path.join(REPORT_DIR, "WAVE1_EVIDENCE_GROUNDED_RESCORE.json");
const OUT_STOP = path.join(REPORT_DIR, "WAVE1_STOP_BEHAVIOR_ANALYSIS.json");

if (!fs.existsSync(INPUT)) {
  console.error(JSON.stringify({ ok: false, error: "WAVE1_RESULTS.json missing", path: INPUT }, null, 2));
  process.exit(2);
}

const original = JSON.parse(fs.readFileSync(INPUT, "utf8"));
const rescored = rescoreWave1Report(original);
const stopAnalysis = analyzeAdaptiveStopBehavior(original);

fs.writeFileSync(OUT_RESCORE, `${JSON.stringify(rescored, null, 2)}\n`, "utf8");
fs.writeFileSync(OUT_STOP, `${JSON.stringify(stopAnalysis, null, 2)}\n`, "utf8");

console.log(
  JSON.stringify(
    {
      ok: true,
      networkUsed: false,
      paidUsd: 0,
      previousDecision: original.decision?.decision || null,
      correctedDecision: rescored.decision.decision,
      correctedReason: rescored.decision.reason,
      materialCaseIds: rescored.materialCaseIds,
      drawerOnlyPreviouslyLifted: rescored.drawerOnlyPreviouslyLifted,
      stopSummary: stopAnalysis.summary,
      outRescore: path.relative(ROOT, OUT_RESCORE).replace(/\\/g, "/"),
      outStop: path.relative(ROOT, OUT_STOP).replace(/\\/g, "/"),
    },
    null,
    2,
  ),
);
