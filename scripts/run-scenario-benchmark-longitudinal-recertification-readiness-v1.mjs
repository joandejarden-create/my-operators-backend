#!/usr/bin/env node
/**
 * Longitudinal benchmark recertification readiness audit (no provider calls).
 */
import {
  runLongitudinalRecertificationReadiness,
  runLongitudinalRecertification,
} from "../lib/ai-visibility/competitive-moat/scenario-benchmark-longitudinal-recertification.js";

const mode = process.argv.includes("--recertify") ? "recertify" : "readiness";
const priorIdx = process.argv.findIndex((a) => a.startsWith("--prior-period="));
const currentIdx = process.argv.findIndex((a) => a.startsWith("--current-period="));
const priorPeriodId =
  priorIdx >= 0 ? process.argv[priorIdx].split("=")[1] : "DEMO_VALIDATION";
const currentPeriodId = currentIdx >= 0 ? process.argv[currentIdx].split("=")[1] : null;

if (mode === "recertify" && currentPeriodId) {
  const report = runLongitudinalRecertification({
    priorPeriodId,
    currentPeriodId,
    writeReport: true,
  });
  console.log("BRAND_AI_LONGITUDINAL_BENCHMARK_RECERTIFICATION_COMPLETE");
  console.log(JSON.stringify({
    ok: report.ok !== false,
    priorPeriodId: report.priorPeriodId,
    currentPeriodId: report.currentPeriodId,
    readyForCertification: (report.readyForCertification || []).map((c) => c.label),
    frozen: report.frozenBaseline,
  }, null, 2));
} else {
  const report = runLongitudinalRecertificationReadiness({ writeReport: true });
  console.log("BRAND_AI_LONGITUDINAL_BENCHMARK_RECERTIFICATION_READINESS_COMPLETE");
  console.log(JSON.stringify({
    periodArchitecture: report.periodArchitecture?.MEASUREMENT_PERIOD_PRESENT,
    CROSS_PERIOD_DEDUP_RISK: report.CROSS_PERIOD_DEDUP_RISK,
    FIX_REQUIRED: report.FIX_REQUIRED,
    frozen: report.frozenBaseline,
    target6: report.target6Prepared,
    command: report.postWaveRecertification?.COMMAND_OR_MODULE,
  }, null, 2));
}
