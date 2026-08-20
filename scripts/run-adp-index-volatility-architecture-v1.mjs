#!/usr/bin/env node
import { writeFileSync, mkdirSync } from "fs";
import { join } from "path";
import { loadPropertyProfile, loadAllPeriods } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { runIndexVolatilityArchitectureDecision } from "../lib/ai-demand-positioning/metrics/index-volatility-architecture-v1.js";

const PROPERTY_ID = "adp_waterstone_boca_raton";

function main() {
  const profile = loadPropertyProfile(PROPERTY_ID);
  const periods = loadAllPeriods(PROPERTY_ID);
  const parsed = periods.filter((p) => (p.observations || []).some((o) => o.parsed));
  const period = parsed[parsed.length - 1];
  const scenarios = buildScenarioUniverse(profile);
  const report = runIndexVolatilityArchitectureDecision({
    period,
    scenarios,
    propertyProfile: profile,
    allPeriods: periods,
  });
  const dir = join(process.cwd(), "reports/ai-demand-positioning");
  mkdirSync(dir, { recursive: true });
  const out = join(dir, "index-volatility-architecture-decision-v1.json");
  writeFileSync(out, JSON.stringify(report, null, 2));
  console.log("Wrote", out);
  console.log("FINAL", report.final);
  console.log("NEXT", report.next);
  console.log("STRATEGY", report.indexStrategy.choice);
  console.log("DISPLAY", report.recommendedExecutiveRepresentation.choice);
  console.log("WAVE", report.nextMeasurementWave.RUN_NEW_WAVE_NOW, report.nextMeasurementWave.EXPECTED_VALUE);
}

main();
