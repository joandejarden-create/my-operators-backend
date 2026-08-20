#!/usr/bin/env node
import { writeFileSync, mkdirSync } from "fs";
import { join } from "path";
import { loadPropertyProfile, loadAllPeriods } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { runScenarioDensityAndBenchmarkRateArchitecture } from "../lib/ai-demand-positioning/metrics/scenario-density-benchmark-rate-architecture-v1.js";

const PROPERTY_ID = "adp_waterstone_boca_raton";

function main() {
  const profile = loadPropertyProfile(PROPERTY_ID);
  const periods = loadAllPeriods(PROPERTY_ID);
  const parsed = periods.filter((p) => (p.observations || []).some((o) => o.parsed));
  const period = parsed[parsed.length - 1];
  const scenarios = buildScenarioUniverse(profile);
  const report = runScenarioDensityAndBenchmarkRateArchitecture({
    period,
    scenarios,
    propertyProfile: profile,
    allPeriods: periods,
  });
  const dir = join(process.cwd(), "reports/ai-demand-positioning");
  mkdirSync(dir, { recursive: true });
  const out = join(dir, "scenario-density-core-benchmark-rate-architecture-v1.json");
  writeFileSync(out, JSON.stringify(report, null, 2));
  console.log("Wrote", out);
  console.log("FINAL", report.final);
  console.log("NEXT", report.next);
  console.log("LIVE_SCENARIOS", report.liveScenarioCount);
  console.log("FUTURE_TOTAL", report.futureMeasurementWave.NEW_TOTAL_SCENARIOS);
}

main();
