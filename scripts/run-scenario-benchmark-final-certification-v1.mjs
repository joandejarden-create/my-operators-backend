#!/usr/bin/env node
import { runScenarioBenchmarkFinalCertification } from "../lib/ai-visibility/competitive-moat/scenario-benchmark-final-certification.js";

const report = runScenarioBenchmarkFinalCertification({ writeReport: true });
console.log("BRAND_AI_SCENARIO_INDEX_FINAL_CERTIFICATION_COMPLETE");
console.log("final:", report.final);
console.log("counts:", report.certificationCounts);
console.log("UI:", report.SCENARIO_BENCHMARK_UI);
for (const c of report.candidates) {
  console.log(
    `- ${c.SUBJECT} ${c.SCENARIO}: index=${c.INDEX} status=${c.FINAL_STATUS} match=${c.INDEX_MATCH}`
  );
}
console.log("wrote reports/ai-visibility/scenario-benchmark-final-certification-v1.json");
