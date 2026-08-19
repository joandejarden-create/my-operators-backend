#!/usr/bin/env node
import { runScenarioBenchmarkCompositionRemediation } from "../lib/ai-visibility/competitive-moat/scenario-benchmark-composition.js";

const report = runScenarioBenchmarkCompositionRemediation({ writeReport: true });
console.log("BRAND_AI_SCENARIO_BENCHMARK_REMEDIATION_COMPLETE");
console.log("final:", report.final);
console.log("next:", report.next);
console.log("policy:", report.benchmarkPolicy.RECOMMENDED_POLICY);
console.log("production:", report.productionClassification);
console.log("fragileProduction:", report.FRAGILE_PRODUCTION);
console.log("providerCalls:", report.providerCalls);
console.log("wrote reports/ai-visibility/scenario-benchmark-composition-v1.json");
