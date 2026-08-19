#!/usr/bin/env node
import { runScenarioBenchmarkValidation } from "../lib/ai-visibility/competitive-moat/scenario-benchmark-validation.js";

const report = runScenarioBenchmarkValidation({ writeReport: true });
console.log("BRAND_AI_SCENARIO_BENCHMARK_VALIDATION_COMPLETE");
console.log("final:", report.final);
console.log("next:", report.next);
console.log("recalc:", report.recalculation);
console.log("production:", report.productionClassification);
console.log("providerCalls:", report.providerCalls);
console.log("wrote reports/ai-visibility/scenario-benchmark-validation-v1.json");
