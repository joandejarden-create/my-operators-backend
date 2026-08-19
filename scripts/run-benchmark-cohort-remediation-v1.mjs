#!/usr/bin/env node
/**
 * Run benchmark cohort remediation V1 — stored corpus only, no provider calls.
 */
import { runBenchmarkCohortRemediation } from "../lib/ai-visibility/competitive-moat/benchmark-cohort-remediation.js";

const report = runBenchmarkCohortRemediation({ writeReport: true });
console.log("BRAND_AI_BENCHMARK_COHORT_REMEDIATION_COMPLETE");
console.log("final:", report.final);
console.log("next:", report.next);
console.log("eligible:", report.benchmarkEligibleCount);
console.log("VALID/LIMITED/SUPPRESSED:", report.scenarioIndexReadiness);
console.log("symmetry unjustified:", report.symmetry.ASYMMETRIC_UNJUSTIFIED);
console.log("providerCalls:", report.providerCalls);
console.log("wrote reports/ai-visibility/benchmark-cohort-remediation-v1.json");
