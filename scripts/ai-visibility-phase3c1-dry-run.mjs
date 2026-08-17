#!/usr/bin/env node
/**
 * Phase 3C.1 — Discoverability foundation dry-run (fixture default).
 * Optional: --bounded-live --max-brands=1 for single bounded public fetch.
 */
import { executePhase3c1 } from "../lib/ai-visibility/phase3c1-orchestrator.js";

const args = process.argv.slice(2);
const boundedLive = args.includes("--bounded-live");
const maxArg = args.find((a) => a.startsWith("--max-brands="));
const maxPilotBrands = maxArg ? Number(maxArg.split("=")[1]) : 1;

const report = await executePhase3c1({
  boundedLivePilot: boundedLive,
  maxPilotBrands,
});

console.log("\n=== Phase 3C.1 Foundation ===\n");
console.log(`BUILD_STATUS: ${report.BUILD_STATUS}`);
console.log(`PUBLIC_CHECK_ENGINE_READY: ${report.PUBLIC_CHECK_ENGINE?.READY}`);
console.log(`COMPOSITE_SCORE: ${report.COMPOSITE_SCORE?.ALLOWED}`);
console.log(`PILOT_EXECUTED: ${report.PUBLIC_PILOT?.EXECUTED}`);
console.log(`LIVE_AI_CALLS: ${report.ACTIVITY.LIVE_AI_PROVIDER_CALLS}`);
console.log(`Report: ${report.reportPath}`);
process.exit(report.BUILD_STATUS.includes("PASS") ? 0 : 1);
