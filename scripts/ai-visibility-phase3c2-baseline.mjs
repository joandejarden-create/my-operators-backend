#!/usr/bin/env node
/**
 * Phase 3C.2 — Discoverability public baseline.
 * Default: preflight + fixture baseline (no network).
 * Optional: --bounded-live --max-brands=N for live HTTP checks.
 * Optional: --preflight-only
 * Optional: --brand-ids=id1,id2 (priority subset; merges into latest report)
 */
import {
  buildPhase3c2Preflight,
  executePhase3c2,
} from "../lib/ai-visibility/phase3c2-orchestrator.js";

const args = process.argv.slice(2);
const preflightOnly = args.includes("--preflight-only");
const boundedLive = args.includes("--bounded-live");
const maxArg = args.find((a) => a.startsWith("--max-brands="));
const maxBrands = maxArg ? Number(maxArg.split("=")[1]) : undefined;
const brandIdsArg = args.find((a) => a.startsWith("--brand-ids="));
const brandIds = brandIdsArg
  ? brandIdsArg
      .slice("--brand-ids=".length)
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean)
  : undefined;

const preflight = buildPhase3c2Preflight({ brandIds, boundedLive });
console.log("\n=== Phase 3C.2 Preflight ===\n");
console.log(`DISCOVERABILITY_ELIGIBLE_BRANDS: ${preflight.DISCOVERABILITY_ELIGIBLE_BRANDS}`);
console.log(`ELIGIBLE: ${(preflight.eligibleBrandNames || []).join(", ") || "—"}`);
console.log(`GOVERNED_URLS: ${preflight.GOVERNED_URLS}`);
console.log(`MISSING_GOVERNED_WEBSITE: ${preflight.MISSING_GOVERNED_WEBSITE}`);
console.log(`EXPECTED_PUBLIC_CHECKS: ${preflight.EXPECTED_PUBLIC_CHECKS}`);
console.log(`ESTIMATED_COST: ${preflight.ESTIMATED_COST}`);
console.log(`CHECK_MODE: ${preflight.CHECK_MODE}`);
console.log(`BLOCKER: ${preflight.BLOCKER || "none"}`);
console.log(`READY_TO_EXECUTE: ${preflight.READY_TO_EXECUTE ? "YES" : "NO"}`);
if (preflight.governedUrlSamples?.length) {
  console.log("GOVERNED_URL samples:");
  for (const s of preflight.governedUrlSamples.slice(0, 16)) {
    console.log(`  - ${s.brandId} · ${s.slotId} · ${s.url}`);
  }
}

if (preflightOnly) {
  process.exit(preflight.READY_TO_EXECUTE ? 0 : 1);
}

if (!preflight.READY_TO_EXECUTE) {
  console.error("Preflight not ready — aborting execute.");
  process.exit(1);
}

const report = await executePhase3c2({
  boundedLive,
  maxBrands,
  brandIds,
  mergeWithLatest: true,
  forceFixture: true,
});

console.log("\n=== Phase 3C.2 Execute ===\n");
console.log(`LIVE_BASELINE_EXECUTED: ${report.LIVE_BASELINE_EXECUTED}`);
console.log(`MODE: ${report.MODE}`);
console.log(`BRANDS_CHECKED_THIS_RUN: ${report.BRANDS_CHECKED_THIS_RUN}`);
console.log(`BRANDS_IN_LATEST: ${report.BRANDS_CHECKED}`);
console.log(`ACCESSIBLE: ${report.ACCESSIBLE}`);
console.log(`CHECK_FAILED: ${report.CHECK_FAILED}`);
console.log(`SOURCE_NOT_CONFIGURED: ${report.SOURCE_NOT_CONFIGURED}`);
console.log(`ARBITRARY_SCORE: ${report.ARBITRARY_SCORE}`);
console.log(`Report: ${report.reportPath}`);
process.exit(report.ok ? 0 : 1);
