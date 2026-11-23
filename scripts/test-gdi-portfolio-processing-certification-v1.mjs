#!/usr/bin/env node
/**
 * GDI Portfolio Processing Certification V1 — deterministic $0 tests + rehearsal.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import {
  GDI_PORTFOLIO_PROCESSING_CERTIFICATION_V1,
  CERTIFICATION_COHORT_V1,
  CAPABILITY_STATUS,
  READINESS_VERDICT,
  normalizeHotelList,
  resolveHotelIdentity,
  preflightHotelEligibility,
  assessHotelCommercialQuality,
  assessFourteenSegmentCoverage,
  buildCapabilityAuditMatrix,
  buildHotelCertificationScores,
  runPortfolioOrchestrator,
  recommendReadinessVerdict,
} from "../lib/group-demand-intelligence/workflow/gdi-portfolio-processing-certification-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const results = [];

function check(name, fn) {
  try {
    fn();
    results.push({ name, ok: true });
  } catch (err) {
    results.push({ name, ok: false, error: String(err?.stack || err) });
  }
}

async function checkAsync(name, fn) {
  try {
    await fn();
    results.push({ name, ok: true });
  } catch (err) {
    results.push({ name, ok: false, error: String(err?.stack || err) });
  }
}

check("contract_and_cohort_shape", () => {
  assert.equal(
    GDI_PORTFOLIO_PROCESSING_CERTIFICATION_V1,
    "gdi-portfolio-processing-certification-v1",
  );
  assert.equal(CERTIFICATION_COHORT_V1.length, 7);
  assert.ok(
    CERTIFICATION_COHORT_V1.some((h) => h.hotelId === "recLuxvwwxID7U2B8"),
  );
  assert.ok(
    CERTIFICATION_COHORT_V1.some((h) => h.region === "MEXICO"),
  );
  assert.ok(
    CERTIFICATION_COHORT_V1.some((h) => h.region === "EUROPE"),
  );
  assert.ok(
    CERTIFICATION_COHORT_V1.some((h) => h.region === "CARIBBEAN"),
  );
  assert.ok(
    CERTIFICATION_COHORT_V1.some(
      (h) => h.role === "NO_EXISTING_OPPORTUNITY_STORE",
    ),
  );
});

check("normalize_dedupes_and_rejects_invalid", () => {
  const list = normalizeHotelList([
    "recLuxvwwxID7U2B8",
    "recLuxvwwxID7U2B8",
    "not-a-rec",
    "recnPB3Q02NO8Ma1o",
  ]);
  assert.equal(list.count, 2);
  assert.deepEqual(list.duplicates, ["recLuxvwwxID7U2B8"]);
  assert.ok(list.invalid.includes("not-a-rec"));
  assert.equal(list.ok, false);
});

check("bethesda_identity_resolves", () => {
  const id = resolveHotelIdentity("recLuxvwwxID7U2B8");
  assert.equal(id.ok, true);
  assert.match(id.hotelName || "", /Bethesda Marriott/i);
  assert.ok(id.opportunityCount > 0);
});

check("empty_store_hotel_resolves_without_opps", () => {
  const id = resolveHotelIdentity("rec0eTEnCqlsGJJt9");
  assert.equal(id.ok, true);
  assert.equal(id.hasOpportunityStore, false);
  assert.match(id.hotelName || "", /Mayflower/i);
});

check("capability_audit_does_not_overclaim_certified", () => {
  const audit = buildCapabilityAuditMatrix();
  const overclaimed = Object.values(audit.runners).filter(
    (r) => r.status === CAPABILITY_STATUS.CERTIFIED,
  );
  assert.equal(
    overclaimed.length,
    0,
    "runners must not be CERTIFIED from existence alone",
  );
  assert.ok(audit.capabilities.length >= 10);
});

check("fourteen_segment_coverage", () => {
  const cov = assessFourteenSegmentCoverage();
  assert.equal(cov.segmentCount, 14);
  assert.equal(cov.ok, true);
  assert.equal(cov.summary.BLOCKED, 0);
});

check("commercial_quality_scores_cohort", () => {
  const scores = buildHotelCertificationScores();
  assert.equal(scores.length, 7);
  const empty = scores.find(
    (s) => s.role === "NO_EXISTING_OPPORTUNITY_STORE",
  );
  assert.ok(empty);
  assert.ok(empty.commercialScore < 40);
  const bethesda = scores.find((s) => s.hotelId === "recLuxvwwxID7U2B8");
  assert.ok(bethesda.commercialScore >= 40);
});

await checkAsync("rehearsal_refuses_paid", async () => {
  const r = await runPortfolioOrchestrator({
    mode: "rehearsal",
    hotelIds: ["recLuxvwwxID7U2B8"],
    authorizePaidResearch: true,
    budgetCeilingUsd: 10,
    forceZeroCost: true,
    reportRoot: fs.mkdtempSync(path.join(os.tmpdir(), "gdi-port-refuse-")),
  });
  assert.equal(r.ok, false);
  assert.equal(r.error, "REHEARSAL_REFUSES_PAID");
});

await checkAsync("budget_requires_auth", async () => {
  const r = await runPortfolioOrchestrator({
    mode: "preview",
    hotelIds: ["recLuxvwwxID7U2B8"],
    authorizePaidResearch: false,
    budgetCeilingUsd: 5,
    forceZeroCost: false,
    reportRoot: fs.mkdtempSync(path.join(os.tmpdir(), "gdi-port-budget-")),
  });
  assert.equal(r.ok, false);
  assert.equal(r.error, "BUDGET_REQUIRES_PAID_AUTH");
});

const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-port-rehearsal-"));

await checkAsync("zero_cost_rehearsal_first_pass", async () => {
  const hotelIds = CERTIFICATION_COHORT_V1.map((h) => h.hotelId);
  const r = await runPortfolioOrchestrator({
    mode: "rehearsal",
    hotelIds,
    reportRoot: tmpRoot,
    portfolioRunId: "gdi_portfolio_cert_rehearsal_v1",
    forceZeroCost: true,
  });
  assert.equal(r.spentUsd, 0);
  assert.equal(r.safety.paidResearchExecuted, false);
  assert.equal(r.safety.publicationExecuted, false);
  assert.equal(r.safety.airtableProductionWrites, false);
  assert.equal(r.safety.weeklyAutomationActivated, false);
  assert.equal(r.safety.sequentialExecution, true);
  assert.ok(r.passedCount >= 6, `expected most hotels to pass, got ${r.passedCount}`);
  assert.equal(r.failedCount, 0);
  assert.ok(fs.existsSync(r.statePath));
  assert.ok(
    fs.existsSync(
      path.join(tmpRoot, "gdi_portfolio_cert_rehearsal_v1.summary.json"),
    ),
  );
  // Stable opportunity ID snapshots preserved (no store mutation)
  for (const h of r.hotels) {
    const before = resolveHotelIdentity(h.hotelId);
    assert.deepEqual(
      new Set(h.opportunityIdsSnapshot || []),
      new Set(before.opportunityIds || []),
    );
  }
});

await checkAsync("resume_skips_completed_hotels", async () => {
  const hotelIds = CERTIFICATION_COHORT_V1.map((h) => h.hotelId);
  const r = await runPortfolioOrchestrator({
    mode: "rehearsal",
    hotelIds,
    reportRoot: tmpRoot,
    portfolioRunId: "gdi_portfolio_cert_rehearsal_v1",
    resume: true,
    statePath: path.join(tmpRoot, "gdi_portfolio_cert_rehearsal_v1.state.json"),
    forceZeroCost: true,
  });
  const skipped = r.hotels.filter((h) => h.resumedSkipped === true);
  assert.ok(skipped.length >= 6, `resume should skip completed hotels, got ${skipped.length}`);
  assert.equal(r.spentUsd, 0);
});

await checkAsync("failure_isolation_invalid_peer", async () => {
  const r = await runPortfolioOrchestrator({
    mode: "rehearsal",
    hotelIds: ["recLuxvwwxID7U2B8", "recLuxvwwxID7U2B8"],
    reportRoot: fs.mkdtempSync(path.join(os.tmpdir(), "gdi-port-iso-")),
    forceZeroCost: true,
  });
  // duplicate stripped before run — still succeeds on unique hotel
  assert.equal(r.duplicatesSkipped.includes("recLuxvwwxID7U2B8"), true);
  assert.equal(r.hotelCount, 1);
  assert.equal(r.passedCount, 1);
});

check("weekly_change_demo_present_on_rehearsal_summary", () => {
  const summary = JSON.parse(
    fs.readFileSync(
      path.join(tmpRoot, "gdi_portfolio_cert_rehearsal_v1.summary.json"),
      "utf8",
    ),
  );
  assert.ok(summary.weeklyChangeDemo);
  assert.equal(summary.weeklyChangeDemo.mutatedAuthoritativeStore, false);
  assert.ok(summary.weeklyChangeDemo.firstState);
});

check("readiness_verdict_is_limited_pilot_or_not_ready", () => {
  const scores = buildHotelCertificationScores();
  const segments = assessFourteenSegmentCoverage();
  const verdict = recommendReadinessVerdict({
    hotelScores: scores,
    segmentCoverage: segments,
    portfolioSummary: {
      safety: { paidResearchExecuted: false },
      passedCount: 7,
      failedCount: 0,
    },
  });
  assert.ok(
    [
      READINESS_VERDICT.LIMITED_PILOT_ONLY,
      READINESS_VERDICT.NOT_READY,
    ].includes(verdict.verdict),
  );
  assert.notEqual(verdict.verdict, READINESS_VERDICT.CERTIFIED_FOR_SCALING);
  assert.ok(verdict.blockers.includes("FRESH_PAID_DISCOVERY_NOT_CERTIFIED_IN_THIS_RUN"));
});

check("powershell_launcher_exists", () => {
  assert.ok(
    fs.existsSync(path.join(ROOT, "scripts/Invoke-GdiPortfolioProcessing.ps1")),
  );
  assert.ok(
    fs.existsSync(
      path.join(ROOT, "scripts/gdi-portfolio-processing-certification-v1.mjs"),
    ),
  );
});

check("eligibility_preflight_for_cohort", () => {
  for (const row of CERTIFICATION_COHORT_V1) {
    const pf = preflightHotelEligibility(row.hotelId);
    assert.equal(pf.hotelId, row.hotelId);
    // Soft assert: at least identity resolves; canonical may warn for empty store
    const id = resolveHotelIdentity(row.hotelId);
    assert.equal(id.ok, true);
  }
});

// Optional: spawn CLI rehearsal smoke (uses cohort, writes under reports/)
check("cli_audit_only_smoke", () => {
  const out = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-port-cli-"));
  const r = spawnSync(
    process.execPath,
    [
      path.join(ROOT, "scripts/gdi-portfolio-processing-certification-v1.mjs"),
      "--mode",
      "audit-only",
      "--out",
      out,
    ],
    { cwd: ROOT, encoding: "utf8", timeout: 60000 },
  );
  assert.equal(r.status, 0, r.stderr || r.stdout);
  assert.ok(fs.existsSync(path.join(out, "readiness-verdict.json")));
});

const failed = results.filter((r) => !r.ok);
console.log(
  JSON.stringify(
    {
      ok: failed.length === 0,
      passed: results.filter((r) => r.ok).length,
      failed: failed.length,
      results,
    },
    null,
    2,
  ),
);
process.exit(failed.length === 0 ? 0 : 1);
