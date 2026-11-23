#!/usr/bin/env node
/**
 * V1.1 paid-pilot controls — zero-spend tests.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  PILOT_AGGREGATE_CEILING_USD,
  PILOT_PER_HOTEL_CEILING_USD,
  PILOT_HOTELS,
  verifyDualSpendCaps,
  buildProviderCallPlan,
  runPortfolioPaidPilot,
} from "../lib/group-demand-intelligence/workflow/gdi-portfolio-cert-v1-1-paid-pilot-v1.js";
import {
  loadOrCreatePaidBudgetLedger,
  remainingBudgetUsd,
} from "../lib/group-demand-intelligence/research/gdi-authorized-paid-budget-ledger-v1.js";

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

const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-v11-caps-"));

check("pilot_ceilings", () => {
  assert.equal(PILOT_AGGREGATE_CEILING_USD, 25);
  assert.equal(PILOT_PER_HOTEL_CEILING_USD, 12.5);
  assert.equal(PILOT_HOTELS.length, 2);
  assert.equal(PILOT_HOTELS[0].hotelId, "recLuxvwwxID7U2B8");
  assert.equal(PILOT_HOTELS[1].hotelId, "recrPQcZg7SFARRb2");
  const sum = PILOT_HOTELS.reduce((s, h) => s + h.perHotelCeilingUsd, 0);
  assert.equal(sum, 25);
});

check("dual_spend_caps_init_durable", () => {
  const caps = verifyDualSpendCaps({ root: tmpRoot });
  assert.equal(caps.ok, true);
  assert.equal(caps.sumPerHotelCaps, 25);
  assert.equal(caps.hotelLedgers.length, 2);
  for (const h of caps.hotelLedgers) {
    assert.equal(h.hardCeilingUsd, 12.5);
    assert.ok(h.durablePath.includes("paid-budget-ledgers"));
    assert.ok(fs.existsSync(h.durablePath));
  }
});

check("ceiling_raise_refused", () => {
  const id = PILOT_HOTELS[0].authorizationId;
  loadOrCreatePaidBudgetLedger({
    authorizationId: id,
    hardCeilingUsd: 12.5,
    root: tmpRoot,
    resume: true,
  });
  const raised = loadOrCreatePaidBudgetLedger({
    authorizationId: id,
    hardCeilingUsd: 50,
    root: tmpRoot,
    resume: true,
    allowCeilingRaise: false,
  });
  assert.equal(raised.ledger.hardCeilingUsd, 12.5);
  assert.equal(raised.ledger.ceilingRaiseRefused, true);
});

check("provider_call_plan_zero_spend", () => {
  const plan = buildProviderCallPlan({ root: tmpRoot });
  assert.equal(plan.safety.surfe, false);
  assert.equal(plan.safety.airtableApply, false);
  assert.equal(plan.safety.publication, false);
  assert.equal(plan.hotelPlans.length, 2);
  assert.ok(Array.isArray(plan.fourteenSegmentIds));
  assert.equal(plan.fourteenSegmentIds.length, 14);
  // Env may or may not allow ok=true; blockers must be structured
  assert.ok(Array.isArray(plan.blockers));
});

await checkAsync("execute_paid_false_does_not_spend", async () => {
  const r = await runPortfolioPaidPilot({
    executePaid: false,
    reportRoot: path.join(tmpRoot, "preflight-out"),
  });
  assert.equal(r.mode, "preflight");
  assert.equal(r.spentUsd, 0);
});

check("powershell_modes_present", () => {
  const ps1 = fs.readFileSync(
    path.join(ROOT, "scripts/Invoke-GdiPortfolioProcessing.ps1"),
    "utf8",
  );
  assert.match(ps1, /PaidPilotPreflight/);
  assert.match(ps1, /PaidPilotExecute/);
  assert.match(ps1, /PerHotelBudgetUSD/);
  assert.match(ps1, /FOUNDER_GDI_PORTFOLIO_CERT_V1_1_PAID_25/);
});

const failed = results.filter((r) => !r.ok);
console.log(
  JSON.stringify(
    {
      ok: failed.length === 0,
      passed: results.filter((r) => r.ok).length,
      failed: failed.length,
      results,
      safety: { paidCalls: false },
    },
    null,
    2,
  ),
);
process.exit(failed.length === 0 ? 0 : 1);
