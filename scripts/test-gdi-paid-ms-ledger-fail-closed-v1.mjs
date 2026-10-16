#!/usr/bin/env node
/**
 * Authorization ledger must not silently bootstrap empty when missing.
 * Wrong-path / missing-ledger fail closed for paid envelope.
 * Accounting hold enforced for Parallel + Surfe; invalid hold fails closed.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  readParallelBudgetLedger,
  paidMultisegmentAuthorizationLedgerPath,
  remainingParallelBudgetUsd,
  accountingHoldUsd,
  reserveParallelBudget,
  commitParallelBudgetReservation,
  releaseParallelBudgetReservation,
  convertAccountingHoldToIncurred,
  releaseAccountingHold,
  increaseAccountingHold,
} from "../lib/group-demand-intelligence/parallel/budget-ledger-v1.js";
import {
  remainingSharedBudgetUsd,
  reserveSurfeSpend,
} from "../lib/group-demand-intelligence/workflow/surfe-usage-ledger-v1.js";

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-ledger-fc-"));
const paidEnvFlags = {
  GDI_PARALLEL_ENABLED: "1",
  GDI_PARALLEL_KILL_SWITCH: "0",
  GDI_PARALLEL_BUDGET_REQUIRE_FLAG: "0",
};

function writeAuthLedger(dir, doc) {
  const holdPath = path.join(dir, "paid-multisegment-budget", "authorization-ledger-v1.json");
  fs.mkdirSync(path.dirname(holdPath), { recursive: true });
  fs.writeFileSync(holdPath, JSON.stringify(doc, null, 2) + "\n");
  return holdPath;
}

// Missing authorization ledger → fail closed
{
  const env = { GDI_DATA_ROOT: tmp };
  const ledgerPath = paidMultisegmentAuthorizationLedgerPath(env);
  assert.equal(fs.existsSync(ledgerPath), false);
  let threw = null;
  try {
    readParallelBudgetLedger({
      ledgerPath,
      authorizationEnvelope: true,
      budgetUsd: 11,
      env,
    });
  } catch (err) {
    threw = err;
  }
  assert.ok(threw, "must throw");
  assert.equal(threw.code, "PAID_MS_LEDGER_MISSING");
  assert.equal(fs.existsSync(ledgerPath), false, "must not create empty file");
  console.log("PASS missing_auth_ledger_fail_closed");
}

// Explicit bootstrap allowed only when opted in
{
  const env = { GDI_DATA_ROOT: path.join(tmp, "boot") };
  const ledgerPath = paidMultisegmentAuthorizationLedgerPath(env);
  const ledger = readParallelBudgetLedger({
    ledgerPath,
    authorizationEnvelope: true,
    allowBootstrapEmpty: true,
    budgetUsd: 11,
    env,
  });
  assert.equal(ledger.bootstrappedEmpty, true);
  assert.equal(ledger.budgetUsd, 11);
  assert.equal((ledger.entries || []).length, 0);
  assert.ok(fs.existsSync(ledgerPath));
  console.log("PASS explicit_bootstrap_empty");
}

// Wrong path (distinct tmp) does not invent spend from another tree
{
  const envA = { GDI_DATA_ROOT: path.join(tmp, "tree-a") };
  const envB = { GDI_DATA_ROOT: path.join(tmp, "tree-b") };
  const pathA = paidMultisegmentAuthorizationLedgerPath(envA);
  readParallelBudgetLedger({
    ledgerPath: pathA,
    authorizationEnvelope: true,
    allowBootstrapEmpty: true,
    budgetUsd: 11,
    env: envA,
  });
  const spent = {
    version: "gdi-parallel-budget-ledger-v1",
    currency: "USD",
    budgetUsd: 11,
    reservedUsd: 0,
    estimatedSpentUsd: 2.85,
    actualBilledUsd: null,
    entries: [
      {
        reservationId: "rsv_test_a",
        label: "paid_ms:contact_wisedocs_may",
        estimatedUsd: 0.15,
        status: "spent",
        providerRunId: "trun_test_a",
      },
    ],
    updatedAt: new Date().toISOString(),
  };
  fs.writeFileSync(pathA, JSON.stringify(spent, null, 2) + "\n");

  let threw = null;
  try {
    remainingSharedBudgetUsd({ env: envB });
  } catch (err) {
    threw = err;
  }
  assert.ok(threw, "tree-b missing ledger must fail");
  assert.equal(threw.code, "PAID_MS_LEDGER_MISSING");

  const remA = remainingSharedBudgetUsd({ env: envA });
  assert.equal(Number(remA.toFixed(2)), 8.15);
  console.log("PASS wrong_path_isolation");
}

// Non-auth parallel ledger may still create (historical pilot path)
{
  const pilotPath = path.join(tmp, "parallel-budget", "ledger.json");
  const ledger = readParallelBudgetLedger({
    ledgerPath: pilotPath,
    budgetUsd: 2,
    env: {},
  });
  assert.equal(ledger.budgetUsd, 2);
  assert.ok(fs.existsSync(pilotPath));
  console.log("PASS pilot_ledger_may_bootstrap");
}

// Accounting hold reduces available and is not treated as reservation/incurred
{
  const holdPath = writeAuthLedger(path.join(tmp, "hold-tree"), {
    version: "gdi-parallel-budget-ledger-v1",
    currency: "USD",
    budgetUsd: 11,
    reservedUsd: 0,
    estimatedSpentUsd: 5.25,
    accountingHoldUsd: 1.25,
    actualBilledUsd: null,
    entries: [],
    updatedAt: new Date().toISOString(),
  });
  const ledger = readParallelBudgetLedger({
    ledgerPath: holdPath,
    authorizationEnvelope: true,
    budgetUsd: 11,
    env: {},
  });
  assert.equal(accountingHoldUsd(ledger), 1.25);
  assert.equal(Number(remainingParallelBudgetUsd(ledger).toFixed(2)), 4.5);
  assert.equal(Number(ledger.reservedUsd || 0), 0);
  assert.equal(Number(ledger.estimatedSpentUsd || 0), 5.25);

  let blocked = null;
  try {
    reserveParallelBudget({
      label: "test_hold_block",
      estimatedUsd: 4.51,
      reservationId: "rsv_test_hold_block",
      jobKey: "test:hold_block",
      ledgerPath: holdPath,
      env: paidEnvFlags,
    });
  } catch (err) {
    blocked = err;
  }
  assert.ok(blocked, "hold must block over-available reserve");
  assert.equal(blocked.code, "PARALLEL_BUDGET_EXHAUSTED");
  assert.equal(Number(Number(blocked.remainingUsd).toFixed(2)), 4.5);
  console.log("PASS parallel_reservation_exceeds_available_after_hold");
}

// Surfe reservation exceeding available after hold (dedicated Surfe path)
{
  const root = path.join(tmp, "surfe-hold");
  writeAuthLedger(root, {
    version: "gdi-parallel-budget-ledger-v1",
    currency: "USD",
    budgetUsd: 11,
    reservedUsd: 0,
    estimatedSpentUsd: 5.25,
    accountingHoldUsd: 1.25,
    actualBilledUsd: null,
    entries: [],
    updatedAt: new Date().toISOString(),
  });
  const env = { ...paidEnvFlags, GDI_DATA_ROOT: root };
  const rem = remainingSharedBudgetUsd({ env });
  assert.equal(Number(rem.toFixed(2)), 4.5);
  const blocked = reserveSurfeSpend({
    estimatedUsd: 4.51,
    person: { name: "Test Person", organization: "Test Org", domain: "test.example" },
    hotelId: "recTest",
    requestedChannels: ["email"],
    env,
    label: "surfe_hold_block_test",
  });
  assert.equal(blocked.ok, false);
  assert.equal(blocked.blocked, true);
  assert.equal(blocked.reason, "PARALLEL_BUDGET_EXHAUSTED");
  assert.equal(Number(Number(blocked.remainingUsd).toFixed(2)), 4.5);
  console.log("PASS surfe_reservation_exceeds_available_after_hold");
}

// Invalid / negative hold fail closed
{
  for (const bad of [-0.01, Number.NaN, Number.POSITIVE_INFINITY, "abc"]) {
    let threw = null;
    try {
      accountingHoldUsd({ accountingHoldUsd: bad });
    } catch (err) {
      threw = err;
    }
    assert.ok(threw, `must reject ${String(bad)}`);
    assert.equal(threw.code, "ACCOUNTING_HOLD_INVALID");
  }
  // Missing hold field → 0 for valid legacy ledger
  assert.equal(accountingHoldUsd({ budgetUsd: 11, estimatedSpentUsd: 1 }), 0);
  console.log("PASS invalid_negative_hold_fail_closed");
}

// Hold persistence through reserve / commit / release
{
  const holdPath = writeAuthLedger(path.join(tmp, "hold-persist"), {
    version: "gdi-parallel-budget-ledger-v1",
    currency: "USD",
    budgetUsd: 11,
    reservedUsd: 0,
    estimatedSpentUsd: 5.25,
    accountingHoldUsd: 1.25,
    actualBilledUsd: null,
    entries: [],
    updatedAt: new Date().toISOString(),
  });
  const env = paidEnvFlags;
  const rsv = reserveParallelBudget({
    label: "persist_hold",
    estimatedUsd: 0.25,
    reservationId: "rsv_persist_hold",
    jobKey: "test:persist_hold",
    ledgerPath: holdPath,
    env,
  });
  assert.equal(rsv.ok, true);
  let ledger = readParallelBudgetLedger({
    ledgerPath: holdPath,
    authorizationEnvelope: true,
    env: {},
  });
  assert.equal(accountingHoldUsd(ledger), 1.25);
  assert.equal(Number(ledger.reservedUsd.toFixed(2)), 0.25);
  assert.equal(Number(remainingParallelBudgetUsd(ledger).toFixed(2)), 4.25);

  commitParallelBudgetReservation("rsv_persist_hold", { ledgerPath: holdPath, env });
  ledger = readParallelBudgetLedger({
    ledgerPath: holdPath,
    authorizationEnvelope: true,
    env: {},
  });
  assert.equal(accountingHoldUsd(ledger), 1.25);
  assert.equal(Number(ledger.reservedUsd.toFixed(2)), 0);
  assert.equal(Number(ledger.estimatedSpentUsd.toFixed(2)), 5.5);
  assert.equal(Number(remainingParallelBudgetUsd(ledger).toFixed(2)), 4.25);

  // Fresh reservation then release — hold still present
  const holdPath2 = writeAuthLedger(path.join(tmp, "hold-persist-rel"), {
    version: "gdi-parallel-budget-ledger-v1",
    currency: "USD",
    budgetUsd: 11,
    reservedUsd: 0,
    estimatedSpentUsd: 5.25,
    accountingHoldUsd: 1.25,
    actualBilledUsd: null,
    entries: [],
    updatedAt: new Date().toISOString(),
  });
  reserveParallelBudget({
    label: "persist_hold_rel",
    estimatedUsd: 0.25,
    reservationId: "rsv_persist_hold_rel",
    jobKey: "test:persist_hold_rel",
    ledgerPath: holdPath2,
    env,
  });
  releaseParallelBudgetReservation("rsv_persist_hold_rel", {
    ledgerPath: holdPath2,
    env,
    reason: "test",
  });
  ledger = readParallelBudgetLedger({
    ledgerPath: holdPath2,
    authorizationEnvelope: true,
    env: {},
  });
  assert.equal(accountingHoldUsd(ledger), 1.25);
  assert.equal(Number(remainingParallelBudgetUsd(ledger).toFixed(2)), 4.5);
  console.log("PASS hold_persistence_through_reserve_commit_release");
}

// Idempotent reconciliation: release overlap + increase unmatched; convert hold
{
  const holdPath = writeAuthLedger(path.join(tmp, "hold-recon"), {
    version: "gdi-parallel-budget-ledger-v1",
    currency: "USD",
    budgetUsd: 11,
    reservedUsd: 0,
    estimatedSpentUsd: 5.25,
    accountingHoldUsd: 1.25,
    actualBilledUsd: null,
    entries: [],
    updatedAt: new Date().toISOString(),
  });

  const rel1 = releaseAccountingHold({
    amountUsd: 1.0,
    releaseId: "rel_lmc_surfe_already_incurred_v1",
    reason: "Four LMC Surfe already in estimatedSpent",
    ledgerPath: holdPath,
  });
  assert.equal(rel1.idempotent, false);
  assert.equal(Number(rel1.ledger.accountingHoldUsd.toFixed(2)), 0.25);

  const rel2 = releaseAccountingHold({
    amountUsd: 1.0,
    releaseId: "rel_lmc_surfe_already_incurred_v1",
    reason: "Four LMC Surfe already in estimatedSpent",
    ledgerPath: holdPath,
  });
  assert.equal(rel2.idempotent, true);
  assert.equal(Number(rel2.ledger.accountingHoldUsd.toFixed(2)), 0.25);
  assert.equal(Number(rel2.ledger.estimatedSpentUsd.toFixed(2)), 5.25);

  const inc1 = increaseAccountingHold({
    amountUsd: 1.0,
    holdId: "hold_scaling_wisedocs_shipyard_unmatched_v1",
    reason: "EXECUTED_UNMATCHED Surfe claims",
    ledgerPath: holdPath,
  });
  assert.equal(inc1.idempotent, false);
  assert.equal(Number(inc1.ledger.accountingHoldUsd.toFixed(2)), 1.25);

  const inc2 = increaseAccountingHold({
    amountUsd: 1.0,
    holdId: "hold_scaling_wisedocs_shipyard_unmatched_v1",
    reason: "EXECUTED_UNMATCHED Surfe claims",
    ledgerPath: holdPath,
  });
  assert.equal(inc2.idempotent, true);
  assert.equal(Number(inc2.ledger.accountingHoldUsd.toFixed(2)), 1.25);

  // Convert hold → incurred must not double-deduct if re-run
  const conv1 = convertAccountingHoldToIncurred({
    amountUsd: 0.25,
    conversionId: "conv_fifth_lmc_surfe_v1",
    label: "fifth_lmc_surfe",
    ledgerPath: holdPath,
  });
  assert.equal(conv1.idempotent, false);
  assert.equal(Number(conv1.ledger.accountingHoldUsd.toFixed(2)), 1.0);
  assert.equal(Number(conv1.ledger.estimatedSpentUsd.toFixed(2)), 5.5);

  const conv2 = convertAccountingHoldToIncurred({
    amountUsd: 0.25,
    conversionId: "conv_fifth_lmc_surfe_v1",
    label: "fifth_lmc_surfe",
    ledgerPath: holdPath,
  });
  assert.equal(conv2.idempotent, true);
  assert.equal(Number(conv2.ledger.accountingHoldUsd.toFixed(2)), 1.0);
  assert.equal(Number(conv2.ledger.estimatedSpentUsd.toFixed(2)), 5.5);
  console.log("PASS idempotent_reconciliation_and_hold_conversion");
}

console.log("OK gdi-paid-ms-ledger-fail-closed-v1");
