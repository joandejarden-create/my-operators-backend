#!/usr/bin/env node
/**
 * Authorization ledger must not silently bootstrap empty when missing.
 * Wrong-path / missing-ledger fail closed for paid envelope.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  readParallelBudgetLedger,
  paidMultisegmentAuthorizationLedgerPath,
  remainingParallelBudgetUsd,
} from "../lib/group-demand-intelligence/parallel/budget-ledger-v1.js";
import { remainingSharedBudgetUsd } from "../lib/group-demand-intelligence/workflow/surfe-usage-ledger-v1.js";

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-ledger-fc-"));

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
  // Write spent entry only in tree-a
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

console.log("OK gdi-paid-ms-ledger-fail-closed-v1");
