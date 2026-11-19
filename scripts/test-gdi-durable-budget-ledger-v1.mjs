#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  resolveDurableLedgerPath,
  loadOrCreatePaidBudgetLedger,
  reserveQuotedCall,
  settleReservedCall,
  remainingBudgetUsd,
} from "../lib/group-demand-intelligence/research/gdi-authorized-paid-budget-ledger-v1.js";

const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-durable-ledger-"));

const unauthorized = resolveDurableLedgerPath("GDI_UNAUTHORIZED", { root: tmpRoot });
assert.equal(unauthorized.ok, false);

const empty = resolveDurableLedgerPath("", { root: tmpRoot });
assert.equal(empty.ok, false);

const authA = "GDI_SLICE1_TEST_AUTH_A";
const authB = "GDI_SLICE1_TEST_AUTH_B";

const run1Out = path.join(tmpRoot, "runs", "out-a-1", "paid-budget-ledger.json");
const run2Out = path.join(tmpRoot, "runs", "out-a-2", "paid-budget-ledger.json");

const first = loadOrCreatePaidBudgetLedger({
  authorizationId: authA,
  hardCeilingUsd: 1.0,
  priorSpendUsd: 0,
  root: tmpRoot,
  snapshotPath: run1Out,
});
assert.equal(first.ok, true);
assert.ok(first.durablePath.includes("paid-budget-ledgers"));
assert.ok(fs.existsSync(first.durablePath));
assert.ok(fs.existsSync(run1Out));

const reserve = reserveQuotedCall(first.ledger, { quotedMaxUsd: 0.25 });
assert.equal(reserve.ok, true);
settleReservedCall(first.ledger, {
  quotedMaxUsd: 0.25,
  actualCostUsd: 0.2,
  stage: "DEEP_RESEARCH",
  opportunityId: "opp_test_1",
});
assert.equal(Number(first.ledger.spentUsd), 0.2);

const second = loadOrCreatePaidBudgetLedger({
  authorizationId: authA,
  hardCeilingUsd: 1.0,
  root: tmpRoot,
  snapshotPath: run2Out,
});
assert.equal(second.ok, true);
assert.equal(second.durablePath, first.durablePath);
assert.equal(Number(second.ledger.spentUsd), 0.2);
assert.equal(Number(second.ledger.reservedUsd), 0);
assert.ok(remainingBudgetUsd(second.ledger) < 0.81);

const raiseRefused = loadOrCreatePaidBudgetLedger({
  authorizationId: authA,
  hardCeilingUsd: 50,
  root: tmpRoot,
  allowCeilingRaise: false,
});
assert.equal(Number(raiseRefused.ledger.hardCeilingUsd), 1.0);
assert.equal(raiseRefused.ledger.ceilingRaiseRefused, true);

const other = loadOrCreatePaidBudgetLedger({
  authorizationId: authB,
  hardCeilingUsd: 2.0,
  root: tmpRoot,
});
assert.equal(Number(other.ledger.spentUsd), 0);
assert.notEqual(other.durablePath, first.durablePath);

const missingAuth = loadOrCreatePaidBudgetLedger({
  authorizationId: "GDI_UNAUTHORIZED",
  hardCeilingUsd: 1,
  root: tmpRoot,
});
assert.equal(missingAuth.ok, false);

fs.rmSync(tmpRoot, { recursive: true, force: true });
console.log("test-gdi-durable-budget-ledger-v1: PASS");
