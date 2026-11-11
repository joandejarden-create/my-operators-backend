/**
 * Benchmark-only attempt store. Not Airtable. Not the opportunity file.
 */

import fs from "node:fs";
import path from "node:path";
import { createHash, randomBytes } from "node:crypto";
import { fileURLToPath } from "node:url";
import { withParallelBudgetLock } from "../parallel/budget-ledger-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

export const BENCHMARK_EVIDENCE_VERSION = "gdi-benchmark-evidence-ledger-v1";

const TERMINAL = new Set([
  "succeeded",
  "failed",
  "refused",
  "duplicate",
]);

export function benchmarkEvidenceLedgerPath(env = process.env) {
  if (env.GDI_BENCHMARK_EVIDENCE_LEDGER_PATH) {
    return path.resolve(env.GDI_BENCHMARK_EVIDENCE_LEDGER_PATH);
  }
  const root = env.GDI_DATA_ROOT
    ? path.resolve(env.GDI_DATA_ROOT)
    : path.join(ROOT, "data/group-demand-intelligence");
  return path.join(root, "benchmark-provider-evidence", "cohort-a-attempts.json");
}

export function sha256Text(value) {
  return createHash("sha256").update(String(value ?? ""), "utf8").digest("hex");
}

export function normalizeEvidenceText(value) {
  return String(value ?? "").replace(/\s+/g, " ").trim();
}

export function benchmarkRequestFingerprint(parts = {}) {
  const keys = Object.keys(parts).sort();
  const stable = {};
  for (const key of keys) stable[key] = parts[key];
  return sha256Text(JSON.stringify(stable));
}

function emptyLedger() {
  return {
    version: BENCHMARK_EVIDENCE_VERSION,
    attempts: [],
    updatedAt: new Date().toISOString(),
  };
}

export function readBenchmarkEvidenceLedger({ ledgerPath, createIfMissing = false } = {}) {
  const resolved = ledgerPath || benchmarkEvidenceLedgerPath();
  if (!fs.existsSync(resolved)) {
    if (!createIfMissing) {
      return { ...emptyLedger(), path: resolved, missing: true };
    }
    fs.mkdirSync(path.dirname(resolved), { recursive: true });
    const fresh = emptyLedger();
    fs.writeFileSync(resolved, JSON.stringify(fresh, null, 2) + "\n");
    return { ...fresh, path: resolved };
  }
  const ledger = JSON.parse(fs.readFileSync(resolved, "utf8"));
  return { ...ledger, path: resolved };
}

function writeUnlocked(ledgerPath, ledger) {
  ledger.updatedAt = new Date().toISOString();
  const tmp = `${ledgerPath}.${process.pid}.${Date.now()}.tmp`;
  const copy = { ...ledger };
  delete copy.path;
  delete copy.missing;
  fs.writeFileSync(tmp, JSON.stringify(copy, null, 2) + "\n");
  fs.renameSync(tmp, ledgerPath);
}

export function findBenchmarkAttempt(ledger, requestFingerprint) {
  const rows = ledger.attempts || [];
  for (let i = rows.length - 1; i >= 0; i -= 1) {
    if (rows[i].requestFingerprint === requestFingerprint) return rows[i];
  }
  return null;
}

export function isTerminalBenchmarkAttempt(attempt) {
  return Boolean(attempt && TERMINAL.has(attempt.status));
}

export function saveBenchmarkAttempt(attempt, opts = {}) {
  const resolved = opts.ledgerPath || benchmarkEvidenceLedgerPath(opts.env || process.env);
  return withParallelBudgetLock(resolved, () => {
    const ledger = readBenchmarkEvidenceLedger({
      ledgerPath: resolved,
      createIfMissing: true,
    });
    const attemptId =
      attempt.attemptId ||
      `bat_${Date.now().toString(36)}_${randomBytes(3).toString("hex")}`;
    const row = { ...attempt, attemptId };
    const index = (ledger.attempts || []).findIndex((item) => item.attemptId === attemptId);
    if (index >= 0) ledger.attempts[index] = { ...ledger.attempts[index], ...row };
    else ledger.attempts.push(row);
    writeUnlocked(resolved, ledger);
    return row;
  });
}

export function listOpenBenchmarkAttempts(opts = {}) {
  const ledger = readBenchmarkEvidenceLedger({
    ledgerPath: opts.ledgerPath || benchmarkEvidenceLedgerPath(opts.env || process.env),
  });
  return (ledger.attempts || []).filter(
    (attempt) => attempt.status === "reserved" || attempt.status === "in_flight"
  );
}
