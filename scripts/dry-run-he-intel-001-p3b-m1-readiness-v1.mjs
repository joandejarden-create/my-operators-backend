#!/usr/bin/env node
/**
 * P3B-M1 readiness dry-run — mocks only; refuses live paid spend.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import {
  runP3BM1ReadinessValidation,
  buildP3BM1PlannedOperations,
  buildProposedExecutionCommand,
  probeConfiguredProviderAvailability,
} from "../lib/hotel-intelligence/research-engine-v1/benchmark-p3b-m1/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
for (const p of [
  path.join(ROOT, ".env"),
  "C:/Dev/dealality/my-operators-backend/.env",
]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p });
    break;
  }
}

const report = await runP3BM1ReadinessValidation({ memoryOnly: true });
const summary = {
  ok: report.ok,
  phase: report.phase,
  contract: report.contract,
  paidExecutionAuthorized: report.paidExecutionAuthorized,
  founderAuthorizationGate: report.founderAuthorizationGate,
  availability: probeConfiguredProviderAvailability(),
  plannedOperations: buildP3BM1PlannedOperations(),
  proposedExecutionCommand: buildProposedExecutionCommand(),
  atomicBudget: {
    hardCapUsd: report.atomicBudget.hardCapUsd,
    perHotelCapUsd: report.atomicBudget.perHotelCapUsd,
    refusedWithoutAuth: report.atomicBudget.refusedWithoutAuth,
    resume: report.atomicBudget.resume,
    sharedCapExhausted: report.atomicBudget.sharedCapExhausted,
    canParallelBypassSharedBudget:
      report.atomicBudget.snapshot.canParallelBypassSharedBudget,
  },
  cohort: report.cohort,
  guarantees: report.guarantees,
  leakageViolations: report.leakageViolations,
};

const out = path.join(
  ROOT,
  "reports/hotel-explorer/HE-INTEL-001-P3B-M1-readiness-dry-run-v1.json"
);
fs.mkdirSync(path.dirname(out), { recursive: true });
fs.writeFileSync(out, JSON.stringify(summary, null, 2));
console.log(JSON.stringify({ ok: report.ok, out }, null, 2));
process.exit(report.ok ? 0 : 2);
