#!/usr/bin/env node
/**
 * Read-only Full GDI E2E completion evaluation.
 *
 * Safe against active Renaissance/Tampa/ADP runs: does not mutate journals,
 * does not resubmit research, does not publish.
 *
 * Usage:
 *   node scripts/gdi-full-e2e-status-v1.mjs --hotel <id> --run-dir <runDir>
 *   node scripts/gdi-full-e2e-status-v1.mjs --hotel <id> --resume-run <runId> [--out-dir ...]
 *   node scripts/gdi-full-e2e-status-v1.mjs --evidence <path/to/fixture-or-evidence.json>
 *   node scripts/gdi-full-e2e-status-v1.mjs --write-contract-doc [--out-dir reports/gdi-full-e2e-contract]
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  FULL_E2E_CONTRACT_VERSION,
  buildFullE2eContractDocument,
} from "../lib/group-demand-intelligence/workflow/full-e2e-contract-v1.js";
import { evaluateFullE2eCompletion } from "../lib/group-demand-intelligence/workflow/full-e2e-completion-evaluator-v1.js";
import {
  buildCompletionInputFromRun,
  writeFullE2eArtifacts,
} from "../lib/group-demand-intelligence/workflow/full-e2e-completion-input-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function arg(name, fallback = null) {
  const i = process.argv.indexOf(name);
  if (i < 0) return fallback;
  return process.argv[i + 1] || fallback;
}
function has(flag) {
  return process.argv.includes(flag);
}

const hotelId = String(arg("--hotel", "") || "").trim();
const runDirArg = arg("--run-dir");
const resumeRun = arg("--resume-run");
const evidencePath = arg("--evidence") || arg("--completion-evidence");
const outDir =
  arg("--out-dir") || path.join(ROOT, "reports/gdi-full-e2e-contract");
const writeContractDoc = has("--write-contract-doc");

if (writeContractDoc) {
  fs.mkdirSync(outDir, { recursive: true });
  const doc = buildFullE2eContractDocument();
  const p = path.join(outDir, "full-e2e-contract.json");
  fs.writeFileSync(p, JSON.stringify(doc, null, 2) + "\n");
  console.log(JSON.stringify({ ok: true, wrote: p, version: doc.version }, null, 2));
  if (!hotelId && !runDirArg && !evidencePath && !resumeRun) process.exit(0);
}

let runDir = runDirArg ? path.resolve(runDirArg) : null;
if (!runDir && resumeRun) {
  const base =
    arg("--workflow-out-dir") ||
    path.join(ROOT, "reports/gdi-repeatable-workflow-v1");
  runDir = path.join(base, "runs", resumeRun);
}

if (evidencePath && !runDir) {
  const evidence = JSON.parse(fs.readFileSync(path.resolve(evidencePath), "utf8"));
  const completion = evaluateFullE2eCompletion(evidence);
  fs.mkdirSync(outDir, { recursive: true });
  const artifacts = writeFullE2eArtifacts({
    outDir,
    contractDoc: buildFullE2eContractDocument(),
    completion,
    inputSnapshot: evidence,
  });
  console.log(
    JSON.stringify(
      {
        ok: true,
        mode: "evidence_only",
        overallStatus: completion.overallStatus,
        canClaimComplete: completion.canClaimComplete,
        ...artifacts,
        reasons: completion.reasons,
        blockers: completion.blockers,
      },
      null,
      2
    )
  );
  process.exit(completion.canClaimComplete ? 0 : 0);
}

if (!runDir) {
  console.error(
    "Required: --run-dir <path> or --resume-run <id> or --evidence <json> (or --write-contract-doc alone)"
  );
  process.exit(2);
}

const ledgerPath = path.join(runDir, "stage-completion-ledger.json");
if (!fs.existsSync(ledgerPath)) {
  console.error(`Missing ledger: ${ledgerPath}`);
  process.exit(2);
}

const ledger = JSON.parse(fs.readFileSync(ledgerPath, "utf8"));
const overlay = evidencePath
  ? JSON.parse(fs.readFileSync(path.resolve(evidencePath), "utf8"))
  : {};

const input = buildCompletionInputFromRun({
  ledger,
  runDir,
  overlay,
  runMode: "status_only",
});
if (hotelId && input.hotelId && hotelId !== input.hotelId) {
  console.error(
    JSON.stringify({
      ok: false,
      error: "hotel_mismatch",
      expected: hotelId,
      ledgerHotelId: input.hotelId,
    })
  );
  process.exit(2);
}

const completion = evaluateFullE2eCompletion(input);
const statusOut = path.join(runDir, "full_e2e");
fs.mkdirSync(statusOut, { recursive: true });
const artifacts = writeFullE2eArtifacts({
  outDir: statusOut,
  contractDoc: buildFullE2eContractDocument(),
  completion,
  inputSnapshot: input,
});
// Also mirror into reports pack (read-only evaluation copy — does not claim run ownership)
fs.mkdirSync(outDir, { recursive: true });
const mirrorName = `completion-${ledger.hotelId || hotelId || "unknown"}-${ledger.runId || "run"}.json`;
fs.writeFileSync(
  path.join(outDir, mirrorName),
  JSON.stringify(
    {
      evaluatedAt: new Date().toISOString(),
      contractVersion: FULL_E2E_CONTRACT_VERSION,
      sourceRunDir: runDir,
      readOnly: true,
      didNotResubmitResearch: true,
      didNotMutateJournal: true,
      completion,
    },
    null,
    2
  ) + "\n"
);

console.log(
  JSON.stringify(
    {
      ok: true,
      mode: "status_only",
      hotelId: input.hotelId,
      runId: input.runId,
      overallStatus: completion.overallStatus,
      canClaimComplete: completion.canClaimComplete,
      segmentSummary: completion.segmentSummary,
      artifacts,
      mirror: path.join(outDir, mirrorName),
      reasons: completion.reasons,
      blockers: completion.blockers,
      note: "Read-only evaluation. Do not treat as a new Full E2E execution.",
    },
    null,
    2
  )
);
