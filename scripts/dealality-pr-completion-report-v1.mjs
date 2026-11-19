#!/usr/bin/env node
/**
 * Universal PR completion + workspace ownership report (read-only).
 *
 *   npm run dealality:pr-completion-report -- --stream he --pr 399
 *   npm run dealality:pr-completion-report -- --audit
 *   npm run dealality:pr-completion-report -- --stream gdi --json
 *
 * Never resets, cleans, stashes, or switches branches.
 */
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  auditWorkspaceOwnership,
  buildPrCompletionReport,
  formatPrCompletionReport,
} from "../lib/dealality-dev-os/pr-completion-ownership.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function getArg(argv, name) {
  const i = argv.indexOf(name);
  return i >= 0 ? argv[i + 1] : "";
}
function has(argv, name) {
  return argv.includes(name);
}

const argv = process.argv.slice(2);
const json = has(argv, "--json");
const audit = has(argv, "--audit");
const stream = (getArg(argv, "--stream") || "").toLowerCase();
const pr = getArg(argv, "--pr") || null;
const mergeSha = getArg(argv, "--merge-sha") || null;
const cursorActiveBranch = getArg(argv, "--cursor-branch") || null;
const featureBranchRetired = getArg(argv, "--feature-branch-retired") || null;
const peerImpact = getArg(argv, "--peer-impact") || null;
const ignoredEvidence = getArg(argv, "--ignored-evidence") || null;

const mergedRaw = getArg(argv, "--merged");
const ciRaw = getArg(argv, "--ci-passed");
const merged = mergedRaw === "" ? null : mergedRaw === "true" || mergedRaw === "1";
const ciPassed = ciRaw === "" ? null : ciRaw === "true" || ciRaw === "1";

if (audit) {
  const snap = auditWorkspaceOwnership(ROOT);
  if (json) {
    console.log(JSON.stringify(snap, null, 2));
  } else {
    console.log(`[dealality:pr-completion] ownership audit (${snap.rows.length} permanent workspaces)`);
    for (const row of snap.rows) {
      console.log(
        `  [${row.status}] ${row.stream} path=${row.path} branch=${row.branch} dirty=${row.dirtyCount}`,
      );
    }
  }
  const blocked = snap.rows.some((r) => String(r.status).startsWith("BLOCKED"));
  process.exit(blocked ? 2 : 0);
}

if (!stream) {
  console.error("Usage: dealality:pr-completion-report -- --stream <he|adp|gdi|integration> [--pr n] [--audit]");
  process.exit(2);
}

const report = buildPrCompletionReport({
  registryRoot: ROOT,
  stream,
  pr,
  mergeSha,
  merged: mergedRaw === "" ? null : merged,
  ciPassed: ciRaw === "" ? null : ciPassed,
  cursorActiveBranch,
  featureBranchRetired,
  peerImpact,
  ignoredEvidencePreserved: ignoredEvidence,
});

if (json) console.log(JSON.stringify(report, null, 2));
else console.log(formatPrCompletionReport(report));

process.exit(report.ok ? 0 : 2);
