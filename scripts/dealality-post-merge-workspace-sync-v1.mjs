#!/usr/bin/env node
/**
 * Dealality Dev OS — post-merge product workspace sync CLI.
 *
 * Usage:
 *   npm run dealality:post-merge-sync -- --mode status
 *   npm run dealality:post-merge-sync -- --mode post-merge --stream he
 *   npm run dealality:post-merge-sync -- --mode post-merge --stream adp --merged-branch fix/... --apply
 *   npm run dealality:post-merge-sync -- --mode post-merge --stream he --pr 390 --apply
 *   npm run dealality:post-merge-sync -- --mode peer-sync --apply
 *
 * Default is dry-run. Pass --apply to mutate (never reset/clean/stash-discard).
 * Explicit --merged-branch and/or --pr is required to authorize return-home.
 */
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  applyPostMergeWorkspaceSync,
  formatSyncPlan,
  planPostMergeWorkspaceSync,
  PRODUCT_STREAMS,
} from "../lib/dealality-dev-os/post-merge-workspace-sync.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function getArg(argv, name) {
  const i = argv.indexOf(name);
  return i >= 0 ? argv[i + 1] : "";
}

function has(argv, name) {
  return argv.includes(name);
}

function parseStreams(raw) {
  if (!raw) return PRODUCT_STREAMS.slice();
  return String(raw)
    .split(",")
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean);
}

const argv = process.argv.slice(2);
const json = has(argv, "--json");
const apply = has(argv, "--apply");
const mode = getArg(argv, "--mode") || (apply ? "post-merge" : "status");
const affectedStream = (getArg(argv, "--stream") || getArg(argv, "--affected-stream") || "").toLowerCase();
const mergedBranch = getArg(argv, "--merged-branch") || "";
const pr = getArg(argv, "--pr") || "";
const streams = parseStreams(getArg(argv, "--streams"));
const noFetch = has(argv, "--no-fetch");

if (affectedStream && !PRODUCT_STREAMS.includes(affectedStream) && affectedStream !== "integration") {
  console.error(`Unknown --stream '${affectedStream}'. Expected one of: ${PRODUCT_STREAMS.join(", ")}`);
  process.exit(2);
}

const plan = planPostMergeWorkspaceSync({
  registryRoot: ROOT,
  mode,
  affectedStream: affectedStream || null,
  mergedBranch: mergedBranch || null,
  pr: pr || null,
  streams: mode === "peer-sync" && affectedStream
    ? streams.filter((s) => s !== affectedStream)
    : streams,
  fetch: !noFetch,
  apply,
});

if (!apply) {
  if (json) {
    process.stdout.write(`${JSON.stringify({ plan, applied: false }, null, 2)}\n`);
  } else {
    console.log(formatSyncPlan(plan));
    console.log("  (dry-run — re-run with --apply to mutate; never resets/cleans/discards)");
  }
  process.exit(plan.ok ? 0 : 1);
}

const result = applyPostMergeWorkspaceSync({ ...plan, dryRun: false }, { apply: true });
if (json) {
  process.stdout.write(`${JSON.stringify({ plan, result }, null, 2)}\n`);
} else {
  console.log(formatSyncPlan(plan));
  console.log(`[dealality:post-merge-sync] apply ok=${result.ok}`);
  for (const r of result.results || []) {
    const flag = r.skipped ? "SKIP" : r.applied ? "OK" : "FAIL";
    console.log(`  [${flag}] ${r.stream} action=${r.action}${r.reason ? ` — ${r.reason}` : ""}`);
    for (const step of r.steps || []) {
      if (step.formatted) console.log(step.formatted.replace(/^/gm, "    "));
      else if (step.error) console.log(`    ERROR ${step.stage}: ${step.error}`);
      else if (step.preflight && !step.preflight.ok) {
        console.log(`    ERROR ${step.stage}: ${step.preflight.errors?.join("; ")}`);
      }
    }
  }
}

process.exit(result.ok ? 0 : 1);
