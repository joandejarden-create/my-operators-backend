#!/usr/bin/env node
/**
 * Permanent Full E2E certification runner.
 *
 * Layer A (ordinary PR CI): sanitized offline fixtures — always run.
 * Layer B (authoritative durable evidence): optional, separate from PR CI.
 *   Requires gitignored data/adp-durable-published/** materialized locally.
 *   Never invents certification packages. Never copies production secrets into Git.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const LAYER_A = path.join(ROOT, "scripts/test-adp-permanent-full-e2e-certification-v1.mjs");
const DURABLE_HILTON = path.join(
  ROOT,
  "data/adp-durable-published/current/adp_hilton_times_square/manifest.json"
);
const REQUIRE_LAYER_B = process.env.ADP_PERMANENCE_REQUIRE_LAYER_B === "1";

const layerA = spawnSync(process.execPath, [LAYER_A], {
  cwd: ROOT,
  stdio: "inherit",
  env: process.env,
});
if (layerA.status !== 0) {
  process.exit(layerA.status == null ? 1 : layerA.status);
}

const durablePresent = fs.existsSync(DURABLE_HILTON);
const layerB = {
  ok: durablePresent,
  status: durablePresent ? "AVAILABLE" : "BLOCKED_MISSING_AUTHORITATIVE_EVIDENCE",
  required: "data/adp-durable-published/current/adp_hilton_times_square/manifest.json",
  note: durablePresent
    ? "Durable published volume present — run Layer B reconciliation via documented read-only process (see reports/.../PHASE1_1_LAYER_B_DURABLE_EVIDENCE_PROCESS.md)."
    : "data/adp-durable-published/** is gitignored. Materialize durable published current/snapshots locally for Layer B. Do not invent certification packages or commit production evidence to Git.",
  unresolvedExternalDependency: !durablePresent,
};

console.log(JSON.stringify({ layer: "B", gate: "adp-permanent-full-e2e-durable", ...layerB }, null, 2));

if (REQUIRE_LAYER_B && !durablePresent) {
  process.exit(2);
}

process.exit(0);
