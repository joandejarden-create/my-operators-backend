#!/usr/bin/env node
/**
 * Wrapper for permanent Full E2E certification gate.
 * Durable published fixtures live under data/adp-durable-published/** (gitignored).
 * When missing, fail closed with a clear prerequisite message — do not invent cert data.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { spawnSync } from "node:child_process";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REQUIRED = path.join(
  ROOT,
  "data/adp-durable-published/current/adp_hilton_times_square/manifest.json"
);

if (!fs.existsSync(REQUIRED)) {
  console.error(
    JSON.stringify(
      {
        ok: false,
        gate: "test:adp-permanent-full-e2e-certification-v1",
        code: "DURABLE_PUBLISHED_FIXTURES_MISSING",
        required: "data/adp-durable-published/current/adp_hilton_times_square/manifest.json",
        note:
          "data/adp-durable-published/** is gitignored. Materialize durable published current/snapshots locally before this gate can run. Do not weaken the gate or invent certification packages.",
        unresolvedExternalDependency: true,
      },
      null,
      2
    )
  );
  process.exit(2);
}

const script = path.join(ROOT, "scripts/test-adp-permanent-full-e2e-certification-v1.mjs");
const result = spawnSync(process.execPath, [script], {
  cwd: ROOT,
  stdio: "inherit",
  env: process.env,
});
process.exit(result.status == null ? 1 : result.status);
