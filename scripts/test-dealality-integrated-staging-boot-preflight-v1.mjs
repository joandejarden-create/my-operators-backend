#!/usr/bin/env node
/**
 * Regression: integrated staging boot preflight must not depend on unmerged PR #496 module.
 */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PREFLIGHT = path.join(ROOT, "scripts/dealality-integrated-staging-boot-preflight-v1.mjs");
const UNMERGED = path.join(ROOT, "lib/dealality-dev-os/integrated-staging-v1.js");

assert.equal(fs.existsSync(PREFLIGHT), true, "preflight script must exist");
assert.equal(
  fs.existsSync(UNMERGED),
  false,
  "integrated-staging-v1.js must NOT be on main — it belongs to open PR #496",
);

const run = spawnSync(process.execPath, [PREFLIGHT], {
  cwd: ROOT,
  encoding: "utf8",
  env: { ...process.env },
});
assert.equal(run.status, 0, `preflight should pass on main checkout: ${run.stderr || run.stdout}`);
const payload = JSON.parse(run.stdout);
assert.equal(payload.ok, true);
assert.equal(payload.unmergedStagingModulePresent, false);
assert.match(String(payload.canonicalBoot || ""), /server\.js/);

// Simulate stale import of the unmerged module → fail closed with clear code
const stale = path.join(ROOT, "tmp/test-stale-integrated-staging-import.mjs");
fs.mkdirSync(path.dirname(stale), { recursive: true });
fs.writeFileSync(
  stale,
  `import "../lib/dealality-dev-os/integrated-staging-v1.js";\n`,
  "utf8",
);
const bad = spawnSync(process.execPath, [PREFLIGHT, "tmp/test-stale-integrated-staging-import.mjs"], {
  cwd: ROOT,
  encoding: "utf8",
  env: { ...process.env },
});
assert.notEqual(bad.status, 0, "stale unmerged import must fail preflight");
assert.match(bad.stderr || "", /PREFLIGHT_IMPORT_FAILED|ERR_MODULE_NOT_FOUND|unmerged|integrated-staging-v1/i);

console.log(
  JSON.stringify(
    {
      ok: true,
      gate: "test-dealality-integrated-staging-boot-preflight-v1",
      unmergedModuleAbsentOnMain: true,
      staleImportFailsClosed: true,
    },
    null,
    2,
  ),
);
