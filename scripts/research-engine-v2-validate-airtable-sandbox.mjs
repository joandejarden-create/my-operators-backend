/**
 * Validate Airtable sandbox isolation for Mexico VIC → BE pilot.
 *
 * Read-only. No production writes. No VIC/62 artifact mutation.
 *
 *   npm run research-engine-v2:validate-airtable-sandbox
 */

import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import {
  STATUS,
  validateAirtableSandbox,
  renderSandboxValidationMarkdown,
  assertSandboxReadyForVicBePatch,
} from "../lib/research-engine-v2/airtable-sandbox-validation.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORTS = join(ROOT, "reports/research-engine-v2");
const DOCS = join(ROOT, "docs/data-intelligence");

function writeJson(path, obj) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, JSON.stringify(obj, null, 2), "utf8");
}
function writeMd(path, text) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, text, "utf8");
}

async function main() {
  console.log("[sandbox-validation] starting (read-only; no Airtable writes)");

  const report = await validateAirtableSandbox();
  const md = renderSandboxValidationMarkdown(report);
  const docsMd = `${md}

## Purpose

This gate proves a **dedicated Airtable sandbox/test base** before any Mexico VIC → Brand Explorer pilot patch can execute.

Production (\`AIRTABLE_BASE_ID\`) remains read-only for comparison. Frozen 62 and frozen VIC artifacts are never modified by this validator.

## Gate for patch runners

Any VIC → BE sandbox patch runner must call \`assertSandboxReadyForVicBePatch()\` from \`lib/research-engine-v2/airtable-sandbox-validation.js\`. If validation is not \`${STATUS.READY}\`, execution must throw and abort.
`;

  writeJson(join(REPORTS, "airtable-sandbox-validation.json"), report);
  writeMd(join(REPORTS, "airtable-sandbox-validation.md"), md);
  writeMd(join(DOCS, "airtable-sandbox-validation-for-vic-be-pilot.md"), docsMd);

  console.log(`[sandbox-validation] status=${report.status}`);
  console.log(
    `[sandbox-validation] prod=${report.production_base_id_masked} sandbox=${report.sandbox_base_id_masked} ids_differ=${report.ids_differ}`
  );
  console.log(`[sandbox-validation] patch_may_execute=${report.vic_sandbox_patch_may_execute}`);
  if (report.blockers?.length) {
    console.log(`[sandbox-validation] blockers: ${report.blockers.join(", ")}`);
  }

  // Prove the hard gate blocks when not ready
  if (report.status !== STATUS.READY) {
    try {
      await assertSandboxReadyForVicBePatch();
      console.error("[sandbox-validation] ERROR: assertSandboxReadyForVicBePatch should have thrown");
      process.exitCode = 1;
      return;
    } catch (err) {
      console.log(`[sandbox-validation] gate confirmed blocked: ${err.message.slice(0, 160)}`);
    }
    process.exitCode = 2;
    return;
  }

  console.log("[sandbox-validation] READY — VIC BE sandbox patch may proceed under sandbox write adapter only");
}

main().catch((err) => {
  console.error("[sandbox-validation] FATAL", err);
  process.exit(1);
});
