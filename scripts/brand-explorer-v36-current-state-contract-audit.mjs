#!/usr/bin/env node
/**
 * Brand Explorer v36A Current-State Contract Audit (read-only).
 *
 *   npm run brand-explorer-v36-current-state-contract-audit
 */
import { spawnSync } from "child_process";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import {
  AUDIT_VERSION,
  buildBrandExplorerV36CurrentStateContractAuditReport,
  writeBrandExplorerV36AuditReports,
} from "../lib/partner-intelligence/brand-explorer-v36-current-state-contract-audit.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");

const VALIDATION_SCRIPTS = [
  "test:brand-explorer-active-profile-staged-apply",
  "test:partner-intelligence-publish-readiness",
  "test:partner-intelligence-profile-governance-publish",
];

function runValidation(scriptName) {
  const res = spawnSync("npm", ["run", scriptName], {
    cwd: ROOT,
    shell: true,
    encoding: "utf8",
    env: process.env,
  });
  return {
    script: scriptName,
    status: res.status === 0 ? "pass" : "fail",
    exitCode: res.status,
    stderrTail: (res.stderr || "").split("\n").slice(-5).join("\n").trim() || null,
  };
}

async function main() {
  const validationTests = VALIDATION_SCRIPTS.map(runValidation);
  const report = await buildBrandExplorerV36CurrentStateContractAuditReport({ validationTests });
  const paths = writeBrandExplorerV36AuditReports(report, ROOT);

  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);
  console.log(`Wrote ${paths.docPath}`);
  console.log(`Audit: ${AUDIT_VERSION} (read-only)`);
  for (const t of validationTests) {
    console.log(`Validation ${t.script}: ${t.status}${t.exitCode != null ? ` (${t.exitCode})` : ""}`);
  }
  const failed = validationTests.filter((t) => t.status !== "pass");
  if (failed.length) {
    console.error("One or more validation tests failed.");
    process.exit(1);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
