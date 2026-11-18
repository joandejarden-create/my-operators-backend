#!/usr/bin/env node
/**
 * Offline gates for Full E2E PowerShell / deep-research certification V1.
 * No paid calls. No Airtable production writes.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import {
  demonstrateDiscoveryDeepSeparationFixture,
} from "../lib/group-demand-intelligence/research/gdi-segment-stage-call-budget-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-full-e2e-powershell-deep-research-certification-v1",
);
const PS1 = path.join(ROOT, "scripts/gdi-workflow.ps1");

const ps1 = fs.readFileSync(PS1, "utf8");
assert.match(ps1, /\$useCanonicalResearch/);
assert.match(ps1, /\[switch\]\$Legacy/);
assert.match(ps1, /gdi-canonical-full-e2e-entry-v1\.mjs/);
assert.doesNotMatch(ps1, /--authorize-paid-discovery/);
assert.doesNotMatch(ps1, /--authorize-paid-research/);

const budget = demonstrateDiscoveryDeepSeparationFixture();
assert.equal(budget.correctedCapRespected, true);
assert.equal(budget.fifthDiscoveryBlocked, true);
assert.equal(budget.deepAllowedAfterFourDiscovery, true);
assert.equal(budget.legacySharedCapWouldBreach, true);

const chainSrc = fs.readFileSync(
  path.join(
    ROOT,
    "lib/group-demand-intelligence/workflow/original-full-e2e-research-chain-v1.js",
  ),
  "utf8",
);
assert.match(chainSrc, /segmentStageCallBudget/);
assert.match(chainSrc, /BUDGET_STAGE\.DEEP_RESEARCH/);

const run = spawnSync(
  process.execPath,
  [
    path.join(
      ROOT,
      "scripts/gdi-full-e2e-powershell-deep-research-certification-v1.mjs",
    ),
  ],
  { cwd: ROOT, encoding: "utf8", timeout: 300000 },
);
assert.equal(run.status, 0, run.stderr || run.stdout.slice(-1000));

const required = [
  "FOUNDER_EXECUTIVE_SUMMARY.md",
  "POWERSHELL_TO_CANONICAL_EXECUTION_TRACE.md",
  "FOURTEEN_SEGMENT_FUNCTIONAL_CERTIFICATION.md",
  "DEEP_RESEARCH_EVIDENCE_AND_QUALITY.md",
  "SIX_OPPORTUNITY_COMMERCIAL_RESCORE.md",
  "CONTACT_RESEARCH_END_TO_END.md",
  "BEFORE_AFTER_COMMERCIAL_CONVERSION.md",
  "POWERHELL_VS_CANONICAL_PARITY_RESULTS.md",
  "PERSISTENCE_AND_WEEKLY_CHANGE_CERTIFICATION.md",
  "EXA_BUDGET_ENFORCEMENT_TESTS.md",
  "DRAWER_QUALITY_VERIFICATION.md",
  "REMAINING_INTEGRATION_GAPS.md",
  "TEST_RESULTS.md",
  "FOUNDER_NEXT_APPROVAL.md",
  "ACCEPTANCE.json",
  "CERTIFICATION_PAYLOAD.json",
];
for (const name of required) {
  assert.ok(fs.existsSync(path.join(OUT, name)), `missing ${name}`);
}

const acceptance = JSON.parse(
  fs.readFileSync(path.join(OUT, "ACCEPTANCE.json"), "utf8"),
);
assert.equal(acceptance.acceptance.powershellFullRunMapsToCanonical, true);
assert.equal(acceptance.acceptance.fourteenSegmentsTestedBeyondInvocation, true);
assert.equal(acceptance.acceptance.contactsRetained, true);
assert.equal(acceptance.acceptance.budgetPreventsSharedCapBreach, true);
assert.equal(acceptance.acceptance.noPaidSpend, true);
assert.equal(acceptance.acceptance.noAirtableProductionWrites, true);
assert.equal(acceptance.acceptance.sixOppsEvidenceGrounded, true);
assert.equal(acceptance.acceptance.drawerAccurate, true);
assert.equal(acceptance.acceptance.persistenceFailClosed, true);

console.log(
  JSON.stringify(
    {
      ok: true,
      outDir: OUT,
      powershellDefaultCanonical: true,
      budgetSeparation: true,
      acceptanceKeys: Object.keys(acceptance.acceptance),
    },
    null,
    2,
  ),
);
