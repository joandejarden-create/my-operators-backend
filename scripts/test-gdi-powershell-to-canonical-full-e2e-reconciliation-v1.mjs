#!/usr/bin/env node
/**
 * Offline gates for PowerShell ↔ canonical Full E2E reconciliation.
 * No paid calls. No Airtable writes.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import {
  runRestoredContactInvestigationPass,
} from "../lib/group-demand-intelligence/workflow/gdi-original-full-e2e-execution-restore-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-powershell-to-canonical-full-e2e-reconciliation-v1",
);
const CHAIN = path.join(
  ROOT,
  "lib/group-demand-intelligence/workflow/original-full-e2e-research-chain-v1.js",
);
const RESTORE = path.join(
  ROOT,
  "lib/group-demand-intelligence/workflow/gdi-original-full-e2e-execution-restore-v1.js",
);

let failed = 0;
function assert(cond, msg) {
  if (!cond) {
    failed += 1;
    console.error("FAIL:", msg);
  } else {
    console.log("ok:", msg);
  }
}

const chainSrc = fs.readFileSync(CHAIN, "utf8");
assert(
  chainSrc.includes("primaryContactName"),
  "processed rows serialize primaryContactName",
);
assert(
  chainSrc.includes("salesReadyContactNames"),
  "processed rows serialize salesReadyContactNames",
);

const restoreSrc = fs.readFileSync(RESTORE, "utf8");
assert(
  restoreSrc.includes("eligibleNamed[0]") &&
    restoreSrc.includes("Ensure a named commercial entry point"),
  "restore pass hydrates primaryContact from eligible classified people",
);

const positive = runRestoredContactInvestigationPass({
  id: "test_positive",
  organizationName: "GuidePoint Security",
  evidence: [
    {
      statement:
        "Jason Danner, Director of Events at GuidePoint Security, hosts the 2027 Bethesda training summit.",
      url: "https://example.com/guidepoint",
    },
  ],
});
const posName =
  positive.opportunity?.primaryContact?.name ||
  positive.pass?.classifiedContacts?.find((c) => c.eligible || c.salesReadyContactOk)
    ?.name ||
  "";
assert(/jason/i.test(String(posName)), `positive fixture recovers Jason (got: ${posName})`);

const negative = runRestoredContactInvestigationPass({
  id: "test_negative",
  organizationName: "Speculative Co",
  evidence: [
    {
      statement: "Taylor Swift performed at an unrelated stadium tour.",
      url: "https://example.com/unrelated",
    },
  ],
});
const negName = String(negative.opportunity?.primaryContact?.name || "");
const negSales = Number(negative.pass?.salesReadyContacts || 0);
assert(
  !/taylor swift/i.test(negName) || negSales === 0,
  "negative fixture does not promote unrelated celebrity as sales-ready",
);

const run = spawnSync(
  process.execPath,
  [path.join(ROOT, "scripts/gdi-powershell-to-canonical-full-e2e-reconciliation-v1.mjs")],
  { cwd: ROOT, encoding: "utf8" },
);
assert(run.status === 0, "reconciliation script exits 0");

const required = [
  "EXECUTIVE_ROOT_CAUSE.md",
  "ORIGINAL_POWERSHELL_WORKFLOW_MAP.md",
  "CURRENT_CANONICAL_WORKFLOW_MAP.md",
  "CAPABILITY_GAP_MATRIX.md",
  "HISTORICAL_CONTACT_SUCCESS_CASES.md",
  "PR382_ZERO_CONTACT_ROOT_CAUSE.md",
  "MINIMAL_INTEGRATION_RECOVERY_PLAN.md",
  "BEFORE_AFTER_VALIDATION.md",
  "FOUNDER_NEXT_DECISIONS.md",
];
for (const name of required) {
  assert(fs.existsSync(path.join(OUT, name)), `report ${name}`);
}

const validation = JSON.parse(
  fs.readFileSync(path.join(OUT, "BEFORE_AFTER_VALIDATION.json"), "utf8"),
);
assert(validation.paidCallsThisRun === 0, "zero paid calls");
assert(validation.airtableWrites === 0, "zero Airtable writes");
assert(
  Number(validation.after?.withPrimaryContactName || 0) > 0,
  "recovers named contacts from PR382 state",
);

if (failed) {
  console.error(`\n${failed} assertion(s) failed`);
  process.exit(1);
}
console.log("\nAll reconciliation gates passed.");
