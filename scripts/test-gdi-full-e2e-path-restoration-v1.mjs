/**
 * Zero-paid regression for the restored Tampa and Aimbridge paths
 * and the five execution corrections. Does not call providers.
 */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { classifyCommercialDemandThesis } from "../lib/group-demand-intelligence/commercial-demand-thesis-standard-v1.js";
import { DEMAND_THESIS_STANDARD } from "../lib/group-demand-intelligence/commercial-demand-thesis-standard-v1.js";
import { evaluateSharedCommercialEligibility } from "../lib/group-demand-intelligence/commercial-eligibility-contract-v1.js";
import { classifyContactEligibility } from "../lib/group-demand-intelligence/contact-eligibility-standard-v1.js";
import { sanitizeOpportunityForCustomerPresentation } from "../lib/group-demand-intelligence/customer-presentation-sanitize-v1.js";
import { reconcileAuthoritativeBudget } from "../lib/group-demand-intelligence/workflow/full-e2e-verification-adapters-v1.js";
import { runWeeklyRefreshCycle } from "../lib/group-demand-intelligence/workflow/weekly-refresh-orchestrator-v1.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

const generatedOnly = classifyCommercialDemandThesis({
  hotelOpportunityThesis: "The association needs a guestroom block at this hotel.",
});
assert.equal(generatedOnly.thesisStandard, DEMAND_THESIS_STANDARD.SPECULATIVE_DEMAND);
assert.equal(generatedOnly.mayQualifySalesReadyDemandGate, false);

const confirmed = classifyCommercialDemandThesis({
  evidenceQuote: "The housing bureau published a guestroom block for delegates.",
});
assert.equal(confirmed.thesisStandard, DEMAND_THESIS_STANDARD.CONFIRMED_DEMAND);

const inferred = classifyCommercialDemandThesis({
  evidence: [
    { value: "The 2026 oncology conference is held at the downtown convention center." },
    { value: "1200 delegates are expected." },
  ],
});
assert.equal(inferred.thesisStandard, DEMAND_THESIS_STANDARD.RELIABLE_INFERRED_DEMAND);
assert.equal(inferred.mayQualifySalesReadyDemandGate, true);

const speculativeEligibility = evaluateSharedCommercialEligibility({
  id: "fixture-speculative",
  organizationName: "Example Association",
  hotelOpportunityThesis: "The group needs guestrooms.",
  primaryContact: { name: "Alex Morgan", role: "Events Director", email: "alex@example.org" },
});
assert.notEqual(speculativeEligibility.status, "SALES_READY");

const intermediary = classifyContactEligibility(
  {
    name: "Alex Morgan",
    role: "Destination Management Company Director",
    email: "alex@example.org",
    organization: "Example DMC",
  },
  {
    evidenceQuote: "Alex Morgan at Example DMC is arranging overnight lodging for the offsite meeting.",
    organizationName: "Example DMC",
  }
);
assert.equal(
  /second contact|direct hotel-booking remit|direct booking authority/i.test(
    JSON.stringify(intermediary)
  ),
  false
);

const sanitized = sanitizeOpportunityForCustomerPresentation({
  id: "fixture-contact",
  organizationName: "Example Association",
  displayTitle: "Example meeting",
  primaryContact: {
    name: "Alex Morgan",
    email: "alex@example.org",
    role: "Events Director",
    surfeEnrichmentEligible: true,
    surfeEnriched: true,
    webhoundRunId: "hidden",
  },
  surfeEnrichmentEligible: true,
  webhound: { internal: true },
  labels: ["webhound", "sports"],
  contactResearchAudit: { webhoundCalls: 2 },
});
const sanitizedJson = JSON.stringify(sanitized);
assert.match(sanitizedJson, /Alex Morgan/);
assert.match(sanitizedJson, /alex@example\.org/);
assert.equal(/surfeEnrich|webhound/i.test(sanitizedJson), false);
assert.equal(sanitized.labels.includes("sports"), true);

const ledgerDir = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-ledger-"));
const ledgerPath = path.join(ledgerDir, "authorization-ledger-v1.json");
const historical = {
  version: "gdi-parallel-budget-ledger-v1",
  authorization: { authorizationId: "HISTORICAL_ONLY" },
  budgetUsd: 11,
  estimatedSpentUsd: 5.25,
};
fs.writeFileSync(ledgerPath, JSON.stringify(historical));
const before = fs.readFileSync(ledgerPath);
const budget = reconcileAuthoritativeBudget({ ledgerPath, hotelId: "recFixture" });
assert.equal(budget.reconciled, false);
assert.equal(budget.reason, "historical_ledger_is_not_run_authorization");
assert.equal(budget.spendAuthorized, false);
assert.equal(fs.readFileSync(ledgerPath).equals(before), true);

const refreshRoot = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-refresh-"));
await runWeeklyRefreshCycle({
  hotelId: "recFixtureDryRun",
  opportunities: [],
  dryRun: true,
  invokeDownstream: false,
  env: { ...process.env, GDI_DATA_ROOT: refreshRoot },
});
assert.equal(fs.existsSync(refreshRoot), true);
const refreshFiles = fs.readdirSync(refreshRoot);
assert.deepEqual(refreshFiles, []);

const workflow = fs.readFileSync(
  path.join(root, "scripts/gdi-research-to-page-workflow-v1.mjs"),
  "utf8"
);
assert.equal(workflow.includes("ops.slice(0, 60)"), false);
assert.equal(workflow.includes("GDI_WEEKLY_REFRESH_PERSIST_STATE_ON_DRY_RUN"), false);
assert.equal(workflow.includes("paidMultisegmentAuthorizationLedgerPath()"), true);

const help = spawnSync(
  process.execPath,
  ["scripts/gdi-scaling-companies-hotel-runner-v1.mjs", "--help"],
  { cwd: root, encoding: "utf8" }
);
assert.equal(help.status, 0);
assert.match(help.stdout, /preview/);

const out = path.join(refreshRoot, "aimbridge-accounting");
const orchestrator = spawnSync(
  process.execPath,
  [
    "scripts/gdi-full-e2e-fourteen-segment-orchestrator-v1.mjs",
    "--dry-run",
    "--hotels",
    "recCAGqkERSXgiku4,recTk5I8VOzsPhxDz,recXh4NgF8sDbi9cQ",
    "--out",
    out,
  ],
  { cwd: root, encoding: "utf8" }
);
assert.equal(orchestrator.status, 0, orchestrator.stderr || orchestrator.stdout);
const report = JSON.parse(fs.readFileSync(path.join(out, "orchestrator-report.json"), "utf8"));
assert.equal(report.mode, "ACCOUNTING_ONLY");
assert.equal(report.executeDiscovery, false);
assert.equal(report.authorizePaidDiscovery, false);
assert.equal(report.authorizePaidResearch, false);
assert.equal(report.cellCount, 42);

console.log(JSON.stringify({ ok: true, mode: report.mode, cellCount: report.cellCount }));
