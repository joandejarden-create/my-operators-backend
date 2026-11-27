#!/usr/bin/env node
/**
 * Machine-readable founder UI contract gate for the four recovered HE tabs.
 *
 *   node scripts/hotel-explorer/audit-he-founder-ui-contract-v1.mjs
 *   node scripts/hotel-explorer/audit-he-founder-ui-contract-v1.mjs --browser
 *
 * Browser-dependent HE_CANONICAL_BROWSER_PARITY is NOT_TESTED when --browser is omitted.
 * API/live probes never upgrade a failed visual/static check.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const browserEnabled = process.argv.includes("--browser");
const skipBrowser = process.argv.includes("--skip-browser") || !browserEnabled;
const CONTRACT_PATH = path.join(
  ROOT,
  "configs/hotel-explorer/he-founder-ui-contract-four-tabs-v1.json"
);

function read(rel) {
  return fs.readFileSync(path.join(ROOT, rel), "utf8");
}

function gate(ok) {
  return ok ? "PASS" : "FAIL";
}

const contract = JSON.parse(fs.readFileSync(CONTRACT_PATH, "utf8"));

const files = {
  contactJs: read("public/js/hotel-contact-intelligence.js"),
  contactCss: read("public/css/hotel-contact-intelligence.css"),
  presentationCss: read("public/css/he-customer-presentation-v1.css"),
  ocnJs: read("public/js/he-ownership-control-network-v1.js"),
  explorerCss: read("public/css/hotel-explorer.css"),
  workspaceJs: read("public/js/hotel-intelligence-workspace.js"),
  workspaceCss: read("public/css/hotel-intelligence-workspace.css"),
  detailJs: read("public/js/hotel-detail-panel.js"),
  detailCss: read("public/css/hotel-detail-panel.css"),
  aboutJs: read("public/js/he-about-this-analysis-v1.js"),
  factoryPs1: read("scripts/hotel-explorer/Invoke-HotelExplorerFactory.ps1"),
  packageJson: read("package.json"),
};

const scopes = {
  HE_CONTACT_CX_REGRESSION: ["contactJs", "contactCss", "presentationCss"],
  HE_OWNERSHIP_GRAPH_REGRESSION: ["ocnJs", "explorerCss"],
  HE_PORTFOLIO_LAYOUT_REGRESSION: ["workspaceJs", "workspaceCss", "explorerCss"],
  HE_SUBMARKET_PRESENTATION_REGRESSION: [
    "detailJs",
    "detailCss",
    "aboutJs",
    "presentationCss",
  ],
};

const requirementResults = {};
for (const req of contract.requirements || []) {
  const mustInclude = req.staticMarkers?.mustInclude || [];
  const mustExclude = req.staticMarkers?.mustExclude || [];
  const scopeKeys = scopes[req.id] || Object.keys(files);
  const blob = scopeKeys.map((k) => files[k] || "").join("\n");
  const missing = mustInclude.filter((m) => !blob.includes(m));
  const leaked = mustExclude.filter((m) => blob.includes(m));
  const ok = missing.length === 0 && leaked.length === 0;
  requirementResults[req.id] = {
    status: gate(ok),
    tab: req.tab,
    component: req.component,
    scope: scopeKeys,
    missing,
    leaked,
    sourcePr: contract.sourcePrs || null,
  };
}

function runNpm(script, extraArgs = []) {
  const r = spawnSync("npm", ["run", script, ...extraArgs], {
    cwd: ROOT,
    encoding: "utf8",
    shell: true,
  });
  return { script, ok: r.status === 0, status: r.status, stdout: r.stdout || "", stderr: r.stderr || "" };
}

const supporting = {
  contactDirectory: runNpm("test:he-contact-intelligence-v3-directory"),
  orgPortfolio: runNpm("test:he-org-portfolio-ui-cleanup-v1"),
  customerPresentation: runNpm("audit:he-customer-presentation-v1"),
  executiveNarrative: fs.existsSync(
    path.join(ROOT, "scripts/test-he-executive-ownership-narrative-v1.mjs")
  )
    ? runNpm("test:he-executive-ownership-narrative-v1")
    : { script: "test:he-executive-ownership-narrative-v1", ok: false, status: 1, stdout: "", stderr: "missing" },
};

const supportingOk = Object.values(supporting).every((s) => s.ok);

let browserParity = {
  status: "NOT_TESTED",
  ok: false,
  reason: skipBrowser ? "browser skipped" : "pending",
};
if (!skipBrowser) {
  const ocn = runNpm("verify:he-ownership-graph-layout-browser-v1", ["--", "--require-server"]);
  const portfolio = runNpm("he:org-portfolio-layout-audit");
  const presentation = runNpm("audit:he-customer-presentation-v1", ["--", "--browser"]);
  const ok = ocn.ok && portfolio.ok && presentation.ok;
  browserParity = {
    status: ok ? "PASS" : "FAIL",
    ok,
    checks: {
      ownershipGraph: ocn.ok ? "PASS" : "FAIL",
      orgPortfolio: portfolio.ok ? "PASS" : "FAIL",
      customerPresentation: presentation.ok ? "PASS" : "FAIL",
    },
  };
}

const factoryWired =
  /audit-he-founder-ui-contract-v1/.test(files.factoryPs1) &&
  /test:he-org-portfolio-ui-cleanup-v1/.test(files.factoryPs1) &&
  /audit-he-customer-presentation-v1/.test(files.factoryPs1) &&
  /"audit:he-founder-ui-contract-v1"/.test(files.packageJson);

const tabGatesPass = Object.values(requirementResults).every((r) => r.status === "PASS");
const founderContractOk = tabGatesPass && supportingOk && factoryWired;

const gates = {
  HE_CONTACT_CX_REGRESSION: requirementResults.HE_CONTACT_CX_REGRESSION?.status || "FAIL",
  HE_OWNERSHIP_GRAPH_REGRESSION: requirementResults.HE_OWNERSHIP_GRAPH_REGRESSION?.status || "FAIL",
  HE_PORTFOLIO_LAYOUT_REGRESSION: requirementResults.HE_PORTFOLIO_LAYOUT_REGRESSION?.status || "FAIL",
  HE_SUBMARKET_PRESENTATION_REGRESSION:
    requirementResults.HE_SUBMARKET_PRESENTATION_REGRESSION?.status || "FAIL",
  HE_FOUNDER_UI_CONTRACT: founderContractOk ? "PASS" : "FAIL",
  HE_CANONICAL_BROWSER_PARITY: browserParity.status,
  FULL_HE_E2E_VALIDATED: "NO",
};

const ok =
  gates.HE_CONTACT_CX_REGRESSION === "PASS" &&
  gates.HE_OWNERSHIP_GRAPH_REGRESSION === "PASS" &&
  gates.HE_PORTFOLIO_LAYOUT_REGRESSION === "PASS" &&
  gates.HE_SUBMARKET_PRESENTATION_REGRESSION === "PASS" &&
  gates.HE_FOUNDER_UI_CONTRACT === "PASS" &&
  gates.FULL_HE_E2E_VALIDATED === "NO" &&
  (skipBrowser || gates.HE_CANONICAL_BROWSER_PARITY === "PASS");

const outDir = path.join(ROOT, "reports/hotel-explorer");
fs.mkdirSync(outDir, { recursive: true });
const outPath = path.join(outDir, "HE-FOUNDER-UI-CONTRACT-FOUR-TABS-V1.json");
const report = {
  version: "he-founder-ui-contract-four-tabs-v1",
  generatedAt: new Date().toISOString(),
  contractPath: "configs/hotel-explorer/he-founder-ui-contract-four-tabs-v1.json",
  browserEnabled: !skipBrowser,
  ok,
  gates,
  requirementResults,
  supporting: Object.fromEntries(
    Object.entries(supporting).map(([k, v]) => [k, { script: v.script, ok: v.ok, status: v.status }])
  ),
  factoryWired,
  browserParity,
  rootCauseSummary: contract.rootCauseSummary || null,
};
fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
console.log(JSON.stringify(report, null, 2));

assert.equal(gates.FULL_HE_E2E_VALIDATED, "NO");
if (!ok) {
  const failed = Object.entries(gates)
    .filter(([k, v]) => k !== "FULL_HE_E2E_VALIDATED" && v !== "PASS" && v !== "NOT_TESTED")
    .map(([k, v]) => `${k}=${v}`);
  assert.fail(`founder UI contract gates failed: ${failed.join(", ")}`);
}
