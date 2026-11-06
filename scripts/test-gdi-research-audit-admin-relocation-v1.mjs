#!/usr/bin/env node
/**
 * GDI Research Audit relocation — customer GDI → AI Demand Admin.
 *
 *   node scripts/test-gdi-research-audit-admin-relocation-v1.mjs
 *   npm run test:gdi-research-audit-admin-relocation-v1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  customerSafeRunStatusLabel,
  redactCustomerFacingGdiSummary,
} from "../lib/group-demand-intelligence/customer-safe-run-status-label-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

function read(rel) {
  return fs.readFileSync(path.join(ROOT, rel), "utf8");
}

function check(name, fn) {
  fn();
  console.log(`ok - ${name}`);
}

check("customer_gdi_has_no_research_audit_tab", () => {
  const app = read("public/js/group-demand-intelligence/app.js");
  assert.ok(!/label:\s*"Research Audit"/.test(app), "auth GDI must not register Research Audit tab");
  assert.ok(
    !/\{ id:\s*"audit"/.test(app),
    "auth GDI must not include audit tab id in contentTabsHtml"
  );
  assert.ok(
    !/\/research-runs/.test(app),
    "auth GDI must not fetch research-runs (admin-only)"
  );
  assert.ok(
    !/function renderAudit\b/.test(app),
    "auth GDI must not render research audit panel"
  );
  assert.match(
    app,
    /customerSafeRunStatusLabel/,
    "auth GDI must sanitize property-bar run status"
  );
});

check("share_gdi_has_no_research_audit_or_costs", () => {
  const share = read("public/js/group-demand-intelligence/share-app.js");
  assert.ok(!/Research Audit/i.test(share));
  assert.ok(!/\/research-runs/.test(share));
  assert.ok(!/webhoundUsd|researchCostUsd|gdi_run_/i.test(share));
  assert.match(share, /customerSafeRunStatusLabel/);
});

check("ai_demand_admin_hosts_gdi_research_audit", () => {
  const shell = read("public/js/admin-ai-demand-admin.js");
  const html = read("public/app/admin/ai-demand-admin.html");
  const mod = read("public/js/admin-gdi-research-audit.js");

  assert.match(shell, /"gdi-research-audit"\s*:\s*true/);
  assert.match(shell, /adaTabGdiResearchAudit/);
  assert.match(shell, /adaPanelGdiResearchAudit/);
  assert.match(html, /data-tab="gdi-research-audit"/);
  assert.match(html, /id="adaTabGdiResearchAudit"/);
  assert.match(html, /id="adaPanelGdiResearchAudit"/);
  assert.match(html, /GDI Research/);
  assert.match(html, /admin-gdi-research-audit\.js/);
  assert.match(mod, /\/research-runs/);
  assert.match(mod, /dealality-adp-admin-tab/);
  assert.match(mod, /graTableBody/);
});

check("visibility_audit_nav_not_regressed", () => {
  const shell = read("public/js/admin-ai-demand-admin.js");
  const html = read("public/app/admin/ai-demand-admin.html");
  assert.match(shell, /"visibility-audit"\s*:\s*true/);
  assert.match(html, /id="adaTabVisibilityAudit"/);
  assert.match(html, /id="adaPanelVisibilityAudit"/);
  assert.match(html, /admin-ai-visibility-audit\.js/);
});

check("research_runs_api_is_admin_only", () => {
  const server = read("server.js");
  const listRe =
    /app\.get\(\s*"\/api\/group-demand-intelligence\/hotels\/:hotelId\/research-runs"\s*,([\s\S]*?)getGdiResearchRuns\s*\)/;
  const detailRe =
    /app\.get\(\s*"\/api\/group-demand-intelligence\/hotels\/:hotelId\/research-runs\/:runId"\s*,([\s\S]*?)getGdiResearchRun\s*\)/;
  const listMatch = server.match(listRe);
  const detailMatch = server.match(detailRe);
  assert.ok(listMatch, "research-runs list route must exist");
  assert.ok(detailMatch, "research-runs detail route must exist");
  assert.match(listMatch[1], /requireAdminAccess/);
  assert.match(detailMatch[1], /requireAdminAccess/);
  assert.ok(
    !/gdiPilotReadAuth/.test(listMatch[1]),
    "research-runs list must not use pilot-read auth"
  );
  assert.ok(
    !/gdiPilotReadAuth/.test(detailMatch[1]),
    "research-runs detail must not use pilot-read auth"
  );
});

check("customer_summary_redacts_internal_research_fields", () => {
  const api = read("api/group-demand-intelligence.js");
  assert.match(api, /redactCustomerFacingGdiSummary/);
  const redacted = redactCustomerFacingGdiSummary({
    runStatus: "COMPLETED",
    runStatusLabel: "Completed",
    latestRunId: "gdi_run_abc12345",
    researchCostUsd: 12.5,
    costBreakdown: { totalUsd: 12.5, webhoundUsd: 4 },
    runHistoryProvenance: "weekly_refresh_state",
    lastResearchAt: "2026-09-16T00:00:00.000Z",
  });
  assert.equal(redacted.latestRunId, undefined);
  assert.equal(redacted.researchCostUsd, undefined);
  assert.equal(redacted.costBreakdown, undefined);
  assert.equal(redacted.runHistoryProvenance, undefined);
  assert.equal(customerSafeRunStatusLabel(redacted), "Research Completed");
  assert.equal(
    customerSafeRunStatusLabel({ runStatus: "NEVER_RUN" }),
    "Not run yet"
  );
});

check("admin_workspace_nav_test_includes_gdi_research_audit", () => {
  const nav = read("scripts/test-adp-admin-workspace-navigation-v1.mjs");
  assert.match(nav, /gdi-research-audit/);
  assert.match(nav, /adaTabGdiResearchAudit/);
  assert.match(nav, /adaPanelGdiResearchAudit/);
});

console.log(
  JSON.stringify(
    {
      ok: true,
      contract: "gdi-research-audit-admin-relocation-v1",
      token: "GDI_RESEARCH_AUDIT_MOVED_TO_AI_DEMAND_ADMIN_STATIC",
    },
    null,
    2
  )
);
