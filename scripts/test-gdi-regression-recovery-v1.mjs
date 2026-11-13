#!/usr/bin/env node
/**
 * Regression lock: restore approved GDI improvements that lived on unmerged
 * branches / uncommitted polish, and prevent silent reversion to legacy UI.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { customerFacingOpportunityTitle } from "../lib/group-demand-intelligence/customer-facing-opportunity-title-contract-v1.js";
import { accountLevelDisplayTitle } from "../lib/group-demand-intelligence/opportunity-list-dto.js";
import {
  sanitizeOpportunityForCustomerPresentation,
} from "../lib/group-demand-intelligence/customer-presentation-sanitize-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log("PASS", name);
  } catch (err) {
    failed += 1;
    console.error("FAIL", name, err.message);
  }
}

const css = fs.readFileSync(
  path.join(ROOT, "public/css/group-demand-intelligence.css"),
  "utf8"
);
const ui = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
  "utf8"
);
const app = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/app.js"),
  "utf8"
);
const html = fs.readFileSync(
  path.join(ROOT, "public/group-demand-intelligence.html"),
  "utf8"
);

check("clean_opportunity_display_names", () => {
  assert.equal(
    customerFacingOpportunityTitle("AMWA 2027 Interim Meeting — Sep 11–12"),
    "AMWA 2027 Interim Meeting"
  );
  assert.equal(
    customerFacingOpportunityTitle(
      "PDA TRI Bethesda Training — December 2026 Lodging"
    ),
    "PDA TRI Bethesda Training"
  );
  assert.equal(
    customerFacingOpportunityTitle(
      "ASAE Annual Meeting & Exposition 2029 — Washington, DC (Early Watch)"
    ),
    "ASAE Annual Meeting & Exposition 2029"
  );
  assert.equal(
    accountLevelDisplayTitle({
      title: "AMWA 2027 Interim Meeting — Sep 11–12",
      displayTitle: "AMWA 2027 Interim Meeting — Sep 11–12",
    }),
    "AMWA 2027 Interim Meeting"
  );
});

check("simplified_customer_facing_research_copy", () => {
  // Restore approved sanitizer behavior (do not invent a new summarizer):
  // strip provider brands from customer copy; keep commercial facts.
  const dirty = sanitizeOpportunityForCustomerPresentation({
    id: "gdi_opp_regression_copy",
    title: "AMWA 2027 Interim Meeting — Sep 11–12",
    summaryWhat:
      "Webhound / Parallel.ai notes: AMWA lists Interim Meeting for Bethesda lodging demand.",
    hotelOpportunityThesis: "Pursue room block near the meeting dates.",
  });
  assert.equal(dirty.displayTitle, "AMWA 2027 Interim Meeting");
  const summary = String(dirty.summaryWhat || "");
  assert.equal(/webhound|parallel\.ai/i.test(summary), false);
  assert.match(summary, /AMWA/i);
  assert.ok(summary.length > 0, "summary retained commercially");
});

check("market_scope_toolbar_height_tokens", () => {
  assert.match(css, /--gdi-toolbar-control-height:\s*3rem/);
  assert.match(
    css,
    /\.gdi-page\s+\.gdi-market-scope\s*\{[^}]*height:\s*var\(--gdi-toolbar-control-height\)/s
  );
  assert.match(
    css,
    /\.gdi-page\s+\.gdi-market-scope__btn\s*\{[^}]*height:\s*100%/s
  );
});

check("competitive_chip_and_status_control_removed", () => {
  assert.doesNotMatch(ui, /data-gdi-competitive-status/);
  assert.doesNotMatch(ui, /gdi-competitive-status__btn/);
  assert.match(ui, /data-gdi-market-scope/);
  assert.match(ui, /Competitor chip removed from Sales Opportunities/);
});

check("preserved_ui_polish", () => {
  assert.match(ui, /Group & Demand\\nOpportunities/);
  assert.match(ui, /function resetViewButtonHtml/);
  assert.match(ui, /filter-count-badge/);
  assert.match(app, /UI\.resetViewButtonHtml/);
  assert.match(css, /\.filter-count-badge/);
  assert.match(ui, /research-compact/);
  // "X updated" header counter must stay empty
  assert.match(
    ui,
    /function weeklyDeltaHeaderSummaryHtml\(\/\* counts \*\/\)[\s\S]*?return "";/
  );
  assert.match(
    html,
    /gdi-title-sourcing-consistency-v1|gdi-card-no-verification-pill-v1|gdi-card-no-market-scope-pill-v1|gdi-opp-type-classification-v1|gdi-regression-recovery-v1|gdi-component-parity-v1|gdi-three-way-canonical-v1/
  );
  assert.match(html, /gdi-guided-report-tour\.js/);
  // Card-level Market Scope action pills must stay off opportunity tiles.
  assert.doesNotMatch(
    ui,
    /gdi-pill-cmi-action[\s\S]{0,80}competitiveCommercialActionLabel/
  );
});

check("title_contract_module_present", () => {
  assert.ok(
    fs.existsSync(
      path.join(
        ROOT,
        "lib/group-demand-intelligence/customer-facing-opportunity-title-contract-v1.js"
      )
    )
  );
});

if (failed) {
  console.error(`\n${failed} failed`);
  process.exit(1);
}
console.log("\nAll GDI regression-recovery checks passed");
