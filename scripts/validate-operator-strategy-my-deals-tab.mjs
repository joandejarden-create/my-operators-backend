#!/usr/bin/env node
/**
 * Validates Operator Strategy native My Deals table UX.
 */

import { readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

function fail(msg) {
  console.error("FAIL:", msg);
  process.exitCode = 1;
}

function pass(msg) {
  console.log("PASS:", msg);
}

const html = readFileSync(join(root, "public/my-deals.html"), "utf8");
const js = readFileSync(join(root, "public/js/operator-strategy-my-deals.js"), "utf8");

const panelStart = html.indexOf('id="sectionOperatorStrategy"');
const panelEnd = html.indexOf('id="sectionContactedBrands"');
const panel = panelStart >= 0 ? html.slice(panelStart, panelEnd) : "";
const panelLower = panel.toLowerCase();

const htmlRequired = [
  'aria-label="Operator Strategy"',
  "operatorStrategySearchInput",
  "Search company or deal",
  "operatorStrategyAlignmentFilter",
  "All alignment signals",
  "operatorStrategyClearFiltersBtn",
  "Reset View",
  "operatorStrategyTable",
  "operatorStrategyTableBody",
  "Project / Deal",
  "Operating Company",
  "Project Location",
  "Score",
  "Review Status",
  "Key Consideration",
  "Data Confidence",
  "Call to Action",
  "operatorStrategyBulkActionsBtn",
  "operatorStrategyBulkDropdown",
  "operatorStrategySelectAllCheckbox",
  "cell-checkbox",
  "sort-indicator",
  'data-sort="projectName"',
  'data-sort="companyName"',
  'data-sort="location"',
  'data-sort="alignmentScoreOptional"',
  'data-sort="reviewStatus"',
  'data-sort="keyConsideration"',
  'data-sort="dataConfidence"',
  "Loading operator strategy",
  "Operator strategy rows will appear once operator alignment signals are available",
];

const htmlForbidden = [
  "operator-strategy-title",
  '<h2 class="operator-strategy-title">',
  "operator-strategy-subcopy",
  "Review operating companies under consideration across active deals",
  'id="operatorStrategyDealFilter"',
  'for="operatorStrategyDealFilter"',
  "operatorStrategyRefreshBtn",
  "Refresh Operator Strategy",
  "Operating Companies for Consideration",
  "operator-strategy-table-heading",
  "Switch deal",
  "Deal actions",
  "Operating Pathways to Validate",
  "operatorStrategyDealSelect",
  "operatorStrategySwitchDeal",
  "operatorStrategySummaryGrid",
  "Ready for Outreach",
];

const tableStart = panel.indexOf('id="operatorStrategyTable"');
const tableEnd = panel.indexOf("</table>", tableStart);
const operatorTable =
  tableStart >= 0 && tableEnd > tableStart
    ? panel.slice(tableStart, tableEnd)
    : "";

if (operatorTable.includes('class="col-cta" data-sort')) {
  fail("Call to Action column must not be sortable");
} else pass("CTA column has no data-sort");

const jsRequired = [
  "operator-strategy-row-checkbox",
  "updateBulkActionsState",
  "operatorStrategyBulkActionsBtn",
  "handleSort",
  "sortFilteredRows",
  "updateSortHeaderUI",
  "data-os-action=\"view-oas\"",
  "data-os-action=\"view-ocs\"",
  "data-os-action=\"open-profile\"",
  "data-os-action=\"more\"",
  "operator-strategy-more-menu",
  "aria-label",
  "loadPipeline",
  "preloadFromDeals",
  "onDealsLoaded",
  "setDealFilter",
  "onTabActivated",
  "operatorStrategyClearDealFilter",
  "Filtered to:",
  "Clear filter",
];

const jsForbidden = [
  'getElementById("operatorStrategyDealFilter")',
  "operatorStrategyRefreshBtn",
  "populateDealFilterOptions",
  'class="deal-row-checkbox"',
];

if (!js.includes("Add to Operator Review") || !js.includes("Prepare Outreach")) {
  fail("more menu must include disabled review/outreach labels");
} else pass("more menu disabled actions documented in JS");

const banned = [
  /recommended operators/i,
  /best operators/i,
  /top operators/i,
  /preferred operators/i,
  /dealality recommends/i,
];

for (const s of htmlRequired) {
  if (!panel.includes(s)) fail("panel missing: " + s);
  else pass("panel has " + s);
}

for (const s of htmlForbidden) {
  if (panelLower.includes(s.toLowerCase())) fail("panel should not include: " + s);
  else pass("panel absent " + s);
}

for (const s of jsRequired) {
  if (!js.includes(s)) fail("JS missing: " + s);
  else pass("JS has " + s);
}

if (!js.includes('String(row.dealId || "") + "|" + String(row.operatorId || "")')) {
  fail("rowKey must dedupe by dealId + operatorId (not operatorId only)");
} else {
  pass("rowKey uses dealId + operatorId");
}

for (const s of jsForbidden) {
  if (js.includes(s)) fail("JS should not include: " + s);
  else pass("JS absent " + s);
}

if (js.includes('data-os-action="add-review"') && js.includes('title="View Operator Alignment Snapshot"')) {
  fail("disabled CTAs should be in more menu, not inline add-review button");
} else pass("disabled CTAs not inline in CTA row");

if (!js.includes("loadPipeline(true)")) {
  fail("pipeline should support forced reload (loadPipeline(true))");
} else pass("forced pipeline reload supported");

if (!js.includes("preloadFromDeals")) {
  fail("missing preloadFromDeals for page-load hydration");
} else pass("preloadFromDeals present");

if (!html.includes("preloadFromDeals") && !html.includes("operator-strategy-my-deals.js?v=")) {
  fail("my-deals.html should cache-bust operator-strategy-my-deals.js");
} else pass("operator-strategy script cache-bust on my-deals");

for (const re of banned) {
  if (re.test(panel)) fail("banned in panel copy: " + re);
  else pass("no banned in panel " + re);
}

try {
  readFileSync(join(root, "docs/operator-strategy-my-deals-tab.md"), "utf8");
  pass("docs exist");
} catch {
  fail("missing docs/operator-strategy-my-deals-tab.md");
}

if (process.exitCode) {
  console.error("\nValidation failed.");
  process.exit(1);
}
console.log("\nAll Operator Strategy native table checks passed.");
