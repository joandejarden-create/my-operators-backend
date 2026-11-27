#!/usr/bin/env node
/**
 * GDI Executive Summary simplification + Admin dashboards + shared tiles.
 * DEALALITY_ADDITIVE_NON_REGRESSION_V1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildGdiExecutiveSummaryV2,
  stripResearchIntelligencePerspective,
  prioritizeCustomerOpportunitiesV2,
  GDI_EXECUTIVE_SUMMARY_V2,
} from "../lib/group-demand-intelligence/executive-summary/gdi-executive-summary-v2.js";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");

function read(rel) {
  return fs.readFileSync(path.join(root, rel), "utf8");
}

const esTab = read("public/js/group-demand-intelligence/executive-summary-tab.js");
assert.equal(/data-perspective/.test(esTab), false, "ES tab must not render secondary perspective tabs");
assert.equal(/Executive Overview/.test(esTab), false, "no Executive Overview secondary tab label");
assert.match(esTab, /opportunityCardsGridHtml/);
assert.match(esTab, /DealalityGdiUi/);
assert.match(esTab, /renderPrioritySharedTiles/);
assert.match(esTab, /Research Receipt/);
assert.match(esTab, /consolidated-customer/);

const appJs = read("public/app.js");
assert.match(appJs, /GDI Sales Performance/);
assert.match(appJs, /GDI Research Intelligence/);
assert.match(appJs, /\/admin\/gdi-sales-performance/);
assert.match(appJs, /\/admin\/gdi-research-intelligence/);

assert.ok(fs.existsSync(path.join(root, "public/app/admin/gdi-sales-performance.html")));
assert.ok(fs.existsSync(path.join(root, "public/app/admin/gdi-research-intelligence.html")));
assert.ok(fs.existsSync(path.join(root, "public/js/admin-gdi-dashboards.js")));

const salesHtml = read("public/app/admin/gdi-sales-performance.html");
const riHtml = read("public/app/admin/gdi-research-intelligence.html");
assert.match(salesHtml, /SupportAdminGate/);
assert.match(riHtml, /SupportAdminGate/);
assert.match(salesHtml, /Access restricted/);
assert.match(riHtml, /Access restricted/);

const ui = read("public/js/group-demand-intelligence/dealality-gdi-ui.js");
assert.match(ui, /opportunityTileHtml:\s*opportunityTileHtml/);
assert.match(ui, /opportunityCardsGridHtml:\s*opportunityCardsGridHtml/);

const sharedDoc = read("docs/ai-build-system/GDI_SHARED_OPPORTUNITY_TILE_COMPONENT_V1.md");
assert.match(sharedDoc, /opportunityTileHtml/);

const store = createGdiPublicationPackageStore({});
const ptr = store.readPointer("recnPB3Q02NO8Ma1o", "STAGED");
assert.ok(ptr?.publicationVersion, "Wyndham STAGED required");
const loaded = store.loadPackage("recnPB3Q02NO8Ma1o", ptr.publicationVersion);
assert.equal(loaded.ok, true);
assert.equal(loaded.package.opportunities.length, 26, "Wyndham STAGED 26 preserved");

const withIntel = buildGdiExecutiveSummaryV2({
  hotelId: "recnPB3Q02NO8Ma1o",
  hotelName: "Wyndham San Diego Bayside",
  opportunities: loaded.package.opportunities,
  founderPreview: true,
  customerAuthorizedInventory: false,
  dataState: "FOUNDER_PREVIEW",
  publicationMeta: {
    publicationVersion: ptr.publicationVersion,
    createdAt: loaded.package.createdAt,
  },
  includeResearchIntelligence: true,
});

assert.equal(withIntel.version, GDI_EXECUTIVE_SUMMARY_V2);
assert.ok(withIntel.sections.salesPerformance);
assert.ok(withIntel.sections.researchIntelligence);
assert.equal(withIntel.sections.priorityCommercialOpportunities.max, 4);
assert.ok(
  withIntel.sections.priorityCommercialOpportunities.opportunityRows.length <= 4,
);
assert.equal(
  withIntel.sections.priorityCommercialOpportunities.sharedTileComponent,
  "DealalityGdiUi.opportunityTileHtml",
);

const pri = prioritizeCustomerOpportunitiesV2(loaded.package.opportunities, 4);
assert.ok(pri.length <= 4);

const stripped = stripResearchIntelligencePerspective(withIntel);
assert.equal(stripped.sections.researchIntelligence, undefined);
assert.equal(stripped.sections.salesPerformance, undefined);
assert.deepEqual(stripped.perspectives.available, ["EXECUTIVE_OVERVIEW"]);
assert.equal(stripped.perspectives.secondaryTabsRemoved, true);
assert.ok(stripped.sections.researchProductivity);
assert.ok(stripped.sections.priorityCommercialOpportunities);
assert.ok(
  stripped.sections.executiveOverview?.customerResearchReceipt ||
    stripped.sections.researchReceipt,
);

console.log(
  JSON.stringify(
    {
      ok: true,
      wyndhamStagedCount: 26,
      priorityMax: 4,
      priorityRows: withIntel.sections.priorityCommercialOpportunities.opportunityRows
        .length,
      sharedTile:
        withIntel.sections.priorityCommercialOpportunities.sharedTileComponent,
      customerStrippedSales: stripped.sections.salesPerformance === undefined,
      customerStrippedRi: stripped.sections.researchIntelligence === undefined,
    },
    null,
    2,
  ),
);
