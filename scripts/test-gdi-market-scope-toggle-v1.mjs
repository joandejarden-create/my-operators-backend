#!/usr/bin/env node
/**
 * GDI Market Scope toggle V1 — population control is orthogonal to demand types.
 * All / Hotel / Competitive; Opportunity Type dropdown stays demand-category only.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import vm from "node:vm";
import {
  annotateInventoryPopulations,
  BETHESDA_HOTEL_ID,
  buildInventoryPopulationCounts,
  selectBethesdaSalesOpportunityInventory,
} from "../lib/group-demand-intelligence/bethesda-67-sales-baseline-cmi-v1.js";
import { mergeBethesdaInternalResearchOpportunities } from "../lib/group-demand-intelligence/research/bethesda-internal-research-union-v1.js";
import {
  filterOpportunitiesByCommercialSurface,
  GDI_LIST_SURFACE_VIEW,
} from "../lib/group-demand-intelligence/gdi-commercial-surface-disposition-v1.js";
import { applyBethesdaCommercialEventFamilyPresentation } from "../lib/group-demand-intelligence/research/bethesda-88-commercial-event-family-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

function loadUi() {
  const src = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
    "utf8"
  );
  const sandbox = { window: {}, globalThis: {}, console, URL, URLSearchParams };
  sandbox.window = sandbox;
  sandbox.globalThis = sandbox;
  vm.runInNewContext(src, sandbox, { filename: "dealality-gdi-ui.js" });
  return sandbox.DealalityGdiUi;
}

const UI = loadUi();
assert.ok(UI?.MARKET_SCOPE, "MARKET_SCOPE exported");
assert.ok(UI.marketScopeFilterHtml, "marketScopeFilterHtml exported");
assert.ok(UI.filterOpportunities, "filterOpportunities exported");

const checks = [];
function check(name, fn) {
  try {
    fn();
    checks.push({ name, ok: true });
    console.log(`PASS ${name}`);
  } catch (err) {
    checks.push({ name, ok: false, err });
    console.error(`FAIL ${name}`);
    console.error(err);
  }
}

check("toolbar_html_places_market_scope_after_opportunity_type", () => {
  const html = UI.propertyBarHtml({
    mode: "auth",
    hotelId: BETHESDA_HOTEL_ID,
    hotels: [{ hotelId: BETHESDA_HOTEL_ID, hotelName: "Bethesda Marriott" }],
    activeOpportunityType: "",
    opportunityTypeOptions: [{ value: "", label: "All types", count: 10 }],
    activeMarketScope: "ALL",
    marketScopeCounts: { ALL: 10, HOTEL: 9, COMPETITIVE: 2 },
    showWeeklyFilter: true,
    weeklyCounts: { all: 10 },
  });
  const typeIdx = html.indexOf("gdiOpportunityTypeFilter");
  const scopeIdx = html.indexOf("gdi-market-scope");
  assert.ok(typeIdx >= 0, "opportunity type present");
  assert.ok(scopeIdx > typeIdx, "market scope after opportunity type");
  assert.match(html, /Market Scope/);
  assert.match(html, /data-gdi-market-scope="ALL"/);
  assert.match(html, /data-gdi-market-scope="HOTEL"/);
  assert.match(html, /data-gdi-market-scope="COMPETITIVE"/);
  assert.doesNotMatch(html, /Sales Opportunities \(/);
  assert.doesNotMatch(html, /All Commercial Intelligence/);
});

check("opportunity_type_options_exclude_population_controls", () => {
  const opts = UI.opportunityTypeFilterOptionsFromCounts({
    all: 5,
    SCALING_COMPANY: 2,
    ASSOCIATION_CONFERENCE_GROUP: 3,
    SALES_OPPORTUNITIES: 5,
    COMPETITIVE_MARKET_INTELLIGENCE: 1,
    ALL_COMMERCIAL_INTELLIGENCE: 5,
  });
  assert.equal(opts[0].value, "");
  assert.equal(opts[0].label, "All types");
  assert.ok(opts.every((o) => !UI.INVENTORY_POPULATION_VALUES || true));
  assert.ok(
    !opts.some((o) =>
      /SALES_OPPORTUNITIES|COMPETITIVE_MARKET_INTELLIGENCE|ALL_COMMERCIAL_INTELLIGENCE/.test(
        o.value
      )
    )
  );
});

check("legacy_population_opportunityType_migrates_to_marketScope", () => {
  const f = UI.normalizeBrowsePopulationFilters({
    opportunityType: "COMPETITIVE_MARKET_INTELLIGENCE",
  });
  assert.equal(f.marketScope, "COMPETITIVE");
  assert.equal(f.opportunityType, "");
});

check("default_reset_is_all_opportunities", () => {
  const reset = UI.resetBrowseFilters();
  assert.equal(reset.marketScope, "ALL");
  assert.equal(reset.opportunityType, "");
  const ctrlSrc = fs.readFileSync(
    path.join(
      ROOT,
      "public/js/group-demand-intelligence/gdi-universal-presentation-controller.js"
    ),
    "utf8"
  );
  assert.match(ctrlSrc, /marketScope:\s*"ALL"/);
  assert.match(ctrlSrc, /opportunityType:\s*""/);
});

check("filter_dedupes_and_scopes_populations", () => {
  const rows = [
    {
      id: "hotel-only",
      inventoryPopulations: ["SALES_OPPORTUNITIES"],
      primaryCommercialEligible: true,
      commercialSurfaceDisposition: "COMMERCIAL_OPPORTUNITY",
    },
    {
      id: "dual",
      inventoryPopulations: [
        "SALES_OPPORTUNITIES",
        "COMPETITIVE_MARKET_INTELLIGENCE",
      ],
      cmiVerified: true,
      primaryCommercialEligible: true,
      commercialSurfaceDisposition: "COMMERCIAL_OPPORTUNITY",
    },
    {
      id: "cmi-only",
      inventoryPopulations: ["COMPETITIVE_MARKET_INTELLIGENCE"],
      cmiVerified: true,
      primaryCommercialEligible: true,
      commercialSurfaceDisposition: "COMMERCIAL_OPPORTUNITY",
    },
  ];
  const all = UI.filterOpportunities(rows, { marketScope: "ALL" });
  assert.equal(all.length, 3);
  const hotel = UI.filterOpportunities(rows, { marketScope: "HOTEL" });
  assert.deepEqual(
    hotel.map((o) => o.id).sort(),
    ["dual", "hotel-only"]
  );
  const competitive = UI.filterOpportunities(rows, {
    marketScope: "COMPETITIVE",
  });
  assert.deepEqual(
    competitive.map((o) => o.id).sort(),
    ["cmi-only", "dual"]
  );
  const counts = UI.countByMarketScope(rows);
  assert.equal(counts.ALL, 3);
  assert.equal(counts.HOTEL, 2);
  assert.equal(counts.COMPETITIVE, 2);
});

check("bethesda_verified_cmi_ledger_counts_via_market_scope", () => {
  const doc = JSON.parse(
    fs.readFileSync(
      path.join(
        ROOT,
        `data/group-demand-intelligence/hotels/${BETHESDA_HOTEL_ID}/opportunities.json`
      ),
      "utf8"
    )
  );
  const union = mergeBethesdaInternalResearchOpportunities(
    BETHESDA_HOTEL_ID,
    doc.opportunities || []
  );
  const family = applyBethesdaCommercialEventFamilyPresentation(
    union.opportunities,
    { hideLinkedFromList: true }
  );
  const surface = filterOpportunitiesByCommercialSurface(
    family.opportunities,
    GDI_LIST_SURFACE_VIEW.COMMERCIAL
  );
  const selected = selectBethesdaSalesOpportunityInventory(surface, {
    salesTeamInventory: true,
    founderReview: true,
  }).selected;
  const annotated = annotateInventoryPopulations(selected, {
    hotelId: BETHESDA_HOTEL_ID,
  });
  const libCounts = buildInventoryPopulationCounts(annotated);
  assert.equal(libCounts.salesOpportunities, 67);
  assert.equal(libCounts.competitiveMarketIntelligence, 4);
  assert.equal(libCounts.allCommercialIntelligence, 67);

  const uiCounts = UI.countByMarketScope(annotated);
  assert.equal(uiCounts.ALL, 67);
  assert.equal(uiCounts.HOTEL, 67);
  assert.equal(uiCounts.COMPETITIVE, 4);

  const all = UI.filterOpportunities(annotated, { marketScope: "ALL" });
  const hotel = UI.filterOpportunities(annotated, { marketScope: "HOTEL" });
  const cmi = UI.filterOpportunities(annotated, {
    marketScope: "COMPETITIVE",
  });
  assert.equal(all.length, 67);
  assert.equal(hotel.length, 67);
  assert.equal(cmi.length, 4);
  assert.equal(new Set(all.map((o) => o.id)).size, 67);
  assert.ok(cmi.every((o) => o.cmiVerified === true));
});

check("css_and_apps_wire_market_scope", () => {
  const css = fs.readFileSync(
    path.join(ROOT, "public/css/group-demand-intelligence.css"),
    "utf8"
  );
  assert.match(css, /\.gdi-market-scope/);
  const app = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/app.js"),
    "utf8"
  );
  const share = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/share-app.js"),
    "utf8"
  );
  assert.match(app, /data-gdi-market-scope/);
  assert.match(share, /data-gdi-market-scope/);
  assert.match(app, /activeMarketScope/);
  assert.match(share, /activeMarketScope/);
});

const failed = checks.filter((c) => !c.ok);
if (failed.length) {
  console.error(`\n${failed.length}/${checks.length} failed`);
  process.exit(1);
}
console.log(`\n${checks.length}/${checks.length} passed`);
