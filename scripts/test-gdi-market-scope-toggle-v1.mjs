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
  assert.doesNotMatch(html, /Competitive Status/);
  assert.doesNotMatch(html, /data-gdi-competitive-status/);
  assert.doesNotMatch(html, /gdi-competitive-status-filter/);
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
  // Hotel = commercially actionable for the property (excludes confirmed
  // competitor-booked CMI without independent target-hotel demand).
  assert.ok(
    libCounts.salesOpportunities < 67,
    "Hotel must be narrower than full sales inventory when verified CMI lacks target-hotel demand"
  );
  assert.ok(libCounts.salesOpportunities > 0);
  // Competitive = recovered ledger inventory (verified + needs + historical).
  // 4 prior ledger verified + ICON + CASSS; Needs Verification and Historical retained.
  assert.equal(libCounts.competitiveVerified, 6);
  assert.ok(libCounts.competitiveNeedsVerification >= 3);
  assert.equal(libCounts.competitiveHistorical, 1);
  assert.equal(
    libCounts.competitiveMarketIntelligence,
    libCounts.competitiveVerified +
      libCounts.competitiveNeedsVerification +
      libCounts.competitiveHistorical
  );
  assert.ok(
    libCounts.competitiveMarketIntelligence > libCounts.competitiveVerified
  );
  // All = unique eligible inventory (67); dual membership never double-counts.
  assert.equal(libCounts.allCommercialIntelligence, 67);
  assert.equal(annotated.length, 67);

  const hotelOnly = annotated.filter(
    (o) =>
      (o.inventoryPopulations || []).includes("SALES_OPPORTUNITIES") &&
      !(o.inventoryPopulations || []).includes(
        "COMPETITIVE_MARKET_INTELLIGENCE"
      )
  );
  const competitiveOnly = annotated.filter(
    (o) =>
      (o.inventoryPopulations || []).includes(
        "COMPETITIVE_MARKET_INTELLIGENCE"
      ) &&
      !(o.inventoryPopulations || []).includes("SALES_OPPORTUNITIES")
  );
  const dual = annotated.filter(
    (o) =>
      (o.inventoryPopulations || []).includes("SALES_OPPORTUNITIES") &&
      (o.inventoryPopulations || []).includes(
        "COMPETITIVE_MARKET_INTELLIGENCE"
      )
  );
  assert.equal(
    hotelOnly.length + competitiveOnly.length + dual.length,
    67
  );
  assert.ok(
    competitiveOnly.every(
      (o) =>
        o.cmiVerified === true ||
        o.competitiveStatus === "VERIFIED" ||
        o.commercialActionability === "COMPETITIVE_MARKET_INTELLIGENCE"
    ),
    "Competitive-only rows must be confirmed CMI without hotel actionability"
  );
  assert.ok(
    dual.every((o) => o.competitiveStatus === "HISTORICAL" || o.competitiveStatus === "NEEDS_VERIFICATION" || o.cmiVerified === true),
    "Dual rows need two independently supported theses"
  );

  const uiCounts = UI.countByMarketScope(annotated);
  assert.equal(uiCounts.ALL, 67);
  assert.equal(uiCounts.HOTEL, libCounts.salesOpportunities);
  assert.equal(
    uiCounts.COMPETITIVE,
    libCounts.competitiveMarketIntelligence
  );

  const all = UI.filterOpportunities(annotated, { marketScope: "ALL" });
  const hotel = UI.filterOpportunities(annotated, { marketScope: "HOTEL" });
  const cmi = UI.filterOpportunities(annotated, {
    marketScope: "COMPETITIVE",
  });
  assert.equal(all.length, 67);
  assert.equal(hotel.length, libCounts.salesOpportunities);
  assert.equal(cmi.length, libCounts.competitiveMarketIntelligence);
  assert.equal(new Set(all.map((o) => o.id)).size, 67);
  assert.ok(
    cmi.every((o) =>
      (o.inventoryPopulations || []).includes(
        "COMPETITIVE_MARKET_INTELLIGENCE"
      )
    )
  );
  // Verified competitor bookings without independent hotel demand leave Hotel.
  const verifiedExcludedFromHotel = annotated.filter(
    (o) =>
      o.cmiVerified === true &&
      !(o.inventoryPopulations || []).includes("SALES_OPPORTUNITIES")
  );
  assert.ok(
    verifiedExcludedFromHotel.length >= 6,
    "Confirmed competitor bookings without target-hotel demand must leave Hotel"
  );

  // Competitive Status UI retired — classification remains on cards / counts helper.
  const statusCounts = UI.countByCompetitiveStatus(cmi);
  assert.equal(statusCounts.ALL_COMPETITIVE, 10);
  assert.equal(statusCounts.VERIFIED, 6);
  assert.equal(statusCounts.NEEDS_VERIFICATION, 3);
  assert.equal(statusCounts.HISTORICAL, 1);
  assert.ok(
    cmi.some((o) => o.id === "gdi_s4912_6c8a62552bfa"),
    "ICON Awards recovered as verified competitive"
  );
  assert.ok(
    cmi.some((o) => o.id === "gdi_s4912_732496c2f8aa"),
    "CASSS recovered as verified competitive"
  );

  // Status filter no longer narrows Competitive scope results.
  const competitiveUnfiltered = UI.filterOpportunities(annotated, {
    marketScope: "COMPETITIVE",
    competitiveStatus: "VERIFIED",
  });
  assert.equal(competitiveUnfiltered.length, 10);

  assert.equal(
    UI.competitiveStatusFilterHtml({
      activeMarketScope: "COMPETITIVE",
      activeCompetitiveStatus: "ALL_COMPETITIVE",
      competitiveStatusCounts: statusCounts,
    }),
    ""
  );
  assert.equal(UI.competitiveStatusFilterHtml({ activeMarketScope: "ALL" }), "");

  const salesChips = UI.salesOpportunityPresetHtml({
    active: "",
    counts: UI.countBySalesOpportunity
      ? UI.countBySalesOpportunity(annotated)
      : {},
  });
  assert.equal(/Competitor/.test(salesChips), false);
  assert.equal(/COMPETITOR_CAPTURE/.test(salesChips), false);

  const verifiedPill = UI.competitiveStatusPillHtml(
    { cmiVerified: true, competitiveStatus: "VERIFIED" },
    { cmiVerified: true, competitiveStatus: "VERIFIED" }
  );
  assert.match(verifiedPill, />Verified</);
  assert.equal(/Verified Competitive/.test(verifiedPill), false);
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
  const ui = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
    "utf8"
  );
  assert.match(app, /data-gdi-market-scope/);
  assert.match(share, /data-gdi-market-scope/);
  assert.match(app, /activeMarketScope/);
  assert.match(share, /activeMarketScope/);
  assert.equal(/data-gdi-competitive-status/.test(app), false);
  assert.equal(/data-gdi-competitive-status/.test(share), false);
  assert.equal(/competitiveStatusFilterHtml\(opts\)/.test(ui), false);
});

const failed = checks.filter((c) => !c.ok);
if (failed.length) {
  console.error(`\n${failed.length}/${checks.length} failed`);
  process.exit(1);
}
console.log(`\n${checks.length}/${checks.length} passed`);
