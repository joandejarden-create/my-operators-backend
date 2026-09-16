/**
 * GDI Dealality UI unification regressions.
 * Classification: REUSABLE_PRODUCT_UI tests — no research/scoring changes.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import vm from "node:vm";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");

function loadUi() {
  const src = fs.readFileSync(
    path.join(root, "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
    "utf8"
  );
  const sandbox = { window: {}, console };
  sandbox.window = sandbox;
  vm.runInNewContext(src, sandbox);
  assert.ok(sandbox.DealalityGdiUi, "DealalityGdiUi missing");
  return sandbox.DealalityGdiUi;
}

function read(rel) {
  return fs.readFileSync(path.join(root, rel), "utf8");
}

const UI = loadUi();
let failed = 0;

function check(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}`);
    console.error(err && err.stack ? err.stack : err);
  }
}

check("product_page_header_adp_pattern", () => {
  const html = UI.hotelShellHtml({
    mode: "auth",
    badges: {},
  });
  assert.match(html, /Group Demand Intelligence/);
  assert.match(html, /Methodology/);
  assert.match(html, /aiv-dashboard-header/);
  assert.match(html, /aiv-disclaimer-box/);
  assert.match(html, /aiv-header-subtitle/);
  assert.doesNotMatch(html, /Bethesda/);
  assert.doesNotMatch(html, /gdi-demand-parent/);
  assert.doesNotMatch(html, /gdi-module-nav/);
  assert.doesNotMatch(html, /Demand Positioning/);
});

check("property_bar_keeps_research_status", () => {
  const html = UI.propertyBarHtml({
    mode: "auth",
    hotel: {
      hotelName: "Harborview Conference Hotel",
      city: "Charleston",
      state: "South Carolina",
    },
    hotelId: "recSyntheticAlpha",
    lastResearch: "Sep 13, 2026",
    runStatus: "COMPLETED",
    actionHtml: '<button type="button" id="gdiRunBtn">Run Research</button>',
  });
  assert.match(html, /Harborview Conference Hotel — Charleston, South Carolina/);
  assert.match(html, /Last Research/);
  assert.match(html, /Sep 13, 2026/);
  assert.match(html, /Run Status/);
  assert.match(html, /COMPLETED/);
  assert.match(html, /gdi-property-stat__value/);
  assert.match(html, /gdiRunBtn/);
  assert.match(html, /filters-section/);
  assert.match(html, /aiv-filters-section/);
  assert.doesNotMatch(html, /filter-select gdi-filter-static/);
  assert.doesNotMatch(html, /Bethesda/);
});

check("pilot_badge_optional", () => {
  const withBadge = UI.hotelShellHtml({
    badges: { pilot: true },
    mode: "auth",
  });
  const without = UI.hotelShellHtml({
    badges: {},
    mode: "auth",
  });
  assert.match(withBadge, /PILOT/);
  assert.doesNotMatch(without, /PILOT/);
  assert.doesNotMatch(without, /Read-only/);
});

check("primary_kpi_strip_from_arbitrary_hotel", () => {
  const opps = JSON.parse(
    read("fixtures/group-demand-intelligence/synthetic-hotel-alpha/opportunities.json")
  ).opportunities;
  const metrics = UI.buildMetricModel({
    summary: {
      qualifiedCount: 2,
      highPriorityCount: 1,
      mediumPriorityCount: 0,
      watchlistCount: 1,
      validated: 0,
      pendingValidation: 2,
      confirmedNew: 0,
      alreadyKnown: 0,
      worthPursuingNow: 0,
      notRelevant: 0,
    },
    opportunities: opps,
  });
  assert.equal(metrics.primary.length <= 5, true);
  const html = UI.primaryKpiHtml(metrics.primary);
  assert.match(html, /aiv-kpi/);
  assert.match(html, /Opportunities/);
  assert.match(html, /High Priority/);
  assert.match(html, /Medium Priority/);
  assert.doesNotMatch(html, /Pursue Now/);
  const terr = UI.secondarySummaryHtml("Demand Territory", metrics.territories);
  assert.match(terr, /Charleston Core/);
  assert.match(terr, /Regional Stretch/);
  assert.doesNotMatch(terr, /Bethesda/);
});

check("weekly_brief_action_first", () => {
  const card = UI.weeklyBriefCardHtml(
    {
      opportunityId: "gdi_opp_synth_alpha_1",
      title: "Southeast Clinical Leaders Summit 2027",
      organizationName: "Coastal Clinical Society",
      segment: "Medical",
      eventStartDate: "2027-04-12",
      eventEndDate: "2027-04-14",
      hotelFitScore: 81,
      evidenceConfidence: 72,
      whyNow: "Venue TBD",
      recommendedNextStep: "Introduce inventory",
      primaryContact: { name: "Alex Rivera", role: "Meetings Manager", email: "a@b.c" },
    },
    {
      priority: "HIGH_PRIORITY",
      opportunityQualificationLabel: "Strong",
      eventStartDate: "2027-04-12",
      eventEndDate: "2027-04-14",
    }
  );
  assert.match(card, /gdi-brief-card--action/);
  assert.match(card, /gdi-brief-card--high/);
  assert.match(card, /Apr 12-14, 2027/);
  assert.match(card, /Why Now/);
  assert.match(card, /Recommended Action/);
  assert.match(card, /View Details/);
  assert.doesNotMatch(card, /Opportunity Type/);
});

check("main_tab_label_two_lines", () => {
  const tabs = UI.contentTabsHtml("opportunities", [UI.GDI_MAIN_TAB]);
  assert.match(tabs, /Group Demand<br>Intelligence/);
  assert.doesNotMatch(tabs, /Weekly Brief/);
});

check("priority_presets_and_results_toolbar", () => {
  const counts = UI.countByPriority([
    { priority: "HIGH_PRIORITY" },
    { priority: "HIGH_PRIORITY" },
    { priority: "WATCHLIST" },
  ]);
  assert.equal(counts[""], 3);
  assert.equal(counts.HIGH_PRIORITY, 2);
  assert.equal(counts.WATCHLIST, 1);
  const presets = UI.priorityPresetHtml({
    active: "HIGH_PRIORITY",
    counts,
  });
  assert.match(presets, /chain-scale-legend/);
  assert.match(presets, /chain-scale-legend-item/);
  assert.match(presets, /chain-scale-legend-swatch/);
  assert.match(presets, /data-gdi-priority="HIGH_PRIORITY"/);
  assert.match(presets, /aria-label="Priority filters"/);
  assert.match(presets, />All \(3\)/);
  assert.match(presets, />High \(2\)/);
  assert.match(presets, /Medium/);
  assert.match(presets, /Watch/);
  assert.match(presets, / active/);
  assert.doesNotMatch(presets, /data-gdi-booking=/);
  assert.doesNotMatch(presets, /Bethesda/);
  const actionCounts = UI.countByActionStatus([
    { bookingWindowStatus: "CONTACT_NOW" },
    { bookingWindowStatus: "CONTACT_NOW" },
    { bookingWindowStatus: "WATCH" },
  ]);
  const actionPresets = UI.actionPresetHtml({
    active: "CONTACT_NOW",
    counts: actionCounts,
  });
  assert.match(actionPresets, /Action Window/);
  assert.match(actionPresets, /data-gdi-booking="CONTACT_NOW"/);
  assert.match(actionPresets, /Pursue Now \(2\)/);
  assert.match(actionPresets, />All \(3\)/);
  const chrome = UI.opportunityBrowseChromeHtml({
    activePriority: "HIGH_PRIORITY",
    priorityCounts: counts,
    activeBooking: "CONTACT_NOW",
    actionCounts: actionCounts,
    shown: 2,
    total: 3,
    sort: "hotelFitScore",
    viewMode: "tiles",
    noun: "Opportunities",
  });
  assert.match(chrome, /gdi-browse/);
  assert.match(chrome, /gdi-browse-presets/);
  assert.match(chrome, /Action Window/);
  assert.match(chrome, /data-gdi-priority="HIGH_PRIORITY"/);
  assert.match(chrome, /data-gdi-booking="CONTACT_NOW"/);
  assert.doesNotMatch(chrome, /gdiFilterSegment/);
  assert.doesNotMatch(chrome, /All segments/);
  const toolbar = UI.resultsToolbarHtml({
    shown: 2,
    total: 3,
    sort: "hotelFitScore",
    viewMode: "tiles",
    noun: "Opportunities",
  });
  assert.match(toolbar, /results-toolbar/);
  assert.match(toolbar, /results-count/);
  assert.match(toolbar, /Showing/);
  assert.match(toolbar, /Opportunities/);
  assert.doesNotMatch(toolbar, /brief items/);
  assert.match(toolbar, /sort-select/);
  assert.match(toolbar, /gdiSortSelect/);
  assert.match(toolbar, /sort-icon/);
  assert.match(toolbar, /data-gdi-view="tiles"/);
  const listGrid = UI.opportunityCardsGridHtml(
    [
      {
        id: "gdi_opp_a",
        title: "Alpha Summit",
        summaryWhat: "Open venue selection for a coastal clinical meeting.",
        priority: "HIGH_PRIORITY",
        bookingWindowStatus: "CONTACT_NOW",
      },
    ],
    "list"
  );
  assert.match(listGrid, /gdi-results-grid--list/);
  assert.match(listGrid, /data-gdi-view-mode="list"/);
  assert.match(listGrid, /brand-card--gdi-opp/);
  assert.match(listGrid, /gdi-priority-high/);
  assert.match(listGrid, /Alpha Summit/);
  assert.match(listGrid, /Open venue selection/);
  assert.match(listGrid, /View Details/);
  assert.doesNotMatch(listGrid, /gdi-brief-card/);
});

check("opportunity_tile_and_sort_filter", () => {
  const tile = UI.opportunityTileHtml(
    {
      opportunityId: "gdi_opp_synth_alpha_1",
      title: "Southeast Clinical Leaders Summit 2027",
      organizationName: "Coastal Clinical Society",
      segment: "Medical",
      eventStartDate: "2027-04-20",
      eventEndDate: "2027-04-23",
      hotelFitScore: 81,
      evidenceConfidence: 72,
      summaryWhat:
        "Multi-day coastal clinical summit with open venue selection.",
      whyNow: "Venue selection still open for coastal medical planners.",
      primaryContact: {
        name: "Alex Rivera",
        email: "a@b.c",
        phone: "+1 555-0100",
      },
    },
    {
      priority: "HIGH_PRIORITY",
      bookingWindowStatus: "CONTACT_NOW",
      eventStartDate: "2027-04-20",
      eventEndDate: "2027-04-23",
      summaryWhat:
        "Multi-day coastal clinical summit with open venue selection.",
    }
  );
  assert.match(tile, /brand-card/);
  assert.match(tile, /brand-card--gdi-opp/);
  assert.match(tile, /gdi-priority-high/);
  assert.match(tile, /brand-card__name/);
  assert.match(tile, /gdi-tile-pills/);
  assert.doesNotMatch(tile, /gdi-pill-high/);
  assert.match(tile, /gdi-pill-action-pursue/);
  assert.match(tile, /gdi-pill-date/);
  assert.match(tile, /PURSUE NOW/);
  assert.match(tile, /Apr 20-23, 2027/);
  assert.match(tile, /data-gdi-tile-booking="CONTACT_NOW"/);
  assert.doesNotMatch(tile, /brand-card__type/);
  assert.match(tile, /Southeast Clinical Leaders Summit/);
  assert.doesNotMatch(tile, /brand-card__chip/);
  const titleMatch = tile.match(/<div class="brand-card__name">([^<]*)<\/div>/);
  assert.ok(titleMatch);
  assert.doesNotMatch(titleMatch[1], /2027/);
  assert.match(tile, /Multi-day coastal clinical summit/);
  assert.doesNotMatch(tile, /Venue selection still open/);
  assert.doesNotMatch(tile, /2027-04-20/);
  assert.equal(
    UI.scrubEventDatesFromText(
      "18th Annual NICE Conference and Expo 2027 — location TBD (Jun 7-9)"
    ),
    "18th Annual NICE Conference and Expo — location TBD"
  );
  assert.equal(
    UI.scrubEventDatesFromText(
      "NIST NICE 18th Annual Conference and Expo, June 7–9, 2027; location TBA."
    ),
    "NIST NICE 18th Annual Conference and Expo; location TBA."
  );
  assert.equal(
    UI.eventDateRangeLabel("2027-04-20", "2027-04-20"),
    "Apr 20-20, 2027"
  );
  assert.equal(
    UI.eventDateRangeLabel("2027-04-20", "2027-05-02"),
    "Apr 20-May 2, 2027"
  );
  assert.match(tile, /Alex Rivera/);
  assert.match(tile, /a@b\.c/);
  assert.match(tile, /\+1 555-0100/);
  assert.match(tile, /brand-card__footer--split/);
  assert.match(tile, /View Details/);
  assert.doesNotMatch(tile, /brand-card__logo/);
  assert.doesNotMatch(tile, /Fit 81/);
  assert.doesNotMatch(tile, /Evidence 72/);
  assert.doesNotMatch(tile, /Bethesda/);
  const sorted = UI.sortOpportunities(
    [
      { title: "B", priority: "WATCHLIST", hotelFitScore: 50 },
      { title: "A", priority: "HIGH_PRIORITY", hotelFitScore: 90 },
    ],
    "priority",
    1
  );
  assert.equal(sorted[0].title, "A");
  const filtered = UI.filterOpportunities(
    [
      { priority: "HIGH_PRIORITY", segment: "Medical" },
      { priority: "WATCHLIST", segment: "Sports" },
    ],
    { priority: "HIGH_PRIORITY" }
  );
  assert.equal(filtered.length, 1);
  assert.equal(filtered[0].segment, "Medical");
  const sample = [
    { priority: "HIGH_PRIORITY", bookingWindowStatus: "CONTACT_NOW" },
    { priority: "HIGH_PRIORITY", bookingWindowStatus: "QUALIFY_NOW" },
    { priority: "WATCHLIST", bookingWindowStatus: "CONTACT_NOW" },
  ];
  const actionFacet = UI.facetCountByActionStatus(sample, {
    priority: "HIGH_PRIORITY",
  });
  assert.equal(actionFacet.CONTACT_NOW, 1);
  assert.equal(actionFacet.QUALIFY_NOW, 1);
  assert.equal(actionFacet[""], 2);
  const priorityFacet = UI.facetCountByPriority(sample, {
    booking: "CONTACT_NOW",
  });
  assert.equal(priorityFacet.HIGH_PRIORITY, 1);
  assert.equal(priorityFacet.WATCHLIST, 1);
  assert.equal(priorityFacet[""], 2);
  const reconciled = UI.reconcileBrowseFilters(
    sample,
    { priority: "HIGH_PRIORITY", booking: "WATCH" },
    "priority"
  );
  assert.equal(reconciled.booking, "");
});

check("detail_intelligence_to_action_framework", () => {
  const html = UI.intelligenceDetailHtml(
    {
      summaryWhat: "See this",
      summaryWhyMatters: "Matters",
      whyNow: "Now",
      recommendedAction: "Do X",
      hotelFitScore: 80,
      evidenceConfidence: 70,
      opportunityQualificationLabel: "Strong",
      bookingWindowStatus: "CONTACT_NOW",
      sources: [],
      primaryContact: { name: "Alex Rivera", role: "Meetings Manager" },
    },
    {},
    { validationHtml: "<div>validation</div>" }
  );
  assert.match(html, /What We See/);
  assert.match(html, /Why It Matters/);
  assert.match(html, /Why Now/);
  assert.match(html, /Recommended Action/);
  assert.match(html, /Contact/);
  assert.match(html, /Evidence/);
  assert.match(html, /Hotel Validation/);
  assert.match(html, /PURSUE NOW/);
  assert.doesNotMatch(html, /Why Bethesda/);
});

check("no_bethesda_hardcode_in_shared_ui_module", () => {
  const src = read("public/js/group-demand-intelligence/dealality-gdi-ui.js");
  const r = UI.assertNoHotelHardcode(src);
  assert.equal(r.ok, true, r.hits.join(", "));
});

check("share_app_uses_shared_shell_and_no_hardcoded_territory_options", () => {
  const share = read("public/js/group-demand-intelligence/share-app.js");
  assert.match(share, /DealalityGdiUi/);
  assert.match(share, /hotelShellHtml/);
  assert.match(share, /propertyBarHtml/);
  assert.match(share, /opportunityBrowseChromeHtml/);
  assert.match(share, /opportunityCardsGridHtml/);
  assert.match(share, /buildMetricModel/);
  assert.match(share, /intelligenceDetailHtml/);
  assert.match(share, /Hotel Validation/);
  assert.doesNotMatch(share, /BETHESDA_MONTGOMERY_CORE/);
  assert.doesNotMatch(share, /Why Bethesda Marriott/);
  assert.doesNotMatch(share, /weekly-brief/);
  assert.doesNotMatch(share, /renderBrief/);
});

check("auth_app_single_browse_tab", () => {
  const app = read("public/js/group-demand-intelligence/app.js");
  assert.match(app, /GDI_MAIN_TAB/);
  assert.doesNotMatch(app, /weekly-brief/);
  assert.doesNotMatch(app, /renderBrief/);
  assert.doesNotMatch(app, /Weekly Brief/);
  assert.match(app, /gdiResetViewBtn/);
  assert.match(app, /resetBrowseView/);
  assert.match(app, /btn-clear/);
});

check("auth_app_uses_profile_driven_hotel_name", () => {
  const app = read("public/js/group-demand-intelligence/app.js");
  assert.match(app, /DealalityGdiUi/);
  assert.match(app, /hotelShellHtml/);
  assert.match(app, /propertyBarHtml/);
  assert.match(app, /opportunityBrowseChromeHtml/);
  assert.match(app, /hotelProfile/);
  assert.doesNotMatch(app, /gdi-entity__name">Bethesda Marriott/);
  assert.doesNotMatch(app, /Why Bethesda Marriott/);
  assert.doesNotMatch(app, /BETHESDA_MONTGOMERY_CORE/);
});

check("synthetic_second_hotel_portable", () => {
  const profile = JSON.parse(
    read("fixtures/group-demand-intelligence/synthetic-hotel-alpha/profile.json")
  );
  const oppsDoc = JSON.parse(
    read("fixtures/group-demand-intelligence/synthetic-hotel-alpha/opportunities.json")
  );
  const bar = UI.propertyBarHtml({
    hotel: profile.identity,
    mode: "share",
    lastResearch: "—",
    runStatus: "—",
  });
  const metrics = UI.buildMetricModel({
    summary: {
      qualifiedCount: oppsDoc.opportunities.length,
      highPriorityCount: 1,
      mediumPriorityCount: 0,
      watchlistCount: 1,
      validated: 0,
      pendingValidation: 2,
    },
    opportunities: oppsDoc.opportunities,
  });
  assert.match(bar, /Harborview Conference Hotel/);
  assert.equal(metrics.territories.length, 2);
  assert.equal(metrics.primary[0].value, 2);
  console.log("UI_PORTABLE");
});

check("css_defines_platform_tokens_and_page_header", () => {
  const css = read("public/css/group-demand-intelligence.css");
  assert.match(css, /--neutral--800/);
  assert.match(css, /--accent--primary-1/);
  assert.match(css, /--secondary--color-2/);
  assert.match(css, /aiv-dashboard/);
  assert.match(css, /gdi-results-grid--list/);
  assert.match(css, /\.gdi-results-grid--list[\s\S]*max-width:\s*100%/);
  assert.doesNotMatch(css, /gdi-results-grid--list[\s\S]*max-width:\s*920px/);
  assert.match(css, /gdi-filter-static/);
  assert.match(css, /chain-scale-legend/);
  assert.match(css, /results-toolbar/);
  assert.match(css, /results-grid/);
  assert.match(css, /brand-card/);
  assert.match(css, /brand-card__more-btn/);
  assert.match(css, /-webkit-line-clamp:\s*2/);
  assert.match(css, /Inter/);
  assert.match(css, /\.aiv-kpi/);
  assert.match(css, /\.gdi-brief-card--action/);
  assert.match(css, /\.gdi-detail-framework/);
});

if (failed) {
  console.error(`\n${failed} check(s) failed`);
  process.exit(1);
}
console.log("\nAll GDI Dealality UI unification checks passed.");
