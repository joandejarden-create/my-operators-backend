#!/usr/bin/env node
/**
 * GDI_UNIVERSAL_OPPORTUNITY_NEED_DATE_SVG_V2 regression gate.
 * Asserts every opportunity has needDateIndicator.{state,icon}; covers all six states.
 * Zero paid research. No Airtable writes. No publication.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  OPPORTUNITY_NEED_DATE_INDICATOR_STATE,
  runUniversalOpportunityNeedDateIndicatorGate,
  resolveUniversalOpportunityNeedDateIndicatorV2,
  attachUniversalOpportunityNeedDateIndicator,
} from "../lib/group-demand-intelligence/need-date-demand/gdi-universal-opportunity-need-date-indicator-v2.js";
import {
  attachHotelNeedDateTilePreviewsSync,
  runNeedDateTilePreviewGate,
} from "../lib/group-demand-intelligence/need-date-demand/opportunity-need-date-tile-preview-v1.js";
import { resolveHotelNeedDateConfiguration } from "../lib/group-demand-intelligence/need-date-demand/gdi-universal-need-date-intelligence-v1.js";
import { listCanonicalGdiEligibleHotels } from "../lib/group-demand-intelligence/need-date-demand/gdi-hotel-layer-model-v2.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-universal-opportunity-need-date-svg-v2",
);
fs.mkdirSync(OUT, { recursive: true });

const failures = [];
const checks = [];

function check(id, ok, detail) {
  checks.push({ id, ok: Boolean(ok), detail: detail || null });
  if (!ok) failures.push({ id, detail });
}

const core = runUniversalOpportunityNeedDateIndicatorGate();
check("six_state_gate", core.pass, core.failures);

const legacy = runNeedDateTilePreviewGate();
check("legacy_tile_preview_gate_updated", legacy.pass, legacy.failures);

const uiSrc = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
  "utf8",
);
check("ui_calendar_x", /function gdiSvgCalendarX/.test(uiSrc));
check("ui_never_empty_return", !/if \(state === "NO_MEANINGFUL_MATCH"\) return ""/.test(uiSrc));
check(
  "ui_resolver_present",
  /function resolveOpportunityNeedDateIndicatorForUi/.test(uiSrc) &&
    /function needDateIntelligenceIconHtml/.test(uiSrc),
);
check(
  "ui_legend_four_customer_states_v4",
  /Understanding the Icons/.test(uiSrc) &&
    /Matches a Confirmed Need Date/.test(uiSrc) &&
    /Potential Match/.test(uiSrc) &&
    /Not Yet Assessed/.test(uiSrc) &&
    /No Current Match/.test(uiSrc) &&
    /function gdiSvgCalendarQuestion/.test(uiSrc) &&
    /needDateLegendRowHtml/.test(uiSrc) &&
    /Green: Confirmed/.test(uiSrc),
);
check(
  "ui_single_metadata_row",
  /gdi-property-bar__history-item--need-dates/.test(uiSrc) &&
    !/gdi-property-bar__need-dates-label/.test(uiSrc),
);

const cssSrc = fs.readFileSync(
  path.join(ROOT, "public/css/group-demand-intelligence.css"),
  "utf8",
);
check("css_no_match", /gdi-need-date-icon--no-match/.test(cssSrc));
check("css_not_established", /gdi-need-date-icon--not-established/.test(cssSrc));
check("css_estimated", /gdi-need-date-icon--estimated/.test(cssSrc));

const dtoSrc = fs.readFileSync(
  path.join(ROOT, "lib/group-demand-intelligence/opportunity-list-dto.js"),
  "utf8",
);
check("dto_needDateIndicator", /needDateIndicator:/.test(dtoSrc));

// Spice — NOT_ESTABLISHED → all seven customer calendar-question (V3 Not yet assessed)
const spiceId = "recKRJjcPnb4tVDDS";
const spiceCfg = resolveHotelNeedDateConfiguration(spiceId);
check(
  "spice_hotel_not_established",
  spiceCfg.status === "NOT_ESTABLISHED",
  spiceCfg,
);

const {
  resolveApprovedPublicationOpportunities,
  SPICE_ISLAND_CERTIFIED_SEVEN_IDS,
} = await import(
  "../lib/group-demand-intelligence/durable-publication/gdi-approved-publication-opportunity-source-v1.js"
);
const spiceResolved = resolveApprovedPublicationOpportunities({
  hotelId: spiceId,
  opportunityIds: [...SPICE_ISLAND_CERTIFIED_SEVEN_IDS],
  root: ROOT,
});
let spiceOpps = spiceResolved.opportunities || [];
check("spice_approved_seven_source", spiceOpps.length >= 7, {
  count: spiceOpps.length,
  missing: spiceResolved.missing || [],
});
if (!spiceOpps.length) {
  spiceOpps = Array.from({ length: 7 }, (_, i) => ({
    id: `spice_placeholder_${i}`,
    title: `Spice placeholder ${i}`,
  }));
}

const spiceAttached = attachHotelNeedDateTilePreviewsSync(
  spiceId,
  spiceOpps.slice(0, 7),
  {
    needPeriods: [],
    hotelNeedDateConfig: {
      hotelId: spiceId,
      status: "NOT_ESTABLISHED",
      confirmedPeriodCount: 0,
      estimatedPeriodCount: 0,
    },
  },
);
const spiceOk = spiceAttached.every(
  (o) =>
    o.needDateIndicator &&
    o.needDateIndicator.state === "NOT_YET_ASSESSED" &&
    o.needDateIndicator.icon === "calendar-question" &&
    o.needDateIndicator.underlyingState ===
      OPPORTUNITY_NEED_DATE_INDICATOR_STATE.HOTEL_NEED_DATES_NOT_ESTABLISHED &&
    o.tileNeedDatePreview?.visible === true,
);
check("spice_seven_calendar_question_v3", spiceOk, {
  count: spiceAttached.length,
  states: spiceAttached.map((o) => o.needDateIndicator?.state),
  underlying: spiceAttached.map((o) => o.needDateIndicator?.underlyingState),
});

// Bethesda — HOTEL_CONFIRMED fixture path
const beth = attachHotelNeedDateTilePreviewsSync(
  "recLuxvwwxID7U2B8",
  [
    {
      id: "b1",
      eventStartDate: "2027-01-14",
      eventEndDate: "2027-01-16",
      title: "Jan overlap",
    },
    {
      id: "b2",
      eventStartDate: "2028-06-01",
      eventEndDate: "2028-06-03",
      title: "Outside",
    },
    { id: "b3", title: "Unresolved timing" },
  ],
  {
    needPeriods: [
      {
        needPeriodId: "ndp_beth_2027_01",
        hotelId: "recLuxvwwxID7U2B8",
        periodLabel: "January 2027",
        startDate: "2027-01-01",
        endDate: "2027-01-31",
        status: "ACTIVE",
        source: "HOTEL_REPORTED",
        preferenceOverrides: { scenarioClassification: "ACTUAL_HOTEL_INPUT" },
      },
    ],
    hotelNeedDateConfig: {
      hotelId: "recLuxvwwxID7U2B8",
      status: "HOTEL_CONFIRMED",
      confirmedPeriodCount: 8,
      estimatedPeriodCount: 0,
    },
  },
);
check(
  "bethesda_all_have_indicator",
  beth.every((o) => o.needDateIndicator?.icon && o.needDateIndicator?.state),
);
check(
  "bethesda_outside_is_no_current_match_v3",
  beth.find((o) => o.id === "b2")?.needDateIndicator?.state === "NO_CURRENT_MATCH" &&
    beth.find((o) => o.id === "b2")?.needDateIndicator?.underlyingState ===
      "NO_DATE_MATCH",
  beth.find((o) => o.id === "b2")?.needDateIndicator,
);
check(
  "bethesda_unresolved_not_yet_assessed_v3",
  beth.find((o) => o.id === "b3")?.needDateIndicator?.state === "NOT_YET_ASSESSED" &&
    beth.find((o) => o.id === "b3")?.needDateIndicator?.underlyingState ===
      "TIMING_RESEARCH_REQUIRED",
);

// Waterstone — MARKET_ESTIMATED + test scenarios excluded → V3 Not yet assessed
// (estimate alone must not auto-promote to Potential Match without timing thesis)
const ws = attachHotelNeedDateTilePreviewsSync(
  "recgMYovrrZDJMqzX",
  [{ id: "w1", title: "WS opp", eventStartDate: "2027-08-15", eventEndDate: "2027-08-16" }],
  {
    needPeriods: [
      {
        needPeriodId: "ndp_ws_test",
        status: "TEST_SCENARIO",
        isTestScenario: true,
        source: "FOUNDER_TEST",
        startDate: "2027-08-01",
        endDate: "2027-08-31",
      },
    ],
    hotelNeedDateConfig: {
      hotelId: "recgMYovrrZDJMqzX",
      status: "MARKET_ESTIMATED",
      confirmedPeriodCount: 0,
      estimatedPeriodCount: 4,
    },
  },
);
check(
  "waterstone_estimated_not_verified_v3",
  ws[0]?.needDateIndicator?.state === "NOT_YET_ASSESSED" &&
    ws[0]?.needDateIndicator?.icon === "calendar-question" &&
    ws[0]?.needDateIndicator?.underlyingState === "MARKET_NEED_DATES_ESTIMATED" &&
    ws[0]?.needDateIndicator?.state !== "MATCHES_NEED_DATE",
  ws[0]?.needDateIndicator,
);

// Hotel with no research → V3 Not yet assessed
const fresh = attachUniversalOpportunityNeedDateIndicator(
  { id: "fresh", title: "New" },
  {
    hotelNeedDateConfig: {
      status: "NOT_ESTABLISHED",
      confirmedPeriodCount: 0,
      estimatedPeriodCount: 0,
    },
  },
);
check(
  "no_research_hotel_indicator_v3",
  fresh.needDateIndicator?.state === "NOT_YET_ASSESSED" &&
    fresh.needDateIndicator?.icon === "calendar-question" &&
    fresh.needDateIndicator?.underlyingState === "HOTEL_NEED_DATES_NOT_ESTABLISHED",
);

// Never invent verified on estimated
const noFake = resolveUniversalOpportunityNeedDateIndicatorV2({
  opportunity: { eventStartDate: "2027-08-15", eventEndDate: "2027-08-16" },
  hotelNeedDateConfig: {
    status: "MARKET_ESTIMATED",
    confirmedPeriodCount: 0,
    estimatedPeriodCount: 4,
  },
  timingPreview: {
    indicatorState: "VERIFIED_DATE_MATCH",
    visible: true,
    demotedFromVerifiedBecause: "MARKET_ESTIMATED_PERIOD",
  },
});
check(
  "never_verified_on_estimated",
  noFake.state !== "VERIFIED_DATE_MATCH" && noFake.icon != null,
  noFake,
);

let eligibleHotels = [];
try {
  eligibleHotels = listCanonicalGdiEligibleHotels?.() || [];
} catch {
  try {
    const mod = await import(
      "../lib/group-demand-intelligence/need-date-demand/gdi-hotel-layer-model-v2.js"
    );
    eligibleHotels =
      mod.listCanonicalGdiEligibleHotels?.() ||
      mod.auditHotelLayerModelV2?.()?.hotels ||
      [];
  } catch {
    eligibleHotels = [];
  }
}

const report = {
  version: "GDI_UNIVERSAL_OPPORTUNITY_NEED_DATE_SVG_V2",
  generatedAt: new Date().toISOString(),
  pass: failures.length === 0,
  failureCount: failures.length,
  failures,
  checks,
  statesCovered: core.statesCovered,
  spiceSeven: {
    count: spiceAttached.length,
    allCalendarMinus: spiceOk,
  },
  eligibleHotelCountHint: Array.isArray(eligibleHotels)
    ? eligibleHotels.length
    : null,
};

fs.writeFileSync(
  path.join(OUT, "gate-report.json"),
  JSON.stringify(report, null, 2),
);

console.log(
  JSON.stringify(
    {
      ok: report.pass,
      failures: failures.length,
      statesCovered: core.statesCovered,
      spiceSeven: report.spiceSeven,
    },
    null,
    2,
  ),
);

if (failures.length) {
  console.error(failures);
  process.exit(1);
}
