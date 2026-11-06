#!/usr/bin/env node
/**
 * GDI_NEED_DATE_INTELLIGENCE_UX_SIMPLIFICATION_V3 regression gate.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  NEED_DATE_CUSTOMER_DISPLAY,
  runNeedDateCustomerPresentationV3Gate,
} from "../lib/group-demand-intelligence/need-date-demand/gdi-need-date-customer-presentation-v3.js";
import { attachHotelNeedDateTilePreviewsSync } from "../lib/group-demand-intelligence/need-date-demand/opportunity-need-date-tile-preview-v1.js";
import { NEED_PERIOD_STATUS } from "../lib/group-demand-intelligence/need-date-demand/need-date-demand-contract-v1.js";
import { resolveHotelNeedDateConfiguration } from "../lib/group-demand-intelligence/need-date-demand/gdi-universal-need-date-intelligence-v1.js";
import {
  resolveApprovedPublicationOpportunities,
  SPICE_ISLAND_CERTIFIED_SEVEN_IDS,
  SPICE_ISLAND_HOTEL_ID,
} from "../lib/group-demand-intelligence/durable-publication/gdi-approved-publication-opportunity-source-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-need-date-intelligence-ux-simplification-v3",
);
fs.mkdirSync(OUT, { recursive: true });

const failures = [];
const checks = [];
function check(id, ok, detail) {
  checks.push({ id, ok: Boolean(ok), detail: detail || null });
  if (!ok) failures.push({ id, detail });
}

const gate = runNeedDateCustomerPresentationV3Gate();
check("four_state_gate", gate.pass, gate.failures);

const uiSrc = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
  "utf8",
);
check("ui_calendar_question", /function gdiSvgCalendarQuestion/.test(uiSrc));
check("ui_no_purple_customer_legend", !/gdi-need-date-icon--research/.test(
  uiSrc.match(/function needDateInformationLegendHtml[\s\S]*?^  function /m)?.[0] || "",
) || !/Timing needs research/.test(uiSrc));
check(
  "ui_legend_v4",
  /Understanding the Icons/.test(uiSrc) &&
    /Matches a Confirmed Need Date/.test(uiSrc) &&
    /Potential Match/.test(uiSrc) &&
    /Not Yet Assessed/.test(uiSrc) &&
    /No Current Match/.test(uiSrc) &&
    /Meetings & Catering/.test(uiSrc) &&
    /Green: Confirmed/.test(uiSrc),
);
check("ui_market_estimated_label", /Market Estimated/.test(uiSrc));
check(
  "ui_no_hotel_legend_section",
  !/Hotel Need Dates<\/div>/.test(uiSrc) ||
    !/gdi-need-date-legend-section__title">Hotel Need Dates/.test(uiSrc),
);

const cssSrc = fs.readFileSync(
  path.join(ROOT, "public/css/group-demand-intelligence.css"),
  "utf8",
);
check("css_matches", /gdi-need-date-icon--matches/.test(cssSrc));
check("css_potential", /gdi-need-date-icon--potential/.test(cssSrc));
check("css_not_assessed", /gdi-need-date-icon--not-assessed/.test(cssSrc));
check(
  "css_distinct_grays",
  /--gdi-need-date-not-assessed:\s*#94a3b8/i.test(cssSrc) &&
    /--gdi-need-date-no-match:\s*#64748b/i.test(cssSrc),
);

// Spice seven → Not yet assessed / calendar-question
// Use deploy-bundled approved publication source (main), not research inventory.
const spiceId = SPICE_ISLAND_HOTEL_ID;
const spiceResolved = resolveApprovedPublicationOpportunities({
  hotelId: spiceId,
  opportunityIds: [...SPICE_ISLAND_CERTIFIED_SEVEN_IDS],
  root: ROOT,
});
const spiceOpps = spiceResolved.opportunities || [];
check("spice_seven_source", spiceOpps.length === 7, {
  count: spiceOpps.length,
  missing: spiceResolved.missing || [],
});
const spiceAttached = attachHotelNeedDateTilePreviewsSync(spiceId, spiceOpps, {
  needPeriods: [],
  hotelNeedDateConfig: {
    hotelId: spiceId,
    status: "NOT_ESTABLISHED",
    confirmedPeriodCount: 0,
    estimatedPeriodCount: 0,
  },
});
const spiceOk = spiceAttached.every(
  (o) =>
    o.needDateIndicator?.state === NEED_DATE_CUSTOMER_DISPLAY.NOT_YET_ASSESSED &&
    o.needDateIndicator?.icon === "calendar-question",
);
check("spice_seven_not_yet_assessed", spiceOk, {
  states: spiceAttached.map((o) => o.needDateIndicator?.state),
  icons: spiceAttached.map((o) => o.needDateIndicator?.icon),
});

// Bethesda fixture path
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
    { id: "b3", title: "Unresolved" },
  ],
  {
    needPeriods: [
      {
        needPeriodId: "ndp_beth_2027_01",
        hotelId: "recLuxvwwxID7U2B8",
        periodLabel: "January 2027",
        startDate: "2027-01-01",
        endDate: "2027-01-31",
        status: NEED_PERIOD_STATUS.ACTIVE,
        source: "HOTEL_REPORTED",
        revenueNeed: [],
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
  "bethesda_all_have_v3",
  beth.every((o) => o.needDateIndicator?.icon && o.needDateIndicator?.state),
);
check(
  "bethesda_verified_is_matches",
  beth.find((o) => o.id === "b1")?.needDateIndicator?.state ===
    NEED_DATE_CUSTOMER_DISPLAY.MATCHES_NEED_DATE,
  beth.find((o) => o.id === "b1")?.needDateIndicator,
);
check(
  "bethesda_outside_no_current_match",
  beth.find((o) => o.id === "b2")?.needDateIndicator?.state ===
    NEED_DATE_CUSTOMER_DISPLAY.NO_CURRENT_MATCH,
);
check(
  "bethesda_unresolved_not_assessed",
  beth.find((o) => o.id === "b3")?.needDateIndicator?.state ===
    NEED_DATE_CUSTOMER_DISPLAY.NOT_YET_ASSESSED,
);

// Waterstone — estimate alone → not assessed; never green
const ws = attachHotelNeedDateTilePreviewsSync(
  "recgMYovrrZDJMqzX",
  [{ id: "w1", title: "WS", eventStartDate: "2027-08-15", eventEndDate: "2027-08-16" }],
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
  "waterstone_not_verified",
  ws[0]?.needDateIndicator?.state !== NEED_DATE_CUSTOMER_DISPLAY.MATCHES_NEED_DATE &&
    ws[0]?.needDateIndicator?.icon === "calendar-question",
  ws[0]?.needDateIndicator,
);

const hotelCfg = resolveHotelNeedDateConfiguration(spiceId);
check("spice_hotel_not_established", hotelCfg.status === "NOT_ESTABLISHED");

const report = {
  version: "GDI_NEED_DATE_INTELLIGENCE_UX_SIMPLIFICATION_V3",
  generatedAt: new Date().toISOString(),
  pass: failures.length === 0,
  failureCount: failures.length,
  failures,
  checks,
  statesCovered: gate.statesCovered,
  spiceSeven: { count: spiceAttached.length, allNotYetAssessed: spiceOk },
};

fs.writeFileSync(path.join(OUT, "gate-report.json"), JSON.stringify(report, null, 2));
console.log(JSON.stringify({ ok: report.pass, failures: failures.length, statesCovered: gate.statesCovered, spiceSeven: report.spiceSeven }, null, 2));
if (failures.length) {
  console.error(failures);
  process.exit(1);
}
