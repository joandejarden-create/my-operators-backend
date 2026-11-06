#!/usr/bin/env node
/**
 * Permanent regression gate — GDI universal Need-Date tile icons V1.
 * Zero-paid. No provider calls. No Airtable writes. No inventory mutations.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_NEED_DATE_TILE_PREVIEW_V1,
  NEED_DATE_TILE_INDICATOR,
  attachHotelNeedDateTilePreviewsSync,
  filterCustomerFacingNeedPeriodsForTileIcons,
  isPositiveNeedDateIconSuppressed,
  runNeedDateTilePreviewGate,
} from "../lib/group-demand-intelligence/need-date-demand/opportunity-need-date-tile-preview-v1.js";
import { NEED_PERIOD_STATUS } from "../lib/group-demand-intelligence/need-date-demand/need-date-demand-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-universal-need-date-tile-icons-v1");
fs.mkdirSync(OUT, { recursive: true });

const failures = [];
const checks = [];

function check(id, ok, detail) {
  checks.push({ id, ok: Boolean(ok), detail: detail || null });
  if (!ok) failures.push({ id, detail });
}

const gate = runNeedDateTilePreviewGate();
check("core_gate", gate.pass, gate.failures);

// UI / CSS source contracts
const uiPath = path.join(
  ROOT,
  "public/js/group-demand-intelligence/dealality-gdi-ui.js",
);
const uiSrc = fs.readFileSync(uiPath, "utf8");
check("ui_calendar_check_svg", /function gdiSvgCalendarCheck/.test(uiSrc));
check("ui_calendar_clock_svg", /function gdiSvgCalendarClock/.test(uiSrc));
check("ui_calendar_search_svg", /function gdiSvgCalendarSearch/.test(uiSrc));
check(
  "ui_need_date_icon_html",
  /function needDateIntelligenceIconHtml/.test(uiSrc),
);
check(
  "ui_tile_coexists_with_revenue",
  /revenueStreamIconsHtml\(it, linked\)\s*\+\s*\n?\s*needDateIntelligenceIconHtml\(it, linked\)/.test(
    uiSrc,
  ) ||
    (uiSrc.includes("revenueStreamIconsHtml(it, linked)") &&
      uiSrc.includes("needDateIntelligenceIconHtml(it, linked)")),
);
check(
  "ui_drawer_section",
  /function needDateMatchDrawerSectionHtml/.test(uiSrc) &&
    /needDateMatchDrawerSectionHtml\(o\)/.test(uiSrc),
);
check("ui_not_clickable_v1", /Informational only — not clickable/.test(uiSrc));

const cssPath = path.join(ROOT, "public/css/group-demand-intelligence.css");
const cssSrc = fs.readFileSync(cssPath, "utf8");
check("css_verified_green", /gdi-need-date-icon--verified/.test(cssSrc));
check("css_flexible_gold", /gdi-need-date-icon--flexible/.test(cssSrc));
check("css_research_purple", /gdi-need-date-icon--research/.test(cssSrc));
check(
  "css_uses_platform_tokens",
  cssSrc.includes("--system--green-300") &&
    cssSrc.includes("--secondary--color-5") &&
    cssSrc.includes("--accent--primary-1"),
);

// List DTO + API wiring
const dtoSrc = fs.readFileSync(
  path.join(ROOT, "lib/group-demand-intelligence/opportunity-list-dto.js"),
  "utf8",
);
check("dto_tile_preview_field", /tileNeedDatePreview:/.test(dtoSrc));

const apiSrc = fs.readFileSync(
  path.join(ROOT, "api/group-demand-intelligence.js"),
  "utf8",
);
check(
  "api_list_attach",
  /attachHotelNeedDateTilePreviews/.test(apiSrc) &&
    /getGdiOpportunities/.test(apiSrc),
);
check("api_detail_attach", apiSrc.includes("need-date tile preview detail"));

// TEST_SCENARIO exclusion (Waterstone historical)
const filtered = filterCustomerFacingNeedPeriodsForTileIcons([
  {
    needPeriodId: "ndp_ws_test",
    status: NEED_PERIOD_STATUS.TEST_SCENARIO,
    isTestScenario: true,
    source: "FOUNDER_TEST",
    periodLabel: "August 2027",
    startDate: "2027-08-01",
    endDate: "2027-08-31",
  },
  {
    needPeriodId: "ndp_beth_jan",
    status: NEED_PERIOD_STATUS.ACTIVE,
    source: "HOTEL_REPORTED",
    periodLabel: "January 2027",
    startDate: "2027-01-01",
    endDate: "2027-01-31",
    preferenceOverrides: { scenarioClassification: "ACTUAL_HOTEL_INPUT" },
  },
]);
check(
  "test_scenario_filtered_out",
  filtered.length === 1 && filtered[0].needPeriodId === "ndp_beth_jan",
);

// Competitor suppress helper
check(
  "competitor_suppress",
  isPositiveNeedDateIconSuppressed({
    id: "gdi_opp_asa_advance_2027_disqualified",
    commercialAccessibilityClass: "COMPETITOR_COMMITTED",
    title: "already contracted at Gaylord National",
  }) === true,
);

// Revenue icons must remain independent — attaching need-date preview must not
// strip revenueStreamIcons.
const withBoth = attachHotelNeedDateTilePreviewsSync(
  "recLuxvwwxID7U2B8",
  [
    {
      id: "combo",
      hotelId: "recLuxvwwxID7U2B8",
      title: "January Combined Programme",
      eventStartDate: "2027-01-12",
      eventEndDate: "2027-01-14",
      revenueStreamIcons: { guestrooms: "active", eventFnb: "active" },
      revenueStreamClass: "COMBINED_GUESTROOMS_EVENT_FNB",
    },
  ],
  {
    needPeriods: [
      {
        needPeriodId: "ndp_bethesda_marriott_2027_01",
        hotelId: "recLuxvwwxID7U2B8",
        periodLabel: "January 2027",
        startDate: "2027-01-01",
        endDate: "2027-01-31",
        status: NEED_PERIOD_STATUS.ACTIVE,
        source: "HOTEL_REPORTED",
        revenueNeed: [],
      },
    ],
  },
);
check(
  "revenue_icons_preserved",
  withBoth[0]?.revenueStreamIcons?.guestrooms === "active" &&
    withBoth[0]?.tileNeedDatePreview?.indicatorState === "MATCHES_NEED_DATE" &&
    withBoth[0]?.needDateIndicator?.underlyingState ===
      NEED_DATE_TILE_INDICATOR.VERIFIED_DATE_MATCH,
);

// Tooltip + a11y on verified (V3 customer copy; months preserved)
const tip = withBoth[0]?.tileNeedDatePreview;
check(
  "tooltip_verified",
  tip &&
    (/Matches a Confirmed Need Date/i.test(tip.tooltipText || "") ||
      /Matches a need date/i.test(tip.tooltipText || "") ||
      /Date match:/i.test(tip.tooltipText || "")) &&
    /January 2027/.test(tip.tooltipText || ""),
);
check(
  "a11y_label_verified",
  tip &&
    (/Matches a Confirmed Need Date/i.test(tip.accessibleLabel || "") ||
      /Matches a need date/i.test(tip.accessibleLabel || "") ||
      /Need-date verified match/i.test(tip.accessibleLabel || "")),
);

// Contract version stamp
check(
  "contract_version",
  tip?.version === GDI_NEED_DATE_TILE_PREVIEW_V1,
);

const report = {
  schemaVersion: "gdi-universal-need-date-tile-icons-gate-v1",
  generatedAt: new Date().toISOString(),
  pass: failures.length === 0,
  failureCount: failures.length,
  checkCount: checks.length,
  failures,
  checks,
  coreGate: gate,
  paidResearchSpend: 0,
  airtableProductionWrites: 0,
  inventoryMutations: 0,
};

fs.writeFileSync(
  path.join(OUT, "01-permanent-regression-gate.json"),
  JSON.stringify(report, null, 2),
);

console.log(
  JSON.stringify(
    {
      ok: report.pass,
      failureCount: failures.length,
      checkCount: checks.length,
      out: path.relative(ROOT, path.join(OUT, "01-permanent-regression-gate.json")),
    },
    null,
    2,
  ),
);

if (!report.pass) {
  console.error(failures);
  process.exit(1);
}
