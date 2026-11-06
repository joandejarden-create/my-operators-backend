#!/usr/bin/env node
/**
 * Permanent regression — universal Need-Date icons across hotels.
 * Zero-paid. No Airtable writes. No inventory mutations.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  NEED_DATE_TILE_INDICATOR,
  attachHotelNeedDateTilePreviewsSync,
  filterCustomerFacingNeedPeriodsForTileIcons,
  runNeedDateTilePreviewGate,
} from "../lib/group-demand-intelligence/need-date-demand/opportunity-need-date-tile-preview-v1.js";
import { NEED_PERIOD_STATUS } from "../lib/group-demand-intelligence/need-date-demand/need-date-demand-contract-v1.js";
import { classifyHotelNeedDateIcons } from "../lib/group-demand-intelligence/need-date-demand/need-date-tile-icon-eligibility-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

const gate = runNeedDateTilePreviewGate();
assert.equal(gate.pass, true, JSON.stringify(gate.failures));

const BETHESDA = "recLuxvwwxID7U2B8";
const WATERSTONE = "recgMYovrrZDJMqzX";
const OTHER = "recterKmNPdntCm0v";

const bethesdaPeriods = [
  {
    needPeriodId: "ndp_bethesda_marriott_2027_01",
    hotelId: BETHESDA,
    periodLabel: "January 2027",
    startDate: "2027-01-01",
    endDate: "2027-01-31",
    status: NEED_PERIOD_STATUS.ACTIVE,
    source: "HOTEL_REPORTED",
  },
];

const waterstoneTestPeriods = [
  {
    needPeriodId: "ndp_airtable_waterstone_aug_2027",
    hotelId: WATERSTONE,
    periodLabel: "August 2027",
    startDate: "2027-08-01",
    endDate: "2027-08-31",
    status: NEED_PERIOD_STATUS.TEST_SCENARIO,
    source: "founder_test_scenario_v3",
    isTestScenario: true,
  },
];

const futureHotelPeriods = [
  {
    needPeriodId: "ndp_future_hotel_2027_03",
    hotelId: OTHER,
    periodLabel: "March 2027",
    startDate: "2027-03-01",
    endDate: "2027-03-31",
    status: NEED_PERIOD_STATUS.ACTIVE,
    source: "AIRTABLE",
  },
];

// Multi-hotel distinct periods
const bOps = [
  {
    id: "opp_b_jan",
    hotelId: BETHESDA,
    title: "Bethesda January Conference",
    eventStartDate: "2027-01-10",
    eventEndDate: "2027-01-12",
  },
];
const wOps = [
  {
    id: "opp_w_aug",
    hotelId: WATERSTONE,
    title: "Waterstone August Regatta",
    eventStartDate: "2027-08-10",
    eventEndDate: "2027-08-12",
  },
];
const oOps = [
  {
    id: "opp_o_mar",
    hotelId: OTHER,
    title: "Chapultepec March Catering Day",
    eventStartDate: "2027-03-15",
    eventEndDate: "2027-03-15",
  },
];

const b = classifyHotelNeedDateIcons(BETHESDA, bOps, bethesdaPeriods, {
  hotelName: "Bethesda Marriott",
});
assert.equal(b.counts.verified, 1);

const wFiltered = filterCustomerFacingNeedPeriodsForTileIcons(waterstoneTestPeriods);
assert.equal(wFiltered.length, 0, "Waterstone TEST_SCENARIO must not be operational");
const w = classifyHotelNeedDateIcons(WATERSTONE, wOps, waterstoneTestPeriods);
assert.equal(w.counts.verified, 0);
assert.equal(w.counts.flexible, 0);
assert.equal(w.counts.timingResearch, 0);
assert.equal(w.stamped[0]?.tileNeedDatePreview?.visible, false);

// Future hotel auto-eligibility (Active AIRTABLE source — no hotel-specific code)
const o = classifyHotelNeedDateIcons(OTHER, oOps, futureHotelPeriods, {
  hotelName: "Grand Fiesta Americana Chapultepec",
});
assert.equal(o.operationalPeriodCount, 1);
assert.equal(o.counts.verified, 1);

// Hotel switch isolation: Waterstone classification must not leak into Bethesda IDs
const mixed = attachHotelNeedDateTilePreviewsSync(BETHESDA, bOps, {
  needPeriods: bethesdaPeriods,
});
assert.equal(
  mixed[0].tileNeedDatePreview.indicatorState,
  NEED_DATE_TILE_INDICATOR.VERIFIED_DATE_MATCH,
);
const wrongHotel = attachHotelNeedDateTilePreviewsSync(WATERSTONE, bOps, {
  needPeriods: [], // Waterstone operational empty
});
assert.equal(wrongHotel[0].tileNeedDatePreview?.visible, false);

// Competitor commit suppress
const suppressed = attachHotelNeedDateTilePreviewsSync(
  BETHESDA,
  [
    {
      id: "opp_competitor",
      hotelId: BETHESDA,
      title: "Already contracted at Rival Hotel",
      eventStartDate: "2027-01-20",
      eventEndDate: "2027-01-22",
      commercialAccessibilityClass: "COMPETITOR_COMMITTED",
    },
  ],
  { needPeriods: bethesdaPeriods },
);
assert.equal(suppressed[0].tileNeedDatePreview?.visible, false);

// API has no Bethesda-only attach gate
const apiSrc = fs.readFileSync(
  path.join(ROOT, "api/group-demand-intelligence.js"),
  "utf8",
);
assert.match(apiSrc, /attachHotelNeedDateTilePreviews/);
assert.equal(
  /if\s*\([^\n]*recLuxvwwxID7U2B8[^\n]*\)[^\n]*attachHotelNeedDateTilePreviews/.test(
    apiSrc,
  ),
  false,
);

// UI drawer contract language
const uiSrc = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
  "utf8",
);
assert.match(
  uiSrc,
  /Potential timing flexibility identified\. Confirm event or travel dates with the organizer/,
);
assert.match(
  uiSrc,
  /Opportunity timing has not been confirmed\. Further qualification is required before treating this as a Need-Date match/,
);
assert.match(uiSrc, /Opportunity timing:/);

console.log(
  JSON.stringify(
    {
      ok: true,
      bethesdaVerified: b.counts.verified,
      waterstoneIconsSuppressed: true,
      futureHotelAutoEligible: o.counts.verified === 1,
      competitorSuppressed: true,
      hotelSwitchIsolation: true,
    },
    null,
    2,
  ),
);
