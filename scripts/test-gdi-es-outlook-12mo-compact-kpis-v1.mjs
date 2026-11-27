#!/usr/bin/env node
/**
 * Commercial Demand Outlook: full calendar year + compact KPI layout.
 *   npm run test:gdi-es-outlook-12mo-compact-kpis-v1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildCommercialDemandOutlook,
  buildFullYearOutlookTimeline,
  resolveOutlookApplicableYear,
} from "../lib/group-demand-intelligence/executive-summary/gdi-executive-summary-v2.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
let failed = 0;

function check(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}: ${err && err.message ? err.message : err}`);
  }
}

check("applicable_year_is_modal_not_hardcoded", () => {
  assert.equal(resolveOutlookApplicableYear(["2027-01", "2027-04", "2026-11"]), 2027);
  assert.equal(resolveOutlookApplicableYear(["2026-01", "2027-01", "2027-02"]), 2027);
  assert.equal(resolveOutlookApplicableYear([]), new Date().getUTCFullYear());
});

check("full_year_timeline_zero_fills_jan_dec_and_keeps_extras", () => {
  const byMonth = new Map([
    [
      "2027-04",
      {
        month: "2027-04",
        monthLabel: "Apr 2027",
        count: 2,
        opportunityIds: ["a", "b"],
      },
    ],
    [
      "2028-02",
      {
        month: "2028-02",
        monthLabel: "Feb 2028",
        count: 1,
        opportunityIds: ["c"],
      },
    ],
  ]);
  const { timeline, applicableYear } = buildFullYearOutlookTimeline(byMonth, 2027);
  assert.equal(applicableYear, 2027);
  assert.equal(timeline.length, 13); // 12 months 2027 + Feb 2028
  const jan = timeline.find((t) => t.month === "2027-01");
  const apr = timeline.find((t) => t.month === "2027-04");
  const feb28 = timeline.find((t) => t.month === "2028-02");
  assert.equal(jan.count, 0);
  assert.equal(jan.zeroFilled, true);
  assert.equal(apr.count, 2);
  assert.equal(feb28.count, 1);
  for (let i = 1; i < timeline.length; i += 1) {
    assert.ok(timeline[i - 1].month < timeline[i].month);
  }
});

check("buildCommercialDemandOutlook_fills_twelve_months", () => {
  const outlook = buildCommercialDemandOutlook([
    {
      opportunityId: "1",
      eventStartDate: "2027-04-02",
      datePillDisplayLabel: "Apr 2–5, 2027",
    },
    {
      opportunityId: "2",
      eventStartDate: "2027-07-12",
      datePillDisplayLabel: "Jul 12–16, 2027",
    },
    {
      opportunityId: "3",
      datePillDisplayLabel: "Ongoing",
    },
  ]);
  assert.equal(outlook.applicableYear, 2027);
  assert.ok(outlook.timeline.length >= 12);
  const yearMonths = outlook.timeline.filter((t) =>
    String(t.month).startsWith("2027-"),
  );
  assert.equal(yearMonths.length, 12);
  assert.equal(
    outlook.timeline.reduce((n, t) => n + t.count, 0),
    2,
  );
  assert.equal(outlook.buckets.establishedEventDates, 2);
});

check("ui_markup_and_css_compact_outlook_only", () => {
  const js = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/executive-summary-tab.js"),
    "utf8",
  );
  const css = fs.readFileSync(
    path.join(ROOT, "public/css/group-demand-intelligence.css"),
    "utf8",
  );
  const html = fs.readFileSync(
    path.join(ROOT, "public/group-demand-intelligence.html"),
    "utf8",
  );
  assert.match(js, /gdi-es-outlook__top/);
  assert.match(js, /gdi-es-outlook__buckets--compact/);
  assert.match(js, /grid-template-columns:repeat\(/);
  assert.match(css, /\.gdi-es-outlook__top/);
  assert.match(css, /\.gdi-es-outlook__buckets--compact/);
  assert.match(css, /grid-template-columns:\s*repeat\(4,/);
  assert.match(html, /gdi-es-outlook-12mo-kpis-20261127/);
  // Do not regress unrelated ES sections in this scoped change.
  assert.match(js, /Demand Composition/);
  assert.match(js, /Dealality Research Receipt/);
});

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log("\nOK test:gdi-es-outlook-12mo-compact-kpis-v1");
