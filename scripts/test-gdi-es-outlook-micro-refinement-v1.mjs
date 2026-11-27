#!/usr/bin/env node
/**
 * Commercial Demand Outlook micro-refinement:
 * KPI number baseline, dim zero months, two-line month labels.
 *   npm run test:gdi-es-outlook-micro-refinement-v1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { buildCommercialDemandOutlook } from "../lib/group-demand-intelligence/executive-summary/gdi-executive-summary-v2.js";

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

check("metric_logic_unchanged_buckets_and_zero_fill", () => {
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
    { opportunityId: "3", datePillDisplayLabel: "Ongoing" },
  ]);
  assert.equal(outlook.applicableYear, 2027);
  assert.equal(outlook.buckets.establishedEventDates, 2);
  assert.equal(outlook.buckets.ongoingAccountRelationships, 1);
  const jan = outlook.timeline.find((t) => t.month === "2027-01");
  const apr = outlook.timeline.find((t) => t.month === "2027-04");
  assert.equal(jan.count, 0);
  assert.equal(apr.count, 1);
  assert.equal(jan.monthLabel, "Jan 2027");
});

check("ui_only_scoped_outlook_micro_changes", () => {
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
  assert.match(js, /outlookMonthParts/);
  assert.match(js, /is-zero/);
  assert.match(js, /gdi-es-columns__month/);
  assert.match(js, /gdi-es-columns__year/);
  assert.match(css, /margin-top:\s*auto/);
  assert.match(css, /\.gdi-es-columns--outlook \.gdi-es-columns__item\.is-zero/);
  assert.match(html, /gdi-es-outlook-micro-20261127/);
  // Unrelated ES sections untouched by this change.
  assert.match(js, /Demand Composition/);
  assert.match(js, /Dealality Research Receipt/);
  assert.match(js, /Priority Commercial Opportunities/);
});

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log("\nOK test:gdi-es-outlook-micro-refinement-v1");
