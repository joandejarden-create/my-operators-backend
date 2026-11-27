#!/usr/bin/env node
/**
 * Customer-facing Research Hours methodology copy — ranges from live benchmarks.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildCustomerFacingResearchHoursMethodology,
  formatResearchEffortHoursRange,
  getResearchEffortBenchmark,
  CUSTOMER_METHODOLOGY_ACTIVITY_KEYS,
  WORKING_DAY_ASSUMPTION,
} from "../lib/group-demand-intelligence/executive-summary/gdi-research-effort-benchmark-contract-v1.js";
import { buildGdiResearchProductivity } from "../lib/group-demand-intelligence/executive-summary/gdi-research-productivity-v1.js";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}`);
    console.error(err);
  }
}

check("methodology_ranges_match_benchmark_config", () => {
  const expl = buildCustomerFacingResearchHoursMethodology();
  assert.equal(expl.activities.length, CUSTOMER_METHODOLOGY_ACTIVITY_KEYS.length);
  assert.equal(expl.hoursPerWorkingDay, WORKING_DAY_ASSUMPTION.hoursPerWorkingDay);
  for (const a of expl.activities) {
    const b = getResearchEffortBenchmark(a.activityKey);
    assert.ok(b, a.activityKey);
    assert.equal(a.label, b.customerFacingActivityLabel);
    assert.equal(a.rangeLabel, formatResearchEffortHoursRange(b.hours));
    assert.equal(a.hours.low, b.hours.low);
    assert.equal(a.hours.high, b.hours.high);
  }
  // Founder's proposed "2–5 minutes" must NOT appear — actual discovery is hours-scale.
  const blob = JSON.stringify(expl);
  assert.doesNotMatch(blob, /2–5 minutes/);
  assert.doesNotMatch(blob, /WHO\/HOW|residual review|provisional benchmark contract/i);
  assert.match(expl.disclaimer, /not independently validated industry standards/i);
  assert.match(expl.calculationNote, /avoiding counting the same work twice/i);
  assert.match(expl.calculationNote, /Ordinary sales pursuit/i);
  assert.match(expl.workingDayNote, new RegExp(String(WORKING_DAY_ASSUMPTION.hoursPerWorkingDay)));
});

check("productivity_payload_includes_methodology_explanation", () => {
  const store = createGdiPublicationPackageStore({});
  const ptr = store.readPointer("recnPB3Q02NO8Ma1o", "STAGED");
  const loaded = store.loadPackage("recnPB3Q02NO8Ma1o", ptr.publicationVersion);
  const before = buildGdiResearchProductivity({
    opportunities: loaded.package.opportunities,
  });
  assert.ok(before.methodologyExplanation?.activities?.length >= 5);
  assert.ok(before.customerMetrics.estimatedResearchHoursSaved.value > 0);
  const hours = before.customerMetrics.estimatedResearchHoursSaved.value;
  const days = before.customerMetrics.equivalentWorkingDaysSaved.value;
  // Re-build — KPIs unchanged by methodology copy attachment.
  const after = buildGdiResearchProductivity({
    opportunities: loaded.package.opportunities,
  });
  assert.equal(after.customerMetrics.estimatedResearchHoursSaved.value, hours);
  assert.equal(after.customerMetrics.equivalentWorkingDaysSaved.value, days);
});

check("ui_renders_methodology_list_from_explanation", () => {
  const js = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/executive-summary-tab.js"),
    "utf8",
  );
  assert.match(js, /function renderResearchHoursMethodology/);
  assert.match(js, /gdi-es-methodology__list/);
  assert.match(js, /methodologyExplanation/);
  assert.doesNotMatch(
    js,
    /Verified research activity volumes × provisional benchmarks/,
  );
  const css = fs.readFileSync(
    path.join(ROOT, "public/css/group-demand-intelligence.css"),
    "utf8",
  );
  assert.match(css, /\.gdi-es-methodology__list/);
});

check("format_range_helpers", () => {
  assert.equal(
    formatResearchEffortHoursRange({ low: 0.25, high: 0.75 }),
    "15–45 minutes",
  );
  assert.equal(
    formatResearchEffortHoursRange({ low: 1.5, high: 4.0 }),
    "1.5–4 hours",
  );
});

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log("\nOK test:gdi-es-research-hours-methodology-copy-v1");
