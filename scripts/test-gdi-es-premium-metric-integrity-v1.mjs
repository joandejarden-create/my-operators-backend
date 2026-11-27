#!/usr/bin/env node
/**
 * Executive Summary premium design — metric integrity + copy/non-regression gates.
 *   npm run test:gdi-es-premium-metric-integrity-v1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildGdiExecutiveSummaryV2,
  buildCommercialDemandOutlook,
  resolveDemandTimingDisplay,
} from "../lib/group-demand-intelligence/executive-summary/gdi-executive-summary-v2.js";
import { buildGdiResearchProductivity } from "../lib/group-demand-intelligence/executive-summary/gdi-research-productivity-v1.js";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const store = createGdiPublicationPackageStore({});
const ptr = store.readPointer("recnPB3Q02NO8Ma1o", "STAGED");
assert.ok(ptr?.publicationVersion);
const loaded = store.loadPackage("recnPB3Q02NO8Ma1o", ptr.publicationVersion);
const opps = loaded.package.opportunities;
assert.equal(opps.length, 26);

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

check("ongoing_display_is_not_established_event_date", () => {
  const ongoing = resolveDemandTimingDisplay({
    eventDateDisplay: "Ongoing",
    opportunityTypeFilterCategory: "SCALING_COMPANY",
  });
  assert.equal(ongoing.kind, "ONGOING_ACCOUNT");
  const dated = resolveDemandTimingDisplay({
    eventStartDate: "2027-04-02",
    eventEndDate: "2027-04-05",
    eventDateDisplay: "Apr 2–5, 2027",
  });
  assert.equal(dated.kind, "ESTABLISHED_EVENT_DATE");
});

check("outlook_buckets_reconcile_and_chart_uses_iso_only", () => {
  const outlook = buildCommercialDemandOutlook(opps);
  const sum =
    outlook.buckets.establishedEventDates +
    outlook.buckets.estimatedDemandPeriods +
    outlook.buckets.ongoingAccountRelationships +
    outlook.buckets.timingNotEstablished;
  assert.equal(sum, 26);
  assert.equal(outlook.buckets.establishedEventDates, 8);
  assert.ok(outlook.buckets.ongoingAccountRelationships >= 15);
  const chartTotal = outlook.timeline.reduce((n, r) => n + r.count, 0);
  assert.equal(chartTotal, 8);
  assert.ok(outlook.timeline.every((t) => t.monthLabel && /20\d{2}/.test(t.monthLabel)));
});

check("kpi_ribbon_competitive_actionable_need_periods", () => {
  const payload = buildGdiExecutiveSummaryV2({
    hotelId: "recnPB3Q02NO8Ma1o",
    hotelName: "Wyndham San Diego Bayside",
    opportunities: opps,
    founderPreview: true,
    publicationMeta: { publicationVersion: ptr.publicationVersion },
  });
  const ribbon = payload.sections.executiveOverview.commercialKpiRibbon;
  const byKey = Object.fromEntries(ribbon.map((k) => [k.key, k]));
  assert.equal(byKey.qualified.value, 26);
  assert.equal(byKey.competitive.value, 3);
  assert.equal(byKey.actionable.value, 17); // Build Account 15 + Pursue 2
  assert.equal(byKey.upcoming.value, 8);
  assert.equal(byKey.needPeriods.value, null);
  assert.equal(byKey.needPeriods.availability, "HOTEL_INPUT_REQUIRED");
  assert.equal(byKey.new.value, null);
});

check("assessment_has_three_primary_cards", () => {
  const payload = buildGdiExecutiveSummaryV2({
    hotelId: "recnPB3Q02NO8Ma1o",
    opportunities: opps,
    founderPreview: true,
    publicationMeta: { publicationVersion: ptr.publicationVersion },
  });
  const a = payload.sections.executiveOverview.executiveAssessment;
  assert.equal(a.primaryCards.length, 3);
  assert.ok(a.optionalCards.some((c) => c.key === "competitiveCapture"));
  assert.equal(
    a.optionalCards.some((c) => /not available|technical/i.test(c.body || "")),
    false,
  );
});

check("productivity_overlap_prevention_and_provisional", () => {
  const prod = buildGdiResearchProductivity({ opportunities: opps });
  assert.equal(prod.certificationStatus, "PROVISIONAL_NOT_CERTIFIED");
  assert.equal(prod.estimateClass, "PROVISIONAL_ESTIMATE");
  const discovery = prod.activityLines.find(
    (l) => l.activityKey === "demand_discovery_and_screening",
  );
  const deep = prod.activityLines.find(
    (l) => l.activityKey === "deep_opportunity_research",
  );
  const pursuit = prod.activityLines.find(
    (l) => l.activityKey === "sales_pursuit_strategy_preparation",
  );
  assert.equal(deep.volume, 26);
  assert.equal(discovery.volume, 0); // all deep → discovery not double-counted
  assert.equal(pursuit.volume, null);
  const hours = prod.customerMetrics.estimatedResearchHoursSaved.value;
  assert.ok(hours != null && hours < 119, `expected <119 after overlap fix, got ${hours}`);
  assert.ok(hours > 40, `expected meaningful savings, got ${hours}`);
});

check("customer_copy_audit_on_shell_assets", () => {
  const js = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/executive-summary-tab.js"),
    "utf8",
  );
  assert.doesNotMatch(js, /not a technical validation page/i);
  assert.doesNotMatch(js, /canonical demand segment/i);
  assert.doesNotMatch(js, /Same opportunity tiles as/i);
  assert.match(js, /Dealality Research Receipt/);
  assert.match(js, /Your [Tt]ime\. Better [Ii]nvested/);
  assert.match(js, /opportunityCardsGridHtml/);
  assert.match(js, /Research Funnel/);
  assert.match(js, /gdi-es-kpi__unit/);
  const html = fs.readFileSync(
    path.join(ROOT, "public/group-demand-intelligence.html"),
    "utf8",
  );
  assert.match(html, /executive-summary-tab\.js\?v=gdi-es-receipt-funnel-v1/);
});

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log("\nOK test:gdi-es-premium-metric-integrity-v1");
