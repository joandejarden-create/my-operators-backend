#!/usr/bin/env node
/**
 * GDI customer CSV export — sales-facing Excel file (not JSON DTO).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_CUSTOMER_CSV_HEADERS,
  GDI_CUSTOMER_CSV_CORE_HEADERS,
  GDI_CUSTOMER_CSV_DETAIL_HEADERS,
  buildCustomerExportCsv,
  toCustomerExportRow,
  escapeCsvCell,
  buildCustomerCsvFilename,
  customerCsvContentDisposition,
  assertCustomerCsvClean,
  toCsvIsoDate,
  compareDetailToExportRow,
  enrichOpportunityForCustomerExport,
} from "../lib/group-demand-intelligence/customer-csv-export.js";
import { buildExportCsv } from "../lib/group-demand-intelligence/live-commercial-quality-v1.js";
import { filterSalespersonView } from "../lib/group-demand-intelligence/opportunity-factory.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

let failed = 0;
let passed = 0;
function check(name, fn) {
  try {
    fn();
    passed += 1;
    console.log("PASS", name);
  } catch (err) {
    failed += 1;
    console.error("FAIL", name, err.message);
  }
}

const sample = {
  id: "gdi_opp_demo_1",
  title: 'Annual "Best" Meeting, Bethesda',
  organizationName: "Org =Growth",
  segment: "Associations",
  priority: "MEDIUM_PRIORITY",
  eventStartDate: "2026-11-13T00:00:00.000Z",
  eventEndDate: "2026-11-15",
  opportunityQualification: "ACTIONABLE_NOW",
  bookingWindowStatus: "QUALIFY_NOW",
  demandTerritoryFit: "DMV_COMPETITIVE",
  demandTerritoryFitLabel: "DMV Competitive",
  summaryWhat: "Multi-day association meeting",
  whyNow: "Housing TBD",
  recommendedAction: "Qualify room block",
  hotelId: "recLuxvwwxID7U2B8",
  hotelFitScore: 88,
  evidenceConfidence: 70,
  _listDto: true,
  schemaVersion: "should_not_appear",
  primaryContact: {
    name: "Alex Planner",
    role: "Meetings Director",
    email: "alex@example.org",
    phone: "+1-301-555-0100",
    phoneTypeLabel: "Direct",
  },
  commercialProgression: {
    currentStatusLabel: "Worth pursuing",
    latestActionLabel: "Contacted",
    latestActionDate: "2026-09-10",
    latestOutcomeLabel: "No response yet",
    latestOutcomeDate: "2026-09-12",
  },
};

check("header_order_core_prefix_preserved", () => {
  const core = [
    "Dealality Opportunity ID",
    "Opportunity",
    "Organization",
    "Segment",
    "Priority",
    "Start Date",
    "End Date",
    "Qualification",
    "Booking Window",
    "Demand Territory",
    "Opportunity Summary",
    "Why Now",
    "Recommended Action",
    "Contact Name",
    "Contact Role",
    "Email",
    "Phone",
    "Phone Type",
    "Current Status",
    "Latest Action",
    "Latest Action Date",
    "Latest Outcome",
    "Latest Outcome Date",
  ];
  assert.deepEqual(GDI_CUSTOMER_CSV_HEADERS.slice(0, 23), core);
  assert.ok(GDI_CUSTOMER_CSV_HEADERS.length > 23, "full-detail columns added");
  assert.ok(GDI_CUSTOMER_CSV_HEADERS.includes("What Is Happening"));
  assert.ok(GDI_CUSTOMER_CSV_HEADERS.includes("Why This Hotel Fits"));
  assert.ok(GDI_CUSTOMER_CSV_HEADERS.includes("Full Recommended Outreach"));
  assert.ok(GDI_CUSTOMER_CSV_HEADERS.includes("Source URLs"));
  assert.ok(GDI_CUSTOMER_CSV_HEADERS.includes("Hotel Fit Explanation"));
});

check("real_csv_not_json", () => {
  const csv = buildCustomerExportCsv([sample]);
  assert.equal(csv.charCodeAt(0), 0xfeff); // BOM
  const body = csv.slice(1);
  assert.ok(!body.trimStart().startsWith("{"));
  assert.ok(body.startsWith("Dealality Opportunity ID,"));
  const clean = assertCustomerCsvClean(csv);
  assert.equal(clean.ok, true, JSON.stringify(clean.hits));
});

check("labels_not_raw_enums", () => {
  const row = toCustomerExportRow(sample);
  assert.equal(row.Priority, "Medium Priority");
  assert.equal(row["Booking Window"], "Qualify Now");
  assert.equal(row["Demand Territory"], "DMV Competitive");
  assert.equal(row["Start Date"], "2026-11-13");
  assert.equal(row["End Date"], "2026-11-15");
});

check("contact_and_progression_flattened", () => {
  const row = toCustomerExportRow(sample);
  assert.equal(row["Contact Name"], "Alex Planner");
  assert.equal(row.Email, "alex@example.org");
  assert.equal(row["Current Status"], "Worth pursuing");
  assert.equal(row["Latest Action"], "Contacted");
  assert.equal(row["Latest Action Date"], "2026-09-10");
  const csv = buildCustomerExportCsv([sample]);
  assert.doesNotMatch(csv, /primaryContact/);
  assert.doesNotMatch(csv, /commercialProgression/);
  assert.doesNotMatch(csv, /"name":/);
});

check("no_internal_fields", () => {
  const csv = buildCustomerExportCsv([sample]);
  assert.doesNotMatch(csv, /hotelId/);
  assert.doesNotMatch(csv, /schemaVersion/);
  assert.doesNotMatch(csv, /_listDto/);
  assert.doesNotMatch(csv, /hotelFitScore/);
  assert.doesNotMatch(csv, /evidenceConfidence/);
  assert.doesNotMatch(csv, /should_not_appear/);
});

check("comma_quote_escaping", () => {
  const csv = buildCustomerExportCsv([sample]);
  assert.match(csv, /"Annual ""Best"" Meeting, Bethesda"/);
});

check("csv_injection_guard", () => {
  assert.equal(escapeCsvCell("=CMD()"), "'=CMD()");
  assert.equal(escapeCsvCell("+1234"), "'+1234");
  assert.equal(escapeCsvCell("@sum"), "'@sum");
  const csv = buildCustomerExportCsv([
    { ...sample, whyNow: "=HYPERLINK(\"http://evil\")" },
  ]);
  assert.match(csv, /'=HYPERLINK/);
});

check("utf8_characters", () => {
  const csv = buildCustomerExportCsv([
    { ...sample, title: "Congreso Español — Bogotá", organizationName: "México A.C." },
  ]);
  assert.match(csv, /Congreso Español/);
  assert.match(csv, /México/);
  assert.equal(csv.charCodeAt(0), 0xfeff);
});

check("filename_readable_no_record_id", () => {
  const name = buildCustomerCsvFilename("Bethesda Marriott");
  assert.equal(name, "Bethesda Marriott - GDI Opportunities.csv");
  assert.doesNotMatch(name, /recLux/);
  const disp = customerCsvContentDisposition("Bethesda Marriott");
  assert.match(disp, /attachment;/);
  assert.match(disp, /Bethesda Marriott - GDI Opportunities\.csv/);
});

check("iso_date_blank_unknown", () => {
  assert.equal(toCsvIsoDate(null), "");
  assert.equal(toCsvIsoDate("not-a-date"), "");
  assert.equal(toCsvIsoDate("2026-05-01"), "2026-05-01");
});

check("buildExportCsv_alias", () => {
  const csv = buildExportCsv([sample], { hotelId: "recX", name: "Hotel" }, {});
  assert.equal(csv.charCodeAt(0), 0xfeff);
  assert.match(csv, /Dealality Opportunity ID/);
});

check("multiline_cells_quoted", () => {
  const csv = buildCustomerExportCsv(
    [
      {
        ...sample,
        whatIsHappening: "Line one.\nLine two with, comma.",
        sources: [{ url: "https://example.com/a" }, { url: "https://example.com/b" }],
      },
    ],
    { enrich: false }
  );
  assert.match(csv, /"Line one\.\nLine two with, comma\."/);
});

check("bethesda_canary_row_count", () => {
  const doc = JSON.parse(
    fs.readFileSync(
      path.join(
        ROOT,
        "data/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/opportunities.json"
      ),
      "utf8"
    )
  );
  const raw = doc.opportunities || [];
  const view = filterCustomerFacingOpportunities(filterSalespersonView(raw), {
    hotelId: "recLuxvwwxID7U2B8",
  });
  assert.equal(view.length, 19, `expected 19 customer-facing, got ${view.length}`);
  const csv = buildCustomerExportCsv(view, {
    hotelId: "recLuxvwwxID7U2B8",
    enrich: true,
  });
  const body = csv.replace(/^\uFEFF/, "");
  assert.ok(body.startsWith(GDI_CUSTOMER_CSV_HEADERS.join(",")));
  const ids = view.map((o) => o.id);
  for (const id of ids) {
    assert.ok(csv.includes(id), `missing id ${id}`);
  }
  assert.equal(new Set(ids).size, 19);
  const clean = assertCustomerCsvClean(csv);
  assert.equal(clean.ok, true, JSON.stringify(clean.hits));
  assert.match(csv, /What Is Happening/);
  assert.match(csv, /Why This Hotel Fits/);
  assert.equal(GDI_CUSTOMER_CSV_CORE_HEADERS.length, 23);
  assert.equal(
    GDI_CUSTOMER_CSV_HEADERS.length,
    GDI_CUSTOMER_CSV_CORE_HEADERS.length + GDI_CUSTOMER_CSV_DETAIL_HEADERS.length
  );

  let truncated = 0;
  let omitted = 0;
  let complete = 0;
  let unavailable = 0;
  for (const opp of view) {
    const withHotel = { ...opp, hotelId: "recLuxvwwxID7U2B8" };
    const exportRow = toCustomerExportRow(
      enrichOpportunityForCustomerExport(withHotel, {
        hotelId: "recLuxvwwxID7U2B8",
      })
    );
    const parity = compareDetailToExportRow(withHotel, exportRow);
    for (const p of parity) {
      if (p.classification === "present_and_complete") complete += 1;
      else if (p.classification === "source_unavailable") unavailable += 1;
      else if (p.classification === "present_in_source_omitted_by_export")
        omitted += 1;
      else if (p.classification === "present_in_export_but_truncated")
        truncated += 1;
    }
  }
  assert.equal(omitted, 0, `export omitted customer-visible fields: ${omitted}`);
  assert.equal(
    truncated,
    0,
    `export truncated customer-visible fields: ${truncated}`
  );
  console.log(
    "  bethesda_rows",
    19,
    "cols",
    GDI_CUSTOMER_CSV_HEADERS.length,
    "detail_cols",
    GDI_CUSTOMER_CSV_DETAIL_HEADERS.length,
    "parity",
    { complete, unavailable, omitted, truncated }
  );
});

check("regression_cohort_exports", () => {
  const hotels = [
    "recLuxvwwxID7U2B8",
    "recG66DQJKP2c0UNh",
    "recIwaP1etgx2g9nA",
  ];
  // Waterstone + NOW NOW NOHO if present
  for (const id of [
    "recgMYovrrZDJMqzX", // waterstone often
    "recGkME49yYuxQl0u",
  ]) {
    hotels.push(id);
  }
  for (const hotelId of hotels) {
    const p = path.join(
      ROOT,
      `data/group-demand-intelligence/hotels/${hotelId}/opportunities.json`
    );
    if (!fs.existsSync(p)) continue;
    const doc = JSON.parse(fs.readFileSync(p, "utf8"));
    const opps = filterSalespersonView(doc.opportunities || doc || []);
    if (!Array.isArray(opps) || !opps.length) continue;
    const csv = buildCustomerExportCsv(opps.slice(0, 5));
    assert.equal(assertCustomerCsvClean(csv).ok, true, hotelId);
    assert.match(csv, /Dealality Opportunity ID/);
  }
});

console.log(JSON.stringify({ suite: "gdi-customer-csv-export", passed, failed }, null, 2));
process.exit(failed ? 1 : 0);
