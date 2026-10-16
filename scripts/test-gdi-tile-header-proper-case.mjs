/**
 * Lock Proper Case on browse-tile white headers (displayTitle + secondary meta).
 * Classification + sourcing status must share the same presentation formatter.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import vm from "node:vm";
import {
  accountLevelDisplayTitle,
  toProperCaseDisplayTitle,
} from "../lib/group-demand-intelligence/opportunity-list-dto.js";

const samples = [
  [
    "A. Dominick Events — Professional planners & intermediaries with recurring event business",
    "A. Dominick Events — Professional Planners & Intermediaries With Recurring Event Business",
  ],
  [
    "N Street Village — Association lunches, chapter meetings & awards dinners",
    "N Street Village — Association Lunches, Chapter Meetings & Awards Dinners",
  ],
  [
    "CSI DMC - Washington, DC — Professional planners & intermediaries with recurring event business",
    "CSI DMC - Washington, DC — Professional Planners & Intermediaries With Recurring Event Business",
  ],
  ["CORPORATE ACCOUNT RELATIONSHIP", "Corporate Account Relationship"],
  ["Local Meetings & Catering", "Local Meetings & Catering"],
  ["Hotel sourcing unconfirmed", "Hotel Sourcing Unconfirmed"],
  [
    "Primary Venue Selected / Overflow Possible",
    "Primary Venue Selected / Overflow Possible",
  ],
  ["RFP / Active Sourcing", "RFP / Active Sourcing"],
  ["Hotel / Venue TBD", "Hotel / Venue TBD"],
  ["nih nyc dmc plus arias", "NIH NYC DMC PLUS ARIAS"],
];

for (const [raw, expected] of samples) {
  assert.equal(toProperCaseDisplayTitle(raw), expected);
}

assert.equal(
  accountLevelDisplayTitle({
    title:
      "Events by Andre Wells — Professional planners & intermediaries with recurring event business",
    organizationName: "Events by Andre Wells",
  }),
  "Events By Andre Wells — Professional Planners & Intermediaries With Recurring Event Business"
);

const uiPath = path.join(
  path.dirname(fileURLToPath(import.meta.url)),
  "../public/js/group-demand-intelligence/dealality-gdi-ui.js"
);
const uiSrc = fs.readFileSync(uiPath, "utf8");
assert.match(uiSrc, /function toProperCaseDisplayTitle\s*\(/);
assert.match(uiSrc, /function formatTileSecondaryHeadlineLabel\s*\(/);
assert.match(uiSrc, /toProperCaseDisplayTitle\(\s*scrubEventDatesFromText\(titleRaw/);
assert.match(
  uiSrc,
  /var classLabel = formatTileSecondaryHeadlineLabel\(classBits\[0\]\)/
);
assert.match(
  uiSrc,
  /esc\(formatTileSecondaryHeadlineLabel\(sourcingLbl\)\)/
);
assert.doesNotMatch(
  uiSrc,
  /sourcingLbl\s*\?\s*" · " \+ esc\(String\(sourcingLbl\)\)/
);

const cssPath = path.join(
  path.dirname(fileURLToPath(import.meta.url)),
  "../public/css/group-demand-intelligence.css"
);
const cssSrc = fs.readFileSync(cssPath, "utf8");
assert.match(
  cssSrc,
  /\.gdi-page \.brand-card__meta \{[\s\S]*?text-transform:\s*none;/
);

const sandbox = {
  window: {},
  globalThis: {},
  console,
  URL,
  URLSearchParams,
};
sandbox.window = sandbox;
sandbox.globalThis = sandbox;
vm.runInNewContext(uiSrc, sandbox, { filename: "dealality-gdi-ui.js" });
const UI = sandbox.DealalityGdiUi;
assert.ok(UI, "DealalityGdiUi missing");
assert.equal(typeof UI.formatTileSecondaryHeadlineLabel, "function");

const labelCases = [
  ["Hotel sourcing unconfirmed", "Hotel Sourcing Unconfirmed"],
  ["HOTEL_SOURCING_UNCONFIRMED", "Hotel Sourcing Unconfirmed"],
  [
    "Primary Venue Selected / Overflow Possible",
    "Primary Venue Selected / Overflow Possible",
  ],
  [
    "PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE",
    "Primary Venue Selected / Overflow Possible",
  ],
  ["Association occasion", "Association Occasion"],
  ["Preferred Lodging Partnership", "Preferred Lodging Partnership"],
  ["Scaling Company", "Scaling Company"],
  ["Local Meetings & Catering", "Local Meetings & Catering"],
  ["RFP / Active Sourcing", "RFP / Active Sourcing"],
  ["Hotel / Venue TBD", "Hotel / Venue TBD"],
];
for (const [raw, expected] of labelCases) {
  assert.equal(UI.formatTileSecondaryHeadlineLabel(raw), expected, raw);
}

const reviewedHtml = UI.opportunityTileHtml(
  {
    opportunityId: "gdi_opp_test_reviewed",
    displayTitle: "AHIMA Advocacy Summit 2027",
    organizationName: "AHIMA",
    opportunityTypeLabel: "Association occasion",
    venueSourcingStatusLabel: "Hotel sourcing unconfirmed",
    summaryWhat: "Reviewed prospect with usable contact.",
    primaryContact: { name: "Ada Lovelace", title: "Meetings Lead" },
  },
  {
    displayTitle: "AHIMA Advocacy Summit 2027",
    opportunityTypeLabel: "Association occasion",
    venueSourcingStatusLabel: "Hotel sourcing unconfirmed",
    priority: "HIGH_PRIORITY",
    bookingWindowStatus: "CONTACT_NOW",
  },
  {}
);
assert.match(
  reviewedHtml,
  /Association Occasion · Hotel Sourcing Unconfirmed/
);
assert.doesNotMatch(reviewedHtml, /Hotel sourcing unconfirmed/);
assert.match(reviewedHtml, /brand-card__meta/);
assert.match(reviewedHtml, /Ada Lovelace|View Details/);

const overflowHtml = UI.opportunityTileHtml(
  {
    opportunityId: "gdi_opp_test_overflow",
    displayTitle: "Broadway League Awards Dinner",
    organizationName: "Broadway League",
    opportunityTypeLabel: "Association Occasion",
    venueSourcingStatusLabel: "Primary Venue Selected / Overflow Possible",
    summaryWhat: "Legacy event with overflow possible.",
  },
  {
    venueSourcingStatusLabel: "Primary Venue Selected / Overflow Possible",
    opportunityTypeLabel: "Association Occasion",
  },
  {}
);
assert.match(
  overflowHtml,
  /Association Occasion · Primary Venue Selected \/ Overflow Possible/
);

const escaped = UI.opportunityTileHtml(
  {
    opportunityId: "gdi_opp_test_escape",
    displayTitle: "Test <script> Occasion",
    organizationName: "Org & Co",
    opportunityTypeLabel: "Scaling Company",
    venueSourcingStatusLabel: 'Hotel sourcing unconfirmed"><img src=x>',
  },
  {},
  {}
);
assert.match(escaped, /Scaling Company · Hotel Sourcing Unconfirmed/);
assert.doesNotMatch(escaped, /<script>/);
assert.doesNotMatch(escaped, /"><img/);
assert.match(escaped, /&lt;script&gt;|&amp;/);

console.log("test:gdi-tile-header-proper-case OK");
