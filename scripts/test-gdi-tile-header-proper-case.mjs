/**
 * Lock Proper Case on browse-tile white headers (displayTitle + meta label).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
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
  ["Local Meetings & Catering", "Local Meetings & Catering"],
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
assert.match(uiSrc, /toProperCaseDisplayTitle\(\s*scrubEventDatesFromText\(titleRaw/);
// Meta classification must NOT be Proper-Cased in JS — CSS keeps ALL CAPS
assert.doesNotMatch(
  uiSrc,
  /var classLabel = toProperCaseDisplayTitle\(classBits\[0\]\)/
);

const cssPath = path.join(
  path.dirname(fileURLToPath(import.meta.url)),
  "../public/css/group-demand-intelligence.css"
);
const cssSrc = fs.readFileSync(cssPath, "utf8");
assert.match(
  cssSrc,
  /\.gdi-page \.brand-card__meta \{[\s\S]*?text-transform:\s*uppercase;/
);

console.log("test:gdi-tile-header-proper-case OK");
