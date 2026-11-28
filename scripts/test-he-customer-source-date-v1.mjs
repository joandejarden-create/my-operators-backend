#!/usr/bin/env node
/**
 * Regression: Sources & Evidence DATE column consolidates Published + Verified/Accessed.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  HE_CUSTOMER_SOURCE_DATE_V1,
  SOURCE_DATE_SORT_PRIORITY,
  formatReadableSourceDate,
  formatCustomerSourceDateCell,
  citationDateSortIso,
  compareCitationDates,
} from "../lib/hotel-intelligence/sources/he-customer-source-date-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");

assert.equal(HE_CUSTOMER_SOURCE_DATE_V1, "he-customer-source-date-v1");
assert.deepEqual(SOURCE_DATE_SORT_PRIORITY, [
  "publication_date",
  "verified_at",
  "accessed_at",
]);

assert.equal(formatReadableSourceDate("2026-11-19"), "Nov 19, 2026");
assert.equal(formatReadableSourceDate(null), null);
assert.equal(formatReadableSourceDate(""), null);

const pubOnly = formatCustomerSourceDateCell({
  publication_date: "2026-11-19",
  verified_at: null,
  accessed_at: null,
});
assert.deepEqual(pubOnly.lines, ["Published: Nov 19, 2026"]);
assert.equal(pubOnly.sortValue, "2026-11-19");

const accOnly = formatCustomerSourceDateCell({
  publication_date: null,
  verified_at: "2026-10-01",
  accessed_at: null,
});
assert.deepEqual(accOnly.lines, ["Accessed: Oct 1, 2026"]);
assert.equal(accOnly.sortValue, "2026-10-01");

const both = formatCustomerSourceDateCell({
  publication_date: "2026-11-19",
  verified_at: "2026-11-20",
  accessed_at: "2026-11-18",
});
assert.deepEqual(both.lines, [
  "Published: Nov 19, 2026",
  "Accessed: Nov 20, 2026",
]);
assert.equal(both.sortValue, "2026-11-19", "sort prefers publication_date");

const neither = formatCustomerSourceDateCell({});
assert.deepEqual(neither.lines, ["—"]);
assert.equal(neither.sortValue, null);

assert.equal(
  citationDateSortIso({ verified_at: "2026-05-01", accessed_at: "2026-06-01" }),
  "2026-05-01",
  "verified precedes accessed when publication missing"
);

const older = { publication_date: "2020-01-01" };
const newer = { publication_date: "2026-11-19" };
const undated = {};
assert.ok(compareCitationDates(newer, older, "desc") < 0);
assert.ok(compareCitationDates(undated, newer, "desc") > 0, "nulls last");

const workspace = fs.readFileSync(
  path.join(root, "public/js/hotel-intelligence-workspace.js"),
  "utf8"
);
assert.match(workspace, /th\("Date"\)/);
assert.doesNotMatch(workspace, /th\("Published"\)/);
assert.doesNotMatch(workspace, /th\("Verified \/ accessed"\)/);
assert.match(workspace, /formatCustomerSourceDateCell|HeCustomerSourceDate/);
assert.match(workspace, /compareCitationDates|SOURCE_DATE_SORT_PRIORITY/);

const browser = fs.readFileSync(
  path.join(root, "public/js/he-customer-source-date-v1.js"),
  "utf8"
);
assert.match(browser, /HeCustomerSourceDate/);
assert.match(browser, /Published:/);
assert.match(browser, /Accessed:/);

const radar = fs.readFileSync(
  path.join(root, "public/deal-capture-radar-with-ranked-list.html"),
  "utf8"
);
assert.match(radar, /he-customer-source-date-v1\.js/);

console.log(
  JSON.stringify(
    {
      ok: true,
      test: "test-he-customer-source-date-v1",
      sortPriority: SOURCE_DATE_SORT_PRIORITY,
      samples: { pubOnly: pubOnly.text, both: both.text, neither: neither.text },
    },
    null,
    2
  )
);
console.log("OK — HE customer source DATE column gate");
