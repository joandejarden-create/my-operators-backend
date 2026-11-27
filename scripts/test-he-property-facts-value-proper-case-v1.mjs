#!/usr/bin/env node
/**
 * Static gate: Property Facts right-column descriptive values use Proper Case.
 * Does not rewrite left-column labels or mutate Airtable/canonical records.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import vm from "node:vm";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const hdp = fs.readFileSync(path.join(ROOT, "public/js/hotel-detail-panel.js"), "utf8");

assert.match(hdp, /function formatPropertyFactsValue\s*\(/);
assert.match(hdp, /\(Brand Parent\)/);
assert.match(hdp, /Sq\. Ft\./);
assert.match(hdp, /Milestone/);
// Labels remain unchanged by this focused task (no ALL CAPS rewrite in this PR).
assert.match(hdp, /label:\s*"Last renovated"/);
assert.match(hdp, /label:\s*"Meeting space"/);
assert.doesNotMatch(hdp, /label:\s*"LAST RENOVATED"/);

// Extract and execute only the formatter helper.
const start = hdp.indexOf("function formatPropertyFactsValue(text)");
const end = hdp.indexOf("\n  function formatParentCompanyDisplay", start);
assert.ok(start >= 0 && end > start, "formatter block bounds");
const fnSrc = hdp.slice(start, end);
const sandbox = {};
vm.runInNewContext(fnSrc + "\nthis.formatPropertyFactsValue = formatPropertyFactsValue;", sandbox);

const f = sandbox.formatPropertyFactsValue;
assert.equal(
  f("Marriott International (brand parent)"),
  "Marriott International (Brand Parent)"
);
assert.equal(
  f("18,719 sq ft · 27 rooms · largest: Grand Ballroom, 4,592 sq ft"),
  "18,719 Sq. Ft. · 27 Rooms · Largest: Grand Ballroom, 4,592 Sq. Ft."
);
assert.equal(f("2015 (major); 2017 milestone"), "2015 (Major); 2017 Milestone");
assert.equal(f("IHG"), "IHG");
assert.equal(f("Upper Upscale"), "Upper Upscale");

console.log("ok - he-property-facts-value-proper-case-v1");
