/**
 * Unit checks for landing analytics version / cutover filters.
 * Run: node scripts/test-landing-analytics-version.mjs
 */
import assert from "assert";
import {
  applyLandingReportFilters,
  buildCutoverCompare,
  filterEventsByLandingVersion,
  parseCutover,
  parseReportFilters,
  parseVersionFilter,
} from "../lib/marketing-landing-events-version.js";

const events = [
  { ts: "2026-07-20T12:00:00.000Z", event: "page_land", sessionId: "a", landingVersion: "v9" },
  { ts: "2026-07-21T12:00:00.000Z", event: "cta_click", sessionId: "a", landingVersion: "v9" },
  { ts: "2026-08-02T12:00:00.000Z", event: "page_land", sessionId: "b", landingVersion: "old-home" },
  { ts: "2026-08-02T12:05:00.000Z", event: "cta_click", sessionId: "b", landingVersion: "old-home" },
  { ts: "2026-08-03T09:00:00.000Z", event: "page_land", sessionId: "c" }, // untagged legacy
];

assert.strictEqual(parseVersionFilter("new"), "old-home");
assert.strictEqual(parseVersionFilter("v9"), "previous");
assert.strictEqual(parseCutover("2026-08-01"), "2026-08-01T00:00:00.000Z");

const previous = filterEventsByLandingVersion(events, "previous");
assert.strictEqual(previous.length, 3, "previous includes v9 + untagged");

const neu = filterEventsByLandingVersion(events, "old-home");
assert.strictEqual(neu.length, 2, "old-home only");

const filters = parseReportFilters({
  version: "all",
  cutover: "2026-08-01",
  era: "after",
});
const applied = applyLandingReportFilters(events, filters);
assert.strictEqual(applied.events.length, 3, "era=after keeps post-cutover rows (old-home + untagged)");
assert.ok(applied.compare);
assert.strictEqual(applied.compare.before.sessions, 1);
assert.strictEqual(applied.compare.after.sessions, 2);

const compareOnly = buildCutoverCompare(events, "2026-08-01");
assert.strictEqual(compareOnly.before.ctaClicks, 1);
assert.strictEqual(compareOnly.after.ctaClicks, 1);

console.log("ok: landing analytics version filters");
