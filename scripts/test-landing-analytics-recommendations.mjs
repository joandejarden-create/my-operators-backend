/**
 * Unit checks for locale filters + recommendation engine.
 * Run: node scripts/test-landing-analytics-recommendations.mjs
 */
import assert from "assert";
import {
  eventLocale,
  filterEventsByLocale,
  buildLocaleCompare,
  parseLocaleFilter,
} from "../lib/marketing-landing-events-locale.js";
import { buildLandingRecommendations } from "../lib/marketing-landing-events-recommendations.js";
import { buildFunnelFromEvents } from "../lib/marketing-landing-events-sessions.js";
import { applyLandingReportFilters } from "../lib/marketing-landing-events-version.js";

assert.strictEqual(parseLocaleFilter("spanish"), "es");
assert.strictEqual(eventLocale({ language: "es" }), "es");
assert.strictEqual(eventLocale({ path: "/es?utm=x" }), "es");
assert.strictEqual(eventLocale({ path: "/" }), "en");

const mixed = [
  { ts: "2026-08-01T10:00:00.000Z", event: "page_land", sessionId: "en1", language: "en", path: "/", landingVersion: "old-home" },
  { ts: "2026-08-01T10:00:05.000Z", event: "first_scroll", sessionId: "en1", language: "en", path: "/" },
  { ts: "2026-08-01T10:00:10.000Z", event: "section_view", sessionId: "en1", section: "oh-how-we-do-it", language: "en" },
  { ts: "2026-08-01T10:00:20.000Z", event: "cta_click", sessionId: "en1", location: "hero", language: "en" },
  { ts: "2026-08-01T11:00:00.000Z", event: "page_land", sessionId: "es1", language: "es", path: "/es", landingVersion: "old-home" },
  { ts: "2026-08-01T11:00:30.000Z", event: "page_land", sessionId: "es2", path: "/es/home", landingVersion: "old-home" },
];

assert.strictEqual(filterEventsByLocale(mixed, "es").length, 2);
assert.strictEqual(filterEventsByLocale(mixed, "en").length, 4);

const compare = buildLocaleCompare(mixed);
assert.strictEqual(compare.en.sessions, 1);
assert.strictEqual(compare.es.sessions, 2);
assert.strictEqual(compare.en.ctaRate, 100);

const applied = applyLandingReportFilters(mixed, {
  version: "old-home",
  locale: "es",
});
assert.strictEqual(applied.events.length, 2);
assert.ok(applied.localeCompare);
assert.strictEqual(applied.filters.locale, "es");

// Old-home section aliases should count toward How / funnel
const funnel = buildFunnelFromEvents(mixed.filter((e) => e.sessionId === "en1"));
const how = funnel.steps.find((s) => s.key === "reached_how");
assert.ok(how && how.count === 1, "oh-how-we-do-it maps to reached_how");

const lowScrollAggregate = {
  totals: { sessions: 20, videoOpens: 0, emailCaptures: 0 },
  funnel: {
    sessionCount: 20,
    biggestDropOff: {
      fromLabel: "Started Scrolling",
      toLabel: "Saw Platform Section",
      drop: 12,
      dropRate: 60,
    },
    steps: [
      { key: "landed", count: 20, rate: 100 },
      { key: "scrolled", count: 6, rate: 30 },
      { key: "reached_how", count: 2, rate: 10 },
      { key: "cta_click", count: 0, rate: 0 },
    ],
  },
  benchmarks: [
    { funnelKey: "scrolled", targetRate: 70, goodMin: 55 },
    { funnelKey: "cta_click", targetRate: 8, goodMin: 4 },
  ],
  ctaPaths: { paths: [] },
  faqHeatmap: [],
  ctaLocations: [],
  video: { hasData: false, sessionsOpened: 0 },
};

const recs = buildLandingRecommendations(lowScrollAggregate, {
  localeCompare: compare,
  localeFilter: "all",
});
assert.ok(recs.items.some((r) => r.id === "low-scroll"));
assert.ok(recs.items.some((r) => r.id === "zero-cta"));
assert.ok(recs.summary.actionableCount >= 1);

console.log("ok: landing analytics locale + recommendations");
