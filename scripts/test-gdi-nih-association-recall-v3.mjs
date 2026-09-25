#!/usr/bin/env node
/**
 * V3 unit tests: official-page-fetch body assessment + vague title filter + source rank v3.
 *   node scripts/test-gdi-nih-association-recall-v3.mjs
 */
import assert from "assert";
import {
  assessOfficialPageBody,
  BODY_QUALITY,
  FETCH_METHOD,
  OFFICIAL_PAGE_FETCH_VERSION,
} from "../lib/group-demand-intelligence/official-page-fetch.js";
import {
  extractEventSeriesFromCalendarPage,
  DISCOVERY_LANE,
} from "../lib/group-demand-intelligence/calendar-series-extractor.js";
import {
  scoreOfficialSource,
  SOURCE_RANK_VERSION,
} from "../lib/group-demand-intelligence/official-source-ranking.js";
import { validateQualifiedCandidate } from "../lib/group-demand-intelligence/promote-qualified-opportunity.js";
import { OPPORTUNITY_QUALIFICATION, ROOM_DEMAND_STATUS } from "../lib/group-demand-intelligence/claim-types.js";

assert.ok(OFFICIAL_PAGE_FETCH_VERSION);
assert.ok(String(SOURCE_RANK_VERSION).includes("v3"));

// Thin SPA shell → render recommended
const thin = assessOfficialPageBody(
  '<div id="root"></div><script src="/app.js"></script>',
  "Loading…",
  "https://calendar.nih.gov/"
);
assert.strictEqual(thin.renderRecommended, true);
assert.ok(
  thin.quality === BODY_QUALITY.JS_RENDER_REQUIRED ||
    thin.quality === BODY_QUALITY.STATIC_HTML_EMPTY
);

// Rich static body → no render
const richText = "A".repeat(2500) + " Annual Symposium March 2027 accommodations lodging hotel";
const rich = assessOfficialPageBody(`<html><body>${richText}</body></html>`, richText, "https://example.org/events");
assert.strictEqual(rich.renderRecommended, false);
assert.strictEqual(rich.quality, BODY_QUALITY.STATIC_HTML_OK);

// Vague NIH titles rejected
const vague = extractEventSeriesFromCalendarPage({
  url: "https://www.nih.gov/news-events/events",
  text: `
Natcher Meetings
NIH event
March 2027
High-Risk, High-Reward Research Symposium
March 10-12, 2027
NIH Natcher Conference Center
Accommodations forthcoming
`,
  discoveryMode: DISCOVERY_LANE.NIH_CALENDAR,
});
assert.ok(
  !vague.cycles.some((c) => /^natcher meetings$/i.test(c.title.trim())),
  "vague Natcher Meetings rejected"
);
assert.ok(
  !vague.cycles.some((c) => /^nih event$/i.test(c.title.trim())),
  "vague NIH event rejected"
);
assert.ok(
  vague.cycles.some((c) => /high-risk|high-reward|symposium/i.test(c.title)),
  "named symposium kept"
);

// Source rank V3 boosts official future-meeting / housing paths
const govCal = scoreOfficialSource("https://commonfund.nih.gov/highrisk/symposium", {
  title: "High-Risk Research Symposium",
  knownOfficialDomains: ["nih.gov", "commonfund.nih.gov"],
});
const aggreg = scoreOfficialSource("https://www.10times.com/something", {
  title: "Conference listing",
});
assert.ok(govCal.score > aggreg.score + 40, "official >> aggregator");
assert.ok(govCal.reasons.includes("high_value_official_path_v3") || govCal.score >= 60);

// Promotion validation accepts WATCH/HOUSING with lodging evidence
const ok = validateQualifiedCandidate({
  id: "gdi_opp_test_housing_2027",
  hotelId: "recLuxvwwxID7U2B8",
  title: "Named Research Symposium 2027",
  opportunityQualification: OPPORTUNITY_QUALIFICATION.MODERATE,
  eventStartDate: "2027-03-10",
  officialSource: "https://commonfund.nih.gov/highrisk/symposium",
  roomDemandStatus: ROOM_DEMAND_STATUS.HOUSING_PENDING,
  lodgingEvidence: "Housing pending / open lodging path",
});
assert.strictEqual(ok.ok, true, JSON.stringify(ok.failed));

assert.ok(FETCH_METHOD.RENDERED_PLAYWRIGHT);
console.log(JSON.stringify({ ok: true, suite: "gdi_nih_association_recall_v3" }, null, 2));
