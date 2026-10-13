#!/usr/bin/env node
/**
 * Unit tests: calendar series extractor + official source ranking + no benchmark leak.
 *   node scripts/test-gdi-event-series-recall-v2.mjs
 */
import assert from "assert";
import {
  extractEventSeriesFromCalendarPage,
  cyclesToDiscoveryCandidates,
  DISCOVERY_LANE,
} from "../lib/group-demand-intelligence/calendar-series-extractor.js";
import {
  scoreOfficialSource,
  rankOrganicByOfficialSource,
} from "../lib/group-demand-intelligence/official-source-ranking.js";
import { OPPORTUNITY_TYPE } from "../lib/group-demand-intelligence/claim-types.js";

const sampleText = `
Save the Date
Annual Biomedical Research Symposium
March 10-12, 2027
NIH Natcher Conference Center, Bethesda, MD
Accommodations information forthcoming.

Clinical Network Scientific Meeting
April 15-17, 2027
Natcher Building
Lodging details coming soon.

26th Regional Youth Tournament
January 1-3, 2027
Hotel selections released December 7
Stay to play program available.
`;

const extracted = extractEventSeriesFromCalendarPage({
  url: "https://www.nih.gov/news-events/events",
  text: sampleText,
  organizationHint: "National Institutes of Health",
  discoveryMode: DISCOVERY_LANE.NIH_CALENDAR,
});

assert.ok(extracted.cycles.length >= 2, "should extract multiple named cycles");
assert.ok(
  extracted.cycles.some((c) => /biomedical research symposium/i.test(c.title)),
  "named symposium"
);
assert.ok(
  extracted.cycles.some(
    (c) => c.opportunityType === OPPORTUNITY_TYPE.FIXED_VENUE_OPEN_HOUSING
  ),
  "fixed venue open housing"
);

const cands = cyclesToDiscoveryCandidates(extracted.cycles);
assert.ok(cands.every((c) => c.discoveryMode));
assert.ok(!JSON.stringify(cands).includes("TOPMed"));
assert.ok(!JSON.stringify(cands).includes("Crabtown"));

const gov = scoreOfficialSource("https://www.nih.gov/news-events/events", {
  title: "NIH Events",
});
const agg = scoreOfficialSource("https://10times.com/bethesda-events", {
  title: "Events in Bethesda",
});
assert.ok(gov.score > agg.score, "gov should outrank aggregator");

const ranked = rankOrganicByOfficialSource([
  { url: "https://10times.com/x", title: "Directory" },
  { url: "https://www.nih.gov/events", title: "NIH Events 2027" },
  { url: "https://example.org/future-meetings", title: "Future Meetings" },
]);
assert.equal(ranked[0].url, "https://www.nih.gov/events");

console.log(
  JSON.stringify(
    {
      ok: true,
      test: "GDI_EVENT_SERIES_RECALL_V2_UNITS",
      cycles: extracted.cycles.length,
      series: extracted.series.length,
      govScore: gov.score,
      aggScore: agg.score,
    },
    null,
    2
  )
);
