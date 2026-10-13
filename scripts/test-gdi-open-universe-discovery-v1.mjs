#!/usr/bin/env node
/**
 * GDI open-universe + event-series discovery architecture gate (no live spend).
 * Ensures query classes exist that would cover missed Bethesda recall patterns
 * WITHOUT hardcoding benchmark event names.
 *
 *   node scripts/test-gdi-open-universe-discovery-v1.mjs
 */
import assert from "assert";
import {
  generateOpenUniverseQueries,
  generateEventSeriesExpansionQueries,
  inferOpenUniverseThemes,
} from "../lib/group-demand-intelligence/open-universe-query-generator.js";
import { listGdiResearchMethods } from "../lib/group-demand-intelligence/research-methods.js";
import {
  DISCOVERY_MODE,
  shouldRunDiscoveryMode,
} from "../lib/group-demand-intelligence/discovery-modes.js";
import {
  OPPORTUNITY_TYPE,
  VENUE_SOURCING_STATUS,
  FUTURE_CYCLE_STATE,
} from "../lib/group-demand-intelligence/claim-types.js";
import { normalizeVenueStatus } from "../lib/group-demand-intelligence/provider-normalization.js";

const profile = {
  market: "Bethesda",
  metro: "Washington DC metro",
  archetype: "full-service suburban meeting hotel near federal research campus",
  demandSectors: ["medical", "scientific", "healthcare association", "advocacy", "sports"],
  institutionalAnchors: ["federal research campus", "biomedical"],
  meetingCapability: "large ballroom association meetings",
};

const themes = inferOpenUniverseThemes(profile);
assert.ok(themes.includes("MEDICAL_SCIENTIFIC"));
assert.ok(themes.includes("SPORTS_TOURNAMENT") || themes.includes("ASSOCIATION_ADVOCACY"));

const ou = generateOpenUniverseQueries(profile);
assert.ok(ou.queries.some((q) => q.queryClass === "FUTURE_MEETINGS_CALENDAR"));
assert.ok(ou.queries.some((q) => q.queryClass === "DESTINATION_TBD"));
assert.ok(ou.queries.some((q) => q.queryClass === "FIXED_VENUE_OPEN_HOUSING"));
assert.ok(ou.queries.some((q) => q.queryClass === "STAY_TO_PLAY"));

// Query-class coverage for recall gaps (generic — not seeded event names)
const coverage = {
  NIH_CAMPUS_FIXED_VENUE_HOUSING: ou.queries.some(
    (q) => q.queryClass === "FIXED_VENUE_OPEN_HOUSING"
  ),
  ASSOCIATION_FUTURE_CALENDAR: ou.queries.some(
    (q) => q.queryClass === "FUTURE_MEETINGS_CALENDAR"
  ),
  DESTINATION_TBD_SIGNAL: ou.queries.some((q) => q.queryClass === "DESTINATION_TBD"),
  SPORTS_STAY_TO_PLAY: ou.queries.some((q) => q.queryClass === "STAY_TO_PLAY"),
  EVENT_SERIES_EXPANSION: generateEventSeriesExpansionQueries({
    organization: "Generic Association Placeholder",
  }).ok,
};

assert.ok(Object.values(coverage).every(Boolean), JSON.stringify(coverage));

const methods = listGdiResearchMethods();
for (const id of [
  "GDI-FUTURE-CAL-01",
  "GDI-OPEN-UNIVERSE-01",
  "GDI-FIXED-VENUE-HOUSING-01",
  "GDI-SPORTS-STAY-01",
]) {
  assert.ok(methods.some((m) => m.id === id), `missing method ${id}`);
}

assert.equal(OPPORTUNITY_TYPE.FIXED_VENUE_OPEN_HOUSING, "FIXED_VENUE_OPEN_HOUSING");
assert.equal(normalizeVenueStatus("destination TBD"), VENUE_SOURCING_STATUS.DESTINATION_TBD);
assert.ok(FUTURE_CYCLE_STATE.VENUE_TBD);

assert.equal(
  shouldRunDiscoveryMode(DISCOVERY_MODE.OPEN_UNIVERSE_DISCOVERY, {
    lastRunAt: new Date(Date.now() - 40 * 24 * 60 * 60 * 1000).toISOString(),
  }).run,
  true
);

// Hard forbid benchmark seed leakage in generator output
const forbidden =
  /TOPMed|CTN Annual|NINDS CTE|High-Risk, High-Reward|AMWA 112|AMWA 113|iCAN 2027|AAOS 2027|AAPA 2027|Crabtown|Capital Showdown|Fuel Medical|FedHealth|Quantum Readiness/i;
assert.ok(!forbidden.test(JSON.stringify(ou)), "benchmark names must not appear in generator");

console.log(
  JSON.stringify(
    {
      ok: true,
      test: "GDI_OPEN_UNIVERSE_DISCOVERY_V1",
      themes,
      queryCount: ou.queries.length,
      coverage,
      cadence: {
        weeklyKnownTarget: shouldRunDiscoveryMode(DISCOVERY_MODE.KNOWN_TARGET_MONITORING).run,
        openUniverseHoldWhenFresh: shouldRunDiscoveryMode(
          DISCOVERY_MODE.OPEN_UNIVERSE_DISCOVERY,
          { lastRunAt: new Date().toISOString() }
        ).run,
      },
    },
    null,
    2
  )
);
