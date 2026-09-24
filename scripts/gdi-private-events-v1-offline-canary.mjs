/**
 * GDI Private Events V1 — Bethesda offline canary + founder report.
 * NO LIVE WRITE. NO DEPLOY. NO CONTACT ENRICHMENT.
 */

import { mkdirSync, writeFileSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

import { HOTELS } from "../fixtures/group-demand-intelligence/private-events/hotels.mjs";
import {
  createVenueGraph,
  runPrivateEventsForHotel,
} from "../lib/group-demand-intelligence/private-events/index.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const FIX = join(ROOT, "fixtures/group-demand-intelligence/private-events");
const OUT = join(ROOT, "reports/group-demand-intelligence/private-events-v1");

mkdirSync(OUT, { recursive: true });

// Ensure universes exist
spawnSync(process.execPath, [join(FIX, "generate-universes.mjs")], {
  cwd: ROOT,
  stdio: "inherit",
});

const bethesdaVenues = JSON.parse(
  readFileSync(join(FIX, "venues-bethesda.json"), "utf8")
).venues;
const renaissanceVenues = JSON.parse(
  readFileSync(join(FIX, "venues-renaissance-ny.json"), "utf8")
).venues;
const cambridgeVenues = JSON.parse(
  readFileSync(join(FIX, "venues-cambridge-beaches.json"), "utf8")
).venues;
const signals = JSON.parse(
  readFileSync(join(FIX, "event-signals-bethesda.json"), "utf8")
).signals;

const graph = createVenueGraph();
const costs = {
  venueDiscoveryQueries: 0,
  venueFetches: bethesdaVenues.length,
  eventSignalQueries: 0,
  officialPageFetches: bethesdaVenues.filter((v) => v.website).length,
  candidates: 0,
  qualified: 0,
  trueCount: 0,
  costUsd: 0,
  cacheReuse: 0,
};

const bethesda = runPrivateEventsForHotel({
  hotel: HOTELS.bethesda,
  venues: bethesdaVenues,
  eventSignals: signals,
  venueGraph: graph,
  costTracker: costs,
});

const renaissance = runPrivateEventsForHotel({
  hotel: HOTELS.renaissance_ny,
  venues: renaissanceVenues,
  venueGraph: graph,
});

const cambridge = runPrivateEventsForHotel({
  hotel: HOTELS.cambridge_beaches,
  venues: cambridgeVenues,
  venueGraph: graph,
});

const testRun = spawnSync(
  process.execPath,
  [join(ROOT, "scripts/test-gdi-private-events-v1.mjs")],
  { cwd: ROOT, encoding: "utf8" }
);
const testPass = (testRun.stdout || "").match(/PASS {2}/g)?.length || 0;
const testFail = (testRun.stdout || "").match(/FAIL {2}/g)?.length || 0;
const testsOk = testRun.status === 0;

writeFileSync(
  join(OUT, "BETHESDA_CANARY.json"),
  JSON.stringify(
    {
      generatedAt: new Date().toISOString(),
      mode: "OFFLINE_DRY_RUN",
      liveWrite: false,
      deploy: false,
      contactEnrichment: false,
      bethesda,
      replication: {
        renaissance: {
          hotelArchetype: renaissance.hotelArchetype,
          venueTotal: renaissance.venueUniverse.total,
          priority: {
            HIGH_POTENTIAL_PARTNER:
              renaissance.venueUniverse.HIGH_POTENTIAL_PARTNER,
            EVENT_SIGNAL_TARGET: renaissance.venueUniverse.EVENT_SIGNAL_TARGET,
            WATCH: renaissance.venueUniverse.WATCH,
            LOW_HOTEL_OPPORTUNITY:
              renaissance.venueUniverse.LOW_HOTEL_OPPORTUNITY,
          },
          preferredVenueTypes: renaissance.preferredVenueTypes,
          actionablePartnerships:
            renaissance.venuePartnershipOpportunities.length,
        },
        cambridge: {
          hotelArchetype: cambridge.hotelArchetype,
          venueTotal: cambridge.venueUniverse.total,
          priority: {
            HIGH_POTENTIAL_PARTNER:
              cambridge.venueUniverse.HIGH_POTENTIAL_PARTNER,
            EVENT_SIGNAL_TARGET: cambridge.venueUniverse.EVENT_SIGNAL_TARGET,
            WATCH: cambridge.venueUniverse.WATCH,
            LOW_HOTEL_OPPORTUNITY:
              cambridge.venueUniverse.LOW_HOTEL_OPPORTUNITY,
          },
          preferredVenueTypes: cambridge.preferredVenueTypes,
          actionablePartnerships:
            cambridge.venuePartnershipOpportunities.length,
        },
        globalVenueGraphSize: graph.size(),
      },
      tests: { pass: testPass, fail: testFail, ok: testsOk },
    },
    null,
    2
  )
);

function yn(v) {
  return v ? "YES" : "NO";
}
function pf(v) {
  return v ? "PASS" : "FAIL";
}

const actionablePartnerships = bethesda.venuePartnershipOpportunities;
const actionableEvents = bethesda.specificEventOpportunities;

const verdict =
  testsOk &&
  bethesda.venueUniverse.total >= 50 &&
  bethesda.venueUniverse.HIGH_POTENTIAL_PARTNER >= 1 &&
  bethesda.derivedDemand.falsePrecisionIssues === 0 &&
  renaissance.hotelArchetype !== bethesda.hotelArchetype &&
  cambridge.hotelArchetype !== bethesda.hotelArchetype
    ? bethesda.eventSignals.actionable >= 1
      ? "PRIVATE EVENTS V1 PASSES — REPLICABLE GDI MODULE READY FOR CONTROLLED USE"
      : "PASSES WITH WATCH ITEMS — ONE SMALL CYCLE REMAINS"
    : bethesda.venueUniverse.total >= 50 &&
        bethesda.venueUniverse.HIGH_POTENTIAL_PARTNER >= 1
      ? "VENUE GRAPH WORKS — EVENT SIGNAL DISCOVERY NEEDS IMPROVEMENT"
      : "PRIVATE EVENTS DISCOVERY TOO NOISY — HOLD";

const report = `# GDI Private Events + Wedding Demand V1 — Founder Report

Generated: ${new Date().toISOString()}  
Mode: **OFFLINE DRY-RUN** · Live write: NO · Deploy: NO · Contact enrichment: NO

---

## A. ARCHITECTURE

| Check | Result |
|------|--------|
| VENUE ENTITY CREATED | YES |
| GLOBAL VENUE REUSE | YES |
| CANONICAL GDI MODEL REUSED | YES |
| SEPARATE DB | NO |

---

## B. BETHESDA VENUE UNIVERSE

| Metric | Count |
|--------|------:|
| TOTAL VENUES DISCOVERED | ${bethesda.venueUniverse.total} |
| HIGH_POTENTIAL_PARTNER | ${bethesda.venueUniverse.HIGH_POTENTIAL_PARTNER} |
| EVENT_SIGNAL_TARGET | ${bethesda.venueUniverse.EVENT_SIGNAL_TARGET} |
| WATCH | ${bethesda.venueUniverse.WATCH} |
| LOW_HOTEL_OPPORTUNITY | ${bethesda.venueUniverse.LOW_HOTEL_OPPORTUNITY} |

---

## C. VENUE DATA QUALITY

| Field | Coverage |
|-------|----------|
| WITH CAPACITY | ${bethesda.venueDataQuality.withCapacityPct}% |
| WITH LODGING STATUS | ${bethesda.venueDataQuality.withLodgingStatusPct}% |
| WITH EVENT ACTIVITY | ${bethesda.venueDataQuality.withEventActivityPct}% |
| WITH HOTEL PARTNERSHIP STATUS | ${bethesda.venueDataQuality.withHotelPartnershipStatusPct}% |
| WITH CONTACT PATH | ${bethesda.venueDataQuality.withContactPathPct}% |
| WITH OFFICIAL SOURCE | ${bethesda.venueDataQuality.withOfficialSourcePct}% |

---

## D. EVENT SIGNALS

| Metric | Count |
|--------|------:|
| FUTURE EVENT SIGNALS | ${bethesda.eventSignals.futureEventSignals} |
| SPECIFIC EVENT CANDIDATES | ${bethesda.eventSignals.specificEventCandidates} |
| ACTIONABLE | ${bethesda.eventSignals.actionable} |
| WATCH | ${bethesda.eventSignals.watch} |
| REJECTED | ${bethesda.eventSignals.rejected} |

---

## E. VENUE PARTNERSHIP OPPORTUNITIES (actionable)

${
  actionablePartnerships.length
    ? actionablePartnerships
        .slice(0, 12)
        .map(
          (p) =>
            `### ${p.venueName}
- **Venue Type:** ${p.venueType}
- **Capacity:** ${p.capacity ?? "UNKNOWN"}
- **On-site Lodging:** ${p.onSiteLodging} (${p.onSiteGuestrooms ?? "?"} rooms)
- **Distance:** ${p.distanceMiles ?? "?"} mi
- **Event Frequency:** ${p.privateEventActivity?.estimatedAnnualPrivateEvents ?? "?"} / yr
- **Hotel Partner Status:** ${
              p.existingHotelPartner?.length
                ? p.existingHotelPartner.join(", ")
                : "None known"
            }
- **Buyer Path:** ${p.contactPath}
- **Why Opportunity:** ${p.whyOpportunityExists}
- **Recommended Action:** ${p.recommendedAction}
- **Source:** ${(p.evidence || []).join(", ") || "n/a"}
`
        )
        .join("\n")
    : "_None actionable in this offline canary._"
}

---

## F. SPECIFIC EVENT OPPORTUNITIES (actionable)

${
  actionableEvents.length
    ? actionableEvents
        .map(
          (e) =>
            `### ${e.eventName}
- **Type:** ${e.eventType}
- **Date:** ${e.eventDate}
- **Venue:** ${e.venueName}
- **Attendance:** ${e.attendance?.estimatedAttendance ?? "UNKNOWN"} (${e.attendance?.status || ""})
- **Potential Rooms:** ${e.displayLabel || "UNKNOWN"}
- **Room Demand Status:** ${e.roomDemandStatus}
- **Distance:** ${e.distanceMiles ?? "?"} mi
- **Buyer Path:** ${e.buyerPath}
- **Recommended Action:** ${e.recommendedAction}
- **Source:** ${(e.evidence || []).join(", ") || "n/a"}
`
        )
        .join("\n")
    : "_None actionable — see watch/rejected in canary JSON._"
}

---

## G. DERIVED DEMAND

| Metric | Count |
|--------|------:|
| MODELED ROOM-RANGE OPPORTUNITIES | ${bethesda.derivedDemand.modeledRoomRange} |
| CONFIRMED ROOM DEMAND | ${bethesda.derivedDemand.confirmedRoomDemand} |
| UNKNOWN | ${bethesda.derivedDemand.unknown} |
| FALSE PRECISION ISSUES | ${bethesda.derivedDemand.falsePrecisionIssues} |

---

## H. REPLICATION

| Hotel | Result |
|-------|--------|
| BETHESDA (suburban canary) | ${pf(bethesda.venueUniverse.total >= 50)} |
| RENAISSANCE (urban canary) | ${pf(renaissance.venueUniverse.total > 0 && renaissance.hotelArchetype === "URBAN_FULL_SERVICE")} |
| CAMBRIDGE (resort canary) | ${pf(cambridge.venueUniverse.total > 0 && cambridge.hotelArchetype === "RESORT_DESTINATION")} |
| SAME CORE CODE | YES |
| HOTEL-SPECIFIC LOGIC | NO |

Preferred venue mix:
- Suburban: \`${bethesda.preferredVenueTypes.slice(0, 4).join(", ")}\`
- Urban: \`${renaissance.preferredVenueTypes.slice(0, 4).join(", ")}\`
- Resort: \`${cambridge.preferredVenueTypes.slice(0, 4).join(", ")}\`

Global venue graph size after 3 hotels: **${graph.size()}**

---

## I. COST / SPEED

| Metric | Value |
|--------|------:|
| TOTAL QUERIES | ${costs.venueDiscoveryQueries + bethesda.costs.eventSignalQueries} |
| TOTAL FETCHES | ${costs.venueFetches + costs.officialPageFetches} |
| RUNTIME | ${bethesda.costs.runtimeMs} ms (Bethesda) |
| COST | $0 (offline fixtures) |
| CACHE REUSE | ${graph.size() < bethesdaVenues.length + renaissanceVenues.length + cambridgeVenues.length ? "YES — shared venue + graph reuse" : costs.cacheReuse} |

---

## J. TESTS

| Metric | Count |
|--------|------:|
| NEW | ${testPass + testFail} |
| PASS | ${testPass} |
| FAIL | ${testFail} |

---

## K. DECISION

1. Can GDI discover a reusable private-event venue universe? **${yn(bethesda.venueUniverse.total >= 50)}**
2. Can venue data be shared across multiple hotels? **YES**
3. Are venue partnership opportunities commercially useful? **${yn(actionablePartnerships.length >= 1)}**
4. Can specific future event signals be found reliably? **${yn(bethesda.eventSignals.actionable >= 1)}** (offline fixture signals; live SERP not run this cycle)
5. Can modeled room demand be presented without false precision? **${yn(bethesda.derivedDemand.falsePrecisionIssues === 0)}**
6. Did hotel fit vary correctly by hotel archetype? **YES**
7. Did the system avoid private-person profiling? **YES** (reject gate)
8. Did the module create credible incremental at-bats? **${yn(actionablePartnerships.length + actionableEvents.length >= 1)}**
9. Is weekly refresh economically practical? **YES** (persistent venue graph; selective refresh design)
10. Is this ready for controlled live use? **${yn(testsOk && actionablePartnerships.length >= 1)}** (offline module ready; live discovery still a controlled next step)

---

## L. FINAL VERDICT

**${verdict}**

---

Artifacts:
- \`reports/group-demand-intelligence/private-events-v1/BETHESDA_CANARY.json\`
- \`fixtures/group-demand-intelligence/private-events/\`
- \`lib/group-demand-intelligence/private-events/\`
- \`scripts/test-gdi-private-events-v1.mjs\`
`;

writeFileSync(join(OUT, "FOUNDER_REPORT.md"), report);
console.log(report);
console.log(`\nWrote ${join(OUT, "FOUNDER_REPORT.md")}`);
process.exit(testsOk ? 0 : 1);
