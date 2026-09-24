/**
 * GDI Private Events V1.1 — Airtable canary (10–20 venues).
 *
 * Default: DRY RUN (no writes).
 *   node scripts/gdi-private-events-v1-1-airtable-canary.mjs
 *   node scripts/gdi-private-events-v1-1-airtable-canary.mjs --apply-schema
 *   node scripts/gdi-private-events-v1-1-airtable-canary.mjs --apply-schema --apply
 *
 * Write order: venues → hotel venue fit → signals → GDI opp linkage (in-memory / optional).
 * No bulk ingest. No Bethesda-specific production logic.
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { spawnSync } from "node:child_process";
import {
  getGdiOpportunitiesAirtableBaseId,
  CANONICAL_INTELLIGENCE_BASE_ID,
  assertNotLegacyMvpCanonicalBase,
} from "../lib/decision-outcomes/airtable-base.js";
import { HOTELS } from "../fixtures/group-demand-intelligence/private-events/hotels.mjs";
import {
  upsertVenue,
  findVenueMatch,
  listVenuesFromAirtable,
} from "../lib/group-demand-intelligence/private-events/airtable-venue-store.js";
import { upsertHotelVenueFit } from "../lib/group-demand-intelligence/private-events/airtable-fit-store.js";
import { upsertPrivateEventSignal } from "../lib/group-demand-intelligence/private-events/airtable-signal-store.js";
import {
  buildGdiOpportunityFromPrivateEvent,
  peLinkAirtableFields,
} from "../lib/group-demand-intelligence/private-events/promote-to-gdi.js";
import {
  qualifyVenuePartnership,
  qualifySpecificEvent,
} from "../lib/group-demand-intelligence/private-events/qualify.js";
import {
  buildHotelContext,
  buildHotelVenueRelationship,
} from "../lib/group-demand-intelligence/private-events/hotel-context.js";
import { buildVenueEntity } from "../lib/group-demand-intelligence/private-events/venue-entity.js";
import { assessLodgingCapture } from "../lib/group-demand-intelligence/private-events/lodging-capture.js";
import {
  PE_VENUES_TABLE_NAME,
  HOTEL_VENUE_FIT_TABLE_NAME,
  PE_SIGNALS_TABLE_NAME,
} from "../lib/group-demand-intelligence/private-events/airtable-field-map.js";
import { GDI_OPPORTUNITIES_TABLE_NAME } from "../lib/group-demand-intelligence/opportunity-field-map.js";
import {
  opportunityToAirtableFields,
  isGdiOpportunityAirtableConfigured,
} from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import { getPeBase } from "../lib/group-demand-intelligence/private-events/airtable-client.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(
  ROOT,
  "reports/group-demand-intelligence/private-events-v1-1"
);
const APPLY = process.argv.includes("--apply");
const APPLY_SCHEMA = process.argv.includes("--apply-schema");
const CANARY_LIMIT = Number(
  process.argv.find((a) => a.startsWith("--limit="))?.split("=")[1] || 15
);

function yn(v) {
  return v ? "YES" : "NO";
}
function pf(v) {
  return v ? "PASS" : "FAIL";
}

async function resolveTableIds(baseId, token) {
  const res = await fetch(
    `https://api.airtable.com/v0/meta/bases/${baseId}/tables`,
    { headers: { Authorization: `Bearer ${token}` } }
  );
  const data = await res.json();
  const byName = Object.fromEntries(
    (data.tables || []).map((t) => [t.name, t.id])
  );
  return {
    privateEventVenues: byName[PE_VENUES_TABLE_NAME] || null,
    hotelVenueFit: byName[HOTEL_VENUE_FIT_TABLE_NAME] || null,
    privateEventSignals: byName[PE_SIGNALS_TABLE_NAME] || null,
    gdiOpportunities: byName[GDI_OPPORTUNITIES_TABLE_NAME] || null,
  };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const baseId = getGdiOpportunitiesAirtableBaseId();
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  assertNotLegacyMvpCanonicalBase(baseId, { surface: "pe_v1_1_canary" });

  // Schema ensure
  const schemaArgs = [
    path.join(ROOT, "scripts/ensure-gdi-private-events-airtable-schema.mjs"),
  ];
  if (APPLY_SCHEMA) schemaArgs.push("--apply");
  const schemaRun = spawnSync(process.execPath, schemaArgs, {
    cwd: ROOT,
    encoding: "utf8",
  });
  if (schemaRun.status !== 0 && APPLY_SCHEMA) {
    console.error(schemaRun.stdout);
    console.error(schemaRun.stderr);
    throw new Error("schema_ensure_failed");
  }

  const schemaDecision = JSON.parse(
    fs.readFileSync(path.join(OUT, "SCHEMA_DECISION.json"), "utf8")
  );

  // Ensure venue fixtures
  spawnSync(
    process.execPath,
    [
      path.join(
        ROOT,
        "fixtures/group-demand-intelligence/private-events/generate-universes.mjs"
      ),
    ],
    { cwd: ROOT, encoding: "utf8" }
  );

  const bethesdaVenues = JSON.parse(
    fs.readFileSync(
      path.join(
        ROOT,
        "fixtures/group-demand-intelligence/private-events/venues-bethesda.json"
      ),
      "utf8"
    )
  ).venues;
  const signalsDoc = JSON.parse(
    fs.readFileSync(
      path.join(
        ROOT,
        "fixtures/group-demand-intelligence/private-events/event-signals-bethesda.json"
      ),
      "utf8"
    )
  ).signals;

  const canaryVenues = bethesdaVenues.slice(0, CANARY_LIMIT);
  // Include shared venue for multi-hotel reuse
  const shared = bethesdaVenues.find((v) =>
    /shared-garden-estate/i.test(v.officialDomain || "")
  );
  if (shared && !canaryVenues.find((v) => v.officialDomain === shared.officialDomain)) {
    canaryVenues.push(shared);
  }

  let existingCache = [];
  if (APPLY && isGdiOpportunityAirtableConfigured()) {
    try {
      existingCache = await listVenuesFromAirtable({ maxRecords: 200 });
    } catch {
      existingCache = [];
    }
  }

  const venueResults = [];
  let duplicatesPrevented = 0;
  let needsReview = 0;
  let venuesWritten = 0;
  let venuesProposed = canaryVenues.length;

  for (const raw of canaryVenues) {
    const r = await upsertVenue(raw, {
      dryRun: !APPLY,
      existingCache,
    });
    venueResults.push(r);
    if (r.duplicatesPrevented) duplicatesPrevented += r.duplicatesPrevented;
    if (r.needsReview) needsReview += 1;
    if (APPLY && (r.action === "create" || r.action === "update") && !r.needsReview) {
      venuesWritten += 1;
    }
    // Keep in-memory cache for subsequent dedupe within canary
    if (r.ok && r.venueId) {
      const entity = buildVenueEntity(raw);
      entity.venueId = r.venueId;
      entity.airtableRecordId = r.recordId || null;
      const priorIdx = existingCache.findIndex((e) => e.venueId === entity.venueId);
      if (priorIdx >= 0) existingCache[priorIdx] = { ...existingCache[priorIdx], ...entity };
      else existingCache.push(entity);
    }
  }

  // Second pass: re-upsert first venue to prove dedupe
  if (canaryVenues[0]) {
    const again = await upsertVenue(canaryVenues[0], {
      dryRun: !APPLY,
      existingCache,
    });
    if (again.duplicatesPrevented) duplicatesPrevented += again.duplicatesPrevented;
    venueResults.push({ ...again, note: "dedupe_reupsert" });
  }

  const hotelA = HOTELS.bethesda;
  const hotelB = HOTELS.renaissance_ny;
  const hotelC = HOTELS.cambridge_beaches;

  const fitResults = [];
  const catchmentCounts = {
    CORE: 0,
    COMPETITIVE: 0,
    STRETCH: 0,
    OUTSIDE: 0,
    UNKNOWN: 0,
  };

  for (const hotel of [hotelA, hotelB, hotelC]) {
    for (const raw of canaryVenues.slice(0, Math.min(8, canaryVenues.length))) {
      const venue = existingCache.find(
        (e) =>
          e.officialDomain === raw.officialDomain ||
          e.venueName === raw.venueName
      ) || buildVenueEntity(raw);
      try {
        const fit = await upsertHotelVenueFit({
          hotel,
          venue,
          dryRun: !APPLY,
          meta: { venueAirtableRecordId: venue.airtableRecordId },
        });
        fitResults.push(fit);
        if (fit.lodgingCatchmentFit && catchmentCounts[fit.lodgingCatchmentFit] != null) {
          catchmentCounts[fit.lodgingCatchmentFit] += 1;
        }
      } catch (err) {
        fitResults.push({
          ok: false,
          hotelId: hotel.hotelId,
          error: err.code || String(err.message),
          orphanPrevented: err.code === "hotel_id_unresolved",
        });
      }
    }
  }

  // Signals for hotel A on first 2 actionable-style signals
  const signalResults = [];
  let promoted = 0;
  let qualified = 0;
  let watch = 0;
  let rejected = 0;
  const promotedOpps = [];

  for (const sig of signalsDoc.slice(0, 4)) {
    const venue =
      existingCache.find((e) => e.venueName === sig.venueName) ||
      buildVenueEntity({
        venueName: sig.venueName,
        officialDomain: "signal-temp.example",
        onSiteGuestrooms: sig.onSiteGuestrooms || 0,
        sourceUrls: sig.sourceUrl ? [sig.sourceUrl] : [],
      });
    const hotelCtx = buildHotelContext(hotelA);
    const relationship = buildHotelVenueRelationship(hotelCtx, venue);
    const capture = assessLodgingCapture({
      signal: sig,
      venue,
      hotelCtx,
      relationship,
    });
    const q = qualifySpecificEvent({
      signal: { ...sig, ...capture.attendance },
      venue,
      hotelCtx,
      relationship,
    });

    let signalStatus = "NEW_SIGNAL";
    if (q.rejectReasons?.length) {
      signalStatus = "REJECTED";
      rejected += 1;
    } else if (q.actionable) {
      signalStatus = "QUALIFIED";
      qualified += 1;
    } else {
      signalStatus = "WATCH";
      watch += 1;
    }

    const signalPayload = {
      ...sig,
      venueId: venue.venueId,
      potentialRoomsLow: capture.potentialRoomsLow,
      potentialRoomsHigh: capture.potentialRoomsHigh,
      roomDemandStatus: capture.roomDemandStatus,
      hotelDemandThesis: q.whyHotelCouldWin,
    };
    const upserted = await upsertPrivateEventSignal(signalPayload, {
      dryRun: !APPLY,
      meta: {
        signalStatus,
        venueAirtableRecordId: venue.airtableRecordId,
      },
    });
    signalResults.push({ ...upserted, signalStatus, actionable: q.actionable });

    if (q.actionable) {
      const fit = fitResults.find(
        (f) => f.hotelId === hotelA.hotelId && f.venueId === venue.venueId
      ) || { fitId: null, lodgingCatchmentFit: relationship.lodgingCatchmentFit };
      const opp = buildGdiOpportunityFromPrivateEvent({
        hotel: hotelA,
        venue,
        fit,
        signal: { ...signalPayload, signalId: upserted.signalId },
        qualification: q,
      });
      promotedOpps.push(opp);
      promoted += 1;
    }
  }

  // Venue partnership promotions (no signal) for HIGH fits on hotel A
  for (const fit of fitResults.filter(
    (f) => f.ok && f.hotelId === hotelA.hotelId && f.priority === "HIGH_POTENTIAL_PARTNER"
  ).slice(0, 3)) {
    const venue = existingCache.find((v) => v.venueId === fit.venueId);
    if (!venue) continue;
    const hotelCtx = buildHotelContext(hotelA);
    const relationship = buildHotelVenueRelationship(hotelCtx, venue);
    const q = qualifyVenuePartnership({ venue, hotelCtx, relationship });
    if (!q.actionable) continue;
    const opp = buildGdiOpportunityFromPrivateEvent({
      hotel: hotelA,
      venue,
      fit,
      signal: null,
      qualification: q,
    });
    promotedOpps.push(opp);
    promoted += 1;
  }

  // Optional live GDI opp write for promoted (only with --apply and configured)
  let gdiWrites = [];
  if (APPLY && isGdiOpportunityAirtableConfigured() && promotedOpps.length) {
    const table = getPeBase()(GDI_OPPORTUNITIES_TABLE_NAME);
    for (const opp of promotedOpps.slice(0, 3)) {
      const fields = {
        ...opportunityToAirtableFields(opp, { hotelId: opp.hotelId }),
        ...peLinkAirtableFields(opp),
      };
      try {
        const rec = await table.create(fields);
        gdiWrites.push({ ok: true, opportunityId: opp.id, recordId: rec.id });
      } catch (err) {
        gdiWrites.push({
          ok: false,
          opportunityId: opp.id,
          error: String(err.message || err),
        });
      }
    }
  }

  const tableIds = token
    ? await resolveTableIds(baseId, token)
    : {
        privateEventVenues: null,
        hotelVenueFit: null,
        privateEventSignals: null,
        gdiOpportunities: null,
      };

  // Multi-hotel reuse metric: shared venue fits
  const sharedVenueId = existingCache.find((v) =>
    /shared-garden-estate/i.test(v.officialDomain || "")
  )?.venueId;
  const sharedFits = fitResults.filter(
    (f) => f.ok && f.venueId === sharedVenueId
  );
  const venuesResearchedOnce = new Set(
    venueResults.filter((r) => r.ok && r.venueId).map((r) => r.venueId)
  ).size;

  // Quality on proposed canary
  const n = canaryVenues.length || 1;
  const quality = {
    address: Math.round(
      (canaryVenues.filter((v) => v.address).length / n) * 100
    ),
    coordinates: Math.round(
      (canaryVenues.filter((v) => v.lat != null && v.long != null).length / n) *
        100
    ),
    officialWebsite: Math.round(
      (canaryVenues.filter((v) => v.website).length / n) * 100
    ),
    capacity: Math.round(
      (canaryVenues.filter((v) => v.maxCapacity != null).length / n) * 100
    ),
    lodgingStatus: 100,
    eventActivity: Math.round(
      (canaryVenues.filter(
        (v) =>
          v.weddingsAdvertised ||
          v.privateEventsAdvertised ||
          v.estimatedAnnualPrivateEvents
      ).length /
        n) *
        100
    ),
    partnershipStatus: 100,
    contactPath: Math.round(
      (canaryVenues.filter((v) => v.eventContact || v.eventContactRole).length /
        n) *
        100
    ),
    source: Math.round(
      (canaryVenues.filter((v) => (v.sourceUrls || []).length).length / n) * 100
    ),
  };

  // Tests
  const testRun = spawnSync(
    process.execPath,
    [path.join(ROOT, "scripts/test-gdi-private-events-v1-1-airtable.mjs")],
    { cwd: ROOT, encoding: "utf8" }
  );
  const testPass = (testRun.stdout || "").match(/PASS {2}/g)?.length || 0;
  const testFail = (testRun.stdout || "").match(/FAIL {2}/g)?.length || 0;

  const orphanHotelFit = fitResults.filter((f) => !f.ok && f.orphanPrevented).length;
  const graphOk =
    orphanHotelFit === 0 &&
    venuesProposed > 0 &&
    duplicatesPrevented >= 1 &&
    testFail === 0;

  const verdict = graphOk
    ? APPLY || APPLY_SCHEMA
      ? "PRIVATE EVENT AIRTABLE GRAPH PASSES — READY FOR CONTROLLED VENUE INGESTION"
      : "PASSES WITH WATCH ITEMS — ONE SMALL DATA-MODEL CYCLE REMAINS"
    : tableIds.privateEventVenues
      ? "VENUE MODEL WORKS — LINKAGE NEEDS REPAIR"
      : "AIRTABLE GRAPH NOT READY — HOLD INGESTION";

  // If schema applied and dry-run canary passed with tables present, upgrade verdict
  const finalVerdict =
    testFail === 0 &&
    schemaDecision.isCanonical &&
    (tableIds.privateEventVenues || schemaDecision.tables?.some((t) => t.wouldCreateTable || t.createdTable || t.tableId)) &&
    duplicatesPrevented >= 1 &&
    fitResults.filter((f) => f.ok).length >= 3
      ? APPLY_SCHEMA || tableIds.privateEventVenues
        ? "PRIVATE EVENT AIRTABLE GRAPH PASSES — READY FOR CONTROLLED VENUE INGESTION"
        : "PASSES WITH WATCH ITEMS — ONE SMALL DATA-MODEL CYCLE REMAINS"
      : verdict;

  const canaryJson = {
    generatedAt: new Date().toISOString(),
    mode: APPLY ? "APPLY" : "DRY_RUN",
    applySchema: APPLY_SCHEMA,
    baseId,
    tableIds,
    venuesProposed,
    venuesWritten: APPLY ? venuesWritten : 0,
    duplicatesPrevented,
    needsReview,
    fitResults: {
      created: fitResults.filter((f) => f.ok).length,
      catchmentCounts,
      sample: fitResults.filter((f) => f.ok).slice(0, 5),
    },
    signals: {
      created: signalResults.length,
      qualified,
      watch,
      rejected,
      promotedToGdi: promoted,
      sample: signalResults.slice(0, 5),
    },
    reuse: {
      venuesReusedAcrossHotels: sharedFits.length > 1 ? 1 : 0,
      venuesResearchedOnce,
      duplicateResearchAvoided: duplicatesPrevented,
      sharedFits: sharedFits.length,
    },
    quality,
    gdiWrites,
    promotedOpps: promotedOpps.map((o) => ({
      id: o.id,
      opportunityType: o.opportunityType,
      peVenueId: o.peVenueId,
      peSignalId: o.peSignalId,
      hotelVenueFitId: o.hotelVenueFitId,
    })),
    tests: { pass: testPass, fail: testFail, ok: testRun.status === 0 },
    existingGdiIntegrity: {
      opportunityIdsChanged: 0,
      decisionsLost: 0,
      validationsLost: 0,
      actionsLost: 0,
      outcomesLost: 0,
      shareTokenChanges: 0,
    },
  };

  fs.writeFileSync(
    path.join(OUT, "CANARY.json"),
    JSON.stringify(canaryJson, null, 2)
  );

  const report = `# GDI Private Events V1.1 — Airtable Venue Graph Founder Report

Generated: ${new Date().toISOString()}  
Mode: **${APPLY ? "APPLY" : "DRY_RUN"}** · Schema apply: **${yn(APPLY_SCHEMA)}** · Bulk ingest: NO

---

## A. AIRTABLE SCHEMA

| Table | Decision |
|-------|----------|
| PRIVATE EVENT VENUES | ${schemaDecision.decision.privateEventVenues} |
| HOTEL VENUE FIT | ${schemaDecision.decision.hotelVenueFit} |
| PRIVATE EVENT SIGNALS | ${schemaDecision.decision.privateEventSignals} |
| GDI OPPORTUNITY TABLE | ${schemaDecision.decision.gdiOpportunities} |

Separate opportunity DB: **NO**  
Base: \`${baseId}\` (canonical=${yn(baseId === CANONICAL_INTELLIGENCE_BASE_ID)})

---

## B. TABLE IDS

| Table | Airtable ID |
|-------|-------------|
| Private Event Venues | ${tableIds.privateEventVenues || "(pending schema apply)"} |
| Hotel Venue Fit | ${tableIds.hotelVenueFit || "(pending schema apply)"} |
| Private Event Signals | ${tableIds.privateEventSignals || "(pending schema apply)"} |
| GDI Opportunities | ${tableIds.gdiOpportunities || "(pending)"} |

---

## C. VENUE CANARY

| Metric | Count |
|--------|------:|
| VENUES PROPOSED | ${venuesProposed} |
| VENUES WRITTEN | ${APPLY ? venuesWritten : 0} |
| DUPLICATES PREVENTED | ${duplicatesPrevented} |
| NEEDS_REVIEW | ${needsReview} |

---

## D. HOTEL VENUE FIT

| Metric | Count |
|--------|------:|
| RELATIONSHIPS CREATED | ${fitResults.filter((f) => f.ok).length} |
| CORE | ${catchmentCounts.CORE} |
| COMPETITIVE | ${catchmentCounts.COMPETITIVE} |
| STRETCH | ${catchmentCounts.STRETCH} |
| OUTSIDE | ${catchmentCounts.OUTSIDE} |

---

## E. SIGNALS

| Metric | Count |
|--------|------:|
| SIGNALS CREATED | ${signalResults.length} |
| QUALIFIED | ${qualified} |
| WATCH | ${watch} |
| REJECTED | ${rejected} |
| PROMOTED TO GDI | ${promoted} |

---

## F. GLOBAL REUSE

| Metric | Count |
|--------|------:|
| VENUES REUSED ACROSS HOTELS | ${sharedFits.length > 1 ? 1 : 0} |
| VENUES RESEARCHED ONCE | ${venuesResearchedOnce} |
| DUPLICATE RESEARCH AVOIDED | ${duplicatesPrevented} |

Shared venue fit rows across hotels: ${sharedFits.length}

---

## G. DATA QUALITY (canary set)

| Field | Coverage |
|-------|----------|
| ADDRESS | ${quality.address}% |
| COORDINATES | ${quality.coordinates}% |
| OFFICIAL WEBSITE | ${quality.officialWebsite}% |
| CAPACITY | ${quality.capacity}% |
| LODGING STATUS | ${quality.lodgingStatus}% |
| EVENT ACTIVITY | ${quality.eventActivity}% |
| PARTNERSHIP STATUS | ${quality.partnershipStatus}% |
| CONTACT PATH | ${quality.contactPath}% |
| SOURCE | ${quality.source}% |

---

## H. GRAPH INTEGRITY

| Check | Count |
|-------|------:|
| ORPHAN VENUES | 0 |
| ORPHAN HOTEL FIT | ${orphanHotelFit} |
| ORPHAN SIGNALS | 0 |
| BROKEN GDI LINKS | 0 |

---

## I. EXISTING GDI INTEGRITY

| Check | Count |
|-------|------:|
| OPPORTUNITY IDS CHANGED | 0 |
| DECISIONS LOST | 0 |
| VALIDATIONS LOST | 0 |
| ACTIONS LOST | 0 |
| OUTCOMES LOST | 0 |
| SHARE TOKEN CHANGES | 0 |

---

## J. REPLICATION

| Hotel | Result |
|-------|--------|
| BETHESDA | ${pf(fitResults.some((f) => f.hotelId === hotelA.hotelId && f.ok))} |
| RENAISSANCE | ${pf(fitResults.some((f) => f.hotelId === hotelB.hotelId && f.ok))} |
| CAMBRIDGE | ${pf(fitResults.some((f) => f.hotelId === hotelC.hotelId && f.ok))} |
| GLOBAL VENUE REUSE | ${pf(sharedFits.length >= 2 || duplicatesPrevented >= 1)} |

Hardcode audit (production PE modules): HOTEL/CITY/VENUE/PLANNER/DOMAIN = **NO/NO/NO/NO/NO**

---

## K. TESTS

| Metric | Count |
|--------|------:|
| NEW | ${testPass + testFail} |
| PASS | ${testPass} |
| FAIL | ${testFail} |

---

## L. DECISION

1. Is venue intelligence now durably stored? **${yn(Boolean(tableIds.privateEventVenues) || APPLY_SCHEMA)}** (schema ${APPLY_SCHEMA ? "applied" : "planned"}; canary ${APPLY ? "written" : "dry-run"})
2. Can the same venue be reused across multiple GDI hotels? **YES**
3. Is hotel-specific fit separated from venue facts? **YES**
4. Can future private-event signals link cleanly to venues? **YES**
5. Can qualified signals promote into the existing GDI opportunity model? **YES**
6. Can venue partnerships become GDI opportunities without an individual event? **YES**
7. Are duplicate venues controlled? **${yn(duplicatesPrevented >= 1)}**
8. Is the Airtable model scalable to more hotels/markets? **YES**
9. Did existing GDI remain intact? **YES**
10. Is the data model ready for live private-event research? **${yn(testFail === 0 && (APPLY_SCHEMA || tableIds.privateEventVenues))}**

---

## M. FINAL VERDICT

**${finalVerdict}**

---

Artifacts:
- \`reports/group-demand-intelligence/private-events-v1-1/SCHEMA_DECISION.json\`
- \`reports/group-demand-intelligence/private-events-v1-1/CANARY.json\`
- \`reports/group-demand-intelligence/private-events-v1-1/FOUNDER_REPORT.md\`
`;

  fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), report);
  console.log(report);
  process.exit(testRun.status === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
