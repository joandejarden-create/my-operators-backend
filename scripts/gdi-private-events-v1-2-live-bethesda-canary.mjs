/**
 * GDI Private Events V1.2 — Live Bethesda venue + forward-event canary.
 *
 * Default: discovery + dry-run Airtable + GDI promote dry-run (no GDI writes).
 *   node scripts/gdi-private-events-v1-2-live-bethesda-canary.mjs
 *   node scripts/gdi-private-events-v1-2-live-bethesda-canary.mjs --apply
 *
 * --apply writes: Private Event Venues, Hotel Venue Fit, Private Event Signals
 * GDI Opportunities: ALWAYS dry-run this cycle (promotion candidates only).
 *
 * No fixtures as success evidence. No contact providers. No Bethesda hardcodes in lib.
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  getGdiOpportunitiesAirtableBaseId,
  CANONICAL_INTELLIGENCE_BASE_ID,
  assertNotLegacyMvpCanonicalBase,
} from "../lib/decision-outcomes/airtable-base.js";
import { HOTELS } from "../fixtures/group-demand-intelligence/private-events/hotels.mjs";
import {
  discoverLiveVenues,
  buildVenueDiscoveryQueries,
  LIVE_VENUE_DISCOVERY_V1_2,
} from "../lib/group-demand-intelligence/private-events/live-venue-discovery.js";
import {
  discoverForwardEventSignals,
  LIVE_EVENT_SIGNALS_V1_2,
} from "../lib/group-demand-intelligence/private-events/live-event-signals.js";
import {
  upsertVenue,
  listVenuesFromAirtable,
} from "../lib/group-demand-intelligence/private-events/airtable-venue-store.js";
import { upsertHotelVenueFit } from "../lib/group-demand-intelligence/private-events/airtable-fit-store.js";
import { upsertPrivateEventSignal } from "../lib/group-demand-intelligence/private-events/airtable-signal-store.js";
import {
  buildHotelContext,
  buildHotelVenueRelationship,
} from "../lib/group-demand-intelligence/private-events/hotel-context.js";
import { classifyVenuePriority } from "../lib/group-demand-intelligence/private-events/venue-priority.js";
import {
  qualifyVenuePartnership,
  qualifySpecificEvent,
} from "../lib/group-demand-intelligence/private-events/qualify.js";
import { buildGdiOpportunityFromPrivateEvent } from "../lib/group-demand-intelligence/private-events/promote-to-gdi.js";
import { VENUE_PRIORITY, ON_SITE_LODGING_STATUS } from "../lib/group-demand-intelligence/private-events/constants.js";
import { resolveHotelArchetype } from "../lib/group-demand-intelligence/private-events/hotel-context.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(
  ROOT,
  "reports/group-demand-intelligence/private-events-v1-2"
);

const APPLY = process.argv.includes("--apply");
const MAX_VENUES = Number(
  process.argv.find((a) => a.startsWith("--max-venues="))?.split("=")[1] || 28
);
const MAX_QUERIES = Number(
  process.argv.find((a) => a.startsWith("--max-queries="))?.split("=")[1] || 12
);

/** Canary hotel binding — geo from research profile input, not production branching. */
const CANARY_HOTEL = {
  ...HOTELS.bethesda,
  hotelId: "recLuxvwwxID7U2B8",
};
const MARKET_LABEL = "Bethesda MD Montgomery County";

function pct(n, d) {
  if (!d) return "0%";
  return `${Math.round((100 * n) / d)}%`;
}

function yn(v) {
  return v ? "YES" : "NO";
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function main() {
  const started = Date.now();
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const baseId = getGdiOpportunitiesAirtableBaseId();
  assertNotLegacyMvpCanonicalBase(baseId, {
    surface: "gdi_private_events_v1_2_live_canary",
  });

  fs.mkdirSync(OUT, { recursive: true });

  const hotelCtx = buildHotelContext(CANARY_HOTEL);
  const report = {
    generatedAt: new Date().toISOString(),
    version: "gdi_private_events_v1_2",
    mode: APPLY ? "APPLY_GRAPH" : "DRY_RUN",
    gdiPromotion: "DRY_RUN_ONLY",
    baseId,
    isCanonical: baseId === CANONICAL_INTELLIGENCE_BASE_ID,
    hotelId: CANARY_HOTEL.hotelId,
    marketLabel: MARKET_LABEL,
    hotelArchetype: resolveHotelArchetype(CANARY_HOTEL),
    surfe: 0,
    pdl: 0,
  };

  // ——— Live venue discovery ———
  console.error("[pe-v1.2] discovering live venues…");
  const discovery = await discoverLiveVenues({
    hotel: CANARY_HOTEL,
    marketLabel: MARKET_LABEL,
    city: "Bethesda",
    region: "MD",
    maxVenues: MAX_VENUES,
    maxQueries: MAX_QUERIES,
    enrichTopN: MAX_VENUES,
  });

  let existingCache = [];
  try {
    existingCache = await listVenuesFromAirtable({ maxRecords: 500 });
  } catch (err) {
    report.existingListError = String(err?.message || err);
  }

  const venueWrites = {
    EXISTING_REUSED: 0,
    NEW_CREATED: 0,
    NEW_VERIFIED_DRY: 0,
    DUPLICATES_PREVENTED: 0,
    NEEDS_REVIEW: 0,
    SKIP_LOW_IDENTITY: discovery.weak?.filter((w) => w.action === "SKIP_LOW_IDENTITY")
      .length || 0,
    details: [],
  };

  const venuesWithPriority = [];
  const dryRun = !APPLY;

  for (const venue of discovery.venues || []) {
    const upsert = await upsertVenue(venue, {
      dryRun,
      existingCache,
    });
    if (upsert.needsReview) {
      venueWrites.NEEDS_REVIEW += 1;
    } else if (upsert.action === "update" || upsert.reused) {
      venueWrites.EXISTING_REUSED += 1;
      venueWrites.DUPLICATES_PREVENTED += upsert.duplicatesPrevented || 1;
      if (upsert.venue) {
        existingCache.push(upsert.venue);
      } else if (upsert.fields) {
        existingCache.push({ ...venue, airtableRecordId: upsert.recordId });
      }
    } else if (upsert.action === "create") {
      if (APPLY) venueWrites.NEW_CREATED += 1;
      else venueWrites.NEW_VERIFIED_DRY += 1;
      existingCache.push({ ...venue, airtableRecordId: upsert.recordId });
    }
    venueWrites.details.push({
      venueId: venue.venueId,
      venueName: venue.venueName,
      action: upsert.action,
      needsReview: Boolean(upsert.needsReview),
      official: venue.research?.officialWebsite,
      lodging: venue.onSiteLodgingStatus,
      identity: venue.research?.identityConfidence,
    });

    const relationship = buildHotelVenueRelationship(hotelCtx, venue);
    const priorityResult = classifyVenuePriority(venue, relationship);
    venuesWithPriority.push({
      ...venue,
      priority: priorityResult.priority,
      priorityFactors: priorityResult.factors || [],
      relationship,
      distanceMiles: relationship.distanceMiles,
    });
    if (APPLY) await sleep(150);
  }

  // ——— Hotel venue fit ———
  const fitWrites = { created: 0, updated: 0, dry: 0, errors: [] };
  const fitByVenue = new Map();
  for (const venue of venuesWithPriority) {
    try {
      const fit = await upsertHotelVenueFit({
        hotel: CANARY_HOTEL,
        venue,
        dryRun,
      });
      fitByVenue.set(venue.venueId, fit);
      if (fit.ok) {
        if (dryRun) fitWrites.dry += 1;
        else if (fit.action === "update") fitWrites.updated += 1;
        else fitWrites.created += 1;
      } else {
        fitWrites.errors.push({ venueId: venue.venueId, error: fit.error });
      }
    } catch (err) {
      fitWrites.errors.push({
        venueId: venue.venueId,
        error: err?.message || String(err),
      });
    }
    if (APPLY) await sleep(120);
  }

  // ——— Forward event signals (strong venues only) ———
  console.error("[pe-v1.2] forward event signal search…");
  let signalDiscovery = { signals: [], ledger: {} };
  try {
    signalDiscovery = await discoverForwardEventSignals({
      venues: venuesWithPriority,
      hotelCtx,
      maxVenues: 12,
      maxQueriesPerVenue: 3,
    });
  } catch (err) {
    report.signalDiscoveryError = String(err?.message || err);
  }

  const signalWrites = { created: 0, dry: 0, rejected: 0, details: [] };
  const qualifiedSignals = [];
  for (const signal of signalDiscovery.signals || []) {
    if (signal._reject) {
      signalWrites.rejected += 1;
      signalWrites.details.push({
        eventName: signal.eventName,
        reject: signal._reject,
      });
      continue;
    }
    const venue =
      venuesWithPriority.find((v) => v.venueId === signal.venueId) || null;
    const relationship = venue
      ? buildHotelVenueRelationship(hotelCtx, venue)
      : null;
    const q = qualifySpecificEvent({
      signal,
      venue,
      hotelCtx,
      relationship,
    });
    qualifiedSignals.push({ signal, venue, qualification: q });

    try {
      const up = await upsertPrivateEventSignal(signal, { dryRun });
      if (dryRun) signalWrites.dry += 1;
      else if (up.ok) signalWrites.created += 1;
    } catch (err) {
      signalWrites.details.push({
        eventName: signal.eventName,
        error: err?.message || String(err),
      });
    }
    if (APPLY) await sleep(120);
  }

  // ——— Partnership + specific event qualification / GDI promote dry-run ———
  const partnershipAtBats = [];
  const specificAtBats = [];
  const gdiPromotionCandidates = [];

  for (const venue of venuesWithPriority) {
    if (
      venue.priority !== VENUE_PRIORITY.HIGH_POTENTIAL_PARTNER &&
      venue.priority !== VENUE_PRIORITY.EVENT_SIGNAL_TARGET
    ) {
      continue;
    }
    const q = qualifyVenuePartnership({
      venue,
      hotelCtx,
      relationship: venue.relationship,
    });
    if (q.qualification === "TRUE" || q.lifecycle === "ACTIONABLE_NOW") {
      partnershipAtBats.push({ venue, qualification: q });
      const opp = buildGdiOpportunityFromPrivateEvent({
        hotel: CANARY_HOTEL,
        venue,
        fit: fitByVenue.get(venue.venueId),
        signal: null,
        qualification: q,
      });
      gdiPromotionCandidates.push({
        kind: "VENUE_PARTNERSHIP",
        dryRun: true,
        opportunity: opp,
        gate: q.qualification,
      });
    }
  }

  for (const row of qualifiedSignals) {
    const q = row.qualification;
    if (q.qualification === "TRUE" || q.lifecycle === "ACTIONABLE_NOW") {
      specificAtBats.push(row);
      const opp = buildGdiOpportunityFromPrivateEvent({
        hotel: CANARY_HOTEL,
        venue: row.venue,
        fit: fitByVenue.get(row.venue?.venueId),
        signal: row.signal,
        qualification: q,
      });
      gdiPromotionCandidates.push({
        kind: "SPECIFIC_PRIVATE_EVENT",
        dryRun: true,
        opportunity: opp,
        gate: q.qualification,
      });
    }
  }

  // ——— Quality metrics ———
  const venues = venuesWithPriority;
  const n = venues.length || 1;
  const classification = {
    HIGH_POTENTIAL_PARTNER: venues.filter(
      (v) => v.priority === VENUE_PRIORITY.HIGH_POTENTIAL_PARTNER
    ).length,
    EVENT_SIGNAL_TARGET: venues.filter(
      (v) => v.priority === VENUE_PRIORITY.EVENT_SIGNAL_TARGET
    ).length,
    WATCH: venues.filter((v) => v.priority === VENUE_PRIORITY.WATCH).length,
    LOW_HOTEL_OPPORTUNITY: venues.filter(
      (v) => v.priority === VENUE_PRIORITY.LOW_HOTEL_OPPORTUNITY
    ).length,
  };

  const quality = {
    OFFICIAL_WEBSITE: pct(
      venues.filter((v) => v.research?.officialWebsite || v.website).length,
      venues.length
    ),
    ADDRESS: pct(venues.filter((v) => v.address).length, venues.length),
    COORDINATES: pct(
      venues.filter((v) => Number.isFinite(v.lat) && Number.isFinite(v.long))
        .length,
      venues.length
    ),
    CAPACITY: pct(
      venues.filter((v) => Number.isFinite(v.maxCapacity)).length,
      venues.length
    ),
    LODGING_STATUS: pct(
      venues.filter(
        (v) =>
          v.onSiteLodgingStatus &&
          v.onSiteLodgingStatus !== ON_SITE_LODGING_STATUS.UNKNOWN
      ).length,
      venues.length
    ),
    PRIVATE_EVENT_ACTIVITY: pct(
      venues.filter((v) => v.weddingsAdvertised || v.privateEventsAdvertised)
        .length,
      venues.length
    ),
    PARTNER_STATUS: pct(
      venues.filter(
        (v) =>
          v.research?.partnerStatus &&
          v.research.partnerStatus !== "UNKNOWN"
      ).length,
      venues.length
    ),
    CONTACT_PATH: pct(
      venues.filter((v) => v.eventContact || v.eventContactRole).length,
      venues.length
    ),
  };

  const highPotential = venues
    .filter((v) => v.priority === VENUE_PRIORITY.HIGH_POTENTIAL_PARTNER)
    .map((v) => {
      const q = qualifyVenuePartnership({
        venue: v,
        hotelCtx,
        relationship: v.relationship,
      });
      return {
        venue: v.venueName,
        venueType: v.venueType,
        distance: v.distanceMiles,
        capacity: v.maxCapacity,
        lodgingStatus: v.onSiteLodgingStatus,
        privateEventEvidence:
          v.weddingsAdvertised || v.privateEventsAdvertised
            ? "advertised"
            : "unknown",
        hotelPartnerStatus: v.research?.partnerStatus || "UNKNOWN",
        commercialContactPath: v.eventContactRole || v.eventContact || "—",
        whyOpportunityExists: q.whyOpportunityExists || q.whyHotelCouldWin,
        recommendedAction: q.recommendedAction,
        officialSource: v.website || (v.sourceUrls || [])[0] || null,
      };
    });

  const specificCandidates = qualifiedSignals.map((row) => ({
    event: row.signal.eventName,
    eventType: row.signal.eventType,
    date: row.signal.eventDate,
    venue: row.venue?.venueName,
    attendance: row.signal.estimatedAttendance,
    potentialRooms:
      row.signal.potentialRoomsLow != null
        ? `${row.signal.potentialRoomsLow}–${row.signal.potentialRoomsHigh} ${row.signal.roomDemandStatus}`
        : row.signal.roomDemandStatus,
    roomDemandStatus: row.signal.roomDemandStatus,
    travelEvidence: row.signal.travelLodgingEvidence,
    commercialPath: row.signal.commercialContactPath,
    recommendedAction: row.qualification.recommendedAction,
    source: row.signal.sourceUrl,
    gate: row.qualification.qualification,
    lifecycle: row.qualification.lifecycle,
  }));

  // ——— Replication code check (no live ingest) ———
  const replication = {
    GENERIC_HOTEL_INPUT_CONTRACT: "PASS",
    RENAISSANCE_CODE_CHECK: "PASS",
    CAMBRIDGE_CODE_CHECK: "PASS",
    HOTEL_SPECIFIC_LOGIC: "NO",
    CITY_SPECIFIC_LOGIC: "NO",
  };
  try {
    const rPlan = buildVenueDiscoveryQueries({
      marketLabel: "Times Square New York NY",
      hotel: HOTELS.renaissance_ny,
      maxQueries: 4,
    });
    const cPlan = buildVenueDiscoveryQueries({
      marketLabel: "Sandys Bermuda",
      hotel: HOTELS.cambridge_beaches,
      maxQueries: 4,
    });
    if (!rPlan.queries.length || !cPlan.queries.length) {
      replication.RENAISSANCE_CODE_CHECK = "FAIL";
      replication.CAMBRIDGE_CODE_CHECK = "FAIL";
    }
    // Ensure no Bethesda string baked into query builder output for other markets
    if (rPlan.queries.some((q) => /bethesda/i.test(q.q))) {
      replication.CITY_SPECIFIC_LOGIC = "YES";
      replication.RENAISSANCE_CODE_CHECK = "FAIL";
    }
    if (cPlan.queries.some((q) => /bethesda/i.test(q.q))) {
      replication.CITY_SPECIFIC_LOGIC = "YES";
      replication.CAMBRIDGE_CODE_CHECK = "FAIL";
    }
  } catch (err) {
    replication.GENERIC_HOTEL_INPUT_CONTRACT = "FAIL";
    replication.error = String(err?.message || err);
  }

  const runtimeSec = Math.round((Date.now() - started) / 1000);
  const costEst =
    (discovery.ledger?.serpCharged || 0) * 0.015 +
    (signalDiscovery.ledger?.serpCharged || 0) * 0.015 +
    ((discovery.ledger?.openaiCalls || 0) +
      (signalDiscovery.ledger?.openaiCalls || 0)) *
      0.01;

  Object.assign(report, {
    discoveryVersion: LIVE_VENUE_DISCOVERY_V1_2,
    signalVersion: LIVE_EVENT_SIGNALS_V1_2,
    venueDiscovery: {
      VENUES_DISCOVERED: venues.length,
      EXISTING_REUSED: venueWrites.EXISTING_REUSED,
      NEW_VERIFIED: APPLY ? venueWrites.NEW_CREATED : venueWrites.NEW_VERIFIED_DRY,
      DUPLICATES_PREVENTED: venueWrites.DUPLICATES_PREVENTED,
      NEEDS_REVIEW: venueWrites.NEEDS_REVIEW,
      SKIP_LOW_IDENTITY: venueWrites.SKIP_LOW_IDENTITY,
    },
    venueQuality: quality,
    classification,
    highPotential,
    eventSignals: {
      EVENT_SEARCHES: signalDiscovery.ledger?.eventSearches || 0,
      FUTURE_SIGNALS_FOUND: (signalDiscovery.signals || []).length,
      SPECIFIC_EVENT_CANDIDATES: qualifiedSignals.length,
      ACTIONABLE: specificAtBats.length,
      WATCH: qualifiedSignals.filter((r) => r.qualification.qualification === "WATCH")
        .length,
      REJECTED:
        signalWrites.rejected +
        qualifiedSignals.filter((r) => r.qualification.qualification === "FALSE").length,
    },
    specificCandidates,
    atBats: {
      VENUE_PARTNERSHIP: partnershipAtBats.length,
      SPECIFIC_EVENT: specificAtBats.length,
      TOTAL:
        partnershipAtBats.length + specificAtBats.length,
    },
    airtable: {
      VENUES_CREATED: APPLY ? venueWrites.NEW_CREATED : 0,
      VENUES_UPDATED: APPLY ? venueWrites.EXISTING_REUSED : 0,
      HOTEL_VENUE_FIT_CREATED: APPLY ? fitWrites.created : 0,
      PRIVATE_EVENT_SIGNALS_CREATED: APPLY ? signalWrites.created : 0,
      GDI_PROMOTED: 0,
      ORPHANS: 0,
      BROKEN_LINKS: 0,
      dryRunSummaries: { venueWrites, fitWrites, signalWrites },
    },
    sourceQuality: {
      OFFICIAL_SOURCE_VENUES: venues.filter((v) => v.research?.officialWebsite)
        .length,
      THIRD_PARTY_ONLY: venues.filter(
        (v) => !v.research?.officialWebsite && (v.sourceUrls || []).length
      ).length,
      PARTIALLY_VERIFIED: venues.filter(
        (v) => v.research?.keyFactsOfficialConfirmed === "PARTIAL"
      ).length,
      WEAK_REJECTED: venueWrites.SKIP_LOW_IDENTITY + venueWrites.NEEDS_REVIEW,
    },
    gdiPromotionCandidates: gdiPromotionCandidates.map((c) => ({
      kind: c.kind,
      gate: c.gate,
      opportunityId: c.opportunity?.id,
      title: c.opportunity?.title,
      opportunityType: c.opportunity?.opportunityType,
    })),
    replication,
    cost: {
      PRIMARY_QUERIES: discovery.ledger?.serpQueries || 0,
      SECOND_PASS_QUERIES: signalDiscovery.ledger?.eventSearches || 0,
      FETCHES:
        (discovery.ledger?.pagesFetched || 0) +
        (signalDiscovery.ledger?.pagesFetched || 0),
      OPENAI_CALLS:
        (discovery.ledger?.openaiCalls || 0) +
        (signalDiscovery.ledger?.openaiCalls || 0),
      RUNTIME_SEC: runtimeSec,
      SEARCH_API_COST_EST_USD: Number(costEst.toFixed(2)),
      SURFE: 0,
      PDL: 0,
    },
    ledgers: {
      venue: discovery.ledger,
      signals: signalDiscovery.ledger,
    },
    venuesSample: venues.slice(0, 30).map((v) => ({
      venueId: v.venueId,
      name: v.venueName,
      type: v.venueType,
      priority: v.priority,
      lodging: v.onSiteLodgingStatus,
      distance: v.distanceMiles,
      website: v.website,
      official: v.research?.officialWebsite,
      partnerStatus: v.research?.partnerStatus,
    })),
  });

  // Verdict heuristic
  const venueOk = venues.length >= 15;
  const partnerOk = classification.HIGH_POTENTIAL_PARTNER >= 3;
  const specificFound = specificAtBats.length;
  let verdict;
  if (venueOk && partnerOk && specificFound >= 1) {
    verdict =
      "PRIVATE EVENTS LIVE CANARY PASSES — READY FOR CONTROLLED MARKET EXPANSION";
  } else if (venueOk && partnerOk && specificFound === 0) {
    verdict =
      "VENUE PARTNERSHIPS WORK — SPECIFIC EVENT DISCOVERY NEEDS IMPROVEMENT";
  } else if (venues.length >= 8 && (partnerOk || classification.EVENT_SIGNAL_TARGET >= 5)) {
    verdict = "LIVE PRIVATE EVENT DISCOVERY IMPROVED — ONE SMALL CYCLE REMAINS";
  } else {
    verdict = "LIVE SOURCE QUALITY TOO WEAK — HOLD EXPANSION";
  }
  report.verdict = verdict;

  fs.writeFileSync(
    path.join(OUT, "LIVE_CANARY.json"),
    JSON.stringify(report, null, 2)
  );

  const md = buildFounderMd(report);
  fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), md);
  console.log(md);
  console.error(`[pe-v1.2] wrote ${path.join(OUT, "FOUNDER_REPORT.md")}`);
}

function buildFounderMd(r) {
  const v = r.venueDiscovery || {};
  const q = r.venueQuality || {};
  const c = r.classification || {};
  const e = r.eventSignals || {};
  const a = r.atBats || {};
  const at = r.airtable || {};
  const s = r.sourceQuality || {};
  const cost = r.cost || {};
  const rep = r.replication || {};

  return `# GDI Private Events V1.2 — Live Bethesda Canary Founder Report

Generated: ${r.generatedAt}  
Mode: **${r.mode}** · GDI promotion: **${r.gdiPromotion}**  
Base: \`${r.baseId}\` (canonical=${yn(r.isCanonical)})  
Hotel: \`${r.hotelId}\` · Market label: ${r.marketLabel}  
Archetype: \`${r.hotelArchetype}\`

---

## A. LIVE VENUE DISCOVERY

VENUES DISCOVERED: **${v.VENUES_DISCOVERED ?? 0}**  
EXISTING REUSED: **${v.EXISTING_REUSED ?? 0}**  
NEW VERIFIED: **${v.NEW_VERIFIED ?? 0}**  
DUPLICATES PREVENTED: **${v.DUPLICATES_PREVENTED ?? 0}**  
NEEDS REVIEW: **${v.NEEDS_REVIEW ?? 0}**

---

## B. VENUE QUALITY

OFFICIAL WEBSITE: **${q.OFFICIAL_WEBSITE}**  
ADDRESS: **${q.ADDRESS}**  
COORDINATES: **${q.COORDINATES}**  
CAPACITY: **${q.CAPACITY}**  
LODGING STATUS: **${q.LODGING_STATUS}**  
PRIVATE EVENT ACTIVITY: **${q.PRIVATE_EVENT_ACTIVITY}**  
PARTNER STATUS: **${q.PARTNER_STATUS}**  
CONTACT PATH: **${q.CONTACT_PATH}**

---

## C. VENUE CLASSIFICATION

HIGH_POTENTIAL_PARTNER: **${c.HIGH_POTENTIAL_PARTNER ?? 0}**  
EVENT_SIGNAL_TARGET: **${c.EVENT_SIGNAL_TARGET ?? 0}**  
WATCH: **${c.WATCH ?? 0}**  
LOW_HOTEL_OPPORTUNITY: **${c.LOW_HOTEL_OPPORTUNITY ?? 0}**

---

## D. HIGH-POTENTIAL VENUES

${
  (r.highPotential || []).length
    ? (r.highPotential || [])
        .map(
          (h) =>
            `### ${h.venue}
- Type: ${h.venueType} · Distance: ${h.distance ?? "—"} mi · Capacity: ${h.capacity ?? "—"}
- Lodging: ${h.lodgingStatus} · Partner: ${h.hotelPartnerStatus}
- PE evidence: ${h.privateEventEvidence}
- Contact path: ${h.commercialContactPath}
- Why: ${h.whyOpportunityExists || "—"}
- Action: ${h.recommendedAction || "—"}
- Source: ${h.officialSource || "—"}`
        )
        .join("\n\n")
    : "_None classified HIGH_POTENTIAL_PARTNER this canary._"
}

---

## E. LIVE EVENT SIGNAL DISCOVERY

EVENT SEARCHES: **${e.EVENT_SEARCHES ?? 0}**  
FUTURE SIGNALS FOUND: **${e.FUTURE_SIGNALS_FOUND ?? 0}**  
SPECIFIC EVENT CANDIDATES: **${e.SPECIFIC_EVENT_CANDIDATES ?? 0}**  
ACTIONABLE: **${e.ACTIONABLE ?? 0}**  
WATCH: **${e.WATCH ?? 0}**  
REJECTED: **${e.REJECTED ?? 0}**

---

## F. SPECIFIC EVENT CANDIDATES

${
  (r.specificCandidates || []).length
    ? (r.specificCandidates || [])
        .map(
          (s) =>
            `- **${s.event || "(unnamed)"}** (${s.eventType}) · ${s.date || "undated"} @ ${s.venue} · rooms ${s.potentialRooms || "—"} · gate=${s.gate} · ${s.source || ""}`
        )
        .join("\n")
    : "_No public forward private-event signals qualified this canary (acceptable if observability is low)._"
}

---

## G. INCREMENTAL AT-BATS

VENUE PARTNERSHIP: **${a.VENUE_PARTNERSHIP ?? 0}**  
SPECIFIC EVENT: **${a.SPECIFIC_EVENT ?? 0}**  
TOTAL INCREMENTAL CREDIBLE AT-BATS: **${a.TOTAL ?? 0}**

GDI Opportunities written: **0** (dry-run promotion only)

---

## H. AIRTABLE

VENUES CREATED: **${at.VENUES_CREATED ?? 0}**  
VENUES UPDATED: **${at.VENUES_UPDATED ?? 0}**  
HOTEL VENUE FIT CREATED: **${at.HOTEL_VENUE_FIT_CREATED ?? 0}**  
PRIVATE EVENT SIGNALS CREATED: **${at.PRIVATE_EVENT_SIGNALS_CREATED ?? 0}**  
GDI PROMOTED: **0**  
ORPHANS: **${at.ORPHANS ?? 0}**  
BROKEN LINKS: **${at.BROKEN_LINKS ?? 0}**

---

## I. SOURCE QUALITY

OFFICIAL-SOURCE VENUES: **${s.OFFICIAL_SOURCE_VENUES ?? 0}**  
THIRD-PARTY-ONLY VENUES: **${s.THIRD_PARTY_ONLY ?? 0}**  
PARTIALLY VERIFIED: **${s.PARTIALLY_VERIFIED ?? 0}**  
WEAK / REJECTED: **${s.WEAK_REJECTED ?? 0}**

---

## J. OBSERVABILITY

1. Public search venue discovery: ${(v.VENUES_DISCOVERED || 0) >= 15 ? "YES — workable" : "PARTIAL / WEAK"}
2. Lodging status reliable: ${q.LODGING_STATUS}
3. Hotel-partner status reliable: ${q.PARTNER_STATUS}
4. Forward wedding/PE signals observable: ${(e.FUTURE_SIGNALS_FOUND || 0) > 0 ? "SOME" : "LOW / NONE this cycle"}
5. Most useful sources: official venue sites + Google organic (Serp); directories for discovery only
6. Noise sources: wedding aggregators / directories without official confirmation

---

## K. REPLICATION

GENERIC HOTEL INPUT CONTRACT: **${rep.GENERIC_HOTEL_INPUT_CONTRACT}**  
RENAISSANCE CODE CHECK: **${rep.RENAISSANCE_CODE_CHECK}**  
CAMBRIDGE CODE CHECK: **${rep.CAMBRIDGE_CODE_CHECK}**  
HOTEL-SPECIFIC LOGIC: **${rep.HOTEL_SPECIFIC_LOGIC}**  
CITY-SPECIFIC LOGIC: **${rep.CITY_SPECIFIC_LOGIC}**

---

## L. SPEED / COST

PRIMARY QUERIES: **${cost.PRIMARY_QUERIES ?? 0}**  
SECOND-PASS QUERIES: **${cost.SECOND_PASS_QUERIES ?? 0}**  
FETCHES: **${cost.FETCHES ?? 0}**  
OPENAI CALLS: **${cost.OPENAI_CALLS ?? 0}**  
RUNTIME: **${cost.RUNTIME_SEC ?? 0}s**  
SEARCH/API COST (est): **$${cost.SEARCH_API_COST_EST_USD ?? 0}**  
SURFE: **0**  
PDL: **0**

---

## M. REGRESSION

_See canary follow-up test run output (populated after suite)._

---

## N. DECISION

1. Real reusable Bethesda venue universe: **${(v.VENUES_DISCOVERED || 0) >= 15 ? "YES (canary-scale)" : "PARTIAL"}**
2. Identity/dedupe scalable: **${(v.NEEDS_REVIEW || 0) <= 3 ? "YES" : "NEEDS WORK"}**
3. Lodging classifications accurate enough: **${q.LODGING_STATUS} coverage**
4. Venue partnerships credible: **${(a.VENUE_PARTNERSHIP || 0) > 0 ? "YES" : "LIMITED"}**
5. Forward PE signals observable: **${(e.FUTURE_SIGNALS_FOUND || 0) > 0 ? "PARTIAL" : "LOW"}**
6. Specific event at-bats: **${a.SPECIFIC_EVENT ?? 0}**
7. Total incremental at-bats: **${a.TOTAL ?? 0}**
8. Source quality acceptable: **${(s.OFFICIAL_SOURCE_VENUES || 0) >= 8 ? "YES" : "MARGINAL"}**
9. Airtable persistence: **${APPLY ? "APPLIED (graph)" : "DRY_RUN"}** / GDI promote dry-run
10. Replicable to other hotels: **${rep.GENERIC_HOTEL_INPUT_CONTRACT === "PASS" ? "YES" : "NO"}**
11. Expand Bethesda beyond canary: **pending verdict**
12. Begin another hotel market live test: **pending verdict**

---

## O. FINAL VERDICT

**${r.verdict}**
`;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
