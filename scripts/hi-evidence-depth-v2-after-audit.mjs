#!/usr/bin/env node
/**
 * HI Evidence Depth V2 — post-run audit, GDI counts, Hilton vs Renaissance diagnostic.
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { isHotelIntelligenceComplete } from "../lib/hotel-intelligence/onboarding/hi-completeness-gate.js";
import { loadDomainStatusLedger } from "../lib/hotel-intelligence/onboarding/domain-status-store.js";
import { buildHotelIntelligenceProfile } from "../lib/hotel-intelligence/adp-attributes/build-hotel-intelligence-profile.js";
import { loadHotelIntelligenceFromAirtable } from "../lib/hotel-intelligence/schema/hi-airtable-store.js";
import { listOpportunitiesForHotel } from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import { gdiEventSpaceCapabilitySemantics } from "../lib/hotel-intelligence/onboarding/hi-completeness-gate.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT_DIR = path.join(__dirname, "..", "reports", "hotel-intelligence", "evidence-depth-v2");

const HOTELS = [
  ["recLuxvwwxID7U2B8", "Bethesda Marriott"],
  ["recgMYovrrZDJMqzX", "Waterstone Resort & Marina"],
  ["recG66DQJKP2c0UNh", "Renaissance New York Times Square"],
  ["recGkME49yYuxQl0u", "NOW NOW NoHo"],
  ["rec8hHupaSwiWI3r7", "Hotel Phillips"],
  ["recIwaP1etgx2g9nA", "Cambridge Beaches"],
  ["recsn3BUKJ9PNfeZW", "JW Marriott Monterrey Valle"],
  ["recRXmrakhSAuctwz", "The St. Regis Mexico City"],
  ["recN76iEE6yAaPh8H", "The St. Regis Cap Cana"],
  ["recESHsNsWUFYZrxR", "JW Marriott Hotel Santo Domingo"],
  ["recCEpdskZeUBvQwG", "Hotel Caribe Faranda Grand"],
  ["recD17Kxn6BcJjGFh", "The Westin Monterrey Valle"],
  ["recUOyzOXn2Zdp98I", "Radisson Hotel Santo Domingo"],
  ["recjDsNzu93CFfe87", "Casas del XVI"],
  ["rec9Tp0WBb2uk6w3u", "Faranda Collection Bogotá"],
  ["rece0or38cxo3Fymb", "W Rome"],
  ["rec35fExUxCClpOP6", "Hilton New York Times Square"],
  ["rec2PVBDavppGpenm", "AC Hotel A Coruña"],
  ["recKRJjcPnb4tVDDS", "Spice Island Beach Resort"],
];

const FOCUS_GDI = [
  "recLuxvwwxID7U2B8",
  "recG66DQJKP2c0UNh",
  "recgMYovrrZDJMqzX",
  "rec35fExUxCClpOP6",
  "recGkME49yYuxQl0u",
];

function countFacing(opps) {
  const counts = {
    total: opps.length,
    active: 0,
    watch: 0,
    actionable: 0,
    closed: 0,
    other: 0,
    byState: {},
  };
  for (const o of opps) {
    const s = String(o.customerFacingState || o.status || "UNKNOWN").toUpperCase();
    counts.byState[s] = (counts.byState[s] || 0) + 1;
    if (s === "ACTIVE" || s === "ACTIONABLE_NOW") counts.active += 1;
    if (s === "ACTIONABLE_NOW") counts.actionable += 1;
    if (s === "WATCH" || s === "FUTURE_WATCH") counts.watch += 1;
    if (s === "CLOSED" || s === "DISQUALIFIED") counts.closed += 1;
  }
  counts.customerFacingOpen = counts.active + counts.watch;
  return counts;
}

async function main() {
  const baseline = JSON.parse(
    fs.readFileSync(path.join(OUT_DIR, "BASELINE_FORENSIC.json"), "utf8")
  );
  const beforeById = Object.fromEntries(
    (baseline.hotels || []).map((h) => [h.hpcHotelId, h])
  );

  const afterHotels = [];
  const emptyAfter = { EVENT_SPACES: 0, DEMAND_NODES: 0, SEASONALITY_NEED_PERIODS: 0 };
  const ceilingAfter = { EVENT_SPACES: 0, DEMAND_NODES: 0, SEASONALITY_NEED_PERIODS: 0 };
  const populatedAfter = { EVENT_SPACES: 0, DEMAND_NODES: 0, SEASONALITY_NEED_PERIODS: 0 };

  for (const [id, label] of HOTELS) {
    process.stderr.write(`audit ${label}\n`);
    const gate = await isHotelIntelligenceComplete(id);
    const ledger = loadDomainStatusLedger(id);
    const profile = await buildHotelIntelligenceProfile(id);
    let airtable = null;
    try {
      airtable = await loadHotelIntelligenceFromAirtable(id);
    } catch (err) {
      airtable = { error: err.message };
    }
    const domains = {};
    for (const d of [
      "COMMERCIAL_PROFILE",
      "EVENT_SPACES",
      "DEMAND_NODES",
      "SEASONALITY_NEED_PERIODS",
      "HI_EVIDENCE",
      "ADP_ATTRIBUTES",
    ]) {
      const status =
        gate.domains?.[d]?.domainStatus || ledger.domains?.[d]?.domainStatus || "UNKNOWN";
      domains[d] = {
        domainStatus: status,
        researchDepth: ledger.domains?.[d]?.researchDepth || null,
        rowCount: gate.domains?.[d]?.rowCount ?? ledger.domains?.[d]?.rowCount,
        notes: ledger.domains?.[d]?.notes || gate.domains?.[d]?.notes,
      };
      if (emptyAfter[d] != null) {
        if (status === "RESEARCHED_EMPTY") emptyAfter[d] += 1;
        if (status === "PUBLIC_DATA_CEILING") ceilingAfter[d] += 1;
        if (status === "POPULATED") populatedAfter[d] += 1;
      }
    }

    const eventSem = gdiEventSpaceCapabilitySemantics(domains.EVENT_SPACES.domainStatus);
    const before = beforeById[id];
    afterHotels.push({
      hpcHotelId: id,
      hotel: gate.hotelName || label,
      hiComplete: gate.complete,
      overallStatus: gate.overallStatus,
      domains,
      beforeDomains: before?.domains
        ? {
            EVENT_SPACES: before.domains.EVENT_SPACES?.domainStatus,
            DEMAND_NODES: before.domains.DEMAND_NODES?.domainStatus,
            SEASONALITY_NEED_PERIODS: before.domains.SEASONALITY_NEED_PERIODS?.domainStatus,
          }
        : null,
      recordCounts: {
        eventSpaces: airtable?.eventSpaces?.length || profile.eventSpaces?.length || 0,
        demandNodes: airtable?.demandNodes?.length || profile.demandNodes?.length || 0,
        seasonality: airtable?.seasonality?.length || profile.seasonality?.length || 0,
      },
      canonicalValues: {
        roomsKeys:
          airtable?.commercial?.roomsKeys ??
          profile.identity?.roomsKeys ??
          profile.commercialProfile?.rooms ??
          null,
        totalMeetingSpaceSqFt:
          airtable?.commercial?.totalMeetingSpaceSqFt ??
          profile.commercialProfile?.meetingSpace?.totalSqFt ??
          null,
        meetingRoomCount:
          airtable?.commercial?.meetingRoomCount ??
          profile.commercialProfile?.meetingSpace?.meetingRooms ??
          null,
        largestMeetingSpaceSqFt:
          airtable?.commercial?.largestMeetingSpaceSqFt ??
          profile.commercialProfile?.meetingSpace?.largestRoom?.sqFt ??
          null,
        largestEventCapacity:
          airtable?.commercial?.largestEventCapacity ??
          profile.commercialProfile?.meetingSpace?.largestRoom?.capacity ??
          null,
      },
      gdiEventSemantics: eventSem,
    });
  }

  const gdiCounts = {};
  const gdiReeval = [];
  for (const id of FOCUS_GDI) {
    const opps = await listOpportunitiesForHotel(id);
    gdiCounts[id] = countFacing(opps);
  }
  // Radisson reeval candidate if event now populated
  const radisson = afterHotels.find((h) => h.hpcHotelId === "recUOyzOXn2Zdp98I");
  if (
    radisson?.beforeDomains?.EVENT_SPACES === "RESEARCHED_EMPTY" &&
    radisson?.domains?.EVENT_SPACES?.domainStatus === "POPULATED"
  ) {
    gdiReeval.push({
      hpcHotelId: "recUOyzOXn2Zdp98I",
      hotel: radisson.hotel,
      reason: "EVENT_SPACES_WAS_RESEARCHED_EMPTY_NOW_POPULATED",
      priorMeetingCapabilityState: "KNOWN_EMPTY_OR_UNKNOWN",
      newMeetingCapabilityState: radisson.gdiEventSemantics.meetingCapabilityState,
      action: "REQUALIFY_EXISTING_WATCH_CANDIDATE_CORPUS",
      doNotAutoPromote: true,
    });
  }
  for (const h of afterHotels) {
    if (
      h.beforeDomains?.EVENT_SPACES === "RESEARCHED_EMPTY" &&
      h.domains?.EVENT_SPACES?.domainStatus === "POPULATED"
    ) {
      if (h.hpcHotelId === "recUOyzOXn2Zdp98I") continue;
      gdiReeval.push({
        hpcHotelId: h.hpcHotelId,
        hotel: h.hotel,
        reason: "EVENT_SPACES_POPULATED_FROM_EMPTY",
        action: "REQUALIFY_EXISTING_WATCH_CANDIDATE_CORPUS",
        doNotAutoPromote: true,
      });
    }
  }

  const hilton = afterHotels.find((h) => h.hpcHotelId === "rec35fExUxCClpOP6");
  const renaissance = afterHotels.find((h) => h.hpcHotelId === "recG66DQJKP2c0UNh");
  const hiltonRenaissance = {
    renaissance: {
      hotel: renaissance?.hotel,
      rooms: renaissance?.canonicalValues?.roomsKeys,
      meetingSqFt: renaissance?.canonicalValues?.totalMeetingSpaceSqFt,
      meetingRooms: renaissance?.canonicalValues?.meetingRoomCount,
      largestRoom: renaissance?.canonicalValues?.largestMeetingSpaceSqFt,
      largestCap: renaissance?.canonicalValues?.largestEventCapacity,
      eventStatus: renaissance?.domains?.EVENT_SPACES?.domainStatus,
      demandStatus: renaissance?.domains?.DEMAND_NODES?.domainStatus,
      demandCount: renaissance?.recordCounts?.demandNodes,
      gdi: gdiCounts["recG66DQJKP2c0UNh"],
    },
    hilton: {
      hotel: hilton?.hotel,
      rooms: hilton?.canonicalValues?.roomsKeys,
      meetingSqFt: hilton?.canonicalValues?.totalMeetingSpaceSqFt,
      meetingRooms: hilton?.canonicalValues?.meetingRoomCount,
      largestRoom: hilton?.canonicalValues?.largestMeetingSpaceSqFt,
      largestCap: hilton?.canonicalValues?.largestEventCapacity,
      eventStatus: hilton?.domains?.EVENT_SPACES?.domainStatus,
      demandStatus: hilton?.domains?.DEMAND_NODES?.domainStatus,
      demandCount: hilton?.recordCounts?.demandNodes,
      gdi: gdiCounts["rec35fExUxCClpOP6"],
    },
  };

  const summary = {
    generatedAt: new Date().toISOString(),
    hotelCount: afterHotels.length,
    hiCompleteCount: afterHotels.filter((h) => h.hiComplete).length,
    beforeEmpty: baseline.summary?.researchedEmptyCounts,
    after: {
      RESEARCHED_EMPTY: emptyAfter,
      PUBLIC_DATA_CEILING: ceilingAfter,
      POPULATED: populatedAfter,
    },
    gdiCounts,
    gdiReeval,
    hiltonRenaissance,
  };

  fs.writeFileSync(
    path.join(OUT_DIR, "AFTER_AUDIT.json"),
    JSON.stringify({ summary, hotels: afterHotels }, null, 2)
  );
  fs.writeFileSync(
    path.join(OUT_DIR, "GDI_REEVALUATION_CANDIDATES.json"),
    JSON.stringify({ generatedAt: summary.generatedAt, candidates: gdiReeval }, null, 2)
  );
  fs.writeFileSync(
    path.join(OUT_DIR, "HILTON_VS_RENAISSANCE_HI.json"),
    JSON.stringify(hiltonRenaissance, null, 2)
  );
  console.log(JSON.stringify(summary, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
