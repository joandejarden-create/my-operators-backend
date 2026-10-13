#!/usr/bin/env node
/**
 * HI Evidence Depth V2 — Phase 0 forensic baseline (read-only).
 * Snapshot all active ADP/GDI hotels' HI domain states before mutation.
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { isHotelIntelligenceComplete } from "../lib/hotel-intelligence/onboarding/hi-completeness-gate.js";
import { loadDomainStatusLedger } from "../lib/hotel-intelligence/onboarding/domain-status-store.js";
import { HI_DOMAIN } from "../lib/hotel-intelligence/onboarding/domain-status-v1.js";
import { buildHotelIntelligenceProfile } from "../lib/hotel-intelligence/adp-attributes/build-hotel-intelligence-profile.js";
import { loadHotelIntelligenceFromAirtable } from "../lib/hotel-intelligence/schema/hi-airtable-store.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(REPO_ROOT, "reports", "hotel-intelligence", "evidence-depth-v2");

const EXPECTED_IDS = [
  "recLuxvwwxID7U2B8",
  "recgMYovrrZDJMqzX",
  "recG66DQJKP2c0UNh",
  "recGkME49yYuxQl0u",
  "rec8hHupaSwiWI3r7",
  "recIwaP1etgx2g9nA",
  "recsn3BUKJ9PNfeZW",
  "recRXmrakhSAuctwz",
  "recN76iEE6yAaPh8H",
  "recESHsNsWUFYZrxR",
  "recCEpdskZeUBvQwG",
  "recD17Kxn6BcJjGFh",
  "recUOyzOXn2Zdp98I",
  "recjDsNzu93CFfe87",
  "rec9Tp0WBb2uk6w3u",
  "rece0or38cxo3Fymb",
  "rec35fExUxCClpOP6",
  "rec2PVBDavppGpenm",
  "recKRJjcPnb4tVDDS",
];

function discoverActiveHotels() {
  const auditPath = path.join(
    REPO_ROOT,
    "reports",
    "hotel-intelligence",
    "completeness-onboarding-v1",
    "HI_COMPLETENESS_AUDIT_AFTER.json"
  );
  if (fs.existsSync(auditPath)) {
    const audit = JSON.parse(fs.readFileSync(auditPath, "utf8"));
    const ids = audit.map((r) => r.hpcHotelId).filter(Boolean);
    if (ids.length >= 19) return [...new Set(ids)];
  }
  return EXPECTED_IDS;
}

async function snapshotHotel(hpcHotelId) {
  const gate = await isHotelIntelligenceComplete(hpcHotelId, { skipLiveHpc: false });
  const ledger = loadDomainStatusLedger(hpcHotelId);
  const profile = await buildHotelIntelligenceProfile(hpcHotelId, { skipLiveHpc: false });
  let airtable = null;
  try {
    airtable = await loadHotelIntelligenceFromAirtable(hpcHotelId);
  } catch (err) {
    airtable = { error: err.message || String(err) };
  }

  const domains = {};
  for (const d of Object.values(HI_DOMAIN)) {
    const g = gate.domains?.[d] || {};
    const l = ledger.domains?.[d] || {};
    domains[d] = {
      domainStatus: g.domainStatus || l.domainStatus || "UNKNOWN",
      researchDepth: l.researchDepth || null,
      rowCount: g.rowCount ?? l.rowCount ?? null,
      evidenceCount: g.evidenceCount ?? l.evidenceCount ?? null,
      lastResearchedAt: g.lastResearchedAt || l.lastResearchedAt || null,
      sourceCoverage: g.sourceCoverage || l.sourceCoverage || [],
      sourcesAttempted: l.sourcesAttempted || [],
      notes: g.notes || l.notes || null,
      feedsAdpAttributes: [
        HI_DOMAIN.COMMERCIAL_PROFILE,
        HI_DOMAIN.EVENT_SPACES,
        HI_DOMAIN.DEMAND_NODES,
        HI_DOMAIN.SEASONALITY_NEED_PERIODS,
      ].includes(d),
      affectsGdiHotelFit: [
        HI_DOMAIN.COMMERCIAL_PROFILE,
        HI_DOMAIN.EVENT_SPACES,
        HI_DOMAIN.DEMAND_NODES,
      ].includes(d),
    };
  }

  const commercial = airtable?.commercial || profile.commercialProfile || null;
  const eventSpaces = airtable?.eventSpaces || profile.eventSpaces || [];
  const demandNodes = airtable?.demandNodes || profile.demandNodes || [];
  const seasonality = airtable?.seasonality || profile.seasonality || [];
  const evidence = airtable?.evidence || [];

  const sourceUrls = [
    ...eventSpaces.map((e) => e.sourceUrl || e.fields?.["Source URL"]).filter(Boolean),
    ...evidence.map((e) => e.sourceUrl || e.fields?.["Source URL"]).filter(Boolean),
  ].slice(0, 20);

  return {
    hpcHotelId,
    hotel: gate.hotelName || profile.identity?.hotelName || null,
    country: profile.identity?.country || null,
    market: profile.identity?.market || profile.commercialProfile?.market || null,
    overallStatus: gate.overallStatus,
    hiComplete: gate.complete,
    domains,
    recordCounts: {
      commercial: commercial ? 1 : 0,
      eventSpaces: eventSpaces.length,
      demandNodes: demandNodes.length,
      seasonality: seasonality.length,
      evidence: evidence.length,
    },
    canonicalValues: {
      roomsKeys:
        commercial?.roomsKeys ??
        commercial?.fields?.["Rooms / Keys"] ??
        profile.identity?.roomsKeys ??
        null,
      totalMeetingSpaceSqFt:
        commercial?.totalMeetingSpaceSqFt ??
        commercial?.fields?.["Total Meeting Space Sq Ft"] ??
        profile.commercialProfile?.meetingSpace?.totalSqFt ??
        null,
      meetingRoomCount:
        commercial?.meetingRoomCount ??
        commercial?.fields?.["Meeting Room Count"] ??
        profile.commercialProfile?.meetingSpace?.meetingRooms ??
        null,
      largestMeetingSpaceSqFt:
        commercial?.largestMeetingSpaceSqFt ??
        profile.commercialProfile?.meetingSpace?.largestRoom?.sqFt ??
        null,
      largestEventCapacity:
        commercial?.largestEventCapacity ??
        profile.commercialProfile?.meetingSpace?.largestRoom?.capacity ??
        null,
    },
    sourceUrls: [...new Set(sourceUrls)],
    adpPropertyId: gate.adpPropertyId || profile.identity?.adpPropertyId || null,
  };
}

async function main() {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  const hotelIds = discoverActiveHotels();
  const rows = [];
  for (const id of hotelIds) {
    process.stderr.write(`baseline ${id}...\n`);
    try {
      rows.push(await snapshotHotel(id));
    } catch (err) {
      rows.push({ hpcHotelId: id, error: err.message || String(err) });
    }
  }

  const emptyCounts = {
    EVENT_SPACES: 0,
    DEMAND_NODES: 0,
    SEASONALITY_NEED_PERIODS: 0,
  };
  const researchedEmpty = [];
  for (const r of rows) {
    if (!r.domains) continue;
    for (const d of Object.keys(emptyCounts)) {
      if (r.domains[d]?.domainStatus === "RESEARCHED_EMPTY") {
        emptyCounts[d] += 1;
        researchedEmpty.push({
          hotel: r.hotel,
          hpcHotelId: r.hpcHotelId,
          domain: d,
          status: "RESEARCHED_EMPTY",
          rowCount: r.domains[d].rowCount,
          notes: r.domains[d].notes,
        });
      }
    }
  }

  const summary = {
    generatedAt: new Date().toISOString(),
    hotelCount: rows.length,
    hiCompleteCount: rows.filter((r) => r.hiComplete).length,
    researchedEmptyCounts: emptyCounts,
    researchedEmpty,
    materialDiffNote:
      emptyCounts.EVENT_SPACES === 4 &&
      emptyCounts.DEMAND_NODES === 16 &&
      emptyCounts.SEASONALITY_NEED_PERIODS === 18
        ? "MATCHES_EXPECTED_V1_AGGREGATE"
        : "DIFFERS_FROM_EXPECTED_V1_AGGREGATE",
    expectedAggregate: { EVENT: 4, DEMAND: 16, SEASONALITY: 18 },
  };

  const out = { summary, hotels: rows };
  const outPath = path.join(OUT_DIR, "BASELINE_FORENSIC.json");
  fs.writeFileSync(outPath, JSON.stringify(out, null, 2) + "\n", "utf8");
  const md = [
    "# HI Evidence Depth V2 — Forensic Baseline",
    "",
    `Generated: ${summary.generatedAt}`,
    "",
    `- Hotels: ${summary.hotelCount}`,
    `- HI Complete: ${summary.hiCompleteCount}`,
    `- RESEARCHED_EMPTY Event: ${emptyCounts.EVENT_SPACES}`,
    `- RESEARCHED_EMPTY Demand: ${emptyCounts.DEMAND_NODES}`,
    `- RESEARCHED_EMPTY Seasonality: ${emptyCounts.SEASONALITY_NEED_PERIODS}`,
    `- Aggregate check: ${summary.materialDiffNote}`,
    "",
    "## RESEARCHED_EMPTY enumeration",
    "",
    ...researchedEmpty.map(
      (e) => `- ${e.hotel} (${e.hpcHotelId}) — ${e.domain}`
    ),
    "",
  ].join("\n");
  fs.writeFileSync(path.join(OUT_DIR, "BASELINE_FORENSIC.md"), md, "utf8");
  console.log(JSON.stringify(summary, null, 2));
  console.log(`Wrote ${outPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
