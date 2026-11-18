#!/usr/bin/env node
/**
 * Read-only Rock Creek Mansion venue identity audit (P0).
 * No Airtable mutations.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";
import { computeVenueId } from "../lib/group-demand-intelligence/private-events/venue-entity.js";
import {
  MAP_PE_VENUE,
  MAP_HOTEL_VENUE_FIT,
  MAP_PE_SIGNAL,
  PE_VENUES_TABLE_NAME,
  HOTEL_VENUE_FIT_TABLE_NAME,
  PE_SIGNALS_TABLE_NAME,
} from "../lib/group-demand-intelligence/private-events/airtable-field-map.js";
import { getPeBase } from "../lib/group-demand-intelligence/private-events/airtable-client.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT_DIR = path.join(ROOT, "reports/gdi-multi-hotel-airtable-reconciliation-v2");
const RECORD_ID = "recf35Q9B6jpX3Ivm";
const PILOT_ID = "pev_42542bf58aac936b";
const DOMAIN_ID = "pev_53b689545914754d";

function escapeFormula(v) {
  return String(v || "").replace(/'/g, "\\'");
}

async function selectByField(table, fieldName, value) {
  const formula = `{${fieldName}}='${escapeFormula(value)}'`;
  const rows = await table.select({ filterByFormula: formula, maxRecords: 100 }).all();
  return rows.map((r) => ({
    airtableRecordId: r.id,
    venueId: r.fields[fieldName] || null,
    hotelId: r.fields["Hotel ID"] || r.fields[MAP_HOTEL_VENUE_FIT.hotelId] || null,
    venueName:
      r.fields[MAP_PE_VENUE.venueName] ||
      r.fields[MAP_HOTEL_VENUE_FIT.venueName] ||
      r.fields[MAP_PE_SIGNAL.eventName] ||
      null,
    fitId: r.fields[MAP_HOTEL_VENUE_FIT.fitId] || null,
    signalId: r.fields[MAP_PE_SIGNAL.signalId] || null,
  }));
}

function repoReferences() {
  const needles = [PILOT_ID, DOMAIN_ID, RECORD_ID, "Rock Creek Mansion"];
  const roots = [
    path.join(ROOT, "data/group-demand-intelligence"),
    path.join(ROOT, "reports/gdi-operational-baseline"),
    path.join(ROOT, "reports/gdi-airtable-bethesda-production-pilot-v1"),
    path.join(ROOT, "reports/gdi-full-e2e-airtable-persistence-adapter-v1"),
    path.join(ROOT, "lib/group-demand-intelligence"),
  ];
  const hits = [];
  const walk = (dir, depth = 0) => {
    if (depth > 6 || !fs.existsSync(dir)) return;
    let entries = [];
    try {
      entries = fs.readdirSync(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const ent of entries) {
      const p = path.join(dir, ent.name);
      if (ent.isDirectory()) {
        if (ent.name === "node_modules" || ent.name === ".git") continue;
        walk(p, depth + 1);
        continue;
      }
      if (!/\.(json|js|mjs|md)$/i.test(ent.name)) continue;
      let text = "";
      try {
        text = fs.readFileSync(p, "utf8");
      } catch {
        continue;
      }
      const found = needles.filter((n) => text.includes(n));
      if (found.length) {
        hits.push({
          path: path.relative(ROOT, p).replace(/\\/g, "/"),
          matched: found,
        });
      }
    }
  };
  for (const r of roots) walk(r);
  return hits;
}

async function main() {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  const base = getPeBase();
  const venueTable = base(PE_VENUES_TABLE_NAME);
  const fitTable = base(HOTEL_VENUE_FIT_TABLE_NAME);
  const signalTable = base(PE_SIGNALS_TABLE_NAME);

  const rec = await venueTable.find(RECORD_ID);
  const f = rec.fields || {};
  let payload = null;
  const payloadRaw = f[MAP_PE_VENUE.venuePayloadJson];
  try {
    payload =
      typeof payloadRaw === "string" ? JSON.parse(payloadRaw) : payloadRaw;
  } catch (err) {
    payload = { parseError: String(err?.message || err) };
  }

  const seedDomain = computeVenueId({
    venueName: "Rock Creek Mansion",
    officialDomain: "rockcreekmansion.com",
  });
  const seedCityOnly = computeVenueId({
    venueName: "Rock Creek Mansion",
    city: "Bethesda",
    country: "US",
  });
  const seedCityMd = computeVenueId({
    venueName: "Rock Creek Mansion",
    city: "Bethesda",
    region: "MD",
    country: "US",
  });

  const fitsPilot = await selectByField(fitTable, MAP_HOTEL_VENUE_FIT.venueId, PILOT_ID);
  const fitsDomain = await selectByField(fitTable, MAP_HOTEL_VENUE_FIT.venueId, DOMAIN_ID);
  const signalsPilot = await selectByField(signalTable, MAP_PE_SIGNAL.venueId || "Venue ID", PILOT_ID);
  const signalsDomain = await selectByField(
    signalTable,
    MAP_PE_SIGNAL.venueId || "Venue ID",
    DOMAIN_ID,
  );

  const nameRows = await venueTable
    .select({
      filterByFormula: "FIND('Rock Creek', {Venue Name})",
      maxRecords: 20,
    })
    .all();
  const nameHits = nameRows.map((r) => ({
    airtableRecordId: r.id,
    venueId: r.fields[MAP_PE_VENUE.venueId] || null,
    venueName: r.fields[MAP_PE_VENUE.venueName] || null,
    officialDomain: r.fields[MAP_PE_VENUE.officialDomain] || null,
    website: r.fields[MAP_PE_VENUE.website] || null,
    city: r.fields[MAP_PE_VENUE.city] || null,
  }));

  const doc = {
    version: "gdi-rock-creek-venue-identity-audit-v2",
    generatedAt: new Date().toISOString(),
    mutation: false,
    productionWrites: false,
    record: {
      airtableRecordId: rec.id,
      structuredVenueId: f[MAP_PE_VENUE.venueId] || null,
      venueName: f[MAP_PE_VENUE.venueName] || null,
      website: f[MAP_PE_VENUE.website] || null,
      officialDomain: f[MAP_PE_VENUE.officialDomain] || null,
      city: f[MAP_PE_VENUE.city] || null,
      region: f[MAP_PE_VENUE.region] || null,
      address: f[MAP_PE_VENUE.address] || null,
      onSiteLodgingStatus: f[MAP_PE_VENUE.onSiteLodgingStatus] || null,
      primarySourceUrl: f[MAP_PE_VENUE.primarySourceUrl] || null,
      payloadVenueId: payload?.venueId || payload?.id || null,
      payloadWebsite: payload?.website || null,
      payloadOfficialDomain: payload?.officialDomain || null,
      payloadCity: payload?.city || null,
      payloadLodging: payload?.onSiteLodgingStatus || null,
    },
    idComputation: {
      domainBased: seedDomain,
      cityOnlyNoRegion: seedCityOnly,
      cityWithMd: seedCityMd,
      matches: {
        domainEqualsHistorical: seedDomain === DOMAIN_ID,
        cityOnlyEqualsPilot: seedCityOnly === PILOT_ID,
      },
      explanation:
        "Pilot ID equals computeVenueId(name+city Bethesda) without official domain. Historical ID equals computeVenueId(name+rockcreekmansion.com).",
    },
    linkedRecords: {
      hotelVenueFit: { pilotId: fitsPilot, domainId: fitsDomain },
      privateEventSignals: { pilotId: signalsPilot, domainId: signalsDomain },
    },
    nameSearchHits: nameHits,
    samePhysicalVenue: true,
    authoritativeCanonicalIdProposal: DOMAIN_ID,
    historicalAliasId: PILOT_ID,
    divergenceCause:
      "PR #367 updated structured Venue ID from domain-stable pev_53b689545914754d to geo-only pev_42542bf58aac936b computed by dry-run without website/domain. Venue Payload JSON was intentionally not overwritten and still holds the historical domain-based ID.",
    repoReferenceSample: repoReferences().slice(0, 80),
    shaProbe: createHash("sha256")
      .update("d:rockcreekmansion.com|n:rock creek mansion")
      .digest("hex")
      .slice(0, 16),
  };

  const outPath = path.join(OUT_DIR, "ROCK_CREEK_IDENTITY_AUDIT.json");
  fs.writeFileSync(outPath, JSON.stringify(doc, null, 2));
  console.log(
    JSON.stringify(
      {
        ok: true,
        outPath,
        structuredVenueId: doc.record.structuredVenueId,
        payloadVenueId: doc.record.payloadVenueId,
        authoritativeCanonicalIdProposal: doc.authoritativeCanonicalIdProposal,
        fitsPilot: fitsPilot.length,
        fitsDomain: fitsDomain.length,
        signalsPilot: signalsPilot.length,
        signalsDomain: signalsDomain.length,
        nameHits: nameHits.length,
      },
      null,
      2,
    ),
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
