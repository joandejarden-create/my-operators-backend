#!/usr/bin/env node
/**
 * Read-only Airtable baseline for PE tables + hotel GDI links.
 *   node scripts/gdi-pe-airtable-baseline-v1.mjs
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  isPeAirtableConfigured,
  getPeBase,
} from "../lib/group-demand-intelligence/private-events/airtable-client.js";
import { listVenuesFromAirtable } from "../lib/group-demand-intelligence/private-events/airtable-venue-store.js";
import { listOpportunitiesForHotel } from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import {
  HOTEL_VENUE_FIT_TABLE_NAME,
  PE_SIGNALS_TABLE_NAME,
  MAP_HOTEL_VENUE_FIT,
  MAP_PE_SIGNAL,
  MAP_PE_VENUE,
} from "../lib/group-demand-intelligence/private-events/airtable-field-map.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(
  __dirname,
  "../reports/gdi-operational-baseline/pe-canonical-persist"
);

const HOTELS = [
  { id: "rec0eTEnCqlsGJJt9", name: "Mayflower" },
  { id: "recnPB3Q02NO8Ma1o", name: "San Diego Bayside" },
];

const INTEREST =
  /midway|embarcadero|marina\s*park|corcoran|renwick|portrait|woman.?s\s*club|dar\b|constitution\s*hall|kennedy\s*center|freer|sackler|phillips|textile|national\s*museum|anderson\s*house|decatur/i;

async function listTable(table, formula, max = 200) {
  const base = getPeBase();
  const out = [];
  await base(table)
    .select({
      filterByFormula: formula || undefined,
      pageSize: 50,
      maxRecords: max,
    })
    .eachPage((recs, next) => {
      out.push(...recs);
      next();
    });
  return out;
}

async function main() {
  if (!isPeAirtableConfigured()) throw new Error("pe_airtable_not_configured");
  fs.mkdirSync(OUT, { recursive: true });

  const venues = await listVenuesFromAirtable({ maxRecords: 500 });
  const relevantVenues = venues.filter(
    (v) =>
      INTEREST.test(String(v.venueName || "")) ||
      /san diego|washington/i.test(String(v.city || ""))
  );

  const report = {
    generatedAt: new Date().toISOString(),
    peConfigured: true,
    venuesTotal: venues.length,
    relevantVenues: relevantVenues.map((v) => ({
      venueId: v.venueId,
      name: v.venueName,
      city: v.city,
      airtableRecordId: v._airtableRecordId || null,
      partnerStatus: v.partnerStatus || null,
    })),
    hotels: {},
  };

  for (const h of HOTELS) {
    const fits = await listTable(
      HOTEL_VENUE_FIT_TABLE_NAME,
      `{${MAP_HOTEL_VENUE_FIT.hotelId}}='${h.id}'`,
      200
    );
    const fitRows = fits.map((f) => {
      const fld = f.fields || {};
      return {
        airtableRecordId: f.id,
        fitId: fld[MAP_HOTEL_VENUE_FIT.fitId] || null,
        venueId: fld[MAP_HOTEL_VENUE_FIT.venueId] || null,
        venueName: fld[MAP_HOTEL_VENUE_FIT.venueName] || null,
        currentGdiOpportunityId:
          fld[MAP_HOTEL_VENUE_FIT.currentGdiOpportunityId] || null,
        lodgingCatchmentFit:
          fld[MAP_HOTEL_VENUE_FIT.lodgingCatchmentFit] || null,
        distanceMiles: fld[MAP_HOTEL_VENUE_FIT.distanceMiles] ?? null,
      };
    });

    const venueIds = [
      ...new Set(fitRows.map((r) => r.venueId).filter(Boolean)),
    ];
    const signals = [];
    for (const vid of venueIds) {
      const sigs = await listTable(
        PE_SIGNALS_TABLE_NAME,
        `{${MAP_PE_SIGNAL.venueId}}='${vid}'`,
        50
      );
      for (const s of sigs) {
        const fld = s.fields || {};
        signals.push({
          airtableRecordId: s.id,
          signalId: fld[MAP_PE_SIGNAL.signalId] || null,
          venueId: fld[MAP_PE_SIGNAL.venueId] || null,
          eventName: fld[MAP_PE_SIGNAL.eventName] || null,
        });
      }
    }

    const opps = await listOpportunitiesForHotel(h.id, { maxRecords: 120 });
    const peOpps = opps.filter(
      (o) =>
        o.peVenueId ||
        o.opportunityType === "VENUE_PARTNERSHIP" ||
        INTEREST.test(
          `${o.title || ""} ${o.organizationName || ""} ${o.summaryWhat || ""}`
        )
    );

    report.hotels[h.id] = {
      name: h.name,
      fitCount: fitRows.length,
      fits: fitRows,
      signalCount: signals.length,
      signals,
      opportunitiesTotal: opps.length,
      peOpportunityCount: peOpps.length,
      peOpportunities: peOpps.map((o) => ({
        id: o.id,
        title: o.title,
        opportunityType: o.opportunityType,
        peVenueId: o.peVenueId || null,
        peSignalId: o.peSignalId || null,
        hotelVenueFitId: o.hotelVenueFitId || null,
        airtableRecordId: o._airtableRecordId || null,
        customerVisible: o.customerVisible,
        prospectHotelFacing: o.prospectHotelFacing,
        missingLinks: {
          peVenueId: !o.peVenueId,
          hotelVenueFitId: !o.hotelVenueFitId,
          peSignalId: !o.peSignalId,
        },
      })),
    };
  }

  const outPath = path.join(OUT, "AIRTABLE_BASELINE.json");
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2) + "\n");
  console.log(JSON.stringify({ outPath, venuesTotal: report.venuesTotal, hotels: Object.fromEntries(Object.entries(report.hotels).map(([id, h]) => [id, { name: h.name, fits: h.fitCount, signals: h.signalCount, peOpps: h.peOpportunityCount, peOppIds: h.peOpportunities.map((o) => o.id) }])) }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
