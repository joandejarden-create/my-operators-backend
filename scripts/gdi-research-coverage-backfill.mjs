/**
 * Backfill GDI Research Targets from existing DG / PE intelligence (Bethesda default).
 * Does NOT fabricate historical research runs.
 *
 * Usage:
 *   node scripts/gdi-research-coverage-backfill.mjs --dry-run
 *   node scripts/gdi-research-coverage-backfill.mjs --apply --hotel recLuxvwwxID7U2B8
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  getDgBase,
  isDgAirtableConfigured,
} from "../lib/group-demand-intelligence/demand-generators/airtable-client.js";
import {
  DEMAND_GENERATORS_TABLE_NAME,
  DEMAND_PROGRAMS_TABLE_NAME,
  HOTEL_DEMAND_GENERATOR_FIT_TABLE_NAME,
  MAP_DEMAND_GENERATOR,
  MAP_DEMAND_PROGRAM,
  MAP_HOTEL_GENERATOR_FIT,
} from "../lib/group-demand-intelligence/demand-generators/airtable-field-map.js";
import {
  listVenuesFromAirtable,
} from "../lib/group-demand-intelligence/private-events/airtable-venue-store.js";
import {
  HOTEL_VENUE_FIT_TABLE_NAME,
  MAP_HOTEL_VENUE_FIT,
} from "../lib/group-demand-intelligence/private-events/airtable-field-map.js";
import {
  buildTargetsFromExistingIntelligence,
  upsertResearchTarget,
  isResearchCoverageAirtableConfigured,
  TARGET_TYPE,
} from "../lib/group-demand-intelligence/research-coverage/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const DRY = !APPLY;

const HOTEL_DEFAULT = "recLuxvwwxID7U2B8";
const HOTEL_NAMES = {
  recLuxvwwxID7U2B8: "Bethesda Marriott",
  recG66DQJKP2c0UNh: "Renaissance New York Times Square Hotel",
  recIwaP1etgx2g9nA: "Cambridge Beaches Resort & Spa",
};

function argVal(flag) {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] : null;
}

async function listTable(tableName, mapFn) {
  const base = getDgBase();
  const out = [];
  await base(tableName)
    .select({ pageSize: 100 })
    .eachPage((records, next) => {
      for (const r of records) out.push(mapFn(r));
      next();
    });
  return out;
}

function mapGenerator(r) {
  const f = r.fields || {};
  const G = MAP_DEMAND_GENERATOR;
  return {
    demandGeneratorId: f[G.demandGeneratorId],
    organizationName: f[G.organizationName],
    organizationType: f[G.organizationType],
    officialDomain: f[G.officialDomain],
    website: f[G.website],
    sourceUrls: String(f[G.sourceUrls] || "")
      .split("\n")
      .map((s) => s.trim())
      .filter(Boolean),
    firstSeenAt: f[G.firstSeenAt],
    confidence: f[G.confidence],
    airtableRecordId: r.id,
  };
}

function mapProgram(r) {
  const f = r.fields || {};
  const P = MAP_DEMAND_PROGRAM;
  return {
    programId: f[P.programId],
    seriesId: f[P.seriesId],
    demandGeneratorId: f[P.demandGeneratorId],
    programName: f[P.programName],
    programType: f[P.programType],
    sourceUrls: String(f[P.sourceUrls] || "")
      .split("\n")
      .map((s) => s.trim())
      .filter(Boolean),
    firstSeenAt: f[P.firstSeenAt],
    airtableRecordId: r.id,
  };
}

function mapFit(r) {
  const f = r.fields || {};
  const F = MAP_HOTEL_GENERATOR_FIT;
  return {
    fitId: f[F.fitId],
    hotelId: f[F.hotelId],
    demandGeneratorId: f[F.demandGeneratorId],
    generatorPriority: f[F.generatorPriority],
    fitRationale: f[F.fitRationale],
    airtableRecordId: r.id,
  };
}

function mapVenueFit(r) {
  const f = r.fields || {};
  const F = MAP_HOTEL_VENUE_FIT;
  return {
    fitId: f[F.fitId],
    hotelId: f[F.hotelId],
    venueId: f[F.venueId],
    peVenueId: f[F.venueId],
    airtableRecordId: r.id,
  };
}

async function main() {
  const hotelId = argVal("--hotel") || HOTEL_DEFAULT;
  const hotelName = HOTEL_NAMES[hotelId] || hotelId;
  const outDir = path.join(
    ROOT,
    "reports",
    "group-demand-intelligence",
    "research-coverage-v1"
  );
  fs.mkdirSync(outDir, { recursive: true });

  if (!isDgAirtableConfigured()) {
    throw new Error("Demand Generator Airtable not configured");
  }
  if (APPLY && !isResearchCoverageAirtableConfigured()) {
    throw new Error("Research coverage Airtable not configured");
  }

  const [generators, programs, fits, venues, venueFits] = await Promise.all([
    listTable(DEMAND_GENERATORS_TABLE_NAME, mapGenerator),
    listTable(DEMAND_PROGRAMS_TABLE_NAME, mapProgram),
    listTable(HOTEL_DEMAND_GENERATOR_FIT_TABLE_NAME, mapFit),
    listVenuesFromAirtable({ maxRecords: 500 }).catch(() => []),
    listTable(HOTEL_VENUE_FIT_TABLE_NAME, mapVenueFit).catch(() => []),
  ]);

  const hotelFits = fits.filter((f) => String(f.hotelId) === String(hotelId));
  const pack = buildTargetsFromExistingIntelligence({
    hotelId,
    hotelName,
    generators: generators.filter((g) => g.demandGeneratorId),
    programs: programs.filter((p) => p.programId),
    fits: hotelFits,
    venues: Array.isArray(venues) ? venues : [],
    venueFits: Array.isArray(venueFits) ? venueFits : [],
  });

  const results = [];
  for (const t of pack.targets) {
    const links = {
      generatorRecordId:
        t.targetType === TARGET_TYPE.DEMAND_GENERATOR
          ? generators.find((g) => g.demandGeneratorId === t.entityId)?.airtableRecordId
          : generators.find((g) => g.demandGeneratorId === t.demandGeneratorId)?.airtableRecordId,
      programRecordId:
        t.targetType === TARGET_TYPE.PROGRAM
          ? programs.find((p) => p.programId === t.programId)?.airtableRecordId
          : undefined,
      venueRecordId:
        t.targetType === TARGET_TYPE.PRIVATE_EVENT_VENUE
          ? (Array.isArray(venues) ? venues : []).find(
              (v) => (v.venueId || v.peVenueId) === t.venueId
            )?.airtableRecordId ||
            (Array.isArray(venues) ? venues : []).find(
              (v) => (v.venueId || v.peVenueId) === t.venueId
            )?.id
          : undefined,
    };
    const upserted = await upsertResearchTarget(t, { dryRun: DRY, links });
    results.push({
      targetId: t.targetId,
      targetType: t.targetType,
      priority: t.priority,
      action: upserted.action,
      recordId: upserted.recordId || null,
    });
  }

  const report = {
    generatedAt: new Date().toISOString(),
    apply: APPLY,
    hotelId,
    hotelName,
    sourceCounts: {
      generators: generators.length,
      programs: programs.length,
      hotelFits: hotelFits.length,
      venues: Array.isArray(venues) ? venues.length : 0,
      venueFits: Array.isArray(venueFits) ? venueFits.length : 0,
    },
    totals: pack.totals,
    results,
    note: "No historical research runs fabricated.",
  };

  const outPath = path.join(outDir, `BACKFILL_${hotelId}${DRY ? "_DRY" : ""}.json`);
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log(JSON.stringify({ ok: true, outPath, totals: pack.totals, apply: APPLY }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
