#!/usr/bin/env node
/**
 * Local GDI visibility + Airtable persistence diagnosis (read-only for Airtable).
 * Writes evidence under reports/gdi-local-storage-parity/
 *
 *   node scripts/gdi-local-storage-parity-diagnose.mjs
 *   node scripts/gdi-local-storage-parity-diagnose.mjs --ports 8080,8091,8092
 *   node scripts/gdi-local-storage-parity-diagnose.mjs --airtable
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "reports/gdi-local-storage-parity");
fs.mkdirSync(OUT, { recursive: true });

const MAY = "rec0eTEnCqlsGJJt9";
const BETH = "recLuxvwwxID7U2B8";

function arg(name, fallback = null) {
  const i = process.argv.indexOf(name);
  if (i < 0) return fallback;
  return process.argv[i + 1] || fallback;
}
function has(flag) {
  return process.argv.includes(flag);
}

const ports = String(arg("--ports", "8080,8091,8092"))
  .split(",")
  .map((s) => Number(s.trim()))
  .filter(Boolean);

function write(name, data) {
  const p = path.join(OUT, name);
  fs.writeFileSync(
    p,
    typeof data === "string" ? data : JSON.stringify(data, null, 2) + "\n"
  );
  return p;
}

function redactEnv() {
  return {
    PORT: process.env.PORT || null,
    NODE_ENV: process.env.NODE_ENV || null,
    GDI_OPPORTUNITIES_PERSISTENCE: process.env.GDI_OPPORTUNITIES_PERSISTENCE || "(unset)",
    GDI_DATA_ROOT: process.env.GDI_DATA_ROOT || "(unset → default data/group-demand-intelligence)",
    GDI_REVIEWED_PROSPECT_PUBLICATION_V1:
      process.env.GDI_REVIEWED_PROSPECT_PUBLICATION_V1 || "(unset → local default ON)",
    GDI_REVIEWED_PROSPECT_HOTEL_IDS:
      process.env.GDI_REVIEWED_PROSPECT_HOTEL_IDS ||
      "(unset → built-in Bethesda+Mayflower)",
    GROUP_DEMAND_INTELLIGENCE_V1: process.env.GROUP_DEMAND_INTELLIGENCE_V1 || "(unset)",
    GROUP_DEMAND_INTELLIGENCE_PILOT_READ:
      process.env.GROUP_DEMAND_INTELLIGENCE_PILOT_READ || "(unset → local auto)",
    GDI_OPPORTUNITIES_AIRTABLE_BASE_ID:
      process.env.GDI_OPPORTUNITIES_AIRTABLE_BASE_ID ||
      process.env.AIRTABLE_GDI_BASE_ID ||
      "(from lib default)",
    AIRTABLE_TOKEN_PRESENT: Boolean(
      process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT
    ),
    RAILWAY_ENVIRONMENT: process.env.RAILWAY_ENVIRONMENT || "(unset)",
  };
}

function fsStats(filePath, label) {
  if (!fs.existsSync(filePath)) {
    return { label, exists: false };
  }
  const doc = JSON.parse(fs.readFileSync(filePath, "utf8"));
  const ops = doc.opportunities || [];
  const reviewed = ops.filter(
    (o) => o.prospectPublicationBasis === "REVIEWED_PACKAGE_V1"
  );
  const facing = reviewed.filter((o) => o.prospectHotelFacing === true);
  const priority = facing.filter(
    (o) => String(o.prospectReviewSection || "").toUpperCase() !== "EXPLORATORY"
  );
  const exploratory = facing.filter(
    (o) => String(o.prospectReviewSection || "").toUpperCase() === "EXPLORATORY"
  );
  const watchlistPri = facing.filter(
    (o) => String(o.priority || "").toUpperCase() === "WATCHLIST"
  );
  return {
    label,
    exists: true,
    path: filePath,
    total: ops.length,
    reviewed: reviewed.length,
    facingReviewed: facing.length,
    prioritySection: priority.length,
    exploratorySection: exploratory.length,
    facingWithWatchlistPriority: watchlistPri.length,
    importAt: doc.reviewedProspectImport?.importedAt || null,
    sha256: createHash("sha256").update(fs.readFileSync(filePath)).digest("hex"),
    facingIds: facing.map((o) => o.id || o.opportunityId).sort(),
    priorityIds: priority.map((o) => o.id || o.opportunityId).sort(),
    exploratoryIds: exploratory.map((o) => o.id || o.opportunityId).sort(),
  };
}

async function probePort(port) {
  const base = `http://127.0.0.1:${port}`;
  const out = { port, base, ok: false };
  try {
    const hotels = await fetch(`${base}/api/group-demand-intelligence/hotels`).then(
      (r) => r.json()
    );
    out.hotelCount = hotels.hotels?.length ?? hotels?.length ?? null;
  } catch (err) {
    out.error = String(err?.message || err);
    return out;
  }
  out.ok = true;
  out.hotels = {};
  for (const hotelId of [MAY, BETH]) {
    try {
      const summary = await fetch(
        `${base}/api/group-demand-intelligence/hotels/${hotelId}/summary`
      ).then((r) => r.json());
      const list = await fetch(
        `${base}/api/group-demand-intelligence/hotels/${hotelId}/opportunities`
      ).then((r) => r.json());
      const full = await fetch(
        `${base}/api/group-demand-intelligence/hotels/${hotelId}/opportunities?view=full`
      ).then((r) => r.json());
      const ops = full.opportunities || [];
      const reviewed = ops.filter(
        (o) => o.prospectPublicationBasis === "REVIEWED_PACKAGE_V1"
      );
      const priority = reviewed.filter(
        (o) => String(o.prospectReviewSection || "").toUpperCase() !== "EXPLORATORY"
      );
      const exploratory = reviewed.filter(
        (o) => String(o.prospectReviewSection || "").toUpperCase() === "EXPLORATORY"
      );
      out.hotels[hotelId] = {
        listCount: list.count,
        fullCount: full.count,
        reviewedInFull: reviewed.length,
        prioritySection: priority.length,
        exploratorySection: exploratory.length,
        visibilityWarning: list.visibilityWarning || null,
        reviewedProspectImport: list.reviewedProspectImport || null,
        flagHasReviewedKey: Object.prototype.hasOwnProperty.call(
          summary.flag || {},
          "reviewedProspectPublicationV1"
        ),
        reviewedProspectPublicationV1:
          summary.flag?.reviewedProspectPublicationV1 ?? null,
        pilotReadAllowed: summary.flag?.pilotReadAllowed ?? null,
        localDevAutoPilotRead: summary.flag?.localDevAutoPilotRead ?? null,
        summaryDiscovered: summary.summary?.opportunitiesDiscovered ?? null,
        runStatus: summary.summary?.runStatus ?? null,
        lastResearchAt: summary.summary?.lastResearchAt ?? null,
        sample: ops[0]
          ? {
              id: ops[0].id,
              priority: ops[0].priority,
              bookingWindowStatus: ops[0].bookingWindowStatus,
              prospectPublicationBasis: ops[0].prospectPublicationBasis,
              prospectReviewSection: ops[0].prospectReviewSection,
              customerFacingLabel: ops[0].customerFacingLabel,
              primaryContactName:
                ops[0].primaryContact?.name || ops[0].primaryContactName,
              sourcingStatus: ops[0].sourcingStatus || ops[0].venueSourcingStatus,
            }
          : null,
        ids: ops.map((o) => o.id).sort(),
        reviewedIds: reviewed.map((o) => o.id).sort(),
      };
    } catch (err) {
      out.hotels[hotelId] = { error: String(err?.message || err) };
    }
  }
  return out;
}

async function readAirtableReviewed() {
  const { listOpportunitiesForHotel } = await import(
    "../lib/group-demand-intelligence/airtable-opportunity-store.js"
  );
  const { isGdiOpportunityAirtableConfigured, getGdiOpportunitiesAirtableBaseId } =
    await import("../lib/group-demand-intelligence/airtable-opportunity-store.js");
  const { GDI_OPPORTUNITIES_TABLE_NAME } = await import(
    "../lib/group-demand-intelligence/opportunity-field-map.js"
  );
  if (!isGdiOpportunityAirtableConfigured()) {
    return { ok: false, reason: "airtable_not_configured" };
  }
  const result = {
    ok: true,
    baseId: getGdiOpportunitiesAirtableBaseId(),
    tableName: GDI_OPPORTUNITIES_TABLE_NAME,
    hotels: {},
  };
  for (const hotelId of [MAY, BETH]) {
    const ops = await listOpportunitiesForHotel(hotelId);
    const reviewed = ops.filter(
      (o) => o.prospectPublicationBasis === "REVIEWED_PACKAGE_V1"
    );
    const facing = reviewed.filter((o) => o.prospectHotelFacing === true);
    const priority = facing.filter(
      (o) => String(o.prospectReviewSection || "").toUpperCase() !== "EXPLORATORY"
    );
    const exploratory = facing.filter(
      (o) => String(o.prospectReviewSection || "").toUpperCase() === "EXPLORATORY"
    );
    const readyLike = ops.filter(
      (o) =>
        o.prospectPublicationBasis !== "REVIEWED_PACKAGE_V1" &&
        (o.opportunityQualification === "TRUE_ACTIONABLE" ||
          o.maturityState === "ACTIONABLE" ||
          o.customerFacingState === "READY" ||
          o.priority === "HIGH_PRIORITY" ||
          o.priority === "MEDIUM_PRIORITY")
    );
    result.hotels[hotelId] = {
      total: ops.length,
      reviewed: reviewed.length,
      facingReviewed: facing.length,
      prioritySection: priority.length,
      exploratorySection: exploratory.length,
      nonReviewedReadyLikeApprox: readyLike.length,
      facingIds: facing.map((o) => o.id || o.opportunityId).sort(),
      priorityIds: priority.map((o) => o.id || o.opportunityId).sort(),
      exploratoryIds: exploratory.map((o) => o.id || o.opportunityId).sort(),
      records: facing.map((o) => ({
        opportunityId: o.id || o.opportunityId,
        airtableRecordId: o._airtableRecordId || o.airtableRecordId || null,
        hotelId: o.hotelId,
        organizationName: o.organizationName,
        title: o.title || o.opportunityName,
        priority: o.priority,
        bookingWindowStatus: o.bookingWindowStatus,
        prospectReviewSection: o.prospectReviewSection,
        prospectPublicationBasis: o.prospectPublicationBasis,
        contactName: o.primaryContact?.name || o.primaryContactName,
        contactEmail: o.primaryContact?.email || o.primaryContactEmail,
        sourcingStatus: o.sourcingStatus || o.venueSourcingStatus,
        payloadEmbedded: Boolean(o.opportunityPayloadJson || o.primaryContact || o.sources),
        peVenueId: o.peVenueId || null,
        peSignalId: o.peSignalId || null,
        hotelVenueFitId: o.hotelVenueFitId || null,
      })),
    };
  }
  return result;
}

const report = {
  asOf: new Date().toISOString(),
  repo: ROOT,
  diagnoseEnvSnapshot: redactEnv(),
  note: "diagnoseEnvSnapshot is this script's process env, not necessarily each listening server's env",
  filesystemStores: {
    defaultMay: fsStats(
      path.join(ROOT, "data/group-demand-intelligence/hotels", MAY, "opportunities.json"),
      "default_fs_mayflower"
    ),
    defaultBeth: fsStats(
      path.join(ROOT, "data/group-demand-intelligence/hotels", BETH, "opportunities.json"),
      "default_fs_bethesda"
    ),
    isolatedMay: fsStats(
      path.join(
        ROOT,
        "reports/gdi-prospect-publication-implementation/_local-gdi-data/hotels",
        MAY,
        "opportunities.json"
      ),
      "isolated_fs_mayflower"
    ),
    isolatedBeth: fsStats(
      path.join(
        ROOT,
        "reports/gdi-prospect-publication-implementation/_local-gdi-data/hotels",
        BETH,
        "opportunities.json"
      ),
      "isolated_fs_bethesda"
    ),
  },
  ports: [],
  airtable: null,
};

for (const port of ports) {
  // eslint-disable-next-line no-await-in-loop
  report.ports.push(await probePort(port));
}

if (has("--airtable")) {
  try {
    report.airtable = await readAirtableReviewed();
  } catch (err) {
    report.airtable = { ok: false, error: String(err?.message || err) };
  }
}

// Root-cause heuristics
const p8080 = report.ports.find((p) => p.port === 8080);
const p8091 = report.ports.find((p) => p.port === 8091);
report.rootCauseHeuristics = {
  port8080_missingReviewedPublicationCode:
    p8080?.ok &&
    p8080.hotels?.[MAY]?.flagHasReviewedKey === false &&
    p8080.hotels?.[MAY]?.listCount === 0,
  port8091_matchesExpectedReviewedCounts:
    p8091?.ok &&
    p8091.hotels?.[MAY]?.listCount === 11 &&
    p8091.hotels?.[BETH]?.listCount === 32,
  expectedPackage: {
    bethesda: { priority: 11, exploratory: 2, facingReviewed: 13 },
    mayflower: { priority: 9, exploratory: 2, facingReviewed: 11 },
  },
  interpretation: [
    "If 8080 lacks reviewedProspectPublicationV1 flag key → stale server build (pre-reviewed-publication modules).",
    "If 8091 shows 11/32 with flag true → isolated GDI_DATA_ROOT + current code is healthy.",
    "Production SoT is Airtable opportunityPayloadJson; local FS can mirror but must run current code + publication flag.",
  ],
};

write("runtime-diagnosis.json", report);
console.log(
  JSON.stringify(
    {
      ok: true,
      out: OUT,
      rootCauseHeuristics: report.rootCauseHeuristics,
      ports: report.ports.map((p) => ({
        port: p.port,
        ok: p.ok,
        may: p.hotels?.[MAY]?.listCount,
        beth: p.hotels?.[BETH]?.listCount,
        reviewedFlag: p.hotels?.[MAY]?.reviewedProspectPublicationV1,
        flagKey: p.hotels?.[MAY]?.flagHasReviewedKey,
      })),
      airtable: report.airtable
        ? {
            ok: report.airtable.ok,
            mayFacing: report.airtable.hotels?.[MAY]?.facingReviewed,
            bethFacing: report.airtable.hotels?.[BETH]?.facingReviewed,
          }
        : "skipped (pass --airtable)",
    },
    null,
    2
  )
);
