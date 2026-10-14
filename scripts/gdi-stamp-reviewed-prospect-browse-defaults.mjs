/**
 * In-place stamp of honest browse defaults on existing REVIEWED_PACKAGE_V1 rows.
 * Does NOT create records, re-import packages, or invent Ready/HIGH_PRIORITY.
 *
 *   node scripts/gdi-stamp-reviewed-prospect-browse-defaults.mjs --hotel rec0eTEnCqlsGJJt9 --dry-run
 *   node scripts/gdi-stamp-reviewed-prospect-browse-defaults.mjs --both --apply
 */
import "dotenv/config";
import {
  loadOpportunitiesCanonical,
  saveOpportunitiesCanonical,
  getGdiOpportunityPersistenceMode,
} from "../lib/group-demand-intelligence/opportunity-persistence.js";
import {
  REVIEWED_PROSPECT_PUBLICATION_BASIS,
  REVIEWED_PROSPECT_HOTEL_IDS,
} from "../lib/group-demand-intelligence/reviewed-prospect-publication-v1.js";
import { PRIORITY, BOOKING_WINDOW } from "../lib/group-demand-intelligence/claim-types.js";
import { invalidateGdiHotelReadCache } from "../lib/group-demand-intelligence/read-cache.js";
import { clearGdiRepositoryReadCache } from "../lib/group-demand-intelligence/repository.js";

function arg(name) {
  const i = process.argv.indexOf(name);
  if (i === -1) return null;
  const next = process.argv[i + 1];
  if (!next || next.startsWith("--")) return true;
  return next;
}

const dryRun = !process.argv.includes("--apply");
const both = process.argv.includes("--both");
const hotelArg = arg("--hotel");
const adapter = String(arg("--adapter") || "")
  .trim()
  .toLowerCase();
if (adapter === "filesystem" || adapter === "fs") {
  process.env.GDI_OPPORTUNITIES_PERSISTENCE = "filesystem";
} else if (adapter === "airtable" || adapter === "at" || !adapter) {
  process.env.GDI_OPPORTUNITIES_PERSISTENCE = "airtable";
}
const hotels = both
  ? [...REVIEWED_PROSPECT_HOTEL_IDS]
  : hotelArg && hotelArg !== true
    ? [String(hotelArg).trim()]
    : [];

if (!hotels.length) {
  console.error(
    "Usage: --hotel <id> | --both  [--adapter airtable] [--dry-run|--apply]"
  );
  process.exit(1);
}

async function stampHotel(hotelId) {
  const doc = await loadOpportunitiesCanonical(hotelId);
  const ops = Array.isArray(doc?.opportunities) ? doc.opportunities : [];
  let changed = 0;
  const next = ops.map((o) => {
    if (!o || typeof o !== "object") return o;
    if (String(o.prospectPublicationBasis || "") !== REVIEWED_PROSPECT_PUBLICATION_BASIS) {
      return o;
    }
    if (o.prospectHotelFacing !== true) return o;
    const patch = { ...o };
    let touched = false;
    if (!patch.priority) {
      patch.priority = PRIORITY.WATCHLIST;
      touched = true;
    }
    if (!patch.bookingWindowStatus) {
      patch.bookingWindowStatus = BOOKING_WINDOW.WATCH;
      touched = true;
    }
    if (!patch.hotelId) {
      patch.hotelId = hotelId;
      touched = true;
    }
    if (touched) {
      patch.updatedAt = new Date().toISOString();
      changed += 1;
      return patch;
    }
    return o;
  });

  const result = {
    hotelId,
    persistenceMode: getGdiOpportunityPersistenceMode(),
    total: ops.length,
    reviewedFacing: ops.filter(
      (o) =>
        o?.prospectPublicationBasis === REVIEWED_PROSPECT_PUBLICATION_BASIS &&
        o?.prospectHotelFacing === true
    ).length,
    stamped: changed,
    dryRun,
  };

  if (!dryRun && changed > 0) {
    await saveOpportunitiesCanonical(hotelId, {
      ...doc,
      hotelId,
      opportunities: next,
    });
    clearGdiRepositoryReadCache();
    invalidateGdiHotelReadCache(hotelId);
  }
  return result;
}

const out = [];
for (const hotelId of hotels) {
  out.push(await stampHotel(hotelId));
}
console.log(JSON.stringify({ ok: true, dryRun, results: out }, null, 2));
if (dryRun) {
  console.error("Dry-run only. Re-run with --apply to write.");
}
