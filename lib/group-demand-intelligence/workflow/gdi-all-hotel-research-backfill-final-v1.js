/**
 * Final all-hotel research inventory backfill V1.
 * Research table only. Fail-closed outside exact allowlist authorization.
 */

import fs from "node:fs";
import path from "node:path";
import {
  buildHotelResearchCompositeKey,
  GDI_HOTEL_RESEARCH_OPPORTUNITIES_TABLE_NAME,
} from "../hotel-research-opportunity-field-map.js";
import {
  buildResearchInventoryFields,
  upsertResearchInventoryRecord,
  readBackResearchInventory,
  listResearchInventoryForHotel,
} from "./gdi-hotel-research-inventory-store-v1.js";
import {
  discoverLocalHotelUniverse,
  loadLocalOpportunities,
  loadYotelForbiddenRecreateIds,
  publicationState,
} from "./gdi-complete-research-persistence-v1.js";
import { YOTEL_LEGACY_DELETE_AUTH } from "./gdi-yotel-legacy-gdo-deletion-v1.js";
import { INITIAL_BACKFILL_HOTELS } from "./gdi-yotel-guadalajara-research-backfill-v1.js";

export const GDI_ALL_HOTEL_RESEARCH_BACKFILL_FINAL_V1 =
  "gdi-all-hotel-research-backfill-final-v1";

export const ALL_HOTEL_RESEARCH_BACKFILL_EXACT_WRITE_SET_AUTHORIZATION =
  "gdi-all-hotel-research-inventory-remaining-v1";

const ALIAS_DUPES = Object.freeze(
  new Set([
    "gdi_hotel_renaissance_times_square",
    "gdi_hotel_waterstone_boca_raton",
  ]),
);

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

export function listCanonicalResearchHotels(repoRoot) {
  const hotels = [];
  for (const h of discoverLocalHotelUniverse(repoRoot)) {
    if (ALIAS_DUPES.has(h.hotelId)) continue;
    if (h.hotelId.startsWith("gdi_hotel_")) continue;
    const fsOpp = loadLocalOpportunities(repoRoot, h.hotelId);
    if (!fsOpp.present || !fsOpp.count) continue;
    hotels.push({
      hotelId: h.hotelId,
      hotelName: fsOpp.hotelName || h.hotelId,
      filesystemCount: fsOpp.count,
      rows: fsOpp.rows,
      byId: fsOpp.byId,
    });
  }
  return hotels.sort((a, b) => a.hotelId.localeCompare(b.hotelId));
}

export function buildAllHotelResearchAllowlist(repoRoot) {
  const forbidden = loadYotelForbiddenRecreateIds(repoRoot);
  const hotels = listCanonicalResearchHotels(repoRoot);
  const entries = [];
  const errors = [];

  for (const hotel of hotels) {
    for (const row of hotel.rows) {
      if (
        hotel.hotelId === YOTEL_LEGACY_DELETE_AUTH.hotelId &&
        forbidden.has(row.opportunityId)
      ) {
        errors.push({
          hotelId: hotel.hotelId,
          opportunityId: row.opportunityId,
          code: "FORBIDDEN_YOTEL_LEGACY_ID",
        });
        continue;
      }
      const built = buildResearchInventoryFields(row, hotel);
      if (!built.ok) {
        errors.push({
          hotelId: hotel.hotelId,
          opportunityId: row.opportunityId,
          code: built.code,
          detail: built.detail || null,
        });
        continue;
      }
      entries.push({
        hotelId: hotel.hotelId,
        hotelName: hotel.hotelName,
        opportunityId: row.opportunityId,
        compositeKey: built.compositeKey,
        organizationName: built.fields.organizationName || null,
        title: built.fields.title || null,
        destinationTable: GDI_HOTEL_RESEARCH_OPPORTUNITIES_TABLE_NAME,
        payloadLength: built.payloadLength,
        gdoWrite: false,
        publicationWrite: false,
      });
    }
  }

  const byHotel = {};
  for (const e of entries) {
    byHotel[e.hotelId] = (byHotel[e.hotelId] || 0) + 1;
  }

  const already = INITIAL_BACKFILL_HOTELS.map((h) => h.hotelId);
  const remaining = entries.filter((e) => !already.includes(e.hotelId));

  return {
    version: GDI_ALL_HOTEL_RESEARCH_BACKFILL_FINAL_V1,
    authorizationId: ALL_HOTEL_RESEARCH_BACKFILL_EXACT_WRITE_SET_AUTHORIZATION,
    tableId: "tblbkDj0vhQLfrssV",
    baseId: "appa2cE7FTRmIbB32",
    hotelCount: hotels.length,
    totalResearch: entries.length,
    expectedBaseline: 659,
    alreadyPersistedHotels: already,
    alreadyPersistedExpected: 85,
    remainingCount: remaining.length,
    remainingExpected: 574,
    countsMatchExpected:
      entries.length === 659 && remaining.length === 574 && errors.length === 0,
    byHotel,
    errors,
    entries,
    remainingEntries: remaining,
    safeguards: {
      researchTableOnly: true,
      noGdoWrites: true,
      noPublicationWrites: true,
      yotelLegacyNonRecreation: true,
      forbiddenLegacyIdsLoaded: forbidden.size,
    },
  };
}

export function validateAllHotelBackfillAuthorization(input = {}) {
  const authorizeProductionWrites = input.authorizeProductionWrites === true;
  const exactWriteSetAuthorization = text(input.exactWriteSetAuthorization);
  const apply = input.apply === true;
  if (!authorizeProductionWrites) {
    return { ok: false, code: "MISSING_AUTHORIZE_PRODUCTION_WRITES" };
  }
  if (
    exactWriteSetAuthorization !==
    ALL_HOTEL_RESEARCH_BACKFILL_EXACT_WRITE_SET_AUTHORIZATION
  ) {
    return {
      ok: false,
      code: "INVALID_EXACT_WRITE_SET_AUTHORIZATION",
      expected: ALL_HOTEL_RESEARCH_BACKFILL_EXACT_WRITE_SET_AUTHORIZATION,
    };
  }
  if (!apply) return { ok: true, apply: false, code: "DRY_RUN_ONLY" };
  return {
    ok: true,
    apply: true,
    code: "AUTHORIZED_REMAINING_RESEARCH_INVENTORY_BACKFILL",
  };
}

/**
 * @param {object} opts
 * @param {'remaining'|'all'|'initial85'} [opts.scope]
 */
export async function executeAllHotelResearchBackfill({
  repoRoot,
  scope = "remaining",
  apply = false,
  authorizeProductionWrites = false,
  exactWriteSetAuthorization = "",
  sleepMs = 220,
} = {}) {
  const allowlist = buildAllHotelResearchAllowlist(repoRoot);
  if (!allowlist.countsMatchExpected) {
    return {
      ok: false,
      blocked: true,
      code: "ALLOWLIST_COUNT_MISMATCH",
      detail: {
        total: allowlist.totalResearch,
        remaining: allowlist.remainingCount,
        errors: allowlist.errors,
      },
      allowlist,
      results: [],
    };
  }

  let work = allowlist.entries;
  if (scope === "remaining") work = allowlist.remainingEntries;
  else if (scope === "initial85") {
    const ids = new Set(INITIAL_BACKFILL_HOTELS.map((h) => h.hotelId));
    work = allowlist.entries.filter((e) => ids.has(e.hotelId));
  }

  const auth = validateAllHotelBackfillAuthorization({
    authorizeProductionWrites,
    exactWriteSetAuthorization,
    apply,
  });
  // Idempotency verification (initial85 dry-run) does not require production auth
  const needsAuth = apply === true;
  if (needsAuth && !auth.ok) {
    return {
      ok: false,
      blocked: true,
      code: auth.code,
      allowlist,
      results: [],
    };
  }

  const liveApply = apply && auth.apply === true;
  const hotels = listCanonicalResearchHotels(repoRoot);
  const byHotel = Object.fromEntries(hotels.map((h) => [h.hotelId, h]));
  const results = [];

  for (const entry of work) {
    const hotel = byHotel[entry.hotelId];
    const opp = hotel?.byId.get(entry.opportunityId);
    if (!opp) {
      results.push({
        ...entry,
        ok: false,
        action: "FAILED",
        code: "SOURCE_ROW_MISSING",
      });
      continue;
    }
    const result = await upsertResearchInventoryRecord({
      opportunity: opp,
      hotel,
      dryRun: !liveApply,
    });
    const row = { ...entry, ...result };
    if (liveApply && result.ok && result.airtableRecordId && result.action !== "UNCHANGED") {
      try {
        const rb = await readBackResearchInventory(result.airtableRecordId);
        row.readBack = {
          ok:
            rb.compositeKey === entry.compositeKey &&
            rb.hotelId === entry.hotelId &&
            rb.opportunityId === entry.opportunityId,
          airtableRecordId: rb.id,
          payloadPresent: Boolean(rb.fields?.researchPayloadJson),
        };
        if (!row.readBack.ok) row.ok = false;
      } catch (err) {
        row.readBack = { ok: false, error: String(err?.message || err) };
        row.ok = false;
      }
    }
    results.push(row);
    if (sleepMs > 0 && liveApply) {
      await new Promise((r) => setTimeout(r, sleepMs));
    }
  }

  const tally = (action) => results.filter((r) => r.action === action).length;
  const failed = results.filter((r) => !r.ok);

  const postHotelCounts = {};
  if (liveApply || scope === "all" || scope === "initial85") {
    for (const h of hotels) {
      try {
        const rows = await listResearchInventoryForHotel(h.hotelId);
        postHotelCounts[h.hotelId] = rows.length;
      } catch (err) {
        postHotelCounts[h.hotelId] = { error: String(err?.message || err) };
      }
    }
  }

  return {
    ok: failed.length === 0,
    version: GDI_ALL_HOTEL_RESEARCH_BACKFILL_FINAL_V1,
    scope,
    liveApply,
    authorization: auth,
    allowlistSummary: {
      total: allowlist.totalResearch,
      remaining: allowlist.remainingCount,
      work: work.length,
      countsMatchExpected: allowlist.countsMatchExpected,
    },
    counts: {
      created: tally("CREATED"),
      updated: tally("UPDATED"),
      unchanged: tally("UNCHANGED"),
      wouldCreate: tally("WOULD_CREATE"),
      wouldUpdate: tally("WOULD_UPDATE"),
      failed: failed.length,
    },
    postHotelCounts,
    gdoWrites: 0,
    publicationWrites: 0,
    results,
  };
}

export async function reconcileAllHotelsPostApply(repoRoot) {
  const hotels = listCanonicalResearchHotels(repoRoot);
  const rows = [];
  for (const h of hotels) {
    let atCount = 0;
    let atError = null;
    let duplicates = 0;
    try {
      const at = await listResearchInventoryForHotel(h.hotelId);
      atCount = at.length;
      const keys = at.map((r) => r.fields?.compositeKey).filter(Boolean);
      duplicates = keys.length - new Set(keys).size;
    } catch (err) {
      atError = String(err?.message || err);
    }
    const pub = publicationState(repoRoot, h.hotelId);
    rows.push({
      hotelId: h.hotelId,
      hotelName: h.hotelName,
      filesystemCount: h.filesystemCount,
      airtableResearchCount: atCount,
      missing: Math.max(0, h.filesystemCount - (atCount || 0)),
      extra: Math.max(0, (atCount || 0) - h.filesystemCount),
      duplicates,
      airtableError: atError,
      publicationCurrent: Boolean(pub.current),
    });
  }
  const fsTotal = rows.reduce((n, r) => n + r.filesystemCount, 0);
  const atTotal = rows.reduce(
    (n, r) => n + (typeof r.airtableResearchCount === "number" ? r.airtableResearchCount : 0),
    0,
  );
  return {
    hotelCount: rows.length,
    filesystemTotal: fsTotal,
    airtableResearchTotal: atTotal,
    complete: fsTotal === atTotal && rows.every((r) => r.missing === 0 && r.duplicates === 0 && !r.airtableError),
    rows,
  };
}

export function compositeKey(hotelId, opportunityId) {
  return buildHotelResearchCompositeKey(hotelId, opportunityId);
}

export default {
  GDI_ALL_HOTEL_RESEARCH_BACKFILL_FINAL_V1,
  ALL_HOTEL_RESEARCH_BACKFILL_EXACT_WRITE_SET_AUTHORIZATION,
  listCanonicalResearchHotels,
  buildAllHotelResearchAllowlist,
  validateAllHotelBackfillAuthorization,
  executeAllHotelResearchBackfill,
  reconcileAllHotelsPostApply,
};
