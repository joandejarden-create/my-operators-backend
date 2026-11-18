/**
 * Controlled initial research-inventory backfill:
 * YOTEL (42) + Guadalajara (43) = 85 records.
 * Research table only. No GDO. No publication.
 */

import fs from "node:fs";
import path from "node:path";
import {
  buildHotelResearchCompositeKey,
} from "../hotel-research-opportunity-field-map.js";
import {
  buildResearchInventoryFields,
  upsertResearchInventoryRecord,
  readBackResearchInventory,
  listResearchInventoryForHotel,
} from "./gdi-hotel-research-inventory-store-v1.js";
import { loadYotelForbiddenRecreateIds } from "./gdi-complete-research-persistence-v1.js";
import { YOTEL_LEGACY_DELETE_AUTH } from "./gdi-yotel-legacy-gdo-deletion-v1.js";

export const GDI_YOTEL_GUADALAJARA_RESEARCH_BACKFILL_V1 =
  "gdi-yotel-guadalajara-research-backfill-v1";

/** Frozen founder authorization for the exact 85-record research-table batch. */
export const YOTEL_GUADALAJARA_85_EXACT_WRITE_SET_AUTHORIZATION =
  "yotel-guadalajara-research-inventory-85-v1";

export const INITIAL_BACKFILL_HOTELS = Object.freeze([
  Object.freeze({
    key: "yotel",
    hotelId: "recrPQcZg7SFARRb2",
    hotelName: "YOTEL Geneva Lake",
    expectedCount: 42,
  }),
  Object.freeze({
    key: "guadalajara",
    hotelId: "recCAGqkERSXgiku4",
    hotelName: "Sheraton Guadalajara Expo",
    expectedCount: 43,
  }),
]);

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

export function loadHotelOpportunities(repoRoot, hotelId) {
  const p = path.join(
    repoRoot,
    "data/group-demand-intelligence/hotels",
    hotelId,
    "opportunities.json",
  );
  if (!fs.existsSync(p)) {
    return { present: false, path: p, rows: [], byId: new Map() };
  }
  const j = JSON.parse(fs.readFileSync(p, "utf8"));
  const arr = Array.isArray(j) ? j : j.opportunities || [];
  const byId = new Map();
  const rows = [];
  for (const o of arr) {
    const id = text(o.id || o.opportunityId);
    if (!id) continue;
    const row = { ...o, opportunityId: id, id, hotelId: text(o.hotelId) || hotelId };
    byId.set(id, row);
    rows.push(row);
  }
  return { present: true, path: p, rows, byId, hotelName: j.hotelName || null };
}

export function buildExact85Allowlist(repoRoot) {
  const forbidden = loadYotelForbiddenRecreateIds(repoRoot);
  const entries = [];
  const errors = [];

  for (const hotel of INITIAL_BACKFILL_HOTELS) {
    const fsOpp = loadHotelOpportunities(repoRoot, hotel.hotelId);
    if (!fsOpp.present) {
      errors.push({ hotelId: hotel.hotelId, code: "MISSING_OPPORTUNITIES_FILE" });
      continue;
    }
    if (fsOpp.rows.length !== hotel.expectedCount) {
      errors.push({
        hotelId: hotel.hotelId,
        code: "COUNT_MISMATCH",
        expected: hotel.expectedCount,
        actual: fsOpp.rows.length,
      });
    }
    for (const row of fsOpp.rows) {
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
        destinationTable: "GDI Hotel Research Opportunities",
        proposedAction: "CREATE_OR_UPDATE_RESEARCH_INVENTORY",
        fieldNames: Object.keys(built.fields),
        payloadLength: built.payloadLength,
        commerciallyQualified: built.commerciallyQualified === true,
        gdoWrite: false,
        publicationWrite: false,
      });
    }
  }

  const yotel = entries.filter((e) => e.hotelId === "recrPQcZg7SFARRb2");
  const guad = entries.filter((e) => e.hotelId === "recCAGqkERSXgiku4");
  const exactMatch =
    errors.length === 0 &&
    yotel.length === 42 &&
    guad.length === 43 &&
    entries.length === 85;

  return {
    version: GDI_YOTEL_GUADALAJARA_RESEARCH_BACKFILL_V1,
    authorizationId: YOTEL_GUADALAJARA_85_EXACT_WRITE_SET_AUTHORIZATION,
    exactMatch,
    expectedTotal: 85,
    actualTotal: entries.length,
    yotelCount: yotel.length,
    guadalajaraCount: guad.length,
    errors,
    entries,
    safeguards: {
      researchTableOnly: true,
      noGdoWrites: true,
      noPublicationWrites: true,
      yotelLegacyNonRecreation: true,
      forbiddenLegacyIdsLoaded: forbidden.size,
    },
  };
}

export function validateInitialBackfillAuthorization(input = {}) {
  const authorizeProductionWrites = input.authorizeProductionWrites === true;
  const exactWriteSetAuthorization = text(input.exactWriteSetAuthorization);
  const apply = input.apply === true;

  if (!authorizeProductionWrites) {
    return { ok: false, code: "MISSING_AUTHORIZE_PRODUCTION_WRITES" };
  }
  if (
    exactWriteSetAuthorization !==
    YOTEL_GUADALAJARA_85_EXACT_WRITE_SET_AUTHORIZATION
  ) {
    return {
      ok: false,
      code: "INVALID_EXACT_WRITE_SET_AUTHORIZATION",
      expected: YOTEL_GUADALAJARA_85_EXACT_WRITE_SET_AUTHORIZATION,
    };
  }
  if (!apply) {
    return { ok: true, apply: false, code: "DRY_RUN_ONLY" };
  }
  return {
    ok: true,
    apply: true,
    code: "AUTHORIZED_85_RESEARCH_INVENTORY_BATCH",
  };
}

/**
 * Apply or dry-run the exact 85 allowlist. Stops if allowlist is not exact.
 */
export async function executeYotelGuadalajaraResearchBackfill({
  repoRoot,
  apply = false,
  authorizeProductionWrites = false,
  exactWriteSetAuthorization = "",
  sleepMs = 220,
} = {}) {
  const allowlist = buildExact85Allowlist(repoRoot);
  if (!allowlist.exactMatch) {
    return {
      ok: false,
      code: "ALLOWLIST_NOT_EXACT_85",
      blocked: true,
      detail:
        "Proposed set differs from 42 YOTEL + 43 Guadalajara — stop for founder review",
      allowlist,
      results: [],
    };
  }

  const auth = validateInitialBackfillAuthorization({
    authorizeProductionWrites,
    exactWriteSetAuthorization,
    apply,
  });
  if (apply && !auth.ok) {
    return {
      ok: false,
      code: auth.code,
      blocked: true,
      allowlist,
      results: [],
    };
  }

  const liveApply = apply && auth.apply === true;
  const results = [];
  const byHotel = Object.fromEntries(
    INITIAL_BACKFILL_HOTELS.map((h) => [
      h.hotelId,
      loadHotelOpportunities(repoRoot, h.hotelId),
    ]),
  );

  for (const entry of allowlist.entries) {
    const opp = byHotel[entry.hotelId]?.byId.get(entry.opportunityId);
    if (!opp) {
      results.push({
        ...entry,
        ok: false,
        action: "FAILED",
        code: "SOURCE_ROW_MISSING",
      });
      continue;
    }
    const hotel = INITIAL_BACKFILL_HOTELS.find((h) => h.hotelId === entry.hotelId);
    const result = await upsertResearchInventoryRecord({
      opportunity: opp,
      hotel,
      dryRun: !liveApply,
    });
    results.push({ ...entry, ...result });

    if (liveApply && result.ok && result.airtableRecordId) {
      try {
        const rb = await readBackResearchInventory(result.airtableRecordId);
        const keyOk = rb.compositeKey === entry.compositeKey;
        const hotelOk = rb.hotelId === entry.hotelId;
        const oppOk = rb.opportunityId === entry.opportunityId;
        result.readBack = {
          ok: keyOk && hotelOk && oppOk,
          airtableRecordId: rb.id,
          compositeKey: rb.compositeKey,
          payloadPresent: Boolean(rb.fields?.researchPayloadJson),
        };
        if (!result.readBack.ok) result.ok = false;
      } catch (err) {
        result.readBack = {
          ok: false,
          error: String(err?.message || err),
        };
        result.ok = false;
      }
    }

    if (sleepMs > 0) await new Promise((r) => setTimeout(r, sleepMs));
  }

  const created = results.filter((r) => r.action === "CREATED").length;
  const updated = results.filter((r) => r.action === "UPDATED").length;
  const wouldCreate = results.filter((r) => r.action === "WOULD_CREATE").length;
  const wouldUpdate = results.filter((r) => r.action === "WOULD_UPDATE").length;
  const failed = results.filter((r) => !r.ok);

  let postHotelCounts = null;
  if (liveApply) {
    postHotelCounts = {};
    for (const h of INITIAL_BACKFILL_HOTELS) {
      const rows = await listResearchInventoryForHotel(h.hotelId);
      postHotelCounts[h.hotelId] = rows.length;
    }
  }

  return {
    ok: failed.length === 0,
    version: GDI_YOTEL_GUADALAJARA_RESEARCH_BACKFILL_V1,
    liveApply,
    authorization: auth,
    allowlistSummary: {
      exactMatch: allowlist.exactMatch,
      total: allowlist.actualTotal,
      yotel: allowlist.yotelCount,
      guadalajara: allowlist.guadalajaraCount,
    },
    counts: { created, updated, wouldCreate, wouldUpdate, failed: failed.length },
    postHotelCounts,
    gdoWrites: 0,
    publicationWrites: 0,
    results,
  };
}

export function compositeKey(hotelId, opportunityId) {
  return buildHotelResearchCompositeKey(hotelId, opportunityId);
}

export default {
  GDI_YOTEL_GUADALAJARA_RESEARCH_BACKFILL_V1,
  YOTEL_GUADALAJARA_85_EXACT_WRITE_SET_AUTHORIZATION,
  INITIAL_BACKFILL_HOTELS,
  buildExact85Allowlist,
  validateInitialBackfillAuthorization,
  executeYotelGuadalajaraResearchBackfill,
};
