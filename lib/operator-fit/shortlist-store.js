/**
 * Operator Fit Shortlist — file-backed store for internal pilot.
 * Preserves immutable decision snapshots. Not Operator Deal Requests.
 */

import { readFileSync, writeFileSync, existsSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  SHORTLIST_STATUS,
  buildShortlistDecisionSnapshot,
  fieldsFromShortlistCreate,
  map_operatorShortlistFields,
} from "./shortlist.js";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
const DEFAULT_PATH = join(ROOT, "data", "operator-fit", "shortlist-store.json");

function emptyStore() {
  return {
    version: 1,
    tableName: "Operator Fit - Shortlist",
    notOdr: true,
    updatedAt: null,
    records: [],
  };
}

export function getShortlistStorePath(customPath) {
  return customPath || process.env.OPERATOR_FIT_SHORTLIST_STORE_PATH || DEFAULT_PATH;
}

export function loadShortlistStore(path = getShortlistStorePath()) {
  if (!existsSync(path)) return emptyStore();
  try {
    const raw = JSON.parse(readFileSync(path, "utf8"));
    if (!raw || !Array.isArray(raw.records)) return emptyStore();
    return { ...emptyStore(), ...raw, records: raw.records };
  } catch (err) {
    console.error("[operator-fit-shortlist] load failed", err?.message || err);
    return emptyStore();
  }
}

export function saveShortlistStore(store, path = getShortlistStorePath()) {
  mkdirSync(dirname(path), { recursive: true });
  const next = {
    ...store,
    updatedAt: new Date().toISOString(),
    notOdr: true,
  };
  writeFileSync(path, JSON.stringify(next, null, 2));
  return next;
}

/**
 * Create shortlist row with frozen snapshot. Does not create ODR / outreach.
 */
export function createShortlistEntry(input = {}, path = getShortlistStorePath()) {
  const store = loadShortlistStore(path);
  const snapshot = buildShortlistDecisionSnapshot(input);
  const shortlistId = input.shortlistId || `osl_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  const airtableFields = fieldsFromShortlistCreate({ ...input, shortlistId }, snapshot);

  const record = {
    id: shortlistId,
    dealId: input.dealId || "",
    dealLabel: input.dealLabel || "",
    operatorId: input.operatorId || input.operatorRecordId || "",
    operatorName: input.operatorName || "",
    brand: input.brand || "",
    candidateType: input.candidateType || "Third-party operator",
    operatingStructure: input.operatingStructure || "",
    status: input.status || SHORTLIST_STATUS.SHORTLISTED,
    shortlistedDate: airtableFields[map_operatorShortlistFields.shortlistedDate],
    shortlistedBy: input.shortlistedBy || "",
    advisorNote: input.advisorNote || "",
    outreachStatus: input.outreachStatus || "Not started",
    snapshot,
    /** Frozen Airtable-shaped fields for ensure/apply scripts */
    airtableFields,
    removedDate: null,
    removedBy: null,
    removalReason: null,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  };

  store.records.push(record);
  saveShortlistStore(store, path);
  return record;
}

export function listShortlistForDeal(dealId, { includeRemoved = true, path } = {}) {
  const store = loadShortlistStore(path);
  const id = String(dealId || "").trim();
  return store.records.filter((r) => {
    if (r.dealId !== id) return false;
    if (!includeRemoved && r.status === SHORTLIST_STATUS.REMOVED) return false;
    return true;
  });
}

/**
 * Soft-remove — preserves original snapshot; never overwrite snapshot fields.
 */
export function removeShortlistEntry(shortlistId, { removedBy = "", reason = "", path } = {}) {
  const store = loadShortlistStore(path);
  const rec = store.records.find((r) => r.id === shortlistId);
  if (!rec) return null;
  rec.status = SHORTLIST_STATUS.REMOVED;
  rec.removedDate = new Date().toISOString().slice(0, 10);
  rec.removedBy = removedBy;
  rec.removalReason = reason || "";
  rec.updatedAt = new Date().toISOString();
  // Preserve snapshot immutably
  if (rec.airtableFields) {
    rec.airtableFields[map_operatorShortlistFields.status] = SHORTLIST_STATUS.REMOVED;
    rec.airtableFields[map_operatorShortlistFields.removedDate] = rec.removedDate;
    rec.airtableFields[map_operatorShortlistFields.removedBy] = removedBy;
    rec.airtableFields[map_operatorShortlistFields.removalReason] = reason || "";
  }
  saveShortlistStore(store, path);
  return rec;
}

export function updateShortlistStatus(shortlistId, status, { advisorNote, path } = {}) {
  const store = loadShortlistStore(path);
  const rec = store.records.find((r) => r.id === shortlistId);
  if (!rec) return null;
  const allowed = Object.values(SHORTLIST_STATUS);
  if (!allowed.includes(status)) {
    throw new Error(`Invalid shortlist status: ${status}`);
  }
  rec.status = status;
  if (advisorNote != null) rec.advisorNote = advisorNote;
  rec.updatedAt = new Date().toISOString();
  if (rec.airtableFields) {
    rec.airtableFields[map_operatorShortlistFields.status] = status;
    if (advisorNote != null) {
      rec.airtableFields[map_operatorShortlistFields.advisorNote] = advisorNote;
    }
  }
  saveShortlistStore(store, path);
  return rec;
}

/**
 * Attach current evaluation metrics without mutating snapshot.
 */
export function withCurrentVsSnapshot(entry, current = {}) {
  const snap = entry.snapshot || {};
  return {
    ...entry,
    currentAlignment: current.alignment ?? null,
    currentReadiness: current.readiness ?? null,
    currentEligibility: current.eligibility ?? null,
    currentConfidence: current.confidence ?? null,
    changeSinceShortlist: {
      alignmentDelta:
        snap.alignment != null && current.alignment != null
          ? Math.round((Number(current.alignment) - Number(snap.alignment)) * 10) / 10
          : null,
      readinessChanged: snap.readiness && current.readiness ? snap.readiness !== current.readiness : null,
      eligibilityChanged:
        snap.eligibility && current.eligibility ? snap.eligibility !== current.eligibility : null,
    },
  };
}

export { SHORTLIST_STATUS, map_operatorShortlistFields };
