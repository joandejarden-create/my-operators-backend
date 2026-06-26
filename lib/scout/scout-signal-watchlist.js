/**
 * Scout Opportunity Signals watchlist — read/write Scout Opportunity Signals table only.
 */

import {
  SCOUT_OPPORTUNITY_SIGNALS_TABLE,
  WATCHLIST_FIELDS,
  SIGNAL_TYPE_OPTIONS,
  CONFIDENCE_OPTIONS,
  ACTIONABILITY_OPTIONS,
  REVIEW_STATUS_OPTIONS,
} from "./scout-signal-watchlist-fields.js";
import { getPlatformBase } from "../hotel-census/platform-base.js";
import { exactMatchKey } from "../hotel-census/brand-alias-resolve.js";

const FORMULA_CHUNK = 8;

function escFormula(value) {
  return String(value ?? "").replace(/'/g, "\\'");
}

function nowIso() {
  return new Date().toISOString();
}

function parseJsonSafe(text) {
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

function isSelectValid(value, allowed) {
  return allowed.includes(exactMatchKey(value));
}

/**
 * Map generated Scout signal → Airtable write payload.
 * @param {object} signal
 * @param {{ reviewStatus?: string, internalNotes?: string, assignedTo?: string, preserveGeneratedAt?: string }} [opts]
 */
export function normalizeSignalForAirtable(signal, opts = {}) {
  const signalId = exactMatchKey(signal?.signalId);
  if (!signalId) {
    return { ok: false, error: "signal.signalId is required" };
  }

  const signalType = exactMatchKey(signal.signalType);
  if (!SIGNAL_TYPE_OPTIONS.includes(signalType)) {
    return { ok: false, error: `Invalid signalType: ${signalType || "(empty)"}` };
  }

  const confidence = exactMatchKey(signal.confidence);
  const actionability = exactMatchKey(signal.actionability);
  const reviewStatus = exactMatchKey(opts.reviewStatus || signal.reviewStatus || "New");

  if (!isSelectValid(confidence, CONFIDENCE_OPTIONS)) {
    return { ok: false, error: `Invalid confidence: ${confidence}` };
  }
  if (!isSelectValid(actionability, ACTIONABILITY_OPTIONS)) {
    return { ok: false, error: `Invalid actionability: ${actionability}` };
  }
  if (!isSelectValid(reviewStatus, REVIEW_STATUS_OPTIONS)) {
    return { ok: false, error: `Invalid reviewStatus: ${reviewStatus}` };
  }

  const generatedAt = opts.preserveGeneratedAt || signal.source?.generatedAt || nowIso();
  const supportingMetrics = signal.supportingMetrics ?? null;

  const fields = {
    [WATCHLIST_FIELDS.signalId]: signalId,
    [WATCHLIST_FIELDS.signalType]: signalType,
    [WATCHLIST_FIELDS.signalTitle]: exactMatchKey(signal.title),
    [WATCHLIST_FIELDS.country]: exactMatchKey(signal.country),
    [WATCHLIST_FIELDS.market]: exactMatchKey(signal.market),
    [WATCHLIST_FIELDS.submarket]: exactMatchKey(signal.submarket),
    [WATCHLIST_FIELDS.city]: exactMatchKey(signal.city),
    [WATCHLIST_FIELDS.linkedHotelCensusRecordId]: exactMatchKey(signal.linkedHotelRecordId),
    [WATCHLIST_FIELDS.hotelName]: exactMatchKey(signal.hotelName),
    [WATCHLIST_FIELDS.parentCompany]: exactMatchKey(signal.parentCompany),
    [WATCHLIST_FIELDS.brand]: exactMatchKey(signal.brand),
    [WATCHLIST_FIELDS.priorityScore]: Number.isFinite(signal.priorityScore)
      ? signal.priorityScore
      : 0,
    [WATCHLIST_FIELDS.confidence]: confidence,
    [WATCHLIST_FIELDS.actionability]: actionability,
    [WATCHLIST_FIELDS.reason]: exactMatchKey(signal.reason),
    [WATCHLIST_FIELDS.supportingMetricsJson]: supportingMetrics
      ? JSON.stringify(supportingMetrics)
      : "",
    [WATCHLIST_FIELDS.recommendedAction]: exactMatchKey(signal.recommendedAction),
    [WATCHLIST_FIELDS.reviewStatus]: reviewStatus,
    [WATCHLIST_FIELDS.assignedTo]: exactMatchKey(opts.assignedTo ?? signal.assignedTo),
    [WATCHLIST_FIELDS.internalNotes]: exactMatchKey(opts.internalNotes ?? signal.internalNotes),
    [WATCHLIST_FIELDS.source]: exactMatchKey(signal.source?.table || "Hotel Census"),
    [WATCHLIST_FIELDS.generatedAt]: generatedAt,
    [WATCHLIST_FIELDS.lastReviewedAt]: nowIso(),
  };

  return { ok: true, signalId, fields, reviewStatus };
}

export function mapSavedSignalRecord(record) {
  const f = record.fields || {};
  return {
    recordId: record.id,
    signalId: exactMatchKey(f[WATCHLIST_FIELDS.signalId]),
    signalType: exactMatchKey(f[WATCHLIST_FIELDS.signalType]),
    signalTitle: exactMatchKey(f[WATCHLIST_FIELDS.signalTitle]),
    country: exactMatchKey(f[WATCHLIST_FIELDS.country]),
    market: exactMatchKey(f[WATCHLIST_FIELDS.market]),
    submarket: exactMatchKey(f[WATCHLIST_FIELDS.submarket]),
    city: exactMatchKey(f[WATCHLIST_FIELDS.city]),
    linkedHotelCensusRecordId: exactMatchKey(f[WATCHLIST_FIELDS.linkedHotelCensusRecordId]),
    hotelName: exactMatchKey(f[WATCHLIST_FIELDS.hotelName]),
    parentCompany: exactMatchKey(f[WATCHLIST_FIELDS.parentCompany]),
    brand: exactMatchKey(f[WATCHLIST_FIELDS.brand]),
    priorityScore: f[WATCHLIST_FIELDS.priorityScore] ?? null,
    confidence: exactMatchKey(f[WATCHLIST_FIELDS.confidence]),
    actionability: exactMatchKey(f[WATCHLIST_FIELDS.actionability]),
    reason: exactMatchKey(f[WATCHLIST_FIELDS.reason]),
    supportingMetrics: parseJsonSafe(f[WATCHLIST_FIELDS.supportingMetricsJson]),
    recommendedAction: exactMatchKey(f[WATCHLIST_FIELDS.recommendedAction]),
    reviewStatus: exactMatchKey(f[WATCHLIST_FIELDS.reviewStatus]),
    assignedTo: exactMatchKey(f[WATCHLIST_FIELDS.assignedTo]),
    internalNotes: exactMatchKey(f[WATCHLIST_FIELDS.internalNotes]),
    source: exactMatchKey(f[WATCHLIST_FIELDS.source]),
    generatedAt: f[WATCHLIST_FIELDS.generatedAt] || null,
    lastReviewedAt: f[WATCHLIST_FIELDS.lastReviewedAt] || null,
    createDeal: f[WATCHLIST_FIELDS.createDeal] === true,
  };
}

/**
 * @param {string} signalId
 */
export async function findSavedSignalBySignalId(signalId) {
  const base = getPlatformBase();
  if (!base) return { ok: false, error: "Platform base not configured" };

  const id = exactMatchKey(signalId);
  if (!id) return { ok: false, error: "signalId is required" };

  const formula = `{${WATCHLIST_FIELDS.signalId}}='${escFormula(id)}'`;
  const records = await base(SCOUT_OPPORTUNITY_SIGNALS_TABLE)
    .select({ filterByFormula: formula, maxRecords: 1 })
    .firstPage();

  if (!records.length) {
    return { ok: true, found: false, record: null, saved: null };
  }

  return {
    ok: true,
    found: true,
    record: records[0],
    saved: mapSavedSignalRecord(records[0]),
  };
}

/**
 * @param {string[]} signalIds
 * @returns {Promise<Map<string, ReturnType<typeof mapSavedSignalRecord>>>}
 */
export async function loadSavedSignalsBySignalIds(signalIds) {
  const map = new Map();
  const base = getPlatformBase();
  if (!base || !signalIds?.length) return map;

  const unique = [...new Set(signalIds.map((id) => exactMatchKey(id)).filter(Boolean))];
  for (let i = 0; i < unique.length; i += FORMULA_CHUNK) {
    const chunk = unique.slice(i, i + FORMULA_CHUNK);
    const orParts = chunk.map((id) => `{${WATCHLIST_FIELDS.signalId}}='${escFormula(id)}'`);
    const formula = chunk.length === 1 ? orParts[0] : `OR(${orParts.join(",")})`;
    const records = await base(SCOUT_OPPORTUNITY_SIGNALS_TABLE)
      .select({ filterByFormula: formula, pageSize: 100 })
      .all();
    for (const rec of records) {
      const saved = mapSavedSignalRecord(rec);
      if (saved.signalId) map.set(saved.signalId, saved);
    }
  }
  return map;
}

/**
 * @param {object} params
 * @param {object} params.signal Generated signal object
 * @param {string} [params.reviewStatus]
 * @param {string} [params.internalNotes]
 * @param {string} [params.assignedTo]
 */
export async function saveOrUpdateSignal(params) {
  const base = getPlatformBase();
  if (!base) return { ok: false, error: "Platform base not configured" };

  const existing = await findSavedSignalBySignalId(params.signal?.signalId);
  if (!existing.ok) return existing;

  const normalized = normalizeSignalForAirtable(params.signal, {
    reviewStatus: params.reviewStatus,
    internalNotes: params.internalNotes,
    assignedTo: params.assignedTo,
    preserveGeneratedAt: existing.found
      ? existing.saved?.generatedAt || undefined
      : undefined,
  });
  if (!normalized.ok) return normalized;

  if (existing.found) {
    const updated = await base(SCOUT_OPPORTUNITY_SIGNALS_TABLE).update(
      existing.record.id,
      normalized.fields
    );
    return {
      ok: true,
      status: "updated",
      recordId: updated.id,
      signalId: normalized.signalId,
      reviewStatus: normalized.reviewStatus,
    };
  }

  const created = await base(SCOUT_OPPORTUNITY_SIGNALS_TABLE).create(normalized.fields);
  return {
    ok: true,
    status: "created",
    recordId: created.id,
    signalId: normalized.signalId,
    reviewStatus: normalized.reviewStatus,
  };
}

function buildListFormula(filters) {
  const parts = [];
  if (filters.reviewStatus) {
    parts.push(`{${WATCHLIST_FIELDS.reviewStatus}}='${escFormula(filters.reviewStatus)}'`);
  }
  if (filters.country) {
    parts.push(`{${WATCHLIST_FIELDS.country}}='${escFormula(filters.country)}'`);
  }
  if (filters.market) {
    parts.push(`{${WATCHLIST_FIELDS.market}}='${escFormula(filters.market)}'`);
  }
  if (filters.submarket) {
    parts.push(`{${WATCHLIST_FIELDS.submarket}}='${escFormula(filters.submarket)}'`);
  }
  if (filters.parentCompany) {
    parts.push(`{${WATCHLIST_FIELDS.parentCompany}}='${escFormula(filters.parentCompany)}'`);
  }
  if (filters.brand) {
    parts.push(`{${WATCHLIST_FIELDS.brand}}='${escFormula(filters.brand)}'`);
  }
  if (filters.signalType) {
    parts.push(`{${WATCHLIST_FIELDS.signalType}}='${escFormula(filters.signalType)}'`);
  }
  if (filters.minPriorityScore != null && Number.isFinite(filters.minPriorityScore)) {
    parts.push(`{${WATCHLIST_FIELDS.priorityScore}}>=${filters.minPriorityScore}`);
  }
  if (!parts.length) return "";
  return parts.length === 1 ? parts[0] : `AND(${parts.join(",")})`;
}

/**
 * @param {Record<string, string|number>} [filters]
 */
export async function listSavedSignals(filters = {}) {
  const base = getPlatformBase();
  if (!base) return { ok: false, error: "Platform base not configured" };

  const limitRaw = parseInt(filters.limit, 10);
  const limit = Number.isFinite(limitRaw) && limitRaw > 0 ? Math.min(limitRaw, 500) : 100;

  const minPriorityRaw = parseInt(filters.minPriorityScore, 10);
  const parsed = {
    reviewStatus: exactMatchKey(filters.reviewStatus),
    country: exactMatchKey(filters.country),
    market: exactMatchKey(filters.market),
    submarket: exactMatchKey(filters.submarket),
    parentCompany: exactMatchKey(filters.parentCompany),
    brand: exactMatchKey(filters.brand),
    signalType: exactMatchKey(filters.signalType),
    minPriorityScore: Number.isFinite(minPriorityRaw) ? minPriorityRaw : null,
  };

  const formula = buildListFormula(parsed);
  const selectOpts = { pageSize: 100, maxRecords: limit, sort: [{ field: WATCHLIST_FIELDS.lastReviewedAt, direction: "desc" }] };
  if (formula) selectOpts.filterByFormula = formula;

  let records;
  try {
    records = await base(SCOUT_OPPORTUNITY_SIGNALS_TABLE).select(selectOpts).all();
  } catch (err) {
    const msg = err?.message || String(err);
    if (/could not find table|not found/i.test(msg)) {
      return {
        ok: false,
        error: "Scout Opportunity Signals table not found. Run scripts/ensure-scout-opportunity-signals-table.mjs --apply",
      };
    }
    throw err;
  }

  const signals = records.map(mapSavedSignalRecord);
  return { ok: true, signals, count: signals.length, filters: { ...parsed, limit } };
}

const PATCHABLE_FIELDS = {
  reviewStatus: WATCHLIST_FIELDS.reviewStatus,
  internalNotes: WATCHLIST_FIELDS.internalNotes,
  assignedTo: WATCHLIST_FIELDS.assignedTo,
  createDeal: WATCHLIST_FIELDS.createDeal,
};

/**
 * @param {string} signalId
 * @param {object} updates
 */
export async function patchSavedSignal(signalId, updates = {}) {
  const base = getPlatformBase();
  if (!base) return { ok: false, error: "Platform base not configured" };

  const existing = await findSavedSignalBySignalId(signalId);
  if (!existing.ok) return existing;
  if (!existing.found) {
    return { ok: false, error: `No saved signal found for signalId: ${signalId}` };
  }

  const fields = {};
  const updatedFieldKeys = [];

  if (updates.reviewStatus !== undefined) {
    const status = exactMatchKey(updates.reviewStatus);
    if (!isSelectValid(status, REVIEW_STATUS_OPTIONS)) {
      return { ok: false, error: `Invalid reviewStatus: ${status}` };
    }
    fields[WATCHLIST_FIELDS.reviewStatus] = status;
    updatedFieldKeys.push("reviewStatus");
  }

  if (updates.internalNotes !== undefined) {
    fields[WATCHLIST_FIELDS.internalNotes] = exactMatchKey(updates.internalNotes);
    updatedFieldKeys.push("internalNotes");
  }

  if (updates.assignedTo !== undefined) {
    fields[WATCHLIST_FIELDS.assignedTo] = exactMatchKey(updates.assignedTo);
    updatedFieldKeys.push("assignedTo");
  }

  if (updates.createDeal !== undefined) {
    fields[WATCHLIST_FIELDS.createDeal] = updates.createDeal === true;
    updatedFieldKeys.push("createDeal");
  }

  if (!updatedFieldKeys.length) {
    return { ok: false, error: "No patchable fields provided" };
  }

  fields[WATCHLIST_FIELDS.lastReviewedAt] = nowIso();

  const updated = await base(SCOUT_OPPORTUNITY_SIGNALS_TABLE).update(existing.record.id, fields);

  return {
    ok: true,
    status: "updated",
    recordId: updated.id,
    signalId: exactMatchKey(signalId),
    updatedFields: updatedFieldKeys,
    saved: mapSavedSignalRecord(updated),
  };
}

/**
 * @param {object[]} signals
 * @param {Map<string, object>} [savedBySignalId]
 */
export async function annotateGeneratedSignalsWithSavedStatus(signals, savedBySignalId) {
  const list = signals || [];
  if (!list.length) return list;

  const map =
    savedBySignalId ||
    (await loadSavedSignalsBySignalIds(list.map((s) => s.signalId)));

  return list.map((signal) => {
    const saved = map.get(exactMatchKey(signal.signalId));
    return {
      ...signal,
      saved: !!saved,
      savedRecordId: saved?.recordId || null,
      savedReviewStatus: saved?.reviewStatus || null,
      savedAssignedTo: saved?.assignedTo || null,
      savedLastReviewedAt: saved?.lastReviewedAt || null,
    };
  });
}

export { SCOUT_OPPORTUNITY_SIGNALS_TABLE, WATCHLIST_FIELDS, REVIEW_STATUS_OPTIONS };
