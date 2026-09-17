/**
 * Airtable persistence for canonical Decision & Outcome layer.
 * Append-only events; decisions upserted by decisionId / idempotencyKey.
 */

import Airtable from "airtable";
import { escapeAirtableFormulaValue } from "../airtable-utils.js";
import {
  DECISIONS_TABLE_NAME,
  DECISION_EVENTS_TABLE_NAME,
  MAP_DECISION as D,
  MAP_DECISION_EVENT as E,
} from "./field-map.js";
import { EVENT_KIND } from "./types.js";
import {
  getDecisionOutcomesAirtableBaseId,
  assertNotLegacyMvpCanonicalBase,
} from "./airtable-base.js";

export {
  getDecisionOutcomesAirtableBaseId,
  assertNotLegacyMvpCanonicalBase,
  LEGACY_DEAL_CAPTURE_MVP_BASE_ID,
  CANONICAL_INTELLIGENCE_BASE_ID,
} from "./airtable-base.js";

function getToken() {
  return process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "";
}

export function isAirtableConfigured() {
  return Boolean(getToken() && getDecisionOutcomesAirtableBaseId());
}

function getBase() {
  const apiKey = getToken();
  const baseId = getDecisionOutcomesAirtableBaseId();
  if (!apiKey || !baseId) {
    const err = new Error("decision_outcomes_airtable_not_configured");
    err.code = "decision_outcomes_airtable_not_configured";
    throw err;
  }
  assertNotLegacyMvpCanonicalBase(baseId, { surface: "decision_outcomes" });
  return new Airtable({ apiKey }).base(baseId);
}

function decisionsTable() {
  return getBase()(DECISIONS_TABLE_NAME);
}

function eventsTable() {
  return getBase()(DECISION_EVENTS_TABLE_NAME);
}

function omitEmpty(fields) {
  const out = {};
  for (const [k, v] of Object.entries(fields)) {
    if (v === undefined || v === null || v === "") continue;
    out[k] = v;
  }
  return out;
}

function jsonText(value) {
  if (value == null) return undefined;
  if (typeof value === "string") return value;
  try {
    return JSON.stringify(value);
  } catch {
    return undefined;
  }
}

function parseJsonText(value, fallback) {
  if (value == null || value === "") return fallback;
  if (typeof value !== "string") return value;
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function actorUserId(actor) {
  if (!actor) return null;
  if (typeof actor === "string") return actor;
  return actor.userId || actor.email || null;
}

function actorRole(actor) {
  if (!actor || typeof actor === "string") return null;
  return actor.role || null;
}

/** Map in-memory decision → Airtable fields. */
export function decisionToAirtableFields(decision) {
  return omitEmpty({
    [D.decisionId]: decision.decisionId,
    [D.idempotencyKey]: decision.idempotencyKey || undefined,
    [D.hotelId]: decision.hotelId,
    [D.organizationId]: decision.organizationId || undefined,
    [D.entityId]: decision.entityId || undefined,
    [D.productModule]: decision.productModule || undefined,
    [D.decisionType]: decision.decisionType || undefined,
    [D.subjectType]: decision.subjectType || undefined,
    [D.subjectId]: decision.subjectId || undefined,
    [D.recommendation]: decision.recommendation || undefined,
    [D.recommendationSummary]: decision.recommendationSummary || undefined,
    [D.recommendationDate]: decision.recommendationDate || undefined,
    [D.recommendationVersion]:
      decision.recommendationVersion != null
        ? Number(decision.recommendationVersion)
        : undefined,
    [D.decisionStatus]: decision.decisionStatus || undefined,
    [D.confidenceScore]:
      decision.confidence != null ? Number(decision.confidence) : undefined,
    [D.confidenceBand]: decision.confidenceBand || undefined,
    [D.confidenceMethodologyVersion]:
      decision.confidenceMethodologyVersion || undefined,
    [D.evidenceSnapshotSummary]: jsonText(decision.evidenceSnapshotSummary),
    [D.evidenceReferenceIds]: jsonText(decision.evidenceReferenceIds),
    [D.sourceSystem]: decision.sourceSystem || undefined,
    [D.schemaVersion]: decision.schemaVersion || undefined,
    [D.createdAt]: decision.createdAt || undefined,
    [D.updatedAt]: decision.updatedAt || undefined,
    [D.createdBy]: actorUserId(decision.createdBy) || undefined,
    [D.createdByRole]: actorRole(decision.createdBy) || undefined,
    [D.supersedesDecisionId]: decision.supersedesDecisionId || undefined,
  });
}

/** Map Airtable record → in-memory decision. */
export function airtableRecordToDecision(record) {
  if (!record) return null;
  const f = record.fields || {};
  return {
    decisionId: f[D.decisionId] || null,
    idempotencyKey: f[D.idempotencyKey] || null,
    hotelId: f[D.hotelId] || null,
    organizationId: f[D.organizationId] || null,
    entityId: f[D.entityId] || null,
    productModule: f[D.productModule] || null,
    decisionType: f[D.decisionType] || null,
    subjectType: f[D.subjectType] || null,
    subjectId: f[D.subjectId] || null,
    recommendation: f[D.recommendation] || "",
    recommendationSummary: f[D.recommendationSummary] || "",
    recommendationDate: f[D.recommendationDate] || null,
    recommendationVersion: f[D.recommendationVersion] ?? 1,
    decisionStatus: f[D.decisionStatus] || null,
    confidence: f[D.confidenceScore] ?? null,
    confidenceBand: f[D.confidenceBand] || null,
    confidenceMethodologyVersion: f[D.confidenceMethodologyVersion] || null,
    evidenceSnapshotSummary: parseJsonText(
      f[D.evidenceSnapshotSummary],
      f[D.evidenceSnapshotSummary] || null
    ),
    evidenceReferenceIds: parseJsonText(f[D.evidenceReferenceIds], []),
    sourceSystem: f[D.sourceSystem] || null,
    schemaVersion: f[D.schemaVersion] || null,
    createdAt: f[D.createdAt] || null,
    updatedAt: f[D.updatedAt] || null,
    createdBy: {
      userId: f[D.createdBy] || null,
      role: f[D.createdByRole] || null,
    },
    supersedesDecisionId: f[D.supersedesDecisionId] || null,
    _airtableRecordId: record.id,
  };
}

export function extractEventId(event) {
  if (!event) return null;
  return (
    event.eventId ||
    event.validationEventId ||
    event.actionEventId ||
    event.outcomeEventId ||
    null
  );
}

/** Flatten typed event → Airtable Decision Events row. */
export function eventToAirtableFields(event, { productModule = null } = {}) {
  const eventType = event.eventKind || event.eventType;
  const eventId = extractEventId(event);
  let eventSubtype = null;
  let eventValue = null;
  let reason = null;
  let note = null;
  let userId = null;
  let userRole = null;
  let eventDate = null;
  let reportedAt = event.reportedAt || null;
  let createdAt = event.createdAt || null;

  if (eventType === EVENT_KIND.VALIDATION || eventType === "VALIDATION") {
    eventSubtype = event.validationType || null;
    eventValue = event.validationValue || null;
    reason = event.validationReason || null;
    note = event.validationNote || event.note || null;
    userId = actorUserId(event.validator);
    userRole = actorRole(event.validator);
    eventDate = event.validatedAt || null;
    createdAt = createdAt || event.validatedAt || null;
  } else if (eventType === EVENT_KIND.ACTION || eventType === "ACTION") {
    eventSubtype = event.actionType || null;
    // Prefer actionType as the durable customer-visible value; keep status in note/raw if needed.
    eventValue = event.actionType || event.actionStatus || null;
    note = event.actionDescription || event.note || null;
    userId = actorUserId(event.actionOwner);
    userRole = actorRole(event.actionOwner);
    eventDate = event.actionDate || event.completedAt || null;
    createdAt = createdAt || event.actionDate || null;
  } else if (eventType === EVENT_KIND.OUTCOME || eventType === "OUTCOME") {
    eventSubtype = event.outcomeType || null;
    eventValue = event.outcomeValue || event.outcomeType || null;
    reason = event.outcomeReason || null;
    note = event.outcomeNote || event.note || null;
    userId = actorUserId(event.reportedBy);
    userRole = actorRole(event.reportedBy);
    eventDate = event.outcomeDate || null;
    reportedAt = reportedAt || event.reportedAt || event.outcomeDate || null;
    createdAt = createdAt || event.reportedAt || event.outcomeDate || null;
  }

  return omitEmpty({
    [E.eventId]: eventId,
    [E.decisionId]: event.decisionId,
    [E.hotelId]: event.hotelId,
    [E.organizationId]: event.organizationId || undefined,
    [E.productModule]: productModule || event.productModule || undefined,
    [E.subjectId]: event.subjectId || undefined,
    [E.eventType]: eventType,
    [E.eventSubtype]: eventSubtype || undefined,
    [E.eventValue]: eventValue || undefined,
    [E.reason]: reason || undefined,
    [E.note]: note || undefined,
    [E.userId]: userId || undefined,
    [E.userRole]: userRole || undefined,
    [E.eventDate]: eventDate || undefined,
    [E.reportedAt]: reportedAt || undefined,
    [E.createdAt]: createdAt || undefined,
    [E.financialValue]:
      event.financialValue != null ? Number(event.financialValue) : undefined,
    [E.currency]: event.currency || undefined,
    [E.roomNights]:
      event.roomNights != null ? Number(event.roomNights) : undefined,
    [E.revenueValue]:
      event.revenueValue != null ? Number(event.revenueValue) : undefined,
    [E.sourceSurface]: event.sourceSurface || undefined,
    [E.schemaVersion]: event.schemaVersion || undefined,
    [E.causalConfidence]: event.causalConfidence || undefined,
    [E.measurementChange]: jsonText(event.measurementChange),
    [E.temporalAssociation]: jsonText(event.temporalAssociation),
    [E.rawEventJson]: jsonText(event),
  });
}

/** Reconstruct typed event from Airtable + optional rawEventJson. */
export function airtableRecordToEvent(record) {
  if (!record) return null;
  const f = record.fields || {};
  const raw = parseJsonText(f[E.rawEventJson], null);
  if (raw && typeof raw === "object") {
    // Prefer raw payload, but recover typed columns when JSON omitted undefined keys.
    const merged = { ...raw, _airtableRecordId: record.id };
    const kind = String(merged.eventKind || merged.eventType || f[E.eventType] || "").toUpperCase();
    if (kind === "VALIDATION" && !merged.validationValue && f[E.eventValue]) {
      merged.validationValue = f[E.eventValue];
    }
    if (kind === "ACTION") {
      if (!merged.actionType && (f[E.eventSubtype] || f[E.eventValue])) {
        merged.actionType = f[E.eventSubtype] || f[E.eventValue];
      }
      if (!merged.actionStatus && f[E.eventValue]) {
        merged.actionStatus = f[E.eventValue];
      }
    }
    if (kind === "OUTCOME") {
      if (!merged.outcomeType && f[E.eventSubtype]) {
        merged.outcomeType = f[E.eventSubtype];
      }
      if (!merged.outcomeValue && f[E.eventValue]) {
        merged.outcomeValue = f[E.eventValue];
      }
    }
    return merged;
  }

  const eventType = f[E.eventType];
  const eventId = f[E.eventId];
  const base = {
    decisionId: f[E.decisionId] || null,
    hotelId: f[E.hotelId] || null,
    organizationId: f[E.organizationId] || null,
    productModule: f[E.productModule] || null,
    subjectId: f[E.subjectId] || null,
    sourceSurface: f[E.sourceSurface] || null,
    schemaVersion: f[E.schemaVersion] || null,
    _airtableRecordId: record.id,
  };

  if (eventType === "VALIDATION") {
    return {
      ...base,
      eventKind: EVENT_KIND.VALIDATION,
      validationEventId: eventId,
      validationType: f[E.eventSubtype] || null,
      validationValue: f[E.eventValue] || null,
      validationReason: f[E.reason] || null,
      validationNote: f[E.note] || null,
      validator: { userId: f[E.userId] || null, role: f[E.userRole] || null },
      validatedAt: f[E.eventDate] || f[E.createdAt] || null,
    };
  }
  if (eventType === "ACTION") {
    return {
      ...base,
      eventKind: EVENT_KIND.ACTION,
      actionEventId: eventId,
      actionType: f[E.eventSubtype] || null,
      actionStatus: f[E.eventValue] || "RECORDED",
      actionDescription: f[E.note] || null,
      actionOwner: { userId: f[E.userId] || null, role: f[E.userRole] || null },
      actionDate: f[E.eventDate] || null,
      createdAt: f[E.createdAt] || f[E.eventDate] || null,
    };
  }
  if (eventType === "OUTCOME") {
    return {
      ...base,
      eventKind: EVENT_KIND.OUTCOME,
      outcomeEventId: eventId,
      outcomeType: f[E.eventSubtype] || null,
      outcomeValue: f[E.eventValue] || null,
      outcomeReason: f[E.reason] || null,
      outcomeNote: f[E.note] || null,
      outcomeDate: f[E.eventDate] || null,
      reportedAt: f[E.reportedAt] || f[E.eventDate] || null,
      financialValue: f[E.financialValue] ?? null,
      currency: f[E.currency] || null,
      roomNights: f[E.roomNights] ?? null,
      revenueValue: f[E.revenueValue] ?? null,
      causalConfidence: f[E.causalConfidence] || null,
      measurementChange: parseJsonText(f[E.measurementChange], null),
      temporalAssociation: parseJsonText(f[E.temporalAssociation], null),
      reportedBy: { userId: f[E.userId] || null, role: f[E.userRole] || null },
    };
  }
  return {
    ...base,
    eventKind: eventType,
    eventId,
    eventSubtype: f[E.eventSubtype] || null,
    eventValue: f[E.eventValue] || null,
  };
}

async function selectAll(table, opts = {}) {
  const out = [];
  await table
    .select({ pageSize: 100, ...opts })
    .eachPage((records, next) => {
      out.push(...records);
      next();
    });
  return out;
}

export async function findDecisionRecordByDecisionId(decisionId) {
  const id = String(decisionId || "").trim();
  if (!id) return null;
  const formula = `{${D.decisionId}}='${escapeAirtableFormulaValue(id)}'`;
  const rows = await decisionsTable()
    .select({ filterByFormula: formula, maxRecords: 1 })
    .firstPage();
  return rows[0] || null;
}

export async function findDecisionRecordByIdempotencyKey(hotelId, idempotencyKey) {
  const formula = `AND({${D.hotelId}}='${escapeAirtableFormulaValue(hotelId)}',{${D.idempotencyKey}}='${escapeAirtableFormulaValue(idempotencyKey)}')`;
  const rows = await decisionsTable()
    .select({ filterByFormula: formula, maxRecords: 1 })
    .firstPage();
  return rows[0] || null;
}

export async function loadDecision(hotelId, decisionId) {
  const rec = await findDecisionRecordByDecisionId(decisionId);
  if (!rec) return null;
  const decision = airtableRecordToDecision(rec);
  if (hotelId && decision.hotelId && String(hotelId) !== String(decision.hotelId)) {
    const err = new Error("hotel_boundary_violation");
    err.code = "hotel_boundary_violation";
    throw err;
  }
  return decision;
}

export async function findDecisionByIdempotencyKey(hotelId, idempotencyKey) {
  const rec = await findDecisionRecordByIdempotencyKey(hotelId, idempotencyKey);
  return airtableRecordToDecision(rec);
}

export async function findDecisionBySubject(
  hotelId,
  { productModule, subjectId, decisionType = null } = {}
) {
  const parts = [
    `{${D.hotelId}}='${escapeAirtableFormulaValue(hotelId)}'`,
    `{${D.productModule}}='${escapeAirtableFormulaValue(productModule)}'`,
    `{${D.subjectId}}='${escapeAirtableFormulaValue(subjectId)}'`,
  ];
  if (decisionType) {
    parts.push(
      `{${D.decisionType}}='${escapeAirtableFormulaValue(decisionType)}'`
    );
  }
  const formula = `AND(${parts.join(",")})`;
  const rows = await selectAll(decisionsTable(), {
    filterByFormula: formula,
    sort: [{ field: D.recommendationVersion, direction: "desc" }],
  });
  if (!rows.length) return null;
  // Prefer active (non-retired) decisions
  const mapped = rows.map(airtableRecordToDecision);
  const active = mapped.find((d) => {
    const status = String(d.decisionStatus || "").toUpperCase();
    if (status === "CLOSED" || status === "SUPERSEDED") return false;
    if (String(d.idempotencyKey || "").startsWith("SUPERSEDED:")) return false;
    if (String(d.recommendationSummary || "").startsWith("[SUPERSEDED")) {
      return false;
    }
    return true;
  });
  return active || mapped[0];
}

/**
 * Upsert decision by decisionId (preferred) or create.
 * Never invents a new decisionId when one is provided.
 */
export async function saveDecisionRecord(decision) {
  const fields = decisionToAirtableFields(decision);
  const existing = await findDecisionRecordByDecisionId(decision.decisionId);
  if (existing) {
    await decisionsTable().update(existing.id, fields);
    return { ...decision, _airtableRecordId: existing.id, _airtableWrite: "update" };
  }
  const created = await decisionsTable().create(fields);
  return { ...decision, _airtableRecordId: created.id, _airtableWrite: "create" };
}

export async function findEventRecordByEventId(eventId) {
  const id = String(eventId || "").trim();
  if (!id) return null;
  const formula = `{${E.eventId}}='${escapeAirtableFormulaValue(id)}'`;
  const rows = await eventsTable()
    .select({ filterByFormula: formula, maxRecords: 1 })
    .firstPage();
  return rows[0] || null;
}

/**
 * Append-only: create event if eventId absent; if present, return existing (no mutate).
 */
export async function appendEvent(hotelId, decisionId, event, meta = {}) {
  const eventId = extractEventId(event);
  if (!eventId) {
    const err = new Error("event_id_required");
    err.code = "event_id_required";
    throw err;
  }
  const existing = await findEventRecordByEventId(eventId);
  if (existing) {
    return {
      event: airtableRecordToEvent(existing),
      created: false,
      _airtableRecordId: existing.id,
    };
  }
  const decision = meta.decision || (await loadDecision(hotelId, decisionId));
  const fields = eventToAirtableFields(event, {
    productModule: decision?.productModule || event.productModule,
  });
  const created = await eventsTable().create(fields);
  return {
    event: { ...event, _airtableRecordId: created.id },
    created: true,
    _airtableRecordId: created.id,
  };
}

export async function loadEvents(hotelId, decisionId) {
  const formula = `{${E.decisionId}}='${escapeAirtableFormulaValue(decisionId)}'`;
  const rows = await selectAll(eventsTable(), { filterByFormula: formula });
  const events = rows.map(airtableRecordToEvent).filter(Boolean);
  if (hotelId) {
    for (const ev of events) {
      if (ev.hotelId && String(ev.hotelId) !== String(hotelId)) {
        const err = new Error("hotel_boundary_violation");
        err.code = "hotel_boundary_violation";
        throw err;
      }
    }
  }
  return events;
}

export async function listDecisionSummaries(hotelId) {
  const formula = `{${D.hotelId}}='${escapeAirtableFormulaValue(hotelId)}'`;
  const rows = await selectAll(decisionsTable(), { filterByFormula: formula });
  return rows.map((rec) => {
    const d = airtableRecordToDecision(rec);
    return {
      decisionId: d.decisionId,
      idempotencyKey: d.idempotencyKey,
      productModule: d.productModule,
      decisionType: d.decisionType,
      subjectType: d.subjectType,
      subjectId: d.subjectId,
      recommendationVersion: d.recommendationVersion || 1,
      decisionStatus: d.decisionStatus,
      createdAt: d.createdAt,
      updatedAt: d.updatedAt,
    };
  });
}

export async function listHotelsWithDecisions() {
  const rows = await selectAll(decisionsTable(), {
    fields: [D.hotelId],
  });
  const set = new Set();
  for (const rec of rows) {
    const hid = rec.fields?.[D.hotelId];
    if (hid) set.add(String(hid));
  }
  return [...set];
}

export async function listAllDecisionRecords() {
  const rows = await selectAll(decisionsTable());
  return rows.map(airtableRecordToDecision).filter(Boolean);
}
