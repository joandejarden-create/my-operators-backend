/**
 * DecisionOutcomeService — canonical create / validate / action / outcome APIs.
 * Persistence via facade (Airtable primary + optional FS mirror).
 */

import {
  SCHEMA_VERSION,
  DECISION_LIFECYCLE,
  EVENT_KIND,
  CAUSAL_CONFIDENCE,
  PRODUCT_MODULE,
  ACTOR_ROLE,
} from "./types.js";
import {
  buildDecisionIdempotencyKey,
  createDecisionId,
  createEventId,
  findDecisionByIdempotencyKey,
  findDecisionBySubject,
  loadDecision,
  loadEvents,
  listDecisionSummaries,
  listHotelsWithDecisions,
  saveDecisionRecord,
  appendEvent,
  assertHotelBoundary,
  getPersistenceMode,
} from "./persistence.js";
import { projectCurrentState, deriveLifecycle } from "./projection.js";

function nowIso() {
  return new Date().toISOString();
}

function actorFrom(input = {}) {
  return {
    userId: input.userId || input.actorUserId || null,
    role: input.role || input.actorRole || ACTOR_ROLE.OTHER,
    displayName: input.displayName || null,
  };
}

/**
 * Idempotent Decision creation.
 * Same hotel+module+type+subject+recommendationVersion → same active Decision.
 * Guards concurrent races: idempotency key lookup + active subject/version lookup.
 */
export async function createDecision(input = {}) {
  const hotelId = String(input.hotelId || "").trim();
  const productModule = String(input.productModule || "").trim();
  const decisionType = String(input.decisionType || "").trim();
  const subjectId = String(input.subjectId || "").trim();
  const recommendationVersion = Number(input.recommendationVersion) || 1;

  if (!hotelId || !productModule || !decisionType || !subjectId) {
    const err = new Error("decision_required_fields_missing");
    err.code = "decision_required_fields_missing";
    throw err;
  }

  const idempotencyKey = buildDecisionIdempotencyKey({
    hotelId,
    productModule,
    decisionType,
    subjectId,
    recommendationVersion,
  });

  const existing = await findDecisionByIdempotencyKey(hotelId, idempotencyKey);
  if (existing && !isRetiredDecision(existing)) {
    return { decision: existing, created: false };
  }

  // Second line of defense when key missing / race: reuse active subject+version.
  const bySubject = await findDecisionBySubject(hotelId, {
    productModule,
    subjectId,
    decisionType,
  });
  if (
    bySubject &&
    !isRetiredDecision(bySubject) &&
    Number(bySubject.recommendationVersion || 1) === recommendationVersion
  ) {
    return { decision: bySubject, created: false };
  }

  const createdAt = nowIso();
  const decision = {
    decisionId: createDecisionId("dec"),
    idempotencyKey,
    hotelId,
    organizationId: input.organizationId || null,
    entityId: input.entityId || null,
    productModule,
    decisionType,
    subjectType: input.subjectType || null,
    subjectId,
    recommendation: String(input.recommendation || "").trim(),
    recommendationSummary: String(
      input.recommendationSummary || input.recommendation || ""
    )
      .trim()
      .slice(0, 500),
    recommendationDate: input.recommendationDate || createdAt,
    recommendationVersion,
    supersedesDecisionId: input.supersedesDecisionId || null,
    evidenceReferenceIds: Array.isArray(input.evidenceReferenceIds)
      ? input.evidenceReferenceIds
      : [],
    evidenceSnapshotSummary: input.evidenceSnapshotSummary || null,
    confidence: input.confidence ?? null,
    confidenceBand: input.confidenceBand || null,
    confidenceMethodologyVersion:
      input.confidenceMethodologyVersion || "unspecified",
    decisionStatus: DECISION_LIFECYCLE.RECOMMENDED,
    createdAt,
    updatedAt: createdAt,
    createdBy: actorFrom(input.createdBy || input),
    sourceSystem: input.sourceSystem || productModule,
    schemaVersion: SCHEMA_VERSION,
    provenance: input.provenance || null,
  };

  await saveDecisionRecord(decision);
  return { decision, created: true, persistence: getPersistenceMode() };
}

function isRetiredDecision(decision) {
  if (!decision) return true;
  const status = String(decision.decisionStatus || "").toUpperCase();
  if (status === "CLOSED" || status === "SUPERSEDED" || status === "DUPLICATE_RETIRED") {
    return true;
  }
  const key = String(decision.idempotencyKey || "");
  if (key.startsWith("SUPERSEDED:")) return true;
  const summary = String(decision.recommendationSummary || "");
  if (summary.startsWith("[SUPERSEDED")) return true;
  return false;
}

export async function getDecision(hotelId, decisionId) {
  const decision = await loadDecision(hotelId, decisionId);
  if (!decision) return null;
  assertHotelBoundary(hotelId, decision.hotelId);
  const events = await loadEvents(hotelId, decisionId);
  const current = projectCurrentState(decision, events);
  return { decision, events, current };
}

export async function listDecisions(hotelId, { productModule = null } = {}) {
  let rows = await listDecisionSummaries(hotelId);
  if (productModule) {
    rows = rows.filter((d) => d.productModule === productModule);
  }
  const out = [];
  for (const s of rows) {
    const full = await getDecision(hotelId, s.decisionId);
    if (full) out.push(full);
  }
  return out;
}

export async function getSubjectDecision(hotelId, opts) {
  const decision = await findDecisionBySubject(hotelId, opts);
  if (!decision) return null;
  return getDecision(hotelId, decision.decisionId);
}

export async function recordValidation(hotelId, decisionId, input = {}) {
  const decision = await loadDecision(hotelId, decisionId);
  if (!decision) {
    const err = new Error("decision_not_found");
    err.code = "decision_not_found";
    throw err;
  }
  assertHotelBoundary(hotelId, decision.hotelId);

  const event = {
    eventKind: EVENT_KIND.VALIDATION,
    validationEventId: createEventId("val"),
    decisionId,
    hotelId,
    organizationId: decision.organizationId || null,
    subjectId: decision.subjectId,
    productModule: decision.productModule,
    validationType: input.validationType,
    validationValue: input.validationValue,
    validationReason: input.validationReason || null,
    validationNote: input.validationNote || input.note || null,
    validator: actorFrom(input),
    validatedAt: input.validatedAt || nowIso(),
    sourceSurface: input.sourceSurface || "api",
    schemaVersion: SCHEMA_VERSION,
    provenance: input.provenance || null,
  };
  await appendEvent(hotelId, decisionId, event, { decision });

  const events = await loadEvents(hotelId, decisionId);
  const lifecycle = deriveLifecycle(decision, events);
  const updated = {
    ...decision,
    decisionStatus: lifecycle,
    updatedAt: nowIso(),
  };
  await saveDecisionRecord(updated);
  return {
    decision: updated,
    event,
    current: projectCurrentState(updated, events),
    persistence: getPersistenceMode(),
  };
}

export async function recordAction(hotelId, decisionId, input = {}) {
  const decision = await loadDecision(hotelId, decisionId);
  if (!decision) {
    const err = new Error("decision_not_found");
    err.code = "decision_not_found";
    throw err;
  }
  assertHotelBoundary(hotelId, decision.hotelId);

  const event = {
    eventKind: EVENT_KIND.ACTION,
    actionEventId: createEventId("act"),
    decisionId,
    hotelId,
    organizationId: decision.organizationId || null,
    subjectId: decision.subjectId,
    productModule: decision.productModule,
    actionType: input.actionType,
    actionStatus: input.actionStatus || "RECORDED",
    actionDescription: input.actionDescription || null,
    actionOwner: actorFrom(input),
    actionDate: input.actionDate || nowIso(),
    completedAt: input.completedAt || null,
    relatedContactId: input.relatedContactId || null,
    relatedEntityId: input.relatedEntityId || null,
    sourceSurface: input.sourceSurface || "api",
    createdAt: nowIso(),
    schemaVersion: SCHEMA_VERSION,
    provenance: input.provenance || null,
  };
  await appendEvent(hotelId, decisionId, event, { decision });

  const events = await loadEvents(hotelId, decisionId);
  const lifecycle = deriveLifecycle(decision, events);
  const updated = {
    ...decision,
    decisionStatus: lifecycle,
    updatedAt: nowIso(),
  };
  await saveDecisionRecord(updated);
  return {
    decision: updated,
    event,
    current: projectCurrentState(updated, events),
    persistence: getPersistenceMode(),
  };
}

export async function recordOutcome(hotelId, decisionId, input = {}) {
  const decision = await loadDecision(hotelId, decisionId);
  if (!decision) {
    const err = new Error("decision_not_found");
    err.code = "decision_not_found";
    throw err;
  }
  assertHotelBoundary(hotelId, decision.hotelId);

  const event = {
    eventKind: EVENT_KIND.OUTCOME,
    outcomeEventId: createEventId("out"),
    decisionId,
    hotelId,
    organizationId: decision.organizationId || null,
    subjectId: decision.subjectId,
    productModule: decision.productModule,
    outcomeType: input.outcomeType,
    outcomeValue: input.outcomeValue || input.outcomeType || null,
    outcomeReason: input.outcomeReason || null,
    outcomeNote: input.outcomeNote || input.note || null,
    outcomeDate: input.outcomeDate || nowIso(),
    reportedAt: nowIso(),
    financialValue: input.financialValue ?? null,
    currency: input.currency || null,
    roomNights: input.roomNights ?? null,
    revenueValue: input.revenueValue ?? null,
    temporalAssociation: input.temporalAssociation || null,
    measurementChange: input.measurementChange || null,
    causalConfidence: input.causalConfidence || CAUSAL_CONFIDENCE.UNKNOWN,
    sourceSurface: input.sourceSurface || "api",
    reportedBy: actorFrom(input),
    schemaVersion: SCHEMA_VERSION,
    provenance: input.provenance || null,
  };
  await appendEvent(hotelId, decisionId, event, { decision });

  const events = await loadEvents(hotelId, decisionId);
  const lifecycle = deriveLifecycle(decision, events);
  const updated = {
    ...decision,
    decisionStatus: lifecycle,
    updatedAt: nowIso(),
  };
  await saveDecisionRecord(updated);
  return {
    decision: updated,
    event,
    current: projectCurrentState(updated, events),
    persistence: getPersistenceMode(),
  };
}

export async function getDecisionTimeline(hotelId, decisionId) {
  const bundle = await getDecision(hotelId, decisionId);
  if (!bundle) return null;
  const timeline = [...bundle.events].sort((a, b) =>
    String(
      a.validatedAt || a.createdAt || a.reportedAt || a.actionDate || ""
    ).localeCompare(
      String(b.validatedAt || b.createdAt || b.reportedAt || b.actionDate || "")
    )
  );
  return { ...bundle, timeline };
}

export async function getCurrentDecisionState(hotelId, decisionId) {
  const bundle = await getDecision(hotelId, decisionId);
  return bundle ? bundle.current : null;
}

export async function listAllHotelDecisionSummaries() {
  const hotels = await listHotelsWithDecisions();
  const out = [];
  for (const hotelId of hotels) {
    const rows = await listDecisionSummaries(hotelId);
    for (const d of rows) out.push({ ...d, hotelId });
  }
  return out;
}

export { PRODUCT_MODULE, DECISION_LIFECYCLE, getPersistenceMode };
