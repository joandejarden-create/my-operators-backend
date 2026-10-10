/**
 * GDI Pursuit filesystem store — hotel-scoped, audit trailed.
 * Path: data/group-demand-intelligence/hotels/{hotelId}/pursuits.json
 * Does not write Ready/Watch intelligence fields.
 */

import fs from "node:fs";
import path from "node:path";
import { randomBytes } from "node:crypto";
import { hotelDir } from "../repository.js";
import {
  GDI_PURSUIT_STATUS,
  GDI_HOTEL_INCLUSION_STATUS,
  GDI_PURSUIT_RESPONSE_STATUS,
  GDI_PURSUIT_MESSAGE_STATUS,
  GDI_PURSUIT_OUTCOME,
  GDI_PURSUIT_TRIGGER_STATUS,
  isClosedPursuitStatus,
  normalizePursuitStatus,
} from "./pursuit-types-v1.js";

function createId(prefix = "gdi_pursuit") {
  return `${prefix}_${randomBytes(4).toString("hex")}`;
}

function pursuitsPath(hotelId) {
  return path.join(hotelDir(hotelId), "pursuits.json");
}

function readDoc(hotelId) {
  const file = pursuitsPath(hotelId);
  if (!fs.existsSync(file)) {
    return { hotelId, pursuits: [], audit: [], updatedAt: null };
  }
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

function writeDoc(hotelId, doc) {
  const dir = hotelDir(hotelId);
  fs.mkdirSync(dir, { recursive: true });
  const next = {
    ...doc,
    hotelId,
    updatedAt: new Date().toISOString(),
  };
  fs.writeFileSync(pursuitsPath(hotelId), JSON.stringify(next, null, 2));
  return next;
}

function appendAudit(doc, entry) {
  const audit = Array.isArray(doc.audit) ? doc.audit : [];
  audit.push({
    id: createId("gdi_pursuit_evt"),
    timestamp: new Date().toISOString(),
    ...entry,
  });
  // Cap audit trail to last 2000 events per hotel
  doc.audit = audit.slice(-2000);
}

export function loadPursuitsDoc(hotelId) {
  return readDoc(hotelId);
}

export function listPursuits(hotelId, opts = {}) {
  const doc = readDoc(hotelId);
  let rows = doc.pursuits || [];
  if (opts.opportunityId) {
    rows = rows.filter((p) => p.opportunityId === opts.opportunityId);
  }
  if (opts.status) {
    const s = String(opts.status).toUpperCase();
    rows = rows.filter((p) => normalizePursuitStatus(p.pursuitStatus) === s);
  }
  if (opts.activeOnly) {
    rows = rows.filter((p) => !isClosedPursuitStatus(p.pursuitStatus));
  }
  return rows;
}

export function getPursuitById(hotelId, pursuitId) {
  return (readDoc(hotelId).pursuits || []).find((p) => p.pursuitId === pursuitId) || null;
}

export function getPursuitByOpportunityId(hotelId, opportunityId) {
  return (
    (readDoc(hotelId).pursuits || []).find((p) => p.opportunityId === opportunityId) ||
    null
  );
}

export function createPursuitRecord(hotelId, draft, actor = "system") {
  const doc = readDoc(hotelId);
  const existing = (doc.pursuits || []).find(
    (p) => p.opportunityId === draft.opportunityId && !isClosedPursuitStatus(p.pursuitStatus)
  );
  if (existing) {
    return { pursuit: existing, created: false, doc };
  }

  const now = new Date().toISOString();
  const pursuit = {
    pursuitId: draft.pursuitId || createId("gdi_pursuit"),
    hotelId,
    opportunityId: draft.opportunityId,
    accountId: draft.accountId || draft.organizationName || null,
    opportunityTitle: draft.opportunityTitle || null,
    organizationName: draft.organizationName || null,
    gdiFacingState: draft.gdiFacingState || null,
    outreachReadiness: draft.outreachReadiness || null,
    pursuitStatus: draft.pursuitStatus || GDI_PURSUIT_STATUS.NOT_STARTED,
    hotelInclusionStatus:
      draft.hotelInclusionStatus || GDI_HOTEL_INCLUSION_STATUS.UNKNOWN,
    responseStatus: draft.responseStatus || GDI_PURSUIT_RESPONSE_STATUS.NO_OUTREACH,
    assignedTo: draft.assignedTo || "UNASSIGNED",
    contactName: draft.contactName || null,
    contactRole: draft.contactRole || null,
    contactOrganization: draft.contactOrganization || null,
    contactPath: draft.contactPath || null,
    contactEmail: draft.contactEmail || null,
    firstContactDate: draft.firstContactDate || null,
    lastContactDate: draft.lastContactDate || null,
    nextFollowUpDate: draft.nextFollowUpDate || null,
    nextAction: draft.nextAction || null,
    nextActionReason: draft.nextActionReason || null,
    nextActionDueDate: draft.nextActionDueDate || null,
    selectionProcess: draft.selectionProcess || null,
    decisionWindowStart: draft.decisionWindowStart || null,
    decisionWindowEnd: draft.decisionWindowEnd || null,
    nextTrigger: draft.nextTrigger || null,
    triggerType: draft.triggerType || null,
    triggerStatus: draft.triggerStatus || GDI_PURSUIT_TRIGGER_STATUS.OPEN,
    triggerDate: draft.triggerDate || null,
    triggerSource: draft.triggerSource || null,
    draftSubject: draft.draftSubject || null,
    draftMessage: draft.draftMessage || null,
    draftLanguage: draft.draftLanguage || "es",
    messageStatus: draft.messageStatus || GDI_PURSUIT_MESSAGE_STATUS.DRAFT,
    notes: draft.notes || "",
    outcome: draft.outcome || GDI_PURSUIT_OUTCOME.NONE,
    outcomeDate: draft.outcomeDate || null,
    customerDidPursue: draft.customerDidPursue || null,
    hotelSuppliedEvidence: Array.isArray(draft.hotelSuppliedEvidence)
      ? draft.hotelSuppliedEvidence
      : [],
    decisionId: draft.decisionId || null,
    createdAt: now,
    updatedAt: now,
    schemaVersion: 1,
  };

  doc.pursuits = doc.pursuits || [];
  doc.pursuits.push(pursuit);
  appendAudit(doc, {
    actor,
    pursuitId: pursuit.pursuitId,
    field: "pursuitStatus",
    oldValue: null,
    newValue: pursuit.pursuitStatus,
    source: "create_pursuit",
  });
  writeDoc(hotelId, doc);
  return { pursuit, created: true, doc };
}

export function updatePursuitRecord(hotelId, pursuitId, patch, actor = "system", source = "update") {
  const doc = readDoc(hotelId);
  const idx = (doc.pursuits || []).findIndex((p) => p.pursuitId === pursuitId);
  if (idx < 0) return { pursuit: null, updated: false };

  const prev = doc.pursuits[idx];
  const next = { ...prev, ...patch, pursuitId, hotelId, updatedAt: new Date().toISOString() };

  const tracked = [
    "pursuitStatus",
    "hotelInclusionStatus",
    "responseStatus",
    "nextFollowUpDate",
    "nextAction",
    "outcome",
    "assignedTo",
    "messageStatus",
    "customerDidPursue",
  ];
  for (const field of tracked) {
    if (patch[field] !== undefined && String(prev[field] ?? "") !== String(next[field] ?? "")) {
      appendAudit(doc, {
        actor,
        pursuitId,
        field,
        oldValue: prev[field] ?? null,
        newValue: next[field] ?? null,
        source,
      });
    }
  }

  doc.pursuits[idx] = next;
  writeDoc(hotelId, doc);
  return { pursuit: next, updated: true, doc };
}

export function applyFollowUpDue(hotelId, nowDate = new Date().toISOString().slice(0, 10)) {
  const doc = readDoc(hotelId);
  let changed = 0;
  for (let i = 0; i < (doc.pursuits || []).length; i++) {
    const p = doc.pursuits[i];
    if (isClosedPursuitStatus(p.pursuitStatus)) continue;
    const due = String(p.nextFollowUpDate || "").slice(0, 10);
    if (due && due <= nowDate && p.pursuitStatus !== GDI_PURSUIT_STATUS.FOLLOW_UP_DUE) {
      const old = p.pursuitStatus;
      doc.pursuits[i] = {
        ...p,
        pursuitStatus: GDI_PURSUIT_STATUS.FOLLOW_UP_DUE,
        updatedAt: new Date().toISOString(),
      };
      appendAudit(doc, {
        actor: "system",
        pursuitId: p.pursuitId,
        field: "pursuitStatus",
        oldValue: old,
        newValue: GDI_PURSUIT_STATUS.FOLLOW_UP_DUE,
        source: "follow_up_due_engine",
      });
      changed += 1;
    }
  }
  if (changed) writeDoc(hotelId, doc);
  return { changed, pursuits: doc.pursuits || [] };
}

export function appendHotelSuppliedEvidence(hotelId, pursuitId, evidence, actor = "hotel_user") {
  const doc = readDoc(hotelId);
  const idx = (doc.pursuits || []).findIndex((p) => p.pursuitId === pursuitId);
  if (idx < 0) return { pursuit: null };
  const prev = doc.pursuits[idx];
  const entry = {
    id: createId("gdi_hse"),
    provenance: "HOTEL_SUPPLIED_EVIDENCE",
    capturedAt: new Date().toISOString(),
    actor,
    text: evidence.text || evidence.note || "",
    responseClass: evidence.responseClass || null,
  };
  const list = Array.isArray(prev.hotelSuppliedEvidence) ? [...prev.hotelSuppliedEvidence] : [];
  list.push(entry);
  doc.pursuits[idx] = {
    ...prev,
    hotelSuppliedEvidence: list,
    updatedAt: new Date().toISOString(),
  };
  appendAudit(doc, {
    actor,
    pursuitId,
    field: "hotelSuppliedEvidence",
    oldValue: null,
    newValue: entry.id,
    source: "hotel_supplied_evidence",
  });
  writeDoc(hotelId, doc);
  return { pursuit: doc.pursuits[idx], evidence: entry };
}

export function toCustomerPursuitDto(pursuit) {
  if (!pursuit) return null;
  return {
    pursuitId: pursuit.pursuitId,
    hotelId: pursuit.hotelId,
    opportunityId: pursuit.opportunityId,
    opportunityTitle: pursuit.opportunityTitle,
    organizationName: pursuit.organizationName,
    gdiFacingState: pursuit.gdiFacingState,
    outreachReadiness: pursuit.outreachReadiness,
    pursuitStatus: pursuit.pursuitStatus,
    hotelInclusionStatus: pursuit.hotelInclusionStatus,
    responseStatus: pursuit.responseStatus,
    assignedTo: pursuit.assignedTo || "UNASSIGNED",
    contactName: pursuit.contactName,
    contactRole: pursuit.contactRole,
    contactOrganization: pursuit.contactOrganization,
    contactPath: pursuit.contactPath,
    contactEmail: pursuit.contactEmail,
    firstContactDate: pursuit.firstContactDate,
    lastContactDate: pursuit.lastContactDate,
    nextFollowUpDate: pursuit.nextFollowUpDate,
    nextAction: pursuit.nextAction,
    nextActionReason: pursuit.nextActionReason,
    nextActionDueDate: pursuit.nextActionDueDate,
    selectionProcess: pursuit.selectionProcess,
    decisionWindowStart: pursuit.decisionWindowStart,
    decisionWindowEnd: pursuit.decisionWindowEnd,
    nextTrigger: pursuit.nextTrigger,
    triggerType: pursuit.triggerType,
    triggerStatus: pursuit.triggerStatus,
    draftSubject: pursuit.draftSubject,
    draftLanguage: pursuit.draftLanguage,
    messageStatus: pursuit.messageStatus,
    // draftMessage included for pursuit panel only (customer-owned drafts)
    draftMessage: pursuit.draftMessage,
    notes: pursuit.notes,
    outcome: pursuit.outcome,
    outcomeDate: pursuit.outcomeDate,
    customerDidPursue: pursuit.customerDidPursue,
    hotelSuppliedEvidenceCount: Array.isArray(pursuit.hotelSuppliedEvidence)
      ? pursuit.hotelSuppliedEvidence.length
      : 0,
    createdAt: pursuit.createdAt,
    updatedAt: pursuit.updatedAt,
  };
}
