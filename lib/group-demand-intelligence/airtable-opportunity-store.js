/**
 * Airtable store for Group Demand Opportunities (GDI-specific).
 * Does not own Decisions / Decision Events — those stay in lib/decision-outcomes.
 */

import Airtable from "airtable";
import { escapeAirtableFormulaValue } from "../airtable-utils.js";
import { OPPORTUNITY_QUALIFICATION } from "./claim-types.js";
import {
  GDI_OPPORTUNITIES_TABLE_NAME,
  MAP_GDI_OPPORTUNITY as F,
  GDI_OPPORTUNITY_SCHEMA_VERSION,
  VAL_GDI_PRIORITY,
  VAL_GDI_OPPORTUNITY_TYPE,
  VAL_GDI_OPPORTUNITY_TYPE_AIRTABLE_SAFE,
  VAL_GDI_VENUE_STATUS,
  VAL_GDI_ROOM_DEMAND,
} from "./opportunity-field-map.js";
import {
  getGdiOpportunitiesAirtableBaseId,
  assertNotLegacyMvpCanonicalBase,
} from "../decision-outcomes/airtable-base.js";
import { airtableRequestTimeoutMs } from "../http/with-timeout.js";

export { getGdiOpportunitiesAirtableBaseId };

const VAL_QUALIFICATION = Object.freeze(Object.values(OPPORTUNITY_QUALIFICATION));

function getToken() {
  return process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "";
}

function asSelect(value, allowed) {
  const v = value == null ? "" : String(value).trim();
  if (!v) return undefined;
  return allowed.includes(v) ? v : undefined;
}

export function isGdiOpportunityAirtableConfigured() {
  return Boolean(getToken() && getGdiOpportunitiesAirtableBaseId());
}

function getBase() {
  const apiKey = getToken();
  const baseId = getGdiOpportunitiesAirtableBaseId();
  if (!apiKey || !baseId) {
    const err = new Error("gdi_opportunities_airtable_not_configured");
    err.code = "gdi_opportunities_airtable_not_configured";
    throw err;
  }
  assertNotLegacyMvpCanonicalBase(baseId, { surface: "gdi_opportunities" });
  return new Airtable({
    apiKey,
    requestTimeout: airtableRequestTimeoutMs(),
  }).base(baseId);
}

function table() {
  return getBase()(GDI_OPPORTUNITIES_TABLE_NAME);
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

function parseJson(value, fallback) {
  if (value == null || value === "") return fallback;
  if (typeof value !== "string") return value;
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function sourceSummaryFromOpp(opp) {
  const sources = Array.isArray(opp.sources) ? opp.sources : [];
  if (!sources.length) return opp.sourceSummary || null;
  return sources
    .slice(0, 8)
    .map((s) => s.name || s.title || s.url || "")
    .filter(Boolean)
    .join("; ")
    .slice(0, 2000);
}

function contactField(opp, key) {
  const c = opp.primaryContact || {};
  if (key === "id") return opp.primaryContactId || c.id || c.contactId || null;
  if (key === "name") return c.name || opp.primaryContactName || null;
  if (key === "role")
    return c.role || c.contactRole || opp.primaryContactRole || null;
  if (key === "email") return c.email || opp.primaryContactEmail || null;
  if (key === "phone") return c.phone || opp.primaryContactPhone || null;
  return null;
}

/** Map in-memory opportunity → Airtable columns (+ payload for fidelity). */
export function opportunityToAirtableFields(opp, meta = {}) {
  const payload = { ...opp };
  // Avoid nesting huge evidence blobs if already referenced — keep structured result.
  // Still store full customer-facing opp for UI read-back fidelity.
  return omitEmpty({
    [F.opportunityId]: opp.id || opp.opportunityId,
    [F.hotelId]: opp.hotelId || meta.hotelId,
    [F.organizationId]: opp.organizationId || null,
    [F.opportunityName]: opp.title || opp.opportunityName || null,
    [F.organizationName]: opp.organizationName || null,
    [F.organizationEntityId]:
      opp.organizationEntityId || opp.organizationId || null,
    [F.opportunityType]: asSelect(
      opp.opportunityType,
      VAL_GDI_OPPORTUNITY_TYPE_AIRTABLE_SAFE
    ),
    [F.priority]: asSelect(opp.priority, VAL_GDI_PRIORITY),
    [F.actionStatus]:
      opp.actionStatus || opp.salesActionStatus || opp.demandStatus || null,
    [F.eventStartDate]:
      opp.eventStartDate || opp.startDate || opp.dates?.start || null,
    [F.eventEndDate]:
      opp.eventEndDate || opp.endDate || opp.dates?.end || null,
    [F.segment]: opp.segment || opp.demandType || null,
    [F.territory]:
      opp.territory ||
      opp.demandTerritoryLabel ||
      opp.demandTerritory ||
      null,
    [F.venueStatus]: asSelect(
      opp.venueSourcingStatus || opp.venueStatus,
      VAL_GDI_VENUE_STATUS
    ),
    // sourcingStatus may be research taxonomy ≠ venue enum — only write when known
    [F.sourcingStatus]: asSelect(
      opp.sourcingStatus,
      VAL_GDI_VENUE_STATUS
    ),
    [F.attendance]:
      opp.publishedAttendance != null
        ? Number(opp.publishedAttendance)
        : opp.attendance != null
          ? Number(opp.attendance)
          : undefined,
    [F.attendanceStatus]: opp.attendanceStatus || null,
    [F.peakRooms]:
      opp.publishedPeakRooms != null
        ? Number(opp.publishedPeakRooms)
        : opp.peakRooms != null
          ? Number(opp.peakRooms)
          : undefined,
    [F.roomDemandStatus]: asSelect(opp.roomDemandStatus, VAL_GDI_ROOM_DEMAND),
    [F.hotelFit]:
      opp.hotelFitScore != null ? Number(opp.hotelFitScore) : undefined,
    [F.qualification]: asSelect(
      opp.opportunityQualification || opp.qualification,
      VAL_QUALIFICATION
    ),
    [F.evidenceConfidence]:
      opp.evidenceConfidence != null
        ? Number(opp.evidenceConfidence)
        : undefined,
    [F.whyNow]: opp.whyNow || null,
    [F.whyThisMatters]:
      opp.summaryWhyMatters || opp.whyThisMatters || opp.summaryWhyHotel || null,
    [F.hotelWinThesis]:
      opp.hotelOpportunityThesis || opp.hotelWinThesis || null,
    [F.recommendedAction]: opp.recommendedAction || null,
    [F.primaryContactId]: contactField(opp, "id"),
    [F.primaryContactName]: contactField(opp, "name"),
    [F.primaryContactRole]: contactField(opp, "role"),
    [F.primaryContactEmail]: contactField(opp, "email"),
    [F.primaryContactPhone]: contactField(opp, "phone"),
    [F.contactQuality]:
      opp.contactQuality || opp.primaryContact?.contactQuality || null,
    [F.sourceCount]: Array.isArray(opp.sources)
      ? opp.sources.length
      : opp.sourceCount != null
        ? Number(opp.sourceCount)
        : undefined,
    [F.sourceSummary]: sourceSummaryFromOpp(opp),
    [F.lastResearchedAt]:
      opp.lastVerifiedAt || opp.lastResearchedAt || opp.updatedAt || null,
    [F.researchVersion]:
      opp.researchVersion || meta.researchVersion || meta.runId || null,
    [F.decisionId]: opp.decisionId || meta.decisionId || null,
    [F.createdAt]: opp.createdAt || opp.firstSeenAt || null,
    [F.updatedAt]: opp.updatedAt || new Date().toISOString(),
    [F.schemaVersion]:
      opp.schemaVersion || GDI_OPPORTUNITY_SCHEMA_VERSION,
    [F.opportunityPayloadJson]: jsonText(payload),
    [F.hotelIdentityStatus]: meta.hotelIdentityStatus || opp.hotelIdentityStatus || null,
    [F.runId]: meta.runId || opp.runId || null,
    [F.peVenueId]: opp.peVenueId || null,
    [F.peSignalId]: opp.peSignalId || null,
    [F.hotelVenueFitId]: opp.hotelVenueFitId || null,
    [F.demandFamily]: opp.demandFamily || null,
    [F.demandSignalType]: opp.demandSignalType || null,
    // Optional Airtable checkbox; primary signal also lives in opportunityPayloadJson
    ...(opp.isTestData === true ? { [F.isTestData]: true } : {}),
  });
}

/** Reconstruct opportunity from Airtable (prefer payload JSON). */
export function airtableRecordToOpportunity(record) {
  if (!record) return null;
  const f = record.fields || {};
  const payload = parseJson(f[F.opportunityPayloadJson], null);
  if (payload && typeof payload === "object") {
    return {
      ...payload,
      id: payload.id || f[F.opportunityId],
      hotelId: payload.hotelId || f[F.hotelId],
      // Prefer live Airtable columns for linkage / status freshness
      decisionId: f[F.decisionId] || payload.decisionId || null,
      priority: f[F.priority] || payload.priority || null,
      opportunityType: f[F.opportunityType] || payload.opportunityType || null,
      peVenueId: f[F.peVenueId] || payload.peVenueId || null,
      peSignalId: f[F.peSignalId] || payload.peSignalId || null,
      hotelVenueFitId: f[F.hotelVenueFitId] || payload.hotelVenueFitId || null,
      demandFamily: f[F.demandFamily] || payload.demandFamily || null,
      demandSignalType: f[F.demandSignalType] || payload.demandSignalType || null,
      isTestData:
        f[F.isTestData] === true ||
        payload.isTestData === true ||
        false,
      customerVisible:
        payload.customerVisible === false
          ? false
          : f[F.isTestData] === true
            ? false
            : payload.customerVisible !== false,
      updatedAt: f[F.updatedAt] || payload.updatedAt || null,
      lastVerifiedAt: f[F.lastResearchedAt] || payload.lastVerifiedAt || null,
      _airtableRecordId: record.id,
    };
  }
  return {
    id: f[F.opportunityId],
    hotelId: f[F.hotelId],
    organizationId: f[F.organizationId] || null,
    title: f[F.opportunityName] || null,
    organizationName: f[F.organizationName] || null,
    opportunityType: f[F.opportunityType] || null,
    priority: f[F.priority] || null,
    venueSourcingStatus: f[F.venueStatus] || null,
    sourcingStatus: f[F.sourcingStatus] || null,
    roomDemandStatus: f[F.roomDemandStatus] || null,
    hotelFitScore: f[F.hotelFit] ?? null,
    opportunityQualification: f[F.qualification] || null,
    evidenceConfidence: f[F.evidenceConfidence] ?? null,
    whyNow: f[F.whyNow] || null,
    summaryWhyMatters: f[F.whyThisMatters] || null,
    hotelOpportunityThesis: f[F.hotelWinThesis] || null,
    recommendedAction: f[F.recommendedAction] || null,
    primaryContactId: f[F.primaryContactId] || null,
    primaryContact: {
      name: f[F.primaryContactName] || null,
      role: f[F.primaryContactRole] || null,
      email: f[F.primaryContactEmail] || null,
      phone: f[F.primaryContactPhone] || null,
      contactQuality: f[F.contactQuality] || null,
    },
    contactQuality: f[F.contactQuality] || null,
    eventStartDate: f[F.eventStartDate] || null,
    eventEndDate: f[F.eventEndDate] || null,
    segment: f[F.segment] || null,
    decisionId: f[F.decisionId] || null,
    createdAt: f[F.createdAt] || null,
    updatedAt: f[F.updatedAt] || null,
    _airtableRecordId: record.id,
  };
}

async function selectAll(opts = {}) {
  const out = [];
  await table()
    .select({ pageSize: 100, ...opts })
    .eachPage((records, next) => {
      out.push(...records);
      next();
    });
  return out;
}

export async function findOpportunityRecordById(opportunityId) {
  const id = String(opportunityId || "").trim();
  if (!id) return null;
  const formula = `{${F.opportunityId}}='${escapeAirtableFormulaValue(id)}'`;
  const rows = await table()
    .select({ filterByFormula: formula, maxRecords: 1 })
    .firstPage();
  return rows[0] || null;
}

export async function loadOpportunity(hotelId, opportunityId) {
  const rec = await findOpportunityRecordById(opportunityId);
  if (!rec) return null;
  const opp = airtableRecordToOpportunity(rec);
  if (hotelId && opp.hotelId && String(hotelId) !== String(opp.hotelId)) {
    const err = new Error("hotel_boundary_violation");
    err.code = "hotel_boundary_violation";
    throw err;
  }
  return opp;
}

export async function listOpportunitiesForHotel(hotelId) {
  const formula = `{${F.hotelId}}='${escapeAirtableFormulaValue(hotelId)}'`;
  const rows = await selectAll({ filterByFormula: formula });
  return rows.map(airtableRecordToOpportunity).filter(Boolean);
}

/**
 * Upsert by opportunityId. Idempotent.
 */
export async function upsertOpportunity(opp, meta = {}) {
  const opportunityId = opp.id || opp.opportunityId;
  if (!opportunityId) {
    const err = new Error("opportunity_id_required");
    err.code = "opportunity_id_required";
    throw err;
  }
  const fields = opportunityToAirtableFields(
    { ...opp, id: opportunityId },
    meta
  );
  const existing = await findOpportunityRecordById(opportunityId);
  if (existing) {
    // Hotel boundary: do not overwrite another hotel's row
    const existingHotel = existing.fields?.[F.hotelId];
    const nextHotel = fields[F.hotelId];
    if (
      existingHotel &&
      nextHotel &&
      String(existingHotel) !== String(nextHotel)
    ) {
      const err = new Error("opportunity_hotel_conflict");
      err.code = "opportunity_hotel_conflict";
      err.existingHotel = existingHotel;
      err.nextHotel = nextHotel;
      throw err;
    }
    await table().update(existing.id, fields);
    return {
      opportunity: { ...opp, id: opportunityId, _airtableRecordId: existing.id },
      created: false,
      airtableRecordId: existing.id,
    };
  }
  const created = await table().create(fields);
  return {
    opportunity: { ...opp, id: opportunityId, _airtableRecordId: created.id },
    created: true,
    airtableRecordId: created.id,
  };
}

export async function upsertOpportunitiesBatch(hotelId, opportunities, meta = {}) {
  const results = [];
  for (const opp of opportunities || []) {
    results.push(
      await upsertOpportunity(
        { ...opp, hotelId: opp.hotelId || hotelId },
        { ...meta, hotelId }
      )
    );
  }
  return results;
}

export async function setOpportunityDecisionId(opportunityId, decisionId) {
  const existing = await findOpportunityRecordById(opportunityId);
  if (!existing) return null;
  await table().update(existing.id, {
    [F.decisionId]: decisionId || null,
    [F.updatedAt]: new Date().toISOString(),
  });
  return airtableRecordToOpportunity(
    await findOpportunityRecordById(opportunityId)
  );
}
