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

/** Airtable Date columns accept YYYY-MM-DD only — never ranges or prose. */
function sanitizeAirtableDate(value) {
  if (value == null || value === "") return null;
  const s = String(value).trim();
  const m = s.match(/^(\d{4}-\d{2}-\d{2})/);
  if (!m) return null;
  const d = m[1];
  const t = Date.parse(`${d}T00:00:00Z`);
  if (Number.isNaN(t)) return null;
  return d;
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

function omitEmpty(fields, { allowNullKeys = [] } = {}) {
  const allow = new Set(allowNullKeys);
  const out = {};
  for (const [k, v] of Object.entries(fields)) {
    if (v === undefined || v === "") continue;
    if (v === null && !allow.has(k)) continue;
    out[k] = v;
  }
  return out;
}

/** Airtable long-text max; stay under to avoid silent/422 write failures. */
export const AIRTABLE_LONG_TEXT_SAFE_CHARS = 95_000;

/**
 * Strip bulky internal research artifacts from the Airtable payload while
 * preserving customer-facing sales fields. Full opportunity remains on disk /
 * in-memory for research continuity — this compaction is write-path only.
 */
export function compactOpportunityPayloadForAirtable(opp = {}) {
  if (!opp || typeof opp !== "object") return opp;
  const INTERNAL_DROP_KEYS = new Set([
    "supervisorV2",
    "researchAttempts",
    "pendingDiscoverySearches",
    "investigatedUrls",
    "deepResearchEnrichment",
    "targetedEnrichmentRequest",
    "publicBuyerContactEnrichment",
    "lastRetrievalCommercialValue",
    "orsTranscript",
    "orsRaw",
    "longformDossier",
    "dossierRaw",
    "retrievalLog",
    "plannerTrace",
    "exaRawHits",
    "firecrawlRaw",
    "ledgerEntries",
  ]);
  const next = { ...opp };
  for (const k of INTERNAL_DROP_KEYS) {
    if (k in next) delete next[k];
  }
  // Cap oversized nested blobs that still matter for UI but can bloat.
  if (Array.isArray(next.evidence) && next.evidence.length > 40) {
    next.evidence = next.evidence.slice(0, 40);
  }
  if (Array.isArray(next.activityHistory) && next.activityHistory.length > 20) {
    next.activityHistory = next.activityHistory.slice(-20);
  }
  if (Array.isArray(next.publicationEvents) && next.publicationEvents.length > 20) {
    next.publicationEvents = next.publicationEvents.slice(-20);
  }
  if (typeof next.notes === "string" && next.notes.length > 4000) {
    next.notes = `${next.notes.slice(0, 4000)}…`;
  }
  if (typeof next.researchCompleteness === "object" && next.researchCompleteness) {
    next.researchCompleteness = {
      status: next.researchCompleteness.status || null,
      summary: next.researchCompleteness.summary || null,
      version: next.researchCompleteness.version || null,
    };
  }
  let json = "";
  try {
    json = JSON.stringify(next);
  } catch {
    return next;
  }
  if (json.length <= AIRTABLE_LONG_TEXT_SAFE_CHARS) return next;

  // Second-pass emergency trim: drop remaining large non-customer keys.
  const SECONDARY_DROP = [
    "v3SalesAction",
    "v2NextSalesAction",
    "revenueOpportunities",
    "namedStakeholders",
    "unresolvedQuestions",
    "qualificationV2",
    "qualificationV3",
    "deepResearchResultContract",
  ];
  for (const k of SECONDARY_DROP) {
    if (k in next) delete next[k];
  }
  try {
    json = JSON.stringify(next);
  } catch {
    return next;
  }
  if (json.length <= AIRTABLE_LONG_TEXT_SAFE_CHARS) return next;

  // Last resort: keep identity + customer-facing sales presentation only.
  // sharedTenSectionDrawer is rebuilt at share serve-time from CIB/salesBrief.
  const slimBrief = slimCommercialBriefForAirtable(
    next.commercialIntelligenceBrief || next.salesBrief || null,
  );
  const minimal = {
    id: next.id || next.opportunityId,
    opportunityId: next.opportunityId || next.id,
    hotelId: next.hotelId,
    organizationName: next.organizationName,
    company: next.company,
    title: next.title || next.opportunityName,
    opportunityName: next.opportunityName || next.title,
    opportunityType: next.opportunityType,
    detailLayout: next.detailLayout,
    demandFamily: next.demandFamily,
    demandSignalType: next.demandSignalType,
    segment: next.segment,
    researchSegment: next.researchSegment,
    customerVisible: next.customerVisible,
    customerFacingState: next.customerFacingState,
    prospectPublicationBasis: next.prospectPublicationBasis,
    prospectHotelFacing: next.prospectHotelFacing,
    prospectReviewSection: next.prospectReviewSection,
    prospectCommercialLabel: next.prospectCommercialLabel,
    prospectPublicationWithdrawn: next.prospectPublicationWithdrawn,
    prospectSuppressed: next.prospectSuppressed,
    primaryContact: next.primaryContact,
    secondaryContact: next.secondaryContact,
    contactEmail: next.contactEmail,
    contactPhone: next.contactPhone,
    contactName: next.contactName,
    contactRole: next.contactRole,
    contactTitle: next.contactTitle,
    commercialContactPath: next.commercialContactPath,
    organizationContactEmail: next.organizationContactEmail,
    functionalContactEmail: next.functionalContactEmail,
    companyPhone: next.companyPhone,
    contactUrl: next.contactUrl,
    activityHeadline: next.activityHeadline,
    whatIsHappening: next.whatIsHappening,
    potentialHotelBusiness: next.potentialHotelBusiness,
    demandStageLabel: next.demandStageLabel,
    summaryWhat: next.summaryWhat,
    summaryWhyMatters: next.summaryWhyMatters,
    summaryWhyHotel: next.summaryWhyHotel,
    whyNow: next.whyNow,
    whyThisHotel: next.whyThisHotel,
    whyDealalitySelectedThisCompany: next.whyDealalitySelectedThisCompany,
    companyBackground: next.companyBackground,
    growthEvidenceDate: next.growthEvidenceDate,
    activityStatus: next.activityStatus,
    salesBrief: slimBrief,
    commercialIntelligenceBrief: slimBrief,
    researchToDrawerV2: null,
    scalingDrawerModel: null,
    sharedTenSectionDrawer: null,
    segmentOverviewModel: next.segmentOverviewModel || null,
    demandSegmentId: next.demandSegmentId || next.researchSegment || null,
    recommendedAction: next.recommendedAction,
    recommendedNextStep: next.recommendedNextStep,
    namedPersonResearchAttempted: next.namedPersonResearchAttempted,
    whoResearchStatus: next.whoResearchStatus,
    researchLedRemediationFingerprint: next.researchLedRemediationFingerprint,
    sources: Array.isArray(next.sources) ? next.sources.slice(0, 12) : next.sources,
    officialSource: next.officialSource,
    hotelDisplayName: next.hotelDisplayName,
    scalingSalesPresentationVersion: next.scalingSalesPresentationVersion,
    schemaVersion: next.schemaVersion,
    researchVersion: next.researchVersion,
    lastVerifiedAt: next.lastVerifiedAt,
    updatedAt: next.updatedAt,
    firstSeenAt: next.firstSeenAt,
    _airtablePayloadCompacted: true,
    _airtablePayloadCompactReason: "exceeded_long_text_safe_chars",
  };
  try {
    if (JSON.stringify(minimal).length <= AIRTABLE_LONG_TEXT_SAFE_CHARS) {
      return minimal;
    }
  } catch {
    return minimal;
  }
  // Absolute floor: drop brief blobs if still over Airtable long-text ceiling.
  minimal.salesBrief = null;
  minimal.commercialIntelligenceBrief = null;
  minimal.segmentOverviewModel = null;
  minimal.sources = Array.isArray(minimal.sources)
    ? minimal.sources.slice(0, 6)
    : null;
  minimal._airtablePayloadCompactReason =
    "exceeded_long_text_safe_chars_floor";
  return minimal;
}

/** Keep CIB/salesBrief narrative; drop bulky evidence arrays for Airtable. */
function slimCommercialBriefForAirtable(brief) {
  if (!brief || typeof brief !== "object") return brief || null;
  const out = { ...brief };
  for (const k of [
    "supportingEvidence",
    "supportingEvidenceAndUncertainties",
    "sourceMapping",
    "evidence",
    "sources",
    "qualificationQuestions",
  ]) {
    if (Array.isArray(out[k]) && out[k].length > 8) out[k] = out[k].slice(0, 8);
  }
  if (out.eventsOpportunityOverview && typeof out.eventsOpportunityOverview === "object") {
    const eo = { ...out.eventsOpportunityOverview };
    if (Array.isArray(eo.evidence) && eo.evidence.length > 6) {
      eo.evidence = eo.evidence.slice(0, 6);
    }
    out.eventsOpportunityOverview = eo;
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
  if (key === "role") return c.role || c.contactRole || opp.primaryContactRole || null;
  if (key === "email") return c.email || opp.primaryContactEmail || opp.functionalContactEmail || opp.contactEmail || null;
  if (key === "phone") return c.phone || opp.primaryContactPhone || null;
  return null;
}

/** Map in-memory opportunity → Airtable columns (+ payload for fidelity). */
export function opportunityToAirtableFields(opp, meta = {}) {
  // Compact payload for Airtable long-text limit; full opp stays in caller memory/disk.
  const payload = compactOpportunityPayloadForAirtable({ ...opp });
  return omitEmpty(
    {
      [F.opportunityId]: opp.id || opp.opportunityId,
      [F.hotelId]: opp.hotelId || meta.hotelId,
      [F.organizationId]: opp.organizationId || null,
      [F.opportunityName]: opp.title || opp.opportunityName || null,
      [F.organizationName]: opp.organizationName || null,
      [F.organizationEntityId]: opp.organizationEntityId || opp.organizationId || null,
      [F.opportunityType]: asSelect(opp.opportunityType, VAL_GDI_OPPORTUNITY_TYPE_AIRTABLE_SAFE),
      [F.priority]: asSelect(opp.priority, VAL_GDI_PRIORITY),
      [F.actionStatus]: opp.actionStatus || opp.salesActionStatus || opp.demandStatus || null,
      [F.eventStartDate]: sanitizeAirtableDate(
        opp.eventStartDate || opp.startDate || opp.dates?.start || null
      ),
      [F.eventEndDate]: sanitizeAirtableDate(
        opp.eventEndDate || opp.endDate || opp.dates?.end || null
      ),
      [F.segment]: opp.segment || opp.demandType || null,
      [F.territory]: opp.territory || opp.demandTerritoryLabel || opp.demandTerritory || null,
      [F.venueStatus]: asSelect(opp.venueSourcingStatus || opp.venueStatus, VAL_GDI_VENUE_STATUS),
      // sourcingStatus may be research taxonomy ≠ venue enum — only write when known
      [F.sourcingStatus]: asSelect(opp.sourcingStatus, VAL_GDI_VENUE_STATUS),
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
      // When overall numeric is withheld, clear the flat Hotel Fit column so
      // stale scores cannot contradict opportunityPayloadJson / drawer state.
      [F.hotelFit]:
        opp.hotelFitScore != null
          ? Number(opp.hotelFitScore)
          : opp.hotelFitOverallStatus === "insufficient_evidence" ||
              opp.hotelFitCalculationBasis?.numericScoreEmitted === false
            ? null
            : undefined,
      [F.qualification]: asSelect(
        opp.opportunityQualification || opp.qualification,
        VAL_QUALIFICATION
      ),
      [F.evidenceConfidence]:
        opp.evidenceConfidence != null ? Number(opp.evidenceConfidence) : undefined,
      [F.whyNow]: opp.whyNow || null,
      [F.whyThisMatters]:
        opp.summaryWhyMatters || opp.whyThisMatters || opp.summaryWhyHotel || null,
      [F.hotelWinThesis]: opp.hotelOpportunityThesis || opp.hotelWinThesis || null,
      [F.recommendedAction]: opp.recommendedAction || null,
      [F.primaryContactId]: contactField(opp, "id"),
      [F.primaryContactName]: contactField(opp, "name"),
      [F.primaryContactRole]: contactField(opp, "role"),
      [F.primaryContactEmail]: contactField(opp, "email"),
      [F.primaryContactPhone]: contactField(opp, "phone"),
      [F.contactQuality]: opp.contactQuality || opp.primaryContact?.contactQuality || null,
      [F.sourceCount]: Array.isArray(opp.sources)
        ? opp.sources.length
        : opp.sourceCount != null
          ? Number(opp.sourceCount)
          : undefined,
      [F.sourceSummary]: sourceSummaryFromOpp(opp),
      [F.lastResearchedAt]: opp.lastVerifiedAt || opp.lastResearchedAt || opp.updatedAt || null,
      [F.researchVersion]: opp.researchVersion || meta.researchVersion || meta.runId || null,
      [F.decisionId]: opp.decisionId || meta.decisionId || null,
      [F.createdAt]: opp.createdAt || opp.firstSeenAt || null,
      [F.updatedAt]: opp.updatedAt || new Date().toISOString(),
      [F.schemaVersion]: opp.schemaVersion || GDI_OPPORTUNITY_SCHEMA_VERSION,
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
      // Discovery provenance lives in opportunityPayloadJson (schema-safe).
      // Optional Airtable columns exist in MAP for future probe-gated writes.
    },
    { allowNullKeys: [F.hotelFit] }
  );
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
      isTestData: f[F.isTestData] === true || payload.isTestData === true || false,
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
  const rows = await table().select({ filterByFormula: formula, maxRecords: 1 }).firstPage();
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
  const fields = opportunityToAirtableFields({ ...opp, id: opportunityId }, meta);
  const existing = await findOpportunityRecordById(opportunityId);
  if (existing) {
    // Hotel boundary: do not overwrite another hotel's row
    const existingHotel = existing.fields?.[F.hotelId];
    const nextHotel = fields[F.hotelId];
    if (existingHotel && nextHotel && String(existingHotel) !== String(nextHotel)) {
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
      await upsertOpportunity({ ...opp, hotelId: opp.hotelId || hotelId }, { ...meta, hotelId })
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
  return airtableRecordToOpportunity(await findOpportunityRecordById(opportunityId));
}
