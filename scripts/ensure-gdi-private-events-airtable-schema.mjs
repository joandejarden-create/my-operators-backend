/**
 * Ensure Private Events V1.1 Airtable tables on canonical GDI intelligence base.
 *
 * Tables:
 *   - Private Event Venues (CREATE)
 *   - Hotel Venue Fit (CREATE)
 *   - Private Event Signals (CREATE)
 *   - Group Demand Opportunities (EXTEND link fields + opportunityType choices)
 *
 * Usage:
 *   node scripts/ensure-gdi-private-events-airtable-schema.mjs
 *   node scripts/ensure-gdi-private-events-airtable-schema.mjs --apply
 *
 * Never uses Deal Capture MVP (appvtnDurnMSjINP6).
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  getGdiOpportunitiesAirtableBaseId,
  assertNotLegacyMvpCanonicalBase,
  CANONICAL_INTELLIGENCE_BASE_ID,
} from "../lib/decision-outcomes/airtable-base.js";
import {
  GDI_OPPORTUNITIES_TABLE_NAME,
  MAP_GDI_OPPORTUNITY,
  VAL_GDI_OPPORTUNITY_TYPE,
} from "../lib/group-demand-intelligence/opportunity-field-map.js";
import {
  PE_VENUES_TABLE_NAME,
  HOTEL_VENUE_FIT_TABLE_NAME,
  PE_SIGNALS_TABLE_NAME,
  MAP_PE_VENUE,
  MAP_HOTEL_VENUE_FIT,
  MAP_PE_SIGNAL,
  MAP_GDI_PE_LINK,
  VAL_PE_VENUE_TYPE,
  VAL_ON_SITE_LODGING,
  VAL_RESEARCH_STATUS,
  VAL_ANNUAL_EVENT_VOLUME_STATUS,
  VAL_CANONICAL_STATUS,
  VAL_LODGING_CATCHMENT_FIT,
  VAL_PRODUCT_FIT,
  VAL_PARTNERSHIP_POTENTIAL,
  VAL_LODGING_CAPTURE_POTENTIAL,
  VAL_SIGNAL_STATUS,
  VAL_ROOM_DEMAND_CLAIM,
  VAL_ATTENDANCE_STATUS,
  VAL_PE_DEMAND_SIGNAL_TYPE,
  VAL_EVENT_ACTIVITY_EVIDENCE_STATUS,
  VAL_PARTNER_STATUS,
  PE_AIRTABLE_SCHEMA_VERSION,
} from "../lib/group-demand-intelligence/private-events/airtable-field-map.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const REPORT_PATH = path.join(
  ROOT,
  "reports",
  "group-demand-intelligence",
  "private-events-v1-1",
  "SCHEMA_DECISION.json"
);

function choices(names) {
  return { choices: names.map((name) => ({ name })) };
}
function singleSelect(name, optionNames, description) {
  const field = { name, type: "singleSelect", options: choices(optionNames) };
  if (description) field.description = description;
  return field;
}
function checkbox(name, description) {
  const field = { name, type: "checkbox", options: { color: "greenBright", icon: "check" } };
  if (description) field.description = description;
  return field;
}
function dateTimeField(name, description) {
  const field = {
    name,
    type: "dateTime",
    options: {
      dateFormat: { name: "iso" },
      timeFormat: { name: "24hour" },
      timeZone: "utc",
    },
  };
  if (description) field.description = description;
  return field;
}
function dateField(name, description) {
  const field = { name, type: "date", options: { dateFormat: { name: "iso" } } };
  if (description) field.description = description;
  return field;
}
function numberField(name, precision = 0, description) {
  const field = { name, type: "number", options: { precision } };
  if (description) field.description = description;
  return field;
}
function singleLine(name, description) {
  const field = { name, type: "singleLineText" };
  if (description) field.description = description;
  return field;
}
function multiline(name, description) {
  const field = { name, type: "multilineText" };
  if (description) field.description = description;
  return field;
}
function urlField(name, description) {
  const field = { name, type: "url" };
  if (description) field.description = description;
  return field;
}
function emailField(name, description) {
  const field = { name, type: "email" };
  if (description) field.description = description;
  return field;
}
function phoneField(name, description) {
  const field = { name, type: "phoneNumber" };
  if (description) field.description = description;
  return field;
}
function linkField(name, linkedTableId, description) {
  const field = {
    name,
    type: "multipleRecordLinks",
    options: { linkedTableId },
  };
  if (description) field.description = description;
  return field;
}

async function metaFetch(baseId, token, metaPath, init = {}) {
  const url = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}${metaPath}`;
  const res = await fetch(url, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }
  return { res, json };
}

function findTable(tables, nameOrId) {
  return (tables || []).find((t) => t.name === nameOrId || t.id === nameOrId) || null;
}

function existingFieldNames(table) {
  return new Set((table.fields || []).map((f) => f.name));
}

function getPrimaryField(table) {
  if (!table) return null;
  return (
    (table.fields || []).find((f) => f.id === table.primaryFieldId) ||
    (table.fields || [])[0] ||
    null
  );
}

function venueFields() {
  const V = MAP_PE_VENUE;
  return [
    singleLine(V.venueId, "Stable pev_… canonical venue id (primary)"),
    singleLine(V.venueName),
    multiline(V.venueAliases),
    singleSelect(V.venueType, VAL_PE_VENUE_TYPE),
    singleLine(V.address),
    singleLine(V.city),
    singleLine(V.region),
    singleLine(V.postalCode),
    singleLine(V.country),
    numberField(V.latitude, 6),
    numberField(V.longitude, 6),
    urlField(V.website),
    singleLine(V.officialDomain),
    numberField(V.minCapacity, 0),
    numberField(V.maxCapacity, 0),
    singleSelect(V.onSiteLodgingStatus, VAL_ON_SITE_LODGING),
    numberField(V.onSiteGuestrooms, 0),
    checkbox(V.weddingsAdvertised),
    checkbox(V.privateEventsAdvertised),
    numberField(V.estimatedAnnualPrivateEvents, 0),
    singleSelect(V.annualEventVolumeStatus, VAL_ANNUAL_EVENT_VOLUME_STATUS),
    checkbox(V.preferredHotelListed),
    checkbox(V.exclusiveHotelRelationship),
    multiline(V.knownHotelPartners),
    singleSelect(V.eventActivityEvidenceStatus, VAL_EVENT_ACTIVITY_EVIDENCE_STATUS),
    multiline(V.activityEvidenceJson),
    singleSelect(V.partnerStatus, VAL_PARTNER_STATUS),
    singleLine(V.eventContactName),
    singleLine(V.eventContactRole),
    emailField(V.eventContactEmail),
    phoneField(V.eventContactPhone),
    multiline(V.sourceUrls),
    urlField(V.primarySourceUrl),
    singleLine(V.sourceAuthority),
    singleSelect(V.researchStatus, VAL_RESEARCH_STATUS),
    numberField(V.confidence, 2),
    dateTimeField(V.firstSeen),
    dateTimeField(V.lastSeen),
    dateTimeField(V.lastVerified),
    singleSelect(V.canonicalStatus, VAL_CANONICAL_STATUS),
    singleLine(V.matchedBy),
    singleLine(V.matchConfidence),
    multiline(V.mergeReason),
    singleLine(V.schemaVersion),
    multiline(V.venuePayloadJson),
  ];
}

function fitFields(venueTableId) {
  const H = MAP_HOTEL_VENUE_FIT;
  const fields = [
    singleLine(H.fitId, "Stable hvf_… hotel↔venue fit id (primary)"),
    singleLine(H.hotelId, "Canonical HPC / GDI hotel id"),
    singleLine(H.hotelName),
    singleLine(H.venueId),
    singleLine(H.venueName),
    numberField(H.distanceMiles, 1),
    numberField(H.distanceKm, 1),
    numberField(H.driveTimeMinutes, 0),
    singleSelect(H.lodgingCatchmentFit, VAL_LODGING_CATCHMENT_FIT),
    singleSelect(H.productFit, VAL_PRODUCT_FIT),
    singleSelect(H.partnershipPotential, VAL_PARTNERSHIP_POTENTIAL),
    singleSelect(H.lodgingCapturePotential, VAL_LODGING_CAPTURE_POTENTIAL),
    singleLine(H.existingHotelRelationshipStatus),
    singleLine(H.preferredPartnerStatus),
    multiline(H.fitRationale),
    multiline(H.whyHotelCouldWin),
    multiline(H.constraints),
    singleLine(H.currentGdiOpportunityId),
    singleLine(H.currentGdiOpportunityStatus),
    dateTimeField(H.firstEvaluated),
    dateTimeField(H.lastEvaluated),
    dateTimeField(H.lastChanged),
    multiline(H.evidenceUrls),
    numberField(H.confidence, 2),
    singleLine(H.schemaVersion),
    multiline(H.fitPayloadJson),
  ];
  if (venueTableId) {
    fields.splice(5, 0, linkField(H.venueLink, venueTableId, "Link to Private Event Venues"));
  }
  return fields;
}

function signalFields(venueTableId) {
  const S = MAP_PE_SIGNAL;
  const fields = [
    singleLine(S.signalId, "Stable pes_… signal id (primary)"),
    singleLine(S.venueId),
    singleLine(S.venueName),
    singleSelect(S.demandSignalType, VAL_PE_DEMAND_SIGNAL_TYPE),
    singleLine(S.eventName),
    singleLine(S.eventType),
    dateField(S.eventStartDate),
    dateField(S.eventEndDate),
    singleLine(S.dateGranularity),
    numberField(S.estimatedAttendance, 0),
    singleSelect(S.attendanceStatus, VAL_ATTENDANCE_STATUS),
    numberField(S.potentialRoomsLow, 0),
    numberField(S.potentialRoomsHigh, 0),
    singleSelect(S.roomDemandStatus, VAL_ROOM_DEMAND_CLAIM),
    checkbox(S.lodgingMentioned),
    checkbox(S.roomBlockMentioned),
    checkbox(S.hotelMentioned),
    checkbox(S.transportationMentioned),
    singleLine(S.plannerCompany),
    singleLine(S.plannerName),
    singleLine(S.plannerRole),
    urlField(S.sourceUrl),
    singleLine(S.sourceType),
    singleLine(S.sourceAuthority),
    singleSelect(S.signalStatus, VAL_SIGNAL_STATUS),
    multiline(S.hotelDemandThesis),
    multiline(S.evidenceText),
    dateTimeField(S.firstSeen),
    dateTimeField(S.lastSeen),
    dateTimeField(S.lastVerified),
    singleLine(S.promotedGdiOpportunityId),
    singleLine(S.hotelVenueFitId),
    singleLine(S.schemaVersion),
    multiline(S.signalPayloadJson),
  ];
  if (venueTableId) {
    fields.splice(3, 0, linkField(S.venueLink, venueTableId, "Link to Private Event Venues"));
  }
  return fields;
}

function gdiPeExtensionFields() {
  return [
    singleLine(MAP_GDI_PE_LINK.peVenueId, "Private Events venue id (pev_)"),
    singleLine(MAP_GDI_PE_LINK.peSignalId, "Private Events signal id (pes_)"),
    singleLine(MAP_GDI_PE_LINK.hotelVenueFitId, "Hotel Venue Fit id (hvf_)"),
    singleLine(MAP_GDI_PE_LINK.demandFamily),
    singleLine(MAP_GDI_PE_LINK.demandSignalType),
  ];
}

async function createField(baseId, token, tableId, fieldSpec, entry) {
  if (!APPLY) {
    entry.wouldCreateFields.push(fieldSpec.name);
    return { ok: true, dryRun: true };
  }
  const { res, json } = await metaFetch(baseId, token, `/tables/${tableId}/fields`, {
    method: "POST",
    body: JSON.stringify(fieldSpec),
  });
  if (!res.ok) {
    entry.errors.push({ field: fieldSpec.name, status: res.status, error: json });
    return { ok: false, json };
  }
  entry.createdFields.push({ name: fieldSpec.name, id: json.id });
  return { ok: true, json };
}

async function ensureTable({
  baseId,
  token,
  tables,
  tableName,
  primaryFieldName,
  allFields,
  description,
  report,
}) {
  const entry = {
    tableName,
    tableId: null,
    createdTable: false,
    wouldCreateTable: false,
    wouldCreateFields: [],
    createdFields: [],
    skippedExisting: [],
    errors: [],
    decision: "UNKNOWN",
  };
  report.tables.push(entry);

  let table = findTable(tables, tableName);
  if (table) {
    entry.tableId = table.id;
    entry.decision = "REUSED / EXTEND";
    const primary = getPrimaryField(table);
    if (primary?.name !== primaryFieldName) {
      report.blocked = true;
      report.blockReasons.push(
        `Table "${tableName}" primary is "${primary?.name}", expected "${primaryFieldName}"`
      );
      entry.errors.push({ incompatiblePrimary: primary?.name });
      return entry;
    }
    const existing = existingFieldNames(table);
    for (const field of allFields) {
      if (existing.has(field.name)) entry.skippedExisting.push(field.name);
      else await createField(baseId, token, table.id, field, entry);
    }
    return entry;
  }

  entry.decision = "CREATED";
  if (!APPLY) {
    entry.wouldCreateTable = true;
    entry.wouldCreateFields = allFields.map((f) => f.name);
    return entry;
  }

  const { res, json } = await metaFetch(baseId, token, "/tables", {
    method: "POST",
    body: JSON.stringify({ name: tableName, description, fields: allFields }),
  });
  if (!res.ok) {
    entry.errors.push({ createTable: true, status: res.status, error: json });
    report.errors.push({ tableName, error: json });
    return entry;
  }
  table = json;
  tables.push(table);
  entry.createdTable = true;
  entry.tableId = table.id;
  entry.createdFields = (table.fields || []).map((f) => ({ name: f.name, id: f.id }));
  return entry;
}

async function ensureGdiOpportunityExtensions({ baseId, token, tables, report }) {
  const entry = {
    tableName: GDI_OPPORTUNITIES_TABLE_NAME,
    tableId: null,
    decision: "REUSED",
    wouldCreateFields: [],
    createdFields: [],
    skippedExisting: [],
    choiceExtensions: [],
    errors: [],
  };
  report.tables.push(entry);
  const table = findTable(tables, GDI_OPPORTUNITIES_TABLE_NAME);
  if (!table) {
    entry.errors.push({ missing: true });
    report.blocked = true;
    report.blockReasons.push("Group Demand Opportunities table missing");
    return entry;
  }
  entry.tableId = table.id;
  const existing = existingFieldNames(table);
  for (const field of gdiPeExtensionFields()) {
    if (existing.has(field.name)) entry.skippedExisting.push(field.name);
    else await createField(baseId, token, table.id, field, entry);
  }

  // Extend opportunityType choices if missing PE types
  const oppTypeField = (table.fields || []).find(
    (f) => f.name === MAP_GDI_OPPORTUNITY.opportunityType
  );
  if (oppTypeField?.type === "singleSelect") {
    const have = new Set((oppTypeField.options?.choices || []).map((c) => c.name));
    const missing = VAL_GDI_OPPORTUNITY_TYPE.filter((n) => !have.has(n));
    entry.choiceExtensions = missing;
    if (missing.length && APPLY) {
      // Airtable Meta API often rejects singleSelect choice PATCH on existing fields.
      // Preserve existing choices; record manual follow-up for PE opportunity types.
      entry.choiceExtensionsManualRequired = missing;
      entry.errors.push({
        opportunityTypeChoices: {
          note: "Meta API PATCH rejected for select choices — use typecast seed via scripts/repair-gdi-opportunity-type-airtable.mjs --apply-schema (or add SPECIFIC_PRIVATE_EVENT / VENUE_PARTNERSHIP manually in Airtable UI)",
          missing,
        },
      });
    }
  }
  return entry;
}

async function main() {
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const baseId = getGdiOpportunitiesAirtableBaseId();
  if (!token || !baseId) {
    throw new Error(
      `Set AIRTABLE_API_KEY/PAT and AIRTABLE_GDI_BASE_ID (target ${CANONICAL_INTELLIGENCE_BASE_ID})`
    );
  }
  assertNotLegacyMvpCanonicalBase(baseId, { surface: "gdi_private_events_schema" });

  const report = {
    generatedAt: new Date().toISOString(),
    mode: APPLY ? "APPLY" : "DRY_RUN",
    baseId,
    isCanonical: baseId === CANONICAL_INTELLIGENCE_BASE_ID,
    schemaVersion: PE_AIRTABLE_SCHEMA_VERSION,
    decision: {
      privateEventVenues: "CREATE (no existing global venue table)",
      hotelVenueFit: "CREATE (no existing hotel↔venue fit table)",
      privateEventSignals: "CREATE (no generic GDI signal table suitable)",
      gdiOpportunities: "REUSE + EXTEND (link fields + opportunityType choices)",
      separateOpportunityDb: "NO",
      legacyMvpBase: "REJECTED",
    },
    blocked: false,
    blockReasons: [],
    tables: [],
    errors: [],
  };

  const { res, json } = await metaFetch(baseId, token, "/tables");
  if (!res.ok) throw new Error(`meta tables failed: ${JSON.stringify(json)}`);
  const tables = json.tables || [];

  // 1. Venues
  const venueEntry = await ensureTable({
    baseId,
    token,
    tables,
    tableName: PE_VENUES_TABLE_NAME,
    primaryFieldName: MAP_PE_VENUE.venueId,
    allFields: venueFields(),
    description: "Global Private Event Venues — reusable across GDI hotels",
    report,
  });

  // Refresh tables after create
  let venueTableId = venueEntry.tableId;
  if (APPLY && venueEntry.createdTable) {
    const refreshed = await metaFetch(baseId, token, "/tables");
    tables.splice(0, tables.length, ...(refreshed.json.tables || []));
    venueTableId = findTable(tables, PE_VENUES_TABLE_NAME)?.id || venueTableId;
  }

  // 2. Hotel Venue Fit
  await ensureTable({
    baseId,
    token,
    tables,
    tableName: HOTEL_VENUE_FIT_TABLE_NAME,
    primaryFieldName: MAP_HOTEL_VENUE_FIT.fitId,
    allFields: fitFields(venueTableId),
    description: "Hotel ↔ Private Event Venue fit (hotel-specific)",
    report,
  });

  if (APPLY) {
    const refreshed = await metaFetch(baseId, token, "/tables");
    tables.splice(0, tables.length, ...(refreshed.json.tables || []));
    venueTableId = findTable(tables, PE_VENUES_TABLE_NAME)?.id || venueTableId;
  }

  // 3. Signals
  await ensureTable({
    baseId,
    token,
    tables,
    tableName: PE_SIGNALS_TABLE_NAME,
    primaryFieldName: MAP_PE_SIGNAL.signalId,
    allFields: signalFields(venueTableId),
    description: "Private Event Signals — pre-opportunity public demand signals",
    report,
  });

  // 4. Extend GDI Opportunities
  if (APPLY) {
    const refreshed = await metaFetch(baseId, token, "/tables");
    tables.splice(0, tables.length, ...(refreshed.json.tables || []));
  }
  await ensureGdiOpportunityExtensions({ baseId, token, tables, report });

  fs.mkdirSync(path.dirname(REPORT_PATH), { recursive: true });
  fs.writeFileSync(REPORT_PATH, JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));
  console.log(`\nWrote ${REPORT_PATH}`);
  if (report.blocked) process.exit(2);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
