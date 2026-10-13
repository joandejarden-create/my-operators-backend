/**
 * Idempotent Airtable upserts for Hotel Intelligence tables (ADP base).
 */

import Airtable from "airtable";
import { CANONICAL_INTELLIGENCE_BASE_ID } from "../../decision-outcomes/airtable-base.js";
import {
  HI_TABLES,
  MAP_COMMERCIAL_PROFILE as CP,
  MAP_EVENT_SPACE as ES,
  MAP_DEMAND_NODE as DN,
  MAP_SEASONALITY as SP,
  MAP_EVIDENCE as EV,
} from "./hotel-intelligence-schema-v1.js";

function getBaseId() {
  return (
    process.env.ADP_AIRTABLE_BASE_ID ||
    process.env.AIRTABLE_INTELLIGENCE_BASE_ID ||
    CANONICAL_INTELLIGENCE_BASE_ID ||
    "appa2cE7FTRmIbB32"
  ).trim();
}

function getToken() {
  return process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "";
}

function escapeFormula(s) {
  return String(s || "").replace(/"/g, '\\"');
}

function stripEmpty(fields) {
  const out = { ...fields };
  for (const k of Object.keys(out)) {
    if (out[k] === undefined || out[k] === null || out[k] === "") delete out[k];
  }
  return out;
}

function getBase() {
  const token = getToken();
  const baseId = getBaseId();
  if (!token) throw new Error("missing_airtable_token");
  return { base: new Airtable({ apiKey: token }).base(baseId), baseId };
}

async function findByKey(table, keyField, keyValue) {
  const rows = [];
  await table
    .select({
      filterByFormula: `{${keyField}} = "${escapeFormula(keyValue)}"`,
      maxRecords: 5,
    })
    .eachPage((records, next) => {
      for (const r of records) rows.push(r);
      next();
    });
  return rows[0] || null;
}

async function findAllByHpc(table, hpcField, hpcHotelId, extraFormula = null) {
  const rows = [];
  const formula = extraFormula
    ? `AND({${hpcField}} = "${escapeFormula(hpcHotelId)}", ${extraFormula})`
    : `{${hpcField}} = "${escapeFormula(hpcHotelId)}"`;
  await table
    .select({ filterByFormula: formula, pageSize: 100 })
    .eachPage((records, next) => {
      for (const r of records) rows.push(r);
      next();
    });
  return rows;
}

async function upsertByKey(table, keyField, keyValue, fields) {
  const existing = await findByKey(table, keyField, keyValue);
  const payload = stripEmpty(fields);
  if (!existing) {
    const created = await table.create([{ fields: payload }]);
    return { action: "create", id: created[0].id, fields: created[0].fields };
  }
  const updated = await table.update([{ id: existing.id, fields: payload }]);
  return { action: "update", id: updated[0].id, fields: updated[0].fields };
}

function commercialToFields(row) {
  return stripEmpty({
    [CP.profileKey]: row.profileKey,
    [CP.hpcHotelId]: row.hpcHotelId,
    [CP.dealalityHotelId]: row.dealalityHotelId,
    [CP.adpPropertyId]: row.adpPropertyId,
    [CP.hotelName]: row.hotelName,
    [CP.brand]: row.brand,
    [CP.roomsKeys]: row.roomsKeys,
    [CP.suites]: row.suites,
    [CP.singleBedRooms]: row.singleBedRooms,
    [CP.doubleBedRooms]: row.doubleBedRooms,
    [CP.floors]: row.floors,
    [CP.yearBuilt]: row.yearBuilt,
    [CP.yearRenovated]: row.yearRenovated,
    [CP.siteAcres]: row.siteAcres,
    [CP.parkingType]: row.parkingType,
    [CP.parkingDailyRate]: row.parkingDailyRate,
    [CP.busParking]: row.busParking,
    [CP.airportDistanceMiles]: row.airportDistanceMiles,
    [CP.resortFee]: row.resortFee,
    [CP.destinationFee]: row.destinationFee,
    [CP.meetingSpaceFlag]: row.meetingSpaceFlag,
    [CP.totalMeetingSpaceSqFt]: row.totalMeetingSpaceSqFt,
    [CP.meetingRoomCount]: row.meetingRoomCount,
    [CP.largestMeetingSpaceSqFt]: row.largestMeetingSpaceSqFt,
    [CP.largestEventCapacity]: row.largestEventCapacity,
    [CP.outdoorEventSpaceSqFt]: row.outdoorEventSpaceSqFt,
    [CP.semiPrivateSpaceSqFt]: row.semiPrivateSpaceSqFt,
    [CP.checkInTime]: row.checkInTime,
    [CP.checkOutTime]: row.checkOutTime,
    [CP.officialPropertyUrl]: row.officialPropertyUrl,
    [CP.officialEventsUrl]: row.officialEventsUrl,
    [CP.profileCompletenessPct]: row.profileCompletenessPct,
    [CP.lastResearchedAt]: row.lastResearchedAt || new Date().toISOString(),
    [CP.researchVersion]: row.researchVersion,
    [CP.researchStatus]: row.researchStatus,
    [CP.confidence]: row.confidence,
    [CP.conflictStatus]: row.conflictStatus,
    [CP.notes]: row.notes,
    [CP.schemaVersion]: row.schemaVersion,
  });
}

function eventSpaceToFields(row) {
  return stripEmpty({
    [ES.spaceKey]: row.spaceKey,
    [ES.hpcHotelId]: row.hpcHotelId,
    [ES.spaceName]: row.spaceName,
    [ES.spaceType]: row.spaceType,
    [ES.floor]: row.floor,
    [ES.sqFt]: row.sqFt,
    [ES.dimensions]: row.dimensions,
    [ES.ceilingHeight]: row.ceilingHeight,
    [ES.theaterCapacity]: row.theaterCapacity,
    [ES.classroomCapacity]: row.classroomCapacity,
    [ES.banquetCapacity]: row.banquetCapacity,
    [ES.receptionCapacity]: row.receptionCapacity,
    [ES.conferenceCapacity]: row.conferenceCapacity,
    [ES.uShapeCapacity]: row.uShapeCapacity,
    [ES.hollowSquareCapacity]: row.hollowSquareCapacity,
    [ES.boardroomCapacity]: row.boardroomCapacity,
    [ES.outdoor]: row.outdoor,
    [ES.naturalLight]: row.naturalLight,
    [ES.notes]: row.notes,
    [ES.sourceRecordId]: row.sourceRecordId,
    [ES.sourceUrl]: row.sourceUrl,
    [ES.lastVerifiedAt]: row.lastVerifiedAt || new Date().toISOString(),
    [ES.confidence]: row.confidence,
    [ES.active]: row.active !== false,
    [ES.schemaVersion]: row.schemaVersion,
  });
}

function demandNodeToFields(row) {
  return stripEmpty({
    [DN.nodeKey]: row.nodeKey,
    [DN.hpcHotelId]: row.hpcHotelId,
    [DN.demandNodeName]: row.demandNodeName,
    [DN.demandNodeType]: row.demandNodeType,
    [DN.address]: row.address,
    [DN.city]: row.city,
    [DN.state]: row.state,
    [DN.latitude]: row.latitude,
    [DN.longitude]: row.longitude,
    [DN.distanceMiles]: row.distanceMiles,
    [DN.driveTimeMinutes]: row.driveTimeMinutes,
    [DN.transitTimeMinutes]: row.transitTimeMinutes,
    [DN.segmentRelevance]: row.segmentRelevance,
    [DN.groupRelevance]: row.groupRelevance,
    [DN.transientRelevance]: row.transientRelevance,
    [DN.demandStrength]: row.demandStrength,
    [DN.whyRelevant]: row.whyRelevant,
    [DN.sourceUrl]: row.sourceUrl,
    [DN.sourceName]: row.sourceName,
    [DN.sourceType]: row.sourceType,
    [DN.lastVerifiedAt]: row.lastVerifiedAt || new Date().toISOString(),
    [DN.confidence]: row.confidence,
    [DN.active]: row.active !== false,
    [DN.schemaVersion]: row.schemaVersion,
  });
}

function evidenceToFields(row) {
  return stripEmpty({
    [EV.evidenceId]: row.evidenceId,
    [EV.hpcHotelId]: row.hpcHotelId,
    [EV.entityType]: row.entityType,
    [EV.entityRecordId]: row.entityRecordId,
    [EV.fieldName]: row.fieldName,
    [EV.valueObserved]:
      row.valueObserved == null ? undefined : String(row.valueObserved),
    [EV.sourceName]: row.sourceName,
    [EV.sourceType]: row.sourceType,
    [EV.sourceUrl]: row.sourceUrl,
    [EV.sourcePublishedDate]: row.sourcePublishedDate,
    [EV.retrievedAt]: row.retrievedAt || new Date().toISOString(),
    [EV.evidenceSnippet]: row.evidenceSnippet,
    [EV.confidence]: row.confidence,
    [EV.evidenceStrength]: row.evidenceStrength,
    [EV.current]: row.current !== false,
    [EV.supersededBy]: row.supersededBy,
    [EV.notes]: row.notes,
    [EV.schemaVersion]: row.schemaVersion,
  });
}

/**
 * Apply staged Bethesda (or any hotel) HI packet to Airtable.
 * Does not write seasonality/need periods unless provided (and we won't invent them).
 */
export async function applyHotelIntelligencePacket(packet, { dryRun = false } = {}) {
  const { base, baseId } = getBase();
  const summary = {
    ok: true,
    dryRun,
    baseId,
    commercial: null,
    eventSpaces: [],
    demandNodes: [],
    seasonality: [],
    evidence: [],
    conflicts: [],
    skipped: [],
  };

  if (dryRun) {
    summary.commercial = { action: "would_upsert", key: packet.commercial?.profileKey };
    summary.eventSpaces = (packet.eventSpaces || []).map((s) => ({
      action: "would_upsert",
      key: s.spaceKey,
    }));
    summary.demandNodes = (packet.demandNodes || []).map((n) => ({
      action: "would_upsert",
      key: n.nodeKey,
    }));
    summary.evidence = (packet.evidence || []).map((e) => ({
      action: "would_upsert",
      key: e.evidenceId,
    }));
    if (!(packet.seasonality || []).length) {
      summary.skipped.push("seasonality_empty_by_policy");
    }
    return summary;
  }

  if (packet.commercial) {
    const table = base(HI_TABLES.commercialProfiles);
    summary.commercial = await upsertByKey(
      table,
      CP.profileKey,
      packet.commercial.profileKey,
      commercialToFields(packet.commercial)
    );
  }

  for (const space of packet.eventSpaces || []) {
    const table = base(HI_TABLES.eventSpaces);
    summary.eventSpaces.push(
      await upsertByKey(table, ES.spaceKey, space.spaceKey, eventSpaceToFields(space))
    );
  }

  for (const node of packet.demandNodes || []) {
    const table = base(HI_TABLES.demandNodes);
    summary.demandNodes.push(
      await upsertByKey(table, DN.nodeKey, node.nodeKey, demandNodeToFields(node))
    );
  }

  for (const period of packet.seasonality || []) {
    const table = base(HI_TABLES.seasonalityNeedPeriods);
    summary.seasonality.push(
      await upsertByKey(table, SP.periodKey, period.periodKey, stripEmpty({
        [SP.periodKey]: period.periodKey,
        [SP.hpcHotelId]: period.hpcHotelId,
        [SP.periodType]: period.periodType,
        [SP.startMonthDay]: period.startMonthDay,
        [SP.endMonthDay]: period.endMonthDay,
        [SP.seasonLabel]: period.seasonLabel,
        [SP.priority]: period.priority,
        [SP.segment]: period.segment,
        [SP.dayOfWeekPattern]: period.dayOfWeekPattern,
        [SP.description]: period.description,
        [SP.sourceUrl]: period.sourceUrl,
        [SP.sourceName]: period.sourceName,
        [SP.hotelSupplied]: period.hotelSupplied,
        [SP.lastVerifiedAt]: period.lastVerifiedAt || new Date().toISOString(),
        [SP.confidence]: period.confidence,
        [SP.active]: period.active !== false,
        [SP.schemaVersion]: period.schemaVersion,
      }))
    );
  }
  if (!(packet.seasonality || []).length) {
    summary.skipped.push("seasonality_empty_by_policy");
  }

  for (const ev of packet.evidence || []) {
    const table = base(HI_TABLES.evidence);
    summary.evidence.push(
      await upsertByKey(table, EV.evidenceId, ev.evidenceId, evidenceToFields(ev))
    );
  }

  return summary;
}

/**
 * Load HI rows for one HPC hotel (for profile reconstruction).
 */
export async function loadHotelIntelligenceFromAirtable(hpcHotelId) {
  const { base, baseId } = getBase();
  const hpc = String(hpcHotelId || "").trim();

  const commercialRows = await findAllByHpc(
    base(HI_TABLES.commercialProfiles),
    CP.hpcHotelId,
    hpc
  );
  const eventRows = await findAllByHpc(
    base(HI_TABLES.eventSpaces),
    ES.hpcHotelId,
    hpc,
    `{${ES.active}} = 1`
  );
  const demandRows = await findAllByHpc(
    base(HI_TABLES.demandNodes),
    DN.hpcHotelId,
    hpc,
    `{${DN.active}} = 1`
  );
  const seasonRows = await findAllByHpc(
    base(HI_TABLES.seasonalityNeedPeriods),
    SP.hpcHotelId,
    hpc,
    `{${SP.active}} = 1`
  );
  const evidenceRows = await findAllByHpc(
    base(HI_TABLES.evidence),
    EV.hpcHotelId,
    hpc,
    `{${EV.current}} = 1`
  );

  const commercialRec = commercialRows[0] || null;
  const cf = commercialRec?.fields || null;

  return {
    ok: true,
    baseId,
    hpcHotelId: hpc,
    commercial: cf
      ? {
          airtableRecordId: commercialRec.id,
          profileKey: cf[CP.profileKey],
          hpcHotelId: cf[CP.hpcHotelId],
          dealalityHotelId: cf[CP.dealalityHotelId] || null,
          adpPropertyId: cf[CP.adpPropertyId] || null,
          hotelName: cf[CP.hotelName] || null,
          brand: cf[CP.brand] || null,
          roomsKeys: cf[CP.roomsKeys] ?? null,
          yearBuilt: cf[CP.yearBuilt] ?? null,
          yearRenovated: cf[CP.yearRenovated] ?? null,
          siteAcres: cf[CP.siteAcres] ?? null,
          parkingType: cf[CP.parkingType] || null,
          parkingDailyRate: cf[CP.parkingDailyRate] ?? null,
          meetingSpaceFlag: cf[CP.meetingSpaceFlag] ?? null,
          totalMeetingSpaceSqFt: cf[CP.totalMeetingSpaceSqFt] ?? null,
          meetingRoomCount: cf[CP.meetingRoomCount] ?? null,
          largestMeetingSpaceSqFt: cf[CP.largestMeetingSpaceSqFt] ?? null,
          largestEventCapacity: cf[CP.largestEventCapacity] ?? null,
          officialPropertyUrl: cf[CP.officialPropertyUrl] || null,
          officialEventsUrl: cf[CP.officialEventsUrl] || null,
          researchStatus: cf[CP.researchStatus] || null,
          researchVersion: cf[CP.researchVersion] || null,
          confidence: cf[CP.confidence] || null,
          conflictStatus: cf[CP.conflictStatus] ?? null,
          notes: cf[CP.notes] || null,
          lastResearchedAt: cf[CP.lastResearchedAt] || null,
        }
      : null,
    eventSpaces: eventRows.map((r) => ({
      airtableRecordId: r.id,
      spaceKey: r.fields[ES.spaceKey],
      name: r.fields[ES.spaceName],
      type: r.fields[ES.spaceType] || null,
      sqFt: r.fields[ES.sqFt] ?? null,
      theaterCapacity: r.fields[ES.theaterCapacity] ?? null,
      sourceUrl: r.fields[ES.sourceUrl] || null,
      confidence: r.fields[ES.confidence] || null,
    })),
    demandNodes: demandRows.map((r) => ({
      airtableRecordId: r.id,
      nodeKey: r.fields[DN.nodeKey],
      name: r.fields[DN.demandNodeName],
      type: r.fields[DN.demandNodeType] || "Other",
      sourceType: r.fields[DN.sourceType] || null,
      sourceName: r.fields[DN.sourceName] || null,
      sourceUrl: r.fields[DN.sourceUrl] || null,
      confidence: r.fields[DN.confidence] || null,
      demandStrength: r.fields[DN.demandStrength] || null,
    })),
    seasonality: seasonRows
      .filter((r) => !String(r.fields[SP.periodType] || "").includes("Need"))
      .map((r) => ({
        airtableRecordId: r.id,
        periodKey: r.fields[SP.periodKey],
        periodType: r.fields[SP.periodType],
        label: r.fields[SP.seasonLabel] || r.fields[SP.periodType],
        description: r.fields[SP.description] || null,
        hotelSupplied: r.fields[SP.hotelSupplied] === true,
        confidence: r.fields[SP.confidence] || null,
        sourceUrl: r.fields[SP.sourceUrl] || null,
      })),
    needPeriods: seasonRows
      .filter((r) => /Need|Compression/i.test(String(r.fields[SP.periodType] || "")))
      .map((r) => ({
        airtableRecordId: r.id,
        periodKey: r.fields[SP.periodKey],
        periodType: r.fields[SP.periodType],
        label: r.fields[SP.seasonLabel] || r.fields[SP.periodType],
        description: r.fields[SP.description] || null,
        hotelSupplied: r.fields[SP.hotelSupplied] === true,
        confidence: r.fields[SP.confidence] || null,
        sourceUrl: r.fields[SP.sourceUrl] || null,
      })),
    evidence: evidenceRows.map((r) => ({
      airtableRecordId: r.id,
      evidenceId: r.fields[EV.evidenceId],
      entityType: r.fields[EV.entityType],
      fieldName: r.fields[EV.fieldName],
      valueObserved: r.fields[EV.valueObserved],
      sourceName: r.fields[EV.sourceName],
      sourceType: r.fields[EV.sourceType],
      sourceUrl: r.fields[EV.sourceUrl],
      confidence: r.fields[EV.confidence],
      evidenceStrength: r.fields[EV.evidenceStrength],
    })),
  };
}

export {
  getBaseId,
  HI_TABLES,
  CP,
  ES,
  DN,
  SP,
  EV,
};
