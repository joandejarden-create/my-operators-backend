/**
 * Private Event Signals — public event signals before GDI promotion.
 * Signal ≠ opportunity.
 */

import { createHash } from "node:crypto";
import {
  PE_SIGNALS_TABLE_NAME,
  MAP_PE_SIGNAL as F,
  MAP_PE_VENUE,
  PE_VENUES_TABLE_NAME,
  VAL_SIGNAL_STATUS,
  VAL_ROOM_DEMAND_CLAIM,
  VAL_ATTENDANCE_STATUS,
  VAL_PE_DEMAND_SIGNAL_TYPE,
} from "./airtable-field-map.js";
import {
  getPeBase,
  isPeAirtableConfigured,
  omitEmpty,
  asSelect,
  jsonText,
  nowIso,
  schemaVersion,
  findByField,
  createOrUpdate,
} from "./airtable-client.js";

function signalsTable() {
  return getPeBase()(PE_SIGNALS_TABLE_NAME);
}

export function computeSignalId(signal = {}) {
  if (signal.signalId) return String(signal.signalId).trim();
  const seed = [
    signal.venueId || "",
    signal.eventName || "",
    signal.eventDate || signal.eventStartDate || "",
    signal.sourceUrl || "",
  ].join("|");
  return `pes_${createHash("sha256").update(seed).digest("hex").slice(0, 14)}`;
}

export function signalToAirtableFields(signal, meta = {}) {
  const signalId = computeSignalId(signal);
  const demandType = String(
    signal.demandSignalType || signal.eventType || "PRIVATE_EVENT"
  )
    .toUpperCase()
    .replace(/[\s-]+/g, "_");

  return omitEmpty({
    [F.signalId]: signalId,
    [F.venueId]: signal.venueId || null,
    [F.venueName]: signal.venueName || null,
    [F.demandSignalType]: asSelect(demandType, VAL_PE_DEMAND_SIGNAL_TYPE),
    [F.eventName]: signal.eventName || null,
    [F.eventType]: signal.eventType || demandType,
    [F.eventStartDate]: signal.eventStartDate || signal.eventDate || null,
    [F.eventEndDate]: signal.eventEndDate || null,
    [F.dateGranularity]: signal.dateGranularity || signal.eventDateGranularity || "DAY",
    [F.estimatedAttendance]:
      signal.estimatedAttendance != null
        ? Number(signal.estimatedAttendance)
        : undefined,
    [F.attendanceStatus]: asSelect(
      signal.attendanceStatus || "UNKNOWN",
      VAL_ATTENDANCE_STATUS
    ),
    [F.potentialRoomsLow]:
      signal.potentialRoomsLow != null
        ? Number(signal.potentialRoomsLow)
        : undefined,
    [F.potentialRoomsHigh]:
      signal.potentialRoomsHigh != null
        ? Number(signal.potentialRoomsHigh)
        : undefined,
    [F.roomDemandStatus]: asSelect(
      signal.roomDemandStatus || "UNKNOWN",
      VAL_ROOM_DEMAND_CLAIM
    ),
    [F.lodgingMentioned]: Boolean(signal.lodgingMentioned),
    [F.roomBlockMentioned]: Boolean(signal.roomBlockMentioned),
    [F.hotelMentioned]: Boolean(signal.hotelMentioned),
    [F.transportationMentioned]: Boolean(signal.transportationMentioned),
    [F.plannerCompany]: signal.plannerCompany || null,
    [F.plannerName]: signal.plannerName || null,
    [F.plannerRole]: signal.plannerRole || null,
    [F.sourceUrl]: signal.sourceUrl || (signal.sourceUrls || [])[0] || null,
    [F.sourceType]: signal.sourceType || null,
    [F.sourceAuthority]: signal.sourceAuthority || null,
    [F.signalStatus]: asSelect(
      meta.signalStatus || signal.signalStatus || "NEW_SIGNAL",
      VAL_SIGNAL_STATUS
    ),
    [F.hotelDemandThesis]: signal.hotelDemandThesis || meta.hotelDemandThesis || null,
    [F.evidenceText]:
      signal.evidenceText || signal.publicEventEvidence || null,
    [F.firstSeen]: meta.firstSeen || signal.firstSeen || nowIso(),
    [F.lastSeen]: nowIso(),
    [F.lastVerified]: signal.lastVerified || nowIso(),
    [F.promotedGdiOpportunityId]: meta.promotedGdiOpportunityId || null,
    [F.hotelVenueFitId]: meta.hotelVenueFitId || signal.hotelVenueFitId || null,
    [F.schemaVersion]: schemaVersion(),
    [F.signalPayloadJson]: jsonText(signal),
  });
}

export function airtableRecordToSignal(rec) {
  if (!rec) return null;
  const f = rec.fields || {};
  return {
    airtableRecordId: rec.id,
    signalId: f[F.signalId],
    venueId: f[F.venueId],
    venueName: f[F.venueName],
    demandSignalType: f[F.demandSignalType],
    eventName: f[F.eventName],
    eventType: f[F.eventType],
    eventStartDate: f[F.eventStartDate],
    signalStatus: f[F.signalStatus],
    promotedGdiOpportunityId: f[F.promotedGdiOpportunityId],
    hotelVenueFitId: f[F.hotelVenueFitId],
  };
}

export async function upsertPrivateEventSignal(
  signal,
  { dryRun = true, meta = {} } = {}
) {
  if (!signal?.venueId && !signal?.venueName) {
    return { ok: false, error: "signal_missing_venue", orphanPrevented: true };
  }

  const signalId = computeSignalId(signal);
  let prior = null;
  if (isPeAirtableConfigured() && !dryRun) {
    const hits = await findByField(signalsTable(), F.signalId, signalId, 1);
    if (hits[0]) prior = airtableRecordToSignal(hits[0]);
  }

  const fields = signalToAirtableFields(signal, {
    ...meta,
    firstSeen: prior ? undefined : nowIso(),
  });
  // Preserve firstSeen on update: omit if prior exists and we're not dry-run updating
  if (prior && !dryRun) {
    delete fields[F.firstSeen];
  }

  if (!dryRun && isPeAirtableConfigured()) {
    let venueRecId = meta.venueAirtableRecordId || null;
    if (!venueRecId && signal.venueId) {
      const hits = await findByField(
        getPeBase()(PE_VENUES_TABLE_NAME),
        MAP_PE_VENUE.venueId,
        signal.venueId,
        1
      );
      venueRecId = hits[0]?.id || null;
    }
    if (venueRecId) {
      fields[F.venueLink] = [venueRecId];
    }
  }

  const result = await createOrUpdate(dryRun ? null : signalsTable(), {
    recordId: prior?.airtableRecordId || null,
    fields,
    dryRun,
  });

  return {
    ok: true,
    signalId,
    venueId: signal.venueId || null,
    fields,
    ...result,
  };
}

export { PE_SIGNALS_TABLE_NAME, F as PE_SIGNAL_FIELDS };
