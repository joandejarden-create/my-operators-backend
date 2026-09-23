/**
 * GDI Commercial Evidence V4 — enrichment + reusable commercial guards.
 * Does NOT modify Hygiene V3 gates. Annotates candidates and provides
 * post-V3 commercial demotion helpers for sales-noise classes.
 */

export const COMMERCIAL_EVIDENCE_V4 = "gdi_commercial_evidence_v4";

export const EVIDENCE_STATUS = Object.freeze({
  CONFIRMED: "CONFIRMED",
  ESTIMATED: "ESTIMATED",
  INFERRED: "INFERRED",
  UNKNOWN: "UNKNOWN",
  PAST: "PAST",
});

export const CONTACT_PATH_CLASS = Object.freeze({
  NAMED_CONTACT: "NAMED_CONTACT",
  FUNCTIONAL_CONTACT: "FUNCTIONAL_CONTACT",
  GENERIC_CONTACT: "GENERIC_CONTACT",
  NO_CONTACT: "NO_CONTACT",
});

const ES_MONTHS = Object.freeze({
  enero: 1,
  ene: 1,
  febrero: 2,
  feb: 2,
  marzo: 3,
  mar: 3,
  abril: 4,
  abr: 4,
  mayo: 5,
  junio: 6,
  jun: 6,
  julio: 7,
  jul: 7,
  agosto: 8,
  ago: 8,
  septiembre: 9,
  setiembre: 9,
  sep: 9,
  sept: 9,
  octubre: 10,
  oct: 10,
  noviembre: 11,
  nov: 11,
  diciembre: 12,
  dic: 12,
});

const ES_EVENT_RE =
  /\b(?:congreso|convenci[oó]n|foro|cumbre|simposio|encuentro|asamblea|conferencia|feria|reuni[oó]n|incentivo|retiro|evento)\b/i;

const LOCAL_NO_ROOM_RE =
  /\b(?:local\s+members?\s+only|no\s+guestrooms?|no\s+hotel\s+block|day\s+meeting\s+only|single[- ]day\s+(?:local\s+)?(?:meeting|program)|primarily\s+local\s+(?:membership|attendance)|host\s+confirms\s+no\s+(?:sleeping\s+)?rooms?)\b/i;

const GENERIC_INBOX_RE =
  /^(?:info|events|contact|housing|registrar|admin|office|hello|support|team|reservas|reservations)@/i;

/**
 * Parse Spanish / bilingual date strings → ISO YYYY-MM-DD (start) when confident.
 * Does not invent a future year when year is absent.
 */
export function parseSpanishOrFlexibleDate(raw) {
  const s = String(raw || "").trim();
  if (!s) return { iso: null, status: EVIDENCE_STATUS.UNKNOWN, source: null };

  const isoHit = s.match(/\b(20\d{2})-(\d{2})-(\d{2})\b/);
  if (isoHit) {
    return { iso: `${isoHit[1]}-${isoHit[2]}-${isoHit[3]}`, status: EVIDENCE_STATUS.CONFIRMED, source: s };
  }

  const dmy = s.match(/\b(\d{1,2})[\/\-.](\d{1,2})[\/\-.](20\d{2})\b/);
  if (dmy) {
    const d = Number(dmy[1]);
    const m = Number(dmy[2]);
    const y = dmy[3];
    if (m >= 1 && m <= 12 && d >= 1 && d <= 31) {
      return {
        iso: `${y}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`,
        status: EVIDENCE_STATUS.CONFIRMED,
        source: s,
      };
    }
  }

  // 22–24 septiembre 2026 | 22 al 24 de septiembre de 2026
  const esRange = s.match(
    /(\d{1,2})\s*(?:–|-|al)\s*(\d{1,2})\s*(?:de\s+)?([A-Za-záéíóúñ]+)\s*(?:de\s+)?(20\d{2})/i
  );
  if (esRange) {
    const month = ES_MONTHS[esRange[3].toLowerCase()];
    if (month) {
      return {
        iso: `${esRange[4]}-${String(month).padStart(2, "0")}-${String(Number(esRange[1])).padStart(2, "0")}`,
        status: EVIDENCE_STATUS.CONFIRMED,
        source: s,
        endDay: Number(esRange[2]),
      };
    }
  }

  // septiembre 22–24, 2026
  const esUs = s.match(
    /([A-Za-záéíóúñ]+)\s+(\d{1,2})\s*(?:–|-|al)?\s*(\d{1,2})?\s*,?\s*(20\d{2})/i
  );
  if (esUs) {
    const month = ES_MONTHS[esUs[1].toLowerCase()];
    if (month) {
      return {
        iso: `${esUs[4]}-${String(month).padStart(2, "0")}-${String(Number(esUs[2])).padStart(2, "0")}`,
        status: EVIDENCE_STATUS.CONFIRMED,
        source: s,
      };
    }
  }

  // Month + year only → INFERRED first-of-month (not a confirmed day)
  const monthYear = s.match(/\b([A-Za-záéíóúñ]+)\s+(?:de\s+)?(20\d{2})\b/i);
  if (monthYear) {
    const month = ES_MONTHS[monthYear[1].toLowerCase()];
    if (month) {
      return {
        iso: `${monthYear[2]}-${String(month).padStart(2, "0")}-01`,
        status: EVIDENCE_STATUS.INFERRED,
        source: s,
      };
    }
  }

  return { iso: null, status: EVIDENCE_STATUS.UNKNOWN, source: s || null };
}

/**
 * Detect invented future cycle (e.g. 2026 evidence forced to 2027).
 */
export function isUnconfirmedFutureCycleInvention({
  evidenceYears = [],
  claimedStartDate = null,
} = {}) {
  const claimedYear = String(claimedStartDate || "").slice(0, 4);
  if (!/^20\d{2}$/.test(claimedYear)) return false;
  const years = (evidenceYears || []).map(String).filter((y) => /^20\d{2}$/.test(y));
  if (!years.length) return false;
  return !years.includes(claimedYear) && Number(claimedYear) > Math.max(...years.map(Number));
}

export function detectLocalNoRoomProgram(opp = {}) {
  const blob = [
    opp.title,
    opp.whyNow,
    opp.hotelOpportunityThesis,
    opp.hotelDemandThesis,
    opp.housingEvidence,
    opp.roomDemandEvidence,
    opp.summaryWhyHotel,
  ]
    .filter(Boolean)
    .join(" \n ");
  if (LOCAL_NO_ROOM_RE.test(blob) || opp.localAttendanceOnly === true) {
    return {
      localNoRoom: true,
      reasons: ["local_or_zero_guestroom_signal"],
    };
  }
  return { localNoRoom: false, reasons: [] };
}

/**
 * Generic overflow catchment — NOT city-hardcoded.
 * Uses optional km distance + housingCluster relative to hotel submarket labels.
 */
export function assessOverflowMarketRealism({
  eventSubmarket = null,
  hotelSubmarkets = [],
  distanceKm = null,
  housingClusterMentionsHotelCorridor = null,
  overflowEvidence = null,
} = {}) {
  if (overflowEvidence === false) {
    return { plausible: false, reasons: ["explicit_no_overflow_evidence"] };
  }
  if (overflowEvidence === true || overflowEvidence) {
    return { plausible: true, reasons: ["overflow_evidence_present"] };
  }
  if (housingClusterMentionsHotelCorridor === false) {
    return { plausible: false, reasons: ["housing_cluster_excludes_hotel_corridor"] };
  }
  if (Number.isFinite(distanceKm) && distanceKm > 45) {
    return { plausible: false, reasons: [`distance_km_${distanceKm}_above_45`] };
  }
  const eventSm = String(eventSubmarket || "").toLowerCase();
  const hotelSms = (hotelSubmarkets || []).map((s) => String(s).toLowerCase()).filter(Boolean);
  if (eventSm && hotelSms.length) {
    const overlap = hotelSms.some(
      (h) => eventSm.includes(h) || h.includes(eventSm) || tokenOverlap(eventSm, h) >= 1
    );
    if (!overlap && Number.isFinite(distanceKm) && distanceKm > 25) {
      return { plausible: false, reasons: ["submarket_mismatch_and_distance"] };
    }
  }
  return { plausible: true, reasons: ["no_disqualifying_catchment_signal"] };
}

function tokenOverlap(a, b) {
  const ta = new Set(a.split(/[^a-z0-9áéíóúñ]+/i).filter((t) => t.length > 3));
  const tb = new Set(b.split(/[^a-z0-9áéíóúñ]+/i).filter((t) => t.length > 3));
  let n = 0;
  for (const t of ta) if (tb.has(t)) n += 1;
  return n;
}

/**
 * Series / cycle identity helpers for grouping related leads.
 */
export function buildEventSeriesIdentity(opp = {}) {
  const title = String(opp.title || opp.eventName || "").toLowerCase();
  const org = String(opp.organizationName || opp.organization || "").toLowerCase();
  const year = String(opp.eventStartDate || "").slice(0, 4);
  const seriesBase = String(opp.eventSeriesHint || title)
    .toLowerCase()
    .replace(/\b(20\d{2}|edition|edici[oó]n|season|temporada|week\s*\d+)\b/gi, " ")
    .replace(/\b(?:division|divisi[oó]n|bracket|round|heat|stage|fase|grupo|group)\s*[a-z0-9\-]+\b/gi, " ")
    .replace(/\s*[—\-–:|]\s*.*$/g, " ") // trailing sub-event after dash/colon
    .replace(/[^a-z0-9áéíóúñ]+/gi, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 64);
  const eventSeriesId = opp.eventSeriesId || (seriesBase ? `series:${seriesBase}` : null);
  const eventCycleId =
    opp.eventCycleId ||
    (eventSeriesId ? `cycle:${eventSeriesId}|${year || "noyear"}` : null);
  const subEventId = opp.subEventId || opp.subEventHint || null;
  return {
    eventSeriesId,
    eventCycleId,
    subEventId,
    organizationKey: org ? `org:${org.replace(/[^a-z0-9]+/g, "_").slice(0, 48)}` : null,
    normalizedEventName: seriesBase,
    newToGdi: true,
    newToHotel: null, // unknown without CRM
  };
}

export function classifyContactPath(person = {}, functionalContacts = []) {
  const email = String(person?.email || person?.publicEmail || "").trim();
  const name = String(person?.name || "").trim();
  const namedPerson =
    name &&
    name.split(/\s+/).length >= 2 &&
    !/^(unknown|n\/?a|staff|desk|office|info)$/i.test(name) &&
    !/\b(staff|desk|office|inbox)\b/i.test(name);
  if (namedPerson && email && !GENERIC_INBOX_RE.test(email)) {
    return CONTACT_PATH_CLASS.NAMED_CONTACT;
  }
  // Named person with role/generic inbox → still named (partial reachability)
  if (namedPerson) {
    return CONTACT_PATH_CLASS.NAMED_CONTACT;
  }
  if (email && GENERIC_INBOX_RE.test(email)) return CONTACT_PATH_CLASS.GENERIC_CONTACT;
  if ((functionalContacts || []).length) return CONTACT_PATH_CLASS.FUNCTIONAL_CONTACT;
  if (email) return CONTACT_PATH_CLASS.GENERIC_CONTACT;
  return CONTACT_PATH_CLASS.NO_CONTACT;
}

export function hasMeaningfulSource(opp = {}) {
  if (opp.officialSource && /^https?:\/\//i.test(String(opp.officialSource))) return true;
  for (const e of opp.evidenceSources || opp.evidence || opp.sources || []) {
    const url = typeof e === "string" ? e : e?.url || e?.sourceUrl;
    if (url && /^https?:\/\//i.test(String(url))) return true;
  }
  return false;
}

/**
 * Enrich a mapped discovery candidate with V4 commercial evidence fields.
 */
export function enrichCommercialEvidenceV4(opp = {}, { nowDate = null } = {}) {
  const dateParsed = parseSpanishOrFlexibleDate(
    opp.eventStartDate || opp.startDate || opp.dateNote || ""
  );
  let eventStartDate = opp.eventStartDate || dateParsed.iso || null;
  let eventDateStatus =
    opp.eventDateStatus ||
    dateParsed.status ||
    (eventStartDate ? EVIDENCE_STATUS.CONFIRMED : EVIDENCE_STATUS.UNKNOWN);

  if (isUnconfirmedFutureCycleInvention({
    evidenceYears: opp.evidenceYears || [],
    claimedStartDate: eventStartDate,
  })) {
    eventDateStatus = EVIDENCE_STATUS.INFERRED;
    opp = {
      ...opp,
      opportunityType: "FUTURE_CYCLE",
      futureCycleInvented: true,
    };
  }

  if (nowDate && eventStartDate) {
    const now = new Date(`${String(nowDate).slice(0, 10)}T00:00:00Z`);
    const start = new Date(`${String(eventStartDate).slice(0, 10)}T00:00:00Z`);
    if (!Number.isNaN(now.getTime()) && !Number.isNaN(start.getTime()) && start < now) {
      eventDateStatus = EVIDENCE_STATUS.PAST;
    }
  }

  const knownNumericOrText = (v) => {
    if (v == null) return false;
    const s = String(v).trim();
    if (!s) return false;
    if (/^(unknown|unk|n\/?a|desconocido|tbd|null)$/i.test(s)) return false;
    return true;
  };
  const attendanceStatus =
    opp.attendanceStatus ||
    (knownNumericOrText(opp.estimatedAttendance) || knownNumericOrText(opp.attendance)
      ? EVIDENCE_STATUS.ESTIMATED
      : EVIDENCE_STATUS.UNKNOWN);
  const peakRoomsStatus =
    opp.peakRoomsStatus ||
    (knownNumericOrText(opp.estimatedPeakRooms) ||
    knownNumericOrText(opp.peakRooms) ||
    knownNumericOrText(opp.peakRoomEstimate)
      ? EVIDENCE_STATUS.ESTIMATED
      : EVIDENCE_STATUS.UNKNOWN);

  const local = detectLocalNoRoomProgram(opp);
  const identity = buildEventSeriesIdentity({ ...opp, eventStartDate });
  const spanishEvent = ES_EVENT_RE.test(String(opp.title || ""));

  return {
    ...opp,
    eventStartDate,
    eventDateStatus,
    eventDateSource: opp.eventDateSource || dateParsed.source || null,
    attendance: opp.attendance ?? opp.estimatedAttendance ?? null,
    attendanceStatus,
    attendanceSource: opp.attendanceSource || null,
    peakRooms: opp.peakRooms ?? opp.estimatedPeakRooms ?? opp.peakRoomEstimate ?? null,
    peakRoomsStatus,
    peakRoomsSource: opp.peakRoomsSource || opp.roomDemandEvidence || null,
    hotelDemandThesis: opp.hotelDemandThesis || opp.hotelWinThesis || opp.hotelOpportunityThesis || null,
    localAttendanceOnly: local.localNoRoom || Boolean(opp.localAttendanceOnly),
    semanticTypeHint: opp.semanticTypeHint || (spanishEvent ? "EVENT" : undefined),
    ...identity,
    commercialEvidenceV4: {
      version: COMMERCIAL_EVIDENCE_V4,
      localNoRoom: local.localNoRoom,
      hasSource: hasMeaningfulSource(opp),
      spanishEventMarker: spanishEvent,
    },
  };
}

/**
 * Evidence overlay for Hygiene V3 without changing V3 code.
 * Local-no-room / remote overflow / sourceless / invented future → overlays
 * that existing V3 failure classes already respect.
 */
export function buildHygieneEvidenceOverlayV4(opp = {}, catchment = {}) {
  const overlay = {};
  const local = detectLocalNoRoomProgram(opp);
  if (local.localNoRoom) {
    // Prevent weak "large event therefore rooms" — no hotelDemandEvidence.
    overlay.hotelDemandEvidence = false;
    overlay.overflowEvidence = false;
  }
  const overflow = assessOverflowMarketRealism({
    ...catchment,
    overflowEvidence: opp.overflowEvidence,
  });
  if (!overflow.plausible && /OVERFLOW/i.test(String(opp.opportunityType || ""))) {
    overlay.overflowEvidence = false;
  }
  if (opp.futureCycleInvented) {
    overlay.recurrenceEvidence = false;
  }
  if (opp.fullyPlaced || opp.hostHotelSelected) {
    overlay.fullyPlaced = true;
  }
  return overlay;
}

/**
 * Post-V3 commercial demotion for sales-noise (does not change V3 itself).
 * Returns adjusted actionability label for founder/customer readiness scoring.
 */
export function applyCommercialReadinessDemotionV4(v3Row = {}, opp = {}) {
  const act = v3Row.actionabilityV3 || v3Row.actionability;
  if (act !== "TRUE_ACTIONABLE") {
    return { actionability: act, demoted: false, reasons: [] };
  }
  const reasons = [];
  if (!hasMeaningfulSource(opp) && !hasMeaningfulSource(v3Row)) {
    reasons.push("SOURCELESS_TRUE");
  }
  if (detectLocalNoRoomProgram(opp).localNoRoom) {
    reasons.push("LOCAL_NO_ROOM");
  }
  if (opp.futureCycleInvented) {
    reasons.push("UNCONFIRMED_FUTURE_CYCLE");
  }
  const overflow = assessOverflowMarketRealism({
    eventSubmarket: opp.eventSubmarket,
    hotelSubmarkets: opp.hotelSubmarkets,
    distanceKm: opp.distanceKm,
    housingClusterMentionsHotelCorridor: opp.housingClusterMentionsHotelCorridor,
    overflowEvidence: opp.overflowEvidence,
  });
  if (/OVERFLOW/i.test(String(opp.opportunityType || "")) && !overflow.plausible) {
    reasons.push("REMOTE_OVERFLOW");
  }
  if (reasons.length) {
    return { actionability: "INSUFFICIENT", demoted: true, reasons };
  }
  return { actionability: act, demoted: false, reasons: [] };
}

export function completenessFlags(opp = {}, who = null) {
  return {
    date: Boolean(opp.eventStartDate) && opp.eventDateStatus !== EVIDENCE_STATUS.UNKNOWN,
    attendance: opp.attendance != null && opp.attendanceStatus !== EVIDENCE_STATUS.UNKNOWN,
    peakRooms: opp.peakRooms != null && opp.peakRoomsStatus !== EVIDENCE_STATUS.UNKNOWN,
    venue: Boolean(opp.venue || opp.hostHotel || opp.meetingVenue),
    namedWho: who?.contactPath === CONTACT_PATH_CLASS.NAMED_CONTACT || Boolean(who?.named),
    source: hasMeaningfulSource(opp),
    actionPath: Boolean(opp.suggestedAction || opp.recommendedAction),
  };
}
