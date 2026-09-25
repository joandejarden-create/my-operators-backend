/**
 * Normalize candidate opportunities into scored GDI opportunity records.
 */

import { createId } from "./repository.js";
import {
  computeHotelFitScore,
  computeEvidenceConfidence,
  classifyPriority,
  buildHotelFitExplanation,
  buildEvidenceConfidenceExplanation,
  HOTEL_FIT_COMPONENT_LABELS,
  EVIDENCE_CONFIDENCE_TOOLTIP,
} from "./scoring.js";
import { enrichQualificationPrecision, runHighPriorityQualityGate } from "./qualification-precision.js";
import {
  hotelContextFromProfile,
} from "./hotel-capture-capacity.js";
import {
  buildHotelGroupDemandProfile,
  loadHotelDemandConfig,
} from "./hotel-profile.js";
import {
  BOOKING_WINDOW,
  CLAIM_KIND,
  CLAIM_KIND_LABEL,
  PRIORITY,
  SOURCING_STATUS,
  INCREMENTAL_VALUE_STATUS,
  DEMAND_TERRITORY_FIT_LABEL,
  SOURCING_STATUS_LABEL,
  INCREMENTAL_VALUE_STATUS_LABEL,
  BOOKING_WINDOW_LABEL,
  OPPORTUNITY_TYPE_LABEL,
  VENUE_SOURCING_STATUS_LABEL,
  EVENT_LOCATION_STATUS_LABEL,
  ROOM_DEMAND_STATUS_LABEL,
  CONTACT_QUALITY_LABEL,
  CONTACT_GRADE_LABEL,
  TARGET_ROLE_MATCH_LABEL,
  REACTIVATION_SIGNAL_LABEL,
  OPPORTUNITY_QUALIFICATION_LABEL,
  QUALIFICATION_FAILURE_REASON_LABEL,
} from "./claim-types.js";

function evidenceRow(partial) {
  return {
    id: partial.id || createId("gdi_ev"),
    field: partial.field,
    value: partial.value,
    claimKind: partial.claimKind || CLAIM_KIND.FACT,
    sourceUrl: partial.sourceUrl || null,
    sourceTitle: partial.sourceTitle || null,
    sourceDomain: partial.sourceDomain || null,
    sourceType: partial.sourceType || null,
    sourceAuthority: partial.sourceAuthority || "Tier_B",
    accessDate: partial.accessDate || new Date().toISOString().slice(0, 10),
    extractedText: partial.extractedText || null,
    confidence: partial.confidence ?? null,
    supportsFact: partial.supportsFact || partial.field || null,
    researchMethodId: partial.researchMethodId || null,
    researchProvider: partial.researchProvider || null,
    researchLevel: partial.researchLevel || null,
  };
}

function normalizeBookingWindow(raw) {
  const bw = raw || BOOKING_WINDOW.QUALIFY_NOW;
  if (bw === BOOKING_WINDOW.RESEARCH_FURTHER) return BOOKING_WINDOW.QUALIFY_NOW;
  return bw;
}

function buildSourcesSummary(evidence = []) {
  const seen = new Set();
  const sources = [];
  for (const e of evidence) {
    const key = `${e.sourceUrl || ""}|${e.sourceTitle || ""}|${e.sourceDomain || ""}`;
    if (seen.has(key)) continue;
    seen.add(key);
    if (!e.sourceTitle && !e.sourceUrl && !e.sourceDomain) continue;
    sources.push({
      name: e.sourceTitle || e.sourceDomain || "Source",
      sourceType: e.sourceType || null,
      date: e.accessDate || null,
      url: e.sourceUrl || null,
      supportsFact: e.supportsFact || e.field || null,
      claimKind: e.claimKind || null,
    });
  }
  return sources.slice(0, 12);
}

function buildKnownVsEstimated(raw, evidence) {
  if (raw.knownVsEstimated && typeof raw.knownVsEstimated === "object") {
    return raw.knownVsEstimated;
  }
  const pick = (field, value, claimKind) => ({
    field,
    value: value ?? null,
    status: CLAIM_KIND_LABEL[claimKind] || claimKind || "UNKNOWN",
    claimKind: claimKind || CLAIM_KIND.UNKNOWN,
  });
  return {
    verified: [
      pick("Event date", [raw.eventStartDate, raw.eventEndDate].filter(Boolean).join(" → ") || null, raw.eventDateClaimKind || (raw.eventStartDate ? CLAIM_KIND.FACT : CLAIM_KIND.UNKNOWN)),
      pick("Venue status", raw.venueStatus, raw.venueStatusClaimKind || (raw.venueStatus ? CLAIM_KIND.FACT : CLAIM_KIND.UNKNOWN)),
      pick("Organization", raw.organizationName, CLAIM_KIND.FACT),
    ].filter((x) => x.value),
    estimated: [
      pick("Peak rooms", raw.estimatedPeakRooms, raw.estimatedPeakRoomsClaimKind),
      pick("Attendance", raw.estimatedAttendance, raw.estimatedAttendanceClaimKind),
    ].filter((x) => x.value != null && x.claimKind === CLAIM_KIND.ESTIMATED),
    inferred: [
      pick("Demand territory likelihood", raw.demandTerritoryFit, CLAIM_KIND.INFERENCE),
      pick("Demand territory rationale", raw.demandTerritoryRationale, CLAIM_KIND.INFERENCE),
    ].filter((x) => x.value),
    evidenceFieldCounts: {
      verified: evidence.filter((e) => e.claimKind === CLAIM_KIND.FACT).length,
      estimated: evidence.filter((e) => e.claimKind === CLAIM_KIND.ESTIMATED).length,
      inferred: evidence.filter((e) => e.claimKind === CLAIM_KIND.INFERENCE).length,
    },
  };
}

const _hotelContextCache = new Map();

function resolveHotelContext(raw = {}) {
  if (raw.hotelContext && typeof raw.hotelContext === "object") return raw.hotelContext;
  const hotelId = raw.hotelId;
  if (!hotelId) return null;
  if (_hotelContextCache.has(hotelId)) return _hotelContextCache.get(hotelId);
  try {
    const profile = buildHotelGroupDemandProfile(hotelId);
    const config = loadHotelDemandConfig(hotelId);
    const ctx = hotelContextFromProfile(profile, config);
    _hotelContextCache.set(hotelId, ctx);
    return ctx;
  } catch {
    _hotelContextCache.set(hotelId, null);
    return null;
  }
}

export function buildOpportunity(raw = {}) {
  const bookingWindowStatus = normalizeBookingWindow(raw.bookingWindowStatus);
  const demandTerritoryFit = raw.demandTerritoryFit || null;
  const hotelContext = resolveHotelContext(raw);

  // First-pass fit/confidence so qualification heuristics can use scores
  let fit = computeHotelFitScore(raw.fitComponents || {});
  const conf = computeEvidenceConfidence(raw.confidenceInput || {});

  const skipPrecision = raw.skipQualificationPrecision === true;
  const precision = skipPrecision
    ? {}
    : enrichQualificationPrecision({
        ...raw,
        hotelContext,
        hotelFitScore: fit.hotelFitScore,
        evidenceConfidence: conf.evidenceConfidence,
        bookingWindowStatus,
        demandTerritoryFit,
        contactabilityScore: fit.components.contactability,
        geographyFitScore: fit.components.geographyFit,
        physicalFitScore: fit.components.physicalFit,
        timingScore: fit.components.timing,
        commercialValueScore: fit.components.commercialValue,
        historicalFitScore: fit.components.historicalFit,
        competitiveAccessibilityScore: fit.components.competitiveAccessibility,
      });

  // Re-score Hotel Fit after contactability / geography / capacity adjustments from precision pass
  if (precision.fitComponents) {
    fit = computeHotelFitScore({
      ...(raw.fitComponents || {}),
      ...precision.fitComponents,
    });
  }

  // A2: re-run High gate with post-capacity Hotel Fit (precision pass used first-pass score)
  let highPriorityGate = precision.highPriorityGate || null;
  if (!skipPrecision && precision.opportunityQualification) {
    highPriorityGate = runHighPriorityQualityGate(
      {
        ...raw,
        whyNow: raw.whyNow,
        bookingWindowStatus,
        hotelFitScore: fit.hotelFitScore,
        evidenceConfidence: conf.evidenceConfidence,
        captureCapacityState: precision.captureCapacityState,
      },
      {
        venueSourcingStatus: precision.venueSourcingStatus || raw.venueSourcingStatus || null,
        opportunityType: precision.opportunityType || raw.opportunityType || null,
        roomDemandStatus: precision.roomDemandStatus || raw.roomDemandStatus || null,
        eventLocationStatus: precision.eventLocationStatus || raw.eventLocationStatus || null,
        contactQuality: precision.contactQuality || raw.contactQuality || null,
        hotelOpportunityThesis:
          precision.hotelOpportunityThesis ||
          raw.hotelOpportunityThesis ||
          raw.bethesdaWinThesis ||
          null,
        opportunityQualification: precision.opportunityQualification || null,
        demandTerritoryFit,
        hotelFitScore: fit.hotelFitScore,
        evidenceConfidence: conf.evidenceConfidence,
        bookingWindowStatus,
        captureCapacityState: precision.captureCapacityState,
      }
    );
  }

  const priorityResult = classifyPriority({
    hotelFitScore: fit.hotelFitScore,
    evidenceConfidence: conf.evidenceConfidence,
    bookingWindowStatus,
    disqualifyReasons: raw.disqualifyReasons || [],
    demandTerritoryFit,
    opportunityQualification: precision.opportunityQualification || raw.opportunityQualification || null,
    opportunityType: precision.opportunityType || raw.opportunityType || null,
    venueSourcingStatus: precision.venueSourcingStatus || raw.venueSourcingStatus || null,
    roomDemandStatus: precision.roomDemandStatus || raw.roomDemandStatus || null,
    eventLocationStatus: precision.eventLocationStatus || raw.eventLocationStatus || null,
    contactQuality: precision.contactQuality || raw.contactQuality || null,
    hotelOpportunityThesis:
      precision.hotelOpportunityThesis || raw.hotelOpportunityThesis || raw.bethesdaWinThesis || null,
    whyNow: raw.whyNow || null,
    highPriorityGate,
    opportunity: {
      ...raw,
      whyNow: raw.whyNow,
      bookingWindowStatus,
      captureCapacityState: precision.captureCapacityState,
    },
  });

  const evidence = (raw.evidence || []).map(evidenceRow);
  // V1.2: if evidence empty but evidenceSources present, project into evidence rows
  const evidenceFromSources =
    evidence.length > 0
      ? evidence
      : (raw.evidenceSources || raw.sources || [])
          .map((s) => {
            if (!s) return null;
            if (typeof s === "string") return evidenceRow({ url: s, title: "Source" });
            return evidenceRow({
              url: s.url || s.sourceUrl || null,
              title: s.title || s.sourceTitle || "Source",
              note: s.authority || s.note || null,
            });
          })
          .filter(Boolean);

  const primaryOfficial =
    raw.officialSource ||
    raw.sourceObject?.sourceUrl ||
    evidenceFromSources[0]?.url ||
    (raw.evidenceSources || [])[0]?.url ||
    null;
  const fitExplanation =
    raw.fitExplanation ||
    buildHotelFitExplanation({
      components: fit.components,
      demandTerritoryFit,
      fitExplanation: raw.fitExplanation,
    });
  const evidenceConfidenceExplanation =
    raw.evidenceConfidenceExplanation ||
    buildEvidenceConfidenceExplanation({
      evidenceConfidence: conf.evidenceConfidence,
      evidence,
      evidenceConfidenceExplanation: raw.evidenceConfidenceExplanation,
    });

  const sourcingStatus = raw.sourcingStatus || SOURCING_STATUS.UNKNOWN;
  const incrementalValueStatus =
    raw.incrementalValueStatus || INCREMENTAL_VALUE_STATUS.UNKNOWN;

  const opportunityType = precision.opportunityType || raw.opportunityType || null;
  const venueSourcingStatus = precision.venueSourcingStatus || raw.venueSourcingStatus || null;
  const eventLocationStatus = precision.eventLocationStatus || raw.eventLocationStatus || null;
  const roomDemandStatus = precision.roomDemandStatus || raw.roomDemandStatus || null;
  const contactQuality = precision.contactQuality || raw.contactQuality || null;
  const reactivationSignal = precision.reactivationSignal || raw.reactivationSignal || null;
  const opportunityQualification =
    precision.opportunityQualification || raw.opportunityQualification || null;
  const qualificationFailureReason =
    precision.qualificationFailureReason || raw.qualificationFailureReason || null;
  const hotelOpportunityThesis =
    precision.hotelOpportunityThesis ||
    raw.hotelOpportunityThesis ||
    raw.bethesdaWinThesis ||
    null;

  const now = new Date().toISOString();
  return {
    id: raw.id || createId("gdi_opp"),
    hotelId: raw.hotelId,
    organizationId: raw.organizationId || null,
    organizationName: raw.organizationName || null,
    eventId: raw.eventId || null,
    eventSeriesKey: raw.eventSeriesKey || null,
    title: raw.title,
    segment: raw.segment,
    demandType: raw.demandType || "meeting",
    demandStatus: raw.demandStatus,
    priority: priorityResult.priority,
    priorityReason: priorityResult.reason,
    opportunityType,
    opportunityTypeLabel:
      precision.opportunityTypeLabel ||
      OPPORTUNITY_TYPE_LABEL[opportunityType] ||
      opportunityType,
    futureCycleEvidenceState:
      precision.futureCycleEvidenceState || raw.futureCycleEvidenceState || null,
    futureCycleEvidenceStateLabel:
      precision.futureCycleEvidenceStateLabel || raw.futureCycleEvidenceStateLabel || null,
    futureCycleSignals: precision.futureCycleSignals || raw.futureCycleSignals || [],
    whyMonitor: precision.whyMonitor || raw.whyMonitor || null,
    venueSourcingStatus,
    venueSourcingStatusLabel:
      precision.venueSourcingStatusLabel ||
      VENUE_SOURCING_STATUS_LABEL[venueSourcingStatus] ||
      venueSourcingStatus,
    venueSourcingRationale: precision.venueSourcingRationale || raw.venueSourcingRationale || null,
    venueVerificationMethodId:
      precision.venueVerificationMethodId || raw.venueVerificationMethodId || null,
    eventLocationStatus,
    eventLocationStatusLabel:
      precision.eventLocationStatusLabel ||
      EVENT_LOCATION_STATUS_LABEL[eventLocationStatus] ||
      eventLocationStatus,
    eventLocationSummary: precision.eventLocationSummary || raw.eventLocationSummary || null,
    eventLocationSource: precision.eventLocationSource || raw.eventLocationSource || null,
    publishedAttendance: precision.publishedAttendance ?? raw.publishedAttendance ?? null,
    publishedPeakRooms: precision.publishedPeakRooms ?? raw.publishedPeakRooms ?? null,
    publishedRoomNights: precision.publishedRoomNights ?? raw.publishedRoomNights ?? null,
    roomDemandStatus,
    roomDemandStatusLabel:
      precision.roomDemandStatusLabel ||
      ROOM_DEMAND_STATUS_LABEL[roomDemandStatus] ||
      roomDemandStatus,
    roomDemandConfidence: precision.roomDemandConfidence ?? raw.roomDemandConfidence ?? null,
    roomDemandRationale: precision.roomDemandRationale || raw.roomDemandRationale || null,
    hotelOpportunityThesis,
    opportunityQualification,
    opportunityQualificationLabel:
      precision.opportunityQualificationLabel ||
      OPPORTUNITY_QUALIFICATION_LABEL[opportunityQualification] ||
      opportunityQualification,
    qualificationGatePassed:
      precision.qualificationGatePassed != null
        ? precision.qualificationGatePassed
        : raw.qualificationGatePassed ?? null,
    qualificationNotes: precision.qualificationNotes || raw.qualificationNotes || null,
    qualificationFailureReason,
    qualificationFailureReasonLabel: qualificationFailureReason
      ? QUALIFICATION_FAILURE_REASON_LABEL[qualificationFailureReason] ||
        qualificationFailureReason
      : null,
    contactQuality,
    contactQualityLabel:
      precision.contactQualityLabel || CONTACT_QUALITY_LABEL[contactQuality] || contactQuality,
    contactGrade: raw.contactGrade || raw.primaryContact?.contactGrade || null,
    contactGradeLabel:
      raw.contactGradeLabel ||
      CONTACT_GRADE_LABEL[raw.contactGrade] ||
      raw.primaryContact?.contactGradeLabel ||
      null,
    contactConfidence:
      raw.contactConfidence ?? raw.primaryContact?.contactConfidence ?? null,
    contactRoleMatch:
      raw.contactRoleMatch ||
      raw.primaryContact?.targetRoleMatch ||
      raw.primaryContact?.contactRoleMatch ||
      null,
    contactRoleMatchLabel:
      raw.contactRoleMatchLabel ||
      TARGET_ROLE_MATCH_LABEL[raw.contactRoleMatch] ||
      raw.primaryContact?.targetRoleMatchLabel ||
      null,
    whyThisContact: raw.whyThisContact || raw.primaryContact?.whyThisContact || null,
    backupContacts: raw.backupContacts || [],
    contactRole: precision.contactRole || raw.contactRole || null,
    relationshipToEvent: precision.relationshipToEvent || raw.relationshipToEvent || null,
    relationshipConfidence:
      precision.relationshipConfidence || raw.relationshipConfidence || null,
    reactivationSignal,
    reactivationSignalLabel:
      precision.reactivationSignalLabel ||
      REACTIVATION_SIGNAL_LABEL[reactivationSignal] ||
      reactivationSignal,
    reactivationThesis: precision.reactivationThesis || raw.reactivationThesis || null,
    highPriorityGate: highPriorityGate || precision.highPriorityGate || raw.highPriorityGate || null,
    eventStartDate: raw.eventStartDate || null,
    eventEndDate: raw.eventEndDate || null,
    destinationStatus: raw.destinationStatus || null,
    venueStatus: raw.venueStatus || null,
    estimatedAttendance: raw.estimatedAttendance ?? CLAIM_KIND.UNKNOWN,
    estimatedAttendanceClaimKind: raw.estimatedAttendanceClaimKind || CLAIM_KIND.UNKNOWN,
    estimatedPeakRooms: raw.estimatedPeakRooms ?? CLAIM_KIND.UNKNOWN,
    estimatedPeakRoomsClaimKind: raw.estimatedPeakRoomsClaimKind || CLAIM_KIND.UNKNOWN,
    estimatedNights: raw.estimatedNights ?? CLAIM_KIND.UNKNOWN,
    estimatedNightsClaimKind: raw.estimatedNightsClaimKind || CLAIM_KIND.UNKNOWN,
    estimatedRoomNights: raw.estimatedRoomNights ?? CLAIM_KIND.UNKNOWN,
    estimatedRoomNightsClaimKind: raw.estimatedRoomNightsClaimKind || CLAIM_KIND.UNKNOWN,
    estimatedMeetingSize: raw.estimatedMeetingSize ?? CLAIM_KIND.UNKNOWN,
    hotelFitScore: fit.hotelFitScore,
    physicalFitScore: fit.components.physicalFit,
    captureCapacityState: precision.captureCapacityState || raw.captureCapacityState || null,
    captureCapacityStateLabel:
      precision.captureCapacityStateLabel || raw.captureCapacityStateLabel || null,
    estimatedCaptureRooms:
      precision.estimatedCaptureRooms ?? raw.estimatedCaptureRooms ?? null,
    captureRatio: precision.captureRatio ?? raw.captureRatio ?? null,
    captureCapacityRationale:
      precision.captureCapacityRationale || raw.captureCapacityRationale || null,
    geographyFitScore: fit.components.geographyFit,
    timingScore: fit.components.timing,
    commercialValueScore: fit.components.commercialValue,
    historicalFitScore: fit.components.historicalFit,
    competitiveAccessibilityScore: fit.components.competitiveAccessibility,
    contactabilityScore: fit.components.contactability,
    hotelFitComponentLabels: { ...HOTEL_FIT_COMPONENT_LABELS },
    evidenceConfidence: conf.evidenceConfidence,
    evidenceConfidenceFactors: conf.factors,
    evidenceConfidenceExplanation,
    evidenceConfidenceTooltip: EVIDENCE_CONFIDENCE_TOOLTIP,
    bookingWindowStatus,
    bookingWindowLabel: BOOKING_WINDOW_LABEL[bookingWindowStatus] || bookingWindowStatus,
    demandTerritoryFit,
    demandTerritoryFitLabel: demandTerritoryFit
      ? DEMAND_TERRITORY_FIT_LABEL[demandTerritoryFit] || demandTerritoryFit
      : null,
    demandTerritoryRationale: raw.demandTerritoryRationale || null,
    demandTerritoryFitLocked: Boolean(raw.demandTerritoryFitLocked),
    bethesdaWinThesis: raw.bethesdaWinThesis || hotelOpportunityThesis || null,
    marketCompetitors: raw.marketCompetitors || [],
    sourcingStatus,
    sourcingStatusLabel: SOURCING_STATUS_LABEL[sourcingStatus] || sourcingStatus,
    alreadySourcedToHotelDisplay:
      raw.alreadySourcedToHotelDisplay ||
      SOURCING_STATUS_LABEL[sourcingStatus] ||
      "Unknown — requires hotel validation",
    incrementalValueStatus,
    incrementalValueStatusLabel:
      INCREMENTAL_VALUE_STATUS_LABEL[incrementalValueStatus] || incrementalValueStatus,
    whyNow: raw.whyNow || null,
    fitExplanation,
    summaryWhat: raw.summaryWhat || null,
    summaryWhyMatters: raw.summaryWhyMatters || null,
    summaryWhyHotel: raw.summaryWhyHotel || null,
    recommendedAction: precision.recommendedAction || raw.recommendedAction || null,
    likelyCompetitor: raw.likelyCompetitor || null,
    likelyStrCompetitor: raw.likelyStrCompetitor || null,
    likelyGroupCompetitor: raw.likelyGroupCompetitor || null,
    competitorRationale: raw.competitorRationale || null,
    primaryContact: precision.primaryContactEnriched || raw.primaryContact || null,
    contacts: raw.contacts || [],
    meetingHistory: raw.meetingHistory || [],
    evidence: evidenceFromSources,
    sources: raw.sources || buildSourcesSummary(evidenceFromSources),
    // V1.2 discovery routing — must survive seed → buildOpportunity → hygiene
    officialSource: primaryOfficial,
    officialSourceTitle: raw.officialSourceTitle || raw.sourceObject?.sourceTitle || null,
    evidenceSources: raw.evidenceSources || null,
    sourceAuthority: raw.sourceAuthority || raw.sourceObject?.sourceAuthority || null,
    isOfficialSource:
      raw.isOfficialSource ??
      raw.sourceObject?.isOfficial ??
      (primaryOfficial ? /\.(gov|edu|org)\b/i.test(String(primaryOfficial)) : null),
    sourceDomain: raw.sourceDomain || raw.sourceObject?.sourceDomain || null,
    sourceObject: raw.sourceObject || null,
    housingEvidence: raw.housingEvidence || null,
    roomDemandEvidence: raw.roomDemandEvidence || null,
    hotelDemandThesis: raw.hotelDemandThesis || raw.whyHotelDemand || null,
    openSourcingEvidence: raw.openSourcingEvidence ?? null,
    organizationLocation: raw.organizationLocation || null,
    eventLocation: raw.eventLocation || null,
    programLocation: raw.programLocation || null,
    projectLocation: raw.projectLocation || null,
    venueLocation: raw.venueLocation || null,
    hotelDemandLocation: raw.hotelDemandLocation || null,
    locationEvidenceText: raw.locationEvidenceText || raw.geographyEvidence || null,
    locationEvidenceUrl: raw.locationEvidenceUrl || primaryOfficial || null,
    locationEvidenceType: raw.locationEvidenceType || null,
    pageSignalClass: raw.pageSignalClass || null,
    housingStatus: raw.housingStatus || null,
    commercialEvidenceText: raw.commercialEvidenceText || null,
    commercialEvidenceUrl: raw.commercialEvidenceUrl || null,
    commercialEvidenceStatus: raw.commercialEvidenceStatus || null,
    sourcingStatusCommercial: raw.sourcingStatusCommercial || null,
    discoveryLane: raw.discoveryLane || raw.vertical || null,
    demandSignalType: raw.demandSignalType || null,
    eventDateGranularity: raw.eventDateGranularity || null,
    eventDateStatus: raw.eventDateStatus || null,
    eventYear: raw.eventYear || null,
    knownVsEstimated: buildKnownVsEstimated(raw, evidenceFromSources),
    competitors: raw.competitors || [],
    plannerConsideration: raw.plannerConsideration || null,
    researchStatus: raw.researchStatus || "QUALIFIED",
    researchMethodsAttempted:
      precision.researchMethodsAttempted || raw.researchMethodsAttempted || [],
    webhoundUsed: Boolean(raw.webhoundUsed),
    ampfyUsed: Boolean(raw.ampfyUsed),
    firstSeenAt: raw.firstSeenAt || now,
    lastVerifiedAt: raw.lastVerifiedAt || now,
    createdAt: raw.createdAt || now,
    updatedAt: now,
    labels: raw.labels || [],
  };
}

export function filterSalespersonView(opportunities) {
  return (opportunities || []).filter((o) => o.priority !== PRIORITY.DISQUALIFIED);
}

export function buildWeeklyBrief(opportunities, limit = 8) {
  const actionable = filterSalespersonView(opportunities)
    .filter(
      (o) =>
        o.bookingWindowStatus === BOOKING_WINDOW.CONTACT_NOW ||
        o.bookingWindowStatus === BOOKING_WINDOW.QUALIFY_NOW ||
        o.bookingWindowStatus === BOOKING_WINDOW.RESEARCH_FURTHER ||
        o.priority === PRIORITY.HIGH ||
        o.priority === PRIORITY.MEDIUM
    )
    .sort((a, b) => {
      const pRank = { HIGH_PRIORITY: 0, MEDIUM_PRIORITY: 1, WATCHLIST: 2 };
      const pr = (pRank[a.priority] ?? 9) - (pRank[b.priority] ?? 9);
      if (pr !== 0) return pr;
      return (b.hotelFitScore || 0) - (a.hotelFitScore || 0);
    })
    .slice(0, limit);

  return {
    question: "What should the sales team pursue?",
    generatedAt: new Date().toISOString(),
    items: actionable.map((o) => ({
      opportunityId: o.id,
      title: o.title,
      organizationName: o.organizationName,
      segment: o.segment,
      priority: o.priority,
      opportunityType: o.opportunityType || null,
      opportunityTypeLabel: o.opportunityTypeLabel || null,
      opportunityQualification: o.opportunityQualification || null,
      opportunityQualificationLabel: o.opportunityQualificationLabel || null,
      venueSourcingStatus: o.venueSourcingStatus || null,
      venueSourcingStatusLabel: o.venueSourcingStatusLabel || null,
      roomDemandStatus: o.roomDemandStatus || null,
      roomDemandStatusLabel: o.roomDemandStatusLabel || null,
      hotelOpportunityThesis: o.hotelOpportunityThesis || o.bethesdaWinThesis || null,
      contactQuality: o.contactQuality || null,
      contactQualityLabel: o.contactQualityLabel || null,
      contactGrade: o.contactGrade || null,
      contactGradeLabel: o.contactGradeLabel || null,
      contactConfidence: o.contactConfidence ?? null,
      whyThisContact: o.whyThisContact || null,
      estimatedSize:
        o.estimatedPeakRooms !== CLAIM_KIND.UNKNOWN
          ? `${o.estimatedPeakRooms} peak rooms (${CLAIM_KIND_LABEL[o.estimatedPeakRoomsClaimKind] || o.estimatedPeakRoomsClaimKind})`
          : o.estimatedAttendance !== CLAIM_KIND.UNKNOWN
            ? `${o.estimatedAttendance} attendees (${CLAIM_KIND_LABEL[o.estimatedAttendanceClaimKind] || o.estimatedAttendanceClaimKind})`
            : "Size UNKNOWN",
      eventTiming: [o.eventStartDate, o.eventEndDate].filter(Boolean).join(" → ") || "Dates TBD",
      hotelFitScore: o.hotelFitScore,
      evidenceConfidence: o.evidenceConfidence,
      demandTerritoryFit: o.demandTerritoryFit,
      demandTerritoryFitLabel: o.demandTerritoryFitLabel,
      bethesdaWinThesis: o.bethesdaWinThesis || null,
      sourcingStatus: o.sourcingStatus,
      sourcingStatusLabel: o.sourcingStatusLabel,
      incrementalValueStatus: o.incrementalValueStatus,
      incrementalValueStatusLabel: o.incrementalValueStatusLabel,
      whyItMatters: o.summaryWhyMatters,
      whyNow: o.whyNow,
      primaryContact: o.primaryContact,
      competitorRisk: o.likelyCompetitor,
      likelyStrCompetitor: o.likelyStrCompetitor,
      recommendedNextStep: o.recommendedAction,
      bookingWindowStatus: o.bookingWindowStatus,
      bookingWindowLabel: o.bookingWindowLabel,
      plannerConsideration: o.plannerConsideration
        ? {
            appeared: o.plannerConsideration.bethesdaAppeared,
            rank: o.plannerConsideration.bethesdaRank,
            experimental: true,
          }
        : null,
    })),
  };
}

/**
 * Build internal/admin pilot metrics from opportunities + feedback.
 * Not shown to external share users.
 */
export function buildPilotMetrics(opportunities = [], feedbackDoc = null) {
  const list = opportunities || [];
  const qualified = list.filter((o) => o.priority !== PRIORITY.DISQUALIFIED);
  const feedbackItems = feedbackDoc?.items || feedbackDoc?.feedback || [];
  const byOpp = new Map();
  for (const f of feedbackItems) {
    if (!f?.opportunityId) continue;
    byOpp.set(f.opportunityId, f);
  }

  let validated = 0;
  let newToHotel = 0;
  let alreadySourced = 0;
  let worthPursuing = 0;
  let falsePositive = 0;
  let duplicate = 0;
  let contacted = 0;
  let won = 0;
  let lost = 0;

  for (const o of qualified) {
    const fb = byOpp.get(o.id);
    if (fb && (fb.familiarity || fb.commercialStatus || fb.value || fb.quality)) {
      validated += 1;
    }
    if (
      o.incrementalValueStatus === INCREMENTAL_VALUE_STATUS.NEW_TO_HOTEL ||
      o.incrementalValueStatus === INCREMENTAL_VALUE_STATUS.NEW_OPPORTUNITY ||
      fb?.familiarity === "Never Seen"
    ) {
      newToHotel += 1;
    }
    if (
      String(o.sourcingStatus || "").startsWith("HOTEL_CONFIRMED_ALREADY") ||
      fb?.familiarity === "Already Received" ||
      fb?.familiarity === "Already Pursuing"
    ) {
      alreadySourced += 1;
    }
    if (
      fb?.commercialStatus === "Worth Pursuing" ||
      fb?.quality === "Worth Pursuing" ||
      fb?.quality === "Excellent" ||
      fb?.quality === "Excellent Lead"
    ) {
      worthPursuing += 1;
    }
    if (fb?.quality === "Wrong / False Positive" || fb?.commercialStatus === "Not a Fit") {
      falsePositive += 1;
    }
    if (
      o.incrementalValueStatus === INCREMENTAL_VALUE_STATUS.DUPLICATE_OF_EXISTING_SALES_LEAD ||
      fb?.incrementalValueStatus === INCREMENTAL_VALUE_STATUS.DUPLICATE_OF_EXISTING_SALES_LEAD
    ) {
      duplicate += 1;
    }
    if (fb?.salesOutcome === "Contacted" || fb?.salesOutcome === "Response Received") {
      contacted += 1;
    }
    if (fb?.salesOutcome === "Won" || fb?.commercialStatus === "Already Won") won += 1;
    if (
      fb?.salesOutcome === "Lost" ||
      fb?.commercialStatus === "Already Lost" ||
      fb?.commercialStatus === "Already Booked Elsewhere"
    ) {
      lost += 1;
    }
  }

  const pct = (n, d) => (d > 0 ? Math.round((n / d) * 1000) / 10 : null);

  const high = qualified.filter((o) => o.priority === PRIORITY.HIGH);
  const highNamedContact = high.filter(
    (o) =>
      o.contactQuality === "NAMED_DECISION_MAKER" ||
      o.contactQuality === "NAMED_EVENT_MEETINGS_CONTACT" ||
      o.contactQuality === "HOUSING_SOURCING_CONTACT"
  ).length;
  const closedByVenue = list.filter(
    (o) =>
      o.venueSourcingStatus === "FULLY_PLACED" ||
      o.venueSourcingStatus === "PRIMARY_VENUE_SELECTED_NO_OVERFLOW_EVIDENCE" ||
      o.opportunityQualification === "CLOSED"
  ).length;
  const roomDemandUnknown = qualified.filter((o) => o.roomDemandStatus === "UNKNOWN").length;
  const geoUnknown = qualified.filter((o) => o.eventLocationStatus === "UNKNOWN").length;

  return {
    qualifiedOpportunities: qualified.length,
    highPriority: high.length,
    mediumPriority: qualified.filter((o) => o.priority === PRIORITY.MEDIUM).length,
    watchlist: qualified.filter((o) => o.priority === PRIORITY.WATCHLIST).length,
    pctValidatedByHotel: pct(validated, qualified.length),
    pctNewToHotel: pct(newToHotel, qualified.length),
    pctAlreadySourced: pct(alreadySourced, qualified.length),
    pctWorthPursuing: pct(worthPursuing, qualified.length),
    pctFalsePositive: pct(falsePositive, qualified.length),
    pctDuplicateExistingLeads: pct(duplicate, qualified.length),
    opportunitiesContacted: contacted,
    opportunitiesWon: won,
    opportunitiesLost: lost,
    rfpsGenerated: feedbackItems.filter((f) => f.salesOutcome === "RFP Received").length,
    // Qualification-precision pilot metrics (internal)
    highPriorityPrecisionProxyNote:
      "Actionability / High precision require hotel validation; rates below are structural proxies until validated.",
    discoveryRatePct: pct(newToHotel, qualified.length),
    actionabilityRatePct: pct(worthPursuing, Math.max(validated, 1)),
    highPriorityNamedContactRatePct: pct(highNamedContact, Math.max(high.length, 1)),
    qualificationFailureRatePct: pct(closedByVenue, list.length),
    roomDemandUnknownRatePct: pct(roomDemandUnknown, Math.max(qualified.length, 1)),
    eventLocationUnknownRatePct: pct(geoUnknown, Math.max(qualified.length, 1)),
    note:
      validated === 0
        ? "Cannot yet determine incremental value — hotel validation feedback has not been collected."
        : null,
    audience: "internal_admin_only",
  };
}
