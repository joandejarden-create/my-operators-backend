/**
 * GDI Research Airtable Source-of-Truth field ownership V1
 *
 * Permanent contract: enrich the SoT; never silently degrade it.
 * Presentation / slim / STAGED representations are not authoritative over
 * richer research findings merely because they are newer.
 */

export const GDI_RESEARCH_SOT_FIELD_OWNERSHIP_V1 =
  "gdi-research-sot-field-ownership-v1";

/**
 * Authoritative research intelligence — preserve unless independently
 * supported by stronger evidence. Unknown existing keys default to this class.
 */
export const SOT_AUTHORITATIVE_RESEARCH_KEYS = Object.freeze([
  "organizationName",
  "organizationKey",
  "organizationAliases",
  "organizationId",
  "organizationEntityId",
  "meetingHistory",
  "competitors",
  "marketCompetitors",
  "likelyCompetitor",
  "likelyGroupCompetitor",
  "likelyStrCompetitor",
  "competitorRationale",
  "evidence",
  "evidenceConfidence",
  "evidenceConfidenceExplanation",
  "evidenceConfidenceFactors",
  "sources",
  "sourceSummary",
  "sourceUrl",
  "contacts",
  "backupContacts",
  "namedStakeholders",
  "primaryContact",
  "secondaryContact",
  "contactResearchAudit",
  "contactGrade",
  "contactQuality",
  "contactConfidence",
  "contactabilityScore",
  "contactRoleMatch",
  "whyThisContact",
  "revenueStreamClass",
  "revenueStreamLabel",
  "revenueStreamIcons",
  "commercialDemandType",
  "commercialDemandTypeLabel",
  "commercialDemandTypeEvidence",
  "roomDemandStatus",
  "roomDemandLive",
  "roomDemandRationale",
  "roomDemandConfidence",
  "guestroomDemandConfidence",
  "eventFnbDemandConfidence",
  "guestroomThesis",
  "eventFnbThesis",
  "hotelOpportunityThesis",
  "hotelDemandThesis",
  "bethesdaWinThesis",
  "summaryWhat",
  "summaryWhyHotel",
  "summaryWhyMatters",
  "summaryWhyNow",
  "whyNow",
  "whyThisHotel",
  "whatIsHappening",
  "fitExplanation",
  "hotelFitScore",
  "commercialValueScore",
  "geographyFitScore",
  "historicalFitScore",
  "physicalFitScore",
  "timingScore",
  "competitiveAccessibilityScore",
  "demandTerritoryFit",
  "demandTerritoryRationale",
  "venueStatus",
  "venueSourcingStatus",
  "sourcingStatus",
  "futureCycleEvidenceState",
  "futureCycleSignals",
  "eventStartDate",
  "eventEndDate",
  "eventDateDisplay",
  "eventDateStatus",
  "attendance",
  "estimatedAttendance",
  "estimatedRoomNights",
  "estimatedPeakRooms",
  "estimatedNights",
  "opportunityType",
  "opportunityTypeLabel",
  "demandMechanism",
  "demandType",
  "demandStatus",
  "researchStatus",
  "researchMethodsAttempted",
  "researchVersion",
  "labels",
  "knownVsEstimated",
  "highPriorityGate",
  "qualificationGatePassed",
  "qualificationNotes",
  "opportunityQualification",
  "customerReadiness",
  "salesActionPackageV1",
  "commercialSurfaceDisposition",
  "commercialQualityV1",
  "titleQualityRepair",
  "provenance",
  "createdAt",
  "firstSeenAt",
  "lastVerifiedAt",
]);

/**
 * Hotel-specific certification state — controlled FQO / Full E2E overlay.
 */
export const SOT_CERTIFICATION_OWNED_KEYS = Object.freeze([
  "hotelSpecificQualification",
  "finalizedQualified",
  "fqoEvaluation",
  "finalizedQualifiedEvaluation",
  "hotelWideDisposition",
  "salesActionKey",
  "salesActionPillLabel",
  "commercialStageKey",
  "commercialStagePillLabel",
  "certificationSchemaVersion",
  "qualificationProvenance",
  "cmiVerified",
  "commercialPayloadContractVersion",
]);

/**
 * Derived presentation / package values — never overwrite richer SoT facts.
 */
export const SOT_DERIVED_PRESENTATION_KEYS = Object.freeze([
  "slimPayloadVersion",
  "tileClassificationLine",
  "opportunityTypeFilterCategory",
  "opportunityTypeFilterLabel",
  "drawerHeaderTitle",
  "tenSectionDrawer",
  "approvedCustomerFacingDetail",
  "gdiPublicationPackageId",
  "gdiPublicationFingerprint",
  "customerPublicationReady",
]);

/** Telemetry / raw provider blobs — may be dropped; not commercial SoT. */
export const SOT_TELEMETRY_DROP_KEYS = Object.freeze([
  "orsRaw",
  "orsTranscript",
  "firecrawlRaw",
  "exaRawHits",
  "retrievalLog",
  "plannerTrace",
  "ledgerEntries",
  "deepResearchEnrichment",
  "supervisorV2",
  "exaAgentRouting",
  "supportingEvidenceDrawer",
  "rawProviderPacks",
]);

/** Revenue classes ordered weakest → strongest (downgrade is blocked). */
export const REVENUE_STREAM_STRENGTH = Object.freeze({
  GUESTROOMS_ONLY: 1,
  EVENT_FNB_ONLY: 1,
  COMBINED_GUESTROOMS_EVENT_FNB: 2,
});

export function classifySotPayloadKey(key) {
  const k = String(key || "");
  if (!k) return "UNKNOWN";
  if (SOT_TELEMETRY_DROP_KEYS.includes(k)) return "TELEMETRY";
  if (SOT_CERTIFICATION_OWNED_KEYS.includes(k)) return "CERTIFICATION";
  if (SOT_DERIVED_PRESENTATION_KEYS.includes(k)) return "DERIVED_PRESENTATION";
  if (SOT_AUTHORITATIVE_RESEARCH_KEYS.includes(k)) return "AUTHORITATIVE_RESEARCH";
  // Unknown existing keys are authoritative by default (preserve).
  return "AUTHORITATIVE_RESEARCH_UNKNOWN";
}

export default {
  GDI_RESEARCH_SOT_FIELD_OWNERSHIP_V1,
  SOT_AUTHORITATIVE_RESEARCH_KEYS,
  SOT_CERTIFICATION_OWNED_KEYS,
  SOT_DERIVED_PRESENTATION_KEYS,
  SOT_TELEMETRY_DROP_KEYS,
  REVENUE_STREAM_STRENGTH,
  classifySotPayloadKey,
};
