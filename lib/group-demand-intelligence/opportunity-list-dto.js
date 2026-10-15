/**
 * Sparse opportunity projection for list/browse opens.
 * Full opportunity (evidence, competitors, sources, theses, audits) stays on detail GET.
 * Live Commercial Quality V1: include weekly + commercial summary fields for browse.
 *
 * Card PRESENTATION is owned by the UI tile (pre-V5A Bethesda structure).
 * Customer ELIGIBILITY is owned by customer-visibility / active-eligibility gates.
 * Do not couple presentation enrichment into this DTO.
 */

import { formatCustomerDate, unknownDisplay } from "./live-commercial-quality-v1.js";
import { DEMAND_FAMILY_LABEL } from "./demand-signal-types.js";
import { OPPORTUNITY_TYPE_LABEL } from "./claim-types.js";
import {
  classifyOpportunityTypeFilter,
  OPPORTUNITY_TYPE_FILTER_LABEL,
} from "./opportunity-type-filter-v1.js";
import { isGdiCustomerOpportunityReady } from "./customer-readiness-gate-v1.js";
import { evaluateGdiSummaryQuality } from "./opportunity-summary-v1.js";
import { isLegacyVisibilityCompatible } from "./legacy-visibility-compatibility-v1.js";
import {
  buildContactDisplaySurface,
  applyContactDisplaySurfaceToOpportunity,
} from "./contact-display-surface-v1.js";

/** Assessment-status prose is not a sales opportunity summary. */
function isAssessmentStatusProse(text) {
  const t = String(text || "");
  return (
    /dimension-level hotel-fit assessment/i.test(t) ||
    /overall numeric (provisional\/)?withheld/i.test(t) ||
    /weights renormalized/i.test(t) ||
    /applicable dimensions still lacking evidence/i.test(t) ||
    /^provisional hotel-fit score\b/i.test(t)
  );
}

function salesFacingSummaryField(primary, fallback) {
  if (primary && !isAssessmentStatusProse(primary)) return primary;
  if (fallback && !isAssessmentStatusProse(fallback)) return fallback;
  return null;
}

export const GDI_OPPORTUNITY_LIST_SCHEMA = "gdi_opportunity_list_v2";

/**
 * Prefer structured assessment over a stale flat Hotel Fit column.
 * When overall numeric is withheld, never surface a leftover score.
 */
export function reconcileHotelFitScoreForDisplay(opportunity = {}) {
  const basis = opportunity.hotelFitCalculationBasis || {};
  const status = String(opportunity.hotelFitOverallStatus || "").toLowerCase();
  const rationale = String(opportunity.hotelFitRationale || opportunity.hotelFitScoreNote || "");
  const withheldExplicit =
    basis.numericScoreEmitted === false ||
    status === "insufficient_evidence" ||
    /overall numeric withheld|withheld — insufficient|not scored — evidence/i.test(rationale) ||
    /overall numeric withheld/i.test(String(basis.reason || ""));
  if (withheldExplicit) {
    return null;
  }
  const raw = opportunity.hotelFitScore;
  if (raw == null || raw === "") return null;
  const n = Number(raw);
  return Number.isFinite(n) ? n : null;
}

/** Strip provider brand names from customer-facing evidence copy. */
export function scrubCustomerEvidenceCopy(text) {
  if (text == null || text === "") return text == null ? null : "";
  return String(text)
    .replace(/\bpreviously verified via Surfe\b/gi, "established from prior verified enrichment")
    .replace(/\bverified via Surfe\b/gi, "established from prior verified enrichment")
    .replace(/\bvia Surfe\b/gi, "from prior verified enrichment")
    .replace(/\bSurfe\b/gi, "enrichment provider")
    .replace(/\bWebhound\b/gi, "research provider")
    .replace(/\s+/g, " ")
    .trim();
}

function slimPrimaryContact(contact) {
  if (!contact || typeof contact !== "object") return null;
  // Explicit allowlist — never pass Surfe/provider/cost/job metadata to list API
  // Do not surface organization inbox as person email (emailOwnership gate)
  const emailOwnership = contact.emailOwnership || null;
  const email = emailOwnership === "organization_or_department" ? null : contact.email || null;
  return {
    name: contact.name || null,
    role: contact.role || contact.title || null,
    title: contact.title || null,
    email,
    emailOwnership: email ? emailOwnership || "person_specific" : null,
    phone: contact.phone || null,
    phoneTypeLabel:
      contact.phoneTypeLabel ||
      (contact.phone && (!contact.phoneType || /unknown/i.test(String(contact.phoneType)))
        ? "Phone"
        : contact.phoneTypeLabel) ||
      null,
    relevanceEvidence: scrubCustomerEvidenceCopy(
      contact.relevanceEvidence || contact.whyThisContact || null
    ),
    suggestedContactLabel: contact.suggestedContactLabel || null,
    organizationContactRouteLabel: contact.organizationContactRouteLabel || null,
    askToBeDirectedTo: contact.askToBeDirectedTo || null,
  };
}

/**
 * Fields required for browse cards, filters, sorts, and facets.
 * Drawer/detail continues to use GET .../opportunities/:id (full record).
 */
/**
 * Browse-card display title (presentation only — does not mutate canonical title).
 *
 * Prefer a concise account/occasion title already stored on the record
 * (e.g. "AHIMA Advocacy Summit 2027"). Do NOT prepend the full legal
 * organization name when that would duplicate the card meta line or inflate
 * research-note leftovers into "Org — Org acronym Occasion".
 */
export function accountLevelDisplayTitle(opportunity = {}) {
  const o = opportunity || {};
  const org = String(o.organizationName || "").trim();
  const title = String(o.title || "").trim();
  if (!title && !org) return null;
  if (!title) return org || null;

  const stripRoleParen = (t) =>
    String(t || "")
      .replace(
        /\s*\((VENUE OPERATOR|ORGANIZER|SECRETARIAT|PARENT SOCIETY|UNIVERSITY HOST|EVENT ORGANIZER|ORGANIZING TEAM)\)\s*$/i,
        ""
      )
      .trim();

  const cleaned = stripRoleParen(title) || title;

  // Research-note / verification titles — strip sourcing commentary for display.
  if (
    /verified via|verified:|hotel not named|hotel block tba|supported recurring|unconfirmed this pass|exact next dates/i.test(
      cleaned
    )
  ) {
    const left = cleaned.split(/\s*[—–-]\s*/)[0].trim();
    if (left && left.length <= 90) return left;
  }

  // Concise occasion titles already good as-is (with or without year).
  if (
    cleaned.length <= 90 &&
    !/^https?:/i.test(cleaned) &&
    !/verified via|verified:/i.test(cleaned)
  ) {
    // Already org-led with full legal name — keep (stripRoleParen applied).
    if (org && cleaned.toLowerCase().startsWith(org.toLowerCase())) {
      return cleaned;
    }
    // Short occasion title — do NOT prefix full legal org (meta line carries org/segment).
    return cleaned;
  }

  if (org && cleaned.toLowerCase().includes(org.toLowerCase())) {
    return cleaned;
  }

  const role = String(o.participationRole || o.childEntityType || "")
    .replace(/_/g, " ")
    .trim();
  const motionLabel = (role || "Group demand")
    .replace(/\b\w/g, (c) => c.toUpperCase())
    .replace(/\s+/g, " ")
    .trim();
  return cleaned || `${org} — ${motionLabel}`;
}

function sanitizePeakRoomsValue(raw) {
  if (raw == null || raw === "") return null;
  if (typeof raw === "number" && Number.isFinite(raw)) return raw;
  const s = String(raw).trim();
  if (!s) return null;
  // Housing-program status must never appear as a peak-rooms value
  if (/housing\s*program|verified\s*housing|stay[- ]to[- ]play/i.test(s) && !/\d/.test(s)) {
    return null;
  }
  if (/^\d+(\.\d+)?$/.test(s)) return Number(s);
  if (/\d/.test(s) && !/housing|program/i.test(s)) return s;
  return null;
}

function housingProgramFields(o = {}) {
  const status = String(o.housingProgramStatus || o.roomDemandStatus || "").toUpperCase();
  const label = o.housingProgramStatusLabel || o.roomDemandStatusLabel || null;
  if (
    status === "VERIFIED_HOUSING_PROGRAM" ||
    /verified housing program/i.test(String(label || ""))
  ) {
    return {
      housingProgramStatus: "VERIFIED_HOUSING_PROGRAM",
      housingProgramStatusLabel: label || "Verified Housing Program",
    };
  }
  return {
    housingProgramStatus: o.housingProgramStatus || null,
    housingProgramStatusLabel: o.housingProgramStatusLabel || null,
  };
}

export function toOpportunityListDto(opportunity) {
  const raw = opportunity || {};
  // Normalize misfiled top-level contactEmail/contactPhone into display surface
  // so cards never hide a stored usable company phone/email behind "No primary contact".
  const surface = buildContactDisplaySurface(raw);
  const o = applyContactDisplaySurfaceToOpportunity(raw, surface);
  const peak = sanitizePeakRoomsValue(
    o.peakRooms ?? o.estimatedPeakRooms ?? o.publishedPeakRooms ?? null
  );
  const housing = housingProgramFields(o);
  const att = o.attendance ?? o.estimatedAttendance ?? o.publishedAttendance ?? null;
  const opportunityType = o.opportunityType || null;
  const demandFamily = o.demandFamily || null;
  const buyerRole = o.buyerRole || o.primaryContactRole || o.primaryContact?.role || null;
  const publicContactPath =
    o.publicContactPath ||
    o.organizationContactUrl ||
    o.officialContactPath ||
    o.contactUrl ||
    null;
  return {
    id: o.id,
    hotelId: o.hotelId || null,
    title: o.title || null,
    displayTitle: accountLevelDisplayTitle(o),
    organizationName: o.organizationName || null,
    buyerEntity: o.buyerEntity || o.organizer || null,
    buyerRole,
    publicContactPath,
    whoPathClass: o.whoPathClass || o.contactPathClass || null,
    segment: o.segment || null,
    priority: o.priority || null,
    eventStartDate: o.eventStartDate || null,
    eventEndDate: o.eventEndDate || null,
    eventDateStatus: o.eventDateStatus || null,
    eventDateGranularity: o.eventDateGranularity || null,
    eventDateDisplay: o.eventDateDisplay || formatCustomerDate(o),
    eventTiming: o.eventTiming || null,
    activeDateClass: o.activeDateClass || null,
    opportunityType,
    opportunityTypeLabel:
      o.opportunityTypeLabel ||
      (opportunityType ? OPPORTUNITY_TYPE_LABEL[opportunityType] || null : null),
    researchSegment: o.researchSegment || null,
    opportunityTypeFilterCategory: classifyOpportunityTypeFilter(o),
    opportunityTypeFilterLabel:
      OPPORTUNITY_TYPE_FILTER_LABEL[classifyOpportunityTypeFilter(o)] || null,
    demandFamily,
    demandFamilyLabel:
      o.demandFamilyLabel || (demandFamily ? DEMAND_FAMILY_LABEL[demandFamily] || null : null),
    demandSignalType: o.demandSignalType || null,
    demandSignalTypeLabel: o.demandSignalTypeLabel || null,
    peVenueId: o.peVenueId || null,
    peSignalId: o.peSignalId || null,
    hotelVenueFitId: o.hotelVenueFitId || null,
    newnessStatus: o.newnessStatus || null,
    recurrenceClass: o.recurrenceClass || null,
    potentialRoomNights: o.potentialRoomNights ?? null,
    potentialRoomNightsStatus: o.potentialRoomNightsStatus || null,
    hotelFitScore: reconcileHotelFitScoreForDisplay(o),
    hotelFitOverallStatus: o.hotelFitOverallStatus || null,
    hotelFitPosture: o.hotelFitPosture || null,
    hotelFitScoreNote: o.hotelFitScoreNote || null,
    hotelFitRationale: o.hotelFitRationale || null,
    hotelFitQualitativeComponents: o.hotelFitQualitativeComponents || null,
    hotelFitDimensionAssessments: o.hotelFitDimensionAssessments || null,
    hotelFitCalculationBasis: o.hotelFitCalculationBasis || null,
    hotelFitAssessmentVersion: o.hotelFitAssessmentVersion || null,
    hotelFitProvisional: o.hotelFitProvisional === true,
    hotelFitPostureNote: o.hotelFitPostureNote || null,
    hotelFitAssessedAt: o.hotelFitAssessedAt || null,
    physicalFitScore: o.physicalFitScore ?? null,
    geographyFitScore: o.geographyFitScore ?? null,
    timingScore: o.timingScore ?? null,
    commercialValueScore: o.commercialValueScore ?? null,
    historicalFitScore: o.historicalFitScore ?? null,
    competitiveAccessibilityScore: o.competitiveAccessibilityScore ?? null,
    contactabilityScore: o.contactabilityScore ?? null,
    evidenceConfidence: o.evidenceConfidence ?? null,
    evidenceConfidenceExplanation: o.evidenceConfidenceExplanation || null,
    summaryWhat: salesFacingSummaryField(o.summaryWhat, o.summaryWhyMatters),
    // Hotel-specific thesis on browse so list cards match drawer substance.
    // Assessment-status prose stays out of card summary (belongs in Why This Hotel).
    hotelOpportunityThesis: salesFacingSummaryField(
      o.hotelOpportunityThesis,
      o.summaryWhat || o.summaryWhyMatters
    ),
    summaryWhyHotel: salesFacingSummaryField(
      o.summaryWhyHotel,
      o.summaryWhat || o.summaryWhyMatters
    ),
    fitExplanation: o.fitExplanation || null,
    // Coordinator hotel-relevance research (mandatory before qualify/cards)
    hotelRelevanceResearch: o.hotelRelevanceResearch || null,
    hotelRelevanceResearchVersion: o.hotelRelevanceResearchVersion || null,
    hotelRelevanceDisposition: o.hotelRelevanceDisposition || null,
    hotelRelevanceResearchedAt: o.hotelRelevanceResearchedAt || null,
    destinationStatus: o.destinationStatus || null,
    eventCity: o.eventCity || null,
    venueCity: o.venueCity || null,
    whyNow: o.whyNow || null,
    recommendedAction: o.recommendedAction || null,
    recommendedNextStep: o.recommendedNextStep || null,
    opportunityQualification: o.opportunityQualification || null,
    opportunityQualificationLabel: o.opportunityQualificationLabel || null,
    bookingWindowStatus: o.bookingWindowStatus || null,
    bookingWindowLabel: o.bookingWindowLabel || null,
    demandTerritoryFit: o.demandTerritoryFit || null,
    demandTerritoryFitLabel: o.demandTerritoryFitLabel || null,
    primaryContact: slimPrimaryContact(o.primaryContact || surface.primaryContact),
    functionalContactEmail: surface.functionalContactEmail || o.functionalContactEmail || null,
    organizationContactEmail:
      surface.organizationContactEmail ||
      o.organizationContactEmail ||
      o.functionalContactEmail ||
      null,
    companyPhone: surface.companyPhone || o.companyPhone || null,
    companyPhoneLabel: surface.companyPhoneLabel || o.companyPhoneLabel || null,
    contactEmail: surface.functionalContactEmail || null,
    contactPhone: surface.companyPhone || null,
    contactUrl: surface.contactUrl || publicContactPath || null,
    contactDisplayLines: surface.cardLines || null,
    contactDisplaySurfaceVersion: surface.version || null,
    namedBuyerEnrichmentComplete: surface.namedBuyerEnrichmentComplete === true,
    contactChannelSeparation: {
      suggestedPerson: surface.contactChannelSeparation?.suggestedPerson || null,
      personSpecificEmail: surface.contactChannelSeparation?.personSpecificEmail || null,
      personSpecificPhone: surface.contactChannelSeparation?.personSpecificPhone || null,
      organizationRoute: surface.contactChannelSeparation?.organizationRoute || null,
      companyPhone: surface.contactChannelSeparation?.companyPhone || null,
      askToBeDirectedTo: surface.contactChannelSeparation?.askToBeDirectedTo || null,
    },
    contactPathClass: o.contactPathClass || o.whoPathClass || null,
    // Reviewed prospect publication (customer-safe labels; not Ready)
    prospectPublicationBasis: o.prospectPublicationBasis || null,
    prospectReviewCardId: o.prospectReviewCardId || null,
    prospectReviewSection: o.prospectReviewSection || null,
    // Packaging class — independent of Priority High/Medium/Watch filter
    prospectCommercialClassLabel:
      o.prospectCommercialClassLabel ||
      (String(o.prospectReviewSection || "").toUpperCase() === "EXPLORATORY"
        ? "Exploratory account"
        : String(o.prospectReviewSection || "").toUpperCase() === "PRIORITY"
          ? "Core prospect"
          : null),
    prospectCustomerLabel: o.prospectCustomerLabel || o.prospectCommercialLabel || null,
    prospectCommercialLabel: o.prospectCommercialLabel || null,
    priorityReason: o.priorityReason || null,
    bookingWindowReason: o.bookingWindowReason || null,
    priorityActionAssignmentVersion: o.priorityActionAssignmentVersion || null,
    venueSourcingStatusLabel: o.venueSourcingStatusLabel || null,
    bookingResponsibilityLabel: o.bookingResponsibilityLabel || null,
    primaryContactPhoneTypeLabel:
      o.primaryContactPhoneTypeLabel || o.primaryContact?.phoneTypeLabel || null,
    contactCompletenessGroup: o.contactCompletenessGroup ?? null,
    prospectHotelFacing: o.prospectHotelFacing === true,
    peakRoomsClaimKind: o.peakRoomsClaimKind || o.estimatedPeakRoomsClaimKind || null,
    roomDemandStatus: o.roomDemandStatus || null,
    roomDemandStatusLabel:
      housing.housingProgramStatus === "VERIFIED_HOUSING_PROGRAM"
        ? null
        : o.roomDemandStatusLabel || null,
    housingStatus: o.housingStatus || null,
    housingStatusLabel: o.housingStatusLabel || null,
    housingProgramStatus: housing.housingProgramStatus,
    housingProgramStatusLabel: housing.housingProgramStatusLabel,
    estimatedPeakRooms: peak,
    peakRooms: peak,
    peakRoomsStatus: o.peakRoomsStatus || null,
    eventLocationSummary: o.eventLocationSummary || null,
    estimatedAttendance: att,
    attendance: att,
    attendanceStatus: o.attendanceStatus || null,
    weeklyDeltaState: o.weeklyDeltaState || null,
    isNewThisWeek: o.isNewThisWeek === true,
    eventSeriesId: o.eventSeriesId || null,
    relatedOpportunityCount: Array.isArray(o.relatedOpportunityIds)
      ? o.relatedOpportunityIds.length
      : o.relatedOpportunityCount || 0,
    customerFacingState: o.customerFacingState || null,
    customerSurfaceDisposition: o.customerSurfaceDisposition || null,
    gdiMaturityState: o.gdiMaturityState || null,
    gdiMaturityReason: o.gdiMaturityReason || null,
    expansionPilotId: o.expansionPilotId || null,
    parentDemandSignalId: o.parentDemandSignalId || null,
    parentDemandSignalLabel: o.parentDemandSignalLabel || null,
    parentDemandSignalType: o.parentDemandSignalType || null,
    travelingCohortType: o.travelingCohortType || null,
    travelingCohortSummary: o.travelingCohortSummary || null,
    travelingCohortConfidence: o.travelingCohortConfidence || null,
    lodgingControlHypothesis: o.lodgingControlHypothesis || null,
    lodgingControlSummary: o.lodgingControlSummary || null,
    lodgingControlConfidence: o.lodgingControlConfidence || null,
    // Future Watch early-warning card (customer-safe; not Ready)
    watchCardWhyMatters: o.watchCardWhyMatters || null,
    watchCardCurrentStatus: o.watchCardCurrentStatus || null,
    watchCardLodgingController: o.watchCardLodgingController || null,
    watchCardHotelSelectionStatus: o.watchCardHotelSelectionStatus || null,
    watchCardWhenToAct: o.watchCardWhenToAct || null,
    watchCardNextTrigger: o.watchCardNextTrigger || o.nextTriggerCondition || null,
    watchCardMonitoringStatus: o.watchCardMonitoringStatus || null,
    watchCardMonitoringLastChecked: o.watchCardMonitoringLastChecked || null,
    watchCardMonitoringNextExpected: o.watchCardMonitoringNextExpected || null,
    watchCardRecommendedNextStep: o.watchCardRecommendedNextStep || o.recommendedAction || null,
    hotelSelectionProcess: o.hotelSelectionProcess || null,
    outreachReadiness: o.outreachReadiness || null,
    nextTriggerCondition: o.nextTriggerCondition || null,
    // Pursuit workflow mirror (sales status — independent of Ready/Watch)
    pursuitId: o.pursuitId || null,
    pursuitStatus: o.pursuitStatus || null,
    canStartPursuit:
      o.canStartPursuit === true ||
      /^(OUTREACH_NOW|OUTREACH_PREPARE)$/i.test(String(o.outreachReadiness || "")),
    modeledRoomsMin: o.modeledRoomsMin ?? null,
    modeledRoomsMax: o.modeledRoomsMax ?? null,
    modeledDemandDisclaimer: o.modeledDemandDisclaimer || null,
    lodgingVerified: o.lodgingVerified === true,
    headcountVerified: o.headcountVerified === true,
    salesWorkflowState: o.salesWorkflowState || null,
    buyerResearchStatus: o.buyerResearchStatus || null,
    recommendedNextAction: o.recommendedNextAction || o.recommendedAction || null,
    missingValidation: Array.isArray(o.missingValidation) ? o.missingValidation : null,
    // Internal readiness / convergence diagnostics (UI may ignore)
    readinessState: o.customerReadiness?.state || isGdiCustomerOpportunityReady(o).state || null,
    summaryQuality: o.summaryQuality || evaluateGdiSummaryQuality(o).quality || null,
    legacyCompatibility: isLegacyVisibilityCompatible(o),
    _listDto: true,
    schemaVersion: GDI_OPPORTUNITY_LIST_SCHEMA,
  };
}

export function mapOpportunitiesToListDto(opportunities) {
  return (opportunities || []).map(toOpportunityListDto);
}

export { unknownDisplay };
