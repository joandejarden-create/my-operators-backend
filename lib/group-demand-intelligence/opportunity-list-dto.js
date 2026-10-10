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
import { isGdiCustomerOpportunityReady } from "./customer-readiness-gate-v1.js";
import { evaluateGdiSummaryQuality } from "./opportunity-summary-v1.js";
import { isLegacyVisibilityCompatible } from "./legacy-visibility-compatibility-v1.js";

export const GDI_OPPORTUNITY_LIST_SCHEMA = "gdi_opportunity_list_v2";

function slimPrimaryContact(contact) {
  if (!contact || typeof contact !== "object") return null;
  return {
    name: contact.name || null,
    role: contact.role || contact.title || null,
    title: contact.title || null,
    email: contact.email || null,
    phone: contact.phone || null,
    phoneTypeLabel: contact.phoneTypeLabel || null,
  };
}

/**
 * Fields required for browse cards, filters, sorts, and facets.
 * Drawer/detail continues to use GET .../opportunities/:id (full record).
 */
/**
 * Account-first display title for browse cards (presentation only — does not mutate canonical title).
 * Prefer [Organization] — [motion / event context] when title is event-first or role-parenthetical.
 */
export function accountLevelDisplayTitle(opportunity = {}) {
  const o = opportunity || {};
  const org = String(o.organizationName || "").trim();
  const title = String(o.title || "").trim();
  if (!org) return title || null;

  const stripRoleParen = (t) =>
    String(t || "")
      .replace(
        /\s*\((VENUE OPERATOR|ORGANIZER|SECRETARIAT|PARENT SOCIETY|UNIVERSITY HOST|EVENT ORGANIZER|ORGANIZING TEAM)\)\s*$/i,
        ""
      )
      .trim();

  // Already org-led: "Palexpo SA — Watches & Wonders…"
  if (title && title.toLowerCase().startsWith(org.toLowerCase())) {
    return stripRoleParen(title) || title;
  }

  // Bare / short event title (e.g. "AidEx Geneva") → Organization — Event
  if (title && !/[—–-]/.test(title)) {
    return `${org} — ${title}`;
  }

  // Title embeds org somewhere else
  if (title && title.toLowerCase().includes(org.toLowerCase())) {
    return stripRoleParen(title) || title;
  }

  const role = String(o.participationRole || o.childEntityType || "")
    .replace(/_/g, " ")
    .trim();
  const motionLabel = (role || "Group demand")
    .replace(/\b\w/g, (c) => c.toUpperCase())
    .replace(/\s+/g, " ")
    .trim();
  return title ? `${org} — ${stripRoleParen(title)}` : `${org} — ${motionLabel}`;
}

export function toOpportunityListDto(opportunity) {
  const o = opportunity || {};
  const peak = o.peakRooms ?? o.estimatedPeakRooms ?? o.publishedPeakRooms ?? null;
  const att = o.attendance ?? o.estimatedAttendance ?? o.publishedAttendance ?? null;
  const opportunityType = o.opportunityType || null;
  const demandFamily = o.demandFamily || null;
  const buyerRole =
    o.buyerRole || o.primaryContactRole || o.primaryContact?.role || null;
  const publicContactPath =
    o.publicContactPath ||
    o.organizationContactUrl ||
    o.officialContactPath ||
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
    demandFamily,
    demandFamilyLabel:
      o.demandFamilyLabel ||
      (demandFamily ? DEMAND_FAMILY_LABEL[demandFamily] || null : null),
    demandSignalType: o.demandSignalType || null,
    demandSignalTypeLabel: o.demandSignalTypeLabel || null,
    peVenueId: o.peVenueId || null,
    peSignalId: o.peSignalId || null,
    hotelVenueFitId: o.hotelVenueFitId || null,
    newnessStatus: o.newnessStatus || null,
    recurrenceClass: o.recurrenceClass || null,
    potentialRoomNights: o.potentialRoomNights ?? null,
    potentialRoomNightsStatus: o.potentialRoomNightsStatus || null,
    hotelFitScore: o.hotelFitScore ?? null,
    evidenceConfidence: o.evidenceConfidence ?? null,
    summaryWhat: o.summaryWhat || null,
    whyNow: o.whyNow || null,
    recommendedAction: o.recommendedAction || null,
    recommendedNextStep: o.recommendedNextStep || null,
    opportunityQualification: o.opportunityQualification || null,
    opportunityQualificationLabel: o.opportunityQualificationLabel || null,
    bookingWindowStatus: o.bookingWindowStatus || null,
    bookingWindowLabel: o.bookingWindowLabel || null,
    demandTerritoryFit: o.demandTerritoryFit || null,
    demandTerritoryFitLabel: o.demandTerritoryFitLabel || null,
    primaryContact: slimPrimaryContact(o.primaryContact),
    contactPathClass: o.contactPathClass || o.whoPathClass || null,
    peakRoomsClaimKind: o.peakRoomsClaimKind || o.estimatedPeakRoomsClaimKind || null,
    roomDemandStatus: o.roomDemandStatus || null,
    housingStatus: o.housingStatus || null,
    estimatedPeakRooms: peak,
    peakRooms: peak,
    peakRoomsStatus: o.peakRoomsStatus || null,
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
    watchCardRecommendedNextStep:
      o.watchCardRecommendedNextStep || o.recommendedAction || null,
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
    readinessState:
      o.customerReadiness?.state ||
      isGdiCustomerOpportunityReady(o).state ||
      null,
    summaryQuality:
      o.summaryQuality || evaluateGdiSummaryQuality(o).quality || null,
    legacyCompatibility: isLegacyVisibilityCompatible(o),
    _listDto: true,
    schemaVersion: GDI_OPPORTUNITY_LIST_SCHEMA,
  };
}

export function mapOpportunitiesToListDto(opportunities) {
  return (opportunities || []).map(toOpportunityListDto);
}

export { unknownDisplay };
