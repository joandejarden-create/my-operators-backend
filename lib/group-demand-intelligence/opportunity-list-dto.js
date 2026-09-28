/**
 * Sparse opportunity projection for list/browse opens.
 * Full opportunity (evidence, competitors, sources, theses, audits) stays on detail GET.
 * Live Commercial Quality V1: include weekly + commercial summary fields for browse.
 */

import { formatCustomerDate, unknownDisplay } from "./live-commercial-quality-v1.js";
import { DEMAND_FAMILY_LABEL } from "./demand-signal-types.js";
import { OPPORTUNITY_TYPE_LABEL } from "./claim-types.js";
import { applyCommercialCardContract } from "./commercial-card-contract-v1.js";

export const GDI_OPPORTUNITY_LIST_SCHEMA = "gdi_opportunity_list_v3";

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
 * Card commercial fields come from the shared card contract (not FE hardcodes).
 */
export function toOpportunityListDto(opportunity) {
  const enriched = applyCommercialCardContract(opportunity || {});
  const o = enriched;
  const peak = o.peakRooms ?? o.estimatedPeakRooms ?? o.publishedPeakRooms ?? null;
  const att = o.attendance ?? o.estimatedAttendance ?? o.publishedAttendance ?? null;
  const opportunityType = o.opportunityType || null;
  const demandFamily = o.demandFamily || null;
  const cardContact = o.cardContact || null;
  return {
    id: o.id,
    hotelId: o.hotelId || null,
    title: o.title || null,
    organizationName: o.organizationName || null,
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
    commercialMotion: o.commercialMotion || null,
    commercialMotionLabel: o.commercialMotionLabel || null,
    commercialSummary: o.commercialSummary || null,
    cardHotelFitLine: o.cardHotelFitLine || null,
    cardWhyNowLine: o.cardWhyNowLine || null,
    cardFitBadge: o.cardFitBadge || null,
    cardContact,
    entityClass: o.entityClass || null,
    peVenueId: o.peVenueId || null,
    peSignalId: o.peSignalId || null,
    hotelVenueFitId: o.hotelVenueFitId || null,
    newnessStatus: o.newnessStatus || null,
    recurrenceClass: o.recurrenceClass || null,
    potentialRoomNights: o.potentialRoomNights ?? null,
    potentialRoomNightsStatus: o.potentialRoomNightsStatus || null,
    hotelFitScore: o.hotelFitScore ?? null,
    evidenceConfidence: o.evidenceConfidence ?? null,
    summaryWhat: o.commercialSummary || o.summaryWhat || null,
    whyNow: o.cardWhyNowLine || o.whyNow || null,
    recommendedAction: o.recommendedAction || null,
    recommendedNextStep: o.recommendedNextStep || null,
    opportunityQualification: o.opportunityQualification || null,
    opportunityQualificationLabel: o.opportunityQualificationLabel || null,
    bookingWindowStatus: o.bookingWindowStatus || null,
    bookingWindowLabel: o.bookingWindowLabel || null,
    demandTerritoryFit: o.demandTerritoryFit || null,
    demandTerritoryFitLabel: o.demandTerritoryFitLabel || null,
    primaryContact: slimPrimaryContact(o.primaryContact),
    contactPathClass: o.contactPathClass || cardContact?.pathClass || null,
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
    _listDto: true,
    schemaVersion: GDI_OPPORTUNITY_LIST_SCHEMA,
  };
}

export function mapOpportunitiesToListDto(opportunities) {
  return (opportunities || []).map(toOpportunityListDto);
}

export { unknownDisplay };
