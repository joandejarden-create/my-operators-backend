/**
 * Sparse opportunity projection for list/browse opens.
 * Full opportunity (evidence, competitors, sources, theses, audits) stays on detail GET.
 */

export const GDI_OPPORTUNITY_LIST_SCHEMA = "gdi_opportunity_list_v1";

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
export function toOpportunityListDto(opportunity) {
  const o = opportunity || {};
  return {
    id: o.id,
    hotelId: o.hotelId || null,
    title: o.title || null,
    organizationName: o.organizationName || null,
    segment: o.segment || null,
    priority: o.priority || null,
    eventStartDate: o.eventStartDate || null,
    eventEndDate: o.eventEndDate || null,
    eventTiming: o.eventTiming || null,
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
    _listDto: true,
    schemaVersion: GDI_OPPORTUNITY_LIST_SCHEMA,
  };
}

export function mapOpportunitiesToListDto(opportunities) {
  return (opportunities || []).map(toOpportunityListDto);
}
