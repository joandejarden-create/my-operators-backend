/**
 * Eight segment-specific Opportunity Overview (Section 1) schemas.
 * Shared layout system — identity line + seven-field grid (except Scaling narrative).
 * Presentation contracts only; never invent dates, volumes, or procurement access.
 */

export const SEGMENT_OVERVIEW_SCHEMAS_V1 = "gdi-segment-overview-schemas-v1";

/** Shared field shape for structured Section 1 grids. */
export function fieldDef(key, label) {
  return Object.freeze({ key, label });
}

export const SEGMENT_OVERVIEW_SCHEMA = Object.freeze({
  associations_conferences_events: Object.freeze({
    segmentId: "associations_conferences_events",
    overviewMode: "events_structured_grid_v1",
    identityTemplate: "{organizer} — {eventName}",
    fields: Object.freeze([
      fieldDef("timing", "Timing"),
      fieldDef("location", "Location"),
      fieldDef("hotelSourcing", "Hotel sourcing"),
      fieldDef("roomDemand", "Room demand"),
      fieldDef("demandCycle", "Demand cycle"),
      fieldDef("attendance", "Attendance"),
      fieldDef("peakRooms", "Peak rooms"),
    ]),
    hotelSourcingStatuses: Object.freeze([
      "Hotel Sourcing Open",
      "Hotel Sourcing Unconfirmed",
      "Venue Confirmed",
      "Historical Venue Only",
    ]),
    preservesExistingImplementation: true,
  }),
  corporate_accounts_scaling: Object.freeze({
    segmentId: "corporate_accounts_scaling",
    overviewMode: "scaling_narrative_overview_v1",
    identityTemplate: "{companyName}",
    fields: Object.freeze([
      fieldDef("classification", "Corporate Account Relationship"),
      fieldDef("activityOverview", "Commercial overview"),
      fieldDef("demandStage", "Demand stage"),
      fieldDef("timing", "Timing"),
    ]),
    structuredGrid: false,
    preservesExistingImplementation: true,
  }),
  corporate_meetings_incentives: Object.freeze({
    segmentId: "corporate_meetings_incentives",
    overviewMode: "meetings_incentives_overview_v1",
    identityTemplate: "{organizingCompany} — {programmeName}",
    fields: Object.freeze([
      fieldDef("programmeTiming", "Programme Timing"),
      fieldDef("destination", "Destination"),
      fieldDef("hotelResortSourcing", "Hotel / Resort Sourcing"),
      fieldDef("groupParticipantDemand", "Group / Participant Demand"),
      fieldDef("demandCycle", "Demand Cycle"),
      fieldDef("programmeType", "Programme Type"),
      fieldDef("guestroomDemand", "Guestroom Demand"),
    ]),
  }),
  government_public_sector: Object.freeze({
    segmentId: "government_public_sector",
    overviewMode: "government_overview_v1",
    identityTemplate: "{agency} — {programme}",
    fields: Object.freeze([
      fieldDef("timing", "Timing"),
      fieldDef("location", "Location"),
      fieldDef("procurementStatus", "Procurement Status"),
      fieldDef("travelLodgingDemand", "Travel / Lodging Demand"),
      fieldDef("programmeCycle", "Programme Cycle"),
      fieldDef("meetingRequirements", "Meeting Requirements"),
      fieldDef("contractingPathway", "Contracting Pathway"),
    ]),
  }),
  sports_entertainment: Object.freeze({
    segmentId: "sports_entertainment",
    overviewMode: "sports_entertainment_overview_v1",
    identityTemplate: "{organizer} — {eventOrTour}",
    fields: Object.freeze([
      fieldDef("eventTourTiming", "Event / Tour Timing"),
      fieldDef("destination", "Destination"),
      fieldDef("lodgingSourcing", "Lodging Sourcing"),
      fieldDef("teamCrewDemand", "Team / Crew Demand"),
      fieldDef("eventCycle", "Event Cycle"),
      fieldDef("accommodationRequirements", "Accommodation Requirements"),
      fieldDef("estimatedDuration", "Estimated Duration"),
    ]),
  }),
  education_scientific: Object.freeze({
    segmentId: "education_scientific",
    overviewMode: "education_scientific_overview_v1",
    identityTemplate: "{institution} — {programme}",
    fields: Object.freeze([
      fieldDef("programmeTiming", "Programme Timing"),
      fieldDef("destination", "Destination"),
      fieldDef("accommodationSourcing", "Accommodation Sourcing"),
      fieldDef("visitingGroupDemand", "Visiting Group Demand"),
      fieldDef("programmeCycle", "Programme Cycle"),
      fieldDef("institutionProgrammeType", "Institution / Programme Type"),
      fieldDef("lengthOfStay", "Length of Stay"),
    ]),
  }),
  projects_infrastructure: Object.freeze({
    segmentId: "projects_infrastructure",
    overviewMode: "projects_infrastructure_overview_v1",
    identityTemplate: "{sponsor} — {projectName}",
    fields: Object.freeze([
      fieldDef("projectTiming", "Project Timing"),
      fieldDef("projectLocation", "Project Location"),
      fieldDef("accommodationSourcing", "Accommodation Sourcing"),
      fieldDef("visitingTeamDemand", "Visiting Team Demand"),
      fieldDef("projectPhase", "Project Phase"),
      fieldDef("expectedStayPattern", "Expected Stay Pattern"),
      fieldDef("contractingTravelPathway", "Contracting / Travel Pathway"),
    ]),
  }),
  social_special_events: Object.freeze({
    segmentId: "social_special_events",
    overviewMode: "social_special_events_overview_v1",
    identityTemplate: "{organizer} — {eventProgramme}",
    fields: Object.freeze([
      fieldDef("eventTiming", "Event Timing"),
      fieldDef("destination", "Destination"),
      fieldDef("venueHotelSourcing", "Venue / Hotel Sourcing"),
      fieldDef("groupAccommodationDemand", "Group Accommodation Demand"),
      fieldDef("eventCycle", "Event Cycle"),
      fieldDef("eventType", "Event Type"),
      fieldDef("functionCateringDemand", "Function / Catering Demand"),
    ]),
  }),
  local_catering_day_meetings: Object.freeze({
    segmentId: "local_catering_day_meetings",
    overviewMode: "local_catering_day_meetings_overview_v1",
    identityTemplate: "{organization} — {meetingProgramme}",
    fields: Object.freeze([
      fieldDef("timing", "Timing"),
      fieldDef("location", "Location"),
      fieldDef("venueSourcing", "Venue Sourcing"),
      fieldDef("meetingCateringDemand", "Meeting / Catering Demand"),
      fieldDef("demandCycle", "Demand Cycle"),
      fieldDef("attendees", "Attendees"),
      fieldDef("guestroomDemand", "Guestroom Demand"),
    ]),
  }),
  corporate_celebrations_private_events: Object.freeze({
    segmentId: "corporate_celebrations_private_events",
    overviewMode: "corporate_celebrations_overview_v1",
    identityTemplate: "{company} — {celebrationEvent}",
    fields: Object.freeze([
      fieldDef("eventTiming", "Event Timing"),
      fieldDef("destination", "Destination"),
      fieldDef("venueSourcing", "Venue Sourcing"),
      fieldDef("banquetCateringDemand", "Banquet / Catering Demand"),
      fieldDef("eventCycle", "Event Cycle"),
      fieldDef("attendance", "Attendance"),
      fieldDef("guestroomDemand", "Guestroom Demand"),
    ]),
  }),
  travel_trade_group_intermediaries: Object.freeze({
    segmentId: "travel_trade_group_intermediaries",
    overviewMode: "travel_trade_intermediaries_overview_v1",
    identityTemplate: "{intermediary} — {programmeOrAccount}",
    fields: Object.freeze([
      fieldDef("programmeTiming", "Programme Timing"),
      fieldDef("destination", "Destination"),
      fieldDef("hotelSourcing", "Hotel Sourcing"),
      fieldDef("groupDemand", "Group Demand"),
      fieldDef("demandCycle", "Demand Cycle"),
      fieldDef("groupType", "Group Type"),
      fieldDef("accommodationRequirements", "Accommodation Requirements"),
    ]),
  }),
  relocation_extended_stay: Object.freeze({
    segmentId: "relocation_extended_stay",
    overviewMode: "relocation_extended_stay_overview_v1",
    identityTemplate: "{company} — {assignmentProgramme}",
    fields: Object.freeze([
      fieldDef("assignmentTiming", "Assignment Timing"),
      fieldDef("destination", "Destination"),
      fieldDef("accommodationSourcing", "Accommodation Sourcing"),
      fieldDef("stayDemand", "Stay Demand"),
      fieldDef("assignmentCycle", "Assignment Cycle"),
      fieldDef("expectedStayPattern", "Expected Stay Pattern"),
      fieldDef("mobilityTravelPathway", "Mobility / Travel Pathway"),
    ]),
  }),
});

export function getSegmentOverviewSchema(segmentId) {
  return SEGMENT_OVERVIEW_SCHEMA[segmentId] || null;
}

/**
 * Build a conservative overview model from opportunity fields.
 * Never invents values — blanks become honest TBD / Not yet verified.
 */
export function buildSegmentOverviewModel(segmentId, opportunity = {}, opts = {}) {
  const schema = getSegmentOverviewSchema(segmentId);
  if (!schema) return null;

  const text = (v) =>
    String(v || "")
      .replace(/\s+/g, " ")
      .trim();
  const TBD = opts.blankLabel || "Not yet verified";

  if (schema.overviewMode === "scaling_narrative_overview_v1") {
    return {
      version: SEGMENT_OVERVIEW_SCHEMAS_V1,
      segmentId,
      overviewMode: schema.overviewMode,
      structuredGrid: false,
      companyName:
        text(opportunity.displayCompanyName || opportunity.companyName || opportunity.title) ||
        TBD,
      classification: "Corporate Account Relationship",
      activityOverview:
        text(
          opportunity.tileCustomerSummary ||
            opportunity.commercialIntelligenceBrief?.opportunityOverview ||
            opportunity.summaryWhat,
        ) || TBD,
      demandStage: text(opportunity.demandStage || opportunity.stageLabel) || TBD,
      timing: text(opportunity.timingLabel || opportunity.dateLabel) || "Dates TBD",
    };
  }

  if (schema.overviewMode === "events_structured_grid_v1") {
    // Prefer existing certified Events overview when present
    const existing =
      opportunity.eventsOpportunityOverview ||
      opportunity.commercialIntelligenceBrief?.eventsOpportunityOverview;
    if (existing && typeof existing === "object") {
      return {
        version: SEGMENT_OVERVIEW_SCHEMAS_V1,
        segmentId,
        overviewMode: schema.overviewMode,
        structuredGrid: true,
        ...existing,
        fields: schema.fields,
      };
    }
  }

  const org =
    text(
      opportunity.displayCompanyName ||
        opportunity.organizationName ||
        opportunity.companyName ||
        opportunity.agencyName ||
        opportunity.institutionName ||
        opportunity.sponsorName,
    ) || "Organization TBD";
  const programme =
    text(
      opportunity.eventDisplayName ||
        opportunity.eventName ||
        opportunity.programmeName ||
        opportunity.projectName ||
        opportunity.title,
    ) || "Programme TBD";
  let identity =
    org.toLowerCase() === programme.toLowerCase()
      ? org
      : `${org} — ${programme}`;
  // Avoid "Org — Org — Programme" when programme already embeds the org name.
  if (
    programme.toLowerCase().startsWith(org.toLowerCase()) &&
    programme.length > org.length
  ) {
    identity = programme;
  }

  const values = {};
  for (const f of schema.fields) {
    const fromOpp =
      opportunity.segmentOverview?.[f.key] ||
      opportunity[f.key] ||
      opportunity.commercialIntelligenceBrief?.segmentOverview?.[f.key];
    values[f.key] = text(fromOpp) || TBD;
  }

  return {
    version: SEGMENT_OVERVIEW_SCHEMAS_V1,
    segmentId,
    overviewMode: schema.overviewMode,
    structuredGrid: true,
    identityLine: identity,
    fields: schema.fields,
    values,
    ...values,
  };
}
