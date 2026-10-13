/**
 * Demand Generator V1 — Airtable field maps + select value lists.
 */

import {
  DEMAND_GENERATORS_TABLE_NAME,
  DEMAND_PROGRAMS_TABLE_NAME,
  HOTEL_DEMAND_GENERATOR_FIT_TABLE_NAME,
  DEMAND_GENERATOR_SIGNALS_TABLE_NAME,
  ORGANIZATION_TYPE,
  GENERATOR_STATUS,
  PROGRAM_TYPE,
  RECURRENCE_STATUS,
  GENERATOR_PRIORITY,
  GENERATOR_RELEVANCE_CLASS,
  SIGNAL_STATUS,
  TRIGGER_TYPE,
  SOURCE_AUTHORITY,
  DG_SCHEMA_VERSION,
} from "./constants.js";

export {
  DEMAND_GENERATORS_TABLE_NAME,
  DEMAND_PROGRAMS_TABLE_NAME,
  HOTEL_DEMAND_GENERATOR_FIT_TABLE_NAME,
  DEMAND_GENERATOR_SIGNALS_TABLE_NAME,
  DG_SCHEMA_VERSION,
};

export const MAP_DEMAND_GENERATOR = Object.freeze({
  demandGeneratorId: "Demand Generator ID",
  organizationName: "Organization Name",
  organizationAliases: "Organization Aliases",
  organizationType: "Organization Type",
  officialDomain: "Official Domain",
  website: "Website",
  headquartersLocation: "Headquarters Location",
  primaryMarkets: "Primary Markets",
  industry: "Industry / Sector",
  generatorStatus: "Generator Status",
  sourceUrls: "Source URLs",
  sourceAuthority: "Source Authority",
  firstSeenAt: "First Seen",
  lastSeenAt: "Last Seen",
  lastVerifiedAt: "Last Verified",
  confidence: "Confidence",
  nextResearchAt: "Next Research At",
  researchReason: "Research Reason",
  lastResearchResult: "Last Research Result",
  lastMaterialSignalAt: "Last Material Signal At",
  schemaVersion: "Schema Version",
  payloadJson: "Generator Payload JSON",
});

export const MAP_DEMAND_PROGRAM = Object.freeze({
  programId: "Program ID",
  seriesId: "Series ID",
  demandGeneratorId: "Demand Generator ID",
  generatorLink: "Demand Generator",
  programName: "Program Name",
  programAliases: "Program Aliases",
  programType: "Program Type",
  market: "Market / Geography",
  typicalTiming: "Typical Timing",
  recurrenceStatus: "Recurrence Status",
  recurrenceFrequency: "Recurrence Frequency",
  historicalCyclesJson: "Historical Cycles JSON",
  confirmedFutureCycleJson: "Confirmed Future Cycle JSON",
  sourceUrls: "Source URLs",
  firstSeenAt: "First Seen",
  lastSeenAt: "Last Seen",
  lastVerifiedAt: "Last Verified",
  schemaVersion: "Schema Version",
  payloadJson: "Program Payload JSON",
});

export const MAP_HOTEL_GENERATOR_FIT = Object.freeze({
  fitId: "Hotel Generator Fit ID",
  hotelId: "Hotel ID",
  hotelName: "Hotel Name",
  demandGeneratorId: "Demand Generator ID",
  generatorLink: "Demand Generator",
  organizationName: "Organization Name",
  marketRelevance: "Market Relevance",
  productFit: "Product Fit",
  travelDemandPotential: "Travel Demand Potential",
  recurrencePotential: "Recurrence Potential",
  buyerAccessibility: "Buyer Accessibility",
  generatorPriority: "Generator Priority",
  relevanceClass: "Relevance Class",
  fitRationale: "Fit Rationale",
  factorsJson: "Factors JSON",
  lastEvaluatedAt: "Last Evaluated",
  firstEvaluatedAt: "First Evaluated",
  schemaVersion: "Schema Version",
  payloadJson: "Fit Payload JSON",
});

export const MAP_DEMAND_GENERATOR_SIGNAL = Object.freeze({
  signalId: "Demand Generator Signal ID",
  demandGeneratorId: "Demand Generator ID",
  generatorLink: "Demand Generator",
  programId: "Program ID",
  programLink: "Demand Program",
  seriesId: "Series ID",
  cycleId: "Cycle ID",
  hotelId: "Hotel ID",
  hotelGeneratorFitId: "Hotel Generator Fit ID",
  fitLink: "Hotel Demand Generator Fit",
  triggerType: "Trigger Type",
  title: "Title",
  eventStartDate: "Event Start Date",
  eventEndDate: "Event End Date",
  market: "Market",
  hotelDemandThesis: "Hotel Demand Thesis",
  sourceUrl: "Source URL",
  sourceUrls: "Source URLs",
  signalStatus: "Signal Status",
  recommendedAction: "Recommended Action",
  gdiOpportunityId: "GDI Opportunity ID",
  gdiOpportunityLink: "GDI Opportunity",
  failReasonsJson: "Fail Reasons JSON",
  firstSeenAt: "First Seen",
  lastSeenAt: "Last Seen",
  schemaVersion: "Schema Version",
  payloadJson: "Signal Payload JSON",
});

/** Link fields to add on Group Demand Opportunities */
export const MAP_GDI_DG_LINK = Object.freeze({
  dgGeneratorId: "dgGeneratorId",
  dgProgramId: "dgProgramId",
  dgSignalId: "dgSignalId",
  hotelGeneratorFitId: "hotelGeneratorFitId",
  dgGeneratorLink: "Demand Generator",
  dgProgramLink: "Demand Program",
  dgSignalLink: "Demand Generator Signal",
});

export const VAL_ORGANIZATION_TYPE = Object.values(ORGANIZATION_TYPE);
export const VAL_GENERATOR_STATUS = Object.values(GENERATOR_STATUS);
export const VAL_PROGRAM_TYPE = Object.values(PROGRAM_TYPE);
export const VAL_RECURRENCE_STATUS = Object.values(RECURRENCE_STATUS);
export const VAL_GENERATOR_PRIORITY = Object.values(GENERATOR_PRIORITY);
export const VAL_RELEVANCE_CLASS = Object.values(GENERATOR_RELEVANCE_CLASS);
export const VAL_SIGNAL_STATUS = Object.values(SIGNAL_STATUS);
export const VAL_TRIGGER_TYPE = Object.values(TRIGGER_TYPE);
export const VAL_SOURCE_AUTHORITY = Object.values(SOURCE_AUTHORITY);
export const VAL_FIT_BAND = ["HIGH", "MEDIUM", "LOW", "CORE", "COMPETITIVE", "STRETCH", "OUTSIDE", "UNKNOWN"];
