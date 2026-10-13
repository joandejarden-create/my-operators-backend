export {
  GDI_BASE_OF_DEMAND,
  BASE_OF_DEMAND_LIST,
  COVERAGE_STATE,
  OPPORTUNITY_MATURITY,
  BASE_LABELS,
} from "./taxonomy.js";
export { getHotelBaseWeighting, getTenBasesPilotHotels } from "./hotel-weighting.js";
export { buildGdiBaseOfDemandCoverage } from "./coverage.js";
export { assignOpportunityMaturity } from "./maturity.js";
export { scoreChildAccount, selectHighValueChildLeads } from "./child-account-gate.js";
export { buildBaseQueries } from "./base-queries.js";
export {
  decomposePublishedEventDemand,
  decomposeIntlOrgMeeting,
  mineEventParticipants,
  buildRotationOpportunityIntelligence,
  buildRecurringCorporateMeetingPattern,
  buildCorporateTriggerMeetingThesis,
  expandMedicalDemandEcosystem,
  decomposeProjectWorkforceDemand,
  decomposeSportsEntertainmentDemand,
} from "./decomposers.js";
export {
  buildHotelHistoricalDemandPattern,
  buildCompHistoricalDemandPattern,
  findGdiLookalikeAccounts,
} from "./hotel-history-lookalike.js";
export { buildHotelOpportunityThesis } from "./thesis.js";
export { buildApifyTenBasesInventory, enrichCompHotelsViaTripadvisor } from "./apify-base-map.js";
export {
  runTenBasesForHotel,
  COMP_SET_TARGET_HOTELS,
} from "./orchestrator.js";
