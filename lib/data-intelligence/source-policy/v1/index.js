/**
 * Dealality source-policy v1 — reusable gates for hotel property facts.
 *
 * Cvent venue/hotel pages = DISCOVERY_ONLY (not canonical SoT).
 * Cvent event-platform evidence remains allowed under GDI event rules.
 */

export {
  SOURCE_POLICY_VERSION,
  SourceRole,
  SourceContentDomain,
  VerificationStatus,
  CVENT_VENUE_BLOCKED_FIELDS,
} from "./source-roles.js";

export {
  classifySourceContentDomain,
  defaultRoleForDomain,
  normalizeObservation,
  isCventVenueHotelSource,
  isCventEventPlatformSource,
} from "./domain-classifier.js";

export {
  canPersistAsCanonical,
  canUseForScoring,
  canDisplayToCustomer,
  requiresIndependentVerification,
  resolveMixedProvenance,
  createDiscoveryResearchCandidate,
  evaluateSourceGates,
} from "./gates.js";

export {
  CVENT_VENUE_CENSUS_BLOCKED_WRITE_FIELDS,
  filterCventVenueCensusPatch,
  appendCventDiscoveryStewardNote,
} from "./census-cvent-write-filter.js";
