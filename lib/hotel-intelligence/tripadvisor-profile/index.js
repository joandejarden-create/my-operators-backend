/**
 * Tripadvisor hotel profile intelligence — public exports.
 */

export {
  TRIPADVISOR_PROFILE_INTEL_VERSION,
  competitiveRankPercentile,
  ratingHistogramShares,
  normalizeAmenitySet,
  amenityGapsVsComps,
  median,
  buildOwnerCompSnapshot,
} from "./metrics.js";

export {
  TRIPADVISOR_CENSUS_PROFILE_PACK_VERSION,
  TA_FIELD_MAP,
  WRITE_TIER,
  TIER_A_CENSUS_FIELDS,
  TIER_B_CENSUS_FIELDS,
  COMPLETENESS_PRIORITY_FIELDS,
  CALA_COUNTRIES,
} from "./census-map.js";

export {
  buildTripadvisorProfilePack,
  profilePackCoverageFlags,
} from "./profile-pack.js";

export {
  proposeCensusWrites,
  evaluateProductionWriteGate,
} from "./write-policy.js";
