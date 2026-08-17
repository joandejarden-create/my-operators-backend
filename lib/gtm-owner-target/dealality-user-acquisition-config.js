/**
 * Dealality customer acquisition target list — segments, stages, and scoring config.
 * Separate from CoStar owner strike list: tracks potential platform users (owners,
 * developers, capital advisors, conference warm leads).
 */

/** @typedef {"identified"|"researched"|"outreach_ready"|"contacted"|"pilot"|"customer"|"paused"} AcquisitionStage */

/** @typedef {"owner_operator"|"asset_owner"|"developer"|"boutique_owner"|"capital_advisor"|"regional_operator"|"conference_warm"|"linkedin_warm"|"referral"} AcquisitionSegment */

export const MAP_DEALALITY_USER_ACQUISITION = {
  /** Priority for founder outreach (P1 highest). */
  acquisitionPriority: "acquisitionPriority",
  acquisitionSegment: "acquisitionSegment",
  acquisitionStage: "acquisitionStage",
  acquisitionScore: "acquisitionScore",
  alisCala2026Attendee: "alisCala2026Attendee",
  strikeListMember: "strikeListMember",
  outreachReady: "outreachReady",
  sourceTrack: "sourceTrack",
  pitchAngle: "pitchAngle",
};

/** Single source of truth for segment labels in reports. */
export const VAL_ACQUISITION_SEGMENT = {
  ownerOperator: "owner_operator",
  assetOwner: "asset_owner",
  developer: "developer",
  boutiqueOwner: "boutique_owner",
  capitalAdvisor: "capital_advisor",
  regionalOperator: "regional_operator",
  conferenceWarm: "conference_warm",
  linkedinWarm: "linkedin_warm",
  referral: "referral",
};

export const VAL_ACQUISITION_STAGE = {
  identified: "identified",
  researched: "researched",
  outreachReady: "outreach_ready",
  contacted: "contacted",
  pilot: "pilot",
  customer: "customer",
  paused: "paused",
};

/** Weighted signals for acquisition score (0–100). */
export const MAP_ACQUISITION_SCORE_WEIGHTS = {
  strikeList: 25,
  outreachReady: 20,
  verifiedContactV1R: 15,
  verifiedContactV2: 10,
  alisCala2026Attendee: 12,
  brandingIntentHigh: 15,
  brandingIntentMedium: 8,
  calaPortfolio3Plus: 10,
  developerPipeline: 8,
  linkedinPilotTierA: 10,
  capitalAdvisorCalaFocus: 5,
};

/** Minimum score to appear in P1 acquisition slice. */
export const MAP_ACQUISITION_PRIORITY_THRESHOLDS = {
  P1: 55,
  P2: 35,
  P3: 20,
};
