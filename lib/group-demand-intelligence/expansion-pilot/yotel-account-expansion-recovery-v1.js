/**
 * YOTEL Geneva Lake — Account Expansion Recovery Pass V1
 *
 * Expands Future Watch / Ready demand signals into evidence-backed named accounts.
 * Dry-run / report corpus only — does NOT apply to the live bag unless a separate
 * apply script is explicitly run after founder review.
 *
 * Ready / ACTIONABLE thresholds unchanged. No organizer/venue shell restoration.
 */

import { ACCOUNT_QUALITY_CLASS } from "../account-quality-taxonomy-v1.js";
import { BOOKING_WINDOW } from "../claim-types.js";
import {
  GDI_EVIDENCE_TYPE,
  GDI_MATURITY_CLAIM_KIND,
} from "../gdi-evidence-taxonomy-v1.js";
import {
  assignGdiMaturityState,
  GDI_MATURITY_STATE,
  LODGING_CONTROL_HYPOTHESIS,
  TRAVELING_COHORT_TYPE,
} from "../gdi-maturity-v1.js";
import { isGdiCustomerOpportunityReady } from "../customer-readiness-gate-v1.js";
import { classifyBuyerContactPath } from "../buyer-contact-path-taxonomy-v1.js";

export const YOTEL_HOTEL_ID = "recrPQcZg7SFARRb2";
export const YOTEL_EXPANSION_PASS_ID = "yotel_account_expansion_recovery_v1";

export const YOTEL_DEMAND_SIGNALS = Object.freeze([
  {
    id: "yotel_signal_setac_europe_37_2027",
    label: "SETAC Europe 37th Annual Meeting",
    eventStartDate: "2027-04-25",
    eventEndDate: "2027-04-29",
    venue: "Palexpo Congress Centre, Geneva",
    officialSource:
      "https://www.setac.org/discover-events/global-meetings/setac-europe-37th-annual-meeting.html",
  },
  {
    id: "yotel_signal_watches_wonders_2027",
    label: "Watches and Wonders Geneva 2027",
    eventStartDate: "2027-04-05",
    eventEndDate: "2027-04-11",
    venue: "Palexpo, Geneva",
    officialSource:
      "https://www.watchesandwonders.com/content/dam/waw/en/media-center/mc-geneva-2027/2026-09-17/pdf/WWG27_Press%20Release_17.09.26_ENG.pdf",
  },
  {
    id: "yotel_signal_geneva_health_forum_2026",
    label: "Geneva Health Forum 2026",
    eventStartDate: "2026-11-10",
    eventEndDate: "2026-11-12",
    venue: "Campus Biotech, Geneva",
    officialSource: "https://conference2026.genevahealthforum.com/about-the-conference/",
  },
  {
    id: "yotel_signal_ecosoc_ocha",
    label: "ECOSOC / OCHA Humanitarian Affairs Segment",
    eventStartDate: "2026-06-17",
    eventEndDate: "2026-06-19",
    venue: "UNHQ New York (NOT Geneva)",
    officialSource: "https://ecosoc.un.org/en/events/2026/humanitarian-affairs-segment",
  },
  {
    id: "yotel_signal_art_geneve_2026",
    label: "Art Genève 2026",
    eventStartDate: "2026-01-29",
    eventEndDate: "2026-02-01",
    venue: "Palexpo, Geneva",
    officialSource:
      "https://www.palexpo.ch/wp-content/uploads/2026/01/Press_Release_Art-Geneve_2026_EN_09.01.2026-1.pdf",
  },
  {
    id: "yotel_signal_aidex_geneva_2026",
    label: "AidEx Geneva 2026",
    eventStartDate: "2026-10-21",
    eventEndDate: "2026-10-22",
    venue: "Palexpo Halle 4, Geneva",
    officialSource: "https://aid-expo.com/exhibitors-2026",
  },
  {
    id: "yotel_signal_chi_geneva_2026",
    label: "CHI Geneva Centennial 2026",
    eventStartDate: "2026-12-09",
    eventEndDate: "2026-12-13",
    venue: "Palexpo, Geneva",
    officialSource:
      "https://www.chi-geneve.ch/en/Edition-2026/Edition-2026-CHI-Geneva.html",
  },
]);

function signalById(id) {
  return YOTEL_DEMAND_SIGNALS.find((s) => s.id === id) || null;
}

function slug(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 48);
}

/**
 * Researched account candidates. evidenceStatus ACCEPTED|REJECTED.
 * REJECTED rows stay in reports; never become opportunities.
 */
export const YOTEL_ACCOUNT_RESEARCH = Object.freeze([
  // ─── AIDEX (official exhibitor list) ───
  {
    accountName: "CEVA Logistics",
    parentDemandSignalId: "yotel_signal_aidex_geneva_2026",
    accountRole: "EXHIBITOR",
    evidenceStatus: "ACCEPTED",
    hqRegion: "International (global logistics)",
    evidenceItems: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
        evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_EXHIBITOR,
        sourceUrl: "https://aid-expo.com/exhibitors-2026",
        excerpt: "CEVA Logistics listed as AidEx Geneva 2026 exhibitor (stand D6/c)",
        confidence: "HIGH",
      },
    ],
    travelingCohortType: TRAVELING_COHORT_TYPE.EXHIBITOR_TEAM,
    travelingCohortSummary:
      "Humanitarian logistics / business-development booth team traveling for the 2-day AidEx Palexpo show, typically with overnight stays near the airport corridor",
    travelingCohortConfidence: "MEDIUM",
    travelingCohortEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_TRAVEL,
        sourceUrl: "https://aid-expo.com/exhibitors-2026",
        excerpt: "Confirmed international logistics exhibitor at Palexpo AidEx — booth staffing implies short-haul overnight team",
        confidence: "MEDIUM",
      },
    ],
    lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.UNKNOWN,
    lodgingControlSummary: "Corporate travel desk likely; controller not published",
    lodgingControlConfidence: "LOW",
    lodgingControlEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
        excerpt: "No published AidEx housing desk naming CEVA",
        confidence: "LOW",
      },
    ],
    modeledRoomsMin: 8,
    modeledRoomsMax: 20,
    modeledNightsMin: 2,
    modeledNightsMax: 4,
    modeledDemandBasis:
      "Modeled exhibitor booth / BD team band for compact AidEx cycle — lodging not verified",
    hotelFitScore: 84,
    hotelFitRationale: [
      "Airport / Palexpo corridor fit",
      "International logistics crew (not VIP luxury)",
      "Multi-night compact band within YOTEL capacity",
    ],
    publicContactPath: "https://aid-expo.com/exhibitors-2026",
    buyerEntity: "CEVA Logistics — events / humanitarian logistics BD",
    buyerRole: "Events / Humanitarian logistics BD",
    recommendedNextAction:
      "Identify CEVA humanitarian / events travel coordinator; ask whether AidEx booth team lodging is still open near Palexpo/airport",
    usefulnessScore: 88,
    noveltyScore: 82,
  },
  {
    accountName: "Kuehne + Nagel",
    parentDemandSignalId: "yotel_signal_aidex_geneva_2026",
    accountRole: "EXHIBITOR",
    evidenceStatus: "ACCEPTED",
    hqRegion: "International (CH HQ; global ops)",
    evidenceItems: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
        evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_EXHIBITOR,
        sourceUrl: "https://aid-expo.com/exhibitors-2026",
        excerpt: "Kuehne + Nagel A/S listed on AidEx Geneva 2026 exhibitor directory",
        confidence: "HIGH",
      },
    ],
    travelingCohortType: TRAVELING_COHORT_TYPE.EXHIBITOR_TEAM,
    travelingCohortSummary:
      "International humanitarian logistics commercial / operations booth team for AidEx; may mix local CH staff with traveling specialists",
    travelingCohortConfidence: "MEDIUM",
    travelingCohortEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_TRAVEL,
        sourceUrl: "https://aid-expo.com/exhibitors-2026",
        excerpt: "Confirmed AidEx exhibitor; traveling ops specialists plausible despite CH HQ",
        confidence: "MEDIUM",
      },
    ],
    lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.ACCOUNT_DIRECT,
    lodgingControlSummary: "Likely corporate TMC / travel desk; not verified for this event",
    lodgingControlConfidence: "LOW",
    lodgingControlEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
        excerpt: "Large logistics firms typically book via corporate travel; no event-specific desk published",
        confidence: "LOW",
      },
    ],
    modeledRoomsMin: 10,
    modeledRoomsMax: 25,
    modeledNightsMin: 2,
    modeledNightsMax: 4,
    modeledDemandBasis: "Modeled logistics exhibitor team band — lodging not verified",
    hotelFitScore: 86,
    hotelFitRationale: [
      "Palexpo / airport adjacency",
      "Corporate team economics fit YOTEL",
      "Overflow from central Geneva plausible during AidEx week",
    ],
    publicContactPath: "https://aid-expo.com/exhibitors-2026",
    buyerEntity: "Kuehne + Nagel — humanitarian / events travel",
    buyerRole: "Events / Corporate travel",
    recommendedNextAction:
      "Contact K+N humanitarian logistics marketing / events desk to confirm AidEx staffing lodging plan",
    usefulnessScore: 90,
    noveltyScore: 78,
  },
  {
    accountName: "Key Travel",
    parentDemandSignalId: "yotel_signal_aidex_geneva_2026",
    accountRole: "EXHIBITOR",
    evidenceStatus: "ACCEPTED",
    hqRegion: "International TMC (humanitarian travel)",
    isOpportunityMultiplier: true,
    multiplierReason:
      "Humanitarian-sector TMC exhibiting at AidEx — may influence lodging for multiple NGO / aid client groups if contracted",
    evidenceItems: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
        evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_EXHIBITOR,
        sourceUrl: "https://aid-expo.com/exhibitors-2026",
        excerpt: "Key Travel By Trip.Biz listed as AidEx Geneva 2026 exhibitor (stand F7/c)",
        confidence: "HIGH",
      },
    ],
    travelingCohortType: TRAVELING_COHORT_TYPE.PROJECT_TEAM,
    travelingCohortSummary:
      "TMC commercial / account-management team attending AidEx to serve humanitarian travel clients; also a potential lodging-control intermediary for other NGOs",
    travelingCohortConfidence: "MEDIUM",
    travelingCohortEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_TRAVEL,
        sourceUrl: "https://aid-expo.com/exhibitors-2026",
        excerpt: "Confirmed TMC exhibitor; booth team travel + client lodging influence hypothesized",
        confidence: "MEDIUM",
      },
    ],
    lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.TMC,
    lodgingControlSummary:
      "As a humanitarian TMC, Key Travel may control or influence lodging for client delegations — control not confirmed for specific YOTEL inventory",
    lodgingControlConfidence: "MEDIUM",
    lodgingControlEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
        sourceUrl: "https://aid-expo.com/exhibitors-2026",
        excerpt: "Exhibiting as Key Travel (TMC) implies travel-management role; do not assert YOTEL block",
        confidence: "MEDIUM",
      },
    ],
    modeledRoomsMin: 6,
    modeledRoomsMax: 15,
    modeledNightsMin: 2,
    modeledNightsMax: 4,
    modeledDemandBasis:
      "Modeled TMC booth team only — client room influence is separate and unverified",
    hotelFitScore: 88,
    hotelFitRationale: [
      "TMC multiplier for humanitarian demand",
      "Airport corridor product fit",
      "Price-sensitive NGO overflow pattern",
    ],
    publicContactPath: "https://aid-expo.com/exhibitors-2026",
    buyerEntity: "Key Travel — humanitarian account management",
    buyerRole: "Account management / Hotel programme",
    recommendedNextAction:
      "Engage Key Travel hotel programme / humanitarian accounts to ask whether AidEx-week Geneva overflow inventory is still being sourced",
    usefulnessScore: 94,
    noveltyScore: 90,
  },
  {
    accountName: "Air Charter Service",
    parentDemandSignalId: "yotel_signal_aidex_geneva_2026",
    accountRole: "EXHIBITOR",
    evidenceStatus: "ACCEPTED",
    evidenceItems: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
        evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_EXHIBITOR,
        sourceUrl: "https://aid-expo.com/exhibitors-2026",
        excerpt: "Air Charter Service listed on AidEx Geneva 2026 exhibitor directory",
        confidence: "HIGH",
      },
    ],
    travelingCohortType: TRAVELING_COHORT_TYPE.EXHIBITOR_TEAM,
    travelingCohortSummary:
      "Aviation / charter commercial team exhibiting to humanitarian buyers — typically traveling BD + ops staff for the show window",
    travelingCohortConfidence: "MEDIUM",
    travelingCohortEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_TRAVEL,
        sourceUrl: "https://aid-expo.com/exhibitors-2026",
        excerpt: "International air-charter exhibitor implies non-local booth staffing",
        confidence: "MEDIUM",
      },
    ],
    lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.ACCOUNT_DIRECT,
    lodgingControlSummary: "Corporate travel; controller unknown",
    lodgingControlConfidence: "LOW",
    lodgingControlEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
        excerpt: "No published housing controller",
        confidence: "LOW",
      },
    ],
    modeledRoomsMin: 6,
    modeledRoomsMax: 16,
    modeledNightsMin: 2,
    modeledNightsMax: 4,
    modeledDemandBasis: "Modeled exhibitor BD team — lodging not verified",
    hotelFitScore: 82,
    hotelFitRationale: ["Airport adjacency", "International commercial team", "Compact multi-night"],
    publicContactPath: "https://aid-expo.com/exhibitors-2026",
    buyerEntity: "Air Charter Service — events / humanitarian BD",
    buyerRole: "Events / BD",
    recommendedNextAction:
      "Confirm AidEx booth staffing plan and whether Geneva airport-corridor hotels are preferred",
    usefulnessScore: 80,
    noveltyScore: 76,
  },
  {
    accountName: "Maersk Logistics & Services",
    parentDemandSignalId: "yotel_signal_aidex_geneva_2026",
    accountRole: "EXHIBITOR",
    evidenceStatus: "ACCEPTED",
    evidenceItems: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
        evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_EXHIBITOR,
        sourceUrl: "https://aid-expo.com/exhibitors-2026",
        excerpt: "Maersk Logistics & Services Denmark A/S listed on AidEx Geneva 2026 exhibitor directory",
        confidence: "HIGH",
      },
    ],
    travelingCohortType: TRAVELING_COHORT_TYPE.EXHIBITOR_TEAM,
    travelingCohortSummary:
      "Humanitarian logistics commercial team from Maersk exhibiting at AidEx — international overnight booth staffing expected",
    travelingCohortConfidence: "MEDIUM",
    travelingCohortEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_TRAVEL,
        sourceUrl: "https://aid-expo.com/exhibitors-2026",
        excerpt: "Denmark-listed Maersk logistics entity exhibiting in Geneva implies travel",
        confidence: "MEDIUM",
      },
    ],
    lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.UNKNOWN,
    lodgingControlSummary: "Corporate TMC likely; not verified",
    lodgingControlConfidence: "LOW",
    lodgingControlEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
        excerpt: "Controller unknown",
        confidence: "LOW",
      },
    ],
    modeledRoomsMin: 8,
    modeledRoomsMax: 22,
    modeledNightsMin: 2,
    modeledNightsMax: 4,
    modeledDemandBasis: "Modeled logistics exhibitor band — lodging not verified",
    hotelFitScore: 85,
    hotelFitRationale: ["Palexpo corridor", "International logistics crew", "YOTEL price/positioning fit"],
    publicContactPath: "https://aid-expo.com/exhibitors-2026",
    buyerEntity: "Maersk Logistics — humanitarian events",
    buyerRole: "Events / Humanitarian logistics",
    recommendedNextAction:
      "Reach Maersk humanitarian logistics events contact for AidEx Geneva lodging timing",
    usefulnessScore: 87,
    noveltyScore: 80,
  },
  {
    accountName: "International SOS",
    parentDemandSignalId: "yotel_signal_aidex_geneva_2026",
    accountRole: "EXHIBITOR",
    evidenceStatus: "ACCEPTED",
    evidenceItems: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
        evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_EXHIBITOR,
        sourceUrl: "https://aid-expo.com/exhibitors-2026",
        excerpt: "International SOS listed on AidEx Geneva 2026 exhibitor directory",
        confidence: "HIGH",
      },
    ],
    travelingCohortType: TRAVELING_COHORT_TYPE.EXHIBITOR_TEAM,
    travelingCohortSummary:
      "Medical / security assistance commercial team staffing AidEx booth for humanitarian buyers",
    travelingCohortConfidence: "MEDIUM",
    travelingCohortEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_TRAVEL,
        sourceUrl: "https://aid-expo.com/exhibitors-2026",
        excerpt: "Confirmed exhibitor; international assistance firm booth staffing implies travel",
        confidence: "MEDIUM",
      },
    ],
    lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.ACCOUNT_DIRECT,
    lodgingControlSummary: "Corporate travel desk hypothesized",
    lodgingControlConfidence: "LOW",
    lodgingControlEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
        excerpt: "No published lodging controller for AidEx",
        confidence: "LOW",
      },
    ],
    modeledRoomsMin: 6,
    modeledRoomsMax: 18,
    modeledNightsMin: 2,
    modeledNightsMax: 4,
    modeledDemandBasis: "Modeled exhibitor team — lodging not verified",
    hotelFitScore: 80,
    hotelFitRationale: ["International commercial team", "Airport access", "Compact band"],
    publicContactPath: "https://aid-expo.com/exhibitors-2026",
    buyerEntity: "International SOS — events / humanitarian partnerships",
    buyerRole: "Events / Partnerships",
    recommendedNextAction:
      "Confirm booth staffing travel plan and preferred Geneva hotel corridor",
    usefulnessScore: 78,
    noveltyScore: 74,
  },
  {
    accountName: "MOVE ONE",
    parentDemandSignalId: "yotel_signal_aidex_geneva_2026",
    accountRole: "EXHIBITOR",
    evidenceStatus: "ACCEPTED",
    evidenceItems: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
        evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_EXHIBITOR,
        sourceUrl: "https://aid-expo.com/exhibitors-2026",
        excerpt: "MOVE ONE listed as AidEx Geneva 2026 exhibitor (stand C6/c)",
        confidence: "HIGH",
      },
    ],
    travelingCohortType: TRAVELING_COHORT_TYPE.EXHIBITOR_TEAM,
    travelingCohortSummary:
      "Relocation / logistics exhibitor team for humanitarian sector — booth staff overnight near Palexpo",
    travelingCohortConfidence: "MEDIUM",
    travelingCohortEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_TRAVEL,
        sourceUrl: "https://aid-expo.com/exhibitors-2026",
        excerpt: "Confirmed stand exhibitor implies traveling booth/ops staff",
        confidence: "MEDIUM",
      },
    ],
    lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.UNKNOWN,
    lodgingControlSummary: "Unknown controller",
    lodgingControlConfidence: "LOW",
    lodgingControlEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
        excerpt: "Controller unknown",
        confidence: "LOW",
      },
    ],
    modeledRoomsMin: 5,
    modeledRoomsMax: 14,
    modeledNightsMin: 2,
    modeledNightsMax: 4,
    modeledDemandBasis: "Modeled exhibitor team — lodging not verified",
    hotelFitScore: 81,
    hotelFitRationale: ["Palexpo proximity", "Logistics crew economics", "Multi-night"],
    publicContactPath: "https://aid-expo.com/exhibitors-2026",
    buyerEntity: "MOVE ONE — events / BD",
    buyerRole: "Events / BD",
    recommendedNextAction: "Confirm AidEx booth lodging plan near airport/Palexpo",
    usefulnessScore: 76,
    noveltyScore: 80,
  },
  {
    accountName: "Scan Global Logistics",
    parentDemandSignalId: "yotel_signal_aidex_geneva_2026",
    accountRole: "EXHIBITOR",
    evidenceStatus: "ACCEPTED",
    evidenceItems: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
        evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_EXHIBITOR,
        sourceUrl: "https://aid-expo.com/exhibitors-2026",
        excerpt: "SCAN GLOBAL LOGISTICS listed on AidEx Geneva 2026 exhibitor directory",
        confidence: "HIGH",
      },
    ],
    travelingCohortType: TRAVELING_COHORT_TYPE.EXHIBITOR_TEAM,
    travelingCohortSummary:
      "International freight / humanitarian logistics booth team traveling for AidEx",
    travelingCohortConfidence: "MEDIUM",
    travelingCohortEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_TRAVEL,
        sourceUrl: "https://aid-expo.com/exhibitors-2026",
        excerpt: "Confirmed AidEx exhibitor; international logistics staffing travel expected",
        confidence: "MEDIUM",
      },
    ],
    lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.UNKNOWN,
    lodgingControlSummary: "Unknown",
    lodgingControlConfidence: "LOW",
    lodgingControlEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
        excerpt: "Controller unknown",
        confidence: "LOW",
      },
    ],
    modeledRoomsMin: 6,
    modeledRoomsMax: 16,
    modeledNightsMin: 2,
    modeledNightsMax: 4,
    modeledDemandBasis: "Modeled exhibitor band — lodging not verified",
    hotelFitScore: 83,
    hotelFitRationale: ["Airport corridor", "Logistics crew", "Compact AidEx window"],
    publicContactPath: "https://aid-expo.com/exhibitors-2026",
    buyerEntity: "Scan Global Logistics — events",
    buyerRole: "Events",
    recommendedNextAction: "Confirm AidEx staffing lodging corridor preference",
    usefulnessScore: 79,
    noveltyScore: 77,
  },

  // ─── WATCHES & WONDERS (official 2027 brand list) — prefer support-team fit ───
  {
    accountName: "NOMOS Glashütte",
    parentDemandSignalId: "yotel_signal_watches_wonders_2027",
    accountRole: "EXHIBITOR",
    evidenceStatus: "ACCEPTED",
    hqRegion: "Germany",
    evidenceItems: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
        evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_EXHIBITOR,
        sourceUrl:
          "https://www.watchesandwonders.com/content/dam/waw/en/media-center/mc-geneva-2027/2026-09-17/pdf/WWG27_Press%20Release_17.09.26_ENG.pdf",
        excerpt: "NOMOS Glashütte listed among Watches and Wonders Geneva 2027 exhibiting brands",
        confidence: "HIGH",
      },
    ],
    travelingCohortType: TRAVELING_COHORT_TYPE.EXHIBITOR_TEAM,
    travelingCohortSummary:
      "German brand booth / retail / press-support team traveling for the multi-day salon — not assumed to be ultra-VIP executive lodging",
    travelingCohortConfidence: "MEDIUM",
    travelingCohortEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_TRAVEL,
        sourceUrl:
          "https://www.watchesandwonders.com/content/dam/waw/en/media-center/mc-geneva-2027/2026-09-17/pdf/WWG27_Press%20Release_17.09.26_ENG.pdf",
        excerpt: "Confirmed exhibiting brand HQ outside Geneva implies traveling booth/support cohort",
        confidence: "MEDIUM",
      },
    ],
    lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.UNKNOWN,
    lodgingControlSummary: "Brand events / travel desk unknown",
    lodgingControlConfidence: "LOW",
    lodgingControlEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
        excerpt: "No published housing desk for NOMOS at W&W",
        confidence: "LOW",
      },
    ],
    modeledRoomsMin: 10,
    modeledRoomsMax: 28,
    modeledNightsMin: 4,
    modeledNightsMax: 8,
    modeledDemandBasis:
      "Modeled EMEA booth/support team during W&W compression — lodging not verified; down-ranked vs ultra-luxury executive housing",
    hotelFitScore: 78,
    hotelFitRationale: [
      "Support/retail team economics more YOTEL-compatible than ultra-luxury maison executives",
      "Multi-night salon window",
      "Airport / Palexpo overflow during city compression",
    ],
    publicContactPath: "https://www.watchesandwonders.com/en",
    buyerEntity: "NOMOS Glashütte — events / brand operations",
    buyerRole: "Events / Brand operations",
    recommendedNextAction:
      "Identify NOMOS events / brand-ops travel contact for W&W Geneva booth staffing lodging",
    usefulnessScore: 84,
    noveltyScore: 86,
  },
  {
    accountName: "Sinn Spezialuhren",
    parentDemandSignalId: "yotel_signal_watches_wonders_2027",
    accountRole: "EXHIBITOR",
    evidenceStatus: "ACCEPTED",
    hqRegion: "Germany",
    evidenceItems: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
        evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_EXHIBITOR,
        sourceUrl:
          "https://www.watchesandwonders.com/content/dam/waw/en/media-center/mc-geneva-2027/2026-09-17/pdf/WWG27_Press%20Release_17.09.26_ENG.pdf",
        excerpt: "Sinn Spezialuhren listed among W&W Geneva 2027 exhibiting brands",
        confidence: "HIGH",
      },
    ],
    travelingCohortType: TRAVELING_COHORT_TYPE.EXHIBITOR_TEAM,
    travelingCohortSummary:
      "German brand booth and technical/support staff traveling for salon week",
    travelingCohortConfidence: "MEDIUM",
    travelingCohortEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_TRAVEL,
        sourceUrl:
          "https://www.watchesandwonders.com/content/dam/waw/en/media-center/mc-geneva-2027/2026-09-17/pdf/WWG27_Press%20Release_17.09.26_ENG.pdf",
        excerpt: "Confirmed exhibiting brand based outside Geneva",
        confidence: "MEDIUM",
      },
    ],
    lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.ACCOUNT_DIRECT,
    lodgingControlSummary: "Brand travel desk hypothesized",
    lodgingControlConfidence: "LOW",
    lodgingControlEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
        excerpt: "Controller unknown",
        confidence: "LOW",
      },
    ],
    modeledRoomsMin: 8,
    modeledRoomsMax: 22,
    modeledNightsMin: 4,
    modeledNightsMax: 8,
    modeledDemandBasis: "Modeled booth/support team — lodging not verified",
    hotelFitScore: 77,
    hotelFitRationale: ["Support-team ADR fit", "Multi-night", "Compression overflow"],
    publicContactPath: "https://www.watchesandwonders.com/en",
    buyerEntity: "Sinn Spezialuhren — events",
    buyerRole: "Events",
    recommendedNextAction: "Confirm W&W booth staffing lodging corridor",
    usefulnessScore: 80,
    noveltyScore: 84,
  },
  {
    accountName: "Bremont",
    parentDemandSignalId: "yotel_signal_watches_wonders_2027",
    accountRole: "EXHIBITOR",
    evidenceStatus: "ACCEPTED",
    hqRegion: "United Kingdom",
    evidenceItems: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
        evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_EXHIBITOR,
        sourceUrl:
          "https://www.watchesandwonders.com/content/dam/waw/en/media-center/mc-geneva-2027/2026-09-17/pdf/WWG27_Press%20Release_17.09.26_ENG.pdf",
        excerpt: "Bremont listed among W&W Geneva 2027 exhibiting brands",
        confidence: "HIGH",
      },
    ],
    travelingCohortType: TRAVELING_COHORT_TYPE.EXHIBITOR_TEAM,
    travelingCohortSummary:
      "UK brand booth, retail, and press-support cohort traveling for the salon",
    travelingCohortConfidence: "MEDIUM",
    travelingCohortEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_TRAVEL,
        sourceUrl:
          "https://www.watchesandwonders.com/content/dam/waw/en/media-center/mc-geneva-2027/2026-09-17/pdf/WWG27_Press%20Release_17.09.26_ENG.pdf",
        excerpt: "UK-based exhibiting brand confirmed for Geneva salon",
        confidence: "MEDIUM",
      },
    ],
    lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.UNKNOWN,
    lodgingControlSummary: "Unknown",
    lodgingControlConfidence: "LOW",
    lodgingControlEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
        excerpt: "Controller unknown",
        confidence: "LOW",
      },
    ],
    modeledRoomsMin: 10,
    modeledRoomsMax: 26,
    modeledNightsMin: 4,
    modeledNightsMax: 8,
    modeledDemandBasis: "Modeled UK booth/support team — lodging not verified",
    hotelFitScore: 79,
    hotelFitRationale: ["International support team", "Multi-night salon", "Airport overflow"],
    publicContactPath: "https://www.watchesandwonders.com/en",
    buyerEntity: "Bremont — events / brand ops",
    buyerRole: "Events / Brand ops",
    recommendedNextAction: "Confirm W&W Geneva booth team lodging sourcing",
    usefulnessScore: 82,
    noveltyScore: 85,
  },
  {
    accountName: "Grand Seiko",
    parentDemandSignalId: "yotel_signal_watches_wonders_2027",
    accountRole: "EXHIBITOR",
    evidenceStatus: "ACCEPTED",
    hqRegion: "Japan",
    evidenceItems: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
        evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_EXHIBITOR,
        sourceUrl:
          "https://www.watchesandwonders.com/content/dam/waw/en/media-center/mc-geneva-2027/2026-09-17/pdf/WWG27_Press%20Release_17.09.26_ENG.pdf",
        excerpt: "Grand Seiko listed among W&W Geneva 2027 exhibiting brands",
        confidence: "HIGH",
      },
    ],
    travelingCohortType: TRAVELING_COHORT_TYPE.EXHIBITOR_TEAM,
    travelingCohortSummary:
      "International brand support / booth / PR cohort traveling from Japan/EMEA for salon week (executive maison lodging may split elsewhere)",
    travelingCohortConfidence: "MEDIUM",
    travelingCohortEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_TRAVEL,
        sourceUrl:
          "https://www.watchesandwonders.com/content/dam/waw/en/media-center/mc-geneva-2027/2026-09-17/pdf/WWG27_Press%20Release_17.09.26_ENG.pdf",
        excerpt: "Confirmed exhibiting brand with international HQ — support travel expected",
        confidence: "MEDIUM",
      },
    ],
    lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.UNKNOWN,
    lodgingControlSummary: "Brand / agency split possible; unknown",
    lodgingControlConfidence: "LOW",
    lodgingControlEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
        excerpt: "Controller unknown; VIP execs may book centro luxury separately",
        confidence: "LOW",
      },
    ],
    modeledRoomsMin: 12,
    modeledRoomsMax: 35,
    modeledNightsMin: 4,
    modeledNightsMax: 8,
    modeledDemandBasis:
      "Modeled support/ops band only — does not claim executive VIP lodging at YOTEL",
    hotelFitScore: 72,
    hotelFitRationale: [
      "Support crew more YOTEL-fit than maison executives",
      "Compression-week overflow",
      "Down-rank ultra-VIP ADR expectations",
    ],
    publicContactPath: "https://www.watchesandwonders.com/en",
    buyerEntity: "Grand Seiko — events / brand operations",
    buyerRole: "Events / Brand operations",
    recommendedNextAction:
      "Separate support-team lodging from executive hospitality; ask ops desk about airport-corridor inventory",
    usefulnessScore: 74,
    noveltyScore: 70,
  },
  {
    accountName: "Porsche Design",
    parentDemandSignalId: "yotel_signal_watches_wonders_2027",
    accountRole: "EXHIBITOR",
    evidenceStatus: "ACCEPTED",
    hqRegion: "Germany / international",
    evidenceItems: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
        evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_EXHIBITOR,
        sourceUrl:
          "https://www.watchesandwonders.com/content/dam/waw/en/media-center/mc-geneva-2027/2026-09-17/pdf/WWG27_Press%20Release_17.09.26_ENG.pdf",
        excerpt: "Porsche Design listed as new Watches and Wonders Geneva 2027 exhibitor",
        confidence: "HIGH",
      },
    ],
    travelingCohortType: TRAVELING_COHORT_TYPE.SPONSOR_ACTIVATION,
    travelingCohortSummary:
      "Newly joining brand activation / booth / marketing support team for first W&W Geneva participation",
    travelingCohortConfidence: "MEDIUM",
    travelingCohortEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_TRAVEL,
        sourceUrl:
          "https://www.watchesandwonders.com/content/dam/waw/en/media-center/mc-geneva-2027/2026-09-17/pdf/WWG27_Press%20Release_17.09.26_ENG.pdf",
        excerpt: "Official debut brand — activation/support travel expected for first salon",
        confidence: "MEDIUM",
      },
    ],
    lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.ACTIVATION_AGENCY,
    lodgingControlSummary: "Activation agency possible for debut; not confirmed",
    lodgingControlConfidence: "LOW",
    lodgingControlEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
        excerpt: "Debut brands often use agencies; controller not published",
        confidence: "LOW",
      },
    ],
    modeledRoomsMin: 10,
    modeledRoomsMax: 30,
    modeledNightsMin: 4,
    modeledNightsMax: 8,
    modeledDemandBasis: "Modeled debut activation/support band — lodging not verified",
    hotelFitScore: 76,
    hotelFitRationale: ["Debut activation support team", "Multi-night", "Not pure ultra-VIP maison"],
    publicContactPath: "https://www.watchesandwonders.com/en",
    buyerEntity: "Porsche Design — brand activation / events",
    buyerRole: "Brand activation / Events",
    recommendedNextAction:
      "Ask brand activation / events who controls Geneva lodging for first W&W participation",
    usefulnessScore: 81,
    noveltyScore: 92,
  },

  // ─── SETAC — Maastricht 2026 exhibitors as recurring-pattern candidates for Geneva 2027 ───
  {
    accountName: "Agilent Technologies",
    parentDemandSignalId: "yotel_signal_setac_europe_37_2027",
    accountRole: "EXHIBITOR",
    evidenceStatus: "ACCEPTED",
    evidenceNote:
      "Confirmed SETAC Europe Maastricht 2026 exhibitor; Geneva 2027 exhibitor list not yet published — recurring scientific-instrument exhibitor pattern",
    evidenceItems: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
        evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_EXHIBITOR,
        sourceUrl:
          "https://www.project-planets.eu/wp-content/uploads/2026/09/Full-Maastricht-Programme-Book-1.pdf",
        excerpt: "Agilent Technologies listed as SETAC Europe 36th (Maastricht 2026) exhibitor booth 83",
        confidence: "HIGH",
      },
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_RECURRING_PATTERN,
        sourceUrl:
          "https://www.setac.org/discover-events/global-meetings/setac-europe-37th-annual-meeting/exhibitors-sponsors/become-an-exhibitor.html",
        excerpt:
          "SETAC Europe 37th Geneva 2027 exhibitor programme open; Agilent not yet named on Geneva list — treat as recurring-pattern candidate only",
        confidence: "MEDIUM",
      },
    ],
    travelingCohortType: TRAVELING_COHORT_TYPE.EXHIBITOR_TEAM,
    travelingCohortSummary:
      "Scientific instrument booth / applications specialists traveling for SETAC Europe annual meeting week",
    travelingCohortConfidence: "MEDIUM",
    travelingCohortEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_TRAVEL,
        sourceUrl:
          "https://www.project-planets.eu/wp-content/uploads/2026/09/Full-Maastricht-Programme-Book-1.pdf",
        excerpt: "Prior-year confirmed exhibitor; booth staffing travel is the commercial motion",
        confidence: "MEDIUM",
      },
    ],
    lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.UNKNOWN,
    lodgingControlSummary: "Corporate events travel unknown",
    lodgingControlConfidence: "LOW",
    lodgingControlEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
        excerpt: "Controller unknown; Geneva 2027 booth not yet published",
        confidence: "LOW",
      },
    ],
    modeledRoomsMin: 8,
    modeledRoomsMax: 20,
    modeledNightsMin: 3,
    modeledNightsMax: 6,
    modeledDemandBasis:
      "Modeled scientific exhibitor band — Geneva 2027 participation not yet list-confirmed; lodging not verified",
    hotelFitScore: 80,
    hotelFitRationale: [
      "Instrument vendor booth economics fit YOTEL",
      "Palexpo congress corridor",
      "International specialists",
    ],
    publicContactPath:
      "https://www.setac.org/discover-events/global-meetings/setac-europe-37th-annual-meeting/exhibitors-sponsors/become-an-exhibitor.html",
    buyerEntity: "Agilent Technologies — events / exhibition marketing",
    buyerRole: "Exhibition marketing / Events",
    recommendedNextAction:
      "Watch Geneva 2027 exhibitor list publication; if Agilent appears, engage exhibition marketing on Palexpo lodging",
    usefulnessScore: 72,
    noveltyScore: 68,
    forceMaxMaturity: GDI_MATURITY_STATE.CANDIDATE, // Geneva list not published
  },
  {
    accountName: "Labcorp",
    parentDemandSignalId: "yotel_signal_setac_europe_37_2027",
    accountRole: "EXHIBITOR",
    evidenceStatus: "ACCEPTED",
    evidenceItems: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
        evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_EXHIBITOR,
        sourceUrl:
          "https://www.project-planets.eu/wp-content/uploads/2026/09/Full-Maastricht-Programme-Book-1.pdf",
        excerpt: "Labcorp listed as SETAC Europe Maastricht 2026 exhibitor",
        confidence: "HIGH",
      },
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_RECURRING_PATTERN,
        sourceUrl:
          "https://www.setac.org/discover-events/global-meetings/setac-europe-37th-annual-meeting.html",
        excerpt: "Geneva 2027 cycle confirmed; Labcorp Geneva booth not yet list-confirmed",
        confidence: "MEDIUM",
      },
    ],
    travelingCohortType: TRAVELING_COHORT_TYPE.EXHIBITOR_TEAM,
    travelingCohortSummary:
      "Laboratory / testing company exhibition and scientific BD team for SETAC Europe week",
    travelingCohortConfidence: "MEDIUM",
    travelingCohortEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_TRAVEL,
        sourceUrl:
          "https://www.project-planets.eu/wp-content/uploads/2026/09/Full-Maastricht-Programme-Book-1.pdf",
        excerpt: "Prior-year confirmed exhibitor pattern",
        confidence: "MEDIUM",
      },
    ],
    lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.UNKNOWN,
    lodgingControlSummary: "Unknown",
    lodgingControlConfidence: "LOW",
    lodgingControlEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
        excerpt: "Controller unknown",
        confidence: "LOW",
      },
    ],
    modeledRoomsMin: 6,
    modeledRoomsMax: 18,
    modeledNightsMin: 3,
    modeledNightsMax: 6,
    modeledDemandBasis: "Modeled lab-services exhibitor band — Geneva list pending",
    hotelFitScore: 78,
    hotelFitRationale: ["Lab/services booth fit", "Palexpo corridor", "International BD staff"],
    publicContactPath:
      "https://www.setac.org/discover-events/global-meetings/setac-europe-37th-annual-meeting.html",
    buyerEntity: "Labcorp — exhibition / scientific marketing",
    buyerRole: "Exhibition marketing",
    recommendedNextAction: "Re-check Geneva 2027 exhibitor list when published",
    usefulnessScore: 70,
    noveltyScore: 66,
    forceMaxMaturity: GDI_MATURITY_STATE.CANDIDATE,
  },
  {
    accountName: "Smithers",
    parentDemandSignalId: "yotel_signal_setac_europe_37_2027",
    accountRole: "EXHIBITOR",
    evidenceStatus: "ACCEPTED",
    evidenceItems: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
        evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_EXHIBITOR,
        sourceUrl:
          "https://www.project-planets.eu/wp-content/uploads/2026/09/Full-Maastricht-Programme-Book-1.pdf",
        excerpt: "Smithers listed as SETAC Europe Maastricht 2026 exhibitor",
        confidence: "HIGH",
      },
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_RECURRING_PATTERN,
        sourceUrl:
          "https://www.setac.org/discover-events/global-meetings/setac-europe-37th-annual-meeting.html",
        excerpt: "Geneva 2027 cycle; Smithers not yet list-confirmed for Geneva",
        confidence: "MEDIUM",
      },
    ],
    travelingCohortType: TRAVELING_COHORT_TYPE.EXHIBITOR_TEAM,
    travelingCohortSummary:
      "Testing / consultancy booth team traveling for SETAC Europe annual meeting",
    travelingCohortConfidence: "MEDIUM",
    travelingCohortEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_TRAVEL,
        sourceUrl:
          "https://www.project-planets.eu/wp-content/uploads/2026/09/Full-Maastricht-Programme-Book-1.pdf",
        excerpt: "Prior-year confirmed exhibitor",
        confidence: "MEDIUM",
      },
    ],
    lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.UNKNOWN,
    lodgingControlSummary: "Unknown",
    lodgingControlConfidence: "LOW",
    lodgingControlEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
        excerpt: "Controller unknown",
        confidence: "LOW",
      },
    ],
    modeledRoomsMin: 5,
    modeledRoomsMax: 14,
    modeledNightsMin: 3,
    modeledNightsMax: 5,
    modeledDemandBasis: "Modeled exhibitor band — Geneva list pending",
    hotelFitScore: 76,
    hotelFitRationale: ["Testing vendor economics", "Congress week", "International staff"],
    publicContactPath:
      "https://www.setac.org/discover-events/global-meetings/setac-europe-37th-annual-meeting.html",
    buyerEntity: "Smithers — events / exhibition",
    buyerRole: "Events",
    recommendedNextAction: "Confirm when Geneva 2027 exhibitor list publishes",
    usefulnessScore: 68,
    noveltyScore: 65,
    forceMaxMaturity: GDI_MATURITY_STATE.CANDIDATE,
  },

  // ─── ART GENÈVE — international galleries only ───
  {
    accountName: "Tang Contemporary Art",
    parentDemandSignalId: "yotel_signal_art_geneve_2026",
    accountRole: "EXHIBITOR",
    evidenceStatus: "ACCEPTED",
    hqRegion: "Hong Kong",
    evidenceItems: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
        evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_EXHIBITOR,
        sourceUrl:
          "https://www.palexpo.ch/wp-content/uploads/2026/01/Press_Release_Art-Geneve_2026_EN_09.01.2026-1.pdf",
        excerpt: "Tang Contemporary Art, Hong Kong listed among Art Genève 2026 galleries",
        confidence: "HIGH",
      },
    ],
    travelingCohortType: TRAVELING_COHORT_TYPE.EXHIBITOR_TEAM,
    travelingCohortSummary:
      "International gallery install / sales / registrar team traveling for fair build + selling days (5–15 person working group, not collector VIP housing)",
    travelingCohortConfidence: "MEDIUM",
    travelingCohortEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_TRAVEL,
        sourceUrl:
          "https://www.palexpo.ch/wp-content/uploads/2026/01/Press_Release_Art-Geneve_2026_EN_09.01.2026-1.pdf",
        excerpt: "Hong Kong gallery confirmed at Palexpo fair implies traveling install/sales cohort",
        confidence: "MEDIUM",
      },
    ],
    lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.ACCOUNT_DIRECT,
    lodgingControlSummary: "Gallery travel coordinator hypothesized",
    lodgingControlConfidence: "LOW",
    lodgingControlEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
        excerpt: "Controller unknown",
        confidence: "LOW",
      },
    ],
    modeledRoomsMin: 5,
    modeledRoomsMax: 14,
    modeledNightsMin: 4,
    modeledNightsMax: 7,
    modeledDemandBasis: "Modeled gallery working team — lodging not verified",
    hotelFitScore: 74,
    hotelFitRationale: [
      "International working team (not local Geneva gallery)",
      "Install + fair multi-night",
      "Down-rank ultra-collector luxury lodging",
    ],
    publicContactPath: "https://artgeneve.ch/en/home/",
    buyerEntity: "Tang Contemporary Art — fair operations / travel",
    buyerRole: "Fair operations",
    recommendedNextAction:
      "Contact gallery fair ops for Art Genève install/sales team lodging near Palexpo",
    usefulnessScore: 75,
    noveltyScore: 88,
  },
  {
    accountName: "Lee & Bae",
    parentDemandSignalId: "yotel_signal_art_geneve_2026",
    accountRole: "EXHIBITOR",
    evidenceStatus: "ACCEPTED",
    hqRegion: "Busan, Korea",
    evidenceItems: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
        evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_EXHIBITOR,
        sourceUrl:
          "https://www.palexpo.ch/wp-content/uploads/2026/01/Press_Release_Art-Geneve_2026_EN_09.01.2026-1.pdf",
        excerpt: "Lee & Bae, Busan listed among Art Genève 2026 galleries",
        confidence: "HIGH",
      },
    ],
    travelingCohortType: TRAVELING_COHORT_TYPE.EXHIBITOR_TEAM,
    travelingCohortSummary:
      "Korean gallery install and sales team traveling for Art Genève fair week",
    travelingCohortConfidence: "MEDIUM",
    travelingCohortEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_TRAVEL,
        sourceUrl:
          "https://www.palexpo.ch/wp-content/uploads/2026/01/Press_Release_Art-Geneve_2026_EN_09.01.2026-1.pdf",
        excerpt: "Busan gallery at Geneva fair implies international travel cohort",
        confidence: "MEDIUM",
      },
    ],
    lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.UNKNOWN,
    lodgingControlSummary: "Unknown",
    lodgingControlConfidence: "LOW",
    lodgingControlEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
        excerpt: "Controller unknown",
        confidence: "LOW",
      },
    ],
    modeledRoomsMin: 4,
    modeledRoomsMax: 12,
    modeledNightsMin: 4,
    modeledNightsMax: 7,
    modeledDemandBasis: "Modeled gallery working team — lodging not verified",
    hotelFitScore: 73,
    hotelFitRationale: ["International gallery team", "Multi-night install+fair", "Palexpo access"],
    publicContactPath: "https://artgeneve.ch/en/home/",
    buyerEntity: "Lee & Bae — fair operations",
    buyerRole: "Fair operations",
    recommendedNextAction: "Confirm Art Genève team lodging corridor",
    usefulnessScore: 72,
    noveltyScore: 90,
  },
  {
    accountName: "Seventeen",
    parentDemandSignalId: "yotel_signal_art_geneve_2026",
    accountRole: "EXHIBITOR",
    evidenceStatus: "ACCEPTED",
    hqRegion: "London",
    evidenceItems: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
        evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_EXHIBITOR,
        sourceUrl:
          "https://www.palexpo.ch/wp-content/uploads/2026/01/Press_Release_Art-Geneve_2026_EN_09.01.2026-1.pdf",
        excerpt: "Seventeen, London listed among Art Genève 2026 galleries",
        confidence: "HIGH",
      },
    ],
    travelingCohortType: TRAVELING_COHORT_TYPE.EXHIBITOR_TEAM,
    travelingCohortSummary:
      "London gallery install / sales team for Art Genève — compact working group",
    travelingCohortConfidence: "MEDIUM",
    travelingCohortEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_TRAVEL,
        sourceUrl:
          "https://www.palexpo.ch/wp-content/uploads/2026/01/Press_Release_Art-Geneve_2026_EN_09.01.2026-1.pdf",
        excerpt: "London gallery confirmed exhibitor",
        confidence: "MEDIUM",
      },
    ],
    lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.ACCOUNT_DIRECT,
    lodgingControlSummary: "Gallery travel hypothesized",
    lodgingControlConfidence: "LOW",
    lodgingControlEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
        excerpt: "Controller unknown",
        confidence: "LOW",
      },
    ],
    modeledRoomsMin: 4,
    modeledRoomsMax: 10,
    modeledNightsMin: 4,
    modeledNightsMax: 6,
    modeledDemandBasis: "Modeled gallery working team — lodging not verified",
    hotelFitScore: 71,
    hotelFitRationale: ["International working team", "Compact band", "Avoid collector VIP assumption"],
    publicContactPath: "https://artgeneve.ch/en/home/",
    buyerEntity: "Seventeen — fair ops",
    buyerRole: "Fair ops",
    recommendedNextAction: "Confirm fair-week team lodging near Palexpo",
    usefulnessScore: 70,
    noveltyScore: 84,
  },

  // ─── GENEVA HEALTH FORUM — named institutional affiliations from published speakers ───
  {
    accountName: "European Federation of Allergy and Airways Diseases Patients' Associations (EFA)",
    parentDemandSignalId: "yotel_signal_geneva_health_forum_2026",
    accountRole: "SPEAKER_ORG",
    evidenceStatus: "ACCEPTED",
    hqRegion: "Belgium",
    evidenceItems: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
        evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_SPEAKER,
        sourceUrl: "https://worldhealthassembly2026.genevahealthforum.com/speakers/",
        excerpt:
          "Panagiotis Chaslaridis, Senior Policy Advisor, EFA (Belgium) confirmed GHF/WHA side-event speaker",
        confidence: "HIGH",
      },
    ],
    travelingCohortType: TRAVELING_COHORT_TYPE.SPEAKER_FACULTY,
    travelingCohortSummary:
      "Brussels-based patient-advocacy policy staff traveling for GHF/WHA-adjacent Geneva programme (small institutional cohort, not mass attendees)",
    travelingCohortConfidence: "MEDIUM",
    travelingCohortEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_TRAVEL,
        sourceUrl: "https://worldhealthassembly2026.genevahealthforum.com/speakers/",
        excerpt: "Belgium-based EFA speaker confirmed in Geneva programme",
        confidence: "MEDIUM",
      },
    ],
    lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.ACCOUNT_DIRECT,
    lodgingControlSummary: "Association travel desk hypothesized",
    lodgingControlConfidence: "LOW",
    lodgingControlEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
        excerpt: "Controller unknown",
        confidence: "LOW",
      },
    ],
    modeledRoomsMin: 2,
    modeledRoomsMax: 8,
    modeledNightsMin: 2,
    modeledNightsMax: 5,
    modeledDemandBasis:
      "Modeled small institutional speaker/support cohort — lodging not verified; near lower YOTEL band",
    hotelFitScore: 68,
    hotelFitRationale: [
      "International (non-Geneva) institutional traveler",
      "Small band — borderline YOTEL size",
      "Campus Biotech / airport access",
    ],
    publicContactPath: "https://worldhealthassembly2026.genevahealthforum.com/speakers/",
    buyerEntity: "EFA — events / policy travel",
    buyerRole: "Events / Policy travel",
    recommendedNextAction:
      "Confirm whether EFA sends a small support cohort beyond the named speaker for GHF week",
    usefulnessScore: 64,
    noveltyScore: 80,
  },

  // ─── CHI — named sponsors with travel caution ───
  {
    accountName: "Protectas",
    parentDemandSignalId: "yotel_signal_chi_geneva_2026",
    accountRole: "SPONSOR",
    evidenceStatus: "ACCEPTED",
    hqRegion: "Switzerland (security services)",
    evidenceItems: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
        evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_SPONSOR,
        sourceUrl:
          "https://www.chi-geneve.ch/en/Edition-2026/Program/Program-2026-of-the-CHI-Geneva.html",
        excerpt: "Prix des Familles sponsored by Protectas on official CHI Geneva 2026 programme",
        confidence: "HIGH",
      },
    ],
    travelingCohortType: TRAVELING_COHORT_TYPE.SPONSOR_ACTIVATION,
    travelingCohortSummary:
      "Sponsor activation / hospitality staff for CHI programme segment — may be largely local CH; overnight travel weak",
    travelingCohortConfidence: "LOW",
    travelingCohortEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_TRAVEL,
        sourceUrl:
          "https://www.chi-geneve.ch/en/Edition-2026/Program/Program-2026-of-the-CHI-Geneva.html",
        excerpt: "Confirmed sponsor; travel cohort weak if Protectas is Geneva-local operator",
        confidence: "LOW",
      },
    ],
    lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.UNKNOWN,
    lodgingControlSummary: "Unknown; local sponsor risk",
    lodgingControlConfidence: "LOW",
    lodgingControlEvidence: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
        evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
        excerpt: "Local sponsor may not create hotel demand",
        confidence: "LOW",
      },
    ],
    modeledRoomsMin: 2,
    modeledRoomsMax: 8,
    modeledNightsMin: 2,
    modeledNightsMax: 4,
    modeledDemandBasis: "Modeled weak sponsor activation band — lodging not verified",
    hotelFitScore: 45,
    hotelFitRationale: ["Local sponsor risk", "Weak overnight thesis", "Keep as CANDIDATE watch"],
    publicContactPath:
      "https://www.chi-geneve.ch/en/Edition-2026/Program/Program-2026-of-the-CHI-Geneva.html",
    buyerEntity: "Protectas — sponsorship / events",
    buyerRole: "Sponsorship / Events",
    recommendedNextAction: "Verify whether Protectas activation staff travel or are local-only",
    usefulnessScore: 35,
    noveltyScore: 40,
    forceMaxMaturity: GDI_MATURITY_STATE.CANDIDATE,
  },

  // ─── REJECTIONS ───
  {
    accountName: "Rolex",
    parentDemandSignalId: "yotel_signal_watches_wonders_2027",
    accountRole: "EXHIBITOR",
    evidenceStatus: "REJECTED",
    rejectReason:
      "Confirmed W&W exhibitor but ultra-luxury maison executive/VIP lodging incompatible with YOTEL fit priority (down-rank)",
    evidenceItems: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
        evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_EXHIBITOR,
        sourceUrl:
          "https://www.watchesandwonders.com/content/dam/waw/en/media-center/mc-geneva-2027/2026-09-17/pdf/WWG27_Press%20Release_17.09.26_ENG.pdf",
        excerpt: "Rolex listed as W&W 2027 exhibitor",
        confidence: "HIGH",
      },
    ],
  },
  {
    accountName: "Patek Philippe",
    parentDemandSignalId: "yotel_signal_watches_wonders_2027",
    accountRole: "EXHIBITOR",
    evidenceStatus: "REJECTED",
    rejectReason: "Ultra-luxury maison — YOTEL fit down-rank (executive/VIP centro lodging expected)",
    evidenceItems: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
        evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_EXHIBITOR,
        sourceUrl:
          "https://www.watchesandwonders.com/content/dam/waw/en/media-center/mc-geneva-2027/2026-09-17/pdf/WWG27_Press%20Release_17.09.26_ENG.pdf",
        excerpt: "Patek Philippe listed as W&W 2027 exhibitor",
        confidence: "HIGH",
      },
    ],
  },
  {
    accountName: "Catherine Duret",
    parentDemandSignalId: "yotel_signal_art_geneve_2026",
    accountRole: "EXHIBITOR",
    evidenceStatus: "REJECTED",
    rejectReason: "Local Geneva gallery — no international travel thesis",
    evidenceItems: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
        evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_EXHIBITOR,
        sourceUrl:
          "https://www.palexpo.ch/wp-content/uploads/2026/01/Press_Release_Art-Geneve_2026_EN_09.01.2026-1.pdf",
        excerpt: "Catherine Duret, Geneva listed",
        confidence: "HIGH",
      },
    ],
  },
  {
    accountName: "SETAC Europe",
    parentDemandSignalId: "yotel_signal_setac_europe_37_2027",
    accountRole: "ORGANIZER",
    evidenceStatus: "REJECTED",
    rejectReason: "Organizer/society shell — keep Future Watch; no housing control proven",
    evidenceItems: [],
  },
  {
    accountName: "University of Geneva — Institute of Global Health",
    parentDemandSignalId: "yotel_signal_geneva_health_forum_2026",
    accountRole: "UNIVERSITY_HOST",
    evidenceStatus: "REJECTED",
    rejectReason: "Local university host — not automatically the opportunity",
    evidenceItems: [],
  },
  {
    accountName: "Palexpo SA",
    parentDemandSignalId: "yotel_signal_aidex_geneva_2026",
    accountRole: "VENUE_OPERATOR",
    evidenceStatus: "REJECTED",
    rejectReason: "Venue operator leakage — never customer account",
    evidenceItems: [],
  },
  {
    accountName: "United Nations ECOSOC / OCHA",
    parentDemandSignalId: "yotel_signal_ecosoc_ocha",
    accountRole: "SECRETARIAT",
    evidenceStatus: "REJECTED",
    rejectReason:
      "2026 ECOSOC Humanitarian Affairs Segment is officially in New York (UNHQ), not Geneva — no YOTEL Geneva lodging thesis from this cycle",
    evidenceItems: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
        evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_EVENT,
        sourceUrl: "https://ecosoc.un.org/en/events/2026/humanitarian-affairs-segment",
        excerpt: "2026 HAS held in New York 17–19 June 2026",
        confidence: "HIGH",
      },
    ],
  },
  {
    accountName: "Oxfam International",
    parentDemandSignalId: "yotel_signal_ecosoc_ocha",
    accountRole: "PARTICIPANT",
    evidenceStatus: "REJECTED",
    rejectReason: "Named on NY HAS panel — travel destination is New York, not Geneva",
    evidenceItems: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
        evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_PARTICIPATION,
        sourceUrl: "https://igov.un.org/e/plenary/2026/mr.28",
        excerpt: "Oxfam International on NY HAS panel",
        confidence: "HIGH",
      },
    ],
  },
  {
    accountName: "UBS",
    parentDemandSignalId: "yotel_signal_chi_geneva_2026",
    accountRole: "SPONSOR",
    evidenceStatus: "REJECTED",
    rejectReason: "Local Geneva HQ bank sponsor — weak overnight travel thesis for YOTEL",
    evidenceItems: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
        evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_SPONSOR,
        sourceUrl:
          "https://www.chi-geneve.ch/en/Edition-2026/Program/Program-2026-of-the-CHI-Geneva.html",
        excerpt: "Challenge of the Century sponsored by UBS",
        confidence: "HIGH",
      },
    ],
  },
  {
    accountName: "Geneva compression opportunity",
    parentDemandSignalId: "yotel_signal_watches_wonders_2027",
    accountRole: "GENERIC",
    evidenceStatus: "REJECTED",
    rejectReason: "Generic compression shell — no named account",
    evidenceItems: [],
  },
  {
    accountName: "ECOSOC attendees",
    parentDemandSignalId: "yotel_signal_ecosoc_ocha",
    accountRole: "GENERIC",
    evidenceStatus: "REJECTED",
    rejectReason: "Generic shell — named organizations only",
    evidenceItems: [],
  },
  {
    accountName: "Human Rights Watch",
    parentDemandSignalId: "yotel_signal_geneva_health_forum_2026",
    accountRole: "SPEAKER_ORG",
    evidenceStatus: "REJECTED",
    rejectReason:
      "Speaker affiliation confirmed but Geneva office presence makes overnight travel thesis weak / local-heavy",
    evidenceItems: [
      {
        claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
        evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_SPEAKER,
        sourceUrl: "https://worldhealthassembly2026.genevahealthforum.com/speakers/",
        excerpt: "Richard Pearshouse, HRW, listed as speaker",
        confidence: "HIGH",
      },
    ],
  },
]);

export function buildModeledDemandDisclaimer(account) {
  const min = account.modeledRoomsMin;
  const max = account.modeledRoomsMax;
  if (min == null || max == null) return "Modeled demand — lodging not verified.";
  // Avoid "N rooms" phrasing that summary quality treats as unsupported fabrication
  // unless estimatedPeakRooms is stamped as a verified claim.
  return `Estimated room-band ${min}–${max} (modeled). Lodging not verified.`;
}

export function expansionOpportunityId(accountName, signalId) {
  return `gdi_opp_yotel_exp_v1_${slug(accountName)}_${slug(signalId).replace(/^yotel_signal_/, "")}`;
}

export function buildExpansionOpportunityCandidate(account) {
  const signal = signalById(account.parentDemandSignalId);
  if (!signal || account.evidenceStatus !== "ACCEPTED") return null;

  const modeledDisclaimer = buildModeledDemandDisclaimer(account);
  const missingValidation = [
    "lodging_block_unverified",
    "headcount_unverified",
    "named_buyer_person_missing",
    account.lodgingControlHypothesis === LODGING_CONTROL_HYPOTHESIS.UNKNOWN
      ? "lodging_controller_unknown"
      : null,
    account.forceMaxMaturity === GDI_MATURITY_STATE.CANDIDATE
      ? "geneva_cycle_participation_list_pending"
      : null,
  ].filter(Boolean);

  const title = `${account.accountName} — ${signal.label} ${String(account.accountRole || "")
    .replace(/_/g, " ")
    .toLowerCase()
    .replace(/\b\w/g, (c) => c.toUpperCase())}`;

  const summaryWhat = [
    `${account.accountName} is a published ${String(account.accountRole || "participant")
      .replace(/_/g, " ")
      .toLowerCase()} linked to ${signal.label}.`,
    `Traveling cohort: ${account.travelingCohortSummary}.`,
    modeledDisclaimer,
    `YOTEL Geneva Lake fit: airport / Palexpo corridor for international working teams — not venue/organizer shells.`,
    account.evidenceNote ? `Evidence note: ${account.evidenceNote}.` : "",
    `Next: ${account.recommendedNextAction}`,
  ]
    .filter(Boolean)
    .join(" ");

  const sources = (account.evidenceItems || []).map((e) => ({
    url: e.sourceUrl || e.url,
    title: e.excerpt || e.evidenceType,
    type: e.evidenceType,
  }));

  return {
    id: expansionOpportunityId(account.accountName, account.parentDemandSignalId),
    hotelId: YOTEL_HOTEL_ID,
    title,
    organizationName: account.accountName,
    company: account.accountName,
    buyerEntity: account.buyerEntity || account.accountName,
    buyerRole: account.buyerRole || account.accountRole,
    primaryContactRole: account.buyerRole || account.accountRole,
    publicContactPath: account.publicContactPath || signal.officialSource,
    participationRole: account.accountRole,
    accountQualityClass: ACCOUNT_QUALITY_CLASS.TRUE_PARTICIPATING_ACCOUNT,
    parentDemandSignalId: signal.id,
    parentDemandSignalLabel: signal.label,
    parentDemandSignalType: "EVENT",
    eventStartDate: signal.eventStartDate,
    eventEndDate: signal.eventEndDate,
    eventName: signal.label,
    canonicalEventName: signal.label,
    geography: "Geneva, Switzerland",
    market: "Geneva / Palexpo / airport corridor",
    venue: signal.venue,
    segment: String(account.accountRole || "").replace(/_/g, " "),
    summaryWhat,
    summaryWhyHotel: (account.hotelFitRationale || []).join("; "),
    fitExplanation: (account.hotelFitRationale || []).join("; "),
    hotelFitScore: account.hotelFitScore,
    hotelFitRationale: account.hotelFitRationale || [],
    whyNow: `${signal.label} (${signal.eventStartDate}–${signal.eventEndDate}) is a published cycle. Engage travel / events desks while booth and support-team lodging remain open. Do not pitch invented room counts.`,
    recommendedAction: account.recommendedNextAction,
    recommendedNextAction: account.recommendedNextAction,
    recommendedNextStep: account.recommendedNextAction,
    bookingWindowStatus: BOOKING_WINDOW.QUALIFY_NOW,
    priority: "WATCHLIST",
    expansionPassId: YOTEL_EXPANSION_PASS_ID,
    travelingCohortType: account.travelingCohortType,
    travelingCohortSummary: account.travelingCohortSummary,
    travelingCohortConfidence: account.travelingCohortConfidence,
    travelingCohortEvidence: account.travelingCohortEvidence || [],
    lodgingControlHypothesis: account.lodgingControlHypothesis,
    lodgingControlSummary: account.lodgingControlSummary,
    lodgingControlConfidence: account.lodgingControlConfidence,
    lodgingControlEvidence: account.lodgingControlEvidence || [],
    modeledRoomsMin: account.modeledRoomsMin,
    modeledRoomsMax: account.modeledRoomsMax,
    modeledNightsMin: account.modeledNightsMin,
    modeledNightsMax: account.modeledNightsMax,
    modeledDemandBasis: account.modeledDemandBasis,
    modeledDemandDisclaimer: modeledDisclaimer,
    lodgingVerified: false,
    headcountVerified: false,
    missingValidation,
    buyerResearchStatus: "NOT_STARTED",
    evidenceItems: account.evidenceItems || [],
    sources,
    evidenceUrls: sources.map((s) => s.url).filter(Boolean),
    officialSource: sources[0]?.url || signal.officialSource,
    discoverySource: sources[0]?.url || signal.officialSource,
    demandFamily: "EVENT_SPONSOR_EXHIBITOR",
    opportunityType: "GROUP_DEMAND",
    contactResearchAttempted: false,
    whoResearchAttempted: false,
    isOpportunityMultiplier: account.isOpportunityMultiplier === true,
    multiplierReason: account.multiplierReason || null,
    usefulnessScore: account.usefulnessScore ?? null,
    noveltyScore: account.noveltyScore ?? null,
    forceMaxMaturity: account.forceMaxMaturity || null,
    gdiExpansionCorpus: true,
    isTestData: false,
    isDemandGenerator: false,
    demandGeneratorOnly: false,
    customerFacingState: "ACTIVE",
    customerSurfaceDisposition: "KEEP_ACTIVE",
    salesWorkflowState: "UNTOUCHED",
  };
}

/**
 * Evaluate accepted research rows through canonical maturity (no Ready bypass).
 */
export function buildEvaluatedExpansionCandidates(opts = {}) {
  const nowDate = opts.nowDate || "2026-10-05";
  const accepted = YOTEL_ACCOUNT_RESEARCH.filter((a) => a.evidenceStatus === "ACCEPTED");
  const rejected = YOTEL_ACCOUNT_RESEARCH.filter((a) => a.evidenceStatus === "REJECTED");
  const evaluated = [];

  for (const account of accepted) {
    const draft = buildExpansionOpportunityCandidate(account);
    if (!draft) continue;

    const ready = isGdiCustomerOpportunityReady(draft, { nowDate });
    const maturity = assignGdiMaturityState(draft, { nowDate });
    let state = maturity.gdiMaturityState;

    // Cap maturity when Geneva list pending / weak local sponsor
    if (
      account.forceMaxMaturity &&
      maturityRankSafe(state) > maturityRankSafe(account.forceMaxMaturity)
    ) {
      state = account.forceMaxMaturity;
    }

    const buyerPath = classifyBuyerContactPath(draft);

    evaluated.push({
      account,
      opportunity: {
        ...draft,
        gdiMaturityState: state,
        gdiMaturityReason: maturity.gdiMaturityReason,
        gdiMaturityEvaluatedAt: maturity.gdiMaturityEvaluatedAt,
        buyerResearchStatus:
          buyerPath.class === "SOURCE_PAGE" || buyerPath.class === "GENERAL_ORG_CONTACT"
            ? "PATH_INSUFFICIENT_FOR_ACTIONABLE"
            : "NEEDS_BUYER_RESEARCH",
        buyerContactPathClass: buyerPath.class,
        customerVisible: state === GDI_MATURITY_STATE.ACTIONABLE,
      },
      maturity: { ...maturity, gdiMaturityState: state },
      ready,
      buyerPath,
    });
  }

  return { accepted, rejected, evaluated, nowDate };
}

function maturityRankSafe(state) {
  const order = { SIGNAL: 0, CANDIDATE: 1, QUALIFIED: 2, ACTIONABLE: 3 };
  return order[String(state || "")] ?? -1;
}

export { GDI_MATURITY_STATE, LODGING_CONTROL_HYPOTHESIS, TRAVELING_COHORT_TYPE };
