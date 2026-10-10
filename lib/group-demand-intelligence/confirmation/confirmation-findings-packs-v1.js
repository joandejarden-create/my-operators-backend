/**
 * Confirmation research findings packs for Opportunity Confirmation Engine V1.
 * Evidence-backed only — no invented buyers, rooms, or lodging blocks.
 */

import {
  PARTICIPATION_STATUS,
  RECURRENCE_STATUS,
  EXTENDED_COHORT_TYPE,
} from "./opportunity-confirmation-v1.js";
import {
  LODGING_CONTROL_HYPOTHESIS,
} from "../gdi-maturity-v1.js";
import {
  GDI_EVIDENCE_TYPE,
  GDI_MATURITY_CLAIM_KIND,
} from "../gdi-evidence-taxonomy-v1.js";

/** @type {Record<string, object>} */
export const YOTEL_CONFIRMATION_FINDINGS_V1 = Object.freeze({
  "Key Travel": {
    participation: {
      participationStatus: PARTICIPATION_STATUS.CONFIRMED_CURRENT,
      participationConfidence: "HIGH",
      evidenceItems: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_EXHIBITOR,
          sourceUrl: "https://aid-expo.com/exhibitors-2026",
          excerpt: "Key Travel By Trip.Biz listed AidEx Geneva 2026 exhibitor stand F7/c",
          confidence: "HIGH",
        },
      ],
    },
    travelingCohort: {
      travelingCohortType: EXTENDED_COHORT_TYPE.PROJECT_TEAM,
      travelingCohortSummary:
        "Key Travel humanitarian TMC commercial / account team staffing the AidEx booth and engaging NGO travel clients during the Geneva show window",
      travelingCohortConfidence: "MEDIUM",
      travelingCohortEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_TRAVELING_COHORT,
          sourceUrl: "https://www.keytravel.com/who-we-serve/humanitarian-travel/",
          excerpt:
            "Key Travel publishes humanitarian/NGO travel management services including group travel specialist team",
          confidence: "MEDIUM",
        },
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
          evidenceType: GDI_EVIDENCE_TYPE.INFERRED_TRAVEL,
          sourceUrl: "https://aid-expo.com/exhibitors-2026",
          excerpt: "Confirmed AidEx booth implies traveling commercial staffing for show days",
          confidence: "MEDIUM",
        },
      ],
    },
    recurrence: {
      recurrenceStatus: RECURRENCE_STATUS.SINGLE_PRIOR,
      recurrenceConfidence: "MEDIUM",
      recurrenceEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_RECURRING_PATTERN,
          sourceUrl: "https://aid-expo.com/exhibitors/key-travel",
          excerpt: "Key Travel listed as AidEx Expo 2025 exhibitor (prior edition)",
          confidence: "MEDIUM",
        },
      ],
    },
    lodgingControl: {
      lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.TMC,
      lodgingControlSummary:
        "Key Travel is a specialist humanitarian TMC that manages complex travel including group travel for NGOs — may influence client lodging; YOTEL block not evidenced",
      lodgingControlConfidence: "MEDIUM",
      lodgingControlEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
          sourceUrl: "https://www.keytravel.com/who-we-serve/humanitarian-travel/",
          excerpt:
            "Official Key Travel humanitarian travel page describes group travel specialist team for mission-driven orgs",
          confidence: "MEDIUM",
        },
      ],
    },
    buyerPath: {
      publicContactPath: "https://www.keytravel.com/who-we-serve/humanitarian-travel/",
      buyerEntity: "Key Travel — Humanitarian Travel / Group Travel",
      buyerRole: "Humanitarian travel / Group travel specialist",
      buyerContactPathClass: "RELEVANT_FUNCTION_CONTACT",
      pressContactOnly: false,
      tooGeneric: false,
      evidenceItems: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_PARTICIPATION,
          sourceUrl: "https://www.keytravel.com/who-we-serve/humanitarian-travel/",
          excerpt: "Functional humanitarian travel page (not generic homepage)",
          confidence: "HIGH",
        },
      ],
    },
    whyNow: {
      text: "Key Travel is a confirmed AidEx Geneva 2026 exhibitor (21–22 Oct 2026) and a specialist humanitarian TMC; booth staffing and client group-travel planning for the Geneva Palexpo week should be active now.",
      quality: "STRONG",
    },
    recommendedAction: {
      text: "Contact Key Travel's Humanitarian Travel / Group Travel specialist team via the published humanitarian travel function to ask whether AidEx-week Geneva lodging is being sourced for booth staff and/or NGO client groups — and whether airport-corridor inventory (including YOTEL Geneva Lake) is still open.",
    },
    notes: "Strongest YOTEL confirmation candidate — TMC multiplier + functional path",
  },

  "Kuehne + Nagel": {
    participation: {
      participationStatus: PARTICIPATION_STATUS.CONFIRMED_CURRENT,
      participationConfidence: "HIGH",
      evidenceItems: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_EXHIBITOR,
          sourceUrl: "https://aid-expo.com/exhibitors-2026",
          excerpt: "Kuehne + Nagel A/S listed on AidEx Geneva 2026 exhibitor directory",
          confidence: "HIGH",
        },
      ],
    },
    travelingCohort: {
      travelingCohortType: EXTENDED_COHORT_TYPE.LOGISTICS_SUPPORT_TEAM,
      travelingCohortSummary:
        "Emergency & Relief logistics commercial / specialist team staffing AidEx booth for humanitarian buyers",
      travelingCohortConfidence: "MEDIUM",
      travelingCohortEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_TRAVELING_COHORT,
          sourceUrl:
            "https://www.kuehne-nagel.com/services/emergency-relief-logistics/global-crisis-supply-chain",
          excerpt: "K+N publishes dedicated Emergency & Relief logistics specialist network",
          confidence: "MEDIUM",
        },
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_RECURRING_PATTERN,
          sourceUrl:
            "https://www.linkedin.com/posts/mette-oerslund-589a2b16_aidex-emergencyandrelief-activity-7380889651571593218-5jfl",
          excerpt: "Prior AidEx 2025 K+N Emergency & Relief booth announcement (recurrence support)",
          confidence: "MEDIUM",
        },
      ],
    },
    recurrence: {
      recurrenceStatus: RECURRENCE_STATUS.SINGLE_PRIOR,
      recurrenceConfidence: "MEDIUM",
      recurrenceEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_RECURRING_PATTERN,
          sourceUrl: "https://aid-expo.com/exhibitors/kuehne-nagel-s",
          excerpt: "Kuehne + Nagel A/S AidEx Expo 2025 exhibitor listing",
          confidence: "MEDIUM",
        },
      ],
    },
    lodgingControl: {
      lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.ACCOUNT_DIRECT,
      lodgingControlSummary:
        "Corporate / Emergency & Relief travel desk hypothesized for booth staff; controller not named for AidEx 2026",
      lodgingControlConfidence: "LOW",
      lodgingControlEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
          evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
          excerpt: "No published AidEx housing controller for K+N booth team",
          confidence: "LOW",
        },
      ],
    },
    buyerPath: {
      publicContactPath:
        "https://www.kuehne-nagel.com/services/emergency-relief-logistics/global-crisis-supply-chain",
      buyerEntity: "Kuehne+Nagel — Emergency & Relief Logistics",
      buyerRole: "Emergency & Relief logistics / Events",
      buyerContactPathClass: "RELEVANT_FUNCTION_CONTACT",
      pressContactOnly: false,
      tooGeneric: false,
      evidenceItems: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_PARTICIPATION,
          sourceUrl:
            "https://www.kuehne-nagel.com/services/emergency-relief-logistics/global-crisis-supply-chain",
          excerpt: "Dedicated Emergency & Relief logistics function page with talk-to-expert CTA",
          confidence: "HIGH",
        },
      ],
    },
    whyNow: {
      text: "Kuehne+Nagel is confirmed on the AidEx Geneva 2026 exhibitor list; Emergency & Relief commercial staffing for the 21–22 Oct Palexpo show should be arranging travel now.",
      quality: "STRONG",
    },
    recommendedAction: {
      text: "Contact Kuehne+Nagel Emergency & Relief Logistics via the published emergency-relief function to confirm whether the AidEx Geneva 2026 booth team has placed airport-corridor lodging and whether overflow inventory remains open.",
    },
  },

  "CEVA Logistics": {
    participation: {
      participationStatus: PARTICIPATION_STATUS.CONFIRMED_CURRENT,
      participationConfidence: "HIGH",
      evidenceItems: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_EXHIBITOR,
          sourceUrl: "https://aid-expo.com/exhibitors-2026",
          excerpt: "CEVA Logistics listed AidEx Geneva 2026 exhibitor stand D6/c",
          confidence: "HIGH",
        },
      ],
    },
    travelingCohort: {
      travelingCohortType: EXTENDED_COHORT_TYPE.EXHIBITOR_TEAM,
      travelingCohortSummary:
        "Humanitarian logistics BD / booth team traveling for AidEx Geneva show days",
      travelingCohortConfidence: "MEDIUM",
      travelingCohortEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
          evidenceType: GDI_EVIDENCE_TYPE.INFERRED_TRAVEL,
          sourceUrl: "https://aid-expo.com/exhibitors-2026",
          excerpt: "Confirmed stand exhibitor implies traveling booth staffing",
          confidence: "MEDIUM",
        },
      ],
    },
    recurrence: {
      recurrenceStatus: RECURRENCE_STATUS.NONE,
      recurrenceConfidence: "LOW",
      recurrenceEvidence: [],
    },
    lodgingControl: {
      lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.UNKNOWN,
      lodgingControlSummary: "Corporate travel desk likely; not identified for AidEx 2026",
      lodgingControlConfidence: "LOW",
      lodgingControlEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
          evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
          excerpt: "No published lodging controller",
          confidence: "LOW",
        },
      ],
    },
    buyerPath: {
      publicContactPath: "https://aid-expo.com/exhibitors-2026",
      buyerEntity: "CEVA Logistics — humanitarian / events BD",
      buyerRole: "Events / Humanitarian logistics BD",
      buyerContactPathClass: "SOURCE_PAGE",
      pressContactOnly: false,
      tooGeneric: true,
      evidenceItems: [],
      notes: "No dedicated CEVA events/travel function URL evidenced — exhibitor directory only",
    },
    whyNow: {
      text: "CEVA Logistics is a confirmed AidEx Geneva 2026 exhibitor; booth-team lodging for 21–22 Oct should be decided in the near term — but a purchase-decision contact path is not yet public.",
      quality: "ADEQUATE",
    },
    recommendedAction: {
      text: "Locate CEVA Logistics humanitarian or trade-show events function (not only the AidEx directory page) and ask whether the Geneva booth team has already placed airport-corridor lodging.",
    },
  },

  "Maersk Logistics & Services": {
    participation: {
      participationStatus: PARTICIPATION_STATUS.CONFIRMED_CURRENT,
      participationConfidence: "HIGH",
      evidenceItems: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_EXHIBITOR,
          sourceUrl: "https://aid-expo.com/exhibitors-2026",
          excerpt: "Maersk Logistics & Services Denmark A/S listed on AidEx Geneva 2026 directory",
          confidence: "HIGH",
        },
      ],
    },
    travelingCohort: {
      travelingCohortType: EXTENDED_COHORT_TYPE.EXHIBITOR_TEAM,
      travelingCohortSummary:
        "Maersk humanitarian logistics commercial team traveling from Denmark/EMEA for AidEx booth staffing",
      travelingCohortConfidence: "MEDIUM",
      travelingCohortEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
          evidenceType: GDI_EVIDENCE_TYPE.INFERRED_TRAVEL,
          sourceUrl: "https://aid-expo.com/exhibitors-2026",
          excerpt: "Denmark-listed entity exhibiting in Geneva implies travel",
          confidence: "MEDIUM",
        },
      ],
    },
    recurrence: {
      recurrenceStatus: RECURRENCE_STATUS.NONE,
      recurrenceConfidence: "LOW",
      recurrenceEvidence: [],
    },
    lodgingControl: {
      lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.UNKNOWN,
      lodgingControlSummary: "Corporate TMC likely; not named",
      lodgingControlConfidence: "LOW",
      lodgingControlEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
          evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
          excerpt: "Controller unknown",
          confidence: "LOW",
        },
      ],
    },
    buyerPath: {
      publicContactPath: "https://aid-expo.com/exhibitors-2026",
      buyerEntity: "Maersk Logistics — humanitarian events",
      buyerRole: "Events / Humanitarian logistics",
      buyerContactPathClass: "SOURCE_PAGE",
      tooGeneric: true,
      pressContactOnly: false,
      evidenceItems: [],
    },
    whyNow: {
      text: "Maersk Logistics is confirmed for AidEx Geneva 2026; booth travel planning for late October should be underway, pending a usable purchase-path contact.",
      quality: "ADEQUATE",
    },
    recommendedAction: {
      text: "Identify Maersk Logistics humanitarian events / trade-show travel function and confirm whether AidEx Geneva booth lodging has been placed near Palexpo/airport.",
    },
  },

  "NOMOS Glashütte": {
    participation: {
      participationStatus: PARTICIPATION_STATUS.CONFIRMED_CURRENT,
      participationConfidence: "HIGH",
      evidenceItems: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_EXHIBITOR,
          sourceUrl:
            "https://www.watchesandwonders.com/content/dam/waw/en/media-center/mc-geneva-2027/2026-09-17/pdf/WWG27_Press%20Release_17.09.26_ENG.pdf",
          excerpt: "NOMOS Glashütte listed among W&W Geneva 2027 exhibiting brands",
          confidence: "HIGH",
        },
      ],
    },
    travelingCohort: {
      travelingCohortType: EXTENDED_COHORT_TYPE.BRAND_SUPPORT_TEAM,
      travelingCohortSummary:
        "German brand booth / retail / press-support team traveling for multi-day Watches & Wonders Geneva salon",
      travelingCohortConfidence: "MEDIUM",
      travelingCohortEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
          evidenceType: GDI_EVIDENCE_TYPE.INFERRED_TRAVEL,
          sourceUrl:
            "https://www.watchesandwonders.com/content/dam/waw/en/media-center/mc-geneva-2027/2026-09-17/pdf/WWG27_Press%20Release_17.09.26_ENG.pdf",
          excerpt: "Confirmed exhibiting brand HQ outside Geneva",
          confidence: "MEDIUM",
        },
      ],
    },
    recurrence: {
      recurrenceStatus: RECURRENCE_STATUS.MULTI_YEAR,
      recurrenceConfidence: "MEDIUM",
      recurrenceEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_RECURRING_PATTERN,
          sourceUrl:
            "https://www.watchesandwonders.com/content/dam/waw/en/media-center/mc-geneva-2026/2026-04-09/pdf/WWG26_Press-Release_09.04.26_EN_V2.pdf",
          excerpt: "NOMOS Glashütte also listed among W&W Geneva 2026 exhibiting brands",
          confidence: "HIGH",
        },
      ],
    },
    lodgingControl: {
      lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.UNKNOWN,
      lodgingControlSummary: "Brand events / travel desk not published",
      lodgingControlConfidence: "LOW",
      lodgingControlEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
          evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
          excerpt: "No public lodging controller for NOMOS W&W booth team",
          confidence: "LOW",
        },
      ],
    },
    buyerPath: {
      publicContactPath: "https://www.watchesandwonders.com/en",
      buyerEntity: "NOMOS Glashütte — events / brand operations",
      buyerRole: "Events / Brand operations",
      buyerContactPathClass: "SOURCE_PAGE",
      tooGeneric: true,
      pressContactOnly: false,
      evidenceItems: [],
    },
    whyNow: {
      text: "NOMOS is confirmed for Watches & Wonders Geneva 2027 (5–11 Apr); booth and support-team lodging planning typically starts many months ahead — contact path still needs a brand events function URL.",
      quality: "ADEQUATE",
    },
    recommendedAction: {
      text: "Locate NOMOS Glashütte events / brand-operations travel function (not the W&W homepage) and ask whether the Geneva 2027 booth/support team has placed lodging and whether airport-corridor inventory remains open.",
    },
  },

  Bremont: {
    participation: {
      participationStatus: PARTICIPATION_STATUS.CONFIRMED_CURRENT,
      participationConfidence: "HIGH",
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
    },
    travelingCohort: {
      travelingCohortType: EXTENDED_COHORT_TYPE.BRAND_SUPPORT_TEAM,
      travelingCohortSummary:
        "UK brand booth, retail, and press-support cohort traveling for W&W Geneva salon week",
      travelingCohortConfidence: "MEDIUM",
      travelingCohortEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
          evidenceType: GDI_EVIDENCE_TYPE.INFERRED_TRAVEL,
          sourceUrl:
            "https://www.watchesandwonders.com/content/dam/waw/en/media-center/mc-geneva-2027/2026-09-17/pdf/WWG27_Press%20Release_17.09.26_ENG.pdf",
          excerpt: "UK-based exhibiting brand confirmed for Geneva",
          confidence: "MEDIUM",
        },
      ],
    },
    recurrence: {
      recurrenceStatus: RECURRENCE_STATUS.MULTI_YEAR,
      recurrenceConfidence: "MEDIUM",
      recurrenceEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_RECURRING_PATTERN,
          sourceUrl:
            "https://www.watchesandwonders.com/content/dam/waw/en/media-center/mc-geneva-2026/2026-04-09/pdf/WWG26_Press-Release_09.04.26_EN_V2.pdf",
          excerpt: "Bremont listed among W&W Geneva 2026 exhibiting brands",
          confidence: "HIGH",
        },
      ],
    },
    lodgingControl: {
      lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.UNKNOWN,
      lodgingControlSummary: "Unknown brand travel desk",
      lodgingControlConfidence: "LOW",
      lodgingControlEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
          evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
          excerpt: "Controller unknown",
          confidence: "LOW",
        },
      ],
    },
    buyerPath: {
      publicContactPath: "https://www.watchesandwonders.com/en",
      buyerEntity: "Bremont — events / brand ops",
      buyerRole: "Events / Brand ops",
      buyerContactPathClass: "SOURCE_PAGE",
      tooGeneric: true,
      pressContactOnly: false,
      evidenceItems: [],
    },
    whyNow: {
      text: "Bremont is confirmed for W&W Geneva 2027; UK booth/support lodging for April should enter planning well ahead — purchase path still unresolved.",
      quality: "ADEQUATE",
    },
    recommendedAction: {
      text: "Contact Bremont events / brand operations (public function path) to confirm Geneva 2027 booth-team lodging status and corridor preference.",
    },
  },

  "Porsche Design": {
    participation: {
      participationStatus: PARTICIPATION_STATUS.CONFIRMED_CURRENT,
      participationConfidence: "HIGH",
      evidenceItems: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_EXHIBITOR,
          sourceUrl:
            "https://www.watchesandwonders.com/content/dam/waw/en/media-center/mc-geneva-2027/2026-09-17/pdf/WWG27_Press%20Release_17.09.26_ENG.pdf",
          excerpt: "Porsche Design listed among new W&W Geneva 2027 exhibiting brands",
          confidence: "HIGH",
        },
      ],
    },
    travelingCohort: {
      travelingCohortType: EXTENDED_COHORT_TYPE.SPONSOR_ACTIVATION,
      travelingCohortSummary:
        "Debut brand activation / booth / marketing support team for first Watches & Wonders Geneva participation",
      travelingCohortConfidence: "MEDIUM",
      travelingCohortEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
          evidenceType: GDI_EVIDENCE_TYPE.INFERRED_TRAVEL,
          sourceUrl:
            "https://www.watchesandwonders.com/content/dam/waw/en/media-center/mc-geneva-2027/2026-09-17/pdf/WWG27_Press%20Release_17.09.26_ENG.pdf",
          excerpt: "Official debut brand — activation/support travel expected",
          confidence: "MEDIUM",
        },
      ],
    },
    recurrence: {
      recurrenceStatus: RECURRENCE_STATUS.NONE,
      recurrenceConfidence: "HIGH",
      recurrenceEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_PARTICIPATION,
          sourceUrl:
            "https://www.watchesandwonders.com/content/dam/waw/en/media-center/mc-geneva-2027/2026-09-17/pdf/WWG27_Press%20Release_17.09.26_ENG.pdf",
          excerpt: "Announced as new/debut brand for 2027 — no prior W&W Geneva edition evidenced here",
          confidence: "HIGH",
        },
      ],
    },
    lodgingControl: {
      lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.ACTIVATION_AGENCY,
      lodgingControlSummary:
        "Debut activation often uses agency; controller not published. Do not assume YOTEL inventory.",
      lodgingControlConfidence: "LOW",
      lodgingControlEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
          evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
          excerpt: "Activation agency hypothesized for debut; not confirmed",
          confidence: "LOW",
        },
      ],
    },
    buyerPath: {
      publicContactPath: "https://press.porsche-design.com/en/",
      buyerEntity: "Porsche Lifestyle Group — Communications",
      buyerRole: "Head of Communications / PR",
      buyerContactPathClass: "GENERAL_ORG_CONTACT",
      pressContactOnly: true,
      tooGeneric: true,
      namedBuyerPerson: {
        name: "Angélique Kreichgauer",
        role: "Head of Communications",
        email: null,
        sourceUrl:
          "https://press.porsche-design.com/en/new-porsche-design-timepieces-manufactory-in-grenchen-marks-strong-commitment-to-the-future",
      },
      evidenceItems: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_PARTICIPATION,
          sourceUrl:
            "https://press.porsche-design.com/en/new-porsche-design-timepieces-manufactory-in-grenchen-marks-strong-commitment-to-the-future",
          excerpt:
            "Named PR/communications contacts published — press path only, not purchase/lodging decision",
          confidence: "HIGH",
        },
      ],
    },
    whyNow: {
      text: "Porsche Design is newly confirmed for W&W Geneva 2027; debut activation lodging will be planned early — but only press contacts are public so far, not an events/travel purchase path.",
      quality: "ADEQUATE",
    },
    recommendedAction: {
      text: "Do not pitch via PR alone. Ask Porsche Design / Porsche Lifestyle Group events or brand-experience function (not press) whether the Geneva 2027 exhibition/activation team has placed lodging and who controls the hotel decision.",
    },
    notes: "Press contact found but excluded from ACTIONABLE path per policy",
  },

  "Sinn Spezialuhren": {
    participation: {
      participationStatus: PARTICIPATION_STATUS.CONFIRMED_CURRENT,
      participationConfidence: "HIGH",
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
    },
    travelingCohort: {
      travelingCohortType: EXTENDED_COHORT_TYPE.BRAND_SUPPORT_TEAM,
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
    },
    recurrence: {
      recurrenceStatus: RECURRENCE_STATUS.MULTI_YEAR,
      recurrenceConfidence: "MEDIUM",
      recurrenceEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_RECURRING_PATTERN,
          sourceUrl:
            "https://www.watchesandwonders.com/content/dam/waw/en/media-center/mc-geneva-2026/2026-04-09/pdf/WWG26_Press-Release_09.04.26_EN_V2.pdf",
          excerpt: "Sinn Spezialuhren listed among W&W Geneva 2026 exhibiting brands",
          confidence: "HIGH",
        },
      ],
    },
    lodgingControl: {
      lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.ACCOUNT_DIRECT,
      lodgingControlSummary: "Brand travel desk hypothesized; not published",
      lodgingControlConfidence: "LOW",
      lodgingControlEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
          evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
          excerpt: "Controller unknown",
          confidence: "LOW",
        },
      ],
    },
    buyerPath: {
      publicContactPath: "https://www.watchesandwonders.com/en",
      buyerEntity: "Sinn Spezialuhren — events",
      buyerRole: "Events",
      buyerContactPathClass: "SOURCE_PAGE",
      tooGeneric: true,
      pressContactOnly: false,
      evidenceItems: [],
    },
    whyNow: {
      text: "Sinn is confirmed for W&W Geneva 2027; support-team lodging planning should open well before April — events function path still required.",
      quality: "ADEQUATE",
    },
    recommendedAction: {
      text: "Locate Sinn Spezialuhren events / exhibition travel function and confirm Geneva 2027 booth staffing lodging status.",
    },
  },
});

/** @type {Record<string, object>} */
export const WROME_CONFIRMATION_FINDINGS_V1 = Object.freeze({
  "Red Bull": {
    participation: {
      participationStatus: PARTICIPATION_STATUS.CONFIRMED_CURRENT,
      participationConfidence: "HIGH",
      evidenceItems: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_TEAM,
          sourceUrl:
            "https://sailgp.com/news/26/rolex-sailgp-championship-2027-season-rome-debut/",
          excerpt: "SailGP announces Rome debut 11–12 Sep 2027",
          confidence: "HIGH",
        },
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_PARTICIPATION,
          sourceUrl:
            "https://www.linkedin.com/posts/italysailgp_s7-italy-sail-grand-prix-rome-activity-7464627589551169538-eirN",
          excerpt: "Red Bull Italy SailGP Team announces Rome GP with Red Bull among partners",
          confidence: "HIGH",
        },
      ],
    },
    travelingCohort: {
      travelingCohortType: EXTENDED_COHORT_TYPE.SPORTS_TEAM,
      travelingCohortSummary:
        "Italy SailGP race team plus Red Bull brand hospitality / VIP guest program for the Rome GP weekend",
      travelingCohortConfidence: "MEDIUM",
      travelingCohortEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_TRAVELING_COHORT,
          sourceUrl: "https://en.lamilano.it/by-the-media/SailGP-arrives-in-Rome-in-2027--its-debut-in-the-capital/",
          excerpt:
            "Official announcement describes race stadium, hospitality platform, and Red Bull Italy SailGP Team presence",
          confidence: "MEDIUM",
        },
      ],
    },
    recurrence: {
      recurrenceStatus: RECURRENCE_STATUS.NONE,
      recurrenceConfidence: "HIGH",
      recurrenceEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_EVENT,
          excerpt: "Rome debut 2027 — first Italy SailGP in Rome (not a multi-year Rome history)",
          confidence: "HIGH",
        },
      ],
    },
    lodgingControl: {
      lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.ACCOUNT_DIRECT,
      lodgingControlSummary:
        "Team / brand hospitality typically directs preferred hotels; named lodging buyer not published",
      lodgingControlConfidence: "LOW",
      lodgingControlEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
          evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
          excerpt: "No published hotel partner or housing desk for Red Bull Italy SailGP Rome",
          confidence: "LOW",
        },
      ],
    },
    buyerPath: {
      publicContactPath: "https://sailgp.com/teams/italy/",
      buyerEntity: "Red Bull Italy SailGP Team — hospitality / partner services",
      buyerRole: "Team hospitality / Partner services",
      buyerContactPathClass: "SOURCE_PAGE",
      tooGeneric: true,
      pressContactOnly: false,
      evidenceItems: [],
    },
    whyNow: {
      text: "Rome SailGP 11–12 Sep 2027 is confirmed with Red Bull Italy SailGP Team as host-side partner; hospitality and team lodging planning should begin well before race week — purchase path still needs a hospitality desk URL.",
      quality: "ADEQUATE",
    },
    recommendedAction: {
      text: "Contact Red Bull Italy SailGP Team hospitality / partner services (public function path) to ask whether race-week team and VIP hospitality lodging for Rome has been placed and who controls the hotel decision.",
    },
  },

  Azimut: {
    participation: {
      participationStatus: PARTICIPATION_STATUS.CONFIRMED_CURRENT,
      participationConfidence: "HIGH",
      evidenceItems: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_SPONSOR,
          sourceUrl:
            "https://www.linkedin.com/posts/italysailgp_s7-italy-sail-grand-prix-rome-activity-7464627589551169538-eirN",
          excerpt: "Azimut Italia listed among Red Bull Italy SailGP Rome partners",
          confidence: "HIGH",
        },
      ],
    },
    travelingCohort: {
      travelingCohortType: EXTENDED_COHORT_TYPE.SPONSOR_ACTIVATION,
      travelingCohortSummary:
        "Azimut sponsor activation / client hospitality guests supporting Italian SailGP team at Rome GP",
      travelingCohortConfidence: "MEDIUM",
      travelingCohortEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
          evidenceType: GDI_EVIDENCE_TYPE.INFERRED_TRAVEL,
          sourceUrl:
            "https://www.linkedin.com/posts/italysailgp_s7-italy-sail-grand-prix-rome-activity-7464627589551169538-eirN",
          excerpt: "Confirmed partner — activation/hospitality travel plausible for race weekend",
          confidence: "MEDIUM",
        },
      ],
    },
    recurrence: {
      recurrenceStatus: RECURRENCE_STATUS.NONE,
      recurrenceConfidence: "MEDIUM",
      recurrenceEvidence: [],
    },
    lodgingControl: {
      lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.UNKNOWN,
      lodgingControlSummary: "Sponsor hospitality controller unknown",
      lodgingControlConfidence: "LOW",
      lodgingControlEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
          evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
          excerpt: "No published lodging controller",
          confidence: "LOW",
        },
      ],
    },
    buyerPath: {
      publicContactPath: "https://sailgp.com/teams/italy/",
      buyerEntity: "Azimut — sponsorship / hospitality liaison",
      buyerRole: "Sponsorship / Hospitality",
      buyerContactPathClass: "SOURCE_PAGE",
      tooGeneric: true,
      pressContactOnly: false,
      evidenceItems: [],
    },
    whyNow: {
      text: "Azimut is publicly listed as a Red Bull Italy SailGP partner for the Rome 2027 debut; sponsor hospitality planning should start early — buyer path still generic.",
      quality: "ADEQUATE",
    },
    recommendedAction: {
      text: "Identify Azimut sponsorship / hospitality liaison (not SailGP homepage only) and ask whether Rome GP client hospitality lodging has been placed.",
    },
  },

  "ABB S.p.A.": {
    participation: {
      participationStatus: PARTICIPATION_STATUS.CONFIRMED_CURRENT,
      participationConfidence: "HIGH",
      evidenceItems: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_EXHIBITOR,
          sourceUrl: "https://makerfairerome.eu/en/2026/",
          excerpt: "Official MFR2026 context; ABB S.p.A. listed as Partner in exhibitor directory research pack",
          confidence: "HIGH",
        },
      ],
    },
    travelingCohort: {
      travelingCohortType: EXTENDED_COHORT_TYPE.EXHIBITOR_TEAM,
      travelingCohortSummary:
        "ABB partner / demo staff and visiting technical specialists supporting Maker Faire Rome booth activation",
      travelingCohortConfidence: "MEDIUM",
      travelingCohortEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
          evidenceType: GDI_EVIDENCE_TYPE.INFERRED_TRAVEL,
          excerpt: "Confirmed partner exhibitor — booth staffing travel expected",
          confidence: "MEDIUM",
        },
      ],
    },
    recurrence: {
      recurrenceStatus: RECURRENCE_STATUS.NONE,
      recurrenceConfidence: "LOW",
      recurrenceEvidence: [],
    },
    lodgingControl: {
      lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.ACCOUNT_DIRECT,
      lodgingControlSummary: "Corporate events travel hypothesized; not verified",
      lodgingControlConfidence: "LOW",
      lodgingControlEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
          evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
          excerpt: "Controller unknown",
          confidence: "LOW",
        },
      ],
    },
    buyerPath: {
      publicContactPath: "https://makerfairerome.eu/en/2026/",
      buyerEntity: "ABB Italy — events / partner marketing",
      buyerRole: "Events / Partner marketing",
      buyerContactPathClass: "SOURCE_PAGE",
      tooGeneric: true,
      pressContactOnly: false,
      evidenceItems: [],
    },
    whyNow: {
      text: "ABB S.p.A. is evidenced as a Maker Faire Rome 2026 partner; booth/demo lodging for late October should be decided soon — events function path still required.",
      quality: "ADEQUATE",
    },
    recommendedAction: {
      text: "Contact ABB Italy events / partner marketing via a public function path to identify who is coordinating Maker Faire Rome booth-team lodging for centro overflow — do not assume this desk books rooms.",
    },
  },

  Anycubic: {
    participation: {
      participationStatus: PARTICIPATION_STATUS.CONFIRMED_CURRENT,
      participationConfidence: "HIGH",
      evidenceItems: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_EXHIBITOR,
          sourceUrl: "https://makerfairerome.eu/en/2026/",
          excerpt: "Anycubic listed as Partner in MFR2026 exhibitor research pack",
          confidence: "HIGH",
        },
      ],
    },
    travelingCohort: {
      travelingCohortType: EXTENDED_COHORT_TYPE.EXHIBITOR_TEAM,
      travelingCohortSummary:
        "Anycubic international partner / product demo team traveling for Maker Faire Rome activation",
      travelingCohortConfidence: "MEDIUM",
      travelingCohortEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
          evidenceType: GDI_EVIDENCE_TYPE.INFERRED_TRAVEL,
          excerpt: "International partner exhibitor — demo team travel expected",
          confidence: "MEDIUM",
        },
      ],
    },
    recurrence: {
      recurrenceStatus: RECURRENCE_STATUS.NONE,
      recurrenceConfidence: "LOW",
      recurrenceEvidence: [],
    },
    lodgingControl: {
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
    },
    buyerPath: {
      publicContactPath: "https://makerfairerome.eu/en/2026/",
      buyerEntity: "Anycubic — event / brand activation",
      buyerRole: "Event / Brand activation",
      buyerContactPathClass: "SOURCE_PAGE",
      tooGeneric: true,
      pressContactOnly: false,
      evidenceItems: [],
    },
    whyNow: {
      text: "Anycubic is evidenced as Maker Faire Rome 2026 partner; international demo-team lodging should be planned before the fair — purchase path still generic.",
      quality: "ADEQUATE",
    },
    recommendedAction: {
      text: "Identify Anycubic event / brand-activation travel function and confirm Maker Faire Rome team lodging plans for Rome centro.",
    },
  },

  "Banca Ifis": {
    participation: {
      participationStatus: PARTICIPATION_STATUS.CONFIRMED_CURRENT,
      participationConfidence: "HIGH",
      evidenceItems: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_SPONSOR,
          sourceUrl: "https://www.romacinemafest.it/en/banca-ifis-main-partner/",
          excerpt: "Banca Ifis Main Partner for Rome Film Fest from 2026 edition",
          confidence: "HIGH",
        },
      ],
    },
    travelingCohort: {
      travelingCohortType: EXTENDED_COHORT_TYPE.VIP_HOSPITALITY,
      travelingCohortSummary:
        "Banca Ifis executive / client hospitality and Music Award ceremony guests around Rome Film Fest opening week",
      travelingCohortConfidence: "MEDIUM",
      travelingCohortEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
          evidenceType: GDI_EVIDENCE_TYPE.INFERRED_TRAVEL,
          sourceUrl: "https://www.romacinemafest.it/en/banca-ifis-main-partner/",
          excerpt: "Main Partner hospitality programs typically travel for opening / award segments",
          confidence: "MEDIUM",
        },
      ],
    },
    recurrence: {
      recurrenceStatus: RECURRENCE_STATUS.MULTI_YEAR,
      recurrenceConfidence: "MEDIUM",
      recurrenceEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.FACT,
          evidenceType: GDI_EVIDENCE_TYPE.CONFIRMED_RECURRING_PATTERN,
          sourceUrl: "https://www.romacinemafest.it/en/banca-ifis-main-partner/",
          excerpt: "Three-year Main Partner commitment from 2026 edition",
          confidence: "HIGH",
        },
      ],
    },
    lodgingControl: {
      lodgingControlHypothesis: LODGING_CONTROL_HYPOTHESIS.ACCOUNT_DIRECT,
      lodgingControlSummary: "Corporate events / Ifis art hospitality hypothesized; not verified",
      lodgingControlConfidence: "LOW",
      lodgingControlEvidence: [
        {
          claimKind: GDI_MATURITY_CLAIM_KIND.INFERRED,
          evidenceType: GDI_EVIDENCE_TYPE.INFERRED_LODGING_CONTROL,
          excerpt: "No published hotel desk for Banca Ifis Film Fest hospitality",
          confidence: "LOW",
        },
      ],
    },
    buyerPath: {
      publicContactPath: "https://www.romacinemafest.it/en/banca-ifis-main-partner/",
      buyerEntity: "Banca Ifis — events / corporate communications / Ifis art",
      buyerRole: "Events / Corporate communications",
      buyerContactPathClass: "SOURCE_PAGE",
      tooGeneric: true,
      pressContactOnly: false,
      evidenceItems: [],
    },
    whyNow: {
      text: "Banca Ifis is the published Main Partner for Rome Film Fest from 2026; hospitality lodging for opening week should be planned ahead — events purchase path still needs a bank function URL.",
      quality: "ADEQUATE",
    },
    recommendedAction: {
      text: "Contact Banca Ifis events / corporate communications / Ifis art hospitality desk (public function path) to ask whether Film Fest opening-week client hospitality lodging in centro Rome has been placed.",
    },
  },
});
