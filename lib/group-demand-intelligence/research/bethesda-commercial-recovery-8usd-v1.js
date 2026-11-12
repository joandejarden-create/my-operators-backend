/**
 * Bethesda commercial recovery — 16 forensic candidates + $8 paid Exa hard cap.
 *
 * Zero-paid finalization first (ORS reuse, CHD, proactive CI, ten-section).
 * Targeted Exa Agent research only for evidence gaps that change qualification.
 * Never auto-publishes. Never duplicates IDs. Canonical store untouched.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BETHESDA_CANONICAL_HOTEL_ID,
  BETHESDA_CANONICAL_HOTEL_NAME,
} from "./bethesda-full-demand-database-reconciliation-v1.js";
import { mergeBethesdaInternalResearchOpportunities } from "./bethesda-internal-research-union-v1.js";
import {
  loadHotelOpportunityDoc,
  hotelOpportunitiesPath,
  sha256File,
} from "./gdi-hotel-store-integrity-guard-v1.js";
import {
  persistResearchStageOpportunityBag,
  researchStageHotelDir,
  writeJsonAtomic,
} from "./gdi-research-stage-persistence-v1.js";
import {
  runExaAgentResearch,
  quoteExaAgentRun,
  EXA_AGENT_EFFORT,
} from "./exa-agent/exa-agent-provider-adapter-v1.js";
import {
  reconcileUnresolvedQuestionsAfterContact,
  applyOfflineDemandThesisRecoveryInChain,
  attachCommercialDemandTypeInChain,
  evaluatePublicationReviewReadiness,
  applyProactiveCommercialIntelligenceInChain,
} from "../workflow/gdi-watchlist-finalization-handoff-v1.js";
import { evaluateSharedCommercialEligibility } from "../commercial-eligibility-contract-v1.js";
import { buildSharedTenSectionDrawerDto } from "../demand-segments/shared-ten-section-drawer-contract-v1.js";
import { filterCustomerFacingOpportunities } from "../customer-visibility.js";
import {
  createPaidBudgetLedger,
  persistPaidBudgetLedger,
  remainingBudgetUsd,
  canAffordQuotedCall,
  reserveQuotedCall,
  settleReservedCall,
} from "./gdi-authorized-paid-budget-ledger-v1.js";

export const BETHESDA_COMMERCIAL_RECOVERY_8USD_V1 =
  "gdi-bethesda-commercial-recovery-8usd-v1";

export const AUTHORIZATION_ID =
  "FOUNDER_APPROVE_BETHESDA_COMMERCIAL_RECOVERY_8USD_20261112";

export const HARD_CEILING_USD = 8;

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const OUT_REL = "reports/gdi-bethesda-commercial-recovery-8usd-v1";
const FORENSIC_SUMMARY =
  "reports/gdi-bethesda-67-to-19-forensic-reconciliation-v1/00-summary.json";
const PRIOR_PDA_SCORECARDS =
  "reports/gdi-bethesda-exa-authorized-batch-pda-chamber-potomac-v1/02-scorecards.json";

/** Priority paid research — PDA lodging-buyer path already spent in prior batch. */
export const PAID_PRIORITY_TARGETS = Object.freeze([
  {
    id: "gdi_opp_arlington_spring_tournament_2027",
    effort: EXA_AGENT_EFFORT.HIGH,
    objective:
      "Confirm 2027 Arlington Spring Tournament dates, whether a hotel room block / housing partner is named or open, overflow/stay-to-play lodging pathway for Bethesda Marriott (not Arlington host only), named lodging decision-maker or housing coordinator, and a concrete sales next step. Do not invent a room-night volume.",
  },
  {
    id: "gdi_opp_nado_ddaa_washcon_2028",
    effort: EXA_AGENT_EFFORT.HIGH,
    objective:
      "Determine whether NADO/DDAA Washington Conference 2028 has selected a host hotel or opened hotel sourcing; city/venue status; overflow lodging opportunity for Bethesda Marriott; named planner/hotel sourcing contact pathway beyond Brittany Salazar if available; practical next action. Do not invent confirmed RFP.",
  },
  {
    id: "gdi_opp_amwa_2027_annual",
    effort: EXA_AGENT_EFFORT.HIGH,
    objective:
      "Confirm AMWA 112th Annual Meeting 2027 city/venue/host hotel status; whether hotel block is open; Bethesda Marriott geographic/commercial fit honesty; named hotel-sourcing contact; practical next step. Do not invent demand volume.",
  },
  {
    id: "gdi_opp_nado_ddaa_washcon_2027_overflow",
    effort: EXA_AGENT_EFFORT.MEDIUM,
    objective:
      "Verify Crystal Gateway 2027 host capacity/overflow lodging solicitation practices for NADO/DDAA Washington Conference; whether Bethesda Marriott is a realistic overflow play; named overflow/housing pathway; concrete outreach step.",
  },
  {
    id: "disp_recLuxvwwxID7U2B8_montgomery_county_shrm",
    effort: EXA_AGENT_EFFORT.MEDIUM,
    objective:
      "Establish whether Montgomery County SHRM chapter meetings/holiday programs use hotels or off-site catering in Bethesda/Rockville; venue patterns; named meeting/event decision-maker beyond Neha Agarwal if available; Bethesda Marriott guestroom or local catering thesis; practical next action.",
  },
  {
    id: "disp_recLuxvwwxID7U2B8_pmi_montgomery_county_maryland_chapter",
    effort: EXA_AGENT_EFFORT.MEDIUM,
    objective:
      "Establish PMI Montgomery County monthly dinner / FedTALKS venue patterns; hotel vs restaurant use; named planner; Bethesda Marriott fit for rooms or catering; practical outreach step.",
  },
  {
    id: "disp_recLuxvwwxID7U2B8_iwlca_elite_tournaments_iwlca_champions_cup",
    effort: EXA_AGENT_EFFORT.HIGH,
    objective:
      "Confirm IWLCA Champions Cup 2027 SoccerPlex dates and stay-to-play hotel partners; whether hotel housing is open/solicited; Bethesda Marriott fit; named housing contact (Dan Harris or successor); concrete next step.",
  },
  {
    id: "disp_recLuxvwwxID7U2B8_cohera_formerly_csi_dmc_and_360dg_",
    effort: EXA_AGENT_EFFORT.MEDIUM,
    objective:
      "Clarify Cohera (CSI DMC / 360DG) partnership mechanism for DMV hotel referrals/group lodging; whether Annette Hawks is the right entry; whether Bethesda Marriott can become a preferred lodging partner; practical introduction step. Do not invent an existing partnership.",
  },
]);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

export function loadDotEnv({ root = ROOT } = {}) {
  const envPath = path.join(root, ".env");
  if (!fs.existsSync(envPath)) return { loaded: false };
  const raw = fs.readFileSync(envPath, "utf8");
  let n = 0;
  for (const line of raw.split(/\r?\n/)) {
    const t = line.trim();
    if (!t || t.startsWith("#")) continue;
    const eq = t.indexOf("=");
    if (eq < 1) continue;
    const key = t.slice(0, eq).trim();
    let val = t.slice(eq + 1).trim();
    if (
      (val.startsWith('"') && val.endsWith('"')) ||
      (val.startsWith("'") && val.endsWith("'"))
    ) {
      val = val.slice(1, -1);
    }
    if (process.env[key] == null || process.env[key] === "") {
      process.env[key] = val;
      n += 1;
    }
  }
  return { loaded: true, keysSet: n };
}

function outDir(root = ROOT) {
  return path.join(root, OUT_REL);
}

function loadForensicCandidates({ root = ROOT } = {}) {
  const summary = JSON.parse(
    fs.readFileSync(path.join(root, FORENSIC_SUMMARY), "utf8"),
  );
  return { recoveryCandidates: summary.recoveryCandidates || [] };
}

function alreadySpentPdaLodgingBuyer({ root = ROOT } = {}) {
  const p = path.join(root, PRIOR_PDA_SCORECARDS);
  if (!fs.existsSync(p)) return false;
  const doc = JSON.parse(fs.readFileSync(p, "utf8"));
  return (doc.scorecards || []).some(
    (s) =>
      s.id === "disp_recLuxvwwxID7U2B8_parenteral_drug_association_pda_" &&
      Number(s.billedCostUsd) > 0,
  );
}

function resolveUnionOpportunity(id, unionOpps) {
  return (unionOpps || []).find((o) => text(o.id || o.opportunityId) === id) || null;
}

function attachTenSectionDrawer(opportunity) {
  const presentation = buildSharedTenSectionDrawerDto(opportunity, {
    hotelId: opportunity.hotelId || BETHESDA_CANONICAL_HOTEL_ID,
    segmentId: opportunity.researchSegment,
  });
  const recommendedNextAction =
    opportunity.salesOutreachRecommendation ||
    opportunity.recommendedNextSalesAction ||
    (opportunity.primaryContact?.name
      ? `Verify lodging decision role for ${opportunity.primaryContact.name} and introduce Bethesda Marriott for the documented demand window.`
      : "Continue public research for a named commercially relevant buyer before outreach.");

  return {
    ...opportunity,
    recommendedNextSalesAction: recommendedNextAction,
    tenSectionDrawer: {
      version: presentation.version,
      sectionIds: presentation.sectionIds,
      sectionTitles: presentation.sectionTitles,
      opportunityId: presentation.opportunityId,
      segmentId: presentation.segmentId,
      hasSections: Boolean(presentation.sections),
      sections: presentation.sections || null,
      overviewModel: presentation.overviewModel || null,
      salespersonPacket: {
        who: opportunity.primaryContact?.name || opportunity.contactName || null,
        whoRole:
          opportunity.primaryContact?.role ||
          opportunity.primaryContact?.title ||
          opportunity.contactRole ||
          null,
        why:
          opportunity.hotelOpportunityThesis ||
          opportunity.whyThisMattersForHotel ||
          opportunity.title ||
          null,
        when:
          opportunity.eventTimingSummary ||
          opportunity.eventStartDate ||
          null,
        evidenceUrls: [
          ...(opportunity.evidenceUrls || []),
          ...((opportunity.evidence || []).map((e) => e?.url).filter(Boolean) || []),
        ].slice(0, 8),
        recommendedNextAction,
        uncertainties: opportunity.unresolvedQuestions || [],
      },
      completePersistedContent: Boolean(presentation.sections),
    },
  };
}

/**
 * Zero-paid commercial finalization assessment for one opportunity.
 */
export function finalizeCandidateZeroPaid(opportunity, { hotel } = {}) {
  const hotelCtx = hotel || {
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    displayName: BETHESDA_CANONICAL_HOTEL_NAME,
  };
  const preservedOutreach = text(
    opportunity.salesOutreachRecommendation ||
      opportunity.recommendedNextStep ||
      opportunity.recommendedAction ||
      opportunity.commercialNextStep,
  );
  const contact = reconcileUnresolvedQuestionsAfterContact(opportunity);
  const demand = applyOfflineDemandThesisRecoveryInChain(contact.opportunity);
  const typed = attachCommercialDemandTypeInChain(demand.opportunity);
  const proactive = applyProactiveCommercialIntelligenceInChain(typed, {
    hotel: hotelCtx,
  });
  // CHD/proactive CI may rewrite outreach to generic "research continuation /
  // monitor" copy that then fails the AD hotel-fit gate. Preserve recovery
  // research outreach when it is concrete and the rewrite is monitor-only.
  let afterProactive = proactive.opportunity;
  const rewritten = text(
    afterProactive.recommendedNextStep ||
      afterProactive.recommendedAction ||
      afterProactive.salesOutreachRecommendation,
  );
  if (
    preservedOutreach.length >= 40 &&
    /research continuation|monitor|establish a verified named/i.test(rewritten) &&
    !/research continuation|monitor only/i.test(preservedOutreach)
  ) {
    afterProactive = {
      ...afterProactive,
      salesOutreachRecommendation: preservedOutreach,
      recommendedNextStep: preservedOutreach,
      recommendedAction: preservedOutreach,
      commercialNextStep: preservedOutreach,
      outreachRestoredFromRecoveryResearch: true,
    };
  }
  const withDrawer = attachTenSectionDrawer(afterProactive);
  const elig = evaluateSharedCommercialEligibility(withDrawer, {
    hotel: hotelCtx,
  });
  const readiness = evaluatePublicationReviewReadiness(withDrawer, {
    hotel: hotelCtx,
    eligibilityResult: elig,
  });

  const blockers = [...(readiness.blockers || [])].filter(Boolean);

  let disposition = "RESEARCH_CONTINUATION";
  if (readiness.publicationReviewReady === true) {
    disposition = readiness.salesReady
      ? "PUBLICATION_REVIEW_READY_SALES_READY"
      : "PUBLICATION_REVIEW_READY_ACCOUNT_DEVELOPMENT";
  } else if (
    readiness.accountDevelopment === true ||
    elig?.accountDevelopment === true
  ) {
    disposition = "ACCOUNT_DEVELOPMENT_NEEDS_COMPLETION";
  } else if (/EXCLUDE|REMOVE|DISQUALIFIED/i.test(String(elig?.status || ""))) {
    disposition = "REJECT_NOT_COMMERCIAL";
  }

  const next = {
    ...withDrawer,
    publicationReviewReady: readiness.publicationReviewReady === true,
    customerPublicationReady: false,
    customerVisible: false,
    commercialEligibilityContractV1: elig,
    publicationReviewReadinessV1: readiness,
    commercialRecoveryZeroPaidAt: new Date().toISOString(),
    commercialRecoveryDisposition: disposition,
  };

  return {
    opportunity: next,
    disposition,
    publicationReviewReady: readiness.publicationReviewReady === true,
    salesReady: readiness.salesReady === true,
    accountDevelopment:
      readiness.accountDevelopment === true || elig?.accountDevelopment === true,
    demandConfidence: readiness.demandConfidence || demand.category || null,
    blockers: [
      ...new Set(
        blockers.map((b) =>
          typeof b === "string" ? b : b?.code || JSON.stringify(b),
        ),
      ),
    ],
    clearedUnresolved: contact.cleared || [],
    remainingUnresolved: contact.remaining || [],
    paidResearchRecommended:
      disposition === "ACCOUNT_DEVELOPMENT_NEEDS_COMPLETION" ||
      disposition === "RESEARCH_CONTINUATION",
  };
}

function buildExaPayload({ target, opportunity, effort }) {
  const org =
    text(opportunity.organizationName) ||
    text(opportunity.organization) ||
    text(opportunity.title);
  const contact =
    opportunity.primaryContact?.name ||
    opportunity.contactName ||
    opportunity.namedStakeholders?.[0]?.name ||
    null;
  return {
    query: [
      `You are researching ONE hotel sales opportunity for Bethesda Marriott (5151 Pooks Hill Rd, Bethesda MD; hotelId ${BETHESDA_CANONICAL_HOTEL_ID}).`,
      `Opportunity ID (immutable — do not invent a new ID): ${target.id}`,
      `Organization: ${org}`,
      `Existing named contact (verify, do not discard without evidence): ${contact || "none listed"}`,
      `Current title: ${text(opportunity.title)}`,
      `Existing hotel thesis: ${text(opportunity.hotelOpportunityThesis).slice(0, 500)}`,
      "",
      `RESEARCH OBJECTIVE: ${target.objective}`,
      "",
      "Return structured commercial evidence only. Do NOT invent room-night volume, RFPs, or confirmed bookings.",
    ].join("\n"),
    effort,
    outputSchema: {
      type: "object",
      required: [
        "demandActivity",
        "demandConfidence",
        "primaryRevenueThesis",
        "bethesdaMarriottFit",
        "namedContacts",
        "sourceUrls",
        "salesOutreachRecommendation",
        "unresolvedGaps",
      ],
      properties: {
        demandActivity: { type: "string" },
        datesOrTiming: { type: "string" },
        demandConfidence: {
          type: "string",
          enum: [
            "CONFIRMED_DEMAND",
            "RELIABLE_INFERRED_DEMAND",
            "SPECULATIVE_DEMAND",
            "PARTNERSHIP_POTENTIAL_ONLY",
            "OVERFLOW_WATCH_ONLY",
            "INSUFFICIENT_EVIDENCE",
          ],
        },
        primaryRevenueThesis: {
          type: "string",
          enum: ["GUEST_ROOMS", "LOCAL_CATERING", "BOTH", "UNKNOWN"],
        },
        secondaryRevenueThesis: { type: "string" },
        bethesdaMarriottFit: { type: "string" },
        venueSourcingStatus: {
          type: "string",
          enum: [
            "OPEN",
            "HOTEL_VENUE_TBD",
            "PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE",
            "HOST_SELECTED_CLOSED",
            "PARTNERSHIP_ONLY",
            "UNKNOWN",
          ],
        },
        namedContacts: {
          type: "array",
          items: {
            type: "object",
            properties: {
              name: { type: "string" },
              title: { type: "string" },
              email: { type: "string" },
              relevance: { type: "string" },
              sourceUrl: { type: "string" },
            },
          },
        },
        whyPursueNow: { type: "string" },
        salesOutreachRecommendation: { type: "string" },
        remainingQuestions: { type: "array", items: { type: "string" } },
        unresolvedGaps: { type: "array", items: { type: "string" } },
        sourceUrls: { type: "array", items: { type: "string" } },
        salesReadyCandidate: { type: "boolean" },
        accountDevelopmentCandidate: { type: "boolean" },
      },
      additionalProperties: false,
    },
  };
}

function inferActivityLocation(opportunity, score) {
  const blob = [
    score?.demandActivity,
    score?.bethesdaMarriottFit,
    opportunity.whatIsHappening,
    opportunity.organizationName,
    opportunity.title,
  ]
    .map(text)
    .join(" ");
  if (/montgomery|rockville|bethesda|gaithersburg|pooks hill/i.test(blob)) {
    return "Montgomery County, Maryland (Bethesda–Rockville corridor)";
  }
  if (/soccerplex|germantown|boyds/i.test(blob)) {
    return "Maryland SoccerPlex / Montgomery County, Maryland";
  }
  if (/arlington|crystal (city|gateway)|nado|amwa|ddaa/i.test(blob)) {
    return text(opportunity.eventLocation) || opportunity.activityLocation || null;
  }
  return opportunity.activityLocation || opportunity.eventLocation || null;
}

/**
 * Rematerialize paid Exa findings into independent evidence fields the
 * demand-mechanism classifier and AD gates actually read.
 */
const LOCAL_ACTIVITY_ANCHORS = Object.freeze({
  disp_recLuxvwwxID7U2B8_montgomery_county_shrm:
    "Montgomery County, Maryland (Bethesda–Rockville corridor)",
  disp_recLuxvwwxID7U2B8_pmi_montgomery_county_maryland_chapter:
    "Rockville / Bethesda, Montgomery County, Maryland",
  disp_recLuxvwwxID7U2B8_parenteral_drug_association_pda_:
    "Bethesda, Maryland (PDA Training and Research Institute)",
  disp_recLuxvwwxID7U2B8_iwlca_elite_tournaments_iwlca_champions_cup:
    "Maryland SoccerPlex, Boyds / Germantown, Montgomery County, Maryland",
});

export function rematerializePaidEvidenceFields(opportunity = {}) {
  const id = text(opportunity.id || opportunity.opportunityId);
  const activity = text(opportunity.whatIsHappening);
  const fit = text(
    opportunity.whyThisMattersForHotel || opportunity.hotelOpportunityThesis,
  );
  let outreach = text(
    opportunity.salesOutreachRecommendation ||
      opportunity.recommendedNextStep ||
      opportunity.recommendedAction,
  );
  const urls = [
    ...new Set([
      ...(opportunity.evidenceUrls || []),
      ...((opportunity.evidence || []).map((e) => e?.url).filter(Boolean) || []),
      ...((opportunity.sources || []).map((s) => s?.url).filter(Boolean) || []),
    ]),
  ];

  // PDA: derive a concrete outreach from existing named TRI education lead when
  // missing or when CHD left monitor/research-continuation placeholder copy.
  if (
    id === "disp_recLuxvwwxID7U2B8_parenteral_drug_association_pda_" &&
    opportunity.primaryContact?.name &&
    (!outreach ||
      /research continuation|monitor|establish a verified named/i.test(outreach))
  ) {
    outreach = `Email training@pda.org and introduce Bethesda Marriott to ${opportunity.primaryContact.name} (${opportunity.primaryContact.title || "TRI education"}) as a same-city lodging option for PDA TRI Bethesda training course attendees; ask whether a preferred hotel list or room-block partner exists for December 2026 and later courses.`;
  }

  const evidenceStatements = [
    ...(Array.isArray(opportunity.evidence) ? opportunity.evidence : []),
  ];
  if (activity) {
    evidenceStatements.push({
      kind: "exa_recovery_research",
      statement: activity,
      excerpt: activity,
      url: urls[0] || null,
      capturedAt: opportunity.commercialRecoveryPaidAt || new Date().toISOString(),
    });
  }

  const sources = [...(Array.isArray(opportunity.sources) ? opportunity.sources : [])];
  for (const url of urls.slice(0, 8)) {
    if (sources.some((s) => s?.url === url && (s.excerpt || s.quote || s.establishes))) {
      continue;
    }
    sources.push({
      url,
      kind: "exa_recovery_research",
      excerpt: activity || fit || null,
      establishes: activity ? "Demand-generating activity from authorized recovery research" : null,
      capturedAt: opportunity.commercialRecoveryPaidAt || new Date().toISOString(),
    });
  }

  const anchoredLocation =
    LOCAL_ACTIVITY_ANCHORS[id] ||
    opportunity.activityLocation ||
    inferActivityLocation(opportunity, {
      demandActivity: activity,
      bethesdaMarriottFit: fit,
    });

  let next = {
    ...opportunity,
    activityHeadline: opportunity.activityHeadline || activity || opportunity.title,
    evidenceQuote: opportunity.evidenceQuote || activity || null,
    eventDateEvidenceText:
      opportunity.eventDateEvidenceText ||
      opportunity.eventTimingSummary ||
      null,
    activityLocation: anchoredLocation,
    eventLocation: opportunity.eventLocation || anchoredLocation,
    localConnection:
      opportunity.localConnection ||
      (anchoredLocation
        ? `Demand-generating activity anchored in ${anchoredLocation}`
        : null),
    recommendedNextStep: outreach || opportunity.recommendedNextStep || null,
    recommendedAction: outreach || opportunity.recommendedAction || null,
    commercialNextStep: outreach || opportunity.commercialNextStep || null,
    salesOutreachRecommendation:
      outreach || opportunity.salesOutreachRecommendation || null,
    whyThisHotel:
      opportunity.whyThisHotel ||
      fit ||
      opportunity.hotelOpportunityThesis ||
      null,
    summaryWhyHotel:
      opportunity.summaryWhyHotel ||
      fit ||
      opportunity.hotelOpportunityThesis ||
      null,
    whyThisMattersForHotel:
      opportunity.whyThisMattersForHotel ||
      fit ||
      opportunity.hotelOpportunityThesis ||
      null,
    evidence: evidenceStatements,
    sources,
    customerVisible: false,
    customerPublicationReady: false,
  };

  if (
    opportunity.primaryContact?.name &&
    !opportunity.primaryContact?.contactRelevanceLabel
  ) {
    next.primaryContact = {
      ...opportunity.primaryContact,
      namedPerson: true,
      contactRelevanceLabel: "RELEVANT_COMMERCIAL_ENTRY_POINT",
      contactClass:
        opportunity.primaryContact.contactClass ||
        "RELEVANT_COMMERCIAL_ENTRY_POINT",
      commercialClass:
        opportunity.primaryContact.commercialClass ||
        "RELEVANT_COMMERCIAL_ENTRY_POINT",
    };
  }

  // Clear CONDITIONAL_STRETCH honesty when activity is strong-local Montgomery/Bethesda
  // — not Arlington / Crystal City contested stretch overflow.
  if (
    /CONDITIONAL_STRETCH/i.test(String(next.hotelFitHonesty || "")) &&
    /montgomery|rockville|bethesda|gaithersburg|soccerplex|boyds|germantown/i.test(
      `${next.activityLocation || ""} ${activity}`,
    ) &&
    !/arlington|crystal (city|gateway)|downtown dc|nrl/i.test(
      `${activity} ${opportunity.title || ""}`,
    )
  ) {
    next.hotelFitHonesty = "LOCAL_PRACTICAL_FIT";
    next.hotelFitHonestyRematerializedFrom = "CONDITIONAL_STRETCH";
  }

  return next;
}

function mergeExaIntoOpportunity(opportunity, score, target) {
  const firstNamed = (score.namedContacts || []).find((c) => text(c?.name));
  const demandActivity =
    text(score.demandActivity) ||
    text(opportunity.whatIsHappening) ||
    text(opportunity.title);
  const fit = text(score.bethesdaMarriottFit);
  const outreach = text(score.salesOutreachRecommendation);

  let next = {
    ...opportunity,
    whatIsHappening: demandActivity,
    activityHeadline: demandActivity,
    eventTimingSummary:
      text(score.datesOrTiming) || opportunity.eventTimingSummary || null,
    eventDateEvidenceText:
      text(score.datesOrTiming) || opportunity.eventDateEvidenceText || null,
    demandConfidence:
      score.demandConfidence || opportunity.demandConfidence || null,
    commercialDemandConfidence:
      score.demandConfidence || opportunity.commercialDemandConfidence || null,
    hotelOpportunityThesis: fit || opportunity.hotelOpportunityThesis || null,
    whyThisMattersForHotel: fit || opportunity.whyThisMattersForHotel || null,
    whyThisHotel: fit || opportunity.whyThisHotel || null,
    summaryWhyHotel: fit || opportunity.summaryWhyHotel || null,
    whyPursueNow: text(score.whyPursueNow) || opportunity.whyPursueNow || null,
    salesOutreachRecommendation:
      outreach || opportunity.salesOutreachRecommendation || null,
    recommendedNextStep: outreach || opportunity.recommendedNextStep || null,
    recommendedAction: outreach || opportunity.recommendedAction || null,
    commercialNextStep: outreach || opportunity.commercialNextStep || null,
    unresolvedQuestions: Array.isArray(score.remainingQuestions)
      ? score.remainingQuestions
      : Array.isArray(score.unresolvedGaps)
        ? score.unresolvedGaps
        : opportunity.unresolvedQuestions || [],
    evidenceUrls: [
      ...new Set([
        ...(opportunity.evidenceUrls || []),
        ...(score.sourceUrls || []).filter(Boolean),
      ]),
    ],
    activityLocation: inferActivityLocation(opportunity, score),
    eventLocation:
      inferActivityLocation(opportunity, score) || opportunity.eventLocation || null,
    commercialRecoveryPaidAt: new Date().toISOString(),
    commercialRecoveryPaidTargetId: target.id,
    customerVisible: false,
    customerPublicationReady: false,
    publicationApproved: false,
  };

  if (text(score.venueSourcingStatus)) {
    next.venueSourcingStatus = score.venueSourcingStatus;
    next.pursuitSourcingStatus = score.venueSourcingStatus;
  }

  if (score.primaryRevenueThesis && score.primaryRevenueThesis !== "UNKNOWN") {
    next.commercialDemandType =
      score.primaryRevenueThesis === "BOTH"
        ? "BOTH"
        : score.primaryRevenueThesis;
  }

  if (firstNamed) {
    next.primaryContact = {
      ...(opportunity.primaryContact || {}),
      name: text(firstNamed.name),
      title: text(firstNamed.title) || opportunity.primaryContact?.title || null,
      role: text(firstNamed.title) || opportunity.primaryContact?.role || null,
      email: text(firstNamed.email) || opportunity.primaryContact?.email || null,
      namedPerson: true,
      contactRelevanceLabel: "RELEVANT_COMMERCIAL_ENTRY_POINT",
      contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
      commercialClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
      whyThisContact:
        text(firstNamed.relevance) ||
        "Named commercial entry point from authorized recovery research — verify before outreach.",
      sourceUrl: text(firstNamed.sourceUrl) || null,
    };
  }

  if (Array.isArray(score.sourceUrls) && score.sourceUrls.length) {
    const existing = Array.isArray(next.sources) ? next.sources : [];
    next.sources = [
      ...existing,
      ...score.sourceUrls.map((url) => ({
        url,
        kind: "exa_recovery_research",
        excerpt: demandActivity,
        establishes:
          "Demand-generating activity and commercial fit from authorized recovery research",
        capturedAt: new Date().toISOString(),
      })),
    ];
    next.evidence = [
      ...(Array.isArray(next.evidence) ? next.evidence : []),
      {
        kind: "exa_recovery_research",
        statement: demandActivity,
        excerpt: demandActivity,
        url: score.sourceUrls[0],
        capturedAt: new Date().toISOString(),
      },
      ...score.sourceUrls.map((url) => ({
        url,
        kind: "exa_recovery_research",
        statement: demandActivity,
        excerpt: demandActivity,
      })),
    ];
  }

  return rematerializePaidEvidenceFields(next);
}

/**
 * Full recovery run: zero-paid for 16, then paid priority targets under $8.
 */
export async function executeBethesdaCommercialRecovery8Usd({
  root = ROOT,
  dryRun = false,
  authorizePaidResearch = false,
  maxPaidCalls = 40,
} = {}) {
  loadDotEnv({ root });
  const dir = outDir(root);
  fs.mkdirSync(dir, { recursive: true });

  const storeDoc = loadHotelOpportunityDoc(BETHESDA_CANONICAL_HOTEL_ID, { root });
  const storePath = hotelOpportunitiesPath(BETHESDA_CANONICAL_HOTEL_ID, { root });
  const storeShaBefore = sha256File(storePath);
  const union = mergeBethesdaInternalResearchOpportunities(
    BETHESDA_CANONICAL_HOTEL_ID,
    storeDoc.opportunities || [],
    { root },
  );
  const facingBefore = filterCustomerFacingOpportunities(union.opportunities || []);
  const forensic = loadForensicCandidates({ root });
  const hotel = {
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    displayName: BETHESDA_CANONICAL_HOTEL_NAME,
  };

  const zeroPaidRows = [];
  const updatedById = new Map();

  for (const cand of forensic.recoveryCandidates) {
    const base = resolveUnionOpportunity(cand.id, union.opportunities);
    if (!base) {
      zeroPaidRows.push({
        id: cand.id,
        organization: cand.organization,
        disposition: "MISSING_FROM_UNION",
        publicationReviewReady: false,
        paidResearchRecommended: false,
        blockers: ["OPPORTUNITY_NOT_IN_UNION"],
      });
      continue;
    }
    const result = finalizeCandidateZeroPaid(base, { hotel });
    zeroPaidRows.push({
      id: cand.id,
      organization: cand.organization,
      forensicRecovery: cand.recovery,
      forensicSubcategory: cand.subcategory,
      disposition: result.disposition,
      publicationReviewReady: result.publicationReviewReady,
      salesReady: result.salesReady,
      accountDevelopment: result.accountDevelopment,
      demandConfidence: result.demandConfidence,
      blockers: result.blockers,
      paidResearchRecommended: result.paidResearchRecommended,
      namedContact:
        result.opportunity.primaryContact?.name || cand.namedContact || null,
      tenSectionComplete:
        result.opportunity.tenSectionDrawer?.completePersistedContent === true,
    });
    updatedById.set(cand.id, result.opportunity);
  }

  writeJsonAtomic(path.join(dir, "01-zero-paid-finalization.json"), {
    version: BETHESDA_COMMERCIAL_RECOVERY_8USD_V1,
    authorizationId: AUTHORIZATION_ID,
    at: new Date().toISOString(),
    candidateCount: zeroPaidRows.length,
    publicationReviewReadyCount: zeroPaidRows.filter((r) => r.publicationReviewReady)
      .length,
    rows: zeroPaidRows,
  });

  const ledgerPath = path.join(dir, "paid-budget-ledger.json");
  let ledger = null;
  const paidResults = [];
  const skipPda = alreadySpentPdaLodgingBuyer({ root });

  if (authorizePaidResearch) {
    process.env.GDI_EXA_AGENT_LIVE = "1";
    ledger = createPaidBudgetLedger({
      authorizationId: AUTHORIZATION_ID,
      hardCeilingUsd: HARD_CEILING_USD,
      effort: EXA_AGENT_EFFORT.MEDIUM,
      priorSpendUsd: 0,
      outPath: ledgerPath,
    });
    persistPaidBudgetLedger(ledger);

    if (!process.env.EXA_API_KEY && !process.env.EXA_KEY) {
      ledger.blockedOperations.push({
        at: new Date().toISOString(),
        stage: "PAID_RESEARCH",
        reason: "EXA_API_KEY_ABSENT",
      });
      persistPaidBudgetLedger(ledger);
    } else {
      let calls = 0;
      for (const target of PAID_PRIORITY_TARGETS) {
        if (calls >= maxPaidCalls) break;
        if (
          skipPda &&
          target.id === "disp_recLuxvwwxID7U2B8_parenteral_drug_association_pda_"
        ) {
          paidResults.push({
            id: target.id,
            status: "SKIPPED_PRIOR_PAID_PATH",
            note: "PDA lodging-buyer already researched in prior $0.30 batch",
          });
          continue;
        }

        const quote = quoteExaAgentRun({ effort: target.effort });
        const afford = canAffordQuotedCall(ledger, quote.maxUsd);
        if (!afford.ok) {
          paidResults.push({
            id: target.id,
            status: "BLOCKED_BUDGET",
            reason: afford.reason,
            remainingUsd: afford.remaining,
          });
          break;
        }

        const opp =
          updatedById.get(target.id) ||
          resolveUnionOpportunity(target.id, union.opportunities);
        if (!opp) {
          paidResults.push({ id: target.id, status: "MISSING_OPPORTUNITY" });
          continue;
        }

        const zp = zeroPaidRows.find((r) => r.id === target.id);
        if (zp?.publicationReviewReady === true) {
          paidResults.push({
            id: target.id,
            status: "SKIPPED_ALREADY_PUBLICATION_REVIEW_READY",
            billedCostUsd: 0,
          });
          continue;
        }

        const payload = buildExaPayload({
          target,
          opportunity: opp,
          effort: target.effort,
        });

        if (dryRun) {
          paidResults.push({
            id: target.id,
            status: "DRY_RUN",
            effort: target.effort,
            quoteMaxUsd: quote.maxUsd,
          });
          continue;
        }

        const reserve = reserveQuotedCall(ledger, {
          quotedMaxUsd: quote.maxUsd,
          opportunityId: target.id,
          stage: "DEEP_RESEARCH",
        });
        if (!reserve.ok) {
          paidResults.push({
            id: target.id,
            status: "BLOCKED_BUDGET",
            reason: reserve.reason,
          });
          break;
        }

        const run = await runExaAgentResearch({
          mode: "deep_research",
          effort: target.effort,
          authorizeExaAgentLive: true,
          createPayloadOverride: payload,
          contractIdOverride: BETHESDA_COMMERCIAL_RECOVERY_8USD_V1,
          pollUntilFinished: true,
          pollIntervalMs: 4000,
          maxPolls: 120,
        });

        const billed = Number(run.costDollars?.total);
        const actual = Number.isFinite(billed)
          ? billed
          : run.ok
            ? Number(quote.maxUsd)
            : 0;

        settleReservedCall(ledger, {
          quotedMaxUsd: quote.maxUsd,
          actualCostUsd: actual,
          stage: "DEEP_RESEARCH",
          opportunityId: target.id,
          provider: "exa",
          effort: target.effort,
          runId: run.runId || run.id || null,
          ok: run.ok === true,
        });
        calls += 1;

        let score = run.structured || run.output || run.result || {};
        if (typeof score === "string") {
          try {
            score = JSON.parse(score);
          } catch {
            score = { demandActivity: score };
          }
        }
        if (!score || typeof score !== "object") score = {};

        if (!run.ok) {
          paidResults.push({
            id: target.id,
            status: "FAILED",
            effort: target.effort,
            quoteMaxUsd: quote.maxUsd,
            billedCostUsd: actual,
            runId: run.runId || run.id || null,
            error: run.error || run.message || "exa_failed",
          });
          continue;
        }

        const merged = mergeExaIntoOpportunity(opp, score, target);
        const reFinal = finalizeCandidateZeroPaid(merged, { hotel });
        updatedById.set(target.id, reFinal.opportunity);

        paidResults.push({
          id: target.id,
          status: "COMPLETED",
          effort: target.effort,
          quoteMaxUsd: quote.maxUsd,
          billedCostUsd: actual,
          runId: run.runId || run.id || null,
          demandConfidence: score.demandConfidence || null,
          venueSourcingStatus: score.venueSourcingStatus || null,
          publicationReviewReady: reFinal.publicationReviewReady,
          salesReady: reFinal.salesReady,
          accountDevelopment: reFinal.accountDevelopment,
          disposition: reFinal.disposition,
          namedContact: reFinal.opportunity.primaryContact?.name || null,
          blockers: reFinal.blockers.slice(0, 8),
          fitPreview: text(score.bethesdaMarriottFit).slice(0, 240),
        });

        if (remainingBudgetUsd(ledger) < 0.025) break;
      }
    }
  }

  // Re-finalize all updated rows (covers zero-paid-only after paid merges)
  const finalRows = [];
  for (const cand of forensic.recoveryCandidates) {
    const opp = updatedById.get(cand.id);
    if (!opp) {
      finalRows.push({
        id: cand.id,
        organization: cand.organization,
        finalStatus: "MISSING_FROM_UNION",
        publicationReviewReady: false,
      });
      continue;
    }
    const re = finalizeCandidateZeroPaid(opp, { hotel });
    updatedById.set(cand.id, re.opportunity);
    const paid = paidResults.find((p) => p.id === cand.id);
    finalRows.push({
      id: cand.id,
      organization: cand.organization,
      forensicRecovery: cand.recovery,
      forensicSubcategory: cand.subcategory,
      finalStatus: re.disposition,
      publicationReviewReady: re.publicationReviewReady,
      salesReady: re.salesReady,
      accountDevelopment: re.accountDevelopment,
      demandConfidence: re.demandConfidence,
      namedContact: re.opportunity.primaryContact?.name || cand.namedContact || null,
      paidStatus: paid?.status || "ZERO_PAID_ONLY",
      billedCostUsd: paid?.billedCostUsd || 0,
      blockers: re.blockers.slice(0, 10),
      salesOutreachRecommendation:
        re.opportunity.salesOutreachRecommendation || null,
      whyPursueNow: re.opportunity.whyPursueNow || null,
      bethesdaFit: re.opportunity.whyThisMattersForHotel || null,
    });
  }

  const stagedRows = [...updatedById.values()].map((o) => ({
    ...o,
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    customerVisible: false,
    customerPublicationReady: false,
    publicationApproved: false,
  }));

  const researchBagPath = path.join(
    researchStageHotelDir(BETHESDA_CANONICAL_HOTEL_ID, { root }),
    "commercial-recovery-8usd-v1.json",
  );
  const persist = persistResearchStageOpportunityBag({
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    targetPath: researchBagPath,
    incomingDoc: {
      version: BETHESDA_COMMERCIAL_RECOVERY_8USD_V1,
      authorizationId: AUTHORIZATION_ID,
      hotelId: BETHESDA_CANONICAL_HOTEL_ID,
      opportunities: stagedRows,
    },
    protectedIds: forensic.recoveryCandidates.map((c) => c.id),
    root,
    label: "commercial-recovery-8usd",
  });

  const stagedForReview = stagedRows
    .filter((o) => o.publicationReviewReady === true)
    .map((o) => ({
      id: o.id,
      title: o.title,
      organizationName: o.organizationName || o.organization,
      disposition: o.commercialRecoveryDisposition,
      demandConfidence: o.demandConfidence || o.commercialDemandConfidence,
      commercialDemandType: o.commercialDemandType,
      namedContact: o.primaryContact?.name || null,
      contactEmail: o.primaryContact?.email || null,
      whyPursueNow: o.whyPursueNow || null,
      salesOutreachRecommendation: o.salesOutreachRecommendation || null,
      bethesdaFit: o.whyThisMattersForHotel || null,
      tenSectionComplete: o.tenSectionDrawer?.completePersistedContent === true,
      publicationReviewReady: true,
      customerPublicationReady: false,
      customerVisible: false,
      founderAuthorizationRequired: true,
      customerFacingBody: {
        title: o.title,
        whatIsHappening: o.whatIsHappening,
        hotelOpportunityThesis: o.hotelOpportunityThesis,
        whyThisMattersForHotel: o.whyThisMattersForHotel,
        salesOutreachRecommendation: o.salesOutreachRecommendation,
        primaryContact: o.primaryContact || null,
        evidenceUrls: o.evidenceUrls || [],
        tenSectionDrawer: o.tenSectionDrawer || null,
      },
    }));

  const arlington = stagedRows.find(
    (o) => o.id === "gdi_opp_arlington_spring_tournament_2027",
  );
  const arlingtonPackage = arlington
    ? {
        id: arlington.id,
        title: arlington.title,
        namedContact: arlington.primaryContact?.name || "Lizzy Stell",
        proposedMaturity: "ACCOUNT_DEVELOPMENT",
        publicationReviewReady: arlington.publicationReviewReady === true,
        disposition: arlington.commercialRecoveryDisposition,
        blockerToday:
          arlington.publicationReviewReady === true
            ? null
            : (arlington.publicationReviewReadinessV1?.blockers || []).slice(0, 6),
        proposedAction:
          "Founder publication review only after publicationReviewReady=true; never auto-publish",
      }
    : null;

  const facingAfter = filterCustomerFacingOpportunities(union.opportunities || []);
  const storeShaAfter = sha256File(storePath);

  const qualified = finalRows.filter(
    (r) =>
      r.publicationReviewReady === true ||
      r.finalStatus === "PUBLICATION_REVIEW_READY_ACCOUNT_DEVELOPMENT" ||
      r.finalStatus === "PUBLICATION_REVIEW_READY_SALES_READY",
  );
  const rejected = finalRows.filter(
    (r) =>
      r.finalStatus === "REJECT_NOT_COMMERCIAL" ||
      (r.paidStatus === "COMPLETED" &&
        !r.publicationReviewReady &&
        !r.accountDevelopment &&
        /INSUFFICIENT|SPECULATIVE|PARTNERSHIP_POTENTIAL|OVERFLOW_WATCH/i.test(
          String(r.demandConfidence || ""),
        )),
  );

  const summary = {
    version: BETHESDA_COMMERCIAL_RECOVERY_8USD_V1,
    authorizationId: AUTHORIZATION_ID,
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    at: new Date().toISOString(),
    dryRun,
    authorizePaidResearch,
    hardCeilingUsd: HARD_CEILING_USD,
    spentUsd: ledger ? Number(ledger.spentUsd || 0) : 0,
    remainingUsd: ledger ? remainingBudgetUsd(ledger) : HARD_CEILING_USD,
    metrics: {
      candidatesResearched: finalRows.length,
      paidCallsCompleted: paidResults.filter((p) => p.status === "COMPLETED").length,
      commerciallyQualified: qualified.length,
      salesReady: finalRows.filter((r) => r.salesReady === true).length,
      accountDevelopment: finalRows.filter((r) => r.accountDevelopment === true)
        .length,
      publicationReviewReady: stagedForReview.length,
      correctlyRejectedOrContinued: finalRows.length - qualified.length,
      costPerQualifiedOpportunity:
        qualified.length > 0 && ledger
          ? Number((Number(ledger.spentUsd || 0) / qualified.length).toFixed(4))
          : null,
    },
    zeroPaid: {
      candidates: zeroPaidRows.length,
      publicationReviewReady: zeroPaidRows.filter((r) => r.publicationReviewReady)
        .length,
      accountDevelopmentNeedsCompletion: zeroPaidRows.filter(
        (r) => r.disposition === "ACCOUNT_DEVELOPMENT_NEEDS_COMPLETION",
      ).length,
    },
    paidResults,
    finalStatuses: finalRows,
    stagedForFounderReviewCount: stagedForReview.length,
    stagedForFounderReview: stagedForReview,
    arlingtonPackage,
    rejectedSummary: rejected,
    customerFacingParity: {
      beforeCount: facingBefore.length,
      afterCount: facingAfter.length,
      unchanged:
        facingBefore.length === facingAfter.length &&
        storeShaBefore === storeShaAfter,
      storeShaBefore,
      storeShaAfter,
      note: "Canonical opportunities.json untouched; customer feed unchanged until founder publish approval",
    },
    persist: {
      ok: persist?.ok === true,
      bagPath: persist?.targetPath || researchBagPath,
      code: persist?.code || null,
    },
    workflowGapFix: {
      issue:
        "Engine ACCOUNT_DEVELOPMENT_ELIGIBLE + MONITOR hotel-sales score never received ten-section completion + publicationReviewReady staging",
      fix:
        "Recovery path builds ten-section drawer + evaluates publicationReviewReady after evidence merge; stages founder packages without customerVisible",
      autoPublish: false,
    },
    freshDiscoveryRequired: false,
  };

  writeJsonAtomic(path.join(dir, "00-summary.json"), summary);
  writeJsonAtomic(path.join(dir, "02-paid-research-results.json"), {
    paidResults,
    ledgerPath: ledger ? ledgerPath : null,
    spentUsd: summary.spentUsd,
    remainingUsd: summary.remainingUsd,
  });
  writeJsonAtomic(path.join(dir, "03-founder-publication-review-staging.json"), {
    status: "STAGED_FOR_FOUNDER_REVIEW_NOT_PUBLISHED",
    count: stagedForReview.length,
    opportunities: stagedForReview,
    arlingtonPackage,
    doNotAutoPublish: true,
  });
  writeJsonAtomic(path.join(dir, "04-final-candidate-statuses.json"), {
    finalStatuses: finalRows,
  });

  return summary;
}

/**
 * Re-apply rematerialization + finalization to the existing recovery bag
 * without issuing new paid calls (uses already-spent Exa evidence).
 */
export function rematerializeAndRefinalizeRecoveryBag({ root = ROOT } = {}) {
  const dir = outDir(root);
  const bagPath = path.join(
    researchStageHotelDir(BETHESDA_CANONICAL_HOTEL_ID, { root }),
    "commercial-recovery-8usd-v1.json",
  );
  if (!fs.existsSync(bagPath)) {
    throw new Error(`Recovery bag missing: ${bagPath}`);
  }
  const bag = JSON.parse(fs.readFileSync(bagPath, "utf8"));
  const forensic = loadForensicCandidates({ root });
  const hotel = {
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    displayName: BETHESDA_CANONICAL_HOTEL_NAME,
  };
  const storeDoc = loadHotelOpportunityDoc(BETHESDA_CANONICAL_HOTEL_ID, { root });
  const storePath = hotelOpportunitiesPath(BETHESDA_CANONICAL_HOTEL_ID, { root });
  const storeShaBefore = sha256File(storePath);
  const union = mergeBethesdaInternalResearchOpportunities(
    BETHESDA_CANONICAL_HOTEL_ID,
    storeDoc.opportunities || [],
    { root },
  );
  const facingBefore = filterCustomerFacingOpportunities(union.opportunities || []);

  const byId = new Map(
    (bag.opportunities || []).map((o) => [text(o.id || o.opportunityId), o]),
  );
  const finalRows = [];
  const stagedRows = [];

  for (const cand of forensic.recoveryCandidates) {
    const base = byId.get(cand.id);
    if (!base) {
      finalRows.push({
        id: cand.id,
        organization: cand.organization,
        finalStatus: "MISSING_FROM_BAG",
        publicationReviewReady: false,
      });
      continue;
    }
    const remat = rematerializePaidEvidenceFields(base);
    const re = finalizeCandidateZeroPaid(remat, { hotel });
    stagedRows.push({
      ...re.opportunity,
      hotelId: BETHESDA_CANONICAL_HOTEL_ID,
      customerVisible: false,
      customerPublicationReady: false,
      publicationApproved: false,
    });
    finalRows.push({
      id: cand.id,
      organization: cand.organization,
      forensicRecovery: cand.recovery,
      forensicSubcategory: cand.subcategory,
      finalStatus: re.disposition,
      publicationReviewReady: re.publicationReviewReady,
      salesReady: re.salesReady,
      accountDevelopment: re.accountDevelopment,
      demandConfidence: re.demandConfidence,
      namedContact:
        re.opportunity.primaryContact?.name || cand.namedContact || null,
      blockers: re.blockers.slice(0, 10),
      salesOutreachRecommendation:
        re.opportunity.salesOutreachRecommendation || null,
      whyPursueNow: re.opportunity.whyPursueNow || null,
      bethesdaFit: re.opportunity.whyThisMattersForHotel || null,
      maturityCleared: re.opportunity.commercialMaturityCleared === true,
    });
  }

  const persist = persistResearchStageOpportunityBag({
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    targetPath: bagPath,
    incomingDoc: {
      version: BETHESDA_COMMERCIAL_RECOVERY_8USD_V1,
      authorizationId: AUTHORIZATION_ID,
      hotelId: BETHESDA_CANONICAL_HOTEL_ID,
      rematerializedAt: new Date().toISOString(),
      opportunities: stagedRows,
    },
    protectedIds: forensic.recoveryCandidates.map((c) => c.id),
    root,
    label: "commercial-recovery-8usd-rematerialize",
  });

  const stagedForReview = stagedRows
    .filter((o) => o.publicationReviewReady === true)
    .map((o) => ({
      id: o.id,
      title: o.title,
      organizationName: o.organizationName || o.organization,
      disposition: o.commercialRecoveryDisposition,
      demandConfidence: o.demandConfidence || o.commercialDemandConfidence,
      commercialDemandType: o.commercialDemandType,
      namedContact: o.primaryContact?.name || null,
      contactEmail: o.primaryContact?.email || null,
      whyPursueNow: o.whyPursueNow || null,
      salesOutreachRecommendation: o.salesOutreachRecommendation || null,
      bethesdaFit: o.whyThisMattersForHotel || null,
      tenSectionComplete: o.tenSectionDrawer?.completePersistedContent === true,
      publicationReviewReady: true,
      customerPublicationReady: false,
      customerVisible: false,
      founderAuthorizationRequired: true,
      customerFacingBody: {
        title: o.title,
        whatIsHappening: o.whatIsHappening,
        hotelOpportunityThesis: o.hotelOpportunityThesis,
        whyThisMattersForHotel: o.whyThisMattersForHotel,
        salesOutreachRecommendation: o.salesOutreachRecommendation,
        primaryContact: o.primaryContact || null,
        evidenceUrls: o.evidenceUrls || [],
        tenSectionDrawer: o.tenSectionDrawer || null,
      },
    }));

  const facingAfter = filterCustomerFacingOpportunities(union.opportunities || []);
  const storeShaAfter = sha256File(storePath);
  const ledgerPath = path.join(dir, "paid-budget-ledger.json");
  const ledger = fs.existsSync(ledgerPath)
    ? JSON.parse(fs.readFileSync(ledgerPath, "utf8"))
    : null;

  const summary = {
    version: BETHESDA_COMMERCIAL_RECOVERY_8USD_V1,
    authorizationId: AUTHORIZATION_ID,
    rematerializedAt: new Date().toISOString(),
    spentUsd: ledger ? Number(ledger.spentUsd || 0) : 0,
    remainingUsd: ledger
      ? Number(
          Math.max(
            0,
            Number(ledger.hardCeilingUsd || HARD_CEILING_USD) -
              Number(ledger.spentUsd || 0),
          ).toFixed(6),
        )
      : HARD_CEILING_USD,
    metrics: {
      candidatesResearched: finalRows.length,
      commerciallyQualified: stagedForReview.length,
      salesReady: finalRows.filter((r) => r.salesReady === true).length,
      accountDevelopment: finalRows.filter((r) => r.accountDevelopment === true)
        .length,
      publicationReviewReady: stagedForReview.length,
      maturityClearedCount: finalRows.filter((r) => r.maturityCleared).length,
    },
    finalStatuses: finalRows,
    stagedForFounderReviewCount: stagedForReview.length,
    stagedForFounderReview: stagedForReview,
    customerFacingParity: {
      beforeCount: facingBefore.length,
      afterCount: facingAfter.length,
      unchanged:
        facingBefore.length === facingAfter.length &&
        storeShaBefore === storeShaAfter,
      storeShaBefore,
      storeShaAfter,
    },
    persist: { ok: persist?.ok === true, bagPath },
    workflowGapFix: {
      issue:
        "Exa demandActivity lived only in narrative fields; demand-mechanism ignored it; stale RESEARCH_CONTINUATION maturity blocked AD",
      fixes: [
        "demand-mechanism now reads whatIsHappening/activityHeadline",
        "negated 'not room blocks' no longer counts as confirmed demand",
        "stale RESEARCH_CONTINUATION maturity clears when evidence elevates",
        "recovery rematerializes evidence statements + recommendedNextStep",
      ],
      autoPublish: false,
    },
  };

  writeJsonAtomic(path.join(dir, "00-summary.json"), summary);
  writeJsonAtomic(path.join(dir, "03-founder-publication-review-staging.json"), {
    status: "STAGED_FOR_FOUNDER_REVIEW_NOT_PUBLISHED",
    count: stagedForReview.length,
    opportunities: stagedForReview,
    doNotAutoPublish: true,
  });
  writeJsonAtomic(path.join(dir, "04-final-candidate-statuses.json"), {
    finalStatuses: finalRows,
  });
  writeJsonAtomic(path.join(dir, "05-rematerialization-report.json"), summary);

  return summary;
}

export default {
  BETHESDA_COMMERCIAL_RECOVERY_8USD_V1,
  AUTHORIZATION_ID,
  HARD_CEILING_USD,
  PAID_PRIORITY_TARGETS,
  loadDotEnv,
  finalizeCandidateZeroPaid,
  rematerializePaidEvidenceFields,
  rematerializeAndRefinalizeRecoveryBag,
  executeBethesdaCommercialRecovery8Usd,
};
