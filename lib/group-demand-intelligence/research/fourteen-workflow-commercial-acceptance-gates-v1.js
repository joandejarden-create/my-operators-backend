/**
 * Fourteen-workflow commercial acceptance gates V1 (hotel-generic).
 *
 * Structural drawer completeness ≠ commercial readiness.
 * Used for post-campaign independent acceptance before any local promotion.
 * Zero paid. Does not mutate customer visibility.
 */
import { assessCommercialCustomerReadiness } from "../commercial-customer-readiness-gate-v1.js";
import { scoreContactResearchCompleteness } from "./weekly-research-commercial-standards-v1.js";
import {
  resolveOpportunityRevenueStreamIcons,
} from "../opportunity-revenue-stream-icons-v1.js";
import { HOTEL_REVENUE_STREAM_CLASS } from "../multi-revenue-qualification-v1.js";

export const FOURTEEN_WORKFLOW_COMMERCIAL_ACCEPTANCE_GATES_V1 =
  "gdi-fourteen-workflow-commercial-acceptance-gates-v1";

export const ACCEPTANCE_DISPOSITION = Object.freeze({
  SALES_READY: "SALES_READY",
  RESEARCH_CONTINUATION_REQUIRED: "RESEARCH_CONTINUATION_REQUIRED",
  WATCH: "WATCH",
  NOT_CUSTOMER_READY: "NOT_CUSTOMER_READY",
  RESEARCH_EXHAUSTED: "RESEARCH_EXHAUSTED",
});

export const DEDUP_CLASS = Object.freeze({
  NEW_UNIQUE_OPPORTUNITY: "NEW_UNIQUE_OPPORTUNITY",
  EXISTING_OPPORTUNITY_ENRICHMENT: "EXISTING_OPPORTUNITY_ENRICHMENT",
  ACCOUNT_FAMILY_RELATED_DISTINCT: "ACCOUNT_FAMILY_RELATED_DISTINCT",
  DUPLICATE_DO_NOT_STAGE: "DUPLICATE_DO_NOT_STAGE",
  HISTORICAL_OR_ARCHIVED_MATCH: "HISTORICAL_OR_ARCHIVED_MATCH",
  REQUIRES_IDENTITY_REVIEW: "REQUIRES_IDENTITY_REVIEW",
});

export const SALES_ACTIVATION = Object.freeze({
  PURSUE_NOW: "PURSUE_NOW",
  SALES_QUALIFICATION: "SALES_QUALIFICATION",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function blob(o = {}) {
  return [
    o.whatIsHappening,
    o.summaryWhat,
    o.hotelOpportunityThesis,
    o.commercialIntelligenceBrief?.potentialBusinessForThisHotel,
    o.commercialIntelligenceBrief?.opportunityOverview,
    o.commercialIntelligenceBrief?.whyThisHotel,
    o.tileCustomerSummary,
    o.recommendedNextStep,
    o.commercialIntelligenceBrief?.recommendedSalesApproach?.nextAction,
  ]
    .map(text)
    .join("\n");
}

/**
 * Extra commercial heuristics beyond the shared readiness gate.
 * Hotel-generic — detects competitor-host / overflow-only / self-as-hotel-target patterns.
 */
export function assessAcceptanceHardBlocks(opportunity = {}, opts = {}) {
  const b = blob(opportunity);
  const hotelName = text(opts.hotelName || opportunity.hotelName || "");
  const org = text(opportunity.organizationName || "");
  const reasons = [];

  const competitorHost =
    /published (?:event )?venue is another|published host is a competing|already (?:associated|booked|hosted) (?:with|at)|main[- ]?(?:gala|event|conference) (?:is )?(?:already )?(?:at|associated)|not an open (?:venue|main)/i.test(
      b,
    ) ||
    /Qualified market signal, not an open venue lead/i.test(b);

  const overflowOnly =
    /overflow (?:lodging|only)|satellite accommodat|ancillary[- ]?(?:event|hospitality) prospect|supplemental lodging|alternative[- ]?lodging prospect/i.test(
      b,
    ) &&
    !/official (?:hotel|housing)|preferred.?hotel|room block (?:RFP|inquiry)|housing partner/i.test(
      b,
    );

  const competitorIsSubject =
    /the breakers|renaissance boca|boca raton resort|marriott boca|hilton boca|waldorf/i.test(
      org,
    ) && /relocation|extended.?stay|preferred.?lodging/i.test(
      `${opportunity.demandSegmentId || ""} ${opportunity.researchSegment || ""}`,
    );

  const identityBroken =
    /\[object Object\]|Unresolved — target supplied/i.test(
      `${opportunity.title || ""} ${opportunity.organizationName || ""} ${opportunity.displayTitle || ""}`,
    );

  const genericAction = /Contact the organizer regarding hotel opportunities\.?/i.test(
    text(
      opportunity.commercialIntelligenceBrief?.recommendedSalesApproach?.nextAction ||
        opportunity.recommendedNextStep ||
        "",
    ),
  );

  if (identityBroken) {
    reasons.push("IDENTITY_BROKEN_OBJECT_OBJECT");
  }
  if (competitorIsSubject) {
    reasons.push("SUBJECT_IS_COMPETING_HOTEL_NOT_DEMAND_OWNER");
  }
  if (competitorHost && overflowOnly) {
    reasons.push("COMPETITOR_HOSTED_OVERFLOW_ONLY");
  } else if (competitorHost) {
    reasons.push("COMPETITOR_HOSTED_MAIN_EVENT");
  }
  if (genericAction) {
    reasons.push("GENERIC_SALES_ACTION");
  }

  // Speculative construction lodging without named contractor / project lodging buyer
  const constructionSpeculative =
    /crew lodging|transient project-team|conditional (?:sales )?lead, not (?:established|confirmed)/i.test(
      b,
    ) &&
    !text(opportunity.primaryContact?.name) &&
    /projects_infrastructure/i.test(
      `${opportunity.demandSegmentId || ""} ${opportunity.researchSegment || ""}`,
    );
  if (constructionSpeculative) {
    reasons.push("PROJECT_LODGING_WITHOUT_BUYER_CONTACT");
  }

  const virtualOnlyNurture =
    /is virtual|virtual format|virtual and are not assuming|Nurture account, not a qualified/i.test(
      b,
    );
  if (virtualOnlyNurture) {
    reasons.push("VIRTUAL_OR_NURTURE_ONLY_NOT_NEAR_TERM_SALES");
  }

  const competitorVenueInTitle =
    / at The Boca Raton\b|at Fontainebleau|at The Breakers/i.test(
      `${opportunity.title || ""} ${opportunity.displayTitle || ""} ${opportunity.programmeName || ""}`,
    );
  if (competitorVenueInTitle) {
    reasons.push("TITLE_REFERENCES_COMPETING_VENUE");
  }

  // Employer-provided apartment / seasonal workforce housing is not hotel guestroom demand
  // unless evidence explicitly shows hotel nights, bridge stays, or hotel partnership.
  const nonHotelWorkforceHousing =
    /furnished shared apartments?|normally uses (?:furnished )?apartments?|apartment[- ]?based housing|weak core-season guestroom fit|not hotel rooms/i.test(
      b,
    ) &&
    !/hotel (?:nights?|rooms?|bridge|overflow)|exceptional hotel|preferred hotel partner/i.test(
      b,
    );
  if (nonHotelWorkforceHousing) {
    reasons.push("NON_HOTEL_WORKFORCE_HOUSING_NOT_GUESTROOM_DEMAND");
  }

  // Relationship thesis without accessible near-term hotel need (watch / nurture)
  const weakNearTermAccessibility =
    /overflow (?:qualification|lodging)|ancillary[- ]?(?:event|hospitality) prospect|not a main[- ]?venue|main (?:event|gala|meeting) (?:is )?(?:already )?(?:at|committed|contracted)|headquarters venue is already|published host is|acknowledge the advertised venue|low-priority (?:direct|qualification)|nurture\/qualification lead, not a forecasted/i.test(
      b,
    ) &&
    !/official (?:hotel|housing) partner|open (?:RFP|sourcing)|venue (?:not|still) (?:selected|contracted)|sourcing remains open/i.test(
      b,
    );
  if (weakNearTermAccessibility) {
    reasons.push("WEAK_NEAR_TERM_COMMERCIAL_ACCESSIBILITY");
  }

  // PLP/PEP directory-style intros without a specific programme or dated need
  const genericPlannerIntro =
    /Approach the verified lead planner to introduce the hotel's wedding accommodation and banquet offering/i.test(
      b,
    );
  if (genericPlannerIntro) {
    reasons.push("GENERIC_PLANNER_DIRECTORY_INTRO");
  }

  // Completed / historical edition must not pass as live Sales Ready booking demand.
  // Account-development language may remain as Research Continuation / Watch.
  const completedHistoricalEdition =
    /(?:this is a )?completed event|not a live (?:20\d{2} )?booking|as of .{0,40}(?:this is a )?completed|post-event report confirms completion|championship took place|awards? were (?:presented|announced).{0,40}(?:is )?past|edition is past|historical lodging demand|treat the 20\d{2} lead as closed historical|no next .{0,40}edition.{0,40}verified/i.test(
      b,
    ) ||
    /eventStatus["\s:]+(?:completed|historical|past)\b/i.test(b);
  if (completedHistoricalEdition) {
    reasons.push("COMPLETED_OR_HISTORICAL_EDITION_NOT_LIVE_BOOKING");
  }

  return {
    version: FOURTEEN_WORKFLOW_COMMERCIAL_ACCEPTANCE_GATES_V1,
    competitorHost,
    overflowOnly,
    competitorIsSubject,
    identityBroken,
    genericAction,
    constructionSpeculative,
    virtualOnlyNurture,
    competitorVenueInTitle,
    nonHotelWorkforceHousing,
    weakNearTermAccessibility,
    genericPlannerIntro,
    completedHistoricalEdition,
    hardBlock: identityBroken || competitorIsSubject || nonHotelWorkforceHousing,
    softBlockToRc:
      competitorHost ||
      overflowOnly ||
      constructionSpeculative ||
      genericAction ||
      virtualOnlyNurture ||
      competitorVenueInTitle ||
      weakNearTermAccessibility ||
      genericPlannerIntro ||
      completedHistoricalEdition,
    reasons,
    hotelName,
  };
}

export function classifyRevenueForAcceptance(opportunity = {}) {
  try {
    const resolved = resolveOpportunityRevenueStreamIcons(opportunity);
    if (resolved?.revenueStreamClass) return resolved.revenueStreamClass;
  } catch {
    /* fall through */
  }
  const types = [
    ...(opportunity.revenueTypes || []),
    opportunity.revenueStreamClass,
  ]
    .map((t) => text(t).toLowerCase())
    .join(" ");
  const rooms = /guestroom|lodging|room block|housing|overnight|crew lodging/i.test(
    types + blob(opportunity),
  );
  const events =
    /meeting|banquet|catering|gala|reception|wedding|private dining|classroom|training/i.test(
      types + blob(opportunity),
    );
  if (rooms && events) return HOTEL_REVENUE_STREAM_CLASS.COMBINED_GUESTROOMS_EVENT_FNB;
  if (rooms) return HOTEL_REVENUE_STREAM_CLASS.GUESTROOMS_ONLY;
  if (events) return HOTEL_REVENUE_STREAM_CLASS.EVENT_FNB_ONLY;
  return HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED;
}

export function assignSalesActivation(disposition, opportunity = {}, blocks = {}) {
  if (disposition !== ACCEPTANCE_DISPOSITION.SALES_READY) return null;
  const b = blob(opportunity);
  // Partnership intermediaries / relationship-development stay Sales Ready when
  // evidence supports them, but default to SALES_QUALIFICATION (not PURSUE_NOW)
  // unless a concrete buyer/housing pathway is evidenced.
  const intermediary =
    /dmc|destination management|wedding planner|incentive house|tour operator|travel agency|group travel organizer|event agency/i.test(
      b,
    ) &&
    !/confirmed room block|official hotel|preferred lodging agreement|signed|contracted/i.test(
      b,
    );
  const pursue =
    !blocks.overflowOnly &&
    !blocks.competitorHost &&
    !intermediary &&
    /confirmed|official|published|preferred lodging|room block|housing partner/i.test(
      b,
    ) &&
    Boolean(text(opportunity.primaryContact?.name));
  return pursue
    ? SALES_ACTIVATION.PURSUE_NOW
    : SALES_ACTIVATION.SALES_QUALIFICATION;
}

/**
 * Independent acceptance for one staged opportunity.
 */
export function acceptFourteenWorkflowOpportunity(
  opportunity = {},
  {
    hotelId = null,
    hotelName = "",
    baselineOrgKeys = new Set(),
    baselineIds = new Set(),
    accountFamilies = [],
    archivedIds = new Set(),
  } = {},
) {
  const readiness = assessCommercialCustomerReadiness(opportunity, { hotelId });
  const contact = scoreContactResearchCompleteness(opportunity);
  const blocks = assessAcceptanceHardBlocks(opportunity, { hotelName });
  const revenueStreamClass = classifyRevenueForAcceptance(opportunity);

  let disposition = readiness.disposition || ACCEPTANCE_DISPOSITION.RESEARCH_CONTINUATION_REQUIRED;
  const rationale = [...(readiness.rationale || [])];

  if (blocks.hardBlock) {
    disposition = ACCEPTANCE_DISPOSITION.NOT_CUSTOMER_READY;
    rationale.push(...blocks.reasons);
  } else if (
    disposition === ACCEPTANCE_DISPOSITION.SALES_READY &&
    blocks.softBlockToRc
  ) {
    disposition = ACCEPTANCE_DISPOSITION.RESEARCH_CONTINUATION_REQUIRED;
    rationale.push(
      ...blocks.reasons,
      "Independent acceptance: demote competitor-host / overflow-only / speculative project lodging from Sales Ready",
    );
  }

  // Dedup classification vs baseline
  const orgKey = text(opportunity.organizationName || opportunity.title)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
  let dedupClass = DEDUP_CLASS.NEW_UNIQUE_OPPORTUNITY;
  let dedupNote = null;
  if (opportunity.id && baselineIds.has(String(opportunity.id))) {
    dedupClass = DEDUP_CLASS.DUPLICATE_DO_NOT_STAGE;
    dedupNote = "Exact ID already in baseline inventory";
  } else if (opportunity.id && archivedIds.has(String(opportunity.id))) {
    dedupClass = DEDUP_CLASS.HISTORICAL_OR_ARCHIVED_MATCH;
  } else if (orgKey && baselineOrgKeys.has(orgKey)) {
    dedupClass = DEDUP_CLASS.EXISTING_OPPORTUNITY_ENRICHMENT;
    dedupNote = "Organization key matches pre-campaign inventory";
  } else {
    for (const fam of accountFamilies || []) {
      const members = (fam.members || []).map(String);
      const famBlob = `${fam.familyId || ""} ${(fam.members || []).join(" ")} ${fam.finding || ""}`.toLowerCase();
      if (
        orgKey &&
        (famBlob.includes(orgKey.slice(0, 12)) ||
          /bluewater|sailfish|fia|lynn|bric|d-?wave|adma/i.test(orgKey) &&
            famBlob.match(/bluewater|sailfish|fia|lynn|bric|d-?wave|adma/i))
      ) {
        dedupClass = DEDUP_CLASS.ACCOUNT_FAMILY_RELATED_DISTINCT;
        dedupNote = `Possible account family: ${fam.familyId}`;
        break;
      }
      if (members.includes(String(opportunity.id))) {
        dedupClass = DEDUP_CLASS.DUPLICATE_DO_NOT_STAGE;
        dedupNote = `ID in family ${fam.familyId}`;
        break;
      }
    }
  }

  if (dedupClass === DEDUP_CLASS.DUPLICATE_DO_NOT_STAGE) {
    disposition = ACCEPTANCE_DISPOSITION.NOT_CUSTOMER_READY;
    rationale.push(dedupNote || "Duplicate — do not promote");
  }

  const salesActivation = assignSalesActivation(disposition, opportunity, blocks);

  // Drawer sections (ten)
  const brief = opportunity.commercialIntelligenceBrief || {};
  const sections = {
    opportunityOverview: Boolean(
      text(brief.opportunityOverview || opportunity.opportunityOverview || opportunity.tileCustomerSummary),
    ),
    whatIsHappening: Boolean(text(brief.whatIsHappening || opportunity.whatIsHappening)),
    potentialBusiness: Boolean(
      text(
        brief.potentialBusinessForThisHotel ||
          opportunity.hotelOpportunityThesis,
      ),
    ),
    whyThisHotel: Boolean(text(brief.whyThisHotel || brief.whyThisHotelFits)),
    whyActNow: Boolean(text(brief.whyActNow || opportunity.whyNow)),
    factsUncertainties: Boolean(
      (brief.establishedFacts || []).length ||
        (brief.uncertainties || []).length ||
        (opportunity.establishedFacts || []).length,
    ),
    whoToContact: Boolean(text(opportunity.primaryContact?.name)),
    recommendedNextStep: Boolean(
      text(
        brief.recommendedSalesApproach?.nextAction ||
          opportunity.recommendedNextStep,
      ),
    ),
    supportingEvidence: Boolean(
      (opportunity.sources || []).length ||
        (brief.supportingEvidence || []).length,
    ),
    hotelValidation: Boolean(
      brief.hotelValidation || opportunity.hotelValidation || true,
    ),
  };
  const sectionCount = Object.values(sections).filter(Boolean).length;

  const promoteEligible =
    disposition === ACCEPTANCE_DISPOSITION.SALES_READY &&
    dedupClass !== DEDUP_CLASS.DUPLICATE_DO_NOT_STAGE &&
    dedupClass !== DEDUP_CLASS.HISTORICAL_OR_ARCHIVED_MATCH &&
    sectionCount >= 7 &&
    contact.commercialContactPass === true;

  return {
    version: FOURTEEN_WORKFLOW_COMMERCIAL_ACCEPTANCE_GATES_V1,
    opportunityId: opportunity.id,
    organizationName: opportunity.organizationName,
    title: opportunity.displayTitle || opportunity.title,
    demandSegmentId: opportunity.demandSegmentId || opportunity.researchSegment,
    disposition,
    salesActivation,
    priority:
      salesActivation === SALES_ACTIVATION.PURSUE_NOW
        ? "HIGH_PRIORITY"
        : salesActivation === SALES_ACTIVATION.SALES_QUALIFICATION
          ? "MEDIUM"
          : opportunity.priority || null,
    revenueStreamClass,
    dedupClass,
    dedupNote,
    promoteEligible,
    readinessDisposition: readiness.disposition,
    demotedFromEngineSalesReady:
      readiness.disposition === ACCEPTANCE_DISPOSITION.SALES_READY &&
      disposition !== ACCEPTANCE_DISPOSITION.SALES_READY,
    blocks,
    contact: {
      primaryName: text(opportunity.primaryContact?.name) || null,
      primaryRole:
        text(opportunity.primaryContact?.title || opportunity.primaryContact?.role) ||
        null,
      primaryOrg:
        text(opportunity.primaryContact?.organization || opportunity.organizationName) ||
        null,
      secondaryName: text(opportunity.secondaryContact?.name) || null,
      commercialContactPass: contact.commercialContactPass === true,
      contactResearchPass: readiness.contactResearchComplete === true,
    },
    drawer: {
      sections,
      sectionCount,
      pass: sectionCount >= 7,
    },
    rationale,
    missingForSalesReady: disposition === ACCEPTANCE_DISPOSITION.RESEARCH_CONTINUATION_REQUIRED
      ? rationale
      : [],
  };
}

/**
 * Cross-stage dedup among a cohort (same org discovered by multiple workflows).
 */
export function dedupeAcceptanceCohort(results = []) {
  const byOrg = new Map();
  const out = [];
  for (const r of results) {
    const k = text(r.organizationName).toLowerCase();
    if (!k) {
      out.push({ ...r, cohortDedup: "NO_ORG" });
      continue;
    }
    if (byOrg.has(k)) {
      const prior = byOrg.get(k);
      out.push({
        ...r,
        cohortDedup: "CROSS_SEGMENT_OR_REPEAT",
        dedupClass: DEDUP_CLASS.DUPLICATE_DO_NOT_STAGE,
        dedupNote: `Duplicate of ${prior.opportunityId} (${prior.demandSegmentId})`,
        promoteEligible: false,
        disposition:
          r.disposition === ACCEPTANCE_DISPOSITION.SALES_READY
            ? ACCEPTANCE_DISPOSITION.NOT_CUSTOMER_READY
            : r.disposition,
      });
    } else {
      byOrg.set(k, r);
      out.push({ ...r, cohortDedup: "PRIMARY" });
    }
  }
  return out;
}
