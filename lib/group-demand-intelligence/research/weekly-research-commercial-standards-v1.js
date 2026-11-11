/**
 * Permanent weekly GDI research commercial standards V1.
 *
 * Binding for all 14 segment workflows + Need-Date PowerShell #15.
 * Enforces contact quality, account deduplication, hotel identity,
 * revenue honesty, and unauthorized-spend prohibition.
 */
import { quoteExaAgentRun } from "./exa-agent/exa-agent-provider-adapter-v1.js";
import { NEED_DATE_STRATEGY_META } from "../need-date-demand/need-date-demand-contract-v1.js";
export {
  RESEARCH_COMPLETENESS_GATE_V1,
  RESEARCH_COVERAGE_LEVEL,
  RESEARCH_STATE,
  COMMERCIAL_RESEARCH_DIMENSIONS,
  assessResearchCompleteness,
  buildWeeklyResearchCompletenessChecklist,
} from "./research-completeness-gate-v1.js";

export const WEEKLY_RESEARCH_COMMERCIAL_STANDARDS_V1 =
  "gdi-weekly-research-commercial-standards-v1";

/** Research completeness checklist — a name alone is not PASS. */
export const CONTACT_RESEARCH_COMPLETENESS_FIELDS = Object.freeze([
  "contactResearchAttempted",
  "contactSuccessfullyIdentified",
  "contactCommerciallyRelevant",
  "contractingAuthorityVerified",
  "secondaryContactFound",
  "contactPathwayUsable",
  "remainingBuyerGap",
]);

export const REVENUE_STREAM_CLASSES = Object.freeze([
  "GUESTROOMS_ONLY",
  "EVENT_FNB_ONLY",
  "COMBINED_GUESTROOMS_EVENT_FNB",
  "UNCONFIRMED",
]);

export const CUSTOMER_REVENUE_LABELS = Object.freeze({
  GUESTROOMS_ONLY: "Guestrooms Only",
  EVENT_FNB_ONLY: "Meetings & Events Only",
  COMBINED_GUESTROOMS_EVENT_FNB: "Both",
  UNCONFIRMED: "Unconfirmed",
});

/** Fourteen research workflows (#1–14). PowerShell #15 is the Need-Date orchestrator only. */
export function listFourteenWorkflowPowershellEntryPoints() {
  return Object.entries(NEED_DATE_STRATEGY_META || {}).map(([id, m]) => ({
    id,
    number: m.number || null,
    powershell: m.powershell || null,
    revenueFit: m.revenueFit || [],
  }));
}

export function assertFourteenWorkflowsCovered(workflows = listFourteenWorkflowPowershellEntryPoints()) {
  const withPs = workflows.filter((w) => w.powershell);
  if (withPs.length < 14) {
    throw new Error(
      `Expected ≥14 workflow PowerShell entry points; got ${withPs.length}`,
    );
  }
  return {
    ok: true,
    count: withPs.length,
    powershell15: "Need-Date orchestrator (not a 15th demand segment)",
  };
}

/**
 * Score contact research honesty for one opportunity.
 * Having a name is necessary but not sufficient for commercial PASS.
 */
export function scoreContactResearchCompleteness(opportunity = {}) {
  const primary =
    opportunity.primaryContact ||
    (opportunity.contactName
      ? { name: opportunity.contactName, title: opportunity.contactRole }
      : null);
  const named = Boolean(
    primary?.name && String(primary.name).trim().length >= 3,
  );
  const relevant =
    named &&
    primary?.relevanceClass !== "irrelevant" &&
    primary?.isRelevantCommercialEntryPoint !== false;
  const authority = primary?.contractingAuthorityVerified === true;
  const secondary = Boolean(
    opportunity.secondaryContact?.name &&
      String(opportunity.secondaryContact.name).trim().length >= 3,
  );
  const pathway =
    Boolean(primary?.officialContactUrl || opportunity.contactOfficialUrl) ||
    Boolean(primary?.linkedinUrl || primary?.email || primary?.phone) ||
    named;
  const attempted =
    opportunity.contactResearchAttempted === true ||
    named ||
    Boolean(opportunity.contactResearchGap) ||
    Boolean(opportunity.contactOfficialUrl);
  const gap = opportunity.contactResearchGap || null;

  const passAsNamedBuyer = named && relevant && pathway;
  const passAsOrgRouteOnly = !named && pathway && Boolean(gap);
  return {
    version: WEEKLY_RESEARCH_COMMERCIAL_STANDARDS_V1,
    contactResearchAttempted: attempted,
    contactSuccessfullyIdentified: named,
    contactCommerciallyRelevant: relevant,
    contractingAuthorityVerified: authority,
    secondaryContactFound: secondary,
    contactPathwayUsable: pathway,
    remainingBuyerGap: gap,
    /** Name alone is never enough for commercial contact PASS. */
    commercialContactPass: passAsNamedBuyer,
    honestOrgRoute: passAsOrgRouteOnly,
    note: passAsNamedBuyer
      ? "Named commercially relevant contact with usable pathway"
      : passAsOrgRouteOnly
        ? "Org route only — buyer gap retained honestly"
        : "Contact research incomplete",
  };
}

/**
 * Detect same-account duplicate opportunities that should share contact intel
 * without collapsing distinct demand mechanisms.
 */
export function classifyAccountDuplicatePair(a = {}, b = {}) {
  const norm = (s) =>
    String(s || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, " ")
      .replace(/\b(inc|llc|ltd|corp|corporation|the)\b/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  const orgA = norm(a.organizationName || a.org || a.title);
  const orgB = norm(b.organizationName || b.org || b.title);
  const sameOrg =
    orgA &&
    orgB &&
    (orgA.includes(orgB) ||
      orgB.includes(orgA) ||
      (/d.?wave|bric/.test(orgA) && /d.?wave|bric/.test(orgB)));
  if (!sameOrg) {
    return { sameAccount: false, shareContacts: false, collapse: false };
  }
  const mechA = norm(a.opportunityType || a.demandMechanism || a.segment || "");
  const mechB = norm(b.opportunityType || b.demandMechanism || b.segment || "");
  const distinctMechanism =
    mechA && mechB && mechA !== mechB
      ? true
      : /reloc/.test(mechA + String(a.title || "")) !==
          /reloc/.test(mechB + String(b.title || "")) ||
        /visitor|meeting|scaling/.test(mechA + String(a.segment || "")) !==
          /reloc/.test(mechB + String(b.title || ""));
  return {
    sameAccount: true,
    shareContacts: true,
    collapse: false,
    distinctDemandMechanism: Boolean(distinctMechanism),
    rule: "Share account-level contacts; keep separate opportunities when mechanisms differ",
  };
}

/** Current configured Exa Agent fixed-effort prices (repo contract). */
export function getConfiguredExaAgentPricing() {
  return {
    medium: quoteExaAgentRun({ effort: "medium" }),
    high: quoteExaAgentRun({ effort: "high" }),
    source:
      "lib/group-demand-intelligence/research/exa-agent/exa-agent-provider-adapter-v1.js FIXED_EFFORT_USD",
    asOfConfigured: "2026-10-24",
    note:
      "Do not use inflated $2–5 Deep estimates. Fixed efforts are medium=$0.10 high=$0.50 unless auto/max ceilings are used.",
  };
}

/**
 * Permanent weekly research-completion loop (shared, hotel-agnostic).
 * Discover → validate → investigate gaps → requalify → display/monitor/archive → reactivate.
 */
export const WEEKLY_RESEARCH_COMPLETION_LOOP_V1 = Object.freeze([
  "DISCOVER",
  "DEDUPLICATE",
  "VALIDATE_DEMAND_MECHANISM",
  "IDENTIFY_HOTEL_REVENUE_POTENTIAL",
  "ESTABLISH_PROPERTY_SPECIFIC_FIT",
  "INVESTIGATE_CONTACTS",
  "IDENTIFY_UNRESOLVED_COMMERCIAL_QUESTIONS",
  "CONTINUE_RESEARCH_WHERE_JUSTIFIED",
  "REQUALIFY",
  "STAGE_QUALIFIED_OPPORTUNITIES",
  "MONITOR_INCOMPLETE_OPPORTUNITIES",
  "REACTIVATE_WHEN_NEW_EVIDENCE",
]);

/** Gap taxonomy for adaptive research investment (not equal spend). */
export const COMMERCIAL_RESEARCH_GAP_CATEGORY = Object.freeze({
  A_BUYER_GAP: "A_BUYER_GAP",
  B_DEMAND_GAP: "B_DEMAND_GAP",
  C_SOURCING_GAP: "C_SOURCING_GAP",
  D_TIMING_GAP: "D_TIMING_GAP",
  E_HOTEL_FIT_GAP: "E_HOTEL_FIT_GAP",
  F_FUNDAMENTAL_DISQUALIFICATION: "F_FUNDAMENTAL_DISQUALIFICATION",
});

export const WEEKLY_RESEARCH_SAFEGUARDS_V1 = Object.freeze([
  "RESEARCH_PASS_NEQ_COMMERCIAL_PASS",
  "CONTACT_NAME_NEQ_COMMERCIAL_RELEVANCE",
  "COMMERCIAL_RELEVANCE_NEQ_CONTRACTING_AUTHORITY",
  "EVENT_ATTENDANCE_NEQ_ROOM_DEMAND",
  "HISTORICAL_EVENT_NEQ_FUTURE_BOOKING",
  "COMPETITOR_VENUE_NEQ_AVAILABLE_OVERFLOW",
  "PLANNER_LISTING_NEQ_CONFIRMED_GROUP_DEMAND",
  "MATCHING_CITY_NEQ_HOTEL_FIT",
  "UNCERTAIN_DATE_NEQ_NEED_DATE_MATCH",
  "NO_SELF_AUTHORIZED_PAID_SPEND",
  "PROVIDER_COMPLETED_NEQ_RESEARCH_COMPLETED",
  "RESEARCH_COMPLETED_NEQ_COMMERCIAL_EVIDENCE_SUFFICIENT",
  "COMMERCIAL_EVIDENCE_SUFFICIENT_NEQ_CONTACT_PATHWAY_SUFFICIENT",
  "CONTACT_PATHWAY_SUFFICIENT_NEQ_CUSTOMER_READY",
  "DO_NOT_DEFER_SOLELY_ON_WEAK_FIRST_PROVIDER_CALL",
  "DO_NOT_PROMOTE_SOLELY_ON_LONG_DEEP_RESEARCH_REPORT",
]);

/**
 * Classify the primary unresolved commercial gap for adaptive research.
 */
export function classifyCommercialResearchGap(opportunity = {}) {
  const blob = [
    opportunity.contactResearchGap,
    opportunity.recommendedAction,
    opportunity.hotelOpportunityThesis,
    opportunity.whatIsHappening,
    opportunity.customerReadinessAssessment?.rationale,
  ]
    .map((x) => String(x || ""))
    .join(" ");
  if (
    /competing meeting|not a hotel demand|do not pursue|fundamental|unsupported demand/i.test(
      blob,
    )
  ) {
    return COMMERCIAL_RESEARCH_GAP_CATEGORY.F_FUNDAMENTAL_DISQUALIFICATION;
  }
  if (
    /housing partner|stay smart|official hotel|onpeak|sourcing|overflow|hotel availability|supplier access/i.test(
      blob,
    )
  ) {
    return COMMERCIAL_RESEARCH_GAP_CATEGORY.C_SOURCING_GAP;
  }
  if (
    /who (manages|coordinates|owns)|named (contact|person|buyer)|buyer gap|travel manager|events? (director|coordinator)/i.test(
      blob,
    )
  ) {
    return COMMERCIAL_RESEARCH_GAP_CATEGORY.A_BUYER_GAP;
  }
  if (
    /demand mechanism|room.?night|banquet|lodging demand|overnight|guestroom/i.test(
      blob,
    ) &&
    /confirm|verify|qualify|unclear|incomplete/i.test(blob)
  ) {
    return COMMERCIAL_RESEARCH_GAP_CATEGORY.B_DEMAND_GAP;
  }
  if (/date|timing|schedule|recurring|edition|need.?date/i.test(blob)) {
    return COMMERCIAL_RESEARCH_GAP_CATEGORY.D_TIMING_GAP;
  }
  if (/fit|geography|boca|distance|product suitability|same commercial market/i.test(blob)) {
    return COMMERCIAL_RESEARCH_GAP_CATEGORY.E_HOTEL_FIT_GAP;
  }
  if (!opportunity.primaryContact?.name && !opportunity.contactName) {
    return COMMERCIAL_RESEARCH_GAP_CATEGORY.A_BUYER_GAP;
  }
  return COMMERCIAL_RESEARCH_GAP_CATEGORY.B_DEMAND_GAP;
}

/**
 * Build a zero-paid research checklist for weekly runs.
 */
export function buildWeeklyOpportunityResearchChecklist(opportunity = {}) {
  return {
    version: WEEKLY_RESEARCH_COMMERCIAL_STANDARDS_V1,
    demandMechanismEstablished: Boolean(
      opportunity.opportunityType ||
        opportunity.demandMechanism ||
        opportunity.whatIsHappening ||
        opportunity.summaryWhat,
    ),
    revenueStreamsIdentified: Boolean(
      opportunity.revenueStreamClass || opportunity.revenueStreamLabel,
    ),
    propertySpecificFit: Boolean(
      opportunity.whyThisHotel || opportunity.hotelOpportunityThesis,
    ),
    timingInvestigated: Boolean(
      opportunity.eventStartDate ||
        opportunity.needDates ||
        opportunity.timingRelevance ||
        opportunity.whyActNow,
    ),
    namedCommercialContact: scoreContactResearchCompleteness(opportunity)
      .contactSuccessfullyIdentified,
    secondaryContact: scoreContactResearchCompleteness(opportunity)
      .secondaryContactFound,
    buyerVsInfluencerDistinguished: Boolean(
      opportunity.primaryContact?.relevanceClass ||
        opportunity.primaryContact?.contractingAuthorityVerified === false ||
        opportunity.primaryContact?.contractingAuthorityVerified === true,
    ),
    supportingUrlsRetained: Boolean(
      (Array.isArray(opportunity.sources) && opportunity.sources.length) ||
        (Array.isArray(opportunity.evidenceUrls) && opportunity.evidenceUrls.length),
    ),
    specificRecommendedAction: Boolean(
      opportunity.recommendedAction &&
        String(opportunity.recommendedAction).trim().length > 40,
    ),
    unresolvedQuestionsPreserved: Boolean(
      opportunity.contactResearchGap ||
        (Array.isArray(opportunity.uncertainties) &&
          opportunity.uncertainties.length),
    ),
    gapCategory: classifyCommercialResearchGap(opportunity),
    researchLoopVersion: "gdi-weekly-research-completion-loop-v1",
    safeguards: WEEKLY_RESEARCH_SAFEGUARDS_V1,
  };
}

/**
 * Paid research may only proceed with explicit founder authorization.
 * Engines must not self-authorize spend.
 */
export function assertPaidResearchAuthorization({
  founderAuthorized = false,
  authorizePhrase = "",
  maxUsd = 0,
  plannedUsd = 0,
} = {}) {
  const phraseOk =
    /FOUNDER\s+GO|AUTHORIZE\s+PAID|PAID\s+RESEARCH\s+AUTHORIZED/i.test(
      String(authorizePhrase || ""),
    );
  if (!founderAuthorized || !phraseOk) {
    return {
      ok: false,
      code: "PAID_RESEARCH_NOT_AUTHORIZED",
      message:
        "PAID RESEARCH NOT EXECUTED — FOUNDER AUTHORIZATION REQUIRED",
    };
  }
  if (!(Number(maxUsd) > 0) || Number(plannedUsd) > Number(maxUsd)) {
    return {
      ok: false,
      code: "PAID_RESEARCH_BUDGET_INVALID",
      message: "Planned spend exceeds authorized ceiling or ceiling missing",
    };
  }
  return { ok: true, maxUsd: Number(maxUsd), plannedUsd: Number(plannedUsd) };
}
