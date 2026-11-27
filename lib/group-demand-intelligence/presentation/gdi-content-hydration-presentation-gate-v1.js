/**
 * Permanent GDI Content Hydration & Presentation Validation gate.
 *
 * Not a sixth research stage — runs as part of Full E2E → STAGED certification.
 * Detects research-sentence titles, taxonomy defaults, missing mandatory
 * commercial fields, contact verification gaps, and cross-surface drift.
 *
 * Does not fabricate data to pass. Optional unavailable fields may be empty.
 *
 * Contract: gdi-content-hydration-presentation-gate-v1
 */

import {
  isResearchParagraphTitle,
  resolveEvidenceBackedCommercialTitle,
} from "./gdi-evidence-backed-commercial-title-v1.js";
import {
  OPPORTUNITY_TYPE_FILTER,
  OPPORTUNITY_TYPE_FILTER_LABEL,
  classifyOpportunityTypeFilter,
} from "../opportunity-type-filter-v1.js";

export const GDI_CONTENT_HYDRATION_PRESENTATION_GATE_V1 =
  "gdi-content-hydration-presentation-gate-v1";

export const CONTENT_FIELD_CLASS = Object.freeze({
  MANDATORY: "MANDATORY",
  CONDITIONAL: "CONDITIONAL",
  OPTIONAL: "OPTIONAL",
});

const SEGMENT_ID_TO_FILTER = Object.freeze({
  corporate_accounts_scaling: OPPORTUNITY_TYPE_FILTER.SCALING_COMPANY,
  associations_conferences_events:
    OPPORTUNITY_TYPE_FILTER.ASSOCIATION_CONFERENCE_GROUP,
  corporate_meetings_incentives:
    OPPORTUNITY_TYPE_FILTER.CORPORATE_MEETINGS_INCENTIVES,
  government_public_sector: OPPORTUNITY_TYPE_FILTER.GOVERNMENT_PUBLIC_SECTOR,
  sports_entertainment: OPPORTUNITY_TYPE_FILTER.SPORTS_ENTERTAINMENT,
  education_scientific: OPPORTUNITY_TYPE_FILTER.EDUCATION_SCIENTIFIC,
  projects_infrastructure: OPPORTUNITY_TYPE_FILTER.PROJECTS_INFRASTRUCTURE,
  social_special_events: OPPORTUNITY_TYPE_FILTER.SOCIAL_SPECIAL_EVENTS,
  local_catering_day_meetings:
    OPPORTUNITY_TYPE_FILTER.LOCAL_CATERING_DAY_MEETINGS,
  corporate_celebrations_private_events:
    OPPORTUNITY_TYPE_FILTER.CORPORATE_CELEBRATIONS_PRIVATE_EVENTS,
  travel_trade_group_intermediaries:
    OPPORTUNITY_TYPE_FILTER.TRAVEL_TRADE_GROUP_INTERMEDIARIES,
  relocation_extended_stay: OPPORTUNITY_TYPE_FILTER.RELOCATION_EXTENDED_STAY,
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function firstText(...vals) {
  for (const v of vals) {
    const s = text(v);
    if (s) return s;
  }
  return "";
}

export function resolveDemandSegmentId(opportunity = {}) {
  const direct = text(
    opportunity.demandSegmentId ||
      opportunity.segmentId ||
      opportunity.researchSegment ||
      opportunity.demandType ||
      "",
  ).toLowerCase();
  if (SEGMENT_ID_TO_FILTER[direct]) return direct;
  const otfc = text(opportunity.opportunityTypeFilterCategory);
  for (const [seg, filter] of Object.entries(SEGMENT_ID_TO_FILTER)) {
    if (filter === otfc) return seg;
  }
  return direct || "";
}

export function resolveOpportunityTypeFilterCategory(opportunity = {}) {
  const baked = text(opportunity.opportunityTypeFilterCategory);
  if (baked && OPPORTUNITY_TYPE_FILTER_LABEL[baked]) return baked;
  const seg = resolveDemandSegmentId(opportunity);
  if (seg && SEGMENT_ID_TO_FILTER[seg]) return SEGMENT_ID_TO_FILTER[seg];
  try {
    return classifyOpportunityTypeFilter(opportunity);
  } catch {
    return OPPORTUNITY_TYPE_FILTER.OTHER_UNCLASSIFIED;
  }
}

function contactName(opportunity = {}) {
  const c =
    opportunity.primaryContact ||
    opportunity.tileContactPreview ||
    opportunity.whoHow?.directBuyer ||
    {};
  return firstText(c.fullName, c.name, c.displayName, opportunity.whoToContactName);
}

function contactTitle(opportunity = {}) {
  const c =
    opportunity.primaryContact ||
    opportunity.tileContactPreview ||
    opportunity.whoHow?.directBuyer ||
    {};
  return firstText(
    c.currentTitle,
    c.professionalTitle,
    c.jobTitle,
    c.title,
    c.role,
    opportunity.contactRole,
  );
}

export function isIncompletePersonName(name = "") {
  const s = text(name);
  if (!s) return true;
  // "Lily Y." / "Suzanne K." style
  if (/^[A-Z][a-z]+\s+[A-Z]\.?$/i.test(s)) return true;
  if (/\b[A-Z]\.$/.test(s) && s.split(/\s+/).length <= 2) return true;
  return false;
}

export function isPollutedProfessionalTitle(title = "") {
  const s = text(title);
  if (!s) return true;
  if (s.startsWith("#")) return true;
  if (/Connector\s*-\s*Advocate/i.test(s)) return true;
  if (/^events$/i.test(s) || /^gala\b/i.test(s)) return true;
  if (s.length > 90) return true;
  if (/exact job title not stated/i.test(s)) return true;
  if (/Role Not Yet Verified/i.test(s)) return true;
  return false;
}

export function hasGenericCommercialFallback(opportunity = {}) {
  const blob = [
    opportunity.whatIsHappening,
    opportunity.summaryWhat,
    opportunity.potentialHotelBusiness,
    opportunity.potentialBusiness,
    opportunity.whyThisHotel,
  ]
    .map(text)
    .join(" ");
  if (/is a dated demand opportunity for/i.test(blob)) return true;
  if (/Public research has not confirmed a contracted room block/i.test(blob)) {
    return "HOTEL_DEMAND_UNVERIFIED_STATEMENT";
  }
  if (/Potential hotel business depends on verified peak rooms/i.test(blob)) {
    return "GENERIC_POTENTIAL_BUSINESS";
  }
  return false;
}

/**
 * Audit one opportunity for content hydration / presentation readiness.
 */
export function auditOpportunityContentHydration(opportunity = {}, opts = {}) {
  const id = text(opportunity.id || opportunity.opportunityId) || "UNKNOWN";
  const title = firstText(opportunity.displayTitle, opportunity.title);
  const findings = [];
  const severity = { blocking: 0, warning: 0, info: 0 };

  function add(level, code, message, fieldClass = CONTENT_FIELD_CLASS.MANDATORY) {
    findings.push({ level, code, message, fieldClass });
    if (level === "blocking") severity.blocking += 1;
    else if (level === "warning") severity.warning += 1;
    else severity.info += 1;
  }

  if (!title || isResearchParagraphTitle(title)) {
    add(
      "blocking",
      "RESEARCH_PARAGRAPH_TITLE",
      "Customer-facing title is missing or is a research sentence",
    );
  }

  const otfc = resolveOpportunityTypeFilterCategory(opportunity);
  const seg = resolveDemandSegmentId(opportunity);
  if (!otfc || otfc === OPPORTUNITY_TYPE_FILTER.OTHER_UNCLASSIFIED) {
    if (!seg) {
      add(
        "blocking",
        "MISSING_DEMAND_CLASSIFICATION",
        "Demand segment / Opportunity Type filter category missing",
      );
    }
  }

  // Detect silent Association Conference default when segment says otherwise
  if (
    seg &&
    SEGMENT_ID_TO_FILTER[seg] &&
    SEGMENT_ID_TO_FILTER[seg] !== OPPORTUNITY_TYPE_FILTER.ASSOCIATION_CONFERENCE_GROUP &&
    otfc === OPPORTUNITY_TYPE_FILTER.ASSOCIATION_CONFERENCE_GROUP
  ) {
    add(
      "blocking",
      "INCORRECT_ASSOCIATION_DEFAULT",
      `Segment ${seg} incorrectly classified as Association Conference`,
    );
  }

  const what = firstText(opportunity.whatIsHappening, opportunity.summaryWhat);
  const potential = firstText(
    opportunity.potentialHotelBusiness,
    opportunity.potentialBusiness,
  );
  const whyHotel = text(opportunity.whyThisHotel);
  if (!what) {
    add("blocking", "MISSING_WHAT_IS_HAPPENING", "Missing What Is Happening");
  }
  if (!potential) {
    add(
      "blocking",
      "MISSING_POTENTIAL_BUSINESS",
      "Missing Potential Business for This Hotel",
    );
  }
  if (!whyHotel) {
    add("warning", "MISSING_WHY_THIS_HOTEL", "Missing Why This Hotel");
  }

  const generic = hasGenericCommercialFallback(opportunity);
  if (generic === true) {
    add("blocking", "GENERIC_NARRATIVE_FALLBACK", "Generic dated-demand narrative");
  } else if (generic) {
    add("warning", String(generic), "Commercial narrative states verification gap");
  }

  const name = contactName(opportunity);
  const role = contactTitle(opportunity);
  if (!name) {
    add("blocking", "MISSING_CONTACT_NAME", "Missing who-to-contact name");
  } else if (isIncompletePersonName(name)) {
    add(
      "warning",
      "INCOMPLETE_CONTACT_NAME",
      `Contact name appears abbreviated: ${name}`,
      CONTENT_FIELD_CLASS.CONDITIONAL,
    );
  }
  if (isPollutedProfessionalTitle(role)) {
    add(
      "warning",
      "CONTACT_TITLE_UNVERIFIED_OR_POLLUTED",
      role
        ? `Professional title not presentation-ready: ${role.slice(0, 80)}`
        : "Professional title missing",
      CONTENT_FIELD_CLASS.CONDITIONAL,
    );
  }

  const salesAction = firstText(
    opportunity.salesActionPillLabel,
    opportunity.salesActionKey,
  );
  if (!salesAction) {
    add(
      "warning",
      "MISSING_SALES_ACTION",
      "Sales Action not hydrated",
      CONTENT_FIELD_CLASS.CONDITIONAL,
    );
  }

  const marketScope =
    opportunity.hotelMarketScopeEligible === true ||
    opportunity.competitiveMarketScopeEligible === true ||
    text(opportunity.marketScopeLabel) ||
    text(opportunity.marketScope);
  if (!marketScope) {
    add(
      "warning",
      "MISSING_MARKET_SCOPE",
      "Market Scope eligibility not hydrated",
      CONTENT_FIELD_CLASS.CONDITIONAL,
    );
  }

  if (opts.requireEvidenceBackedTitleFix) {
    const proposed = resolveEvidenceBackedCommercialTitle(opportunity);
    if (proposed.researchParagraphRemediated) {
      add(
        "info",
        "TITLE_REMEDIATION_AVAILABLE",
        `Evidence-backed title available: ${proposed.displayTitle}`,
        CONTENT_FIELD_CLASS.OPTIONAL,
      );
    }
  }

  const pass = severity.blocking === 0;
  return {
    opportunityId: id,
    organizationName: text(opportunity.organizationName) || null,
    title,
    opportunityTypeFilterCategory: otfc || null,
    demandSegmentId: seg || null,
    pass,
    severity,
    findings,
    certification: pass
      ? "CONTENT_HYDRATION_PASS"
      : "CONTENT_HYDRATION_BLOCKED",
  };
}

/**
 * Audit a hotel inventory (STAGED / candidate package).
 */
export function auditHotelContentHydration(opportunities = [], opts = {}) {
  const rows = (Array.isArray(opportunities) ? opportunities : []).map((o) =>
    auditOpportunityContentHydration(o, opts),
  );
  const blocking = rows.filter((r) => !r.pass);
  const warnings = rows.filter((r) => r.pass && r.severity.warning > 0);
  return {
    contract: GDI_CONTENT_HYDRATION_PRESENTATION_GATE_V1,
    hotelId: opts.hotelId || null,
    hotelName: opts.hotelName || null,
    audited: rows.length,
    passCount: rows.filter((r) => r.pass).length,
    blockedCount: blocking.length,
    warningOnlyCount: warnings.length,
    pass: blocking.length === 0,
    certification: blocking.length === 0
      ? "CONTENT_HYDRATION_PASS"
      : "CONTENT_HYDRATION_BLOCKED",
    rows,
    blockingIds: blocking.map((r) => r.opportunityId),
  };
}

export default {
  GDI_CONTENT_HYDRATION_PRESENTATION_GATE_V1,
  CONTENT_FIELD_CLASS,
  resolveDemandSegmentId,
  resolveOpportunityTypeFilterCategory,
  isIncompletePersonName,
  isPollutedProfessionalTitle,
  hasGenericCommercialFallback,
  auditOpportunityContentHydration,
  auditHotelContentHydration,
};
