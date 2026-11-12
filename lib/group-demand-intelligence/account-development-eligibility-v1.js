/**
 * Account Development eligibility V1
 *
 * Distinct from Sales Ready. Does not auto-elevate to SALES_READY.
 * Does not require a confirmed future event date.
 *
 * Qualifies when:
 * 1. Verified organization identity
 * 2. Named commercially relevant contact OR credible entry point
 * 3. Evidence-grounded hotel-specific relationship potential
 * 4. Realistic sales action
 */

import { looksLikePersonName } from "./secondary-named-contact-v1.js";
import {
  evaluateBuyerCommercialResponsibility,
  evaluateSourceTraceability,
  resolveUsableContactRouteForEligibility,
} from "./commercial-eligibility-contract-v1.js";
import { evaluateRealisticHotelDemandThesis } from "./scaling-publication-qualification-v1.js";
import { normalizeOpportunityForCommercialEligibilityEvaluation } from "./commercial-eligibility-field-normalization-v1.js";
import { classifyCustomerSurfaceOpportunity } from "./customer-surface-revalidation-v1.js";
import { assessAccountDevelopmentHotelFitGate } from "./sales-ready-hotel-fit-gate-v1.js";

export const ACCOUNT_DEVELOPMENT_ELIGIBILITY_V1 =
  "gdi-account-development-eligibility-v1";

export const ACCOUNT_DEVELOPMENT_STATUS = Object.freeze({
  ACCOUNT_DEVELOPMENT: "ACCOUNT_DEVELOPMENT",
  WATCH: "WATCH",
  RESEARCH_CONTINUATION: "RESEARCH_CONTINUATION",
  REJECTED: "REJECTED",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

/**
 * Evaluate Account Development eligibility (never Sales Ready).
 */
export function evaluateAccountDevelopmentEligibility(opportunity = {}, opts = {}) {
  const { opportunity: opp } = normalizeOpportunityForCommercialEligibilityEvaluation(
    opportunity,
    opts
  );

  const gaps = [];
  const org = text(opp.organizationName || opp.company || opp.companyName);
  if (!org) gaps.push("MISSING_ORGANIZATION_IDENTITY");

  const sources = evaluateSourceTraceability(opp);
  if (!sources.ok) gaps.push("SOURCES_INSUFFICIENT");

  const demand = evaluateRealisticHotelDemandThesis(opp);
  const hotelThesis = text(
    opp.whyThisHotel ||
      opp.summaryWhyHotel ||
      opp.hotelOpportunityThesis ||
      opp.hotelFitRationale
  );
  if (!demand.ok && hotelThesis.length < 40) {
    gaps.push("MISSING_HOTEL_RELATIONSHIP_THESIS");
  }

  const buyerPerson = {
    name: text(opp.primaryContact?.name || opp.contactName || ""),
    role: text(
      opp.primaryContact?.role ||
        opp.primaryContact?.title ||
        opp.contactRole ||
        ""
    ),
    title: text(opp.primaryContact?.title || opp.primaryContact?.role || ""),
  };
  const namedOk = looksLikePersonName(buyerPerson.name);
  if (!namedOk) gaps.push("MISSING_NAMED_CONTACT_OR_ENTRY_POINT");

  const responsibility = namedOk
    ? evaluateBuyerCommercialResponsibility(buyerPerson, opp, {
        hotelCommercialAlignment: opts.hotelCommercialAlignment,
      })
    : { ok: false, code: "MISSING_NAMED_RELEVANT_BUYER" };

  // Account Development contact bar is below Sales Ready:
  // named person + non-irrelevant commercial role = credible entry point.
  // Does not require salesReadyContactOk / DIRECT_BUYER.
  // Travel/wedding product & partnership roles remain AD-eligible even when the
  // stricter Sales Ready classifier marks them IRRELEVANT (no Sales Ready elevation).
  const roleText = text(buyerPerson.role || buyerPerson.title);
  const travelWeddingEntryPointRole =
    /wedding|vow|honeymoon|travel|tour|destination|incentive|planner|partnerships?|product manager|groups?\b/i.test(
      roleText
    );
  const adContactOk =
    namedOk &&
    Boolean(roleText) &&
    responsibility.code !== "BUYER_AFFILIATION_MISMATCH" &&
    (responsibility.code !== "BUYER_ROLE_IRRELEVANT" ||
      travelWeddingEntryPointRole);

  if (namedOk && !adContactOk) {
    if (
      responsibility.code === "BUYER_ROLE_IRRELEVANT" &&
      !travelWeddingEntryPointRole
    ) {
      gaps.push("BUYER_ROLE_IRRELEVANT");
    } else if (responsibility.code === "BUYER_AFFILIATION_MISMATCH") {
      gaps.push("BUYER_AFFILIATION_MISMATCH");
    } else {
      gaps.push("BUYER_RESPONSIBILITY_UNSUPPORTED_FOR_AD");
    }
  }

  const route = resolveUsableContactRouteForEligibility(opp, buyerPerson);
  if (!route.ok) gaps.push("MISSING_USABLE_CONTACT_ROUTE");

  const nextStep = text(
    opp.recommendedNextStep || opp.recommendedAction || opp.commercialNextStep
  );
  if (nextStep.length < 24) gaps.push("MISSING_REALISTIC_SALES_ACTION");

  const hotelFitGate = assessAccountDevelopmentHotelFitGate(opp);
  if (!hotelFitGate.ok) {
    const reasonMap = {
      partnership_watch_not_account_development:
        "PARTNERSHIP_WATCH_NOT_ACCOUNT_DEVELOPMENT",
      overflow_watch_without_open_housing_path:
        "OVERFLOW_WATCH_WITHOUT_OPEN_HOUSING_PATH",
      recommended_action_monitor_not_account_development:
        "RECOMMENDED_ACTION_MONITOR_NOT_ACCOUNT_DEVELOPMENT",
      research_continuation_not_account_development:
        "RESEARCH_CONTINUATION_NOT_ACCOUNT_DEVELOPMENT",
      watchlist_watch_title_not_account_development:
        "WATCHLIST_WATCH_TITLE_NOT_ACCOUNT_DEVELOPMENT",
    };
    gaps.push(
      reasonMap[hotelFitGate.reason] ||
        "HOTEL_FIT_NOT_DEFENSIBLE_FOR_ACCOUNT_DEVELOPMENT",
    );
  }

  const surface = classifyCustomerSurfaceOpportunity(opp, {
    hotelId: opts.hotelId || opp.hotelId,
  });
  if (surface.disposition === "PAST_CLOSED") {
    return {
      version: ACCOUNT_DEVELOPMENT_ELIGIBILITY_V1,
      status: ACCOUNT_DEVELOPMENT_STATUS.REJECTED,
      ok: false,
      accountDevelopment: false,
      gaps: ["PAST_CLOSED", ...gaps],
      reasons: ["Historical/closed timing — do not recycle as Account Development"],
      surface,
      demand,
      route,
      responsibility,
      note: "Rejected: past/closed opportunity must not become AD without new future evidence.",
    };
  }

  const hardReject =
    gaps.includes("MISSING_ORGANIZATION_IDENTITY") ||
    gaps.includes("BUYER_ROLE_IRRELEVANT") ||
    gaps.includes("SOURCES_INSUFFICIENT");

  if (hardReject) {
    return {
      version: ACCOUNT_DEVELOPMENT_ELIGIBILITY_V1,
      status: ACCOUNT_DEVELOPMENT_STATUS.REJECTED,
      ok: false,
      accountDevelopment: false,
      gaps,
      surface,
      demand,
      route,
      responsibility,
    };
  }

  const adOk =
    gaps.length === 0 &&
    namedOk &&
    adContactOk &&
    route.ok &&
    sources.ok &&
    (demand.ok || hotelThesis.length >= 40) &&
    nextStep.length >= 24;

  if (adOk) {
    return {
      version: ACCOUNT_DEVELOPMENT_ELIGIBILITY_V1,
      status: ACCOUNT_DEVELOPMENT_STATUS.ACCOUNT_DEVELOPMENT,
      ok: true,
      accountDevelopment: true,
      gaps: [],
      surface,
      demand,
      route,
      responsibility,
      hotelFitGate,
      note: "Account Development only — not Sales Ready; no auto-elevation.",
      doesNotElevateToSalesReady: true,
    };
  }

  // Soft fails with org + named contact → Watch (monitor relationship)
  if (org && namedOk && sources.ok) {
    return {
      version: ACCOUNT_DEVELOPMENT_ELIGIBILITY_V1,
      status: ACCOUNT_DEVELOPMENT_STATUS.WATCH,
      ok: false,
      accountDevelopment: false,
      gaps,
      surface,
      demand,
      route,
      responsibility,
      hotelFitGate,
      note: "Commercially interesting but incomplete for Account Development — Watch.",
    };
  }

  return {
    version: ACCOUNT_DEVELOPMENT_ELIGIBILITY_V1,
    status: ACCOUNT_DEVELOPMENT_STATUS.RESEARCH_CONTINUATION,
    ok: false,
    accountDevelopment: false,
    gaps,
    surface,
    demand,
    route,
    responsibility,
    hotelFitGate,
  };
}
