/**
 * SerpAPI account inspection and search-credit authorization for GDI.
 * The account endpoint is not a search. A prepaid credit still needs a
 * hotel authorization that names SerpAPI. The $0.01 placeholder is not a rate.
 */

import fs from "node:fs";
import {
  fiveHotelResearchLedgerPath,
  loadActiveResearchAuthorization,
} from "./gdi-five-hotel-research-authorization-v1.js";
import { SEARCH_QUERY_ESTIMATE_USD } from "./gdi-evidence-acquisition-v2.js";
import { canaryGrantForHotel, loadSerpApiCanaryAuthorization } from "./gdi-serpapi-canary-authorization-v1.js";

export const SERPAPI_ACCOUNT_CONTRACT_VERSION = "gdi-serpapi-account-contract-v1";
export const SERPAPI_ACCOUNT_API_URL = "https://serpapi.com/account-api";
export const SERPAPI_PRICING_URL = "https://serpapi.com/pricing";
export const SERPAPI_PRICING_CHECKED_ON = "2026-10-22";
export const REJECTED_SERPAPI_AUTHORIZATION_RATE_USD = SEARCH_QUERY_ESTIMATE_USD;

export const PUBLISHED_SERPAPI_PLANS = Object.freeze([
  { planId: "free", planName: "Free", monthlyUsd: 0, searchesPerMonth: 250, throughputPerHour: 50 },
  { planId: "starter", planName: "Starter", monthlyUsd: 25, searchesPerMonth: 1000, throughputPerHour: 200 },
  { planId: "developer", planName: "Developer", monthlyUsd: 75, searchesPerMonth: 5000, throughputPerHour: 1000 },
  { planId: "production", planName: "Production", monthlyUsd: 150, searchesPerMonth: 15000, throughputPerHour: 3000 },
  { planId: "bigdata", planName: "Big Data", monthlyUsd: 275, searchesPerMonth: 30000, throughputPerHour: 6000 },
  { planId: "searcher", planName: "Searcher", monthlyUsd: 725, searchesPerMonth: 100000, throughputPerHour: 20000 },
  { planId: "volume", planName: "Volume", monthlyUsd: 1475, searchesPerMonth: 250000, throughputPerHour: 50000 },
  { planId: "infrastructure", planName: "Infrastructure", monthlyUsd: 2750, searchesPerMonth: 500000, throughputPerHour: 100000 },
]);

function planKey(value) {
  return String(value || "").toLowerCase().replace(/[^a-z0-9]+/g, "").replace(/plan$/, "").replace(/v\d+$/, "");
}

export function matchPublishedPlan(account = {}) {
  const name = planKey(account.plan_name);
  const id = planKey(account.plan_id);
  const searches = Number(account.searches_per_month);
  const named = PUBLISHED_SERPAPI_PLANS.filter((plan) => planKey(plan.planName) === name || planKey(plan.planId) === id);
  const matched = named.find((plan) => plan.searchesPerMonth === searches) || null;
  return {
    matched,
    nameMatched: named.length > 0,
    searchesMatched: Number.isFinite(searches) && PUBLISHED_SERPAPI_PLANS.some((plan) => plan.searchesPerMonth === searches),
  };
}

export function accountApiInspectionPermit() {
  return {
    permitted: true,
    chargeableSearch: false,
    creditsConsumed: 0,
    endpoint: "GET /account.json",
    reason: "SerpApi documents the Account API as free of charge and excluded from the monthly search quota.",
    sourceUrl: SERPAPI_ACCOUNT_API_URL,
    pricingUrl: SERPAPI_PRICING_URL,
    pricingCheckedOn: SERPAPI_PRICING_CHECKED_ON,
  };
}

export function classifySerpApiAccount(account = {}) {
  if (!account || account.ok !== true) {
    return {
      ok: false,
      planInferredFromApiKey: false,
      blocker: "SERPAPI_ACCOUNT_UNAVAILABLE",
      rejectedAuthorizationRateUsd: REJECTED_SERPAPI_AUTHORIZATION_RATE_USD,
      rejectedAuthorizationRateUsed: false,
    };
  }
  const published = matchPublishedPlan(account);
  const accountPrice = account.plan_monthly_price == null ? null : Number(account.plan_monthly_price);
  const listPrice = published.matched ? published.matched.monthlyUsd : null;
  const searches = Number(account.searches_per_month);
  const priceComparison = !published.matched
    ? "PLAN_NOT_MATCHED_TO_PUBLISHED_TABLE"
    : accountPrice == null
      ? "ACCOUNT_PRICE_NOT_REPORTED"
      : accountPrice === listPrice
        ? "ACCOUNT_PRICE_MATCHES_PUBLISHED_LIST"
        : "ACCOUNT_PRICE_DIFFERS_FROM_PUBLISHED_LIST";
  const publishedThroughput = published.matched ? published.matched.throughputPerHour : null;
  const accountThroughput = account.account_rate_limit_per_hour == null ? null : Number(account.account_rate_limit_per_hour);
  const throughputComparison = publishedThroughput == null || accountThroughput == null
    ? "THROUGHPUT_NOT_COMPARED"
    : accountThroughput === publishedThroughput
      ? "ACCOUNT_THROUGHPUT_MATCHES_PUBLISHED_LIST"
      : "ACCOUNT_THROUGHPUT_DIFFERS_FROM_PUBLISHED_LIST";
  const subscriptionMonthlyUsd = accountPrice != null ? accountPrice : null;
  const renewalFields = (account.reportedFieldNames || []).filter((name) => /automatic|auto_renew|early_renew|replenish/i.test(name));
  const automaticRenewalReported = renewalFields.length ? account[renewalFields[0]] : null;
  const allocated = subscriptionMonthlyUsd != null && searches > 0
    ? Math.round((subscriptionMonthlyUsd / searches) * 10000) / 10000
    : null;
  return {
    ok: true,
    planInferredFromApiKey: false,
    accountStatus: account.account_status || null,
    planId: account.plan_id || null,
    planName: account.plan_name || null,
    publishedPlan: published.matched,
    priceComparison,
    subscriptionMonthlyUsd,
    subscriptionPriceSource: accountPrice == null ? null : "account_api",
    publishedListMonthlyUsd: listPrice,
    availableSearchCredits: account.total_searches_left ?? null,
    planSearchesLeft: account.plan_searches_left ?? null,
    extraCredits: account.extra_credits ?? null,
    consumedCreditsThisMonth: account.this_month_usage ?? null,
    searchesPerMonth: Number.isFinite(searches) ? searches : null,
    planRenewalDate: account.plan_renewal_date ?? null,
    hourlyThroughputLimit: accountThroughput,
    publishedThroughputPerHour: publishedThroughput,
    throughputComparison,
    automaticRenewalReported,
    automaticRenewalKnown: automaticRenewalReported != null,
    incrementalCashChargePerIncludedSearchUsd: 0,
    incrementalCashBasis: "Included plan credits do not add a separate per-search cash fee. Automatic Early Renewal, if enabled, charges a new subscription period when credits are exhausted. The account API does not report that toggle.",
    allocatedSubscriptionUsdPerSearch: allocated,
    allocatedCostLabel: "ALLOCATED_SUBSCRIPTION_NOT_INCREMENTAL_CASH",
    billingUnit: "1 successful search consumes 1 credit. Failed and cached searches are not counted.",
    rejectedAuthorizationRateUsd: REJECTED_SERPAPI_AUTHORIZATION_RATE_USD,
    rejectedAuthorizationRateUsed: false,
  };
}

export function evaluateSearchCreditAuthorization(hotelId, opts = {}) {
  const active = opts.active || loadActiveResearchAuthorization(hotelId, opts);
  const file = opts.ledgerPath || fiveHotelResearchLedgerPath();
  const doc = opts.ledger || (fs.existsSync(file) ? JSON.parse(fs.readFileSync(file, "utf8")) : null);
  const row = (doc?.hotels || []).find((item) => item.hotelId === hotelId) || null;
  const permitted = row?.permittedProviders || [];
  const serpapiEnabled = doc?.providers?.serpapi?.enabled === true && permitted.includes("serpapi");
  const base = {
    hotelId,
    authorizationId: active.authorizationId || row?.authorizationId || null,
    hardCapUsd: active.hardCapUsd ?? row?.hardCapUsd ?? null,
    permittedProviders: permitted,
    serpapiEnabled,
    callerFlagsIgnored: true,
    incrementalCashChargeUsd: null,
    rejectedAuthorizationRateUsd: REJECTED_SERPAPI_AUTHORIZATION_RATE_USD,
    rejectedAuthorizationRateUsed: false,
    parallelEnabled: false,
    surfeEnabled: false,
    jevEnabled: false,
  };
  if (!active.ok) return { ...base, authorized: false, blocker: active.reason || "RESEARCH_AUTHORIZATION_ABSENT" };
  if (opts.canaryAmendment !== null) {
    const granted = opts.canaryAmendment
      ? canaryGrantForHotel(opts.canaryAmendment, hotelId, opts.ledger || null)
      : loadSerpApiCanaryAuthorization(hotelId, opts);
    if (granted.ok) {
      return {
        ...base,
        authorized: true,
        blocker: null,
        serpapiEnabled: true,
        engine: granted.engine,
        creditCeiling: granted.creditCeiling,
        allocatedAllowanceUsd: granted.allocatedAllowanceUsd,
        authorizationId: granted.authorizationId,
        reason: "additive_canary_amendment",
      };
    }
  }
  if (!serpapiEnabled) {
    return {
      ...base,
      authorized: false,
      blocker: "SEARCH_CREDIT_CONSUMPTION_NOT_IN_AUTHORIZATION_SCOPE",
      reason: "The active authorization permits official-page research. It does not permit a SerpAPI search credit.",
    };
  }
  return {
    ...base,
    authorized: false,
    blocker: "SEARCH_BILLING_UNIT_NOT_BOUND_TO_HOTEL_CAP",
    reason: "SerpAPI is named, but this contract still refuses a search until prepaid credits, the hotel cap, and automatic renewal are bound to a receipt.",
  };
}

export function buildSearchUsageRecord({
  hotelId,
  authorizationId = null,
  query = "",
  executed = false,
} = {}) {
  return {
    provider: "serpapi",
    endpoint: null,
    executed: false,
    searchId: null,
    normalizedQuery: String(query || "").trim().toLowerCase().replace(/\s+/g, " "),
    creditsConsumed: 0,
    incrementalCashChargeUsd: 0,
    allocatedResearchCostUsd: null,
    hotelId: hotelId || null,
    authorizationId,
    retryCount: 0,
    duplicateSkipped: false,
    reason: executed ? "REFUSED_EXECUTED_FLAG" : "NOT_EXECUTED",
  };
}

export function minimumSearchApprovalAction(accountClass = {}) {
  return {
    required: true,
    existingLedgerPreserved: true,
    doNotRerunCurrentRecorder: "node scripts/record-gdi-five-hotel-research-authorization-v1.mjs --confirm-caps reuses the valid official-page ledger and cannot add SerpAPI. validateFiveHotelResearchLedger rejects a ledger that enables it.",
    founderAction: [
      "Confirm in the SerpApi dashboard that Automatic Early Renewal is off. The account API does not report that toggle.",
      "Approve one amendment for Falls Church Marriott Fairview Park (recRkGGNKslJptFIL, auth_gdi_five_hotel_live_research_v1_recRkGGNKslJptFIL) and Grand Fiesta Americana Chapultepec (recterKmNPdntCm0v, auth_gdi_five_hotel_live_research_v1_recterKmNPdntCm0v).",
      "Limit the provider to SerpAPI Google web search. Leave Parallel, Surfe, and Jev disabled.",
      `Use 1 successful search = 1 prepaid credit. Incremental cash is $0 while credits remain and the plan renewal date does not change. Allocated subscription cost is the account monthly price divided by searches per month, currently ${accountClass.allocatedSubscriptionUsdPerSearch == null ? "unpriced until the account price is present" : `$${accountClass.allocatedSubscriptionUsdPerSearch}`}, and that allocated amount counts against the existing hotel cap.`,
      "Reject SERPAPI_USD_PER_SEARCH and $0.01 as the billing rate.",
      "Set a small credit ceiling per hotel, require a receipt with the search id and the account credit delta, skip a repeated query, and allow one retry.",
      "Forbid plan changes, extra-credit purchases, automatic renewal, Airtable writes, and publication.",
    ],
  };
}

export const SERPAPI_CANARY_CREDIT_CEILING = 20;
export const SERPAPI_INCLUDED_PLAN_USD_PER_CREDIT = 0.025;
export const SERPAPI_CANARY_ALLOCATED_ALLOWANCE_USD = 0.5;

export function prepareSerpApiCanaryAmendment() {
  const hotels = [
    {
      hotelId: "recRkGGNKslJptFIL",
      displayName: "Falls Church Marriott Fairview Park",
      authorizationId: "auth_gdi_five_hotel_live_research_v1_recRkGGNKslJptFIL",
      existingHardCapUsd: 25,
      creditCeiling: SERPAPI_CANARY_CREDIT_CEILING,
      allocatedAllowanceUsd: SERPAPI_CANARY_ALLOCATED_ALLOWANCE_USD,
    },
    {
      hotelId: "recterKmNPdntCm0v",
      displayName: "Grand Fiesta Americana Chapultepec",
      authorizationId: "auth_gdi_five_hotel_live_research_v1_recterKmNPdntCm0v",
      existingHardCapUsd: 30,
      creditCeiling: SERPAPI_CANARY_CREDIT_CEILING,
      allocatedAllowanceUsd: SERPAPI_CANARY_ALLOCATED_ALLOWANCE_USD,
    },
  ];
  return {
    version: "gdi-serpapi-discovery-canary-v1",
    status: "NOT_RECORDED",
    granted: false,
    writesActiveLedger: false,
    provider: "serpapi",
    engine: "google",
    officialPagePermissionPreserved: true,
    historicalApprovalsPreserved: true,
    crossHotelTransfer: false,
    cashPurchasePermitted: false,
    automaticRenewalChangePermitted: false,
    retriesIncludedInCreditCeiling: true,
    includedPlanUsdPerCredit: SERPAPI_INCLUDED_PLAN_USD_PER_CREDIT,
    extraCreditAllocatedUsd: null,
    extraCreditCostRepresentedAsZero: false,
    hotels,
  };
}

export function evaluateCanaryExecution(accountClass = {}, opts = {}) {
  const amendment = prepareSerpApiCanaryAmendment();
  const renewalKnown = accountClass.automaticRenewalKnown === true && accountClass.automaticRenewalReported === false;
  const decisions = amendment.hotels.map((hotel) => evaluateSearchCreditAuthorization(hotel.hotelId, opts));
  const searchAuthorized = decisions.every((row) => row.authorized === true);
  let blocker = null;
  if (!renewalKnown) blocker = "AUTOMATIC_EARLY_RENEWAL_NOT_VERIFIED";
  else if (!searchAuthorized) blocker = decisions.find((row) => !row.authorized)?.blocker || "SEARCH_NOT_AUTHORIZED";
  return {
    execute: false,
    searchesSent: 0,
    creditsConsumed: 0,
    blocker: blocker || "CANARY_NOT_RECORDED",
    renewalKnown,
    searchAuthorized,
    callerFlagsIgnored: true,
    amendment,
    decisions,
  };
}
