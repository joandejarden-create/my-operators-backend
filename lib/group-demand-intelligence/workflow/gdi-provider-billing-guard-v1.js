/**
 * Billing guard for GDI research providers.
 * A provider stays disabled when its current charge cannot be verified.
 * Unknown prices are not replaced with guesses.
 */

import { PARALLEL_PRICE_ESTIMATES_USD } from "../parallel/budget-ledger-v1.js";
import { PROVIDER_COST_ESTIMATES_USD, PROVIDER_KIND } from "../research/provider-cost-router-v1.js";
import {
  SURFE_EMAIL_CREDIT_USD_ESTIMATE,
  SURFE_MOBILE_CREDIT_USD_ESTIMATE,
} from "./paid-multisegment-research-v1.js";
import { PUBLIC_FETCH_COST_USD, SEARCH_QUERY_ESTIMATE_USD } from "./gdi-evidence-acquisition-v2.js";

export const PROVIDER_BILLING_GUARD_VERSION = "gdi-provider-billing-guard-v1";

export function verifiedProviderBilling(env = process.env) {
  const explicitSerp = String(env.SERPAPI_USD_PER_SEARCH || "").trim();
  const surfeRouter = PROVIDER_COST_ESTIMATES_USD[PROVIDER_KIND.SURFE];
  return {
    version: PROVIDER_BILLING_GUARD_VERSION,
    official_page: {
      enabledForCharge: true,
      verifiedChargeUsd: PUBLIC_FETCH_COST_USD,
      researchAuthorizationRequired: true,
      reason: "Code defines the provider charge as zero. A research authorization is still required.",
    },
    parallel: {
      enabled: false,
      verifiedChargeUsd: null,
      reason: "PARALLEL_PRICE_ESTIMATES_NOT_RECHECKED",
      asOf: PARALLEL_PRICE_ESTIMATES_USD.asOf,
      sourceUrl: PARALLEL_PRICE_ESTIMATES_USD.sourceUrl,
    },
    surfe: {
      enabled: false,
      verifiedChargeUsd: null,
      reason: "SURFE_PRICE_DISCREPANCY",
      routerCeilingUsd: surfeRouter,
      emailCreditEstimateUsd: SURFE_EMAIL_CREDIT_USD_ESTIMATE,
      mobileCreditEstimateUsd: SURFE_MOBILE_CREDIT_USD_ESTIMATE,
    },
    serpapi: {
      enabled: false,
      verifiedChargeUsd: null,
      reason: explicitSerp ? "SERPAPI_ENV_RATE_NOT_AN_INVOICE" : "SERPAPI_DEFAULT_RATE_REJECTED",
      rejectedDefaultUsd: SEARCH_QUERY_ESTIMATE_USD,
    },
    jev: {
      enabled: false,
      verifiedChargeUsd: null,
      reason: "JEV_PRICE_IS_A_RANGE",
      routerCeilingUsd: PROVIDER_COST_ESTIMATES_USD[PROVIDER_KIND.JEV_ROUTE],
    },
  };
}
