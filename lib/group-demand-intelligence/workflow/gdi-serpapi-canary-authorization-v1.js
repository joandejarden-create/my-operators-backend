/**
 * Additive SerpAPI Google Search authorization for the five-hotel GDI ledger.
 * The base ledger stays official-page only. This record does not replace it.
 */

import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { isLiveFounderApprover } from "../../hotel-census/hpc-authorized-create-reuse-v1.js";
import {
  fiveHotelResearchLedgerPath,
  validateFiveHotelResearchLedger,
} from "./gdi-five-hotel-research-authorization-v1.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

export const SERPAPI_CANARY_AUTH_VERSION = "gdi-serpapi-canary-authorization-v1";
export const SERPAPI_CANARY_CREDIT_CEILING = 20;
export const SERPAPI_INCLUDED_PLAN_USD_PER_CREDIT = 0.025;
export const SERPAPI_CANARY_ALLOCATED_ALLOWANCE_USD = 0.5;

export const SERPAPI_RENEWAL_VERIFICATION = Object.freeze({
  automaticEarlyRenewal: "OFF",
  source: "founder_supplied_authenticated_serpapi_dashboard_screenshot",
  verifiedOn: "2026-10-22",
  observedSubscription: "Starter",
  observedMonthlyUsd: 25,
  observedPlanSearchesLeft: 365,
  observedExtraCredits: 6289,
  observedRenewalDate: "2026-11-08",
  subscriptionChanged: false,
  note: "The founder supplied authenticated dashboard screenshots. The account API does not expose this control. The screenshot is not the ledger authorization.",
});

const CANARY_HOTELS = Object.freeze([
  {
    hotelId: "recRkGGNKslJptFIL",
    displayName: "Falls Church Marriott Fairview Park",
    authorizationId: "auth_gdi_five_hotel_live_research_v1_recRkGGNKslJptFIL",
    existingHardCapUsd: 25,
  },
  {
    hotelId: "recterKmNPdntCm0v",
    displayName: "Grand Fiesta Americana Chapultepec",
    authorizationId: "auth_gdi_five_hotel_live_research_v1_recterKmNPdntCm0v",
    existingHardCapUsd: 30,
  },
]);

export function serpApiCanaryAmendmentPath() {
  return path.join(root, "data/group-demand-intelligence/gdi-five-hotel-research-authorization-v1/serpapi-canary-amendment-v1.json");
}

export function hashResearchLedger(doc) {
  return crypto.createHash("sha256").update(JSON.stringify(doc)).digest("hex");
}

export function buildSerpApiCanaryAmendment({
  approverIdentity,
  approvedAt,
  baseLedger,
  renewalVerification = SERPAPI_RENEWAL_VERIFICATION,
} = {}) {
  return {
    version: SERPAPI_CANARY_AUTH_VERSION,
    status: "ACTIVE",
    granted: true,
    product: "GDI",
    approverIdentity,
    approvedAt,
    authenticationMechanism: "local_git_identity_matches_consumed_hpc_founder_record",
    baseLedgerSha256: hashResearchLedger(baseLedger),
    baseLedgerPreserved: true,
    officialPagePermissionPreserved: true,
    historicalApprovalsPreserved: true,
    renewalVerification,
    provider: "serpapi",
    engine: "google",
    cashPurchasePermitted: false,
    automaticRenewalChangePermitted: false,
    extraCreditAllocatedUsd: null,
    extraCreditCostRepresentedAsZero: false,
    includedPlanUsdPerCredit: SERPAPI_INCLUDED_PLAN_USD_PER_CREDIT,
    retriesIncludedInCreditCeiling: true,
    parallelEnabled: false,
    surfeEnabled: false,
    jevEnabled: false,
    persistenceAuthorized: false,
    publicationAuthorized: false,
    hotels: CANARY_HOTELS.map((hotel) => ({
      ...hotel,
      creditCeiling: SERPAPI_CANARY_CREDIT_CEILING,
      allocatedAllowanceUsd: SERPAPI_CANARY_ALLOCATED_ALLOWANCE_USD,
      permittedEngines: ["google"],
    })),
  };
}

export function validateSerpApiCanaryAmendment(amendment = {}, baseLedger = null) {
  const errors = [];
  if (amendment.status !== "ACTIVE" || amendment.granted !== true) errors.push("not_granted");
  if (amendment.provider !== "serpapi" || amendment.engine !== "google") errors.push("provider");
  if (!isLiveFounderApprover(amendment.approverIdentity, amendment)) errors.push("approver");
  if (amendment.renewalVerification?.automaticEarlyRenewal !== "OFF") errors.push("renewal");
  if (!amendment.renewalVerification?.source || !amendment.renewalVerification?.verifiedOn) errors.push("renewal_evidence");
  if (amendment.cashPurchasePermitted !== false || amendment.automaticRenewalChangePermitted !== false) errors.push("purchase");
  if (amendment.extraCreditCostRepresentedAsZero !== false || amendment.extraCreditAllocatedUsd != null) errors.push("extra_credit_cost");
  if (amendment.includedPlanUsdPerCredit !== SERPAPI_INCLUDED_PLAN_USD_PER_CREDIT) errors.push("allocation_rate");
  if (amendment.parallelEnabled !== false || amendment.surfeEnabled !== false || amendment.jevEnabled !== false) errors.push("other_providers");
  if (amendment.persistenceAuthorized !== false || amendment.publicationAuthorized !== false) errors.push("publication");
  const rows = Array.isArray(amendment.hotels) ? amendment.hotels : [];
  if (rows.length !== CANARY_HOTELS.length) errors.push("hotel_count");
  for (const expected of CANARY_HOTELS) {
    const row = rows.find((item) => item.hotelId === expected.hotelId);
    if (!row) {
      errors.push(`missing_${expected.hotelId}`);
      continue;
    }
    if (row.authorizationId !== expected.authorizationId) errors.push(`authorization_${expected.hotelId}`);
    if (Number(row.existingHardCapUsd) !== expected.existingHardCapUsd) errors.push(`cap_${expected.hotelId}`);
    if (Number(row.creditCeiling) !== SERPAPI_CANARY_CREDIT_CEILING) errors.push(`ceiling_${expected.hotelId}`);
    if (Number(row.allocatedAllowanceUsd) !== SERPAPI_CANARY_ALLOCATED_ALLOWANCE_USD) errors.push(`allowance_${expected.hotelId}`);
    if (!Array.isArray(row.permittedEngines) || row.permittedEngines.join(",") !== "google") errors.push(`engine_${expected.hotelId}`);
  }
  if (baseLedger) {
    const baseOk = validateFiveHotelResearchLedger(baseLedger);
    if (!baseOk.ok) errors.push("base_ledger");
    if (amendment.baseLedgerSha256 !== hashResearchLedger(baseLedger)) errors.push("base_ledger_hash");
    for (const expected of CANARY_HOTELS) {
      const baseRow = (baseLedger.hotels || []).find((item) => item.hotelId === expected.hotelId);
      if (!baseRow || Number(baseRow.hardCapUsd) !== expected.existingHardCapUsd) errors.push(`base_cap_${expected.hotelId}`);
      if (!baseRow || (baseRow.permittedProviders || []).join(",") !== "official_page") errors.push(`official_page_${expected.hotelId}`);
    }
  }
  return { ok: errors.length === 0, errors };
}

export function canaryGrantForHotel(amendment = {}, hotelId, baseLedger = null) {
  const validation = validateSerpApiCanaryAmendment(amendment, baseLedger);
  if (!validation.ok) return { ok: false, reason: "CANARY_AMENDMENT_INVALID", errors: validation.errors };
  const row = (amendment.hotels || []).find((item) => item.hotelId === hotelId);
  if (!row) return { ok: false, reason: "HOTEL_NOT_IN_CANARY", hotelId };
  return {
    ok: true,
    hotelId,
    authorizationId: row.authorizationId,
    hardCapUsd: row.existingHardCapUsd,
    creditCeiling: row.creditCeiling,
    allocatedAllowanceUsd: row.allocatedAllowanceUsd,
    includedPlanUsdPerCredit: amendment.includedPlanUsdPerCredit,
    engine: "google",
  };
}

export function loadSerpApiCanaryAuthorization(hotelId, opts = {}) {
  const file = opts.amendmentPath || serpApiCanaryAmendmentPath();
  const ledgerFile = opts.ledgerPath || fiveHotelResearchLedgerPath();
  if (!fs.existsSync(file) || !fs.existsSync(ledgerFile)) {
    return { ok: false, reason: "CANARY_AMENDMENT_ABSENT", hotelId };
  }
  const amendment = JSON.parse(fs.readFileSync(file, "utf8"));
  const baseLedger = JSON.parse(fs.readFileSync(ledgerFile, "utf8"));
  const validation = validateSerpApiCanaryAmendment(amendment, baseLedger);
  if (!validation.ok) return { ok: false, reason: "CANARY_AMENDMENT_INVALID", errors: validation.errors, hotelId };
  const row = amendment.hotels.find((item) => item.hotelId === hotelId);
  if (!row) return { ok: false, reason: "HOTEL_NOT_IN_CANARY", hotelId };
  return {
    ok: true,
    hotelId,
    authorizationId: row.authorizationId,
    hardCapUsd: row.existingHardCapUsd,
    creditCeiling: row.creditCeiling,
    allocatedAllowanceUsd: row.allocatedAllowanceUsd,
    includedPlanUsdPerCredit: amendment.includedPlanUsdPerCredit,
    engine: "google",
    renewalVerification: amendment.renewalVerification,
    persistenceAuthorized: false,
    publicationAuthorized: false,
    parallelEnabled: false,
    surfeEnabled: false,
    jevEnabled: false,
    cashPurchasePermitted: false,
  };
}
