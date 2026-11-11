/**
 * Authorized SerpAPI Google discovery for one GDI hotel.
 * Called by the existing full E2E supervisor. It does not publish or write opportunities.
 */

import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { getAccount, serpapiSearch } from "../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import { loadActiveResearchAuthorization } from "./gdi-five-hotel-research-authorization-v1.js";
import { loadSerpApiCanaryAuthorization } from "./gdi-serpapi-canary-authorization-v1.js";
import {
  classifyPageContactForOpportunity,
  readAuthorizedOfficialSource,
  reviewStoredEvidence,
} from "./gdi-live-evidence-yield-v1.js";
import { planDemandSearches } from "./gdi-demand-sourcing-v1.js";
import { classifySource } from "./gdi-buyer-discovery-v1.js";
import { evaluateProspectPublicationEligibility } from "../prospect-publication-eligibility-v1.js";
import { loadLiveResearchCanaryApproval } from "./gdi-founder-approval-v1.js";
import { classifyProviderOutcome } from "./gdi-serpapi-intent-v1.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

export function serpApiUsageLedgerPath() {
  return path.join(root, "data/group-demand-intelligence/gdi-serpapi-canary-v1/usage-ledger-v1.json");
}

const DEMAND = /conference|convention|annual meeting|room block|group lodging|summit|congress|congreso|convenci[oó]n|asamblea|bloque de habitaciones|hospedaje|jornada|simposio|expo/i;
const SKIP_HOST = /google\.|facebook\.|instagram\.|youtube\.|tiktok\.|linkedin\.|twitter\.|wikipedia\.|booking\.|expedia\.|tripadvisor\.|marriott\.com|fiestamericana|youtube|x\.com/i;

const PRIOR_PAGES = {
  recRkGGNKslJptFIL: [
    ["NVAR 2026 Annual Convention and Trade Show", "https://www.nvar.com/convention-2026/"],
    ["VA ABA Annual Conference", "https://virginiaaba.org/annual-conference/"],
    ["Northern Virginia Baptist Association 2026 Annual Session", "https://www.novabaptist.org/2026-annual-session/"],
  ],
  recterKmNPdntCm0v: [
    ["Congreso de la Ciudad de México", "https://www.congresocdmx.gob.mx/convocatorias-1217-2.html"],
    ["Cámara de Comercio de la Ciudad de México", "https://ccmexico.com.mx/asamblea-general-2025-y-toma-de-protesta/"],
  ],
};

const QUERIES = {
  recRkGGNKslJptFIL: {
    hl: "en",
    gl: "us",
    location: "Falls Church, Virginia, United States",
    territory: /falls church|fairfax|fairview|northern virginia|tysons|arlington|mosaic|virginia|washington|dmv/i,
    queries: planDemandSearches({ language: "en", territoryLabel: "Northern Virginia" }).map((row) => row.query),
  },
  recterKmNPdntCm0v: {
    hl: "es",
    gl: "mx",
    location: "Mexico City, Mexico",
    territory: /ciudad de m[eé]xico|cdmx|mexico city|polanco|reforma|chapultepec|m[eé]xico/i,
    queries: planDemandSearches({ language: "es", territoryLabel: "Ciudad de México" }).map((row) => row.query),
  },
};

export function normalizeQueryKey(query) {
  return String(query || "").trim().toLowerCase().replace(/\s+/g, " ");
}

export function interpretAccountDelta(before = {}, after = {}) {
  const planDelta = Number(before.plan_searches_left) - Number(after.plan_searches_left);
  const extraDelta = Number(before.extra_credits) - Number(after.extra_credits);
  const totalDelta = Number(before.total_searches_left) - Number(after.total_searches_left);
  const renewalChanged = String(before.plan_renewal_date || "") !== String(after.plan_renewal_date || "");
  const priceChanged = Number(before.plan_monthly_price) !== Number(after.plan_monthly_price);
  if (renewalChanged || priceChanged) {
    return { stop: true, reason: "SUBSCRIPTION_CHANGED", billedCredits: Number.isFinite(totalDelta) ? totalDelta : null, allocatedUsd: null };
  }
  if (extraDelta > 0) {
    return { stop: true, reason: "EXTRA_CREDIT_CONSUMED", billedCredits: totalDelta, allocatedUsd: null, pool: "extra_credits" };
  }
  if (planDelta === 1 && totalDelta === 1) {
    return { stop: false, reason: "INCLUDED_PLAN_CREDIT", billedCredits: 1, allocatedUsd: 0.025, pool: "included_plan" };
  }
  if (totalDelta === 0) {
    return { stop: false, reason: "NOT_BILLED", billedCredits: 0, allocatedUsd: 0, pool: "not_billed" };
  }
  return { stop: true, reason: "UNEXPLAINED_CREDIT_DELTA", billedCredits: totalDelta, allocatedUsd: null };
}

export function planCreditReservation({ hotelId, query, entries = [], ceiling = 20, allowanceUsd = 0.5, rate = 0.025 } = {}) {
  const queryKey = normalizeQueryKey(query);
  const mine = entries.filter((row) => row.hotelId === hotelId);
  const billed = mine.filter((row) => row.status === "billed").reduce((sum, row) => sum + Number(row.credits || 0), 0);
  const held = mine.filter((row) => row.status === "reserved" || row.status === "executed_pending_settlement").length;
  const same = mine.filter((row) => row.queryKey === queryKey);
  if (same.some((row) => row.status === "billed" || row.status === "cached" || row.status === "executed_pending_settlement")) {
    return { ok: false, reason: "DUPLICATE_QUERY", queryKey, retry: false };
  }
  if (same.some((row) => row.status === "failed" || row.status === "quarantined" || row.status === "reserved" || row.status === "review" || row.confidence === "suspected_consumption" || row.confidence === "unknown")) {
    return { ok: false, reason: "AMBIGUOUS_PRIOR_OUTCOME", queryKey, retry: false };
  }
  if (billed + held + 1 > ceiling) return { ok: false, reason: "CREDIT_CEILING", queryKey };
  const nextAllocated = Math.round((billed + held + 1) * rate * 100) / 100;
  if (nextAllocated > allowanceUsd) return { ok: false, reason: "ALLOCATION_CEILING", queryKey };
  return { ok: true, queryKey, retry: false, creditsReserved: 1 };
}

export function reservedCreditLiability(entries = [], hotelId) {
  let count = 0;
  for (const row of entries) {
    if (row.hotelId !== hotelId) continue;
    if (row.settlementStatus === "confirmed_billed" || row.settlementStatus === "pending_delayed" || row.status === "executed_pending_settlement") count += 1;
    else if (row.status === "billed" && Number(row.credits) > 0) count += Number(row.credits);
    else if (row.status === "reserved") count += 1;
    else if (row.status === "review" && row.httpOk === true && row.searchId && row.deltaReason === "NOT_BILLED") count += 1;
  }
  return count;
}

/**
 * A closed receipt with uncertain usage is a historical exception.
 * Only an unfinished reservation is an active ambiguous request.
 * Age alone never clears either one.
 */
export function classifyAccountingEntry(entry = {}) {
  if (entry.status === "reserved" || entry.status === "sent") {
    return { classification: "ACTIVE_AMBIGUOUS_REQUEST", blocksNewRequests: true, blocksSameQuery: true };
  }
  const planMoved = Number(entry.observedPlanBefore) !== Number(entry.observedPlanAfter);
  const extraMoved = Number(entry.observedExtraBefore) !== Number(entry.observedExtraAfter);
  const pairedNotOneCredit = entry.httpOk === true && entry.searchId && entry.deltaReason && entry.deltaReason !== "INCLUDED_PLAN_CREDIT";
  const failedWithMovement = entry.status === "failed" && (planMoved || extraMoved);
  const uncertainConfidence = entry.confidence === "suspected_consumption" || entry.confidence === "unknown";
  if (entry.status === "review" || uncertainConfidence || pairedNotOneCredit || failedWithMovement || entry.deltaReason === "SUCCESSFUL_SEARCH_ACCOUNT_READ_LAGGED") {
    return {
      classification: "HISTORICAL_UNRESOLVED",
      blocksNewRequests: false,
      blocksSameQuery: entry.status === "review" || entry.status === "failed" || entry.status === "quarantined" || uncertainConfidence,
    };
  }
  if (entry.status === "billed" && entry.deltaReason === "INCLUDED_PLAN_CREDIT") {
    return { classification: "VALID_RECEIPT", blocksNewRequests: false, blocksSameQuery: true };
  }
  return { classification: "CLOSED_OTHER", blocksNewRequests: false, blocksSameQuery: false };
}

/**
 * Live plan the next request must match.
 * A forward settlement reserves a bounded unattributed credit without assigning it to a receipt.
 */
export function accountingAcceptanceHash(terms = {}) {
  const body = {
    hotelId: terms.hotelId,
    approvalId: terms.approvalId,
    checkpointPlanSearchesLeft: terms.checkpointPlanSearchesLeft,
    extraCredits: terms.extraCredits,
    alreadyReservedUnsettledCredits: terms.alreadyReservedUnsettledCredits,
    additionalUnattributedCredits: terms.additionalUnattributedCredits,
    unresolvedSearchIds: [...(terms.unresolvedSearchIds || [])].sort(),
    attributedSearchId: terms.attributedSearchId ?? null,
    cumulativeMaxRequests: terms.cumulativeMaxRequests,
    outboundRequestsAlreadySent: terms.outboundRequestsAlreadySent,
    remainingPrepaidRequests: terms.remainingPrepaidRequests,
    planAllocationUsd: terms.planAllocationUsd,
    incrementalCashUsd: terms.incrementalCashUsd,
    cashPurchase: terms.cashPurchase,
    extraCreditPurchase: terms.extraCreditPurchase,
    stopOnNextUnexplainedMovement: terms.stopOnNextUnexplainedMovement,
  };
  return crypto.createHash("sha256").update(JSON.stringify(body)).digest("hex");
}

export function acceptedAdditionalUnattributedCredit(acceptance, baseline, hotelId, authorization = null) {
  if (!acceptance || acceptance.status !== "AUTHORIZED" || !baseline) return 0;
  if (acceptance.hotelId !== hotelId) return 0;
  if (authorization?.approvalId && acceptance.approvalId !== authorization.approvalId) return 0;
  if (acceptance.acceptanceHash !== accountingAcceptanceHash(acceptance)) return 0;
  if (acceptance.additionalUnattributedCredits !== 1) return 0;
  if (acceptance.alreadyReservedUnsettledCredits !== 1) return 0;
  if (acceptance.attributedSearchId !== null) return 0;
  if (acceptance.cashPurchase !== false || acceptance.extraCreditPurchase !== false) return 0;
  if (acceptance.stopOnNextUnexplainedMovement !== true) return 0;
  if (Number(acceptance.checkpointPlanSearchesLeft) !== Number(baseline.planSearchesLeft)) return 0;
  if (Number(acceptance.extraCredits) !== Number(baseline.extraCredits)) return 0;
  if (Number(baseline.forwardSettlement?.unsettledCredits) !== 1) return 0;
  const ids = [...(acceptance.unresolvedSearchIds || [])].sort().join("|");
  if (ids !== "6adb7b59368070f9fbfaab9b|6adb85cd27e2c1c75aeb000f") return 0;
  return 1;
}

export function expectedForwardPlan(baseline, entries = [], hotelId, acceptance = null, authorization = null) {
  if (!baseline) return { plan: null, extra: null, invalid: true, reason: "CHECKPOINT_MISSING" };
  const forward = baseline.forwardSettlement;
  if (!forward) {
    return { plan: Number(baseline.planSearchesLeft), extra: Number(baseline.extraCredits), invalid: false, exposure: 0 };
  }
  const exposure = Number(forward.unsettledCredits);
  const maxExposure = Number(forward.maxUnsettledCredits ?? 1);
  if (forward.attributedSearchId !== null && forward.attributedSearchId !== undefined) {
    return { plan: null, extra: null, invalid: true, reason: "EXPOSURE_ATTRIBUTED" };
  }
  if (!Number.isInteger(exposure) || exposure < 0 || exposure > maxExposure || maxExposure > 1) {
    return { plan: null, extra: null, invalid: true, reason: "UNBOUNDED_UNSETTLED_EXPOSURE" };
  }
  if (!forward.startedAt || !Number.isFinite(Date.parse(forward.startedAt))) {
    return { plan: null, extra: null, invalid: true, reason: "FORWARD_CHECKPOINT_MISSING" };
  }
  const acceptedAdditional = acceptedAdditionalUnattributedCredit(acceptance, baseline, hotelId, authorization);
  const billedSince = entries.filter((entry) => (
    entry.hotelId === hotelId
    && Date.parse(entry.at) > Date.parse(forward.startedAt)
    && (
      (entry.status === "billed" && Number(entry.credits) === 1 && entry.deltaReason === "INCLUDED_PLAN_CREDIT")
      || (entry.settlementStatus === "confirmed_billed" && entry.settlementEvidence === "ISOLATED_DELAYED_CREDIT")
    )
  )).length;
  return {
    plan: Number(baseline.planSearchesLeft) - exposure - acceptedAdditional - billedSince,
    extra: Number(baseline.extraCredits),
    invalid: false,
    exposure,
    billedSince,
  };
}

export function assessRecoveryBoundary({
  entries = [],
  account = null,
  baseline = null,
  authorization = null,
  now = Date.now(),
  hotelId,
  cumulativeMaxRequests = 100,
  accountingAcceptance = null,
} = {}) {
  const historical = entries.filter((entry) => classifyAccountingEntry(entry).classification === "HISTORICAL_UNRESOLVED");
  const active = entries.filter((entry) => classifyAccountingEntry(entry).classification === "ACTIVE_AMBIGUOUS_REQUEST");
  const sent = entries.filter((entry) => entry.hotelId === hotelId && Number(entry.requestsSent) === 1).length;
  const base = { sent, remaining: Math.max(0, cumulativeMaxRequests - sent), historical: historical.length, active: active.length, founderWaiverRequired: false };
  if (active.length) return { ...base, ok: false, permitNewRequest: false, reason: "ACTIVE_AMBIGUOUS_REQUEST" };
  if (!authorization || authorization.status !== "AUTHORIZED" || authorization.hotelId !== hotelId) {
    return { ...base, ok: false, permitNewRequest: false, reason: "AUTHORIZATION_INVALID" };
  }
  if (!(Date.parse(authorization.expiresAt) > now)) {
    return { ...base, ok: false, permitNewRequest: false, reason: "AUTHORIZATION_EXPIRED" };
  }
  if (sent >= cumulativeMaxRequests) return { ...base, ok: false, permitNewRequest: false, reason: "ALLOWANCE_EXHAUSTED", remaining: 0 };
  const retained = baseline?.retainedDiscrepancy;
  if (!retained || retained.attributedSearchId !== null || retained.observedLaterPlanSearchesLeft == null || !baseline.originAt) {
    return { ...base, ok: false, permitNewRequest: false, reason: "HISTORICAL_DISCREPANCY_NOT_RETAINED" };
  }
  const forwardStartedAt = baseline.forwardSettlement?.startedAt ? Date.parse(baseline.forwardSettlement.startedAt) : NaN;
  const attributedSinceOrigin = entries.filter((entry) => (
    entry.hotelId === hotelId
    && Date.parse(entry.at) > Date.parse(baseline.originAt)
    && !(Number.isFinite(forwardStartedAt) && Date.parse(entry.at) > forwardStartedAt)
    && (
      (entry.status === "billed" && Number(entry.credits) === 1 && entry.deltaReason === "INCLUDED_PLAN_CREDIT")
      || (entry.settlementStatus === "confirmed_billed" && entry.settlementEvidence === "ISOLATED_DELAYED_CREDIT")
    )
  )).length;
  const expectedPlan = Number(retained.observedLaterPlanSearchesLeft) - attributedSinceOrigin;
  if (Number(baseline.planSearchesLeft) !== expectedPlan) {
    return { ...base, ok: false, permitNewRequest: false, reason: "CHECKPOINT_DOES_NOT_ISOLATE_DISCREPANCY" };
  }
  if (!account || account.ok !== true) return { ...base, ok: false, permitNewRequest: false, reason: "ACCOUNT_UNAVAILABLE" };
  if (Number(account.plan_monthly_price) !== 25 || Number(account.searches_per_month) !== 1000) {
    return { ...base, ok: false, permitNewRequest: false, reason: "PLAN_PRICE_CHANGED" };
  }
  const liveExpected = expectedForwardPlan(baseline, entries, hotelId, accountingAcceptance, authorization);
  if (liveExpected.invalid) {
    return { ...base, ok: false, permitNewRequest: false, founderWaiverRequired: true, reason: liveExpected.reason };
  }
  if (Number(account.plan_searches_left) !== liveExpected.plan || Number(account.extra_credits) !== liveExpected.extra) {
    const planDrop = Number(baseline.planSearchesLeft) - Number(account.plan_searches_left);
    const extraSame = Number(account.extra_credits) === Number(baseline.extraCredits);
    const oneCreditReserved = !baseline.forwardSettlement && planDrop === 1 && extraSame;
    return {
      ...base,
      ok: false,
      permitNewRequest: false,
      founderWaiverRequired: !oneCreditReserved && (!extraSame || planDrop > 1),
      boundedOneCredit: oneCreditReserved,
      reason: "UNATTRIBUTED_PROVIDER_MOVEMENT",
    };
  }
  return { ...base, ok: true, permitNewRequest: true, reason: "RECOVERY_BOUNDARY_HOLDS", unsettledCredits: liveExpected.exposure || 0 };
}

function readLedger(file) {
  if (!fs.existsSync(file)) return { entries: [] };
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

function writeLedger(file, doc) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const temp = `${file}.${process.pid}.tmp`;
  fs.writeFileSync(temp, `${JSON.stringify(doc, null, 2)}\n`);
  fs.renameSync(temp, file);
}

function redact(text) {
  return String(text || "")
    .replace(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi, "[redacted-email]")
    .replace(/\+?\d[\d\s().-]{8,}\d/g, "[redacted-phone]")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 280);
}

function organizationFromTitle(title) {
  const cleaned = redact(title).split(/\s+[|–—-]\s+/)[0].trim();
  if (cleaned.length < 4 || cleaned.length > 90) return "";
  if (/google|facebook|youtube|wikipedia/i.test(cleaned)) return "";
  return cleaned;
}

function hostOf(url) {
  try {
    return new URL(url).hostname;
  } catch {
    return "";
  }
}

export async function executeGuardedSerpQuery({
  hotelId,
  query,
  pathway = null,
  workstream = "",
  approvalId,
  approvalQuerySet = [],
  maxRequests = 1,
  allowanceUsd = 0.075,
  language = "en",
  location = "Bethesda, Maryland, United States",
  hl = "en",
  gl = "us",
  ledgerPath,
  recoveryBaseline = null,
  cumulativeMaxRequests = null,
  accountingAcceptance = null,
} = {}) {
  if (!approvalQuerySet.includes(query)) return { ok: false, reason: "QUERY_NOT_APPROVED" };
  const file = ledgerPath || serpApiUsageLedgerPath();
  const ledger = readLedger(file);
  const queryKey = normalizeQueryKey(query);
  const historical = ledger.entries.filter((row) => row.hotelId === hotelId && row.queryKey === queryKey);
  if (historical.some((row) => ["failed", "quarantined", "reserved", "review"].includes(row.status) || row.confidence === "suspected_consumption" || row.confidence === "unknown")) {
    return { ok: false, reason: "AMBIGUOUS_PRIOR_OUTCOME", search: { query, status: "not_sent" } };
  }
  if (ledger.entries.some((row) => row.hotelId === hotelId && (row.status === "reserved" || row.status === "sent"))) {
    return { ok: false, reason: "ACTIVE_AMBIGUOUS_REQUEST", search: { query, status: "not_sent" } };
  }
  if (historical.some((row) => row.status === "executed_pending_settlement" || row.status === "billed")) {
    return { ok: false, reason: "DUPLICATE_QUERY", search: { query, status: "not_sent" } };
  }
  if (cumulativeMaxRequests != null) {
    const sent = ledger.entries.filter((row) => row.hotelId === hotelId && Number(row.requestsSent) === 1).length;
    if (sent >= cumulativeMaxRequests) return { ok: false, reason: "ALLOWANCE_EXHAUSTED", search: { query, status: "not_sent" } };
    if (reservedCreditLiability(ledger.entries, hotelId) + 1 > cumulativeMaxRequests) {
      return { ok: false, reason: "CREDIT_CEILING", search: { query, status: "not_sent" } };
    }
  }
  const approvalEntries = ledger.entries.filter((row) => row.memberstackApprovalId === approvalId && row.hotelId === hotelId);
  const reservation = planCreditReservation({
    hotelId,
    query,
    entries: approvalEntries,
    ceiling: maxRequests,
    allowanceUsd,
    rate: 0.025,
  });
  if (!reservation.ok) return { ok: false, reason: reservation.reason, search: { query, status: "not_sent" } };
  const before = await getAccount();
  if (!before.ok) return { ok: false, reason: "ACCOUNT_UNAVAILABLE", search: { query, status: "not_sent" } };
  if (recoveryBaseline) {
    const expected = expectedForwardPlan(recoveryBaseline, ledger.entries, hotelId, accountingAcceptance, { approvalId });
    if (expected.invalid) return { ok: false, reason: expected.reason, search: { query, status: "not_sent" } };
    if (Number(before.plan_searches_left) !== expected.plan || Number(before.extra_credits) !== expected.extra) {
      return { ok: false, reason: "BASELINE_DRIFT", search: { query, status: "not_sent" } };
    }
  }
  const reserved = {
    executionId: `gdi_req_${crypto.randomBytes(8).toString("hex")}`,
    hotelId,
    queryKey,
    query,
    pathway,
    workstream,
    memberstackApprovalId: approvalId,
    language: hl,
    requestsSent: 1,
    status: "reserved",
    credits: 1,
    at: new Date().toISOString(),
  };
  ledger.entries.push(reserved);
  writeLedger(file, ledger);
  const response = await serpapiSearch({ engine: "google", q: query, num: 8, hl, gl, location }, { timeoutMs: 45000 });
  const after = await getAccount();
  const renewalChanged = String(before.plan_renewal_date || "") !== String(after.plan_renewal_date || "");
  const priceChanged = Number(before.plan_monthly_price) !== Number(after.plan_monthly_price);
  const extraDropped = after.ok && Number(after.extra_credits) < Number(before.extra_credits);
  const delta = after.ok ? interpretAccountDelta(before, after) : { stop: true, reason: "ACCOUNT_UNAVAILABLE" };
  reserved.observedPlanBefore = before.plan_searches_left ?? null;
  reserved.observedPlanAfter = after.plan_searches_left ?? null;
  reserved.observedExtraBefore = before.extra_credits ?? null;
  reserved.observedExtraAfter = after.extra_credits ?? null;
  if (!after.ok || renewalChanged || priceChanged || extraDropped) {
    reserved.status = "review";
    reserved.credits = 0;
    reserved.allocatedUsd = null;
    reserved.searchId = response.search_id || null;
    reserved.httpOk = response.ok === true;
    reserved.deltaReason = !after.ok ? "ACCOUNT_UNAVAILABLE" : extraDropped ? "EXTRA_CREDIT_CONSUMED" : "SUBSCRIPTION_CHANGED";
    writeLedger(file, ledger);
    return { ok: false, reason: reserved.deltaReason, search: { query, searchId: reserved.searchId, status: reserved.status, deltaReason: reserved.deltaReason } };
  }
  const organizations = () => (response.data?.organic_results || []).map((row) => ({
    title: redact(row.title),
    sourceUrl: String(row.link || ""),
    snippet: redact(row.snippet),
    query,
  })).filter((row) => row.sourceUrl && !SKIP_HOST.test(row.sourceUrl));
  if (response.ok && response.search_id && delta.reason === "NOT_BILLED") {
    reserved.executionStatus = "succeeded";
    reserved.settlementStatus = "pending_delayed";
    reserved.status = "executed_pending_settlement";
    reserved.credits = 0;
    reserved.reservedCredits = 1;
    reserved.allocatedUsd = 0.025;
    reserved.pool = "reserved_pending_settlement";
    reserved.deltaReason = "NOT_BILLED";
    reserved.searchId = response.search_id;
    reserved.httpOk = true;
    reserved.confidence = "execution_succeeded_billing_pending";
    reserved.observedHourBefore = before.this_hour_searches ?? null;
    reserved.observedHourAfter = after.this_hour_searches ?? null;
    writeLedger(file, ledger);
    return {
      ok: true,
      settlement: "pending_delayed",
      search: { query, searchId: reserved.searchId, status: "executed_pending_settlement", credits: 0, reservedCredits: 1, allocatedUsd: 0.025, deltaReason: "NOT_BILLED" },
      organizations: organizations(),
    };
  }
  if (!(response.ok && response.search_id) || delta.reason !== "INCLUDED_PLAN_CREDIT") {
    const settled = classifyProviderOutcome({
      responseOk: response.ok === true,
      searchId: response.search_id || "",
      before,
      after,
      timedOut: /abort|timeout/i.test(String(response.error?.message || "")),
    });
    reserved.status = response.ok && response.search_id ? "review" : settled.status;
    reserved.confidence = settled.confidence;
    reserved.credits = 0;
    reserved.suspectedCredits = settled.suspectedCredits;
    reserved.searchId = response.search_id || null;
    reserved.httpOk = response.ok === true;
    reserved.deltaReason = response.ok && response.search_id ? delta.reason : settled.reason;
    writeLedger(file, ledger);
    return { ok: false, reason: reserved.deltaReason, search: { query, searchId: reserved.searchId, status: reserved.status, deltaReason: reserved.deltaReason } };
  }
  reserved.status = "billed";
  reserved.credits = 1;
  reserved.allocatedUsd = 0.025;
  reserved.pool = "included_plan";
  reserved.deltaReason = "INCLUDED_PLAN_CREDIT";
  reserved.searchId = response.search_id;
  reserved.httpOk = true;
  reserved.confidence = "confirmed_receipt";
  reserved.executionStatus = "succeeded";
  reserved.settlementStatus = "confirmed_billed";
  reserved.settlementEvidence = "IMMEDIATE_INCLUDED_PLAN_CREDIT";
  writeLedger(file, ledger);
  return {
    ok: true,
    settlement: "confirmed_billed",
    search: { query, searchId: reserved.searchId, status: "billed", credits: 1, allocatedUsd: 0.025, deltaReason: "INCLUDED_PLAN_CREDIT" },
    organizations: organizations(),
  };
}

export async function runAuthorizedGoogleDiscovery({
  hotelId,
  config = {},
  ledgerPath,
  gates = {},
  queries = null,
  maxSearches = null,
  includePriorPages = true,
} = {}) {
  const research = loadActiveResearchAuthorization(hotelId);
  const canary = loadSerpApiCanaryAuthorization(hotelId);
  if (!canary.ok || !research.ok) {
    return { executed: false, searches: [], organizations: [], creditsConsumed: 0, blocker: canary.reason || research.reason };
  }
  if (gates.publication !== "NOT_AUTHORIZED" || gates.persistence !== "NOT_AUTHORIZED") {
    return { executed: false, searches: [], organizations: [], creditsConsumed: 0, blocker: "PUBLICATION_OR_PERSISTENCE_OPEN" };
  }
  if (canary.parallelEnabled || canary.surfeEnabled || canary.jevEnabled || canary.cashPurchasePermitted) {
    return { executed: false, searches: [], organizations: [], creditsConsumed: 0, blocker: "PROVIDER_SCOPE" };
  }
  const live = loadLiveResearchCanaryApproval();
  if (!live.ok) {
    return { executed: false, searches: [], organizations: [], creditsConsumed: 0, blocker: live.reason };
  }
  const approvedQuery = (live.approval.queries || []).find((row) => row.hotelId === hotelId);
  if (!approvedQuery?.query) {
    return { executed: false, searches: [], organizations: [], creditsConsumed: 0, blocker: "QUERY_NOT_APPROVED" };
  }
  const asked = Array.isArray(queries) && queries.length ? (typeof queries[0] === "string" ? queries[0] : queries[0].query) : approvedQuery.query;
  if (asked !== approvedQuery.query) {
    return { executed: false, searches: [], organizations: [], creditsConsumed: 0, blocker: "QUERY_NOT_APPROVED" };
  }
  const plan = QUERIES[hotelId];
  if (!plan) return { executed: false, searches: [], organizations: [], creditsConsumed: 0, blocker: "HOTEL_NOT_IN_CANARY" };
  const file = ledgerPath || serpApiUsageLedgerPath();
  const ledger = readLedger(file);
  const usedForApproval = ledger.entries.filter((row) => row.memberstackApprovalId === live.approval.approvalId && row.hotelId === hotelId);
  if (usedForApproval.length >= Number(live.approval.maxRequestsPerHotel || 1)) {
    return { executed: false, searches: [], organizations: [], creditsConsumed: 0, blocker: "HOTEL_LIMIT" };
  }
  let reconciled = false;
  for (const entry of ledger.entries) {
    if (entry.hotelId === hotelId && entry.searchId && entry.httpOk === true && entry.status !== "billed") {
      entry.status = "billed";
      entry.credits = 1;
      entry.allocatedUsd = 0.025;
      entry.pool = "included_plan";
      entry.deltaReason = "RECONCILED_SUCCESSFUL_SEARCH";
      reconciled = true;
    }
  }
  if (reconciled) writeLedger(file, ledger);
  const searches = [];
  const organizations = includePriorPages === false ? [] : [
    ...(PRIOR_PAGES[hotelId] || []).map(([organizationName, sourceUrl]) => ({
      hotelId,
      organizationName,
      sourceUrl,
      snippet: "",
      language: plan.hl,
      searchQuery: "prior_authorized_result",
      demandSignalInSnippet: true,
      territoryMatch: true,
      officialEvidence: null,
    })),
  ];
  let stopReason = null;
  const selected = [{ query: approvedQuery.query, pathway: approvedQuery.pathway || null }];
  const limited = selected.slice(0, 1);
  const queue = limited.map((item) => (typeof item === "string" ? { query: item, pathway: null } : item));
  const seenUrls = new Set();

  while (queue.length) {
    const item = queue.shift();
    const query = item.query;
    const reservation = planCreditReservation({
      hotelId,
      query,
      entries: ledger.entries,
      ceiling: canary.creditCeiling,
      allowanceUsd: canary.allocatedAllowanceUsd,
      rate: canary.includedPlanUsdPerCredit,
    });
    if (!reservation.ok) {
      if (reservation.reason === "CREDIT_CEILING" || reservation.reason === "ALLOCATION_CEILING") {
        stopReason = reservation.reason;
        break;
      }
      continue;
    }
    const before = await getAccount();
    if (!before.ok) {
      stopReason = "ACCOUNT_UNAVAILABLE";
      break;
    }
    const reserved = {
      hotelId,
      authorizationId: canary.authorizationId,
      queryKey: reservation.queryKey,
      query,
      pathway: item.pathway || null,
      memberstackApprovalId: live.approval.approvalId,
      workstream: approvedQuery.workstream || "",
      language: approvedQuery.language || plan.hl,
      requestsSent: 1,
      status: "reserved",
      credits: 1,
      at: new Date().toISOString(),
    };
    ledger.entries.push(reserved);
    writeLedger(file, ledger);
    const response = await serpapiSearch({
      engine: "google",
      q: query,
      num: 8,
      hl: plan.hl,
      gl: plan.gl,
      location: plan.location,
    }, { timeoutMs: 45000 });
    const after = await getAccount();
    const renewalChanged = String(before.plan_renewal_date || "") !== String(after.plan_renewal_date || "");
    const priceChanged = Number(before.plan_monthly_price) !== Number(after.plan_monthly_price);
    const extraDropped = after.ok && Number(after.extra_credits) < Number(before.extra_credits);
    const delta = after.ok ? interpretAccountDelta(before, after) : { stop: true, reason: "ACCOUNT_UNAVAILABLE", billedCredits: null, allocatedUsd: null };
    reserved.observedPlanBefore = before.plan_searches_left ?? null;
    reserved.observedPlanAfter = after.plan_searches_left ?? null;
    reserved.observedExtraBefore = before.extra_credits ?? null;
    reserved.observedExtraAfter = after.extra_credits ?? null;
    reserved.observedTotalBefore = before.total_searches_left ?? null;
    reserved.observedTotalAfter = after.total_searches_left ?? null;
    if (!after.ok || renewalChanged || priceChanged || extraDropped) {
      reserved.status = "review";
      reserved.credits = 0;
      reserved.allocatedUsd = null;
      reserved.pool = extraDropped ? "extra_credits" : null;
      reserved.deltaReason = !after.ok ? "ACCOUNT_UNAVAILABLE" : extraDropped ? "EXTRA_CREDIT_CONSUMED" : "SUBSCRIPTION_CHANGED";
      reserved.searchId = response.search_id || null;
      reserved.httpOk = response.ok === true;
      writeLedger(file, ledger);
      stopReason = reserved.deltaReason;
      break;
    }
    if (response.ok && response.search_id) {
      reserved.status = "billed";
      reserved.credits = 1;
      reserved.allocatedUsd = 0.025;
      reserved.pool = "included_plan";
      reserved.deltaReason = delta.reason === "INCLUDED_PLAN_CREDIT" ? "INCLUDED_PLAN_CREDIT" : "SUCCESSFUL_SEARCH_ACCOUNT_READ_LAGGED";
      reserved.searchId = response.search_id;
      reserved.httpOk = true;
      reserved.confidence = delta.reason === "INCLUDED_PLAN_CREDIT" ? "confirmed_receipt" : "confirmed_receipt";
      writeLedger(file, ledger);
      if (delta.reason !== "INCLUDED_PLAN_CREDIT") {
        stopReason = delta.reason;
        break;
      }
    } else {
      const settled = classifyProviderOutcome({
        responseOk: response.ok === true,
        searchId: response.search_id || "",
        before,
        after,
        timedOut: /abort|timeout/i.test(String(response.error?.message || "")),
      });
      reserved.status = settled.status;
      reserved.confidence = settled.confidence;
      reserved.credits = 0;
      reserved.suspectedCredits = settled.suspectedCredits;
      reserved.allocatedUsd = null;
      reserved.searchId = null;
      reserved.httpOk = response.ok === true;
      reserved.deltaReason = settled.reason;
      writeLedger(file, ledger);
      stopReason = settled.reason;
      break;
    }
    writeLedger(file, ledger);
    const organic = (response.data?.organic_results || []).map((row) => ({
      title: redact(row.title),
      url: String(row.link || ""),
      snippet: redact(row.snippet),
      language: plan.hl,
    })).filter((row) => row.url && !SKIP_HOST.test(row.url));
    searches.push({
      query,
      queryKey: reservation.queryKey,
      retry: reservation.retry,
      searchId: reserved.searchId,
      status: reserved.status,
      credits: reserved.credits,
      allocatedUsd: reserved.allocatedUsd,
      deltaReason: reserved.deltaReason,
      results: organic.length,
    });
    for (const row of organic) {
      if (seenUrls.has(row.url)) continue;
      seenUrls.add(row.url);
      const organizationName = organizationFromTitle(row.title);
      if (!organizationName) continue;
      organizations.push({
        hotelId,
        organizationName,
        sourceUrl: row.url,
        snippet: row.snippet,
        language: plan.hl,
        searchQuery: query,
        demandSignalInSnippet: DEMAND.test(`${row.title} ${row.snippet}`),
        territoryMatch: plan.territory.test(`${row.title} ${row.snippet} ${row.url}`),
        officialEvidence: null,
      });
    }
  }

  const official = research.ok;
  const fetched = [];
  if (official) {
    const candidates = organizations.filter((row) => {
      const kind = classifySource(row.sourceUrl, row.organizationName);
      if (["news", "legislature", "directory", "venue_listing", "workforce", "archive"].includes(kind)) return false;
      return row.territoryMatch && (row.demandSignalInSnippet || kind === "official_organization");
    }).slice(0, 12);
    for (const row of candidates) {
      const page = await readAuthorizedOfficialSource(row.sourceUrl, { hotelName: config.displayName, organizationHint: row.organizationName });
      const evidence = page.evidence ? reviewStoredEvidence(page.evidence) : null;
      row.officialEvidence = evidence;
      row.pageStatus = page.status || null;
      fetched.push(row.sourceUrl);
      const transient = {
        id: `canary:${row.organizationName}`,
        organizationName: row.organizationName,
        title: evidence?.eventName || evidence?.title || row.organizationName,
        customerVisible: false,
      };
      const contact = classifyPageContactForOpportunity({ evidence: evidence || {}, opportunity: transient, pageUrl: row.sourceUrl });
      const facing = evaluateProspectPublicationEligibility(transient);
      row.buyerRouteVerified = contact.recovered === true;
      row.hotelFacing = facing.hotelFacing === true && contact.recovered === true;
      const directory = /eventbrite|cvent\.com\/venues|peerspace|northstarmeetings|govdelivery|calendar\.aspx|dossierdeprensa|guiadehoy|cdmx\.info|fxva\.com|archerhotel/i.test(row.sourceUrl);
      row.groupDemandOnOfficialPage = Boolean(evidence && (evidence.eventName || evidence.lodgingEvidence || evidence.eventDate));
      row.stagedCandidate = Boolean(
        !directory &&
        row.territoryMatch &&
        evidence?.eventName &&
        evidence?.eventDate &&
        evidence?.confidence !== "NONE"
      );
    }
  }

  const billedCredits = ledger.entries
    .filter((row) => row.hotelId === hotelId && row.status === "billed")
    .reduce((sum, row) => sum + Number(row.credits || 0), 0);
  const allocatedUsd = Math.round(billedCredits * 0.025 * 1000) / 1000;
  return {
    executed: true,
    hotelId,
    authorizationId: canary.authorizationId,
    language: plan.hl,
    searches: ledger.entries.filter((row) => row.hotelId === hotelId).map((row) => ({
      query: row.query,
      queryKey: row.queryKey,
      retry: false,
      searchId: row.searchId || null,
      status: row.status,
      credits: row.credits,
      allocatedUsd: row.allocatedUsd,
      deltaReason: row.deltaReason,
    })),
    organizations,
    officialPagesFetched: fetched.length,
    creditsConsumed: billedCredits,
    allocatedUsd,
    stopReason,
    stagedCandidates: organizations.filter((row) => row.stagedCandidate).length,
    buyerRoutesVerified: organizations.filter((row) => row.buyerRouteVerified).length,
    hotelFacing: organizations.filter((row) => row.hotelFacing).length,
    publication: gates.publication,
    persistence: gates.persistence,
  };
}
