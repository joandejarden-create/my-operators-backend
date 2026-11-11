/**
 * Local Need-Date campaign ledger (file-based).
 * Isolated from prior research ledgers. Paid spend requires founder GO.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  NEED_DATE_DEMAND_INTELLIGENCE_V1,
  PAID_PROVIDER_BLOCKED,
} from "./need-date-demand-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const DEFAULT_LEDGER_DIR = path.join(
  ROOT,
  "data/group-demand-intelligence/need-date-campaigns",
);

function text(v) {
  return String(v || "").trim();
}

export function needDateCampaignLedgerPath(campaignId, ledgerDir = DEFAULT_LEDGER_DIR) {
  const id = text(campaignId).replace(/[^a-zA-Z0-9_-]/g, "_");
  return path.join(ledgerDir, `${id}.json`);
}

export function createNeedDateCampaignLedger({
  campaignId,
  hotelId,
  needPeriodIds = [],
  selectedWorkflows = [],
  mode = "preview",
  authorizePaidResearch = false,
  budgetCeilingUsd = 0,
  discoveryAllowanceUsd = 0,
  deepResearchAllowanceUsd = 0,
  adaptiveFollowUpAllowanceUsd = 0,
  ledgerDir = DEFAULT_LEDGER_DIR,
} = {}) {
  const id =
    text(campaignId) ||
    `needdate-${new Date().toISOString().replace(/[-:TZ.]/g, "").slice(0, 14)}`;

  const hardCeiling = Number(budgetCeilingUsd) || 0;
  const paidOk = Boolean(authorizePaidResearch) && hardCeiling > 0;

  const ledger = {
    version: NEED_DATE_DEMAND_INTELLIGENCE_V1,
    campaignId: id,
    hotelId: text(hotelId),
    linkedNeedPeriodIds: [...needPeriodIds],
    mode,
    researchStart: new Date().toISOString(),
    researchEnd: null,
    selectedWorkflows: [...selectedWorkflows],
    authorizePaidResearch: paidOk,
    authorizedSpendUsd: paidOk ? hardCeiling : 0,
    budgetCeilingUsd: hardCeiling,
    discoveryAllowanceUsd: Number(discoveryAllowanceUsd) || 0,
    deepResearchAllowanceUsd: Number(deepResearchAllowanceUsd) || 0,
    adaptiveFollowUpAllowanceUsd: Number(adaptiveFollowUpAllowanceUsd) || 0,
    actualSpendUsd: 0,
    reservedUsd: 0,
    newCandidates: 0,
    existingMatches: 0,
    newQualified: 0,
    existingOpportunitiesEnriched: 0,
    duplicatesRejected: 0,
    researchContinuation: 0,
    deliveryStatus: "NOT_STARTED",
    publicationStatus: "NOT_AUTHORIZED",
    runOutcome: null,
    nextReview: null,
    stopConditions: [
      "budget_exhausted",
      "strategies_exhausted",
      "marginal_value_poor",
      "founder_stop",
    ],
    priorLedgerReuseForbidden: true,
    entries: [],
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  };

  if (!paidOk && hardCeiling > 0) {
    ledger.authorizationBlock = PAID_PROVIDER_BLOCKED;
  }

  return ledger;
}

export function assertPaidResearchAllowed(ledger) {
  if (!ledger?.authorizePaidResearch || !(Number(ledger.budgetCeilingUsd) > 0)) {
    const err = new Error(PAID_PROVIDER_BLOCKED.message);
    err.code = PAID_PROVIDER_BLOCKED.code;
    throw err;
  }
}

export function reserveNeedDateSpend(ledger, amountUsd, meta = {}) {
  assertPaidResearchAllowed(ledger);
  const amt = Number(amountUsd) || 0;
  if (amt <= 0) return ledger;
  const nextSpend = Number(ledger.actualSpendUsd || 0) + Number(ledger.reservedUsd || 0) + amt;
  if (nextSpend > Number(ledger.budgetCeilingUsd || 0) + 1e-9) {
    const err = new Error("need_date_budget_hard_stop");
    err.code = "NEED_DATE_BUDGET_HARD_STOP";
    throw err;
  }
  ledger.reservedUsd = Number(ledger.reservedUsd || 0) + amt;
  ledger.entries.push({
    at: new Date().toISOString(),
    type: "reserve",
    amountUsd: amt,
    ...meta,
  });
  ledger.updatedAt = new Date().toISOString();
  return ledger;
}

export function commitNeedDateSpend(ledger, amountUsd, meta = {}) {
  assertPaidResearchAllowed(ledger);
  const amt = Number(amountUsd) || 0;
  ledger.reservedUsd = Math.max(0, Number(ledger.reservedUsd || 0) - amt);
  ledger.actualSpendUsd = Number(ledger.actualSpendUsd || 0) + amt;
  ledger.entries.push({
    at: new Date().toISOString(),
    type: "commit",
    amountUsd: amt,
    ...meta,
  });
  ledger.updatedAt = new Date().toISOString();
  return ledger;
}

export function saveNeedDateCampaignLedger(ledger, ledgerDir = DEFAULT_LEDGER_DIR) {
  fs.mkdirSync(ledgerDir, { recursive: true });
  const p = needDateCampaignLedgerPath(ledger.campaignId, ledgerDir);
  fs.writeFileSync(p, `${JSON.stringify(ledger, null, 2)}\n`);
  return p;
}

export function loadNeedDateCampaignLedger(campaignId, ledgerDir = DEFAULT_LEDGER_DIR) {
  const p = needDateCampaignLedgerPath(campaignId, ledgerDir);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}
