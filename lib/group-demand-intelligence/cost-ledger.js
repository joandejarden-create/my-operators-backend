/**
 * Group Demand Intelligence — research cost ledger + escalation helpers.
 * GDI Webhound hard cap is product-owned ($15 pilot) and independent of
 * Hotel Intelligence Research Center's $5 provider pilot cap.
 */

import {
  GDI_WEBHOUND_HARD_CAP_USD,
  RESEARCH_LEVELS_GDI,
} from "./claim-types.js";
import { isExternalResearchEnabled } from "../hotel-intelligence/research/policy.js";

export function createEmptyCostLedger() {
  return {
    totalUsd: 0,
    webhoundUsd: 0,
    ampfyUsd: 0,
    apifyUsd: 0,
    serpApiUsd: 0,
    llmUsd: 0,
    standardSearchUsd: 0,
    otherUsd: 0,
    queryCount: 0,
    providerCallCount: 0,
    webhoundCalls: [],
    ampfyCalls: [],
    apifyCalls: [],
    escalationLog: [],
    incrementalRoi: [],
  };
}

/**
 * @param {NodeJS.ProcessEnv} [env]
 * @returns {number}
 */
export function getWebhoundHardCapUsd(env = process.env) {
  const raw = Number(env.GDI_WEBHOUND_HARD_CAP_USD);
  if (Number.isFinite(raw) && raw > 0) {
    return Math.min(raw, GDI_WEBHOUND_HARD_CAP_USD);
  }
  return GDI_WEBHOUND_HARD_CAP_USD;
}

export function canSpendWebhound(ledger, additionalUsd = 0) {
  const cap = getWebhoundHardCapUsd();
  const next = Number(ledger.webhoundUsd || 0) + Number(additionalUsd || 0);
  return next <= cap + 1e-9;
}

/**
 * Record a Webhound call. Throws if it would exceed the hard cap.
 */
export function recordWebhoundSpend(ledger, entry) {
  const cost = Number(entry.costUsd || 0);
  if (!canSpendWebhound(ledger, cost)) {
    const err = new Error("gdi_webhound_hard_cap_exceeded");
    err.code = "gdi_webhound_hard_cap_exceeded";
    err.capUsd = getWebhoundHardCapUsd();
    err.currentUsd = ledger.webhoundUsd;
    err.attemptedUsd = cost;
    throw err;
  }
  ledger.webhoundUsd = Number((ledger.webhoundUsd + cost).toFixed(4));
  ledger.totalUsd = Number((ledger.totalUsd + cost).toFixed(4));
  ledger.providerCallCount += 1;
  ledger.webhoundCalls.push({
    ...entry,
    costUsd: cost,
    recordedAt: new Date().toISOString(),
    level: RESEARCH_LEVELS_GDI.L5_WEBHOUND,
  });
  return ledger;
}

export function recordProviderSpend(ledger, provider, costUsd, meta = {}) {
  const cost = Number(costUsd || 0);
  const key =
    provider === "ampfy"
      ? "ampfyUsd"
      : provider === "apify"
        ? "apifyUsd"
        : provider === "serpapi"
          ? "serpApiUsd"
          : provider === "llm"
            ? "llmUsd"
            : provider === "search"
              ? "standardSearchUsd"
              : "otherUsd";
  ledger[key] = Number(((ledger[key] || 0) + cost).toFixed(4));
  ledger.totalUsd = Number((ledger.totalUsd + cost).toFixed(4));
  ledger.providerCallCount += 1;
  if (provider === "ampfy") {
    ledger.ampfyCalls.push({ ...meta, costUsd: cost, recordedAt: new Date().toISOString() });
  }
  if (provider === "apify") {
    ledger.apifyCalls.push({ ...meta, costUsd: cost, recordedAt: new Date().toISOString() });
  }
  return ledger;
}

export function logEscalation(ledger, entry) {
  ledger.escalationLog.push({
    ...entry,
    at: new Date().toISOString(),
  });
  return ledger;
}

export function summarizeCostPerOpportunity(ledger, qualifiedCount, highPriorityCount) {
  const q = Math.max(1, Number(qualifiedCount) || 0);
  const h = Math.max(1, Number(highPriorityCount) || 0);
  return {
    costPerDiscoveredOpportunityUsd: null,
    costPerQualifiedOpportunityUsd: Number((ledger.totalUsd / q).toFixed(4)),
    costPerHighPriorityOpportunityUsd: Number((ledger.totalUsd / h).toFixed(4)),
    webhoundHardCapUsd: getWebhoundHardCapUsd(),
    webhoundRemainingUsd: Number(
      Math.max(0, getWebhoundHardCapUsd() - (ledger.webhoundUsd || 0)).toFixed(4)
    ),
    externalResearchEnabled: isExternalResearchEnabled(),
  };
}

/**
 * Level-4 Ampfy adapter: reserved. Returns not_configured unless env present.
 */
export function resolveAmpfyAdapterStatus(env = process.env) {
  const key = String(env.AMPFY_API_KEY || env.AMPFY_API_TOKEN || "").trim();
  if (!key) {
    return {
      provider: "ampfy",
      status: "provider_not_configured",
      note: "Ampfy is not present in this repo. Level-4 uses Apify where applicable; Ampfy slot reserved.",
    };
  }
  return { provider: "ampfy", status: "configured_not_wired_v1", note: "Key present; GDI Ampfy client deferred." };
}
