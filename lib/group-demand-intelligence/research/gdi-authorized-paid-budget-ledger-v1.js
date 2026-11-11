/**
 * Global authorized paid-research budget ledger V1.
 * Single hard ceiling across hotels × segments × discovery × deep research.
 * Does not invent spend. Records quoted max before call and actual after.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { quoteExaAgentRun, EXA_AGENT_EFFORT } from "./exa-agent/exa-agent-provider-adapter-v1.js";

export const GDI_AUTHORIZED_PAID_BUDGET_LEDGER_V1 =
  "gdi-authorized-paid-budget-ledger-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

export function createPaidBudgetLedger({
  authorizationId = "GDI_UNAUTHORIZED",
  hardCeilingUsd = 0,
  effort = EXA_AGENT_EFFORT.MEDIUM,
  priorSpendUsd = 0,
  outPath = null,
} = {}) {
  const quote = quoteExaAgentRun({ effort });
  const unitMax = Number(quote.maxUsd || 0);
  const ledger = {
    version: GDI_AUTHORIZED_PAID_BUDGET_LEDGER_V1,
    authorizationId,
    hardCeilingUsd: Number(hardCeilingUsd),
    effort,
    quoteBounded: quote.bounded === true,
    quoteUnitMaxUsd: unitMax,
    quoteAssumption: quote.assumption || null,
    quoteAsOf: quote.asOf || null,
    priorSpendUsd: Number(priorSpendUsd || 0),
    spentUsd: 0,
    reservedUsd: 0,
    operations: [],
    blockedOperations: [],
    frozen: false,
    freezeReason: null,
    createdAt: new Date().toISOString(),
    outPath:
      outPath ||
      path.join(
        ROOT,
        "reports/gdi-aimbridge-full-e2e-live-v1",
        "paid-budget-ledger.json"
      ),
  };
  return ledger;
}

/**
 * Resume an existing on-disk ledger without resetting spent operations.
 * Does not invent spend. Clears in-flight reservations from a crashed process.
 */
export function loadPaidBudgetLedger(outPath, {
  authorizationId = null,
  hardCeilingUsd = null,
  effort = EXA_AGENT_EFFORT.MEDIUM,
} = {}) {
  if (!outPath || !fs.existsSync(outPath)) return null;
  const raw = JSON.parse(fs.readFileSync(outPath, "utf8"));
  if (!raw || raw.version !== GDI_AUTHORIZED_PAID_BUDGET_LEDGER_V1) return null;
  const quote = quoteExaAgentRun({ effort: raw.effort || effort });
  const ledger = {
    ...raw,
    authorizationId: authorizationId || raw.authorizationId,
    hardCeilingUsd:
      hardCeilingUsd != null ? Number(hardCeilingUsd) : Number(raw.hardCeilingUsd || 0),
    quoteBounded: quote.bounded === true,
    quoteUnitMaxUsd: Number(quote.maxUsd || raw.quoteUnitMaxUsd || 0),
    quoteAssumption: quote.assumption || raw.quoteAssumption || null,
    quoteAsOf: quote.asOf || raw.quoteAsOf || null,
    spentUsd: Number(raw.spentUsd || 0),
    reservedUsd: 0,
    operations: Array.isArray(raw.operations) ? raw.operations : [],
    blockedOperations: Array.isArray(raw.blockedOperations)
      ? raw.blockedOperations
      : [],
    frozen: raw.frozen === true,
    freezeReason: raw.freezeReason || null,
    outPath,
  };
  persistPaidBudgetLedger(ledger);
  return ledger;
}

export function completedDeepResearchOpportunityIds(ledger) {
  const ids = new Set();
  for (const op of ledger?.operations || []) {
    if (String(op.stage || "") !== "DEEP_RESEARCH") continue;
    const id = String(op.opportunityId || "").trim();
    if (id) ids.add(id);
  }
  return ids;
}

export function remainingBudgetUsd(ledger) {
  const hard = Number(ledger.hardCeilingUsd || 0);
  const spent = Number(ledger.spentUsd || 0);
  const reserved = Number(ledger.reservedUsd || 0);
  return Number(Math.max(0, hard - spent - reserved).toFixed(6));
}

export function canAffordQuotedCall(ledger, quotedMaxUsd = null) {
  if (ledger.frozen) {
    return {
      ok: false,
      reason: ledger.freezeReason || "LEDGER_FROZEN",
      remaining: remainingBudgetUsd(ledger),
    };
  }
  if (ledger.quoteBounded !== true) {
    return {
      ok: false,
      reason: "QUOTE_UNBOUNDED_REFUSE_SPEND",
      remaining: remainingBudgetUsd(ledger),
    };
  }
  const maxCharge = Number(
    quotedMaxUsd != null ? quotedMaxUsd : ledger.quoteUnitMaxUsd || 0
  );
  if (!(maxCharge > 0)) {
    return {
      ok: false,
      reason: "QUOTE_MAX_NOT_POSITIVE",
      remaining: remainingBudgetUsd(ledger),
    };
  }
  const remaining = remainingBudgetUsd(ledger);
  if (maxCharge > remaining + 1e-9) {
    return {
      ok: false,
      reason: "WOULD_EXCEED_HARD_CEILING",
      remaining,
      proposedMax: maxCharge,
    };
  }
  return { ok: true, remaining, proposedMax: maxCharge };
}

export function reserveQuotedCall(ledger, opMeta = {}) {
  const gate = canAffordQuotedCall(ledger, opMeta.quotedMaxUsd);
  if (!gate.ok) return gate;
  ledger.reservedUsd = Number(
    (Number(ledger.reservedUsd || 0) + gate.proposedMax).toFixed(6)
  );
  persistPaidBudgetLedger(ledger);
  return { ok: true, reserved: gate.proposedMax, remaining: remainingBudgetUsd(ledger) };
}

export function settleReservedCall(ledger, {
  quotedMaxUsd,
  actualCostUsd = 0,
  ...op
} = {}) {
  const quoted = Number(quotedMaxUsd || ledger.quoteUnitMaxUsd || 0);
  const actual = Number(actualCostUsd || 0);
  ledger.reservedUsd = Number(
    Math.max(0, Number(ledger.reservedUsd || 0) - quoted).toFixed(6)
  );
  ledger.spentUsd = Number((Number(ledger.spentUsd || 0) + actual).toFixed(6));
  ledger.operations.push({
    at: new Date().toISOString(),
    quotedMaxUsd: quoted,
    actualCostUsd: actual,
    ...op,
  });
  if (actual > quoted * 1.01) {
    ledger.frozen = true;
    ledger.freezeReason = "ACTUAL_EXCEEDED_FIXED_QUOTE_FREEZE";
    ledger.blockedOperations.push({
      at: new Date().toISOString(),
      reason: ledger.freezeReason,
      actual,
      quotedMaxUsd: quoted,
    });
  }
  if (ledger.spentUsd >= Number(ledger.hardCeilingUsd || 0) - 1e-9) {
    ledger.frozen = true;
    ledger.freezeReason = ledger.freezeReason || "HARD_CEILING_REACHED";
  }
  persistPaidBudgetLedger(ledger);
  return ledger;
}

export function releaseReservation(ledger, quotedMaxUsd) {
  const quoted = Number(quotedMaxUsd || ledger.quoteUnitMaxUsd || 0);
  ledger.reservedUsd = Number(
    Math.max(0, Number(ledger.reservedUsd || 0) - quoted).toFixed(6)
  );
  persistPaidBudgetLedger(ledger);
}

export function persistPaidBudgetLedger(ledger) {
  if (!ledger?.outPath) return null;
  fs.mkdirSync(path.dirname(ledger.outPath), { recursive: true });
  const doc = {
    ...ledger,
    remainingUsd: remainingBudgetUsd(ledger),
    updatedAt: new Date().toISOString(),
  };
  fs.writeFileSync(ledger.outPath, `${JSON.stringify(doc, null, 2)}\n`);
  return ledger.outPath;
}

export default {
  GDI_AUTHORIZED_PAID_BUDGET_LEDGER_V1,
  createPaidBudgetLedger,
  loadPaidBudgetLedger,
  completedDeepResearchOpportunityIds,
  remainingBudgetUsd,
  canAffordQuotedCall,
  reserveQuotedCall,
  settleReservedCall,
  releaseReservation,
  persistPaidBudgetLedger,
};
