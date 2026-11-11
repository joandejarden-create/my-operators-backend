/**
 * Controlled aggregate-cap extension for Core+Loyalty execution.
 * Immutable audit entry; does not alter spent, reserved, or duplicate reconciliation.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { withAdpBudgetLock, readAdpBudgetLedger } from "./authorization-ledger-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

export const BUDGET_EXTENSION_VERSION = "core_loyalty_budget_extension_v1";
export const BUDGET_EXTENSION_JOB_KEY =
  "AUTHORIZATION:FOUNDER_CORE_LOYALTY_EXT_20261020_USD3.06";

export const PRIOR_CAP_USD = 238.54;
export const EXTENSION_USD = 3.06;
export const NEW_CAP_USD = 241.6; // 238.54 + 3.06

export function defaultCoreLoyaltyLedgerPath(cwd = ROOT) {
  return path.join(
    cwd,
    "data/ai-demand-positioning/budget/authorization-ledger-core-loyalty-execution-v1.json"
  );
}

/**
 * Raise budgetUsd by exactly EXTENSION_USD to NEW_CAP_USD with an immutable audit entry.
 * Idempotent if already applied.
 */
export function applyCoreLoyaltyBudgetExtension({
  ledgerPath = null,
  cwd = ROOT,
  priorCapUsd = PRIOR_CAP_USD,
  extensionUsd = EXTENSION_USD,
  newCapUsd = NEW_CAP_USD,
  authorizedBy = "founder",
  note = "Controlled extension for remaining Core+Loyalty measurement queue only. No enrichment/recurring monitoring. No publication/deploy.",
} = {}) {
  const pathResolved = ledgerPath || defaultCoreLoyaltyLedgerPath(cwd);
  const expectedNew = Number((Number(priorCapUsd) + Number(extensionUsd)).toFixed(2));
  if (Math.abs(expectedNew - Number(newCapUsd)) > 0.001) {
    return {
      ok: false,
      error: "new_cap_math_mismatch",
      expectedNew,
      newCapUsd,
    };
  }

  return withAdpBudgetLock(pathResolved, () => {
    const ledger = readAdpBudgetLedger({
      ledgerPath: pathResolved,
      allowBootstrapEmpty: false,
    });

    const existing = (ledger.entries || []).find(
      (e) =>
        e.jobKey === BUDGET_EXTENSION_JOB_KEY &&
        e.status === "authorization_extension"
    );
    if (existing || Number(ledger.budgetUsd) === Number(newCapUsd)) {
      return {
        ok: true,
        idempotent: true,
        priorCapUsd: Number(ledger.budgetUsd),
        newCapUsd: Number(ledger.budgetUsd),
        extensionUsd: 0,
        ledger,
        message: "extension_already_applied",
      };
    }

    if (Math.abs(Number(ledger.budgetUsd) - Number(priorCapUsd)) > 0.001) {
      return {
        ok: false,
        error: "unexpected_prior_cap",
        actual: ledger.budgetUsd,
        expected: priorCapUsd,
      };
    }

    const appliedAt = new Date().toISOString();
    ledger.entries = ledger.entries || [];
    ledger.entries.push({
      reservationId: `auth_ext_${Date.now().toString(36)}`,
      jobKey: BUDGET_EXTENSION_JOB_KEY,
      propertyId: null,
      label: `Founder authorized +$${extensionUsd} aggregate cap extension`,
      estimatedUsd: 0,
      actualBilledUsd: 0,
      status: "authorization_extension",
      immutable: true,
      authorizedBy,
      priorCapUsd: Number(priorCapUsd),
      extensionUsd: Number(extensionUsd),
      newCapUsd: Number(newCapUsd),
      appliedAt,
      note,
      version: BUDGET_EXTENSION_VERSION,
    });

    ledger.budgetUsd = Number(newCapUsd);
    ledger.authorization = {
      ...(ledger.authorization || {}),
      authorizedUsd: Number(newCapUsd),
      aggregateCapUsd: Number(newCapUsd),
      priorAuthorizedUsd: Number(priorCapUsd),
      extensionUsd: Number(extensionUsd),
      extensionJobKey: BUDGET_EXTENSION_JOB_KEY,
      extensionAppliedAt: appliedAt,
      extensionAuthorizedBy: authorizedBy,
      // Keep original authorizationId for jobKey continuity / idempotency
      originalAuthorizationId:
        ledger.authorization?.authorizationId ||
        "FOUNDER_CORE_LOYALTY_EXEC_20261020_USD238.54",
    };
    ledger.budgetExtension = {
      version: BUDGET_EXTENSION_VERSION,
      jobKey: BUDGET_EXTENSION_JOB_KEY,
      priorCapUsd: Number(priorCapUsd),
      extensionUsd: Number(extensionUsd),
      newCapUsd: Number(newCapUsd),
      appliedAt,
      immutable: true,
    };

    const tmp = `${pathResolved}.${process.pid}.ext.tmp`;
    fs.writeFileSync(tmp, JSON.stringify(ledger, null, 2) + "\n");
    fs.renameSync(tmp, pathResolved);
    return {
      ok: true,
      idempotent: false,
      priorCapUsd: Number(priorCapUsd),
      extensionUsd: Number(extensionUsd),
      newCapUsd: Number(newCapUsd),
      appliedAt,
      ledger,
    };
  });
}
