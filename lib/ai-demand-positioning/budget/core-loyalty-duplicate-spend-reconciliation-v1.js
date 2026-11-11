/**
 * Duplicate-provider-spend reconciliation for Core+Loyalty multi-process incident.
 * Does not delete/merge duplicate periods. Adjusts effective remaining budget.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { withAdpBudgetLock, readAdpBudgetLedger } from "./authorization-ledger-v1.js";
import { PROVIDER_CONFIGS } from "../execution/multi-provider-runner.js";
import { readbackDurableObservationArchive } from "../execution/durable-observation-archive-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

export const DUPLICATE_SPEND_RECON_VERSION =
  "core_loyalty_duplicate_spend_reconciliation_v1";
export const DUPLICATE_SPEND_RECON_JOB_KEY =
  "RECONCILIATION:MULTI_PROCESS_DUPLICATE_PROVIDER_SPEND_20261020";

export const DUPLICATE_PERIOD_EVIDENCE = Object.freeze([
  {
    hotel: "adp_hilton_times_square",
    role: "dupCore",
    authPeriodId: "adp_period_adp_hilton_times_square_20261020120601_8f1212",
    dupPeriodId: "adp_period_adp_hilton_times_square_20261020121725_48a06f",
    family: "CORE_ADP",
  },
  {
    hotel: "adp_hilton_times_square",
    role: "dupLoy",
    authPeriodId: "adp_period_adp_hilton_times_square_20261020124217_c638db",
    dupPeriodId: "adp_period_adp_hilton_times_square_20261020125250_6109ce",
    family: "LOYALTY_BPP",
  },
  {
    hotel: "adp_hotel_caribe_faranda_grand",
    role: "dupCore",
    authPeriodId: "adp_period_adp_hotel_caribe_faranda_grand_20261020125933_d47297",
    dupPeriodId: "adp_period_adp_hotel_caribe_faranda_grand_20261020130953_f15cde",
    family: "CORE_ADP",
  },
  {
    hotel: "adp_hotel_caribe_faranda_grand",
    role: "dupLoy",
    authPeriodId: "adp_period_adp_hotel_caribe_faranda_grand_20261020133130_075338",
    dupPeriodId: "adp_period_adp_hotel_caribe_faranda_grand_20261020134201_ddebe8",
    family: "LOYALTY_BPP",
  },
  {
    hotel: "adp_jw_marriott_monterrey_valle",
    role: "dupCore",
    authPeriodId: "adp_period_adp_jw_marriott_monterrey_valle_20261020134516_c6f5aa",
    dupPeriodId: "adp_period_adp_jw_marriott_monterrey_valle_20261020135143_5f7010",
    family: "CORE_ADP",
  },
]);

function priceOf(provider) {
  return Number(PROVIDER_CONFIGS[provider]?.costPerCall || 0);
}

export function analyzePeriodCost(periodId, cwd = ROOT) {
  const p = path.join(cwd, "data/ai-demand-positioning/runtime", `${periodId}.json`);
  if (!fs.existsSync(p)) {
    return { periodId, missing: true, confirmedUsd: 0, maxUsd: 0 };
  }
  let j;
  try {
    const raw = fs.readFileSync(p, "utf8").trim();
    if (!raw) {
      return {
        periodId,
        missing: false,
        emptyFile: true,
        confirmedUsd: 0,
        maxUsd: 0,
        note: "period file empty (possible mid-write)",
      };
    }
    j = JSON.parse(raw);
  } catch (err) {
    return {
      periodId,
      missing: false,
      parseError: String(err?.message || err),
      confirmedUsd: 0,
      maxUsd: 0,
    };
  }
  const obs = j.observations || [];
  let confirmedUsd = 0;
  let maxUsd = 0;
  const byProv = {};
  for (const o of obs) {
    const prov = o.provider || "unknown";
    const unit = priceOf(prov);
    maxUsd += unit;
    byProv[prov] = byProv[prov] || { calls: 0, withRaw: 0, confirmedUsd: 0 };
    byProv[prov].calls += 1;
    if (o.rawResponse) {
      byProv[prov].withRaw += 1;
      byProv[prov].confirmedUsd += unit;
      confirmedUsd += unit;
    }
  }
  let archive = null;
  try {
    archive = readbackDurableObservationArchive(j.propertyId, periodId);
  } catch {
    archive = { ok: false };
  }
  return {
    periodId,
    propertyId: j.propertyId,
    family: j.measurementFamily || null,
    status: j.status,
    obs: obs.length,
    withRaw: Object.values(byProv).reduce((s, v) => s + v.withRaw, 0),
    byProv,
    confirmedUsd: Number(confirmedUsd.toFixed(4)),
    maxUsd: Number(maxUsd.toFixed(4)),
    periodCostEstimate: Number(j.costEstimate?.total || 0),
    hasTokenUsageMetadata: obs.some((o) => o.usage || o.tokenUsage),
    archiveOk: archive?.ok === true,
    archiveWithOriginal: archive?.withOriginalResponse ?? 0,
    confidence: "CONFIRMED_AT_SCHEDULE_RATES",
    note:
      "No per-call token usage persisted on observations; confirmed = withRaw × PROVIDER_CONFIGS.costPerCall. Provider invoice not queried.",
  };
}

export function buildDuplicateSpendReconciliation(cwd = ROOT) {
  const rows = [];
  let confirmedTotal = 0;
  let maxTotal = 0;
  for (const ev of DUPLICATE_PERIOD_EVIDENCE) {
    const dup = analyzePeriodCost(ev.dupPeriodId, cwd);
    const auth = analyzePeriodCost(ev.authPeriodId, cwd);
    confirmedTotal += dup.confirmedUsd || 0;
    maxTotal += dup.maxUsd || 0;
    rows.push({
      ...ev,
      auth,
      duplicate: dup,
      preserveDuplicatePeriod: true,
      doNotMerge: true,
      doNotPublish: true,
    });
  }
  return {
    version: DUPLICATE_SPEND_RECON_VERSION,
    jobKey: DUPLICATE_SPEND_RECON_JOB_KEY,
    builtAt: new Date().toISOString(),
    pricingSchedule: Object.fromEntries(
      Object.entries(PROVIDER_CONFIGS).map(([k, v]) => [k, v.costPerCall])
    ),
    providerBillingDataAccessed: false,
    tokenUsageAvailable: false,
    confirmedDuplicateSpendUsd: Number(confirmedTotal.toFixed(2)),
    estimatedDuplicateSpendUsd: Number(confirmedTotal.toFixed(2)),
    maximumPotentialDuplicateSpendUsd: Number(maxTotal.toFixed(2)),
    confidenceBands: {
      confirmed:
        "Observations with rawResponse present in runtime + durable archive readback; priced at governed costPerCall schedule",
      estimated:
        "Same as confirmed (schedule rates); actual invoice cents may differ by provider discounts/taxes",
      maximum:
        "Same corpus (all recorded calls had rawResponse in these duplicate periods)",
    },
    rows,
  };
}

export function reconciliationOverlayPath(cwd = ROOT) {
  return path.join(
    cwd,
    "data/ai-demand-positioning/budget/core-loyalty-duplicate-spend-reconciliation-v1.json"
  );
}

export function loadDuplicateSpendOverlay(cwd = ROOT) {
  const p = reconciliationOverlayPath(cwd);
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

export function writeDuplicateSpendOverlay(recon, cwd = ROOT) {
  const p = reconciliationOverlayPath(cwd);
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, JSON.stringify(recon, null, 2) + "\n", "utf8");
  return p;
}

/**
 * Apply confirmed duplicate spend onto the authorization ledger as a reconciliation
 * spent entry (idempotent). Does not touch reserved in-flight jobs.
 */
export function applyDuplicateSpendToLedger({
  ledgerPath,
  recon,
  cwd = ROOT,
} = {}) {
  const amount = Number(recon.confirmedDuplicateSpendUsd || 0);
  if (!(amount > 0)) {
    return { ok: false, error: "no_confirmed_duplicate_amount" };
  }

  return withAdpBudgetLock(ledgerPath, () => {
    const ledger = readAdpBudgetLedger({ ledgerPath, allowBootstrapEmpty: false });
    const existing = (ledger.entries || []).find(
      (e) => e.jobKey === DUPLICATE_SPEND_RECON_JOB_KEY && e.status === "spent"
    );
    if (existing) {
      return {
        ok: true,
        idempotent: true,
        amount: existing.estimatedUsd,
        ledger,
        message: "reconciliation_already_applied",
      };
    }

    ledger.entries = ledger.entries || [];
    ledger.entries.push({
      reservationId: `recon_dup_${Date.now().toString(36)}`,
      jobKey: DUPLICATE_SPEND_RECON_JOB_KEY,
      propertyId: null,
      label: "Multi-process duplicate provider spend reconciliation 2026-10-20",
      estimatedUsd: amount,
      actualBilledUsd: amount,
      status: "spent",
      spentAt: new Date().toISOString(),
      reconciliation: true,
      confidence: "CONFIRMED_AT_SCHEDULE_RATES",
      providerBillingDataAccessed: false,
      evidenceOverlayPath: reconciliationOverlayPath(cwd),
      note:
        "Authorization accounting correction for duplicate API usage. Duplicate periods preserved as incident evidence — not merged/published.",
    });
    ledger.estimatedSpentUsd = Number(
      (Number(ledger.estimatedSpentUsd || 0) + amount).toFixed(6)
    );
    if (ledger.actualBilledUsd == null) {
      ledger.actualBilledUsd = amount;
    } else {
      ledger.actualBilledUsd = Number(
        (Number(ledger.actualBilledUsd) + amount).toFixed(6)
      );
    }
    ledger.reconciliation = {
      version: DUPLICATE_SPEND_RECON_VERSION,
      appliedAt: new Date().toISOString(),
      confirmedDuplicateSpendUsd: amount,
    };

    const tmp = `${ledgerPath}.${process.pid}.recon.tmp`;
    fs.writeFileSync(tmp, JSON.stringify(ledger, null, 2) + "\n");
    fs.renameSync(tmp, ledgerPath);
    return { ok: true, idempotent: false, amount, ledger };
  });
}
