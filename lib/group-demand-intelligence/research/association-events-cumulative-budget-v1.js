/**
 * Hotel-level cumulative research budget for Association & Events allocations.
 * Caps never reset between waves/runs. Prior Scaling + Association spend is authoritative
 * for Mexico hotels; additional founder-authorized hotels use isolated caps (prior 0 unless set).
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const ASSOCIATION_EVENTS_CUMULATIVE_BUDGET_V1 =
  "gdi-association-events-cumulative-budget-v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

/** Founder-authorized Mexico hotel caps (Scaling + Association shared). */
export const ASSOCIATION_EVENTS_MEXICO_HOTEL_CAPS = Object.freeze({
  recterKmNPdntCm0v: {
    hotelId: "recterKmNPdntCm0v",
    slug: "chapultepec",
    displayName: "Grand Fiesta Americana Chapultepec",
    cumulativeCapUsd: 12,
    providersAllowed: ["exa", "firecrawl"],
    providersForbidden: ["surfe", "pdl", "parallel"],
  },
  recIEcSWuhSCoNLFN: {
    hotelId: "recIEcSWuhSCoNLFN",
    slug: "condesa",
    displayName: "Fiesta Americana Condesa Cancún All Inclusive",
    cumulativeCapUsd: 15,
    providersAllowed: ["exa", "firecrawl"],
    providersForbidden: ["surfe", "pdl", "parallel"],
  },
});

/**
 * Founder-authorized non-Mexico Association Events caps (isolated; do not share Mexico prior).
 * Nobu second wave: APPROVED_NOBU_LONDON_ASSOC_EVENTS_SECOND_WAVE_EXA_USD20 — Exa only.
 */
export const ASSOCIATION_EVENTS_ADDITIONAL_HOTEL_CAPS = Object.freeze({
  recC2kbCaS48jFjFF: {
    hotelId: "recC2kbCaS48jFjFF",
    slug: "nobu-london-portman",
    displayName: "Nobu Hotel London Portman Square",
    cumulativeCapUsd: 20,
    allocationId: "nobu_london_assoc_events_second_wave_v1",
    approvalPhrase: "APPROVED_NOBU_LONDON_ASSOC_EVENTS_SECOND_WAVE_EXA_USD20",
    providersAllowed: ["exa"],
    providersForbidden: ["firecrawl", "surfe", "pdl", "parallel"],
  },
});

/** Union of Mexico + additional authorized hotel caps. */
export const ASSOCIATION_EVENTS_AUTHORIZED_HOTEL_CAPS = Object.freeze({
  ...ASSOCIATION_EVENTS_MEXICO_HOTEL_CAPS,
  ...ASSOCIATION_EVENTS_ADDITIONAL_HOTEL_CAPS,
});

/**
 * Authoritative prior spend from mexico FullE2E reconciliation package
 * (Scaling + Association wave1 + buyer continuation). Updated by campaign ledger.
 * Hotels absent from this map start at prior 0 (e.g. Nobu London second wave).
 */
export const ASSOCIATION_EVENTS_AUTHORITATIVE_PRIOR_SPEND = Object.freeze({
  source:
    "reports/gdi-association-events-mexico-full-e2e-v1/14-per-hotel-spend-and-remaining.json",
  asOf: "2026-10-27",
  recterKmNPdntCm0v: 1.793,
  recIEcSWuhSCoNLFN: 1.135,
  combined: 2.928,
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function num(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

export function hotelCampaignStateDir(hotelId) {
  const slug =
    ASSOCIATION_EVENTS_AUTHORIZED_HOTEL_CAPS[hotelId]?.slug ||
    text(hotelId).replace(/[^\w.-]+/g, "_").slice(0, 40);
  return path.join(ROOT, "reports/gdi-association-events-campaigns", slug);
}

export function hotelCumulativeLedgerPath(hotelId) {
  return path.join(hotelCampaignStateDir(hotelId), "cumulative-spend-ledger.json");
}

export function loadHotelCumulativeLedger(hotelId) {
  const p = hotelCumulativeLedgerPath(hotelId);
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

/**
 * Reconcile remaining authorization for a hotel.
 * Prior spend = max(authoritative baseline, hotel cumulative ledger campaignSpent).
 */
export function reconcileAssociationEventsHotelBudget(hotelId, { interveningSpentUsd = 0 } = {}) {
  const capRow = ASSOCIATION_EVENTS_AUTHORIZED_HOTEL_CAPS[hotelId];
  if (!capRow) {
    return {
      ok: false,
      error: "hotel_not_in_association_events_allocation",
      // Legacy alias kept for older run diagnostics
      legacyError: "hotel_not_in_mexico_association_allocation",
      hotelId,
      version: ASSOCIATION_EVENTS_CUMULATIVE_BUDGET_V1,
    };
  }
  const baselinePrior = num(ASSOCIATION_EVENTS_AUTHORITATIVE_PRIOR_SPEND[hotelId]);
  const ledger = loadHotelCumulativeLedger(hotelId);
  const ledgerCampaignSpent = num(ledger?.campaignSpentUsd);
  const ledgerPriorSnapshot = num(ledger?.priorBaselineSpentUsd);
  // Campaign spent after baseline; total = baseline + campaign (or ledger total if higher)
  const campaignSpent = Math.max(
    ledgerCampaignSpent,
    num(ledger?.totalSpentUsd) - baselinePrior,
    0,
  );
  const priorSpent = Math.max(baselinePrior, ledgerPriorSnapshot || 0);
  const intervening = Math.max(0, num(interveningSpentUsd));
  const totalSpent = Number((priorSpent + campaignSpent + intervening).toFixed(6));
  const remaining = Number(
    Math.max(0, capRow.cumulativeCapUsd - totalSpent).toFixed(6),
  );
  const ok = remaining > 0.01;
  const providersAllowed = Array.isArray(capRow.providersAllowed)
    ? [...capRow.providersAllowed]
    : ["exa", "firecrawl"];
  const providersForbidden = Array.isArray(capRow.providersForbidden)
    ? [...capRow.providersForbidden]
    : ["surfe", "pdl", "parallel"];
  return {
    ok,
    version: ASSOCIATION_EVENTS_CUMULATIVE_BUDGET_V1,
    hotelId,
    slug: capRow.slug,
    displayName: capRow.displayName,
    cumulativeCapUsd: capRow.cumulativeCapUsd,
    allocationId: capRow.allocationId || null,
    priorBaselineSpentUsd: priorSpent,
    campaignSpentUsd: campaignSpent,
    interveningSpentUsd: intervening,
    totalSpentUsd: totalSpent,
    remainingUsd: remaining,
    authorizationValid: ok,
    stopIfInvalid: !ok,
    sourcePrior:
      baselinePrior > 0
        ? ASSOCIATION_EVENTS_AUTHORITATIVE_PRIOR_SPEND.source
        : "isolated_campaign_prior_zero",
    ledgerPath: hotelCumulativeLedgerPath(hotelId),
    providersAllowed,
    providersForbidden,
  };
}

export function recordHotelCampaignSpend(hotelId, {
  runId,
  waveIndex,
  spentUsd,
  objective = null,
  provider = "exa",
} = {}) {
  const recon = reconcileAssociationEventsHotelBudget(hotelId);
  if (!recon.ok && recon.error) return recon;
  const dir = hotelCampaignStateDir(hotelId);
  fs.mkdirSync(dir, { recursive: true });
  const p = hotelCumulativeLedgerPath(hotelId);
  const existing = loadHotelCumulativeLedger(hotelId) || {
    version: ASSOCIATION_EVENTS_CUMULATIVE_BUDGET_V1,
    hotelId,
    priorBaselineSpentUsd: recon.priorBaselineSpentUsd,
    campaignSpentUsd: 0,
    entries: [],
  };
  const add = Math.max(0, num(spentUsd));
  existing.campaignSpentUsd = Number((num(existing.campaignSpentUsd) + add).toFixed(6));
  existing.totalSpentUsd = Number(
    (num(existing.priorBaselineSpentUsd || recon.priorBaselineSpentUsd) +
      existing.campaignSpentUsd).toFixed(6),
  );
  existing.cumulativeCapUsd = recon.cumulativeCapUsd;
  existing.remainingUsd = Number(
    Math.max(0, recon.cumulativeCapUsd - existing.totalSpentUsd).toFixed(6),
  );
  existing.entries = existing.entries || [];
  existing.entries.push({
    at: new Date().toISOString(),
    runId: runId || null,
    waveIndex: waveIndex ?? null,
    spentUsd: add,
    objective,
    provider,
  });
  existing.updatedAt = new Date().toISOString();
  fs.writeFileSync(p, `${JSON.stringify(existing, null, 2)}\n`, "utf8");
  return { ok: true, ledger: existing, path: p };
}

export function acquireHotelCampaignLock(hotelId, runId) {
  const dir = hotelCampaignStateDir(hotelId);
  fs.mkdirSync(dir, { recursive: true });
  const lockPath = path.join(dir, "campaign.lock.json");
  if (fs.existsSync(lockPath)) {
    try {
      const existing = JSON.parse(fs.readFileSync(lockPath, "utf8"));
      const ageMs = Date.now() - Date.parse(existing.at || 0);
      // Stale lock > 3 hours → take over
      if (Number.isFinite(ageMs) && ageMs < 3 * 60 * 60 * 1000 && existing.runId !== runId) {
        return {
          ok: false,
          error: "hotel_campaign_locked",
          lockPath,
          heldBy: existing,
        };
      }
    } catch {
      /* take over corrupt lock */
    }
  }
  const lock = { hotelId, runId, at: new Date().toISOString() };
  fs.writeFileSync(lockPath, `${JSON.stringify(lock, null, 2)}\n`, "utf8");
  return { ok: true, lockPath, lock };
}

export function releaseHotelCampaignLock(hotelId, runId) {
  const lockPath = path.join(hotelCampaignStateDir(hotelId), "campaign.lock.json");
  if (!fs.existsSync(lockPath)) return { ok: true, released: false };
  try {
    const existing = JSON.parse(fs.readFileSync(lockPath, "utf8"));
    if (existing.runId && runId && existing.runId !== runId) {
      return { ok: false, error: "lock_held_by_other", heldBy: existing };
    }
  } catch {
    /* remove anyway */
  }
  fs.unlinkSync(lockPath);
  return { ok: true, released: true };
}
