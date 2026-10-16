/**
 * Durable Surfe / contact-provider usage ledger (backend only).
 * Shares the paid multi-segment $11 authorization envelope — not a separate allowance.
 *
 * Unknown credits/cost stay null (never coerced to 0).
 * One shared-person lookup = one request even if multiple cards benefit.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createHash, randomBytes } from "node:crypto";
import {
  reserveParallelBudget,
  commitParallelBudgetReservation,
  releaseParallelBudgetReservation,
  readParallelBudgetLedger,
  remainingParallelBudgetUsd,
  paidMultisegmentAuthorizationLedgerPath,
} from "../parallel/budget-ledger-v1.js";
import {
  AUTHORIZED_NEW_SPEND_USD,
  SURFE_EMAIL_CREDIT_USD_ESTIMATE,
  SURFE_MOBILE_CREDIT_USD_ESTIMATE,
} from "./paid-multisegment-research-v1.js";
import { personIdentityKey } from "./contact-field-provenance-v1.js";

export const SURFE_USAGE_LEDGER_VERSION = "gdi-surfe-usage-ledger-v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

function defaultUsagePath(env = process.env) {
  if (env.GDI_SURFE_USAGE_LEDGER_PATH) {
    return path.resolve(env.GDI_SURFE_USAGE_LEDGER_PATH);
  }
  const root = env.GDI_DATA_ROOT
    ? path.resolve(env.GDI_DATA_ROOT)
    : path.join(ROOT, "data/group-demand-intelligence");
  return path.join(root, "surfe-usage", "ledger-v1.json");
}

function paidMsBudgetPath(env = process.env) {
  return paidMultisegmentAuthorizationLedgerPath(env);
}

function emptyUsageLedger() {
  return {
    version: SURFE_USAGE_LEDGER_VERSION,
    currency: "USD",
    sharedBudgetEnvelopeUsd: AUTHORIZED_NEW_SPEND_USD,
    sharedBudgetLedgerPath: null,
    entries: [],
    peopleIndex: {},
    updatedAt: new Date().toISOString(),
  };
}

export function readSurfeUsageLedger(opts = {}) {
  const ledgerPath = opts.ledgerPath || defaultUsagePath(opts.env || process.env);
  fs.mkdirSync(path.dirname(ledgerPath), { recursive: true });
  if (!fs.existsSync(ledgerPath)) {
    const fresh = emptyUsageLedger();
    fresh.sharedBudgetLedgerPath = paidMsBudgetPath(opts.env || process.env);
    fs.writeFileSync(ledgerPath, JSON.stringify(fresh, null, 2) + "\n");
    return { ...fresh, path: ledgerPath };
  }
  const doc = JSON.parse(fs.readFileSync(ledgerPath, "utf8"));
  return { ...doc, path: ledgerPath };
}

function writeUsage(ledgerPath, doc) {
  doc.updatedAt = new Date().toISOString();
  const tmp = `${ledgerPath}.${process.pid}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(doc, null, 2) + "\n");
  fs.renameSync(tmp, ledgerPath);
  return doc;
}

/**
 * Find a reusable prior accepted result for the same person identity.
 */
export function findReusableSurfeResult(person = {}, opts = {}) {
  const key = personIdentityKey(person);
  const ledger = readSurfeUsageLedger(opts);
  const hit = ledger.peopleIndex?.[key];
  if (!hit || !hit.lastAcceptedEntryId) return null;
  const entry = (ledger.entries || []).find((e) => e.entryId === hit.lastAcceptedEntryId);
  if (!entry) return null;
  const maxAgeDays = Number(opts.maxAgeDays ?? 90);
  const at = Date.parse(entry.timestamp || "");
  if (Number.isFinite(at)) {
    const ageDays = (Date.now() - at) / (86400 * 1000);
    if (ageDays > maxAgeDays) return null;
  }
  if (entry.outcome !== "accepted" && entry.outcome !== "corroborated") return null;
  return { cacheHit: true, personIdentityKey: key, entry };
}

/**
 * Reserve shared $11 envelope before a Surfe call.
 * estimatedUsd may be null when credit→USD basis is unknown — then block paid call.
 */
export function reserveSurfeSpend({
  estimatedUsd,
  person,
  hotelId,
  opportunityIds = [],
  requestedChannels = ["email"],
  workflowRunId = null,
  env = process.env,
  label = "surfe_contact_enrichment",
} = {}) {
  if (estimatedUsd == null || !Number.isFinite(Number(estimatedUsd))) {
    return {
      ok: false,
      blocked: true,
      reason: "surfe_cost_basis_unknown",
      note: "No usable credit→USD cost basis — report blocker before consuming credits",
      remainingUsd: remainingSharedBudgetUsd({ env }),
    };
  }
  const budgetPath = paidMsBudgetPath(env);
  const personKey = personIdentityKey(person || {});
  const jobKey = `surfe:${personKey}:${(requestedChannels || []).sort().join("+")}`;

  // Deduplicate: reuse before new paid request
  const reusable = findReusableSurfeResult(person, { env });
  if (reusable) {
    return {
      ok: true,
      reuse: true,
      reservationId: null,
      personIdentityKey: personKey,
      jobKey,
      reusable,
      estimatedUsd: 0,
      remainingUsd: remainingSharedBudgetUsd({ env }),
    };
  }

  try {
    const reservation = reserveParallelBudget({
      label,
      processor: "surfe_email",
      estimatedUsd: Number(estimatedUsd),
      jobKey,
      hotelId,
      maxUsd: AUTHORIZED_NEW_SPEND_USD,
      env,
      ledgerPath: budgetPath,
    });
    return {
      ok: true,
      reuse: false,
      reservationId: reservation.reservationId,
      personIdentityKey: personKey,
      jobKey,
      estimatedUsd: Number(estimatedUsd),
      remainingUsd: remainingSharedBudgetUsd({ env }),
      opportunityIds,
      workflowRunId,
      requestedChannels,
    };
  } catch (err) {
    return {
      ok: false,
      blocked: true,
      reason: err?.code || err?.message || "budget_reserve_failed",
      remainingUsd: remainingSharedBudgetUsd({ env }),
      attemptedUsd: Number(estimatedUsd),
    };
  }
}

export function remainingSharedBudgetUsd(opts = {}) {
  const env = opts.env || process.env;
  const ledger = readParallelBudgetLedger({
    ledgerPath: paidMsBudgetPath(env),
    env,
    budgetUsd: AUTHORIZED_NEW_SPEND_USD,
    authorizationEnvelope: true,
    // Read-only remaining queries still fail closed if ledger missing —
    // callers must not treat absence as a full fresh $11 remaining.
    allowBootstrapEmpty: opts.allowBootstrapEmpty === true,
  });
  return remainingParallelBudgetUsd(ledger);
}

export function estimateSurfeUsd({ email = true, mobile = false } = {}) {
  let usd = 0;
  let known = true;
  if (email) {
    if (SURFE_EMAIL_CREDIT_USD_ESTIMATE == null) known = false;
    else usd += SURFE_EMAIL_CREDIT_USD_ESTIMATE;
  }
  if (mobile) {
    if (SURFE_MOBILE_CREDIT_USD_ESTIMATE == null) known = false;
    else usd += SURFE_MOBILE_CREDIT_USD_ESTIMATE;
  }
  return {
    estimatedUsd: known ? Number(usd.toFixed(4)) : null,
    emailCreditsWorstCase: email ? 1 : 0,
    mobileCreditsWorstCase: mobile ? 1 : 0,
    basisKnown: known,
    note: known
      ? "Estimate only — Surfe bills prepaid credits; USD is shared-envelope accounting"
      : "Cost basis unknown",
  };
}

/**
 * Append a usage entry after a Surfe attempt (paid, reuse, error, or no-result).
 */
export function recordSurfeUsageEntry(entry = {}, opts = {}) {
  const ledgerPath = opts.ledgerPath || defaultUsagePath(opts.env || process.env);
  const doc = readSurfeUsageLedger({ ledgerPath, env: opts.env });
  const entryId =
    entry.entryId ||
    `surfe_${Date.now().toString(36)}_${randomBytes(3).toString("hex")}`;
  const personKey =
    entry.personIdentityKey ||
    personIdentityKey({
      name: entry.personName,
      organization: entry.organization,
      domain: entry.domain,
    });

  const row = {
    entryId,
    timestamp: new Date().toISOString(),
    workflowRunId: entry.workflowRunId || null,
    hotelId: entry.hotelId || null,
    personIdentityKey: personKey,
    personName: entry.personName || null,
    organization: entry.organization || null,
    domain: entry.domain || null,
    opportunityIds: entry.opportunityIds || [],
    providerRequestId: entry.providerRequestId || null,
    requestedChannels: entry.requestedChannels || [],
    outcome: entry.outcome || "unknown", // accepted | corroborated | no_result | error | identity_conflict | reused
    error: entry.error || null,
    newRequest: entry.newRequest !== false,
    cacheReuse: entry.cacheReuse === true,
    creditsConsumed:
      entry.creditsConsumed === undefined ? null : entry.creditsConsumed, // null = unknown
    estimatedCostUsd:
      entry.estimatedCostUsd === undefined ? null : entry.estimatedCostUsd,
    actualBilledCostUsd:
      entry.actualBilledCostUsd === undefined ? null : entry.actualBilledCostUsd,
    fieldsReturned: entry.fieldsReturned || [],
    fieldsAccepted: entry.fieldsAccepted || [],
    fieldsRejected: entry.fieldsRejected || [],
    fieldsQuarantined: entry.fieldsQuarantined || [],
    // Persist accepted channel values so reuse can rehydrate without a new Surfe call
    acceptedFieldValues: entry.acceptedFieldValues || null,
    reservationId: entry.reservationId || null,
  };

  doc.entries.push(row);
  doc.sharedBudgetLedgerPath = paidMsBudgetPath(opts.env || process.env);
  if (!doc.peopleIndex) doc.peopleIndex = {};
  if (row.outcome === "accepted" || row.outcome === "corroborated") {
    doc.peopleIndex[personKey] = {
      lastAcceptedEntryId: entryId,
      personName: row.personName,
      updatedAt: row.timestamp,
    };
  }
  // Link opportunity beneficiaries for shared-person reuse accounting
  for (const oid of row.opportunityIds || []) {
    const k = `opp:${oid}`;
    if (!doc.peopleIndex[k]) doc.peopleIndex[k] = { entryIds: [] };
    doc.peopleIndex[k].entryIds = [
      ...new Set([...(doc.peopleIndex[k].entryIds || []), entryId]),
    ];
  }

  writeUsage(ledgerPath, doc);

  if (row.reservationId) {
    if (row.outcome === "error" && row.newRequest && !row.providerRequestId) {
      releaseParallelBudgetReservation(row.reservationId, {
        env: opts.env,
        ledgerPath: paidMsBudgetPath(opts.env || process.env),
        reason: row.error || "surfe_failed_before_request",
      });
    } else if (row.newRequest && row.reservationId) {
      commitParallelBudgetReservation(row.reservationId, {
        env: opts.env,
        ledgerPath: paidMsBudgetPath(opts.env || process.env),
        providerRunId: row.providerRequestId,
      });
    }
  }

  return { entry: row, ledgerPath };
}

export function summarizeSurfeUsage(opts = {}) {
  const doc = readSurfeUsageLedger(opts);
  const entries = doc.entries || [];
  const paid = entries.filter((e) => e.newRequest && !e.cacheReuse);
  const reuse = entries.filter((e) => e.cacheReuse);
  const acceptedEmails = entries.filter((e) =>
    (e.fieldsAccepted || []).includes("email")
  ).length;
  const acceptedPhones = entries.filter((e) =>
    (e.fieldsAccepted || []).includes("phone")
  ).length;
  const uniquePeople = new Set(
    entries.map((e) => e.personIdentityKey).filter(Boolean)
  ).size;
  const opps = new Set();
  for (const e of entries) {
    for (const id of e.opportunityIds || []) opps.add(id);
  }
  let creditsKnown = 0;
  let creditsUnknown = 0;
  let est = 0;
  let actualKnown = 0;
  let actualSum = 0;
  for (const e of paid) {
    if (e.creditsConsumed == null) creditsUnknown += 1;
    else creditsKnown += Number(e.creditsConsumed) || 0;
    if (e.estimatedCostUsd != null) est += Number(e.estimatedCostUsd) || 0;
    if (e.actualBilledCostUsd != null) {
      actualKnown += 1;
      actualSum += Number(e.actualBilledCostUsd) || 0;
    }
  }
  return {
    version: SURFE_USAGE_LEDGER_VERSION,
    paidRequests: paid.length,
    reuseCacheHits: reuse.length,
    uniquePeopleEnriched: uniquePeople,
    acceptedEmails,
    acceptedPhones,
    opportunitiesImproved: opps.size,
    creditsConsumed: creditsUnknown === paid.length && paid.length > 0 ? null : creditsKnown,
    creditsUnknownCount: creditsUnknown,
    estimatedCostUsd: Number(est.toFixed(4)),
    actualBilledCostUsd: actualKnown ? Number(actualSum.toFixed(4)) : null,
    remainingSharedBudgetUsd: remainingSharedBudgetUsd(opts),
    path: doc.path,
  };
}

export function lookupKeyHash(name, organization) {
  return createHash("sha256")
    .update(`${text(name).toLowerCase()}|${text(organization).toLowerCase()}`)
    .digest("hex")
    .slice(0, 12);
}

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}
