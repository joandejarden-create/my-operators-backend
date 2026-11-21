/**
 * Durable FullEnrich spend + non-PII job ledger (V1 reliability hardening).
 *
 * - Survives process restarts (JSON on disk + exclusive file lock).
 * - Concurrent / multi-process safe on a shared filesystem (Surfe-style wx lock).
 * - Reserves credits before provider start; reconciles confirmed / estimated / UNKNOWN.
 * - Never assumes timeout/failure was free — reserved credits become UNKNOWN until reconciled.
 * - Accounting write failures flip accountingHealthy=false and block new paid requests.
 * - Jobs store only operational IDs/status — never emails, phones, or raw provider payloads.
 */

import fs from "node:fs";
import path from "node:path";
import { randomUUID, createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import { FULLENRICH_CREDIT_COSTS } from "../fullenrich/client.js";

export const GDI_FULLENRICH_SPEND_LEDGER_V1 = "gdi-fullenrich-spend-ledger-v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const STALE_LOCK_MS = 30_000;
const DEFAULT_SPEND_CAP = 10;
const DEFAULT_RESERVE_CREDITS =
  FULLENRICH_CREDIT_COSTS.work_email_found_deliverable_high_probability_or_catchall || 1;

const FORBIDDEN_PII_KEYS = Object.freeze([
  "email",
  "emails",
  "phone",
  "phones",
  "mobile",
  "contactDetails",
  "rawPayload",
  "payload",
  "providerPayload",
  "work_emails",
  "personal_emails",
  "most_probable_work_email",
]);

export const FE_JOB_STATUS = Object.freeze({
  RESERVED: "RESERVED",
  SUBMITTED: "SUBMITTED",
  PENDING: "PENDING",
  FINISHED: "FINISHED",
  NO_MATCH: "NO_MATCH",
  TIMEOUT: "TIMEOUT",
  ERROR: "ERROR",
  ABORTED: "ABORTED",
  DUPLICATE_SUPPRESSED: "DUPLICATE_SUPPRESSED",
  RECOVERED: "RECOVERED",
});

export const FE_CHARGE_STATUS = Object.freeze({
  RESERVED: "RESERVED",
  RELEASED: "RELEASED",
  CONFIRMED: "CONFIRMED",
  ESTIMATED: "ESTIMATED",
  UNKNOWN: "UNKNOWN",
});

function text(v) {
  return String(v == null ? "" : v).trim();
}

function sleepSync(ms) {
  try {
    const sab = new SharedArrayBuffer(4);
    const ia = new Int32Array(sab);
    Atomics.wait(ia, 0, 0, ms);
  } catch {
    const end = Date.now() + ms;
    while (Date.now() < end) {
      /* spin */
    }
  }
}

function pidAlive(pid) {
  if (!Number.isFinite(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function ledgerDir(env = process.env) {
  if (env.GDI_FULLENRICH_SPEND_DIR) return path.resolve(env.GDI_FULLENRICH_SPEND_DIR);
  if (env.GDI_FULLENRICH_SPEND_PATH) {
    return path.dirname(path.resolve(env.GDI_FULLENRICH_SPEND_PATH));
  }
  return path.join(ROOT, "data/group-demand-intelligence/fullenrich-spend");
}

export function fullEnrichSpendLedgerPath(env = process.env) {
  if (env.GDI_FULLENRICH_SPEND_PATH) return path.resolve(env.GDI_FULLENRICH_SPEND_PATH);
  return path.join(ledgerDir(env), "gdi-fullenrich-spend-ledger-v1.json");
}

function eventsPath(env = process.env) {
  if (env.GDI_FULLENRICH_SPEND_EVENTS_PATH) {
    return path.resolve(env.GDI_FULLENRICH_SPEND_EVENTS_PATH);
  }
  return path.join(ledgerDir(env), "gdi-fullenrich-spend-events-v1.jsonl");
}

function lockPathFor(file) {
  return `${file}.lock`;
}

function emptyDoc(spendCap = DEFAULT_SPEND_CAP) {
  return {
    version: GDI_FULLENRICH_SPEND_LEDGER_V1,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    spendCap: Number(spendCap) || DEFAULT_SPEND_CAP,
    accountingHealthy: true,
    lastAccountingError: null,
    totals: {
      reservedOpen: 0,
      chargedConfirmed: 0,
      chargedEstimated: 0,
      chargedUnknown: 0,
      releasedUnused: 0,
      eligibleOffers: 0,
      confirmedPaidSubmissions: 0,
      successfulWorkEmailRecoveries: 0,
      noMatches: 0,
      timeouts: 0,
      errors: 0,
      duplicatePaidPrevented: 0,
      recoveredJobs: 0,
    },
    reservations: [],
    jobs: [],
  };
}

function stripForbidden(obj = {}) {
  const out = { ...obj };
  for (const k of FORBIDDEN_PII_KEYS) {
    if (k in out) delete out[k];
  }
  // Nested contactDetails / payload never allowed
  delete out.contactDetails;
  delete out.payload;
  delete out.rawPayload;
  delete out.providerPayload;
  return out;
}

function acquireLedgerLock(lockPath, { retries = 80, delayMs = 25, now = Date.now } = {}) {
  fs.mkdirSync(path.dirname(lockPath), { recursive: true });
  for (let i = 0; i < retries; i += 1) {
    try {
      const fd = fs.openSync(lockPath, "wx");
      fs.writeFileSync(fd, JSON.stringify({ pid: process.pid, at: now() }));
      return {
        release() {
          try {
            fs.closeSync(fd);
          } catch {
            /* ignore */
          }
          try {
            fs.unlinkSync(lockPath);
          } catch {
            /* ignore */
          }
        },
      };
    } catch {
      try {
        const raw = fs.readFileSync(lockPath, "utf8");
        let meta = null;
        try {
          meta = JSON.parse(raw);
        } catch {
          meta = null;
        }
        const st = fs.statSync(lockPath);
        const age = now() - Number(st.mtimeMs || 0);
        const ownerPid = Number(meta?.pid);
        const dead = meta && !pidAlive(ownerPid);
        const aged = age > STALE_LOCK_MS;
        if ((dead && aged) || (!meta && aged)) {
          try {
            fs.unlinkSync(lockPath);
            continue;
          } catch {
            /* race */
          }
        }
      } catch {
        /* disappeared */
      }
      sleepSync(delayMs);
    }
  }
  const err = new Error("FULLENRICH_SPEND_LOCK_UNSAFE");
  err.code = "FULLENRICH_SPEND_LOCK_UNSAFE";
  throw err;
}

function atomicWriteJson(file, doc) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const tmp = `${file}.${process.pid}.${Date.now()}.tmp`;
  fs.writeFileSync(tmp, `${JSON.stringify(doc, null, 2)}\n`);
  fs.renameSync(tmp, file);
}

function appendJsonl(file, row) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.appendFileSync(file, `${JSON.stringify(stripForbidden(row))}\n`);
}

function readDoc(env = process.env) {
  const file = fullEnrichSpendLedgerPath(env);
  if (!fs.existsSync(file)) {
    return { file, doc: emptyDoc(Number(env.GDI_FULLENRICH_SPEND_CAP) || DEFAULT_SPEND_CAP) };
  }
  try {
    const raw = fs.readFileSync(file, "utf8");
    const doc = JSON.parse(raw);
    if (!doc || typeof doc !== "object") throw new Error("invalid");
    doc.reservations = Array.isArray(doc.reservations) ? doc.reservations : [];
    doc.jobs = Array.isArray(doc.jobs) ? doc.jobs : [];
    doc.totals = { ...emptyDoc().totals, ...(doc.totals || {}) };
    if (doc.accountingHealthy !== false) doc.accountingHealthy = true;
    return { file, doc };
  } catch (err) {
    const broken = emptyDoc(Number(env.GDI_FULLENRICH_SPEND_CAP) || DEFAULT_SPEND_CAP);
    broken.accountingHealthy = false;
    broken.lastAccountingError = {
      at: new Date().toISOString(),
      code: "LEDGER_READ_FAILED",
      message: String(err?.message || err).slice(0, 160),
    };
    return { file, doc: broken };
  }
}

function recomputeOpenReserved(doc) {
  let open = 0;
  for (const r of doc.reservations) {
    if (r?.status === FE_CHARGE_STATUS.RESERVED) {
      open += Number(r.creditsReserved) || 0;
    }
  }
  doc.totals.reservedOpen = open;
  return open;
}

function committedSpend(doc) {
  recomputeOpenReserved(doc);
  return (
    Number(doc.totals.reservedOpen || 0) +
    Number(doc.totals.chargedConfirmed || 0) +
    Number(doc.totals.chargedEstimated || 0) +
    Number(doc.totals.chargedUnknown || 0)
  );
}

function withLedger(env, fn) {
  const file = fullEnrichSpendLedgerPath(env);
  fs.mkdirSync(path.dirname(file), { recursive: true });
  let lock;
  try {
    lock = acquireLedgerLock(lockPathFor(file));
  } catch (err) {
    // Fail closed: mark unhealthy if possible, then refuse.
    try {
      const { doc } = readDoc(env);
      doc.accountingHealthy = false;
      doc.lastAccountingError = {
        at: new Date().toISOString(),
        code: err?.code || "LOCK_FAILED",
        message: String(err?.message || err).slice(0, 160),
      };
      atomicWriteJson(file, doc);
    } catch {
      /* ignore */
    }
    return {
      ok: false,
      error: "SPEND_ACCOUNTING_UNSAFE",
      message:
        "FullEnrich spend accounting lock failed. New paid requests are blocked until accounting is healthy.",
    };
  }
  try {
    const live = readDoc(env);
    if (env.GDI_FULLENRICH_SPEND_CAP) {
      live.doc.spendCap =
        Number(env.GDI_FULLENRICH_SPEND_CAP) || live.doc.spendCap || DEFAULT_SPEND_CAP;
    }
    const result = fn(live.doc, live.file);
    live.doc.updatedAt = new Date().toISOString();
    try {
      atomicWriteJson(live.file, live.doc);
    } catch (writeErr) {
      live.doc.accountingHealthy = false;
      live.doc.lastAccountingError = {
        at: new Date().toISOString(),
        code: "LEDGER_WRITE_FAILED",
        message: String(writeErr?.message || writeErr).slice(0, 160),
      };
      try {
        atomicWriteJson(live.file, live.doc);
      } catch {
        /* last resort */
      }
      return {
        ok: false,
        error: "SPEND_ACCOUNTING_UNSAFE",
        message: "FullEnrich spend ledger write failed. New paid requests are blocked.",
        accountingHealthy: false,
      };
    }
    try {
      if (result?.event) appendJsonl(eventsPath(env), result.event);
    } catch {
      /* events are best-effort; ledger is authoritative */
    }
    return result;
  } finally {
    lock.release();
  }
}

export function getFullEnrichSpendBudgetSnapshot(env = process.env) {
  const { doc } = readDoc(env);
  const committed = committedSpend(doc);
  return {
    version: GDI_FULLENRICH_SPEND_LEDGER_V1,
    spendCap: doc.spendCap,
    accountingHealthy: doc.accountingHealthy !== false,
    lastAccountingError: doc.lastAccountingError || null,
    reservedOpen: doc.totals.reservedOpen,
    chargedConfirmed: doc.totals.chargedConfirmed,
    chargedEstimated: doc.totals.chargedEstimated,
    chargedUnknown: doc.totals.chargedUnknown,
    committed,
    remainingBudget: Math.max(0, Number(doc.spendCap) - committed),
    totals: { ...doc.totals },
  };
}

/**
 * Reserve credits before submitting a paid FullEnrich request.
 * Dedupes on clientRequestId — returns existing reservation/job without new spend.
 */
export function reserveFullEnrichSpend(input = {}, env = process.env) {
  const credits = Math.max(
    1,
    Number(input.creditsReserved) || DEFAULT_RESERVE_CREDITS,
  );
  const clientRequestId = text(input.clientRequestId) || null;
  const lookupRequestId = text(input.lookupRequestId) || randomUUID();

  return withLedger(env, (doc) => {
    if (doc.accountingHealthy === false) {
      return {
        ok: false,
        error: "SPEND_ACCOUNTING_UNSAFE",
        message:
          "FullEnrich spend accounting is unhealthy. New paid requests are blocked.",
        accountingHealthy: false,
        lastAccountingError: doc.lastAccountingError,
      };
    }

    if (clientRequestId) {
      const existingJob = doc.jobs.find(
        (j) => j.clientRequestId === clientRequestId,
      );
      const existingRes = doc.reservations.find(
        (r) => r.clientRequestId === clientRequestId,
      );
      if (existingJob || existingRes) {
        doc.totals.duplicatePaidPrevented += 1;
        return {
          ok: true,
          duplicate: true,
          reservationId: existingRes?.reservationId || existingJob?.reservationId || null,
          lookupRequestId: existingJob?.lookupRequestId || lookupRequestId,
          jobId: existingJob?.jobId || null,
          enrichmentId: existingJob?.enrichmentId || null,
          jobStatus: existingJob?.providerStatus || FE_JOB_STATUS.DUPLICATE_SUPPRESSED,
          creditsReserved: existingRes?.creditsReserved || 0,
          chargeStatus: existingRes?.status || null,
          message: "Duplicate client request — reusing existing reservation/job (no new paid submission).",
          event: {
            _type: "duplicate_suppressed",
            at: new Date().toISOString(),
            clientRequestId,
            lookupRequestId: existingJob?.lookupRequestId || lookupRequestId,
          },
        };
      }
    }

    const remaining = Number(doc.spendCap) - committedSpend(doc);
    if (remaining < credits) {
      return {
        ok: false,
        error: "SPEND_CAP_REACHED",
        message: `FullEnrich durable spend cap (${doc.spendCap}) reached (committed=${committedSpend(doc)}).`,
        spendCap: doc.spendCap,
        remainingBudget: Math.max(0, remaining),
        committed: committedSpend(doc),
      };
    }

    const reservationId = randomUUID();
    const jobId = randomUUID();
    const at = new Date().toISOString();
    const reservation = stripForbidden({
      reservationId,
      lookupRequestId,
      clientRequestId,
      hotelId: text(input.hotelId) || null,
      opportunityId: text(input.opportunityId) || null,
      identityKey: text(input.identityKey) || null,
      personIdentityKey: text(input.personIdentityKey) || null,
      channel: "email",
      creditsReserved: credits,
      status: FE_CHARGE_STATUS.RESERVED,
      createdAt: at,
      updatedAt: at,
    });
    const job = stripForbidden({
      jobId,
      lookupRequestId,
      reservationId,
      clientRequestId,
      hotelId: text(input.hotelId) || null,
      opportunityId: text(input.opportunityId) || null,
      identityKey: text(input.identityKey) || null,
      personIdentityKey: text(input.personIdentityKey) || null,
      enrichmentId: null,
      providerStatus: FE_JOB_STATUS.RESERVED,
      submittedAt: null,
      createdAt: at,
      updatedAt: at,
      retryStatus: "NONE",
      reconciliationStatus: "PENDING",
      chargeStatus: FE_CHARGE_STATUS.RESERVED,
      creditsReserved: credits,
      creditsCharged: null,
      creditCertainty: null,
      incrementalEmailFound: false,
      distinguishedFromPublicSource: true,
      error: null,
    });

    doc.reservations.push(reservation);
    doc.jobs.push(job);
    recomputeOpenReserved(doc);

    return {
      ok: true,
      duplicate: false,
      reservationId,
      lookupRequestId,
      jobId,
      creditsReserved: credits,
      chargeStatus: FE_CHARGE_STATUS.RESERVED,
      remainingBudget: Number(doc.spendCap) - committedSpend(doc),
      event: {
        _type: "reserve",
        at,
        reservationId,
        lookupRequestId,
        jobId,
        creditsReserved: credits,
        hotelId: reservation.hotelId,
        opportunityId: reservation.opportunityId,
      },
    };
  });
}

function findJob(doc, { jobId, lookupRequestId, clientRequestId, enrichmentId } = {}) {
  return (
    doc.jobs.find((j) => jobId && j.jobId === jobId) ||
    doc.jobs.find((j) => lookupRequestId && j.lookupRequestId === lookupRequestId) ||
    doc.jobs.find((j) => clientRequestId && j.clientRequestId === clientRequestId) ||
    doc.jobs.find((j) => enrichmentId && j.enrichmentId === enrichmentId) ||
    null
  );
}

function findReservation(doc, reservationId) {
  return doc.reservations.find((r) => r.reservationId === reservationId) || null;
}

/**
 * Attach provider enrichment/job id after successful start (still no PII).
 */
export function markFullEnrichJobSubmitted(input = {}, env = process.env) {
  return withLedger(env, (doc) => {
    const job = findJob(doc, input);
    if (!job) {
      return { ok: false, error: "JOB_NOT_FOUND" };
    }
    const at = new Date().toISOString();
    job.enrichmentId = text(input.enrichmentId) || job.enrichmentId;
    job.providerStatus = FE_JOB_STATUS.SUBMITTED;
    job.submittedAt = at;
    job.updatedAt = at;
    if (job.chargeStatus === FE_CHARGE_STATUS.RESERVED) {
      doc.totals.confirmedPaidSubmissions += 1;
    }
    return {
      ok: true,
      jobId: job.jobId,
      enrichmentId: job.enrichmentId,
      event: {
        _type: "submitted",
        at,
        jobId: job.jobId,
        enrichmentId: job.enrichmentId,
        lookupRequestId: job.lookupRequestId,
      },
    };
  });
}

/**
 * Reconcile reservation after provider outcome.
 * certainty: confirmed | estimated | unknown
 * Never treat timeout/error as free — defaults to UNKNOWN for reserved credits.
 */
export function reconcileFullEnrichSpend(input = {}, env = process.env) {
  return withLedger(env, (doc) => {
    const job = findJob(doc, input);
    if (!job) return { ok: false, error: "JOB_NOT_FOUND" };
    const res = findReservation(doc, job.reservationId);
    const at = new Date().toISOString();
    const providerStatus = text(input.providerStatus) || job.providerStatus;
    const certainty = String(input.creditCertainty || "").toLowerCase();
    const credits =
      Number.isFinite(Number(input.creditsCharged))
        ? Number(input.creditsCharged)
        : Number(job.creditsReserved) || DEFAULT_RESERVE_CREDITS;

    let chargeStatus = FE_CHARGE_STATUS.UNKNOWN;
    if (input.releaseReservation === true) {
      chargeStatus = FE_CHARGE_STATUS.RELEASED;
    } else if (certainty === "confirmed") {
      chargeStatus = FE_CHARGE_STATUS.CONFIRMED;
    } else if (certainty === "estimated") {
      chargeStatus = FE_CHARGE_STATUS.ESTIMATED;
    } else if (certainty === "unknown" || !certainty) {
      // Failures/timeouts/misses without billing confirmation → UNKNOWN (not free).
      chargeStatus = FE_CHARGE_STATUS.UNKNOWN;
    }

    // Move reservation out of RESERVED
    if (res && res.status === FE_CHARGE_STATUS.RESERVED) {
      res.status = chargeStatus;
      res.creditsCharged =
        chargeStatus === FE_CHARGE_STATUS.RELEASED ? 0 : credits;
      res.updatedAt = at;
      recomputeOpenReserved(doc);
      if (chargeStatus === FE_CHARGE_STATUS.CONFIRMED) {
        doc.totals.chargedConfirmed += credits;
      } else if (chargeStatus === FE_CHARGE_STATUS.ESTIMATED) {
        doc.totals.chargedEstimated += credits;
      } else if (chargeStatus === FE_CHARGE_STATUS.UNKNOWN) {
        doc.totals.chargedUnknown += credits;
      } else if (chargeStatus === FE_CHARGE_STATUS.RELEASED) {
        doc.totals.releasedUnused += Number(res.creditsReserved) || 0;
      }
    }

    job.providerStatus = providerStatus;
    job.chargeStatus = chargeStatus;
    job.creditsCharged =
      chargeStatus === FE_CHARGE_STATUS.RELEASED ? 0 : credits;
    job.creditCertainty =
      chargeStatus === FE_CHARGE_STATUS.CONFIRMED
        ? "confirmed"
        : chargeStatus === FE_CHARGE_STATUS.ESTIMATED
          ? "estimated"
          : chargeStatus === FE_CHARGE_STATUS.RELEASED
            ? "released"
            : "unknown";
    job.reconciliationStatus = "RECONCILED";
    job.incrementalEmailFound = input.incrementalEmailFound === true;
    job.distinguishedFromPublicSource = true;
    job.error = text(input.error) || null;
    if (input.enrichmentId) job.enrichmentId = text(input.enrichmentId);
    job.updatedAt = at;

    if (job.incrementalEmailFound) doc.totals.successfulWorkEmailRecoveries += 1;
    if (providerStatus === FE_JOB_STATUS.NO_MATCH) doc.totals.noMatches += 1;
    if (providerStatus === FE_JOB_STATUS.TIMEOUT) doc.totals.timeouts += 1;
    if (providerStatus === FE_JOB_STATUS.ERROR) doc.totals.errors += 1;
    if (providerStatus === FE_JOB_STATUS.RECOVERED) doc.totals.recoveredJobs += 1;

    return {
      ok: true,
      jobId: job.jobId,
      chargeStatus,
      creditsCharged: job.creditsCharged,
      creditCertainty: job.creditCertainty,
      providerStatus: job.providerStatus,
      incrementalEmailFound: job.incrementalEmailFound,
      snapshot: {
        committed: committedSpend(doc),
        remainingBudget: Math.max(0, Number(doc.spendCap) - committedSpend(doc)),
        totals: { ...doc.totals },
      },
      event: {
        _type: "reconcile",
        at,
        jobId: job.jobId,
        lookupRequestId: job.lookupRequestId,
        chargeStatus,
        providerStatus: job.providerStatus,
        creditsCharged: job.creditsCharged,
        incrementalEmailFound: job.incrementalEmailFound,
      },
    };
  });
}

export function recordFullEnrichEligibleOffer(env = process.env) {
  return withLedger(env, (doc) => {
    doc.totals.eligibleOffers += 1;
    return { ok: true, eligibleOffers: doc.totals.eligibleOffers };
  });
}

export function listPendingFullEnrichJobs(env = process.env) {
  const { doc } = readDoc(env);
  return doc.jobs.filter((j) =>
    [FE_JOB_STATUS.RESERVED, FE_JOB_STATUS.SUBMITTED, FE_JOB_STATUS.PENDING, FE_JOB_STATUS.TIMEOUT].includes(
      j.providerStatus,
    ),
  );
}

/** All FE jobs (any status) — used for benchmark eligibility / prior-attempt checks. */
export function listAllFullEnrichJobs(env = process.env) {
  const { doc } = readDoc(env);
  return Array.isArray(doc.jobs) ? doc.jobs.slice() : [];
}

export function readFullEnrichSpendLedgerDoc(env = process.env) {
  return readDoc(env).doc;
}

export function getFullEnrichJob(query = {}, env = process.env) {
  const { doc } = readDoc(env);
  return findJob(doc, query);
}

/**
 * After restart: locate an existing job by client/lookup/enrichment id and mark recoverable.
 * Does NOT start a new paid submission.
 */
export function prepareFullEnrichJobRecovery(query = {}, env = process.env) {
  return withLedger(env, (doc) => {
    const job = findJob(doc, query);
    if (!job) return { ok: false, error: "JOB_NOT_FOUND" };
    if (!job.enrichmentId) {
      return {
        ok: false,
        error: "NO_ENRICHMENT_ID",
        message:
          "Pending job has no enrichmentId yet — do not invent a new paid submission; wait or mark UNKNOWN.",
        jobId: job.jobId,
        providerStatus: job.providerStatus,
      };
    }
    job.retryStatus = "RECOVERY_POLL";
    job.updatedAt = new Date().toISOString();
    return {
      ok: true,
      jobId: job.jobId,
      lookupRequestId: job.lookupRequestId,
      enrichmentId: job.enrichmentId,
      reservationId: job.reservationId,
      providerStatus: job.providerStatus,
      chargeStatus: job.chargeStatus,
      message: "Recover existing enrichment via GET — do not POST a new paid job.",
    };
  });
}

export function markSpendAccountingUnhealthy(error = {}, env = process.env) {
  return withLedger(env, (doc) => {
    doc.accountingHealthy = false;
    doc.lastAccountingError = {
      at: new Date().toISOString(),
      code: text(error.code) || "ACCOUNTING_FAILURE",
      message: text(error.message).slice(0, 160),
    };
    return { ok: true, accountingHealthy: false };
  });
}

export function summarizeFullEnrichSpendMetrics(env = process.env) {
  const snap = getFullEnrichSpendBudgetSnapshot(env);
  const recoveries = Number(snap.totals.successfulWorkEmailRecoveries || 0);
  const confirmed = Number(snap.chargedConfirmed || 0);
  const estimated = Number(snap.chargedEstimated || 0);
  const unknown = Number(snap.chargedUnknown || 0);
  const costCredits = confirmed + estimated + unknown;
  return {
    ...snap,
    metrics: {
      eligibleFallbackRequests: snap.totals.eligibleOffers,
      confirmedPaidSubmissions: snap.totals.confirmedPaidSubmissions,
      successfulWorkEmailRecoveries: recoveries,
      noMatches: snap.totals.noMatches,
      creditsConfirmed: confirmed,
      creditsEstimated: estimated,
      creditsUnknown: unknown,
      incrementalRecoveryRate:
        snap.totals.confirmedPaidSubmissions > 0
          ? Math.round((recoveries / snap.totals.confirmedPaidSubmissions) * 1000) / 10
          : null,
      costPerIncrementalSuccessfulWorkEmail:
        recoveries > 0 ? Math.round((costCredits / recoveries) * 100) / 100 : null,
      duplicatePaidSubmissionsPrevented: snap.totals.duplicatePaidPrevented,
      distinguishedFromPublicSourceGdiContact: true,
    },
  };
}

/** Test helper — wipe ledger directory contents for a given env path. */
export function resetFullEnrichSpendLedgerForTests(env = process.env) {
  const file = fullEnrichSpendLedgerPath(env);
  const dir = path.dirname(file);
  try {
    if (fs.existsSync(file)) fs.unlinkSync(file);
    const lock = lockPathFor(file);
    if (fs.existsSync(lock)) fs.unlinkSync(lock);
    const ev = eventsPath(env);
    if (fs.existsSync(ev)) fs.unlinkSync(ev);
  } catch {
    /* ignore */
  }
  fs.mkdirSync(dir, { recursive: true });
  atomicWriteJson(file, emptyDoc(Number(env.GDI_FULLENRICH_SPEND_CAP) || DEFAULT_SPEND_CAP));
  return getFullEnrichSpendBudgetSnapshot(env);
}

export function hashFullEnrichIdentityKey({
  hotelId,
  opportunityId,
  contactName,
  channel = "email",
} = {}) {
  return createHash("sha256")
    .update(
      [
        String(hotelId || ""),
        String(opportunityId || ""),
        String(contactName || "").toLowerCase().trim(),
        String(channel || "email"),
        "fullenrich",
      ].join("|"),
    )
    .digest("hex")
    .slice(0, 24);
}

export { FORBIDDEN_PII_KEYS, DEFAULT_RESERVE_CREDITS };
