/**
 * Durable Parallel job state — persist provider run IDs before poll.
 * Resume must NOT resubmit a known run ID (AASA duplicate lesson).
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { randomBytes } from "node:crypto";

export const PARALLEL_JOB_STORE_VERSION = "gdi-parallel-job-store-v1";

export const PARALLEL_JOB_STATUS = Object.freeze({
  PLANNED: "planned",
  RESERVED: "reserved",
  SUBMITTED: "submitted",
  RUNNING: "running",
  COMPLETED: "completed",
  FAILED: "failed",
  UNCERTAIN: "uncertain",
  CANCELLED: "cancelled",
});

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

function storeRoot(env = process.env) {
  if (env.GDI_PARALLEL_JOB_STORE_PATH) {
    return path.resolve(env.GDI_PARALLEL_JOB_STORE_PATH);
  }
  const root = env.GDI_DATA_ROOT
    ? path.resolve(env.GDI_DATA_ROOT)
    : path.join(ROOT, "data/group-demand-intelligence");
  return path.join(root, "parallel-jobs");
}

function jobPath(jobKey, env) {
  const safe = String(jobKey || "unknown").replace(/[^a-zA-Z0-9._-]+/g, "_");
  return path.join(storeRoot(env), `${safe}.json`);
}

export function loadParallelJob(jobKey, env = process.env) {
  const p = jobPath(jobKey, env);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

export function saveParallelJob(job, env = process.env) {
  const p = jobPath(job.jobKey, env);
  fs.mkdirSync(path.dirname(p), { recursive: true });
  const next = {
    ...job,
    version: PARALLEL_JOB_STORE_VERSION,
    updatedAt: new Date().toISOString(),
  };
  const tmp = `${p}.${process.pid}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(next, null, 2) + "\n");
  fs.renameSync(tmp, p);
  return next;
}

/**
 * Create or resume a job. If providerRunId already known → do not resubmit.
 */
export function beginOrResumeParallelJob({
  jobKey,
  hotelId,
  label = null,
  env = process.env,
} = {}) {
  const existing = loadParallelJob(jobKey, env);
  if (existing?.providerRunId) {
    return {
      job: existing,
      resume: true,
      shouldSubmit: false,
      reason: "known_provider_run_id_do_not_resubmit",
      providerRunId: existing.providerRunId,
    };
  }
  if (
    existing &&
    (existing.status === PARALLEL_JOB_STATUS.SUBMITTED ||
      existing.status === PARALLEL_JOB_STATUS.RUNNING ||
      existing.status === PARALLEL_JOB_STATUS.UNCERTAIN)
  ) {
    return {
      job: existing,
      resume: true,
      shouldSubmit: false,
      reason: "uncertain_or_in_flight_do_not_resubmit",
      providerRunId: existing.providerRunId || null,
    };
  }
  const job = saveParallelJob(
    {
      jobKey,
      hotelId,
      label,
      status: PARALLEL_JOB_STATUS.PLANNED,
      providerRunId: null,
      reservationId: null,
      createdAt: existing?.createdAt || new Date().toISOString(),
      attempts: existing?.attempts || 0,
    },
    env
  );
  return {
    job,
    resume: Boolean(existing),
    shouldSubmit: true,
    reason: "new_or_retryable",
    providerRunId: null,
  };
}

export function markParallelJobSubmitted(jobKey, { providerRunId, reservationId }, env = process.env) {
  const job = loadParallelJob(jobKey, env) || { jobKey };
  if (job.providerRunId && job.providerRunId !== providerRunId) {
    const err = new Error("parallel_job_run_id_conflict");
    err.code = "PARALLEL_JOB_RUN_ID_CONFLICT";
    err.existing = job.providerRunId;
    err.attempted = providerRunId;
    throw err;
  }
  return saveParallelJob(
    {
      ...job,
      status: PARALLEL_JOB_STATUS.SUBMITTED,
      providerRunId,
      reservationId: reservationId || job.reservationId || null,
      submittedAt: new Date().toISOString(),
      attempts: Number(job.attempts || 0) + 1,
    },
    env
  );
}

export function markParallelJobUncertain(jobKey, reason, env = process.env) {
  const job = loadParallelJob(jobKey, env) || { jobKey };
  return saveParallelJob(
    {
      ...job,
      status: PARALLEL_JOB_STATUS.UNCERTAIN,
      uncertainReason: reason || "submission_timeout_or_unknown_remote_fate",
      uncertainAt: new Date().toISOString(),
    },
    env
  );
}

export function markParallelJobCompleted(jobKey, resultMeta = {}, env = process.env) {
  const job = loadParallelJob(jobKey, env) || { jobKey };
  return saveParallelJob(
    {
      ...job,
      status: PARALLEL_JOB_STATUS.COMPLETED,
      completedAt: new Date().toISOString(),
      resultMeta: {
        candidateCount: resultMeta.candidateCount ?? null,
        paidCalls: resultMeta.paidCalls ?? 0,
        mode: resultMeta.mode || null,
      },
    },
    env
  );
}

export function createParallelJobKey(hotelId, segmentId) {
  return `gdi-parallel-${String(hotelId).slice(0, 12)}-${segmentId || randomBytes(3).toString("hex")}`;
}
