/**
 * Durable Exa Agent deep-research job store — mirror Parallel job-store discipline.
 * Persist provider run IDs before poll. Resume must NOT resubmit a known run ID.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";

export const GDI_EXA_AGENT_DEEP_JOB_STORE_V1 = "gdi-exa-agent-deep-job-store-v1";

export const EXA_AGENT_DEEP_JOB_STATUS = Object.freeze({
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
const ROOT = path.resolve(__dirname, "../../../..");

function storeRoot(env = process.env) {
  if (env.GDI_EXA_AGENT_DEEP_JOB_STORE_PATH) {
    return path.resolve(env.GDI_EXA_AGENT_DEEP_JOB_STORE_PATH);
  }
  const root = env.GDI_DATA_ROOT
    ? path.resolve(env.GDI_DATA_ROOT)
    : path.join(ROOT, "data/group-demand-intelligence");
  return path.join(root, "exa-agent-deep-jobs");
}

export function buildExaAgentDeepJobKey({
  authorizationId = null,
  hotelId = null,
  opportunityId = null,
  researchPurpose = "DEEP_RESEARCH",
  contractId = null,
} = {}) {
  const raw = [
    authorizationId || "no_auth",
    hotelId || "no_hotel",
    opportunityId || "no_opp",
    researchPurpose || "DEEP_RESEARCH",
    contractId || "default_contract",
  ].join("|");
  const hash = createHash("sha256").update(raw).digest("hex").slice(0, 16);
  const safeOpp = String(opportunityId || "opp").replace(/[^a-zA-Z0-9._-]+/g, "_").slice(0, 48);
  return `exa_deep_${safeOpp}_${hash}`;
}

function jobPath(jobKey, env) {
  const safe = String(jobKey || "unknown").replace(/[^a-zA-Z0-9._-]+/g, "_");
  return path.join(storeRoot(env), `${safe}.json`);
}

export function loadExaAgentDeepJob(jobKey, env = process.env) {
  const p = jobPath(jobKey, env);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

export function saveExaAgentDeepJob(job, env = process.env) {
  const p = jobPath(job.jobKey, env);
  fs.mkdirSync(path.dirname(p), { recursive: true });
  const next = {
    ...job,
    version: GDI_EXA_AGENT_DEEP_JOB_STORE_V1,
    updatedAt: new Date().toISOString(),
  };
  const tmp = `${p}.${process.pid}.tmp`;
  fs.writeFileSync(tmp, `${JSON.stringify(next, null, 2)}\n`);
  fs.renameSync(tmp, p);
  return next;
}

/**
 * Create or resume. If providerRunId known → do not resubmit.
 */
export function beginOrResumeExaAgentDeepJob({
  jobKey,
  hotelId = null,
  opportunityId = null,
  authorizationId = null,
  contractId = null,
  env = process.env,
} = {}) {
  const existing = loadExaAgentDeepJob(jobKey, env);
  if (existing?.providerRunId) {
    const completed = existing.status === EXA_AGENT_DEEP_JOB_STATUS.COMPLETED;
    return {
      job: existing,
      resume: true,
      shouldSubmit: false,
      reuseCompleted: completed,
      reason: completed
        ? "completed_provider_run_reuse"
        : "known_provider_run_id_do_not_resubmit",
      providerRunId: existing.providerRunId,
    };
  }
  if (
    existing &&
    (existing.status === EXA_AGENT_DEEP_JOB_STATUS.SUBMITTED ||
      existing.status === EXA_AGENT_DEEP_JOB_STATUS.RUNNING ||
      existing.status === EXA_AGENT_DEEP_JOB_STATUS.UNCERTAIN ||
      existing.status === EXA_AGENT_DEEP_JOB_STATUS.RESERVED)
  ) {
    return {
      job: existing,
      resume: true,
      shouldSubmit: false,
      reuseCompleted: false,
      reason: "in_flight_do_not_resubmit",
      providerRunId: existing.providerRunId || null,
    };
  }
  const job = saveExaAgentDeepJob(
    {
      jobKey,
      hotelId,
      opportunityId,
      authorizationId,
      contractId,
      status: EXA_AGENT_DEEP_JOB_STATUS.PLANNED,
      providerRunId: null,
      costUsd: null,
      createdAt: existing?.createdAt || new Date().toISOString(),
      attempts: Number(existing?.attempts || 0),
    },
    env,
  );
  return {
    job,
    resume: Boolean(existing),
    shouldSubmit: true,
    reuseCompleted: false,
    reason: existing?.status === EXA_AGENT_DEEP_JOB_STATUS.FAILED ? "retry_after_failed" : "new_or_retryable",
    providerRunId: null,
  };
}

export function markExaAgentDeepJobSubmitted(
  jobKey,
  { providerRunId, costUsd = null } = {},
  env = process.env,
) {
  const job = loadExaAgentDeepJob(jobKey, env) || { jobKey };
  if (job.providerRunId && job.providerRunId !== providerRunId) {
    const err = new Error("exa_agent_deep_job_run_id_conflict");
    err.code = "EXA_DEEP_JOB_RUN_ID_CONFLICT";
    err.existing = job.providerRunId;
    err.incoming = providerRunId;
    throw err;
  }
  return saveExaAgentDeepJob(
    {
      ...job,
      providerRunId,
      status: EXA_AGENT_DEEP_JOB_STATUS.SUBMITTED,
      costUsd: costUsd != null ? Number(costUsd) : job.costUsd,
      attempts: Number(job.attempts || 0) + 1,
      submittedAt: new Date().toISOString(),
    },
    env,
  );
}

export function markExaAgentDeepJobCompleted(
  jobKey,
  { providerRunId = null, costUsd = null, resultSummary = null } = {},
  env = process.env,
) {
  const job = loadExaAgentDeepJob(jobKey, env) || { jobKey };
  return saveExaAgentDeepJob(
    {
      ...job,
      providerRunId: providerRunId || job.providerRunId,
      status: EXA_AGENT_DEEP_JOB_STATUS.COMPLETED,
      costUsd: costUsd != null ? Number(costUsd) : job.costUsd,
      resultSummary,
      completedAt: new Date().toISOString(),
    },
    env,
  );
}

export function markExaAgentDeepJobFailed(jobKey, { error = null } = {}, env = process.env) {
  const job = loadExaAgentDeepJob(jobKey, env) || { jobKey };
  return saveExaAgentDeepJob(
    {
      ...job,
      status: EXA_AGENT_DEEP_JOB_STATUS.FAILED,
      lastError: String(error || "unknown"),
      failedAt: new Date().toISOString(),
    },
    env,
  );
}

export default {
  GDI_EXA_AGENT_DEEP_JOB_STORE_V1,
  EXA_AGENT_DEEP_JOB_STATUS,
  buildExaAgentDeepJobKey,
  loadExaAgentDeepJob,
  saveExaAgentDeepJob,
  beginOrResumeExaAgentDeepJob,
  markExaAgentDeepJobSubmitted,
  markExaAgentDeepJobCompleted,
  markExaAgentDeepJobFailed,
};
