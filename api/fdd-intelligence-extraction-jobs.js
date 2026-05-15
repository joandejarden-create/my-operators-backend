/**
 * In-memory extraction jobs for async full FDD extraction (fees + terms).
 * Supports GET snapshots and SSE progress streams (no secrets in payloads).
 */

/** @typedef {{ stage: string, message?: string, index?: number, total?: number, label?: string, sectionsCount?: number }} FddJobProgressPatch */

/** @type {Map<string, object>} */
const jobs = new Map();

/** Drop finished job snapshots after this delay (memory only). */
const JOB_DELETE_AFTER_MS = 30 * 60 * 1000;

function scheduleJobDeletion(jobId) {
  const t = setTimeout(() => jobs.delete(jobId), JOB_DELETE_AFTER_MS);
  if (typeof t.unref === "function") t.unref();
}

function jobToPublic(job) {
  if (!job) return null;
  return {
    jobId: job.jobId,
    documentId: job.documentId,
    fullExtractionRunId: job.fullExtractionRunId,
    status: job.status,
    stage: job.stage,
    message: job.message,
    index: job.index,
    total: job.total,
    label: job.label,
    sectionsCount: job.sectionsCount,
    startedAt: job.startedAt,
    updatedAt: job.updatedAt,
    httpStatus: job.httpStatus,
    error: job.error,
    result: job.result,
  };
}

function writeSse(res, eventName, payload) {
  res.write(`event: ${eventName}\n`);
  res.write(`data: ${JSON.stringify(payload)}\n\n`);
}

function broadcastProgress(job, payload) {
  for (const client of [...job.sseClients]) {
    try {
      writeSse(client, "progress", payload);
    } catch (_) {
      job.sseClients.delete(client);
    }
  }
}

/**
 * @param {string} documentId
 * @param {string} fullExtractionRunId
 */
export function createFddExtractionJob(documentId, fullExtractionRunId) {
  const jobId = `fddjob_${Date.now()}_${Math.random().toString(36).slice(2, 11)}`;
  const job = {
    jobId,
    documentId,
    fullExtractionRunId,
    status: "running",
    stage: "queued",
    message: "Queued…",
    index: null,
    total: null,
    label: null,
    sectionsCount: null,
    startedAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    httpStatus: null,
    error: null,
    result: null,
    sseClients: new Set(),
  };
  jobs.set(jobId, job);
  return jobId;
}

/**
 * @param {string} jobId
 * @param {FddJobProgressPatch} patch
 */
export function emitFddExtractionJobProgress(jobId, patch) {
  const job = jobs.get(jobId);
  if (!job || job.status !== "running") return;
  if (patch.stage != null) job.stage = patch.stage;
  if (patch.message != null) job.message = patch.message;
  if (patch.index != null) job.index = patch.index;
  if (patch.total != null) job.total = patch.total;
  if (patch.label != null) job.label = patch.label;
  if (patch.sectionsCount != null) job.sectionsCount = patch.sectionsCount;
  job.updatedAt = new Date().toISOString();
  broadcastProgress(job, jobToPublic(job));
}

function closeAllSse(job, finalEvent, finalPayload) {
  for (const res of [...job.sseClients]) {
    try {
      if (finalEvent) writeSse(res, finalEvent, finalPayload);
      res.end();
    } catch (_) {
      /* ignore */
    }
    job.sseClients.delete(res);
  }
}

/**
 * @param {string} jobId
 * @param {{ httpStatus: number, body: object }} payload
 */
export function completeFddExtractionJob(jobId, { httpStatus, body }) {
  const job = jobs.get(jobId);
  if (!job) return;
  job.status = "done";
  job.httpStatus = httpStatus;
  job.result = body;
  job.stage = body && body.ok ? "done" : "done";
  job.message =
    body && body.partialSuccess
      ? "Extraction finished (partial success)."
      : body && body.ok
        ? "Extraction complete."
        : "Extraction finished with errors.";
  job.updatedAt = new Date().toISOString();
  const summary = {
    httpStatus,
    ok: !!(body && body.ok),
    partialSuccess: !!(body && body.partialSuccess),
    documentId: body && body.documentId,
    sectionsCount: body && body.sectionsCount,
    feeRowsCreated: body && body.fees && body.fees.rowsCreated,
    termRowsCreated: body && body.terms && body.terms.rowsCreated,
    warnings: body && Array.isArray(body.warnings) ? body.warnings : [],
  };
  closeAllSse(job, "complete", summary);
  scheduleJobDeletion(jobId);
}

/**
 * @param {string} jobId
 * @param {Error | string} err
 */
export function failFddExtractionJob(jobId, err) {
  const job = jobs.get(jobId);
  if (!job) return;
  const msg = err && err.message ? err.message : String(err || "Extraction failed");
  job.status = "failed";
  job.stage = "failed";
  job.message = msg;
  job.error = msg;
  job.updatedAt = new Date().toISOString();
  closeAllSse(job, "joberror", { message: msg });
  scheduleJobDeletion(jobId);
}

/** @param {string} jobId */
export function getFddExtractionJobSnapshot(jobId) {
  return jobToPublic(jobs.get(jobId));
}

/**
 * SSE stream for one job (Express).
 * @param {string} jobId
 * @param {import("http").IncomingMessage} req
 * @param {import("http").ServerResponse} res
 */
export function attachFddExtractionJobSse(jobId, req, res) {
  const job = jobs.get(jobId);
  if (!job) {
    res.status(404).json({ ok: false, error: "Job not found" });
    return;
  }

  if (job.status !== "running") {
    res.statusCode = 200;
    res.setHeader("Content-Type", "text/event-stream; charset=utf-8");
    res.setHeader("Cache-Control", "no-cache, no-transform");
    res.setHeader("Connection", "keep-alive");
    res.setHeader("X-Accel-Buffering", "no");
    if (typeof res.flushHeaders === "function") res.flushHeaders();
    try {
      writeSse(res, "progress", jobToPublic(job));
      if (job.status === "done") {
        const body = job.result || {};
        const summary = {
          httpStatus: job.httpStatus || 200,
          ok: !!body.ok,
          partialSuccess: !!body.partialSuccess,
          documentId: body.documentId,
          sectionsCount: body.sectionsCount,
          feeRowsCreated: body.fees && body.fees.rowsCreated,
          termRowsCreated: body.terms && body.terms.rowsCreated,
          warnings: Array.isArray(body.warnings) ? body.warnings : [],
        };
        writeSse(res, "complete", summary);
      } else {
        writeSse(res, "joberror", { message: job.error || "Extraction failed" });
      }
      res.end();
    } catch (_) {
      try {
        res.end();
      } catch (__) {
        /* ignore */
      }
    }
    return;
  }

  res.statusCode = 200;
  res.setHeader("Content-Type", "text/event-stream; charset=utf-8");
  res.setHeader("Cache-Control", "no-cache, no-transform");
  res.setHeader("Connection", "keep-alive");
  res.setHeader("X-Accel-Buffering", "no");
  if (typeof res.flushHeaders === "function") res.flushHeaders();

  job.sseClients.add(res);
  try {
    writeSse(res, "progress", jobToPublic(job));
  } catch (_) {
    job.sseClients.delete(res);
    return;
  }

  const ping = setInterval(() => {
    try {
      res.write(": ping\n\n");
    } catch (_) {
      clearInterval(ping);
      job.sseClients.delete(res);
    }
  }, 20000);

  req.on("close", () => {
    clearInterval(ping);
    job.sseClients.delete(res);
  });
}
