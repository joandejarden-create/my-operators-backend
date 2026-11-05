/**
 * Bethesda additive maturation checkpoint validation + resume helpers.
 * Public-only resume; does not authorize paid spend or publication.
 */

import fs from "node:fs";
import path from "node:path";
import { RESEARCH_TERMINAL } from "./research-constants-v1.js";

export const CHECKPOINT_STATUS = Object.freeze({
  VALID_COMPLETE: "VALID_COMPLETE",
  INCOMPLETE_IN_PROGRESS: "INCOMPLETE_IN_PROGRESS",
  INCOMPLETE_SCHEMA: "INCOMPLETE_SCHEMA",
  INCOMPLETE_MISSING_ORG: "INCOMPLETE_MISSING_ORG",
  CORRUPT_PARSE: "CORRUPT_PARSE",
  CORRUPT_ID_MISMATCH: "CORRUPT_ID_MISMATCH",
  NOT_ATTEMPTED: "NOT_ATTEMPTED",
});

const TERMINAL = new Set([
  RESEARCH_TERMINAL.PUBLISHABLE_PROSPECT,
  RESEARCH_TERMINAL.EXPLORATORY_PROSPECT,
  RESEARCH_TERMINAL.STOPPED_WITH_LEGITIMATE_REASON,
]);

export function maturationMemoryFileName(opportunityId) {
  return `${String(opportunityId || "x").replace(/[^\w.-]+/g, "_")}.json`;
}

export function maturationMemoryPath(memDir, opportunityId) {
  return path.join(memDir, maturationMemoryFileName(opportunityId));
}

/**
 * Atomic-ish write: write temp then rename.
 * On Windows, rename over an existing target can EPERM — fall back to overwrite.
 */
export function writeMaturationCheckpointAtomic(memDir, state) {
  fs.mkdirSync(memDir, { recursive: true });
  const id = state?.id || state?.opportunityId || "x";
  const target = maturationMemoryPath(memDir, id);
  const tmp = `${target}.${process.pid}.${Date.now()}.tmp`;
  const body = JSON.stringify(state, null, 2) + "\n";
  fs.writeFileSync(tmp, body);
  try {
    fs.renameSync(tmp, target);
  } catch (err) {
    if (err?.code === "EPERM" || err?.code === "EEXIST" || err?.code === "EACCES") {
      try {
        fs.writeFileSync(target, body);
      } finally {
        try {
          fs.rmSync(tmp, { force: true });
        } catch {
          /* ignore temp cleanup */
        }
      }
    } else {
      try {
        fs.rmSync(tmp, { force: true });
      } catch {
        /* ignore */
      }
      throw err;
    }
  }
  return target;
}

/**
 * Validate a checkpoint file for a queue candidate.
 * File existence alone is NOT success.
 */
export function validateMaturationCheckpoint(memDir, candidate = {}) {
  const id = String(candidate.id || candidate.opportunityId || "");
  const filePath = maturationMemoryPath(memDir, id);
  if (!fs.existsSync(filePath)) {
    return {
      status: CHECKPOINT_STATUS.NOT_ATTEMPTED,
      id,
      path: filePath,
      reusable: false,
      retry: true,
    };
  }

  let raw;
  try {
    raw = fs.readFileSync(filePath, "utf8");
  } catch (err) {
    return {
      status: CHECKPOINT_STATUS.CORRUPT_PARSE,
      id,
      path: filePath,
      error: err?.message || String(err),
      reusable: false,
      retry: true,
    };
  }

  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch (err) {
    return {
      status: CHECKPOINT_STATUS.CORRUPT_PARSE,
      id,
      path: filePath,
      error: err?.message || String(err),
      reusable: false,
      retry: true,
    };
  }

  const fileId = String(parsed.id || parsed.opportunityId || "");
  if (fileId && id && fileId !== id) {
    return {
      status: CHECKPOINT_STATUS.CORRUPT_ID_MISMATCH,
      id,
      fileId,
      path: filePath,
      reusable: false,
      retry: true,
    };
  }

  if (!parsed.organizationName && !parsed.company) {
    return {
      status: CHECKPOINT_STATUS.INCOMPLETE_MISSING_ORG,
      id,
      path: filePath,
      reusable: false,
      retry: true,
      state: parsed,
    };
  }

  const rs = String(parsed.researchStatus || "");
  if (rs === RESEARCH_TERMINAL.IN_PROGRESS) {
    return {
      status: CHECKPOINT_STATUS.INCOMPLETE_IN_PROGRESS,
      id,
      path: filePath,
      researchStatus: rs,
      reusable: false,
      retry: true,
      state: parsed,
    };
  }

  const attempts = Array.isArray(parsed.researchAttempts)
    ? parsed.researchAttempts.length
    : 0;
  const hasResearchSignal =
    attempts > 0 ||
    Boolean(parsed.stopReason) ||
    Boolean(parsed.whyNow) ||
    Boolean(parsed.summaryWhat) ||
    Boolean(parsed.buyerRole) ||
    parsed.packetComplete != null ||
    (Array.isArray(parsed.evidence) && parsed.evidence.length > 0);

  if (TERMINAL.has(rs) || (hasResearchSignal && rs && rs !== RESEARCH_TERMINAL.IN_PROGRESS)) {
    // Normalize terminal status for resume skip path
    const normalized = {
      ...parsed,
      id: parsed.id || id,
      researchStatus: TERMINAL.has(rs)
        ? rs
        : RESEARCH_TERMINAL.STOPPED_WITH_LEGITIMATE_REASON,
    };
    return {
      status: CHECKPOINT_STATUS.VALID_COMPLETE,
      id,
      path: filePath,
      researchStatus: normalized.researchStatus,
      stopReason: parsed.stopReason || null,
      attempts,
      bytes: Buffer.byteLength(raw),
      reusable: true,
      retry: false,
      state: normalized,
    };
  }

  if (hasResearchSignal && !rs) {
    const normalized = {
      ...parsed,
      id: parsed.id || id,
      researchStatus: RESEARCH_TERMINAL.STOPPED_WITH_LEGITIMATE_REASON,
      stopReason: parsed.stopReason || "CHECKPOINT_TERMINAL_INFERRED",
    };
    return {
      status: CHECKPOINT_STATUS.VALID_COMPLETE,
      id,
      path: filePath,
      researchStatus: normalized.researchStatus,
      stopReason: normalized.stopReason,
      attempts,
      bytes: Buffer.byteLength(raw),
      reusable: true,
      retry: false,
      state: normalized,
      note: "Inferred terminal from research content (missing researchStatus)",
    };
  }

  return {
    status: CHECKPOINT_STATUS.INCOMPLETE_SCHEMA,
    id,
    path: filePath,
    researchStatus: rs || null,
    attempts,
    reusable: false,
    retry: true,
    state: parsed,
  };
}

/**
 * Split authoritative queue into reusable completions vs work remaining.
 * Duplicate ids in the queue: first occurrence owns the checkpoint; later
 * duplicates reuse the same validation result (shared memory file).
 */
export function planMaturationResume(queue = [], memDir) {
  const byId = new Map();
  const rows = [];
  const skip = [];
  const resume = [];

  for (const candidate of queue) {
    const id = String(candidate.id || candidate.opportunityId || "");
    let validation = byId.get(id);
    if (!validation) {
      validation = validateMaturationCheckpoint(memDir, candidate);
      byId.set(id, validation);
    }

    const row = {
      id,
      organizationName: candidate.organizationName || candidate.company || null,
      segment: candidate.segment || candidate.researchSegment || null,
      checkpointStatus: validation.status,
      reusable: validation.reusable === true,
      retry: validation.retry !== false,
      path: validation.path || null,
      researchStatus: validation.researchStatus || null,
      stopReason: validation.stopReason || null,
      note: validation.note || null,
    };
    rows.push(row);

    if (validation.reusable && validation.state) {
      skip.push({
        ...validation.state,
        _resumedFromCheckpoint: true,
        _checkpointStatus: validation.status,
        // Preserve queue segment if checkpoint lacked it
        segment: validation.state.segment || candidate.segment,
        researchSegment:
          validation.state.researchSegment ||
          candidate.researchSegment ||
          candidate.segment,
        _recoveredFromFalseQuarantine:
          candidate._recoveredFromFalseQuarantine === true ||
          validation.state._recoveredFromFalseQuarantine === true,
      });
    } else {
      resume.push({
        ...candidate,
        _checkpointStatus: validation.status,
        _resumeRequired: true,
      });
    }
  }

  return {
    queueTotal: queue.length,
    uniqueIds: byId.size,
    rows,
    skip,
    resume,
    counts: {
      VALID_COMPLETE: rows.filter(
        (r) => r.checkpointStatus === CHECKPOINT_STATUS.VALID_COMPLETE
      ).length,
      INCOMPLETE: rows.filter((r) =>
        String(r.checkpointStatus).startsWith("INCOMPLETE")
      ).length,
      CORRUPT: rows.filter((r) =>
        String(r.checkpointStatus).startsWith("CORRUPT")
      ).length,
      NOT_ATTEMPTED: rows.filter(
        (r) => r.checkpointStatus === CHECKPOINT_STATUS.NOT_ATTEMPTED
      ).length,
      resumeCount: resume.length,
      skipCount: skip.length,
    },
  };
}
