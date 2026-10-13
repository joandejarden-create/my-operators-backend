/**
 * Durable research-case store for hotel ownership/contact workflow.
 * Staging only — never writes Census/canonical ownership.
 *
 * v2 repairs (Astra F2/F4):
 * - Strict case IDs (reject lossy collisions)
 * - Exclusive lock acquisition + lease fencing/renewal
 * - putCase requires matching lock token when locked
 * - Atomic case file replacement scoped to this store
 * - Corruption ≠ missing case
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import crypto from "node:crypto";
import { ensureDir } from "../local-store.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

export const RESEARCH_CASE_STORE_VERSION = "research-case-store-v2";

const CASE_ID_RE = /^[a-zA-Z0-9][a-zA-Z0-9_-]{0,119}$/;

function nowIso() {
  return new Date().toISOString();
}

function defaultRoot(env = process.env) {
  if (env.RESEARCH_CASE_DATA_ROOT) {
    return path.resolve(String(env.RESEARCH_CASE_DATA_ROOT));
  }
  if (env.CONTACT_INTELLIGENCE_DATA_ROOT) {
    return path.join(path.resolve(String(env.CONTACT_INTELLIGENCE_DATA_ROOT)), "research-cases");
  }
  if (env.HOTEL_INTELLIGENCE_DATA_DIR) {
    return path.join(
      path.resolve(String(env.HOTEL_INTELLIGENCE_DATA_DIR)),
      "contact-intelligence",
      "research-cases"
    );
  }
  return path.join(ROOT, "data", "hotel-intelligence", "contact-intelligence", "research-cases");
}

export function assertValidCaseId(id) {
  const s = String(id || "");
  if (!CASE_ID_RE.test(s)) {
    const err = new Error("INVALID_CASE_ID");
    err.code = "INVALID_CASE_ID";
    err.case_id = id;
    throw err;
  }
  return s;
}

/** @deprecated Prefer assertValidCaseId — kept for callers that only need sanitization of new generated IDs */
function safeId(id) {
  return assertValidCaseId(id);
}

function writeJsonAtomic(filePath, data) {
  ensureDir(path.dirname(filePath));
  const payload = `${JSON.stringify(data, null, 2)}\n`;
  const tmp = `${filePath}.${process.pid}.${Date.now()}.${crypto.randomBytes(3).toString("hex")}.tmp`;
  fs.writeFileSync(tmp, payload, "utf8");
  try {
    const fd = fs.openSync(tmp, "r+");
    try {
      fs.fsyncSync(fd);
    } finally {
      fs.closeSync(fd);
    }
  } catch {
    /* fsync best-effort */
  }
  try {
    fs.renameSync(tmp, filePath);
  } catch (err) {
    // Windows: destination may exist — replace via unlink then rename
    if (err && (err.code === "EEXIST" || err.code === "EPERM" || err.code === "EACCES")) {
      try {
        if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
      } catch {
        /* continue to rename attempt */
      }
      fs.renameSync(tmp, filePath);
    } else {
      try {
        fs.unlinkSync(tmp);
      } catch {
        /* ignore */
      }
      throw err;
    }
  }
}

function readCaseJson(filePath) {
  if (!fs.existsSync(filePath)) {
    return { ok: false, missing: true, case: null };
  }
  let raw;
  try {
    const buf = fs.readFileSync(filePath);
    if (!buf.length || buf.every((b) => b === 0)) {
      return { ok: false, corrupt: true, reason: "EMPTY_OR_ZEROED", case: null };
    }
    raw = buf.toString("utf8").replace(/^\uFEFF/, "");
    if (!raw.trim() || raw.trimStart().startsWith("\u0000")) {
      return { ok: false, corrupt: true, reason: "EMPTY_OR_NULL_BYTES", case: null };
    }
    const parsed = JSON.parse(raw);
    return { ok: true, case: parsed };
  } catch (err) {
    return {
      ok: false,
      corrupt: true,
      reason: err?.message || "JSON_PARSE_FAILED",
      case: null,
    };
  }
}

/**
 * @param {{ root?: string, env?: object }} opts
 */
export function createResearchCaseStore(opts = {}) {
  const root = opts.root || defaultRoot(opts.env || process.env);
  ensureDir(root);
  ensureDir(path.join(root, "cases"));
  ensureDir(path.join(root, "owner-reuse"));
  ensureDir(path.join(root, "locks"));
  ensureDir(path.join(root, "journals"));

  function casePath(caseId) {
    return path.join(root, "cases", `${assertValidCaseId(caseId)}.json`);
  }

  function ownerReusePath(ownerId) {
    const oid = String(ownerId || "");
    if (!CASE_ID_RE.test(oid)) {
      const err = new Error("INVALID_OWNER_ID");
      err.code = "INVALID_OWNER_ID";
      throw err;
    }
    return path.join(root, "owner-reuse", `${oid}.json`);
  }

  function lockPath(caseId) {
    return path.join(root, "locks", `${assertValidCaseId(caseId)}.lock`);
  }

  function journalPath(caseId) {
    return path.join(root, "journals", `${assertValidCaseId(caseId)}.json`);
  }

  function getCase(caseId) {
    if (!caseId) return null;
    let id;
    try {
      id = assertValidCaseId(caseId);
    } catch {
      return null;
    }
    const result = readCaseJson(casePath(id));
    if (result.corrupt) {
      const err = new Error("CASE_CORRUPT");
      err.code = "CASE_CORRUPT";
      err.case_id = id;
      err.reason = result.reason;
      throw err;
    }
    return result.case;
  }

  function getCaseResult(caseId) {
    if (!caseId) return { ok: false, missing: true };
    let id;
    try {
      id = assertValidCaseId(caseId);
    } catch (e) {
      return { ok: false, invalid_id: true, error: e.code || "INVALID_CASE_ID" };
    }
    const result = readCaseJson(casePath(id));
    if (result.corrupt) {
      return { ok: false, corrupt: true, reason: result.reason, case_id: id };
    }
    if (result.missing) return { ok: false, missing: true, case_id: id };
    return { ok: true, case: result.case };
  }

  function listCases() {
    const dir = path.join(root, "cases");
    if (!fs.existsSync(dir)) return [];
    return fs
      .readdirSync(dir)
      .filter((f) => f.endsWith(".json"))
      .map((f) => {
        const r = readCaseJson(path.join(dir, f));
        return r.ok ? r.case : null;
      })
      .filter(Boolean);
  }

  function readLock(caseId) {
    const lp = lockPath(caseId);
    if (!fs.existsSync(lp)) return null;
    try {
      return JSON.parse(fs.readFileSync(lp, "utf8"));
    } catch {
      return null;
    }
  }

  /**
   * Exclusive lock with fencing token. Expired locks may be stolen once.
   */
  function tryAcquireLock(caseId, ttlMs = 120_000) {
    assertValidCaseId(caseId);
    const lp = lockPath(caseId);
    ensureDir(path.dirname(lp));
    const now = Date.now();
    const fence = `f_${crypto.randomBytes(8).toString("hex")}`;
    const lock = {
      case_id: caseId,
      lock_token: `lk_${crypto.randomBytes(6).toString("hex")}`,
      fence_token: fence,
      acquired_at: nowIso(),
      renewed_at: nowIso(),
      expires_at_ms: now + ttlMs,
      ttl_ms: ttlMs,
      pid: process.pid,
    };

    try {
      const fd = fs.openSync(lp, "wx");
      try {
        fs.writeFileSync(fd, JSON.stringify(lock, null, 2));
      } finally {
        fs.closeSync(fd);
      }
      return { ok: true, lock };
    } catch (err) {
      if (err?.code !== "EEXIST") {
        return { ok: false, reason: "LOCK_IO_ERROR", error: err.message };
      }
    }

    const existing = readLock(caseId);
    if (existing?.expires_at_ms && existing.expires_at_ms > now) {
      return { ok: false, reason: "CASE_LOCKED", lock: existing };
    }

    // Expired — remove and retry exclusive create once
    try {
      fs.unlinkSync(lp);
    } catch {
      return { ok: false, reason: "CASE_LOCKED", lock: existing };
    }
    try {
      const fd = fs.openSync(lp, "wx");
      try {
        fs.writeFileSync(fd, JSON.stringify(lock, null, 2));
      } finally {
        fs.closeSync(fd);
      }
      return { ok: true, lock, stole_expired: true };
    } catch {
      return { ok: false, reason: "CASE_LOCKED", lock: readLock(caseId) };
    }
  }

  function renewLease(caseId, lockToken, ttlMs) {
    const existing = readLock(caseId);
    if (!existing) return { ok: false, reason: "LOCK_MISSING" };
    if (lockToken && existing.lock_token !== lockToken) {
      return { ok: false, reason: "LOCK_TOKEN_MISMATCH" };
    }
    const ttl = ttlMs || existing.ttl_ms || 120_000;
    const next = {
      ...existing,
      renewed_at: nowIso(),
      expires_at_ms: Date.now() + ttl,
      fence_token: existing.fence_token || `f_${crypto.randomBytes(8).toString("hex")}`,
    };
    writeJsonAtomic(lockPath(caseId), next);
    return { ok: true, lock: next };
  }

  function releaseLock(caseId, lockToken) {
    const lp = lockPath(caseId);
    if (!fs.existsSync(lp)) return { ok: true };
    const existing = readLock(caseId);
    if (lockToken && existing && existing.lock_token !== lockToken) {
      return { ok: false, reason: "LOCK_TOKEN_MISMATCH" };
    }
    try {
      fs.unlinkSync(lp);
    } catch {
      /* ignore */
    }
    return { ok: true };
  }

  function assertWritable(caseId, writeOpts = {}) {
    const existingLock = readLock(caseId);
    if (!existingLock) return { ok: true };
    if (!writeOpts.lock_token) {
      return { ok: false, reason: "LOCK_TOKEN_REQUIRED", lock: existingLock };
    }
    if (existingLock.lock_token !== writeOpts.lock_token) {
      return { ok: false, reason: "LOCK_TOKEN_MISMATCH", lock: existingLock };
    }
    if (existingLock.expires_at_ms && existingLock.expires_at_ms < Date.now()) {
      return { ok: false, reason: "LOCK_EXPIRED", lock: existingLock };
    }
    if (
      writeOpts.fence_token &&
      existingLock.fence_token &&
      writeOpts.fence_token !== existingLock.fence_token
    ) {
      return { ok: false, reason: "FENCE_TOKEN_MISMATCH", lock: existingLock };
    }
    return { ok: true, lock: existingLock };
  }

  /**
   * Upsert case with checkpoint history. Requires lock token when locked.
   */
  function putCase(caseRecord, writeOpts = {}) {
    if (!caseRecord?.case_id) throw new Error("case_id_required");
    assertValidCaseId(caseRecord.case_id);

    const gate = assertWritable(caseRecord.case_id, writeOpts);
    if (!gate.ok) {
      return { ok: false, conflict: true, reason: gate.reason, lock: gate.lock };
    }

    const loaded = getCaseResult(caseRecord.case_id);
    if (loaded.corrupt) {
      return { ok: false, corrupt: true, reason: loaded.reason };
    }
    const existing = loaded.case || null;

    if (
      writeOpts.if_updated_before &&
      existing?.updated_at &&
      String(existing.updated_at) > String(writeOpts.if_updated_before)
    ) {
      return {
        ok: false,
        conflict: true,
        reason: "newer_case_exists",
        existing,
      };
    }

    if (
      writeOpts.expected_revision != null &&
      existing &&
      Number(existing.revision || 0) !== Number(writeOpts.expected_revision)
    ) {
      return {
        ok: false,
        conflict: true,
        reason: "revision_mismatch",
        existing,
      };
    }

    const checkpoints = Array.isArray(existing?.checkpoints) ? existing.checkpoints.slice() : [];
    if (caseRecord.checkpoint) {
      checkpoints.push({
        ...caseRecord.checkpoint,
        at: nowIso(),
      });
    }

    const operation_journal = (() => {
      // Prefer an explicitly supplied journal that is at least as long as the
      // existing one (accounting corrections / append-only resume writes).
      if (
        Array.isArray(caseRecord.operation_journal) &&
        caseRecord.operation_journal.length >=
          (Array.isArray(existing?.operation_journal) ? existing.operation_journal.length : 0)
      ) {
        return caseRecord.operation_journal.slice();
      }
      const base = Array.isArray(existing?.operation_journal)
        ? existing.operation_journal.slice()
        : [];
      if (caseRecord.journal_entry) {
        base.push({
          ...caseRecord.journal_entry,
          at: nowIso(),
        });
      }
      return base;
    })();

    const incomingBu = caseRecord.budget_usage || {};
    const existingBu = existing?.budget_usage || {};
    // Absolute correction path: provider-confirmed reconcile may decrease an
    // inflated snapshot. Math.max must not resurrect the prior value.
    const absoluteSpentCorrection =
      incomingBu.context_dev_absolute_spent_correction === true ||
      (incomingBu.context_dev_reconciliation_id &&
        incomingBu.context_dev_spent != null &&
        Number(incomingBu.context_dev_spent) < Number(existingBu.context_dev_spent || 0) &&
        (incomingBu.context_dev_reconciliation_source === "provider_key_metadata_per_request" ||
          incomingBu.context_dev_spend_source === "provider_key_metadata_per_request"));

    const next = {
      version: RESEARCH_CASE_STORE_VERSION,
      ...(existing || {}),
      ...caseRecord,
      created_at: existing?.created_at || caseRecord.created_at || nowIso(),
      updated_at: nowIso(),
      revision: Number(existing?.revision || 0) + 1,
      checkpoints: checkpoints.slice(-100),
      operation_journal: operation_journal.slice(-500),
      budget_usage: {
        ...existingBu,
        ...incomingBu,
        // Cumulative counters: never let an explicit zero wipe prior spend on resume
        // unless an explicit absolute reconciliation correction is supplied.
        context_dev_reserved: Math.max(
          Number(existingBu.context_dev_reserved || 0),
          Number(incomingBu.context_dev_reserved || 0)
        ),
        context_dev_spent: absoluteSpentCorrection
          ? Number(incomingBu.context_dev_spent || 0)
          : Math.max(
              Number(existingBu.context_dev_spent || 0),
              Number(incomingBu.context_dev_spent || 0)
            ),
        context_dev_calls: absoluteSpentCorrection
          ? Number(
              incomingBu.context_dev_calls != null
                ? incomingBu.context_dev_calls
                : existingBu.context_dev_calls || 0
            )
          : Math.max(
              Number(existingBu.context_dev_calls || 0),
              Number(incomingBu.context_dev_calls || 0)
            ),
        serpapi_calls: Math.max(
          Number(existingBu.serpapi_calls || 0),
          Number(incomingBu.serpapi_calls || 0)
        ),
        serpapi_usd: Math.max(
          Number(existingBu.serpapi_usd || 0),
          Number(incomingBu.serpapi_usd || 0)
        ),
        // Outstanding reservations: prefer explicit next (including zero) — never max peaks
        serpapi_reserved:
          incomingBu.serpapi_reserved != null
            ? Number(incomingBu.serpapi_reserved || 0)
            : Number(existingBu.serpapi_reserved || 0),
        open_reservations:
          incomingBu.open_reservations != null
            ? Number(incomingBu.open_reservations || 0)
            : Number(existingBu.open_reservations || 0),
        enrichment_calls: Math.max(
          Number(existingBu.enrichment_calls || 0),
          Number(incomingBu.enrichment_calls || 0)
        ),
      },
      completed_work_keys: [
        ...new Set([
          ...(existing?.completed_work_keys || []),
          ...(caseRecord.completed_work_keys || []),
        ]),
      ],
    };
    delete next.checkpoint;
    delete next.journal_entry;
    writeJsonAtomic(casePath(caseRecord.case_id), next);
    return { ok: true, conflict: false, case: next };
  }

  function appendJournal(caseId, entry, writeOpts = {}) {
    return putCase(
      {
        case_id: caseId,
        journal_entry: entry,
      },
      writeOpts
    );
  }

  function getOwnerReuse(ownerId) {
    if (!ownerId) return null;
    try {
      const r = readCaseJson(ownerReusePath(ownerId));
      if (r.corrupt) return null;
      return r.case;
    } catch {
      return null;
    }
  }

  function putOwnerReuse(ownerId, payload) {
    if (!ownerId) throw new Error("owner_id_required");
    const existing = getOwnerReuse(ownerId);
    const next = {
      owner_entity_id: ownerId,
      ...(existing || {}),
      ...payload,
      updated_at: nowIso(),
      reuse_count: (existing?.reuse_count || 0) + (payload?.increment_reuse ? 1 : 0),
    };
    delete next.increment_reuse;
    writeJsonAtomic(ownerReusePath(ownerId), next);
    return next;
  }

  return {
    root,
    version: RESEARCH_CASE_STORE_VERSION,
    getCase,
    getCaseResult,
    putCase,
    listCases,
    tryAcquireLock,
    renewLease,
    releaseLock,
    appendJournal,
    getOwnerReuse,
    putOwnerReuse,
    assertValidCaseId,
  };
}

export function newCaseId(hotelId = null) {
  const raw = hotelId ? String(hotelId).replace(/[^a-zA-Z0-9_-]/g, "_").slice(0, 40) : "case";
  const h = CASE_ID_RE.test(raw) ? raw : "case";
  return `rc_${h}_${crypto.randomBytes(4).toString("hex")}`;
}
