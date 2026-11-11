/**
 * Cross-process exclusive lock for Core+Loyalty paid execution.
 * Prevents concurrent `--apply --continue` / `--apply --initial-batch` doublespend.
 *
 * Ledger withAdpBudgetLock only serializes reserve/commit mutations — it does NOT
 * prevent two processes from each running provider calls under one reservation.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

export const CORE_LOYALTY_EXECUTION_LOCK_VERSION = "core_loyalty_execution_lock_v1";

export function coreLoyaltyExecutionLockPath(cwd = process.cwd()) {
  return path.join(
    cwd || ROOT,
    "data/ai-demand-positioning/budget/core-loyalty-execution-v1.lock"
  );
}

function pidAlive(pid) {
  if (!pid || !Number.isFinite(Number(pid))) return false;
  try {
    process.kill(Number(pid), 0);
    return true;
  } catch (err) {
    return err?.code === "EPERM"; // exists but not owned by us
  }
}

export function readCoreLoyaltyExecutionLock(cwd = process.cwd()) {
  const lockPath = coreLoyaltyExecutionLockPath(cwd);
  const ownerFile = path.join(lockPath, "owner.json");
  if (!fs.existsSync(ownerFile)) return null;
  try {
    return JSON.parse(fs.readFileSync(ownerFile, "utf8"));
  } catch {
    return null;
  }
}

/**
 * Acquire exclusive execution lock. Fails closed if another live PID holds it.
 */
export function acquireCoreLoyaltyExecutionLock({
  cwd = process.cwd(),
  pid = process.pid,
  command = process.argv.join(" "),
  forceIfStale = true,
} = {}) {
  const lockPath = coreLoyaltyExecutionLockPath(cwd);
  fs.mkdirSync(path.dirname(lockPath), { recursive: true });

  const owner = {
    version: CORE_LOYALTY_EXECUTION_LOCK_VERSION,
    pid: Number(pid),
    command: String(command || "").slice(0, 500),
    acquiredAt: new Date().toISOString(),
    hostname: process.env.COMPUTERNAME || process.env.HOSTNAME || null,
  };

  // Atomic exclusive create via mkdir (EEXIST = contention). Stale dead-PID
  // takeover removes then retries mkdir once — never assume unlock without recreate.
  let acquired = false;
  for (let attempt = 0; attempt < 3 && !acquired; attempt++) {
    try {
      fs.mkdirSync(lockPath);
      acquired = true;
    } catch (err) {
      if (err?.code !== "EEXIST") throw err;
      const existing = readCoreLoyaltyExecutionLock(cwd);
      if (existing?.pid && pidAlive(existing.pid)) {
        if (Number(existing.pid) === Number(pid)) {
          return { ok: true, idempotent: true, lockPath, owner: existing };
        }
        const e = new Error(
          `core_loyalty_execution_lock_held: PID ${existing.pid} holds exclusive paid execution lock (acquired ${existing.acquiredAt}). Do not start a second --apply process.`
        );
        e.code = "CORE_LOYALTY_EXEC_LOCK_HELD";
        e.existing = existing;
        throw e;
      }
      if (!forceIfStale) {
        const e = new Error("core_loyalty_execution_lock_stale_refused");
        e.code = "CORE_LOYALTY_EXEC_LOCK_STALE";
        e.existing = existing;
        throw e;
      }
      try {
        fs.rmSync(lockPath, { recursive: true, force: true });
      } catch {
        /* retry */
      }
    }
  }
  if (!acquired) {
    const e = new Error(
      "core_loyalty_execution_lock_acquire_failed: contended or unstable lock directory"
    );
    e.code = "CORE_LOYALTY_EXEC_LOCK_ACQUIRE_FAILED";
    throw e;
  }

  // wx = fail if owner.json already exists (second process raced past mkdir)
  try {
    fs.writeFileSync(
      path.join(lockPath, "owner.json"),
      JSON.stringify(owner, null, 2) + "\n",
      { encoding: "utf8", flag: "wx" }
    );
  } catch (err) {
    try {
      fs.rmSync(lockPath, { recursive: true, force: true });
    } catch {
      /* ignore */
    }
    const e = new Error(
      `core_loyalty_execution_lock_held: lost race writing owner.json (${err.code || err.message})`
    );
    e.code = "CORE_LOYALTY_EXEC_LOCK_HELD";
    throw e;
  }
  return { ok: true, idempotent: false, lockPath, owner };
}

export function releaseCoreLoyaltyExecutionLock({
  cwd = process.cwd(),
  pid = process.pid,
  force = false,
} = {}) {
  const lockPath = coreLoyaltyExecutionLockPath(cwd);
  const existing = readCoreLoyaltyExecutionLock(cwd);
  if (!existing) return { ok: true, released: false };
  if (!force && Number(existing.pid) !== Number(pid)) {
    return { ok: false, released: false, reason: "not_owner", existing };
  }
  try {
    fs.rmSync(lockPath, { recursive: true, force: true });
    return { ok: true, released: true };
  } catch (err) {
    return { ok: false, released: false, error: String(err?.message || err) };
  }
}

/**
 * Claim lock for an already-running authoritative PID (incident recovery).
 */
export function claimCoreLoyaltyExecutionLockForRunningPid(pid, meta = {}, cwd = process.cwd()) {
  return acquireCoreLoyaltyExecutionLock({
    cwd,
    pid,
    command: meta.command || `claimed_for_running_pid_${pid}`,
    forceIfStale: true,
  });
}
