/**
 * V4 Full-Build PERSISTENT supervisor (production worker entrypoint).
 *
 * Requires: ENABLE_CENSUS_V4_WORKER=1
 * Also: ENABLE_VERIFIED_CENSUS_WRITES=1 for live writes
 *
 * Loop:
 *   acquire lease → spawn controller → map exit code → sleep/backoff → repeat
 *
 * Supervisor does NOT exit merely because one controller run finished.
 * Cursor terminal is NOT the production runtime — deploy this on Railway worker.
 */
import fs from "node:fs";
import path from "node:path";
import { spawn, execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import {
  CONTROLLER_EXIT,
  WORKER_STATUS,
  createWorkerId,
  tryAcquireWorkerLock,
  renewWorkerLock,
  releaseWorkerLock,
  writeHeartbeat,
  readJsonSafe,
  sleep,
  supervisorActionForExitCode,
} from "../lib/research-engine-v2/census-autopilot-v4/worker-runtime.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "data/research-engine-v2/census-autopilot-v4-full-universe");
const SEED_DIR = path.join(ROOT, "seed-census-v4-full-universe");
const LOCK = path.join(OUT, "61-census-v4-worker.lock.json");
const HEARTBEAT = path.join(OUT, "55-worker-heartbeat.json");
const STATUS_OUT = path.join(OUT, "61-production-worker-status.json");
const TICKET = path.join(OUT, "50-auto-resume-ticket.json");
const BUILD_STATUS = path.join(OUT, "24-full-build-status.json");
const CONTROLLER_STATUS = path.join(OUT, "48-full-build-controller-status.json");

/**
 * Railway volume mounts over OUT and starts empty. Hydrate once from image seed
 * (outside the mount) so ledger/checkpoints survive first boot without hiding.
 */
/**
 * Image-local assets for discovery. Railpack may drop top-level `reports/` /
 * `seed-*` dirs from the upload, so prefer `lib/census-v4-worker-assets`
 * (under traced lib/) then fall back to seed-census-v4-reports.
 */
function hydrateReportsFromSeed() {
  const reportsDir = path.join(ROOT, "reports");
  const sources = [
    path.join(ROOT, "lib/census-v4-worker-assets"),
    path.join(ROOT, "seed-census-v4-reports"),
    path.join(ROOT, "reports"),
  ];
  fs.mkdirSync(reportsDir, { recursive: true });
  let copied = 0;
  const used = [];
  for (const srcDir of sources) {
    if (!fs.existsSync(srcDir)) continue;
    used.push(path.basename(srcDir));
    for (const ent of fs.readdirSync(srcDir, { withFileTypes: true })) {
      if (!ent.isFile()) continue;
      const src = path.join(srcDir, ent.name);
      if (ent.name.endsWith(".tgz") || ent.name.endsWith(".tar.gz")) {
        try {
          execFileSync("tar", ["-xzf", src, "-C", reportsDir], { stdio: "inherit" });
          copied += 1;
        } catch (err) {
          console.warn("[supervisor] reports seed tar failed", String(err?.message || err));
        }
        continue;
      }
      const dst = path.join(reportsDir, ent.name);
      if (!fs.existsSync(dst)) {
        fs.copyFileSync(src, dst);
        copied += 1;
      }
    }
  }
  if (!used.length) return { hydrated: false, reason: "no_reports_seed" };
  return {
    hydrated: copied > 0,
    reason: copied > 0 ? "reports_seed_copied" : "reports_already_present",
    copied,
    sources: used,
  };
}

function hydrateVolumeFromSeed() {
  if (!fs.existsSync(SEED_DIR)) return { hydrated: false, reason: "no_seed_dir" };
  const ledgerMarker = path.join(OUT, "27-universe-ledger");
  const checkpoint = path.join(OUT, "43-controller-checkpoint-state.json");
  if (fs.existsSync(ledgerMarker) || fs.existsSync(checkpoint)) {
    return { hydrated: false, reason: "volume_already_has_state" };
  }
  fs.mkdirSync(OUT, { recursive: true });
  const walk = (src, dst) => {
    fs.mkdirSync(dst, { recursive: true });
    for (const ent of fs.readdirSync(src, { withFileTypes: true })) {
      const s = path.join(src, ent.name);
      const d = path.join(dst, ent.name);
      if (ent.isDirectory()) walk(s, d);
      else if (ent.name.endsWith(".tgz") || ent.name.endsWith(".tar.gz")) continue;
      else fs.copyFileSync(s, d);
    }
  };
  walk(SEED_DIR, OUT);

  // Ledger may ship compressed to keep Railway upload under timeout limits.
  const ledgerTgz = path.join(SEED_DIR, "27-universe-ledger.tgz");
  if (fs.existsSync(ledgerTgz) && !fs.existsSync(ledgerMarker)) {
    // Prefer system tar (available on Nixpacks/Railway Linux images).
    execFileSync("tar", ["-xzf", ledgerTgz, "-C", OUT], { stdio: "inherit" });
  }
  return { hydrated: true, reason: "seed_copied_to_volume" };
}

const workerId = process.env.CENSUS_V4_WORKER_ID || createWorkerId();
const HEARTBEAT_MS = Number(process.env.CENSUS_V4_HEARTBEAT_MS || 30_000);
const LOCK_TTL_MS = Number(process.env.CENSUS_V4_LOCK_TTL_MS || 120_000);
const MAX_CONTROLLER_RUNS = process.argv.includes("--max-controller-runs")
  ? Number(process.argv[process.argv.indexOf("--max-controller-runs") + 1])
  : Number(process.env.CENSUS_V4_MAX_CONTROLLER_RUNS || 0); // 0 = infinite

function wj(fp, data) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(data, null, 2));
}

function publishStatus(partial) {
  const prev = readJsonSafe(STATUS_OUT) || {};
  const ctrl = readJsonSafe(CONTROLLER_STATUS) || {};
  const ticket = readJsonSafe(TICKET) || {};
  const build = readJsonSafe(BUILD_STATUS) || {};
  const row = {
    ...prev,
    ...partial,
    worker_id: workerId,
    supervisor_pid: process.pid,
    last_heartbeat_at: new Date().toISOString(),
    current_lane: ctrl.current_lane || partial.current_lane || null,
    current_queue_size: ctrl.current_queue_size ?? partial.current_queue_size ?? null,
    actionable_remaining: ctrl.actionable_remaining ?? build.actionable_remaining ?? null,
    last_checkpoint_at: ctrl.last_work_completed_at || null,
    next_retry_at: ticket.next_work_scheduled_at || partial.next_retry_at || null,
    joan_npm_required: false,
    cursor_required: false,
  };
  wj(STATUS_OUT, row);
  writeHeartbeat(HEARTBEAT, row);
  return row;
}

function spawnController() {
  return new Promise((resolve) => {
    const args = ["scripts/v4-full-build-controller.mjs", "--apply"];
    // Prefer short process boundaries for Railway/memory safety; supervisor loops.
    const maxRuntimeMs = Number(process.env.V4_MAX_RUNTIME_MS || 10 * 60 * 1000);
    const hardKillGraceMs = Number(process.env.CENSUS_V4_CONTROLLER_HARD_KILL_GRACE_MS || 60_000);
    const env = {
      ...process.env,
      ENABLE_VERIFIED_CENSUS_WRITES: process.env.ENABLE_VERIFIED_CENSUS_WRITES || "1",
      ENABLE_CENSUS_AUTOPILOT_V4: process.env.ENABLE_CENSUS_AUTOPILOT_V4 || "1",
      V4_MAX_ITERATIONS: process.env.V4_MAX_ITERATIONS || "2",
      V4_MAX_RUNTIME_MS: String(maxRuntimeMs),
    };
    const child = spawn(process.execPath, args, {
      cwd: ROOT,
      env,
      stdio: "inherit",
      shell: false,
    });
    publishStatus({
      worker_status: WORKER_STATUS.RUNNING,
      controller_pid: child.pid,
      last_error: null,
    });
    // If discover/network hangs inside an iteration, controller may never hit its
    // runtime check — supervisor hard-kills so the persistent loop continues.
    const killTimer = setTimeout(() => {
      if (child.killed || child.exitCode != null) return;
      console.warn(
        `[supervisor] controller hard-kill after ${maxRuntimeMs + hardKillGraceMs}ms (hung child)`
      );
      try {
        child.kill("SIGTERM");
      } catch (err) {
        console.warn("[supervisor] SIGTERM failed", String(err?.message || err));
      }
      setTimeout(() => {
        if (child.exitCode == null && !child.killed) {
          try {
            child.kill("SIGKILL");
          } catch {
            /* ignore */
          }
        }
      }, 15_000).unref?.();
    }, maxRuntimeMs + hardKillGraceMs);
    killTimer.unref?.();
    child.on("exit", (code, signal) => {
      clearTimeout(killTimer);
      resolve({ code: code == null ? CONTROLLER_EXIT.UNEXPECTED_FAILURE : code, signal });
    });
  });
}

async function main() {
  const dry = process.argv.includes("--dry-run");
  const once = process.argv.includes("--once"); // legacy one-shot (not production)

  if (process.env.ENABLE_CENSUS_V4_WORKER !== "1" && !process.argv.includes("--force-worker")) {
    console.error(
      JSON.stringify({
        ok: false,
        error: "ENABLE_CENSUS_V4_WORKER=1 required for persistent supervisor",
        hint: "Cursor/local diagnostics may use --force-worker; production Railway worker sets the env flag",
      })
    );
    process.exit(CONTROLLER_EXIT.FATAL_CONFIGURATION);
  }

  if (dry) {
    console.log(JSON.stringify({ ok: true, dry: true, workerId, mode: "persistent_supervisor" }, null, 2));
    process.exit(0);
  }

  const hydrateReports = hydrateReportsFromSeed();
  const hydrate = hydrateVolumeFromSeed();
  console.log(JSON.stringify({ supervisor_hydrate_reports: hydrateReports, supervisor_hydrate: hydrate }, null, 2));

  const startedAt = new Date().toISOString();
  let controllerRuns = 0;
  const boundaryLog = [];

  // Heartbeat timer — renew only; avoid contending with tryAcquire rename races
  const hbTimer = setInterval(() => {
    try {
      renewWorkerLock(LOCK, workerId, LOCK_TTL_MS);
      const prev = readJsonSafe(STATUS_OUT) || {};
      publishStatus({ worker_status: prev.worker_status || WORKER_STATUS.RUNNING });
    } catch (err) {
      console.warn("[supervisor] heartbeat renew failed", String(err?.message || err));
    }
  }, HEARTBEAT_MS);
  hbTimer.unref?.();

  const shutdown = (reason) => {
    clearInterval(hbTimer);
    releaseWorkerLock(LOCK, workerId);
    publishStatus({
      worker_status: reason === "complete" ? WORKER_STATUS.COMPLETE : WORKER_STATUS.STOPPED_UNEXPECTEDLY,
      last_error: reason === "complete" ? null : reason,
      stopped_at: new Date().toISOString(),
    });
  };

  process.on("SIGTERM", () => {
    console.log("[supervisor] SIGTERM — graceful stop, checkpoint preserved");
    shutdown("sigterm");
    process.exit(0);
  });
  process.on("SIGINT", () => {
    console.log("[supervisor] SIGINT — graceful stop");
    shutdown("sigint");
    process.exit(0);
  });

  publishStatus({
    worker_status: WORKER_STATUS.RUNNING,
    started_at: startedAt,
    last_error: null,
  });

  console.log(
    JSON.stringify(
      {
        supervisor: "persistent",
        worker_id: workerId,
        joan_required: false,
        cursor_required: false,
        lock: LOCK,
      },
      null,
      2
    )
  );

  while (true) {
    let acq;
    try {
      acq = tryAcquireWorkerLock(LOCK, { worker_id: workerId, ttl_ms: LOCK_TTL_MS });
    } catch (err) {
      console.warn("[supervisor] lock acquire error — retry", String(err?.message || err));
      await sleep(5_000);
      continue;
    }
    if (!acq.acquired) {
      publishStatus({
        worker_status: WORKER_STATUS.WAITING,
        last_error: null,
        temporary_block_reason: "lease_held_by_other_healthy_worker",
        next_retry_at: new Date(Date.now() + 60_000).toISOString(),
      });
      console.log("[supervisor] lease held by other worker — waiting", acq.holder?.worker_id);
      await sleep(60_000);
      continue;
    }

    // Wrap lock renew after controller so a transient EPERM does not kill supervisor
    try {
      renewWorkerLock(LOCK, workerId, LOCK_TTL_MS);
    } catch (err) {
      console.warn("[supervisor] post-acquire renew warning", String(err?.message || err));
    }

    const build = readJsonSafe(BUILD_STATUS) || {};
    if (build.status === "BLOCKED" || build.controller_status === "BLOCKED") {
      publishStatus({
        worker_status: WORKER_STATUS.HARD_BLOCKED,
        hard_block_reason: build.hard_block_reason || build.stop_reason || "build_status_blocked",
      });
      clearInterval(hbTimer);
      // Stay alive but blocked (don't exit) unless --exit-on-hard-block
      if (process.argv.includes("--exit-on-hard-block")) {
        shutdown("hard_block");
        process.exit(CONTROLLER_EXIT.HARD_CIRCUIT_BREAK);
      }
      await sleep(300_000);
      continue;
    }

    const ticket = readJsonSafe(TICKET);
    if (ticket?.next_work_scheduled_at) {
      const when = Date.parse(ticket.next_work_scheduled_at);
      if (Number.isFinite(when) && Date.now() < when) {
        const wait = Math.min(when - Date.now(), 5 * 60_000);
        publishStatus({
          worker_status: WORKER_STATUS.WAITING,
          temporary_block_reason: "retry_window",
          next_retry_at: ticket.next_work_scheduled_at,
        });
        console.log(`[supervisor] retry window — sleep ${wait}ms`);
        await sleep(wait);
        continue;
      }
    }

    console.log(`[supervisor] launching controller run #${controllerRuns + 1}`);
    const runStarted = new Date().toISOString();
    const { code, signal } = await spawnController();
    controllerRuns += 1;
    try {
      renewWorkerLock(LOCK, workerId, LOCK_TTL_MS);
    } catch (err) {
      console.warn("[supervisor] post-controller renew warning", String(err?.message || err));
    }

    const ctrl = readJsonSafe(CONTROLLER_STATUS) || {};
    const mapped = supervisorActionForExitCode(code);
    boundaryLog.push({
      run: controllerRuns,
      started_at: runStarted,
      ended_at: new Date().toISOString(),
      exit_code: code,
      signal: signal || null,
      stop_reason: ctrl.stop_reason || null,
      supervisor_action: mapped.action,
      actionable_remaining: ctrl.actionable_remaining ?? null,
      lane: ctrl.current_lane || null,
    });
    wj(path.join(OUT, "60-three-boundary-demo.json"), {
      boundaries: boundaryLog,
      passed: boundaryLog.length >= 3,
      joan_commands_between: 0,
    });

    console.log(
      `[supervisor] controller exit code=${code} signal=${signal || "none"} action=${mapped.action}`
    );

    if (mapped.action === "supervisor_exit_complete") {
      publishStatus({ worker_status: WORKER_STATUS.COMPLETE });
      shutdown("complete");
      process.exit(CONTROLLER_EXIT.BUILD_COMPLETE);
    }
    if (mapped.action === "supervisor_hard_block") {
      publishStatus({
        worker_status: WORKER_STATUS.HARD_BLOCKED,
        hard_block_reason: ctrl.hard_block_reason || ctrl.stop_reason || "hard_circuit",
      });
      // Remain living in WAITING/HARD_BLOCKED loop — Joan intervention for breaker only
      await sleep(300_000);
      continue;
    }
    if (mapped.action === "supervisor_fatal") {
      publishStatus({
        worker_status: WORKER_STATUS.STOPPED_UNEXPECTEDLY,
        last_error: "fatal_configuration",
      });
      shutdown("fatal");
      process.exit(CONTROLLER_EXIT.FATAL_CONFIGURATION);
    }

    publishStatus({
      worker_status: WORKER_STATUS.RUNNING,
      last_controller_exit_code: code,
      controller_runs: controllerRuns,
    });

    if (once || (MAX_CONTROLLER_RUNS > 0 && controllerRuns >= MAX_CONTROLLER_RUNS)) {
      // Demo / bounded run — still leave resume ticket for production worker
      publishStatus({
        worker_status: WORKER_STATUS.WAITING,
        temporary_block_reason: MAX_CONTROLLER_RUNS
          ? "max_controller_runs_demo_boundary"
          : "once_mode",
        next_retry_at: new Date().toISOString(),
      });
      wj(path.join(OUT, "50-auto-resume-ticket.json"), {
        resume: true,
        next_work_scheduled_at: new Date().toISOString(),
        joan_required: false,
        note: "supervisor bounded exit — production worker should continue",
      });
      clearInterval(hbTimer);
      // Do not release lock aggressively on demo — allow next supervisor acquire after stale
      releaseWorkerLock(LOCK, workerId);
      console.log(
        JSON.stringify(
          {
            ok: true,
            mode: once ? "once" : "max_controller_runs",
            controllerRuns,
            boundaries: boundaryLog.length,
            production_should_use_infinite_supervisor: true,
          },
          null,
          2
        )
      );
      process.exit(CONTROLLER_EXIT.CHECKPOINT_CONTINUE);
    }

    // Runtime boundary / continue — sleep briefly then relaunch (NO Joan)
    await sleep(mapped.sleep_ms || 2_000);
  }
}

main().catch((e) => {
  console.error(e);
  try {
    publishStatus({
      worker_status: WORKER_STATUS.STOPPED_UNEXPECTEDLY,
      last_error: String(e?.message || e),
    });
  } catch {
    /* ignore */
  }
  process.exit(CONTROLLER_EXIT.UNEXPECTED_FAILURE);
});
