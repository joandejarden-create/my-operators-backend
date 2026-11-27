#!/usr/bin/env node
/**
 * Stable local GDI serve on :8080 from the registered GDI workspace.
 *
 * Why: agent/PowerShell-attached `node server.js` dies when the parent
 * session ends. This launcher starts a detached child and writes a PID file.
 *
 * Ownership: refuses to launch unless cwd is the registered GDI home (or
 * Integration with DEALALITY_8080_OWNER=integration). Env flags alone cannot
 * authorize :8080. Pins GDI_PUBLICATION_PACKAGE_ROOT to the durable GDI store.
 *
 * Reclaim: stops prior PID-file process AND any current :8080 listener, then
 * verifies listen + /api/dev/runtime-identity before reporting success.
 * A spawn alone is NOT success (prevents false OK when a foreign tree races).
 *
 * Usage (from ws-gdi):
 *   node scripts/dealality-gdi-local-8080-stable-launch-v1.mjs
 *   node scripts/dealality-gdi-local-8080-stable-launch-v1.mjs --stop
 */
import { spawn } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import net from "node:net";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { execFileSync } from "node:child_process";
import dotenv from "dotenv";
import {
  buildCanonical8080LaunchEnv,
  DEALALITY_CANONICAL_PORT_8080_OWNERSHIP_V1,
} from "../lib/dealality-dev-os/canonical-port-8080-ownership-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const PID_FILE = path.join(ROOT, "tmp", "gdi-local-8080.pid");
const OUT_LOG = path.join(ROOT, "tmp", "gdi-local-8080.out.log");
const ERR_LOG = path.join(ROOT, "tmp", "gdi-local-8080.err.log");
const META_FILE = path.join(ROOT, "tmp", "gdi-local-8080.meta.json");
const PORT = Number(process.env.PORT || 8080);

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function killPid(pid) {
  if (!Number.isFinite(pid) || pid <= 0) return false;
  try {
    process.kill(pid);
    return true;
  } catch {
    return false;
  }
}

function listListeningPidsWindows(port) {
  try {
    const out = execFileSync(
      "powershell.exe",
      [
        "-NoProfile",
        "-Command",
        `(Get-NetTCPConnection -LocalPort ${port} -State Listen -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess -Unique) -join ','`,
      ],
      { encoding: "utf8", windowsHide: true },
    );
    return String(out || "")
      .split(/[,\s]+/)
      .map((x) => Number(String(x).trim()))
      .filter((n) => Number.isFinite(n) && n > 0);
  } catch {
    return [];
  }
}

function stopExisting() {
  const stopped = [];
  if (fs.existsSync(PID_FILE)) {
    const pid = Number(String(fs.readFileSync(PID_FILE, "utf8")).trim());
    if (killPid(pid)) stopped.push(pid);
    try {
      fs.unlinkSync(PID_FILE);
    } catch {
      /* ignore */
    }
  }
  for (const pid of listListeningPidsWindows(PORT)) {
    if (killPid(pid)) stopped.push(pid);
  }
  return { stopped: stopped.length > 0, pids: [...new Set(stopped)] };
}

function portFree(port) {
  return new Promise((resolve) => {
    const srv = net.createServer();
    srv.once("error", () => resolve(false));
    srv.once("listening", () => {
      srv.close(() => resolve(true));
    });
    srv.listen(port, "127.0.0.1");
  });
}

function fetchJson(url, timeoutMs = 4000) {
  return new Promise((resolve, reject) => {
    const req = http.get(url, { timeout: timeoutMs }, (res) => {
      let body = "";
      res.setEncoding("utf8");
      res.on("data", (c) => {
        body += c;
      });
      res.on("end", () => {
        try {
          resolve({ status: res.statusCode, json: JSON.parse(body) });
        } catch (err) {
          reject(err);
        }
      });
    });
    req.on("error", reject);
    req.on("timeout", () => {
      req.destroy();
      reject(new Error("timeout"));
    });
  });
}

async function waitForCanonicalReady(expectedPid, timeoutMs = 45000) {
  const started = Date.now();
  let lastErr = null;
  while (Date.now() - started < timeoutMs) {
    const listeners = listListeningPidsWindows(PORT);
    if (!listeners.includes(expectedPid)) {
      lastErr = `pid ${expectedPid} not listening on ${PORT} (listeners=${listeners.join(",") || "none"})`;
      await sleep(500);
      continue;
    }
    try {
      const { status, json } = await fetchJson(
        `http://127.0.0.1:${PORT}/api/dev/runtime-identity`,
      );
      if (status === 200 && json?.ok) {
        const basename = String(json.servingDirectoryBasename || "");
        const authorized = json.portOwnership?.authorizedFor8080 === true;
        const owner = json.portOwnership?.ownerStream;
        const staged = json.publication?.wyndhamStagedVersion;
        if (basename !== "ws-gdi") {
          lastErr = `identity basename=${basename} (expected ws-gdi)`;
        } else if (!authorized || (owner !== "gdi" && owner !== "integration")) {
          lastErr = `identity ownership not authorized (owner=${owner})`;
        } else if (!staged) {
          lastErr = "identity missing Wyndham STAGED version";
        } else {
          return { ok: true, identity: json };
        }
      } else {
        lastErr = `identity status=${status}`;
      }
    } catch (err) {
      lastErr = String(err?.message || err);
    }
    await sleep(700);
  }
  return { ok: false, error: lastErr || "ready_timeout" };
}

if (process.argv.includes("--stop")) {
  console.log(JSON.stringify({ ok: true, ...stopExisting() }, null, 2));
  process.exit(0);
}

dotenv.config({ path: path.join(ROOT, ".env"), override: false });
dotenv.config({
  path: path.join("C:/Dev/dealality/my-operators-backend", ".env"),
  override: false,
});

if (PORT !== 8080) {
  console.error(
    JSON.stringify(
      {
        ok: false,
        code: "LAUNCHER_PORT_MUST_BE_8080",
        message:
          "This launcher is only for the canonical founder review port 8080. Use PORT=8095 DEALALITY_ALLOW_WORKTREE_SERVER=1 node server.js for feature work.",
        port: PORT,
      },
      null,
      2,
    ),
  );
  process.exit(1);
}

const launch = buildCanonical8080LaunchEnv(process.env, { cwd: ROOT });
if (!launch.ok) {
  console.error(
    JSON.stringify(
      {
        ok: false,
        contract: DEALALITY_CANONICAL_PORT_8080_OWNERSHIP_V1,
        code: launch.evaluation?.code || "PORT_OWNERSHIP_DENIED",
        message: launch.evaluation?.reason,
        evaluation: launch.evaluation,
      },
      null,
      2,
    ),
  );
  process.exit(1);
}

fs.mkdirSync(path.join(ROOT, "tmp"), { recursive: true });
const stopped = stopExisting();

// Wait until port is free after reclaim.
for (let i = 0; i < 30; i += 1) {
  // eslint-disable-next-line no-await-in-loop
  if (await portFree(PORT)) break;
  // eslint-disable-next-line no-await-in-loop
  await sleep(200);
  for (const pid of listListeningPidsWindows(PORT)) killPid(pid);
}
if (!(await portFree(PORT))) {
  console.error(
    JSON.stringify(
      {
        ok: false,
        code: "PORT_8080_STILL_BUSY",
        message: "Could not reclaim port 8080 after stopping listeners.",
        stopped,
      },
      null,
      2,
    ),
  );
  process.exit(1);
}

const outFd = fs.openSync(OUT_LOG, "a");
const errFd = fs.openSync(ERR_LOG, "a");
const child = spawn(process.execPath, ["server.js"], {
  cwd: ROOT,
  detached: true,
  stdio: ["ignore", outFd, errFd],
  env: launch.env,
  windowsHide: true,
});
child.unref();

const ready = await waitForCanonicalReady(child.pid);
if (!ready.ok) {
  try {
    killPid(child.pid);
  } catch {
    /* ignore */
  }
  console.error(
    JSON.stringify(
      {
        ok: false,
        code: "CANONICAL_READY_VERIFICATION_FAILED",
        message:
          "Spawned process did not prove authorized cumulative identity on :8080. Not reporting success.",
        pid: child.pid,
        detail: ready.error,
        stopped,
      },
      null,
      2,
    ),
  );
  process.exit(1);
}

fs.writeFileSync(PID_FILE, `${child.pid}\n`, "utf8");
const meta = {
  startedAt: new Date().toISOString(),
  pid: child.pid,
  port: 8080,
  cwd: ROOT,
  mode: "detached-stable-launch-v1",
  ownershipContract: DEALALITY_CANONICAL_PORT_8080_OWNERSHIP_V1,
  ownershipCode: launch.evaluation.code,
  ownerStream: launch.evaluation.ownerStream,
  durablePackageRoot: launch.evaluation.durablePackageRoot?.root || null,
  durablePackageSource: launch.evaluation.durablePackageRoot?.source || null,
  readyVerified: true,
  identity: {
    basename: ready.identity.servingDirectoryBasename,
    branch: ready.identity.branch,
    sha: ready.identity.sha,
    ownershipCode: ready.identity.portOwnership?.code,
    wyndhamStagedVersion: ready.identity.publication?.wyndhamStagedVersion,
  },
  note: "Parent-independent; success only after listen + runtime-identity verification.",
};
fs.writeFileSync(META_FILE, `${JSON.stringify(meta, null, 2)}\n`, "utf8");

console.log(
  JSON.stringify(
    {
      ok: true,
      pid: child.pid,
      port: 8080,
      cwd: ROOT,
      pidFile: PID_FILE,
      outLog: OUT_LOG,
      errLog: ERR_LOG,
      metaFile: META_FILE,
      detached: true,
      survivesAgentSession: true,
      permanentService: false,
      readyVerified: true,
      stopped,
      ownership: {
        contract: DEALALITY_CANONICAL_PORT_8080_OWNERSHIP_V1,
        code: launch.evaluation.code,
        ownerStream: launch.evaluation.ownerStream,
        durablePackageRoot: meta.durablePackageRoot,
      },
      identity: meta.identity,
    },
    null,
    2,
  ),
);
