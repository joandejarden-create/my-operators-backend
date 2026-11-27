#!/usr/bin/env node
/**
 * Canonical :8080 live ownership guard.
 *
 * Audits the OS listener command line (not just meta PID files). If a foreign
 * server.js owns :8080, reclaim via the GDI stable launcher.
 *
 *   npm run dealality:canonical-8080-guard -- --once
 *   npm run dealality:canonical-8080-guard -- --watch --interval-ms 15000
 */
import { spawn, execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { auditLiveCanonical8080Listener } from "../lib/dealality-dev-os/canonical-port-8080-ownership-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const META = path.join(ROOT, "tmp", "gdi-canonical-8080-guard.meta.json");
const argv = process.argv.slice(2);
const once = argv.includes("--once") || !argv.includes("--watch");
const intervalMs = Number(
  (argv.includes("--interval-ms") && argv[argv.indexOf("--interval-ms") + 1]) ||
    15000,
);

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function relaunchCanonical() {
  const out = execFileSync(
    process.execPath,
    ["scripts/dealality-gdi-local-8080-stable-launch-v1.mjs"],
    { cwd: ROOT, encoding: "utf8", windowsHide: true },
  );
  return JSON.parse(out);
}

async function tick() {
  const audit = auditLiveCanonical8080Listener({});
  const record = {
    at: new Date().toISOString(),
    audit,
  };
  if (audit.ok) {
    record.action = "NONE";
    fs.mkdirSync(path.dirname(META), { recursive: true });
    fs.writeFileSync(META, `${JSON.stringify(record, null, 2)}\n`);
    return record;
  }
  record.action = "RECLAIM_AND_RELAUNCH";
  try {
    const launch = relaunchCanonical();
    record.launch = {
      ok: launch.ok === true,
      pid: launch.pid,
      identity: launch.identity || null,
    };
  } catch (err) {
    record.launch = { ok: false, error: String(err?.message || err).slice(0, 400) };
  }
  fs.mkdirSync(path.dirname(META), { recursive: true });
  fs.writeFileSync(META, `${JSON.stringify(record, null, 2)}\n`);
  return record;
}

if (once) {
  const result = await tick();
  console.log(JSON.stringify(result, null, 2));
  process.exit(result.audit?.ok || result.launch?.ok ? 0 : 2);
}

console.log(
  JSON.stringify(
    {
      ok: true,
      mode: "watch",
      intervalMs,
      cwd: ROOT,
      note: "Detached watch — audits live :8080 listener and relaunches ws-gdi when foreign.",
    },
    null,
    2,
  ),
);

// Detach-friendly loop (parent may exit).
while (true) {
  // eslint-disable-next-line no-await-in-loop
  const result = await tick();
  if (!result.audit?.ok) {
    console.error(
      `[canonical-8080-guard] reclaimed foreign listener code=${result.audit?.code}`,
    );
  }
  // eslint-disable-next-line no-await-in-loop
  await sleep(intervalMs);
}
