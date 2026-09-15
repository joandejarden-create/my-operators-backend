#!/usr/bin/env node
/**
 * Local route / registration smoke for production-critical surfaces.
 * Does not require network to Railway. Starts a short-lived server only when
 * --listen is passed; default mode validates server.js registration + file paths.
 */
import fs from "node:fs";
import path from "node:path";
import http from "node:http";
import { fileURLToPath, pathToFileURL } from "node:url";
import { spawn } from "node:child_process";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const manifest = JSON.parse(
  fs.readFileSync(path.join(root, "config", "production-required-assets.json"), "utf8")
);

const listen = process.argv.includes("--listen");
const port = Number(process.env.PORT || 8791);

function assertServerMarkers() {
  const serverJs = fs.readFileSync(path.join(root, "server.js"), "utf8");
  const missing = [];
  for (const [id, surface] of Object.entries(manifest.surfaces)) {
    for (const marker of surface.required_server_route_markers || []) {
      if (!serverJs.includes(marker)) missing.push(`${id}: ${marker}`);
    }
    for (const route of surface.required_routes || []) {
      const leaf = route.split("/").pop();
      if (leaf && !serverJs.includes(leaf) && !fs.existsSync(path.join(root, "public", leaf))) {
        missing.push(`${id}: route/file ${route}`);
      }
    }
  }
  return missing;
}

async function fetchOk(url) {
  const res = await fetch(url, { method: "GET", redirect: "manual" });
  return { url, status: res.status };
}

async function runListenSmoke() {
  const child = spawn(process.execPath, ["server.js"], {
    cwd: root,
    env: {
      ...process.env,
      PORT: String(port),
      NODE_ENV: "test",
      // Avoid external side effects during boot where possible
      RAILWAY_ENVIRONMENT: "",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });

  let bootLog = "";
  child.stdout.on("data", (d) => {
    bootLog += d.toString();
  });
  child.stderr.on("data", (d) => {
    bootLog += d.toString();
  });

  const base = `http://127.0.0.1:${port}`;
  const deadline = Date.now() + 45000;
  let ready = false;
  while (Date.now() < deadline) {
    try {
      const r = await fetch(`${base}/`);
      if (r.status === 200 || r.status === 404) {
        ready = true;
        break;
      }
    } catch {
      // wait
    }
    await new Promise((r) => setTimeout(r, 400));
  }

  if (!ready) {
    child.kill("SIGTERM");
    console.error("FAIL LOCAL_ROUTE_SMOKE: server did not become ready");
    console.error(bootLog.slice(-2000));
    process.exit(1);
  }

  const paths = [
    ...(manifest.postdeploy_smoke?.static_paths || []),
    "/hotel-intelligence-golden-demo.html",
    "/css/hotel-intelligence-research-center.css",
    "/owner-ai-demand-share.html",
  ];
  const unique = [...new Set(paths)];
  const results = [];
  for (const p of unique) {
    results.push(await fetchOk(`${base}${p}`));
  }

  child.kill("SIGTERM");

  const bad = results.filter((r) => r.status !== 200);
  if (bad.length) {
    console.error("FAIL LOCAL_ROUTE_SMOKE:");
    for (const b of bad) console.error(`  ${b.status} ${b.url}`);
    process.exit(1);
  }
  console.log("PASS LOCAL_ROUTE_SMOKE (listen)");
  for (const r of results) console.log(`  ${r.status} ${r.url}`);
}

const missing = assertServerMarkers();
if (missing.length) {
  console.error("FAIL LOCAL_ROUTE_SMOKE (registration):");
  for (const m of missing) console.error(`  - ${m}`);
  process.exit(1);
}
console.log("PASS LOCAL_ROUTE_SMOKE (registration markers)");

if (listen) {
  await runListenSmoke();
}

process.exit(0);
