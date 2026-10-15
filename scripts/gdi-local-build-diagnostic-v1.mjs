#!/usr/bin/env node
/**
 * Non-secret local GDI build diagnostic.
 * Identifies which code is serving, persistence adapter, and asset bust.
 * Never prints secrets or full share tokens.
 *
 * Usage:
 *   node scripts/gdi-local-build-diagnostic-v1.mjs
 *   node scripts/gdi-local-build-diagnostic-v1.mjs --base=http://127.0.0.1:8080
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { execSync } from "node:child_process";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const baseArg = process.argv.find((a) => a.startsWith("--base="));
const BASE = (baseArg ? baseArg.slice("--base=".length) : "http://127.0.0.1:8080").replace(
  /\/$/,
  ""
);

function git(cmd) {
  try {
    return execSync(cmd, { cwd: ROOT, encoding: "utf8" }).trim();
  } catch {
    return null;
  }
}

function envFlag(name) {
  const v = process.env[name];
  if (v == null || v === "") return "unset";
  if (/secret|token|key|password/i.test(name)) return "set";
  return String(v);
}

async function fetchText(url) {
  try {
    const res = await fetch(url, { redirect: "manual" });
    const text = await res.text();
    return { status: res.status, text, headers: res.headers };
  } catch (err) {
    return { status: 0, text: "", error: String(err?.message || err) };
  }
}

function extractAssetBusts(html) {
  const busts = new Set();
  for (const m of html.matchAll(/\/(?:css|js\/group-demand-intelligence)\/[^"'?\s]+\?v=([A-Za-z0-9._-]+)/g)) {
    busts.add(m[1]);
  }
  return [...busts];
}

const branch = git("git branch --show-current");
const head = git("git rev-parse HEAD");
const short = head ? head.slice(0, 12) : null;
const dirty = Boolean(git("git status --porcelain"));

const html = await fetchText(`${BASE}/group-demand-intelligence-share.html`);
const health = await fetchText(`${BASE}/api/health`).catch?.(() => null);
const healthAlt = health?.status
  ? health
  : await fetchText(`${BASE}/api/group-demand-intelligence/health`);

const report = {
  ok: html.status === 200,
  checkedAt: new Date().toISOString(),
  preferredWorkspace: ROOT,
  git: {
    branch: branch || "(detached)",
    head: short,
    dirty,
  },
  localServer: {
    baseUrl: BASE,
    htmlStatus: html.status,
    htmlError: html.error || null,
    assetBusts: html.status === 200 ? extractAssetBusts(html.text) : [],
    healthStatus: healthAlt?.status ?? null,
  },
  runtimeHints: {
    GDI_OPPORTUNITIES_PERSISTENCE: envFlag("GDI_OPPORTUNITIES_PERSISTENCE"),
    GDI_DATA_ROOT: process.env.GDI_DATA_ROOT ? "set (sandbox path present)" : "unset (repo default)",
    GDI_SHARE_CAPABILITY_SECRET: envFlag("GDI_SHARE_CAPABILITY_SECRET"),
    GDI_SHARE_CAPABILITY_ALLOW_DEV_SECRET: envFlag("GDI_SHARE_CAPABILITY_ALLOW_DEV_SECRET"),
    NODE_ENV: envFlag("NODE_ENV"),
    PORT: envFlag("PORT"),
  },
  expectedStart: {
    command:
      "PORT=8080 GDI_OPPORTUNITIES_PERSISTENCE=airtable GDI_SHARE_CAPABILITY_ALLOW_DEV_SECRET=1 npm start",
    windowsPowerShell:
      "$env:PORT='8080'; $env:GDI_OPPORTUNITIES_PERSISTENCE='airtable'; $env:GDI_SHARE_CAPABILITY_ALLOW_DEV_SECRET='1'; npm start",
    primaryUrl: "http://127.0.0.1:8080/group-demand-intelligence.html",
    shareUrlPattern: "http://127.0.0.1:8080/group-demand-intelligence-share.html?share=…",
    obsoletePreviewPorts: [8081, 3000],
  },
};

const outDir = path.join(ROOT, "reports", "gdi-local-production-parity");
fs.mkdirSync(outDir, { recursive: true });
const outPath = path.join(outDir, "local-build-diagnostic.json");
fs.writeFileSync(outPath, JSON.stringify(report, null, 2) + "\n");
console.log(JSON.stringify(report, null, 2));
console.log(`Wrote ${outPath}`);
process.exit(report.ok ? 0 : 1);
