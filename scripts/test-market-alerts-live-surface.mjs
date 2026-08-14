#!/usr/bin/env node
/**
 * Market Alerts V1.3.2 — live-surface smoke.
 * Validates the files Express actually serves AND, when a host is reachable,
 * the HTTP HTML/JS the browser would receive (including the app-shell iframe URL).
 *
 * Usage:
 *   npm run test:market-alerts-live-surface
 *   MARKET_ALERTS_LIVE_BASE=https://my-operators-backend-staging.up.railway.app npm run test:market-alerts-live-surface
 *   node scripts/test-market-alerts-live-surface.mjs --require-http
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const UI_VERSION = "1.3.2";
const REQUIRED_HTML_STRINGS = [
  "Actionable",
  "Worth Reviewing",
  "All Market Activity",
  "Top Read",
  "Latest Market Activity",
  "Actionable Now",
];

let failed = 0;

function assert(cond, msg) {
  if (!cond) {
    failed += 1;
    console.error("FAIL:", msg);
  } else {
    console.log("OK:", msg);
  }
}

function readPublic(rel) {
  return fs.readFileSync(path.join(ROOT, "public", rel), "utf8");
}

async function fetchUrl(url, timeoutMs) {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      signal: ac.signal,
      redirect: "follow",
      headers: { "Cache-Control": "no-cache", Pragma: "no-cache" },
    });
    const text = await res.text();
    return {
      ok: res.ok,
      status: res.status,
      text,
      cacheControl: res.headers.get("cache-control") || "",
      contentType: res.headers.get("content-type") || "",
    };
  } finally {
    clearTimeout(t);
  }
}

function assertHtmlSurface(label, html) {
  for (const s of REQUIRED_HTML_STRINGS) {
    assert(html.includes(s), `${label} contains "${s}"`);
  }
  assert(
    html.includes(`name="dealality-market-alerts-ui" content="${UI_VERSION}"`),
    `${label} has UI meta ${UI_VERSION}`
  );
  assert(
    html.includes(`market-alerts.js?v=${UI_VERSION}`),
    `${label} loads market-alerts.js?v=${UI_VERSION}`
  );
  assert(/id="feedModeActionable"/.test(html), `${label} has Actionable button`);
  assert(/id="feedModeWorth"/.test(html), `${label} has Worth Reviewing button`);
  assert(/id="feedModeAll"/.test(html), `${label} has All Market Activity button`);
  assert(!/id="feedModeActionable"[^>]*\bdisabled\b/.test(html), `${label} Actionable not disabled`);
  assert(!/id="feedModeWorth"[^>]*\bdisabled\b/.test(html), `${label} Worth Reviewing not disabled`);
  assert(/id="topReadRail"/.test(html) && /id="topReadList"/.test(html), `${label} has Top Read rail`);
  assert(!/style="display:\s*none;"[^>]*>\s*<h2>Actionable Now/.test(html), `${label} Actionable Now not hidden by default`);
  const htmlOnly = html.replace(/<script[\s\S]*?<\/script>/gi, "");
  assert(!/EARLY_SIGNAL_/.test(htmlOnly), `${label} markup has no EARLY_SIGNAL_`);
  assert(!/EARLY_SIGNAL/.test(htmlOnly), `${label} markup has no EARLY_SIGNAL`);
}

function assertJsSurface(label, js) {
  assert(js.includes("function getUserFacingSourceName"), `${label} has getUserFacingSourceName`);
  assert(js.includes("EARLY_SIGNAL"), `${label} sanitizer still knows EARLY_SIGNAL`);
  assert(js.includes("No widely read alerts yet."), `${label} Top Read empty state`);
  assert(js.includes("No actionable signals right now."), `${label} Actionable Now empty state`);
  assert(!/actionableBtn\.disabled\s*=\s*true/.test(js), `${label} does not disable Actionable by role`);
  assert(js.includes("actionableBtn.disabled = false"), `${label} keeps Actionable enabled`);
}

const htmlDisk = readPublic("market-alerts.html");
const jsDisk = readPublic("market-alerts.js");
const appJsDisk = readPublic("app.js");
const appHtmlDisk = readPublic("app.html");
const serverDisk = fs.readFileSync(path.join(ROOT, "server.js"), "utf8");

console.log("\n--- Disk (files Express sendFile would serve) ---");
assertHtmlSurface("public/market-alerts.html", htmlDisk);
assertJsSurface("public/market-alerts.js", jsDisk);
assert(
  appJsDisk.includes(`MARKET_ALERTS_EMBED_VERSION = '${UI_VERSION}'`),
  `app.js embed version is ${UI_VERSION}`
);
assert(
  appJsDisk.includes("embedQs += '&v=' + MARKET_ALERTS_EMBED_VERSION"),
  "app.js cache-busts Market Alerts iframe HTML"
);
assert(appJsDisk.includes("expectedAssetV") && appJsDisk.includes("childAssetV"), "app.js reloads iframe when v changes");
assert(appHtmlDisk.includes(`/app.js?v=ma-${UI_VERSION}`), `app.html loads cache-busted app.js`);
assert(serverDisk.includes('sendPublicNoStore(res, "market-alerts.html")'), "server no-store /market-alerts.html");
assert(serverDisk.includes('sendPublicNoStore(res, "market-alerts.js")'), "server no-store /market-alerts.js");
assert(serverDisk.includes('sendPublicNoStore(res, "app.js")'), "server no-store /app.js");
assert(serverDisk.includes('app.get("/market-alerts.html"'), "explicit /market-alerts.html route before static");

const cliRequireHttp = process.argv.includes("--require-http");
const envBase = String(process.env.MARKET_ALERTS_LIVE_BASE || "").trim().replace(/\/$/, "");
const bases = [];
if (envBase) bases.push(envBase);
bases.push("http://127.0.0.1:8080");
bases.push("http://localhost:8080");

const uniqueBases = [...new Set(bases)];
let httpChecked = 0;

console.log("\n--- HTTP (actual served surface) ---");
for (const base of uniqueBases) {
  let probe;
  try {
    probe = await fetchUrl(`${base}/market-alerts`, 6000);
  } catch (err) {
    console.log(`SKIP: ${base} not reachable (${err && err.cause ? err.cause.code || err.message : err.message})`);
    continue;
  }
  if (!probe.ok) {
    console.log(`SKIP: ${base}/market-alerts HTTP ${probe.status}`);
    continue;
  }
  httpChecked += 1;
  console.log(`\nHOST ${base}`);
  assert(probe.cacheControl.toLowerCase().includes("no-store") || probe.cacheControl.toLowerCase().includes("no-cache"), `${base} /market-alerts Cache-Control is no-store/no-cache (${probe.cacheControl || "missing"})`);
  assertHtmlSurface(`${base}/market-alerts`, probe.text);

  const iframePath = `/market-alerts.html?embed=1&appShell=1&v=${UI_VERSION}`;
  let iframe;
  try {
    iframe = await fetchUrl(`${base}${iframePath}`, 6000);
  } catch (err) {
    assert(false, `${base}${iframePath} fetch failed: ${err.message}`);
    continue;
  }
  assert(iframe.ok, `${base}${iframePath} HTTP ${iframe.status}`);
  assertHtmlSurface(`${base}${iframePath}`, iframe.text);

  let js;
  try {
    js = await fetchUrl(`${base}/market-alerts.js?v=${UI_VERSION}`, 6000);
  } catch (err) {
    assert(false, `${base}/market-alerts.js fetch failed: ${err.message}`);
    continue;
  }
  assert(js.ok, `${base}/market-alerts.js HTTP ${js.status}`);
  assertJsSurface(`${base}/market-alerts.js?v=${UI_VERSION}`, js.text);

  let appJs;
  try {
    appJs = await fetchUrl(`${base}/app.js?v=ma-${UI_VERSION}`, 6000);
  } catch (err) {
    assert(false, `${base}/app.js fetch failed: ${err.message}`);
    continue;
  }
  assert(appJs.ok, `${base}/app.js HTTP ${appJs.status}`);
  assert(
    appJs.text.includes(`MARKET_ALERTS_EMBED_VERSION = '${UI_VERSION}'`),
    `${base}/app.js embed version ${UI_VERSION}`
  );
}

if (httpChecked === 0) {
  const msg = "no HTTP host reached (localhost:8080 down and MARKET_ALERTS_LIVE_BASE unset/unreachable)";
  if (cliRequireHttp || envBase) {
    assert(false, msg);
  } else {
    console.log("WARN:", msg, "— disk checks still ran. Start `npm start` or set MARKET_ALERTS_LIVE_BASE.");
  }
}

if (failed) {
  console.error(`\n${failed} Market Alerts V1.3.2 live-surface test(s) failed`);
  process.exit(1);
}
console.log("\nAll Market Alerts V1.3.2 live-surface tests passed");
console.log(`HTTP hosts checked: ${httpChecked}`);
