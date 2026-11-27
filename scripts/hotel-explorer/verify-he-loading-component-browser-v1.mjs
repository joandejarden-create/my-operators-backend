#!/usr/bin/env node
/**
 * Browser regression: Hotel Explorer Contact Intelligence must reuse the shared
 * HeLoadingUx / DealalityWaveLoader compact toast — not a custom oversized card.
 *
 *   DEALALITY_LOCAL_ADMIN_BASE=http://127.0.0.1:8080 \
 *     node scripts/hotel-explorer/verify-he-loading-component-browser-v1.mjs
 *
 * Exit 0 on PASS. Without a reachable server, exits 2 (NOT_TESTED) unless
 * --require-server is passed (then exit 1).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const BASE = (
  process.env.DEALALITY_LOCAL_ADMIN_BASE ||
  "http://127.0.0.1:8080"
).replace(/\/$/, "");
const HOTEL = process.env.HE_LOADING_VERIFY_HOTEL_ID || "recLuxvwwxID7U2B8";
const requireServer = process.argv.includes("--require-server");
const OUT_DIR = path.join(ROOT, "reports", "hotel-explorer");
const OUT = path.join(OUT_DIR, "HE-LOADING-COMPONENT-BROWSER-V1.json");

async function probeServer() {
  try {
    const res = await fetch(`${BASE}/deal-capture-radar-with-ranked-list.html`, {
      method: "HEAD",
      signal: AbortSignal.timeout(4000),
    });
    return res.ok || res.status === 405 || res.status === 200;
  } catch {
    return false;
  }
}

const serverOk = await probeServer();
if (!serverOk) {
  const payload = {
    contract: "he-loading-component-browser-v1",
    status: "NOT_TESTED",
    reason: `server_unreachable:${BASE}`,
    generatedAt: new Date().toISOString(),
  };
  fs.mkdirSync(OUT_DIR, { recursive: true });
  fs.writeFileSync(OUT, JSON.stringify(payload, null, 2));
  console.log(JSON.stringify(payload, null, 2));
  process.exit(requireServer ? 1 : 2);
}

async function launchBrowser() {
  try {
    return await chromium.launch({ headless: true });
  } catch (err) {
    const msg = String(err && err.message ? err.message : err);
    if (/Executable doesn't exist|browserType\.launch/i.test(msg)) {
      // Sandbox / CI without Playwright browser cache — use installed Chrome.
      return chromium.launch({ channel: "chrome", headless: true });
    }
    throw err;
  }
}

const browser = await launchBrowser();
const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });

const url =
  `${BASE}/deal-capture-radar-with-ranked-list.html` +
  `?hotelExplorer=${encodeURIComponent(HOTEL)}&hexTab=contacts&cb=${Date.now()}`;

await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });
await page.waitForFunction(() => typeof window.HotelExplorer === "object", null, {
  timeout: 30000,
});

const metrics = await page.evaluate(async (hotelId) => {
  const captures = [];
  const obs = new MutationObserver(() => {
    const toast = document.querySelector(
      ".hci-root--loading .dealality-wave-loader, .he-async-loading .dealality-wave-loader"
    );
    if (!toast) return;
    const r = toast.getBoundingClientRect();
    const s = getComputedStyle(toast);
    const wave = toast.querySelector(".wave-container");
    const wr = wave ? wave.getBoundingClientRect() : null;
    captures.push({
      className: toast.className,
      w: Math.round(r.width),
      h: Math.round(r.height),
      bg: s.backgroundColor,
      borderRadius: s.borderRadius,
      waveW: wr ? Math.round(wr.width) : null,
      waveH: wr ? Math.round(wr.height) : null,
      hasHciLoading: !!document.querySelector(".hci-loading"),
      hasAsync: !!document.querySelector(".he-async-loading"),
      text: (toast.textContent || "").trim().slice(0, 80),
    });
  });
  obs.observe(document.documentElement, { childList: true, subtree: true });

  const origFetch = window.fetch.bind(window);
  window.fetch = (input, init) => {
    const u = String((input && input.url) || input || "");
    if (/contact|corporate/i.test(u)) {
      return new Promise((resolve) =>
        setTimeout(() => resolve(origFetch(input, init)), 1600)
      );
    }
    return origFetch(input, init);
  };

  if (typeof HotelExplorer.open === "function") {
    HotelExplorer.open(hotelId, { tab: "contacts" });
  } else if (typeof HotelExplorer.openDossierById === "function") {
    HotelExplorer.openDossierById(hotelId);
    if (typeof HotelExplorer.setTab === "function") HotelExplorer.setTab("contacts");
  }

  await new Promise((r) => setTimeout(r, 900));
  obs.disconnect();

  const scripts = [...document.querySelectorAll("script[src*='hotel-contact'], link[href*='hotel-explorer.css']")].map(
    (el) => el.src || el.href
  );

  // Prefer the last unique compact capture
  const uniq = [];
  const seen = new Set();
  for (const c of captures) {
    const k = `${c.w}x${c.h}:${c.bg}`;
    if (!seen.has(k)) {
      seen.add(k);
      uniq.push(c);
    }
  }
  const best = uniq.sort((a, b) => a.h - b.h)[0] || null;

  return {
    captureCount: captures.length,
    best,
    uniq,
    scripts,
    finalHasHciLoading: !!document.querySelector(".hci-loading"),
    hexOpen: !!document.querySelector(".hex-root.is-open"),
  };
}, HOTEL);

await browser.close();

const best = metrics.best;
const checks = {
  hexOpen: metrics.hexOpen === true,
  capturedLoader: !!best,
  usesSharedWaveLoader:
    !!best &&
    /dealality-wave-loader/.test(best.className) &&
    /dealality-wave-loader--toast/.test(best.className),
  usesAsyncSection: best?.hasAsync === true,
  noCustomHciLoading: best?.hasHciLoading === false && metrics.finalHasHciLoading === false,
  compactWidth: !!best && best.w > 0 && best.w <= 240,
  compactHeight: !!best && best.h > 0 && best.h <= 72,
  elevatedTheme:
    !!best &&
    (best.bg === "rgb(33, 44, 77)" ||
      best.bg === "rgba(33, 44, 77, 1)" ||
      /33,\s*44,\s*77/.test(best.bg || "")),
  smallWave: !!best && best.waveW != null && best.waveW <= 24 && best.waveH <= 24,
};

const failed = Object.entries(checks)
  .filter(([, ok]) => !ok)
  .map(([k]) => k);

const status = failed.length ? "FAIL" : "PASS";
const payload = {
  contract: "he-loading-component-browser-v1",
  status,
  base: BASE,
  hotelId: HOTEL,
  generatedAt: new Date().toISOString(),
  checks,
  failed,
  metrics,
};

fs.mkdirSync(OUT_DIR, { recursive: true });
fs.writeFileSync(OUT, JSON.stringify(payload, null, 2));
console.log(JSON.stringify(payload, null, 2));

if (status !== "PASS") {
  assert.fail(`he-loading-component-browser-v1 failed: ${failed.join(", ")}`);
}
