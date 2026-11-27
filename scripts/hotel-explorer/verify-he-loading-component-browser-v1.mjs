#!/usr/bin/env node
/**
 * Browser regression: Hotel Explorer Contact Intelligence must reuse the shared
 * HeLoadingUx / DealalityWaveLoader compact black wave toast — not a custom
 * elevated blue-gray card or static icon.
 *
 * Compares Contact Intelligence loader computed styles to a reference
 * DealalityWaveLoader.toastHtml mount (canonical shared component).
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

function isBlackToastBg(bg) {
  const s = String(bg || "");
  // rgba(0,0,0,0.85|0.9) or rgb(0,0,0)
  if (/rgba?\(\s*0\s*,\s*0\s*,\s*0(\s*,\s*0\.(85|9[0-9]?))?\s*\)/i.test(s)) {
    return true;
  }
  return false;
}

function isElevatedBlueGray(bg) {
  return /33\s*,\s*44\s*,\s*77/.test(String(bg || ""));
}

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
  function snap(toast) {
    if (!toast) return null;
    const r = toast.getBoundingClientRect();
    const s = getComputedStyle(toast);
    const wave = toast.querySelector(".wave-container");
    const wr = wave ? wave.getBoundingClientRect() : null;
    const wave1 = toast.querySelector(".wave-1");
    const w1s = wave1 ? getComputedStyle(wave1) : null;
    return {
      className: toast.className,
      w: Math.round(r.width),
      h: Math.round(r.height),
      bg: s.backgroundColor,
      borderRadius: s.borderRadius,
      borderColor: s.borderTopColor,
      padding: s.padding,
      waveW: wr ? Math.round(wr.width) : null,
      waveH: wr ? Math.round(wr.height) : null,
      hasWaveOrb: !!wave,
      hasWaveLayers: !!toast.querySelector(".wave-1") && !!toast.querySelector(".wave-2"),
      waveAnimation:
        w1s && w1s.animationName && w1s.animationName !== "none"
          ? w1s.animationName
          : null,
      text: (toast.textContent || "").trim().slice(0, 80),
    };
  }

  // Reference: canonical shared toast outside .hex-root (no HE overrides).
  const refHost = document.createElement("div");
  refHost.id = "he-loading-ref-toast-host";
  refHost.style.cssText =
    "position:fixed;left:-9999px;top:0;pointer-events:none;z-index:-1;";
  const refHtml =
    window.DealalityWaveLoader && typeof window.DealalityWaveLoader.toastHtml === "function"
      ? window.DealalityWaveLoader.toastHtml("Loading Reference…")
      : "";
  refHost.innerHTML = refHtml;
  document.body.appendChild(refHost);
  const reference = snap(refHost.querySelector(".dealality-wave-loader"));

  const captures = [];
  const obs = new MutationObserver(() => {
    const toast = document.querySelector(
      ".hci-root--loading .dealality-wave-loader, .he-async-loading .dealality-wave-loader"
    );
    if (!toast) return;
    const shot = snap(toast);
    if (!shot) return;
    shot.hasHciLoading = !!document.querySelector(".hci-loading");
    shot.hasAsync = !!document.querySelector(".he-async-loading");
    captures.push(shot);
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

  const scripts = [
    ...document.querySelectorAll(
      "script[src*='hotel-contact'], link[href*='hotel-explorer.css'], link[href*='dealality-wave-loader']"
    ),
  ].map((el) => el.src || el.href);

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

  refHost.remove();

  return {
    captureCount: captures.length,
    best,
    reference,
    uniq,
    scripts,
    finalHasHciLoading: !!document.querySelector(".hci-loading"),
    hexOpen: !!document.querySelector(".hex-root.is-open"),
  };
}, HOTEL);

await browser.close();

const best = metrics.best;
const reference = metrics.reference;

const matchesReference =
  !!best &&
  !!reference &&
  best.bg === reference.bg &&
  best.borderRadius === reference.borderRadius &&
  best.waveW === reference.waveW &&
  best.waveH === reference.waveH &&
  !!best.hasWaveLayers &&
  !!reference.hasWaveLayers;

const checks = {
  hexOpen: metrics.hexOpen === true,
  capturedLoader: !!best,
  usesSharedWaveLoader:
    !!best &&
    /dealality-wave-loader/.test(best.className) &&
    /dealality-wave-loader--toast/.test(best.className),
  usesAsyncSection: best?.hasAsync === true,
  noCustomHciLoading: best?.hasHciLoading === false && metrics.finalHasHciLoading === false,
  compactWidth: !!best && best.w > 0 && best.w <= 360,
  compactHeight: !!best && best.h > 0 && best.h <= 96,
  blackToastTheme: !!best && isBlackToastBg(best.bg) && !isElevatedBlueGray(best.bg),
  animatedWave:
    !!best &&
    best.hasWaveOrb === true &&
    best.hasWaveLayers === true &&
    (best.waveAnimation != null || best.waveW === 24),
  canonicalWaveSize: !!best && best.waveW === 24 && best.waveH === 24,
  referenceMounted: !!reference && isBlackToastBg(reference.bg),
  matchesReferenceLoader: matchesReference,
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
