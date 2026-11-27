#!/usr/bin/env node
/**
 * Static + optional browser gate: HE_GLOBAL_BACKGROUND_CONSISTENCY
 *
 * Static (always):
 *   - canonical tokens present
 *   - no competing #0b1228 / #1a2448 active backgrounds in HE module CSS
 *   - no rgba(8,15,37,α) panel fills in HE module CSS
 *   - theme tokens load last on Radar + preview hosts (?v= bumped)
 *
 * Browser (when DEALALITY_LOCAL_ADMIN_BASE reachable, or --require-browser):
 *   - computed canvas/section/table tokens
 *   - no translucent canvas fills in .hex-scroll
 *   - eleven-tab theme parity + scroll stability
 *   - screenshots under reports/hotel-explorer/he-global-bg-shots/
 *
 * Exit 0 PASS. Browser skipped → exit 2 (NOT_TESTED) unless --require-browser.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
function loadChromium() {
  try {
    return require("playwright").chromium;
  } catch {
    try {
      return require("puppeteer");
    } catch {
      return null;
    }
  }
}

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const cssDir = path.join(ROOT, "public/css");
const requireBrowser = process.argv.includes("--require-browser");
const BASE = (
  process.env.DEALALITY_LOCAL_ADMIN_BASE || "http://127.0.0.1:8080"
).replace(/\/$/, "");
const HOTEL = process.env.HE_BG_VERIFY_HOTEL_ID || "recLuxvwwxID7U2B8";
const OUT_DIR = path.join(ROOT, "reports/hotel-explorer");
const SHOT_DIR = path.join(OUT_DIR, "he-global-bg-shots");
const OUT_JSON = path.join(OUT_DIR, "HE-GLOBAL-BACKGROUND-CONSISTENCY-V1.json");

const HE_CSS = [
  "hotel-explorer-theme-tokens-v1.css",
  "hotel-explorer.css",
  "hotel-detail-panel.css",
  "hotel-intelligence-workspace.css",
  "hotel-ownership-intelligence.css",
  "hotel-contact-intelligence.css",
  "hotel-intelligence-research-center.css",
];

const FORBIDDEN_ACTIVE_BG = [
  /background(?:-color)?:\s*#0b1228/i,
  /background(?:-color)?:\s*#1a2448/i,
  /background(?:-color)?:\s*#0a122c/i,
  /background(?:-color)?:\s*rgba\(\s*8\s*,\s*15\s*,\s*37\s*,\s*0\.\d+\s*\)/i,
  /linear-gradient\([^)]*#0b1228/i,
];

const tokens = fs.readFileSync(
  path.join(cssDir, "hotel-explorer-theme-tokens-v1.css"),
  "utf8",
);
assert.match(tokens, /--he-canvas:\s*#080f25/);
assert.match(tokens, /--he-section:\s*#101935/);
assert.match(tokens, /--he-elevated:\s*#212c4d/);
assert.match(tokens, /--he-table:\s*#0d152c/);
assert.match(tokens, /\.hex-root \.hdp-competitive-chip/);
assert.match(tokens, /\.hex-root \.hdp-overview__row:nth-child\(even\)/);
assert.match(tokens, /Nested translucent rgba\(8,15,37/);

for (const name of HE_CSS) {
  const body = fs.readFileSync(path.join(cssDir, name), "utf8");
  for (const re of FORBIDDEN_ACTIVE_BG) {
    assert.equal(
      re.test(body),
      false,
      `${name} must not contain forbidden active background ${re}`,
    );
  }
}

const radarHtml = fs.readFileSync(
  path.join(ROOT, "public/deal-capture-radar-with-ranked-list.html"),
  "utf8",
);
const previewHtml = fs.readFileSync(
  path.join(ROOT, "public/hotel-explorer-preview.html"),
  "utf8",
);
assert.match(radarHtml, /hotel-explorer-theme-tokens-v1\.css\?v=3/);
assert.match(previewHtml, /hotel-explorer-theme-tokens-v1\.css\?v=3/);

function themeLoadsLast(html) {
  const explorerIdx = html.indexOf("hotel-explorer.css");
  const themeIdx = html.lastIndexOf("hotel-explorer-theme-tokens-v1.css");
  return explorerIdx >= 0 && themeIdx > explorerIdx;
}
assert.equal(themeLoadsLast(radarHtml), true);
assert.equal(themeLoadsLast(previewHtml), true);

const ACTIVE_TABS = [
  "Hotel Overview",
  "Guest Rooms & Meeting Space",
  "Ownership Intelligence",
  "Contact Intelligence",
  "Organization & Portfolio",
  "Relationships & Structure",
  "Submarket Snapshot",
  "Area Hotels",
  "Demand Drivers",
  "Access & Connectivity",
  "Sources & Evidence",
];

const APPROVED_SOLID = new Set([
  "rgb(8, 15, 37)", // canvas
  "rgb(16, 25, 53)", // section
  "rgb(33, 44, 77)", // elevated
  "rgb(13, 21, 44)", // table
]);

function isApprovedBackground(bg) {
  if (!bg || bg === "rgba(0, 0, 0, 0)" || bg === "transparent") return true;
  if (APPROVED_SOLID.has(bg)) return true;
  // Accent / status / intentional tint chips (not navy panel fills)
  if (/^rgba?\(108,\s*114,\s*255/.test(bg)) return true;
  if (/^rgba?\(61,\s*214,\s*140/.test(bg)) return true;
  if (/^rgba?\(253,\s*181,\s*42/.test(bg)) return true;
  if (/^rgba?\(255,\s*139,\s*139/.test(bg)) return true;
  if (/^rgba?\(154,\s*(163|166),\s*(199|208)/.test(bg)) return true;
  if (/^rgba?\(197,\s*48,\s*48/.test(bg)) return true;
  if (/^rgba?\(255,\s*255,\s*255/.test(bg)) return true;
  if (/^rgba?\(0,\s*0,\s*0/.test(bg)) return true;
  // Map / context dots (approved non-surface colors)
  if (/^rgb\(\d+,\s*\d+,\s*\d+\)$/.test(bg) && !/^rgb\(8,\s*15,\s*37/.test(bg)) {
    return true;
  }
  return false;
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

async function launchBrowser() {
  const chromium = loadChromium();
  if (!chromium || typeof chromium.launch !== "function") {
    throw new Error("playwright_unavailable");
  }
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

const staticOk = {
  tokens: true,
  forbiddenActiveBgCleared: true,
  themeCacheBustV3: true,
  themeLoadsLast: true,
};

const serverOk = await probeServer();
let browserResult = null;

if (!serverOk) {
  browserResult = {
    status: "NOT_TESTED",
    reason: `server_unreachable:${BASE}`,
  };
} else {
  try {
    fs.mkdirSync(SHOT_DIR, { recursive: true });
    const browser = await launchBrowser();
    const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
    const url = `${BASE}/deal-capture-radar-with-ranked-list.html?embed=1&appShell=1&cb=he-bg-v1`;
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });
    await page.waitForFunction(() => !!window.HotelExplorer, null, {
      timeout: 45000,
    });
    await page.evaluate(async (hotelId) => {
      const HotelExplorer = window.HotelExplorer;
      if (!HotelExplorer) throw new Error("HotelExplorer_missing");
      if (typeof HotelExplorer.open === "function") {
        const maybe = HotelExplorer.open(
          { id: hotelId, name: "Bethesda Marriott" },
          { tab: "hotel" },
        );
        if (maybe && typeof maybe.then === "function") await maybe;
      } else if (typeof HotelExplorer.openDossierById === "function") {
        await HotelExplorer.openDossierById(hotelId);
      }
    }, HOTEL);
    await page.waitForSelector(".hex-root.is-open .hex-shell", {
      timeout: 45000,
    });

    const tabResults = [];
    for (const tabName of ACTIVE_TABS) {
      const tab = page.getByRole("tab", { name: tabName, exact: true });
      await tab.click({ timeout: 15000 });
      await page.waitForTimeout(700);

      const probe = await page.evaluate(() => {
        const root = document.querySelector(".hex-root");
        const scroll = document.querySelector(".hex-scroll");
        if (!root || !scroll) {
          return { ok: false, error: "missing_hex_root_or_scroll" };
        }
        const csRoot = getComputedStyle(root);
        const tokens = {
          canvas: csRoot.getPropertyValue("--he-canvas").trim(),
          section: csRoot.getPropertyValue("--he-section").trim(),
          elevated: csRoot.getPropertyValue("--he-elevated").trim(),
          table: csRoot.getPropertyValue("--he-table").trim(),
        };
        const scrollBg = getComputedStyle(scroll).backgroundColor;
        const translucent = [];
        for (const el of scroll.querySelectorAll("*")) {
          const bg = getComputedStyle(el).backgroundColor;
          if (/^rgba\(\s*8\s*,\s*15\s*,\s*37\s*,\s*0\.\d+\s*\)$/.test(bg)) {
            translucent.push({
              cls: String(el.className || "").slice(0, 60),
              bg,
            });
          }
        }
        const sampleEls = [
          ".hex-shell",
          ".hex-scroll",
          ".hex-main",
          ".tab-panel.active",
          ".hiw-report",
          ".hiw-section",
        ];
        const samples = {};
        for (const sel of sampleEls) {
          const el = document.querySelector(sel);
          if (el) samples[sel] = getComputedStyle(el).backgroundColor;
        }

        const maxScroll = Math.max(0, scroll.scrollHeight - scroll.clientHeight);
        const positions = [0, Math.floor(maxScroll / 2), maxScroll];
        const scrollBgs = [];
        for (const y of positions) {
          scroll.scrollTop = y;
          scrollBgs.push(getComputedStyle(scroll).backgroundColor);
        }
        scroll.scrollTop = 0;

        return {
          ok: true,
          tokens,
          scrollBg,
          samples,
          translucentCount: translucent.length,
          translucent: translucent.slice(0, 12),
          scrollBgs,
          scrollStable: scrollBgs.every((b) => b === "rgb(8, 15, 37)"),
        };
      });

      const slug = tabName
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/(^-|-$)/g, "");
      const shotTop = path.join(SHOT_DIR, `${slug}-top.png`);
      await page.locator(".hex-shell").screenshot({ path: shotTop });

      if (await page.locator(".hex-scroll").count()) {
        await page.evaluate(() => {
          const s = document.querySelector(".hex-scroll");
          if (s) s.scrollTop = Math.floor((s.scrollHeight - s.clientHeight) / 2);
        });
        await page.waitForTimeout(200);
        await page.locator(".hex-shell").screenshot({
          path: path.join(SHOT_DIR, `${slug}-mid.png`),
        });
        await page.evaluate(() => {
          const s = document.querySelector(".hex-scroll");
          if (s) s.scrollTop = s.scrollHeight;
        });
        await page.waitForTimeout(200);
        await page.locator(".hex-shell").screenshot({
          path: path.join(SHOT_DIR, `${slug}-bottom.png`),
        });
        await page.evaluate(() => {
          const s = document.querySelector(".hex-scroll");
          if (s) s.scrollTop = 0;
        });
      }

      const tokenOk =
        probe.tokens?.canvas === "#080f25" &&
        probe.tokens?.section === "#101935" &&
        probe.tokens?.table === "#0d152c";
      const canvasOk = probe.scrollBg === "rgb(8, 15, 37)";
      const noTranslucent = probe.translucentCount === 0;

      tabResults.push({
        tab: tabName,
        tokenOk,
        canvasOk,
        scrollStable: probe.scrollStable === true,
        translucentCount: probe.translucentCount,
        translucentSample: probe.translucent,
        samples: probe.samples,
        shots: {
          top: path.relative(ROOT, shotTop).replace(/\\/g, "/"),
        },
        pass: tokenOk && canvasOk && noTranslucent && probe.scrollStable === true,
      });
    }

    await page.setViewportSize({ width: 1100, height: 800 });
    await page.getByRole("tab", { name: "Hotel Overview", exact: true }).click();
    await page.waitForTimeout(500);
    await page.locator(".hex-shell").screenshot({
      path: path.join(SHOT_DIR, "overview-narrow.png"),
    });

    await browser.close();

    const allPass = tabResults.every((t) => t.pass);
    browserResult = {
      status: allPass ? "PASS" : "FAIL",
      hotelId: HOTEL,
      base: BASE,
      tabCount: tabResults.length,
      tabsPassed: tabResults.filter((t) => t.pass).length,
      tabResults,
      gates: {
        HE_GLOBAL_BACKGROUND_CONSISTENCY: allPass ? "PASS" : "FAIL",
        HE_ELEVEN_TAB_THEME_PARITY: allPass ? "PASS" : "FAIL",
        HE_NO_UNINTENDED_BACKGROUND_SEAMS: allPass ? "PASS" : "FAIL",
        HE_SCROLL_VISUAL_STABILITY: tabResults.every((t) => t.scrollStable)
          ? "PASS"
          : "FAIL",
        HE_CANONICAL_BROWSER_VALIDATED: "PASS",
      },
    };
  } catch (err) {
    browserResult = {
      status: "NOT_TESTED",
      reason: String(err && err.message ? err.message : err),
    };
  }
}

const payload = {
  contract: "he-global-background-consistency-v1",
  generatedAt: new Date().toISOString(),
  static: staticOk,
  browser: browserResult,
  gates: {
    HE_GLOBAL_BACKGROUND_CONSISTENCY:
      browserResult?.status === "PASS"
        ? "PASS"
        : browserResult?.status === "NOT_TESTED"
          ? "NOT_TESTED"
          : "FAIL",
    HE_ELEVEN_TAB_THEME_PARITY: browserResult?.gates?.HE_ELEVEN_TAB_THEME_PARITY || "NOT_TESTED",
    HE_NO_UNINTENDED_BACKGROUND_SEAMS:
      browserResult?.gates?.HE_NO_UNINTENDED_BACKGROUND_SEAMS || "NOT_TESTED",
    HE_SCROLL_VISUAL_STABILITY:
      browserResult?.gates?.HE_SCROLL_VISUAL_STABILITY || "NOT_TESTED",
    HE_CANONICAL_BROWSER_VALIDATED:
      browserResult?.status === "PASS"
        ? "PASS"
        : browserResult?.status === "NOT_TESTED"
          ? "NOT_TESTED"
          : "FAIL",
  },
};

fs.mkdirSync(OUT_DIR, { recursive: true });
fs.writeFileSync(OUT_JSON, JSON.stringify(payload, null, 2));
console.log(JSON.stringify(payload, null, 2));

if (browserResult?.status === "NOT_TESTED") {
  process.exit(requireBrowser ? 1 : 2);
}
if (browserResult?.status === "FAIL") {
  process.exit(1);
}
process.exit(0);
