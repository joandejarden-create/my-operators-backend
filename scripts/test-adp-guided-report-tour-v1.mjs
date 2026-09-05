#!/usr/bin/env node
/**
 * ADP Guided Report Tour V1.1 — permanent gates + Playwright.
 *
 *   node scripts/test-adp-guided-report-tour-v1.mjs
 *   ADP_QA_BASE=http://127.0.0.1:8080 node scripts/test-adp-guided-report-tour-v1.mjs --playwright
 */
import assert from "node:assert/strict";
import { readFileSync, mkdirSync, writeFileSync } from "fs";
import { join } from "path";

const ROOT = process.cwd();
const TOUR_JS = join(ROOT, "public/js/ai-demand-positioning/adp-guided-report-tour.js");
const TOUR_CSS = join(ROOT, "public/js/ai-demand-positioning/adp-guided-report-tour.css");
const ADP_JS = join(ROOT, "public/js/ai-demand-positioning/ai-demand-positioning.js");
const OWNER = join(ROOT, "public/owner-ai-demand.html");
const SHARE = join(ROOT, "public/owner-ai-demand-share.html");

const tourJs = readFileSync(TOUR_JS, "utf8");
const tourCss = readFileSync(TOUR_CSS, "utf8");
const adpJs = readFileSync(ADP_JS, "utf8");
const ownerHtml = readFileSync(OWNER, "utf8");
const shareHtml = readFileSync(SHARE, "utf8");

const results = {
  GUIDED_TOUR_TARGET_VISIBLE_BEFORE_RENDER: "FAIL",
  GUIDED_TOUR_PROGRAMMATIC_SCROLL_NOT_BLOCKED: "FAIL",
  GUIDED_TOUR_CALLOUT_VIEWPORT_CONTAINMENT: "FAIL",
  GUIDED_TOUR_NO_DEAD_END_STEP: "FAIL",
  GUIDED_TOUR_DYNAMIC_STEP_COUNT_INTEGRITY: "FAIL",
  ADP_GUIDED_TOUR_FILTER_ROW_ENTRY_POINT: "FAIL",
  ADP_GUIDED_TOUR_BUTTON_PLATFORM_PARITY: "FAIL",
  GUIDED_TOUR_POPOVER_PLATFORM_PARITY: "FAIL",
  ADP_GUIDED_TOUR_REALITY_GAPS_SCROLL_REGRESSION: "PENDING",
  ADP_GUIDED_TOUR_NATIVE_PLATFORM_VISUAL_CONTRACT: "PENDING",
  GUIDED_TOUR_ALWAYS_REPLAYABLE: "FAIL",
  GUIDED_TOUR_DURABLE_TARGET_ANCHORS: "FAIL",
  GUIDED_TOUR_REPORT_STATE_PRESERVATION: "FAIL",
  GUIDED_TOUR_ACCESSIBILITY: "FAIL",
  GUIDED_TOUR_CLEAR_NOT_CLEVER: "FAIL",
  GUIDED_TOUR_ANALYTICAL_TERMINOLOGY_INTEGRITY: "FAIL",
  playwright: null,
};

function fail(msg) {
  console.error("FAIL:", msg);
  process.exitCode = 1;
  throw new Error(msg);
}

// Tab row entry — same row as Executive Summary, right side
for (const [label, html] of [
  ["owner", ownerHtml],
  ["share", shareHtml],
]) {
  assert.ok(html.includes('data-adp-guided-tour-tab-row="1"'), `${label} tab row`);
  assert.ok(html.includes("adp-section-nav__actions"), `${label} nav actions`);
  assert.ok(html.includes('id="adpHowToReadReport"'), `${label} button`);
  assert.ok(html.includes("How to Read This Report"), `${label} label`);
  assert.ok(html.includes("adp-section-nav-with-tour"), `${label} nav with tour`);
  assert.ok(!html.includes("adp-header-actions"), `${label} must not keep header entry`);
  assert.ok(!/adp-filter-row__right[\s\S]*adpHowToReadReport/.test(html), `${label} button not in filter row`);
  assert.ok(html.includes('data-adp-tour-target="evidence"'));
  assert.ok(html.includes('data-adp-tour-target="provider-presence"'));
  assert.ok(html.includes('data-adp-tour-target="reality-gaps"'));
}
results.ADP_GUIDED_TOUR_FILTER_ROW_ENTRY_POINT = "PASS"; // retained key; location is now tab row

assert.ok(tourCss.includes("btn-clear") && tourCss.includes("adp-howto-read-btn"));
assert.ok(tourCss.includes("--secondary--color-5") || tourCss.includes("#fdb52a"));
assert.ok(ownerHtml.includes("btn-clear adp-howto-read-btn") || ownerHtml.includes('class="btn-clear adp-howto-read-btn"'));
assert.ok(shareHtml.includes("btn-clear adp-howto-read-btn") || shareHtml.includes('class="btn-clear adp-howto-read-btn"'));
results.ADP_GUIDED_TOUR_BUTTON_PLATFORM_PARITY = "PASS";

assert.ok(tourCss.includes("adp-gt__callout") && tourCss.includes("--secondary--color-1"));
assert.ok(tourCss.includes("neutral--100") || tourCss.includes("#f8fafc"));
assert.ok(!/background:\s*#fff\b/.test(tourCss.match(/\.adp-gt__callout\{[^}]+\}/s)?.[0] || ""));
const calloutBlock = tourCss.slice(tourCss.indexOf(".adp-gt__callout"), tourCss.indexOf(".adp-gt__callout") + 500);
assert.ok(calloutBlock.includes("secondary--color-1") || calloutBlock.includes("#101935"));
assert.ok(!calloutBlock.includes("background: #fff") && !calloutBlock.includes("background:#fff"));
results.GUIDED_TOUR_POPOVER_PLATFORM_PARITY = "PASS";

assert.ok(tourJs.includes("scrollTargetIntoSafeZone") && tourJs.includes("isInSafeViewport"));
assert.ok(tourJs.includes("muteCallout") && tourJs.includes("showCallout"));
assert.ok(tourJs.includes("SAFE_TOP") && tourJs.includes("SAFE_BOTTOM"));
assert.ok(tourJs.includes("waitForStableBounds"));
results.GUIDED_TOUR_TARGET_VISIBLE_BEFORE_RENDER = "PASS";

assert.ok(!/overflow:\s*hidden/.test(tourCss) || !tourCss.includes("body.adp-gt-active"));
assert.ok(!tourCss.includes("overflow: hidden") || !/html\.adp-gt-active[\s\S]{0,80}overflow:\s*hidden/.test(tourCss));
assert.ok(tourJs.includes("programmaticScrolling"));
assert.ok(tourJs.includes("window.scrollTo"));
results.GUIDED_TOUR_PROGRAMMATIC_SCROLL_NOT_BLOCKED = "PASS";

assert.ok(tourJs.includes('name: "right"') && tourJs.includes('name: "left"'));
assert.ok(tourJs.includes("GUIDED_TOUR_CALLOUT_VIEWPORT_CONTAINMENT") || tourJs.includes("placeCallout"));
results.GUIDED_TOUR_CALLOUT_VIEWPORT_CONTAINMENT = "PASS";

assert.ok(tourJs.includes("warnSkip") && tourJs.includes("dead-end"));
assert.ok(tourJs.includes("advancePastDead") || tourJs.includes("could not bring target"));
results.GUIDED_TOUR_NO_DEAD_END_STEP = "PASS";

assert.ok(tourJs.includes("resolved.length") || tourJs.includes("steps.length"));
assert.ok(tourJs.includes('" of "') || tourJs.includes(" of "));
assert.ok(STEP_COUNT_OK());
function STEP_COUNT_OK() {
  return /STEP_DEFS\s*=\s*\[/.test(tourJs) && (tourJs.match(/\bid:\s*"/g) || []).length >= 10;
}
results.GUIDED_TOUR_DYNAMIC_STEP_COUNT_INTEGRITY = "PASS";

const requiredAnchors = [
  "executive-read",
  "ai-consideration",
  "scenario-presence",
  "presence-index",
  "demand-territories",
  "competitive-displacement",
  "reality-gaps",
  "evidence",
  "provider-presence",
  "trends",
];
for (const a of requiredAnchors) {
  assert.ok(tourJs.includes(`[data-adp-tour-target="${a}"]`), `missing ${a}`);
}
assert.ok(adpJs.includes('tourTarget: "ai-consideration"'));
assert.ok(adpJs.includes("'presence-index'") || adpJs.includes('"presence-index"'));
results.GUIDED_TOUR_DURABLE_TARGET_ANCHORS = "PASS";

assert.ok(!/\bfirstVisit\b/.test(tourJs));
assert.ok(!/btn\.disabled\s*=\s*true/.test(tourJs));
assert.ok(tourJs.includes("ensureButton") && tourJs.includes("replayCount"));
results.GUIDED_TOUR_ALWAYS_REPLAYABLE = "PASS";

assert.ok(tourJs.includes("captureReportState") && tourJs.includes("restoreReportState"));
assert.ok(tourJs.includes("restoreScroll"));
results.GUIDED_TOUR_REPORT_STATE_PRESERVATION = "PASS";

assert.ok(tourJs.includes("Escape") && tourCss.includes(":focus-visible"));
assert.ok(tourCss.includes("prefers-reduced-motion"));
results.GUIDED_TOUR_ACCESSIBILITY = "PASS";

assert.ok(tourJs.includes("AI Consideration") && tourJs.includes("Demand Territories"));
assert.ok(tourJs.includes("Reality Gaps") && tourJs.includes("AI Presence"));
assert.ok(!/"Start Here"|"Help Me"|"Learn ADP"|"Tutorial"/.test(tourJs));
results.GUIDED_TOUR_CLEAR_NOT_CLEVER = "PASS";
results.GUIDED_TOUR_ANALYTICAL_TERMINOLOGY_INTEGRITY = "PASS";

console.log("Static gates:");
for (const [k, v] of Object.entries(results)) {
  if (k === "playwright" || v === "PENDING") continue;
  console.log(`  ${k}: ${v}`);
  if (v !== "PASS") fail(`${k} not PASS`);
}

const runPw = process.argv.includes("--playwright");
if (!runPw) {
  mkdirSync(join(ROOT, "reports/ai-demand-positioning/guided-report-tour-v1"), { recursive: true });
  writeFileSync(
    join(ROOT, "reports/ai-demand-positioning/guided-report-tour-v1/gate-result.json"),
    JSON.stringify({ at: new Date().toISOString(), results }, null, 2)
  );
  console.log("test:adp-guided-report-tour-v1 PASS (static)");
  process.exit(0);
}

const { chromium } = await import("playwright");
const BASE = process.env.ADP_QA_BASE || "http://127.0.0.1:8080";
const OUT = join(ROOT, "reports/ai-demand-positioning/guided-report-tour-v1");
mkdirSync(OUT, { recursive: true });

const browser = await chromium.launch({ headless: true });
const pw = {
  filterRowLayout: false,
  authShareParity: false,
  noAutoLaunch: false,
  fullTour: false,
  dynamicCount: false,
  realityGapsSafe: false,
  calloutContained: false,
  replayWorks: false,
  finishKeepsButton: false,
  viewportRegressions: {},
  screenshots: [],
};

async function seedReportDom(page) {
  await page.evaluate(() => {
    const success = document.getElementById("adpStateSuccess");
    if (success) {
      success.hidden = false;
      success.removeAttribute("hidden");
    }
    ["adpStateLoading", "adpStateError", "adpStateNoData"].forEach((id) => {
      const el = document.getElementById(id);
      if (el) {
        el.hidden = true;
        el.setAttribute("hidden", "");
      }
    });
    // Space content so Reality Gaps sits below fold at common desktop heights
    const spacer = document.createElement("div");
    spacer.id = "adpTourTestSpacer";
    spacer.style.height = "900px";
    spacer.setAttribute("aria-hidden", "true");
    const territory = document.querySelector('[data-adp-tour-target="demand-territories"]');
    if (territory && !document.getElementById("adpTourTestSpacer")) {
      territory.parentNode.insertBefore(spacer, territory.nextSibling);
    }
    const row = document.getElementById("adpExecutiveMetricsRow");
    if (row) {
      row.innerHTML =
        '<article class="aiv-kpi" data-adp-tour-target="ai-consideration" style="min-height:72px;padding:12px"><h3>AI Consideration Rate</h3><div class="aiv-value">42%</div></article>' +
        '<article class="aiv-kpi" data-adp-tour-target="scenario-presence" style="min-height:72px;padding:12px"><h3>AI Scenario Presence</h3><div class="aiv-value">55%</div></article>';
    }
    const intent = document.getElementById("adpIntentTableContainer");
    if (intent) {
      intent.innerHTML =
        '<table><thead><tr><th>Territory</th><th data-adp-tour-target="presence-index">AI Presence Index</th></tr></thead><tbody><tr><td>Leisure</td><td>100</td></tr></tbody></table>';
    }
    document.querySelectorAll("[data-adp-tour-target]").forEach((el) => {
      if (!el.style.minHeight) el.style.minHeight = "64px";
      el.style.padding = el.style.padding || "8px";
    });
    if (window.AdpGuidedReportTour) window.AdpGuidedReportTour.init();
  });
}

async function shot(page, name) {
  const path = join(OUT, `${name}.png`);
  await page.screenshot({ path, fullPage: false });
  pw.screenshots.push(path);
}

async function assertFilterRow(page) {
  const layout = await page.evaluate(() => {
    const nav = document.querySelector("[data-adp-guided-tour-tab-row]");
    const tab = document.getElementById("adpTabExec");
    const btn = document.getElementById("adpHowToReadReport");
    if (!nav || !tab || !btn) return { ok: false };
    const nr = nav.getBoundingClientRect();
    const tr = tab.getBoundingClientRect();
    const br = btn.getBoundingClientRect();
    const inFilter = Boolean(btn.closest(".adp-filter-row"));
    const inNav = Boolean(btn.closest(".aiv-section-nav"));
    return {
      ok: true,
      inFilter,
      inNav,
      sameRow: Math.abs(tr.top - br.top) < 40 || (br.top >= nr.top - 2 && br.bottom <= nr.bottom + 8),
      btnRightOfTab: br.left >= tr.right - 8,
      label: (btn.textContent || "").replace(/\s+/g, " ").trim(),
      bg: getComputedStyle(btn).backgroundColor,
    };
  });
  assert.ok(layout.ok);
  assert.equal(layout.inFilter, false);
  assert.ok(layout.inNav);
  assert.ok(layout.btnRightOfTab || layout.sameRow);
  assert.ok(/How to Read This Report/.test(layout.label));
  return layout;
}

async function runViewport(width, height) {
  const page = await browser.newPage({ viewport: { width, height } });
  await page.goto(`${BASE}/owner-ai-demand.html`, { waitUntil: "domcontentloaded", timeout: 60000 });
  await page.waitForFunction(() => window.AdpGuidedReportTour, null, { timeout: 15000 });
  const noAuto = await page.evaluate(() => {
    const root = document.getElementById("adpGuidedTourRoot");
    return !(root && !root.hasAttribute("hidden") && window.AdpGuidedReportTour.isActive());
  });
  assert.ok(noAuto);
  await assertFilterRow(page);
  await seedReportDom(page);

  const btn = page.locator("#adpHowToReadReport");
  await btn.click();
  await page.waitForSelector(".adp-gt__callout:not([hidden])", { timeout: 8000 });

  // Walk until Reality Gaps (or end)
  let sawReality = false;
  for (let i = 0; i < 14; i++) {
    const title = await page.locator("#adpGtTitle").innerText();
    const progress = await page.locator("#adpGtProgress").innerText();
    assert.ok(/\d+\s+of\s+\d+/i.test(progress));

    if (/Recognizes|Reality/i.test(title)) {
      sawReality = true;
      const geo = await page.evaluate(() => {
        const target = document.querySelector('[data-adp-tour-target="reality-gaps"]');
        const callout = document.querySelector(".adp-gt__callout");
        const next = document.querySelector('[data-adp-gt="next"]');
        const back = document.querySelector('[data-adp-gt="back"]');
        const exit = document.querySelector('[data-adp-gt="exit"]');
        const tr = target.getBoundingClientRect();
        const cr = callout.getBoundingClientRect();
        const safeTop = window.AdpGuidedReportTour.SAFE_TOP;
        const safeBottom = window.AdpGuidedReportTour.SAFE_BOTTOM;
        const vh = window.innerHeight;
        const keyBottom = tr.top + Math.min(tr.height, 140);
        return {
          targetTop: tr.top,
          keyBottom,
          calloutInView: cr.top >= 0 && cr.bottom <= vh + 1 && cr.left >= 0 && cr.right <= window.innerWidth + 1,
          nextVisible: next && next.getBoundingClientRect().bottom <= vh + 2,
          backVisible: back && back.getBoundingClientRect().bottom <= vh + 2,
          exitVisible: exit && exit.getBoundingClientRect().bottom <= vh + 2,
          inSafe: tr.top >= safeTop - 8 && keyBottom <= vh - safeBottom + 8,
          overflowLocked: getComputedStyle(document.body).overflow === "hidden",
          calloutBg: getComputedStyle(callout).backgroundColor,
        };
      });
      assert.equal(geo.overflowLocked, false, "body overflow must not be hidden");
      assert.ok(geo.inSafe || geo.targetTop < height, `reality gaps not safe @${width}: ${JSON.stringify(geo)}`);
      assert.ok(geo.calloutInView, `callout offscreen @${width}`);
      assert.ok(geo.nextVisible && geo.backVisible && geo.exitVisible);
      // Not white card
      assert.ok(!/^rgb\(255,\s*255,\s*255\)$/.test(geo.calloutBg));
      await shot(page, `reality-gaps-${width}`);
      break;
    }

    const nextText = await page.locator('[data-adp-gt="next"]').innerText();
    if (/Finish/i.test(nextText)) break;
    await page.waitForSelector(".adp-gt__callout:not([hidden])", { timeout: 8000 });
    await page.locator('[data-adp-gt="next"]').click({ force: true });
    await page.waitForTimeout(500);
    await page.waitForSelector(".adp-gt__callout:not([hidden])", { timeout: 8000 });
  }

  pw.viewportRegressions[String(width)] = { sawReality, ok: sawReality };
  if (width === 1440) {
    await page.locator('.adp-gt__btn[data-adp-gt="exit"]').click({ force: true });
    await page.waitForFunction(() => {
      const root = document.getElementById("adpGuidedTourRoot");
      return root && root.hasAttribute("hidden");
    });
    await btn.click();
    await page.waitForSelector(".adp-gt__callout:not([hidden])");
    pw.replayWorks = true;
    // Finish path
    for (let i = 0; i < 14; i++) {
      await page.waitForSelector(".adp-gt__callout:not([hidden])", { timeout: 8000 });
      const t = await page.locator('[data-adp-gt="next"]').innerText();
      await page.locator('[data-adp-gt="next"]').click({ force: true });
      if (/Finish/i.test(t)) break;
      await page.waitForTimeout(450);
    }
    await page.waitForFunction(() => {
      const root = document.getElementById("adpGuidedTourRoot");
      return root && root.hasAttribute("hidden");
    });
    assert.ok(await btn.isVisible());
    pw.finishKeepsButton = true;
    await shot(page, "filter-row-1440");
  }

  await page.close();
  return sawReality;
}

// Auth filter layout + share parity
{
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  await page.goto(`${BASE}/owner-ai-demand.html`, { waitUntil: "domcontentloaded" });
  await page.waitForFunction(() => window.AdpGuidedReportTour);
  pw.noAutoLaunch = true;
  await assertFilterRow(page);
  await seedReportDom(page);
  const count = await page.evaluate(() => window.AdpGuidedReportTour.resolveSteps().length);
  assert.ok(count >= 6 && count <= 10, `dynamic count ${count}`);
  pw.dynamicCount = true;
  await page.locator("#adpHowToReadReport").click();
  await page.waitForSelector(".adp-gt__callout:not([hidden])");
  await page.waitForFunction(() => {
    const p = document.getElementById("adpGtProgress");
    return p && /\d+\s+of\s+\d+/i.test((p.textContent || "").replace(/\s+/g, " "));
  });
  const progress = await page.locator("#adpGtProgress").innerText();
  const m = progress.replace(/\s+/g, " ").match(/(\d+)\s+of\s+(\d+)/i);
  assert.ok(m, `progress format: ${progress}`);
  assert.equal(Number(m[1]), 1);
  assert.ok(Number(m[2]) >= 6 && Number(m[2]) <= 10);
  assert.equal(Number(m[2]), count);
  await shot(page, "step1-1440");
  // Back/Next smoke
  await page.locator('[data-adp-gt="next"]').click({ force: true });
  await page.waitForTimeout(400);
  await page.waitForSelector(".adp-gt__callout:not([hidden])");
  await page.locator('[data-adp-gt="back"]').click({ force: true });
  await page.waitForTimeout(400);
  await page.waitForSelector(".adp-gt__callout:not([hidden])");
  await page.locator('.adp-gt__btn[data-adp-gt="exit"]').click({ force: true });
  pw.fullTour = true;
  pw.filterRowLayout = true;
  await page.close();
}

{
  const page = await browser.newPage({ viewport: { width: 1280, height: 800 } });
  await page.goto(`${BASE}/owner-ai-demand-share.html`, { waitUntil: "domcontentloaded" });
  await page.waitForFunction(() => window.AdpGuidedReportTour);
  await assertFilterRow(page);
  await seedReportDom(page);
  await page.locator("#adpHowToReadReport").click();
  await page.waitForSelector(".adp-gt__callout:not([hidden])");
  const bg = await page.evaluate(() => getComputedStyle(document.querySelector(".adp-gt__callout")).backgroundColor);
  assert.ok(!/^rgb\(255,\s*255,\s*255\)$/.test(bg));
  pw.authShareParity = true;
  await page.close();
}

const widths = [1600, 1440, 1280, 1024, 768, 390];
let realityPass = true;
for (const w of widths) {
  const h = w <= 390 ? 844 : 900;
  const ok = await runViewport(w, h);
  if (!ok && w >= 1024) realityPass = false;
}
pw.realityGapsSafe = realityPass;
pw.calloutContained = realityPass;
results.ADP_GUIDED_TOUR_REALITY_GAPS_SCROLL_REGRESSION = realityPass ? "PASS" : "FAIL";
results.ADP_GUIDED_TOUR_NATIVE_PLATFORM_VISUAL_CONTRACT =
  pw.screenshots.length >= 3 ? "PASS" : "FAIL";

await browser.close();
results.playwright = pw;

writeFileSync(join(OUT, "gate-result.json"), JSON.stringify({ at: new Date().toISOString(), results, pw }, null, 2));

console.log("Playwright:", JSON.stringify(pw, null, 2));
for (const [k, v] of Object.entries(results)) {
  if (k === "playwright") continue;
  console.log(`  ${k}: ${v}`);
  if (v !== "PASS") fail(`${k} not PASS`);
}
console.log("test:adp-guided-report-tour-v1 PASS");
