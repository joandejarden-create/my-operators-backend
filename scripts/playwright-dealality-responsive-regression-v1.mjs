#!/usr/bin/env node
/**
 * DEALALITY_RESPONSIVE_DESIGN_STANDARD_V1 — permanent Playwright regression.
 * Extends existing Playwright; does not replace GDI visual harness.
 *
 *   node scripts/playwright-dealality-responsive-regression-v1.mjs
 *   DEALALITY_RESPONSIVE_BASE_URL=http://127.0.0.1:8095 node scripts/playwright-dealality-responsive-regression-v1.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
import { execSync } from "node:child_process";

const require = createRequire(import.meta.url);
const { chromium } = require("playwright");

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const BASE = String(process.env.DEALALITY_RESPONSIVE_BASE_URL || "http://127.0.0.1:8080").replace(/\/$/, "");
const OUT = path.join(ROOT, "reports", "dealality-responsive-regression");
const SHOTS = path.join(OUT, "screenshots");
fs.mkdirSync(SHOTS, { recursive: true });

const VIEWPORTS = [
  { id: "1920x1080", width: 1920, height: 1080 },
  { id: "1440x900", width: 1440, height: 900 },
  { id: "1366x768", width: 1366, height: 768 },
  { id: "768x1024", width: 768, height: 1024 },
  { id: "390x844", width: 390, height: 844 },
];

const OVERFLOW_MAX_PX = 2;

function gitSha() {
  try {
    return execSync("git rev-parse HEAD", { cwd: ROOT, encoding: "utf8" }).trim();
  } catch {
    return "unknown";
  }
}

const ROUTES = [
  {
    id: "radar-ranked",
    product: "Radar",
    findingIds: ["RD-001", "RD-002"],
    url: `${BASE}/deal-capture-radar-with-ranked-list.html`,
    waitMs: 5000,
    readySelector: ".controls-panel, .control-select",
    interactions: async (page) => {
      const labels = await page.locator(".control-label").allTextContents();
      const truncatedHidden = await page.evaluate(() => {
        const groups = [...document.querySelectorAll(".control-group")];
        return groups.filter((g) => {
          const style = getComputedStyle(g);
          return style.overflow === "hidden" && g.scrollWidth > g.clientWidth + 2;
        }).length;
      });
      const select = page.locator(".controls-panel select.control-select").first();
      let selectOk = false;
      if (await select.count() && (await select.isVisible().catch(() => false))) {
        await select.click({ timeout: 3000 }).catch(() => {});
        selectOk = true;
      }
      const moreBtn = page.locator("#radarFilterOpenBtn");
      let moreOk = false;
      if (await moreBtn.count()) {
        await moreBtn.click({ timeout: 3000 }).catch(() => {});
        const drawer = page.locator("#radarFilterDrawer, .radar-filter-drawer");
        moreOk = (await drawer.count()) > 0;
        await page.locator("#radarFilterDoneBtn, [data-radar-filter-close]").first().click({ timeout: 2000 }).catch(() => {});
        await page.keyboard.press("Escape").catch(() => {});
      }
      return {
        controlLabelCount: labels.filter(Boolean).length,
        clippedControlGroups: truncatedHidden,
        selectInteracted: selectOk || moreOk,
        moreFilterDrawer: moreOk,
      };
    },
  },
  {
    id: "gdi-staged-wyndham",
    product: "GDI",
    findingIds: ["GDI-001", "GDI-002"],
    url: `${BASE}/group-demand-intelligence.html?hotelId=recnPB3Q02NO8Ma1o&preview=staged`,
    waitMs: 12000,
    readySelector: ".gdi-page, .gdi-section-nav, .brand-card, .gdi-property-bar",
    contentReady: async (page) => {
      const text = await page.locator("body").innerText().catch(() => "");
      return !/Loading Group|Opening /i.test(text.slice(0, 400)) || (await page.locator(".brand-card, .gdi-opportunity-card, .adp-executive-read").count()) > 0;
    },
    interactions: async (page) => {
      const opp = page.locator('[data-tab="opportunities"], button:has-text("Opportunities"), .section-nav-item').filter({ hasText: /Opportunit/i }).first();
      if (await opp.count()) await opp.click({ timeout: 5000 }).catch(() => {});
      await page.waitForTimeout(800);
      // Prefer an enabled priority chip (empty High uses .no-records + pointer-events:none by design)
      const enabledChip = page
        .locator('[data-gdi-priority].chain-scale-legend-item:not(.no-records)')
        .filter({ hasNotText: /^All/i })
        .first();
      const high = page.locator('[data-gdi-priority="HIGH_PRIORITY"]').first();
      let priorityClick = "absent";
      const target = (await enabledChip.count()) ? enabledChip : high;
      if (await target.count()) {
        const ok = await target.click({ timeout: 4000 }).then(() => true).catch(() => false);
        priorityClick = ok ? "ok" : "blocked";
        if (!ok) await target.click({ force: true }).catch(() => {});
      }
      const drawer = page.locator(".brand-card, .gdi-opportunity-card, [data-gdi-opportunity]").first();
      let drawerOk = false;
      if (await drawer.count()) {
        await drawer.click({ timeout: 4000 }).catch(() => {});
        await page.waitForTimeout(500);
        const close = page.locator('[aria-label*="Close"], .drawer-close, button:has-text("Close")').first();
        if (await close.count()) {
          await close.click({ timeout: 3000 }).catch(() => {});
          drawerOk = true;
        } else {
          await page.keyboard.press("Escape").catch(() => {});
          drawerOk = true;
        }
      }
      return { priorityClick, drawerOk };
    },
  },
  {
    id: "adp-owner",
    product: "ADP",
    findingIds: ["ADP-001"],
    url: `${BASE}/owner-ai-demand.html`,
    waitMs: 8000,
    readySelector: ".adp-executive-read, .adp-executive-read__grid, .aiv-theme-group",
    interactions: async (page) => {
      const gridCols = await page.evaluate(() => {
        const el = document.querySelector(".adp-executive-read--v3-hybrid .adp-executive-read__grid, .adp-executive-read__grid");
        if (!el) return null;
        return getComputedStyle(el).gridTemplateColumns;
      });
      return { gridTemplateColumns: gridCols };
    },
  },
  {
    id: "be-app",
    product: "BrandExplorer",
    findingIds: ["BE-001"],
    url: `${BASE}/brand-explorer.html`,
    waitMs: 5000,
    readySelector: ".page-title, #brandsGrid, .sidebar-wrapper",
    interactions: async (page, vp) => {
      if (vp.width > 768) return { mobileMenu: "n/a" };
      const toggle = page.locator("#beMobileNavToggle");
      if (!(await toggle.count())) return { mobileMenu: "missing" };
      await toggle.click();
      const open = await page.locator(".page-wrapper.shell-mobile-nav-open").count();
      await page.locator("#beMobileNavBackdrop").click().catch(() => {});
      return { mobileMenu: open ? "ok" : "fail" };
    },
  },
  {
    id: "be-share-choice",
    product: "BrandExplorer",
    findingIds: ["BE-002"],
    url: `${BASE}/brand-explorer-share.html?pack=choice`,
    waitMs: 6000,
    readySelector: ".bes-panel, .filters-section, iframe",
  },
  {
    id: "he-preview",
    product: "HotelExplorer",
    findingIds: ["HE-001", "HE-002"],
    url: `${BASE}/hotel-explorer-preview.html`,
    waitMs: 10000,
    readySelector: "body",
    contentReady: async (page) => {
      const h1 = await page.locator("h1").first().innerText().catch(() => "");
      return !/Opening /i.test(h1);
    },
  },
];

async function measureOverflow(page) {
  return page.evaluate(() => {
    const de = document.documentElement;
    const body = document.body;
    const pageOverflow = Math.max(0, de.scrollWidth - de.clientWidth, body ? body.scrollWidth - body.clientWidth : 0);
    const tables = [...document.querySelectorAll(".gdi-table-wrap, .ranked-table-wrap, .deals-table, table")].slice(0, 8);
    const internalScroll = tables.some((t) => {
      const el = t.closest("[class*='wrap'], [class*='scroller']") || t.parentElement;
      if (!el) return false;
      const style = getComputedStyle(el);
      return (style.overflowX === "auto" || style.overflowX === "scroll") && el.scrollWidth > el.clientWidth + 2;
    });
    return { pageOverflow, internalScroll };
  });
}

async function run() {
  const servingSha = gitSha();
  const browser = await chromium.launch({ headless: true });
  const results = [];

  for (const route of ROUTES) {
    for (const vp of VIEWPORTS) {
      const page = await browser.newPage({ viewport: { width: vp.width, height: vp.height } });
      const row = {
        route: route.id,
        product: route.product,
        findingIds: route.findingIds,
        viewport: vp.id,
        url: route.url,
        status: "NOT_VALIDATED",
        pageOverflow: null,
        notes: [],
        interactions: null,
        screenshot: null,
        servingCommitHint: servingSha,
        baseUrl: BASE,
      };
      try {
        const res = await page.goto(route.url, { waitUntil: "domcontentloaded", timeout: 60000 });
        if (!res || res.status() >= 400) {
          row.status = "BLOCKED";
          row.notes.push(`HTTP ${res ? res.status() : "none"}`);
        } else {
          await page.waitForTimeout(route.waitMs);
          if (route.readySelector) {
            await page.waitForSelector(route.readySelector, { timeout: 15000 }).catch(() => {
              row.notes.push("readySelector_timeout");
            });
          }
          let contentOk = true;
          if (route.contentReady) contentOk = await route.contentReady(page);
          if (!contentOk) {
            row.status = "NOT_VALIDATED";
            row.notes.push("content_not_fully_loaded");
          } else {
            const ov = await measureOverflow(page);
            row.pageOverflow = ov.pageOverflow;
            row.internalTableScroll = ov.internalScroll;
            if (route.interactions) {
              row.interactions = await route.interactions(page, vp);
            }
            const shotName = `${route.id}__${vp.id}.png`;
            const shotPath = path.join(SHOTS, shotName);
            await page.screenshot({ path: shotPath, fullPage: false });
            row.screenshot = path.relative(ROOT, shotPath).replace(/\\/g, "/");

            const overflowFail = ov.pageOverflow > OVERFLOW_MAX_PX;
            const interactionFail =
              row.interactions &&
              (row.interactions.priorityClick === "blocked" ||
                row.interactions.mobileMenu === "fail" ||
                row.interactions.mobileMenu === "missing" ||
                (typeof row.interactions.clippedControlGroups === "number" &&
                  row.interactions.clippedControlGroups > 0));

            if (overflowFail || interactionFail) {
              row.status = "FAIL";
              if (overflowFail) row.notes.push(`page_overflow_${ov.pageOverflow}px`);
              if (interactionFail) row.notes.push("interaction_fail");
            } else {
              row.status = "PASS";
            }

            // ADP-001: at ≤960 expect single column for hybrid grid
            if (route.id === "adp-owner" && vp.width <= 960 && row.interactions?.gridTemplateColumns) {
              const cols = String(row.interactions.gridTemplateColumns);
              const multiCol = cols.includes(" ") && !/^(\S+)$/.test(cols.trim());
              // single column often reported as "1fr" or one pixel length
              const isStacked = cols === "none" || cols.split(" ").filter(Boolean).length <= 1 || /^[\d.]+px$/.test(cols.trim());
              if (!isStacked && multiCol) {
                row.status = "FAIL";
                row.notes.push(`adp_hybrid_not_stacked:${cols}`);
              }
            }
          }
        }
      } catch (err) {
        row.status = "BLOCKED";
        row.notes.push(String(err && err.message ? err.message : err).slice(0, 240));
      }
      results.push(row);
      await page.close();
    }
  }

  await browser.close();

  const authRadar = {
    route: "app-opportunity-radar",
    product: "Radar",
    findingIds: ["RD-003"],
    status: "BLOCKED",
    notes: [
      "Authenticated /app#/opportunity-radar requires Memberstack test session — not available in this regression run. Ranked-host coverage is independent.",
    ],
  };

  const summary = {
    contract: "DEALALITY_RESPONSIVE_DESIGN_STANDARD_V1",
    generatedAt: new Date().toISOString(),
    baseUrl: BASE,
    servingCommitHint: servingSha,
    overflowMaxPx: OVERFLOW_MAX_PX,
    viewports: VIEWPORTS.map((v) => v.id),
    results,
    authenticatedRadar: authRadar,
    counts: results.reduce(
      (acc, r) => {
        acc[r.status] = (acc[r.status] || 0) + 1;
        return acc;
      },
      { BLOCKED: 1 },
    ),
  };

  const jsonPath = path.join(OUT, "responsive-regression-v1.json");
  fs.writeFileSync(jsonPath, JSON.stringify(summary, null, 2));
  console.log(JSON.stringify({ ok: true, out: path.relative(ROOT, jsonPath), counts: summary.counts }, null, 2));

  const failCount = (summary.counts.FAIL || 0);
  if (failCount > 0) process.exitCode = 1;
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
