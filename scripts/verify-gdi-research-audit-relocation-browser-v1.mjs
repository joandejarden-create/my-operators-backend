#!/usr/bin/env node
/**
 * Browser evidence for GDI Research Audit relocation.
 * Captures desktop + mobile screenshots; does not publish or spend.
 *
 *   DEALALITY_LOCAL_ADMIN_BASE=http://127.0.0.1:8094 \
 *     node scripts/verify-gdi-research-audit-relocation-browser-v1.mjs
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const BASE = (
  process.env.DEALALITY_LOCAL_ADMIN_BASE ||
  "http://127.0.0.1:8094"
).replace(/\/$/, "");
const OUT = path.join(
  ROOT,
  "reports",
  "gdi-research-audit-admin-relocation-v1",
  "screenshots"
);
fs.mkdirSync(OUT, { recursive: true });

async function injectAdminAuth(page) {
  await page.addInitScript(() => {
    const plain = {
      authFetch(url, opts) {
        return fetch(
          url,
          Object.assign({ credentials: "same-origin" }, opts || {})
        );
      },
    };
    Object.defineProperty(window, "DealalityMemberstackAuth", {
      configurable: true,
      get() {
        return plain;
      },
      set() {},
    });
  });
  await page.route("**/api/me**", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        success: true,
        ok: true,
        user: {
          firstName: "Demo",
          lastName: "Dealality",
          email: "dealalitydemo@dealality.com",
        },
        dealality: {
          email: "dealalitydemo@dealality.com",
          role: "admin",
          isAdmin: true,
          isOwner: true,
          isDemo: true,
          adpMonthlyReviewAdmin: true,
          localDealalityAdminAccess: true,
          flags: { isAdmin: true, isDemo: true },
        },
      }),
    });
  });
}

async function shot(page, name) {
  const file = path.join(OUT, name);
  await page.screenshot({ path: file, fullPage: true });
  return file;
}

const browser = await chromium.launch({ headless: true });
const desktop = await browser.newPage({ viewport: { width: 1440, height: 900 } });
await injectAdminAuth(desktop);

// Admin — GDI Research Audit
await desktop.goto(`${BASE}/app/admin/ai-demand-admin.html?tab=gdi-research-audit`, {
  waitUntil: "domcontentloaded",
  timeout: 60000,
});
await desktop.evaluate(async () => {
  if (!window.SupportAdminGate) return;
  await window.SupportAdminGate.requireAdpMonthlyReviewAdmin({
    contentId: "supportGateContent",
    loadingMessage: "Verifying…",
  });
  const loadingEl = document.getElementById("supportGateLoading");
  if (window.SupportAdminGate.hidePageLoading) {
    window.SupportAdminGate.hidePageLoading(loadingEl);
  }
  const content = document.getElementById("supportGateContent");
  if (content) content.hidden = false;
  window.__ADP_ADMIN_WORKSPACE_AUTH__ = true;
  window.dispatchEvent(new CustomEvent("dealality-adp-admin-ready"));
  if (window.__ADP_ADMIN_SET_TAB__) window.__ADP_ADMIN_SET_TAB__("gdi-research-audit");
});
await desktop.waitForSelector("#adaPanelGdiResearchAudit", { timeout: 15000 });
await desktop.waitForFunction(
  () => document.querySelectorAll("#graHotelSelect option").length > 1,
  null,
  { timeout: 30000 }
);
await desktop.selectOption("#graHotelSelect", { index: 1 });
await desktop.waitForTimeout(1500);
await desktop.waitForFunction(
  () => {
    const t = document.getElementById("graStatusLine");
    const body = document.getElementById("graTableBody");
    const status = t ? String(t.textContent || "") : "";
    const rows = body ? body.querySelectorAll("tr").length : 0;
    return /Loaded|No research runs/i.test(status) || rows >= 1;
  },
  null,
  { timeout: 30000 }
);
const adminDesktop = await shot(desktop, "admin-gdi-research-audit-desktop.png");

const adminUi = await desktop.evaluate(() => {
  const tabs = [...document.querySelectorAll(".ada-tabs [data-tab]")].map((el) =>
    el.getAttribute("data-tab")
  );
  const panel = document.getElementById("adaPanelGdiResearchAudit");
  const body = document.getElementById("graTableBody");
  return {
    tabs,
    hasResearchAuditTab: tabs.includes("gdi-research-audit"),
    hasVisibilityAuditTab: tabs.includes("visibility-audit"),
    panelVisible: panel ? panel.hidden === false : false,
    labelPresent: /GDI Research/i.test(document.body.innerText || ""),
    hotelOptions: document.querySelectorAll("#graHotelSelect option").length,
    runRows: body ? body.querySelectorAll("tr").length : 0,
    status: document.getElementById("graStatusLine")?.textContent || "",
  };
});
assert.equal(adminUi.hasResearchAuditTab, true);
assert.equal(adminUi.hasVisibilityAuditTab, true);
assert.equal(adminUi.panelVisible, true);
assert.ok(adminUi.hotelOptions > 1, "hotel catalog must populate");

// Customer GDI — no Research Audit tab
await desktop.goto(`${BASE}/group-demand-intelligence.html`, {
  waitUntil: "domcontentloaded",
  timeout: 60000,
});
await desktop.waitForFunction(
  () => {
    const text = document.body ? String(document.body.innerText || "") : "";
    return (
      document.querySelector(".gdi-shell, .gdi-section-nav, [data-tab]") ||
      /Group|Opportunity|Demand|History unavailable|Research Completed|Unable|error/i.test(
        text
      )
    );
  },
  null,
  { timeout: 45000 }
).catch(() => {});
await desktop.waitForTimeout(1200);
const gdiDesktop = await shot(desktop, "gdi-customer-no-research-audit-desktop.png");
const gdiUi = await desktop.evaluate(() => {
  const tabs = [...document.querySelectorAll("[data-tab]")].map((el) =>
    el.getAttribute("data-tab")
  );
  const text = document.body ? document.body.innerText || "" : "";
  return {
    tabs,
    hasAuditTab: tabs.includes("audit"),
    hasResearchAuditLabel: /Research Audit/i.test(text),
    hasRunIdLeak: /gdi_run_/i.test(text),
    hasCostLeak: /Webhound\s*\$/i.test(text),
  };
});
assert.equal(gdiUi.hasAuditTab, false, "customer GDI must not expose audit tab");
assert.equal(gdiUi.hasResearchAuditLabel, false);
assert.equal(gdiUi.hasRunIdLeak, false);
assert.equal(gdiUi.hasCostLeak, false);

// Mobile Admin
const mobile = await browser.newPage({
  viewport: { width: 390, height: 844 },
  isMobile: true,
  hasTouch: true,
});
await injectAdminAuth(mobile);
await mobile.goto(`${BASE}/app/admin/ai-demand-admin.html?tab=gdi-research-audit`, {
  waitUntil: "domcontentloaded",
  timeout: 60000,
});
await mobile.evaluate(async () => {
  if (!window.SupportAdminGate) return;
  await window.SupportAdminGate.requireAdpMonthlyReviewAdmin({
    contentId: "supportGateContent",
    loadingMessage: "Verifying…",
  });
  const loadingEl = document.getElementById("supportGateLoading");
  if (window.SupportAdminGate.hidePageLoading) {
    window.SupportAdminGate.hidePageLoading(loadingEl);
  }
  const content = document.getElementById("supportGateContent");
  if (content) content.hidden = false;
  window.__ADP_ADMIN_WORKSPACE_AUTH__ = true;
  window.dispatchEvent(new CustomEvent("dealality-adp-admin-ready"));
  if (window.__ADP_ADMIN_SET_TAB__) window.__ADP_ADMIN_SET_TAB__("gdi-research-audit");
});
await mobile.waitForTimeout(800);
const adminMobile = await shot(mobile, "admin-gdi-research-audit-mobile.png");

await mobile.goto(`${BASE}/group-demand-intelligence.html`, {
  waitUntil: "domcontentloaded",
  timeout: 60000,
});
await mobile.waitForTimeout(1200);
const gdiMobile = await shot(mobile, "gdi-customer-no-research-audit-mobile.png");

await browser.close();

const summary = {
  ok: true,
  contract: "gdi-research-audit-relocation-browser-v1",
  base: BASE,
  screenshots: {
    adminDesktop,
    adminMobile,
    gdiDesktop,
    gdiMobile,
  },
  adminUi,
  gdiUi,
};
fs.writeFileSync(
  path.join(OUT, "..", "browser-acceptance.json"),
  JSON.stringify(summary, null, 2)
);
console.log(JSON.stringify(summary, null, 2));
